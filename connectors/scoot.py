"""
Scoot Playwright scraper — CDP Chrome + API interception via Navitaire Angular SPA.

Scoot (IATA: TR) is Singapore Airlines' low-cost subsidiary operating from SIN.
Uses a modern Navitaire Angular 20 booking engine at booking.flyscoot.com.
Protected by Akamai Bot Manager — requires real Chrome via CDP bypass.

Strategy:
1. Launch real Chrome via subprocess + connect via CDP (port 9448) to bypass
   Akamai bot challenge that blocks Playwright's bundled Chromium
2. Visit www.flyscoot.com/en first — Akamai warmup grants clearance cookies
3. Navigate to booking.flyscoot.com — Angular SPA loads with search form
4. Accept cookie consent banner
5. Fill search form via #originStation, #destinationStation, #departureDate
6. Set one-way mode, click "Let's Go!" submit button
7. Intercept the flight availability API response
8. Parse Navitaire Trips[].Flights[] structure with fare bundles for prices
9. Fallback: DOM extraction from flight result cards

Key API structure (Mar 2026):
  Session: GET /api/v1/account/anonymous  → JWT token
  Auth headers: Authorization: <JWT>, x-scoot-appsource: IBE-WEB
  Stations: GET /api/flights/resource/stations?cultureCode=en-sg
  Search: intercepted after form submit (POST availability endpoint)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import subprocess
import time
from datetime import datetime
from typing import Any, Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import find_chrome, stealth_popen_kwargs, _launched_procs

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-SG", "en-US", "en-GB", "en-AU"]
_TIMEZONES = [
    "Asia/Singapore", "Asia/Kuala_Lumpur", "Asia/Bangkok",
    "Asia/Tokyo", "Australia/Sydney",
]

_MAX_ATTEMPTS = 2
_DEBUG_PORT = 9448
_USER_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".scoot_chrome_data")

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch real headed Chrome via CDP (Akamai blocks headless).

    Uses a persistent user-data-dir so Akamai clearance persists across runs.
    """
    global _pw_instance, _browser, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    return _browser
            except Exception:
                pass

        from playwright.async_api import async_playwright

        # Try connecting to existing Chrome on the port first
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
            _pw_instance = pw
            logger.info("Scoot: connected to existing Chrome on port %d", _DEBUG_PORT)
            return _browser
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

        # Launch Chrome HEADED (no --headless) — Akamai blocks headless Chrome.
        chrome = find_chrome()
        os.makedirs(_USER_DATA_DIR, exist_ok=True)
        args = [
            chrome,
            f"--remote-debugging-port={_DEBUG_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_DEBUG_PORT}")
        logger.info("Scoot: Chrome launched headed on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
        return _browser


class ScootConnectorClient:
    """Scoot Playwright scraper — CDP Chrome + Navitaire API interception."""

    def __init__(self, timeout: float = 50.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                offers = await self._attempt_search(req, t0)
                if offers is not None:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)
                logger.warning("Scoot: attempt %d/%d got no results", attempt, _MAX_ATTEMPTS)
            except Exception as e:
                logger.warning("Scoot: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    async def _attempt_search(self, req: FlightSearchRequest, t0: float) -> Optional[list[FlightOffer]]:
        browser = await _get_browser()

        # CDP browsers use default context — reuse existing tab to keep cookies
        is_cdp = hasattr(browser, 'contexts') and browser.contexts
        if is_cdp:
            context = browser.contexts[0]
            # Close extra tabs, use the first page (avoids Akamai issues with new tabs)
            for p in context.pages[1:]:
                try:
                    await p.close()
                except Exception:
                    pass
            if context.pages:
                page = context.pages[0]
            else:
                page = await context.new_page()
        else:
            context = await browser.new_context(
                viewport=random.choice(_VIEWPORTS),
                locale=random.choice(_LOCALES),
                timezone_id=random.choice(_TIMEZONES),
                service_workers="block",
            )
            page = await context.new_page()

        try:
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            # API response capture — only listen after search button is clicked
            captured_data: dict = {}
            api_event = asyncio.Event()
            search_clicked = {"ready": False}

            async def on_response(response):
                if not search_clicked["ready"]:
                    return
                try:
                    url = response.url.lower()
                    status = response.status
                    if status != 200:
                        return
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    # Match search/availability endpoints, skip lowfare (both /estimate and direct)
                    if "lowfare" in url:
                        return
                    if any(k in url for k in [
                        "availability", "/api/flights/search",
                        "flightsearch", "search/flights", "/api/v1/availability",
                        "/api/nsk/", "trips", "air-bounds",
                    ]):
                        data = await response.json()
                        if data and isinstance(data, (dict, list)):
                            captured_data["search"] = data
                            captured_data["search_url"] = response.url
                            api_event.set()
                            logger.info("Scoot: captured search API from %s",
                                        response.url[:100])
                except Exception:
                    pass

            page.on("response", on_response)

            # Step 1: Akamai warmup via www.flyscoot.com
            logger.info("Scoot: Akamai warmup via www.flyscoot.com")
            try:
                await page.goto("https://www.flyscoot.com/en",
                                wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(3)
                await self._dismiss_cookies(page)
            except Exception:
                pass  # main site may timeout — we just need the cookies

            # Step 2: Navigate to booking engine
            logger.info("Scoot: loading booking.flyscoot.com for %s->%s",
                        req.origin, req.destination)
            await page.goto("https://booking.flyscoot.com",
                            wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(5)

            # Accept cookies on booking site too
            await self._dismiss_cookies(page)

            # Wait for SPA to render (use JS to check for visible input — avoids
            # the duplicate-ID trap where wait_for_selector picks the hidden one)
            spa_ready = False
            for _wait_round in range(6):  # up to ~30s total
                spa_ready = await page.evaluate("""() => {
                    const inputs = document.querySelectorAll('input#originStation');
                    return Array.from(inputs).some(i => i.offsetHeight > 0);
                }""")
                if spa_ready:
                    break
                await asyncio.sleep(5)
            if not spa_ready:
                logger.warning("Scoot: search form never appeared")
                return None

            # Step 3: Set one-way
            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            # Step 5: Fill origin
            ok = await self._fill_station(page, "#originStation", req.origin)
            if not ok:
                logger.warning("Scoot: origin fill failed for %s", req.origin)
                return None
            await asyncio.sleep(0.5)

            # Step 6: Fill destination
            ok = await self._fill_station(page, "#destinationStation", req.destination)
            if not ok:
                logger.warning("Scoot: destination fill failed for %s", req.destination)
                return None
            await asyncio.sleep(0.5)

            # Step 7: Fill date
            ok = await self._fill_date(page, req.date_from)
            if not ok:
                logger.warning("Scoot: date fill failed")
                return None
            await asyncio.sleep(0.3)

            # Step 8: Click search (enable API capture first)
            search_clicked["ready"] = True
            await self._click_search(page)

            # Step 9: Wait for API response
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.info("Scoot: API intercept timed out, trying DOM extraction")
                offers = await self._extract_from_dom(page, req)
                if offers:
                    return offers
                offers = await self._extract_from_page_data(page, req)
                return offers if offers else None

            data = captured_data.get("search")
            if data:
                offers = self._parse_navitaire_response(data, req)
                if offers:
                    return offers

            # Fallback to DOM
            return await self._extract_from_dom(page, req) or None

        finally:
            if is_cdp:
                # Don't close the reused tab — just navigate away
                try:
                    await page.goto("about:blank", timeout=5000)
                except Exception:
                    pass
            else:
                await page.close()
                await context.close()

    # ── Cookie / overlay dismissal ──────────────────────────────────────────

    async def _dismiss_cookies(self, page) -> None:
        """Dismiss cookie consent and overlay banners."""
        for selector in [
            "text='Accept all cookies'",
            "button:has-text('Accept all cookies')",
            "button:has-text('Accept All')",
            "button:has-text('Accept')",
        ]:
            try:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="onetrust"], [id*="onetrust"], [class*="modal-overlay"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ── One-way toggle ──────────────────────────────────────────────────────

    async def _set_one_way(self, page) -> None:
        """Click One-Way radio/tab if not already selected."""
        try:
            one_way = page.locator("text='One-Way'").first
            if await one_way.count() > 0:
                await one_way.click(timeout=3000)
                await asyncio.sleep(0.5)
                return
        except Exception:
            pass
        for label in ["One Way", "ONE WAY", "One-way", "one way"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if await el.count() > 0:
                    await el.click(timeout=2000)
                    return
            except Exception:
                continue

    # ── Station (airport) fill ──────────────────────────────────────────────

    async def _fill_station(self, page, selector: str, iata: str) -> bool:
        """Fill an airport input (#originStation or #destinationStation).

        The Angular SPA duplicates elements (visible + hidden tabs).
        We use JS to find the visible input and interact with it directly.
        """
        try:
            is_origin = "origin" in selector.lower()
            field_id = "originStation" if is_origin else "destinationStation"

            # Use JS to click the visible input (avoids duplicate-ID ambiguity)
            clicked = await page.evaluate("""(fieldId) => {
                const inputs = document.querySelectorAll('input#' + fieldId);
                for (const inp of inputs) {
                    if (inp.offsetHeight > 0) {
                        inp.click();
                        inp.focus();
                        return true;
                    }
                }
                return false;
            }""", field_id)
            if not clicked:
                logger.debug("Scoot: no visible input for %s", field_id)
                return False
            await asyncio.sleep(0.5)

            # Clear via JS + Angular event dispatch
            await page.evaluate("""(fieldId) => {
                const inputs = document.querySelectorAll('input#' + fieldId);
                for (const inp of inputs) {
                    if (inp.offsetHeight > 0) {
                        inp.value = '';
                        inp.dispatchEvent(new Event('input', {bubbles: true}));
                        inp.dispatchEvent(new Event('change', {bubbles: true}));
                        return;
                    }
                }
            }""", field_id)
            await asyncio.sleep(0.2)

            # Type IATA code character by character (triggers Angular keypress)
            await page.keyboard.type(iata, delay=100)
            await asyncio.sleep(2.5)

            # Click station suggestion from overlay via JS
            suggestion_clicked = await page.evaluate("""(iata) => {
                const overlays = document.querySelectorAll('.stations-overlay');
                for (const overlay of overlays) {
                    if (overlay.offsetHeight === 0) continue;
                    const byLabel = overlay.querySelector('div[aria-label="' + iata + '"]');
                    if (byLabel && byLabel.offsetHeight > 0) {
                        byLabel.click();
                        return 'aria-label';
                    }
                    const codes = overlay.querySelectorAll('.code');
                    for (const code of codes) {
                        if (code.textContent.trim() === iata && code.offsetHeight > 0) {
                            code.parentElement.click();
                            return 'code-parent';
                        }
                    }
                    const items = overlay.querySelectorAll(
                        'div.current-location, div.station-item, li');
                    for (const item of items) {
                        if (item.textContent.includes(iata) && item.offsetHeight > 0) {
                            item.click();
                            return 'text-match';
                        }
                    }
                }
                return null;
            }""", iata)

            if suggestion_clicked:
                logger.info("Scoot: selected station %s via JS %s", iata, suggestion_clicked)
                await asyncio.sleep(0.5)
                return True

            # Fallback: Playwright selectors
            for sel in [
                f".stations-overlay div[aria-label='{iata}']",
                f".stations-overlay :text-is('{iata}')",
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        logger.info("Scoot: selected station %s via %s", iata, sel)
                        return True
                except Exception:
                    continue

            # Last resort: press Enter for top suggestion
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.5)
            logger.info("Scoot: pressed Enter for station %s", iata)
            return True

        except Exception as e:
            logger.warning("Scoot: station fill error for %s: %s", iata, e)
            return False

    # ── Date fill ───────────────────────────────────────────────────────────

    async def _fill_date(self, page, target: datetime) -> bool:
        """Fill the departure date using the ngb-datepicker calendar.

        Calendar uses: div.ngb-dp-month-name ("March 2026"),
        button[aria-label="Next month"], and
        div.ngb-dp-day[role="gridcell"][aria-label="Thursday, April 9, 2026"].
        """
        try:
            # Click the visible #departureDate to open the calendar
            opened = await page.evaluate("""() => {
                const els = document.querySelectorAll('#departureDate');
                for (const el of els) {
                    if (el.offsetHeight > 0) { el.click(); return true; }
                }
                return false;
            }""")
            if not opened:
                logger.warning("Scoot: no visible #departureDate to click")
                return False
            await asyncio.sleep(1)

            target_month_year = target.strftime("%B %Y")  # e.g. "April 2026"

            # Navigate calendar to the target month
            for _ in range(12):
                visible_months = await page.evaluate("""() => {
                    return Array.from(document.querySelectorAll('.ngb-dp-month-name'))
                        .filter(e => e.offsetHeight > 0)
                        .map(e => e.textContent.trim());
                }""")
                if target_month_year in visible_months:
                    break
                # Click "Next month"
                try:
                    await page.locator(
                        "button[aria-label='Next month']"
                    ).first.click(timeout=3000)
                    await asyncio.sleep(0.5)
                except Exception:
                    logger.warning("Scoot: can't find Next month button")
                    break

            # Build the aria-label for the target day, e.g. "Thursday, April 9, 2026"
            # The calendar uses full day-of-week names
            day_label = target.strftime("%A, %B ") + str(target.day) + target.strftime(", %Y")

            # Click the day cell via JS — click the gridcell div directly
            # (clicking the inner .custom-day span doesn't trigger Angular)
            clicked = await page.evaluate("""(label) => {
                const cells = document.querySelectorAll(
                    'div.ngb-dp-day[role="gridcell"]:not(.disabled)');
                for (const cell of cells) {
                    if (cell.offsetHeight > 0 &&
                        cell.getAttribute('aria-label') === label) {
                        cell.click();
                        return true;
                    }
                }
                return false;
            }""", day_label)

            if clicked:
                logger.info("Scoot: selected date %s", target.strftime("%Y-%m-%d"))
                await asyncio.sleep(0.5)
                await self._close_calendar(page)
                return True

            # Fallback: match by day number within visible non-disabled cells
            day_num = str(target.day)
            clicked2 = await page.evaluate("""(dayNum) => {
                const cells = document.querySelectorAll(
                    'div.ngb-dp-day[role="gridcell"]:not(.disabled)');
                for (const cell of cells) {
                    if (cell.offsetHeight > 0 &&
                        cell.textContent.trim() === dayNum) {
                        cell.click();
                        return true;
                    }
                }
                return false;
            }""", day_num)

            if clicked2:
                logger.info("Scoot: selected date %s via day-number fallback", target.strftime("%Y-%m-%d"))
                await asyncio.sleep(0.5)
                await self._close_calendar(page)
                return True

            logger.warning("Scoot: couldn't select date %s (label=%s)", target.strftime("%Y-%m-%d"), day_label)
            return False
        except Exception as e:
            logger.warning("Scoot: date fill error: %s", e)
            return False

    async def _close_calendar(self, page) -> None:
        """Click the 'Done' button to close the calendar picker."""
        try:
            done_clicked = await page.evaluate("""() => {
                const btns = document.querySelectorAll('button, div[role="button"]');
                for (const btn of btns) {
                    if (btn.offsetHeight > 0 && btn.textContent.trim() === 'Done') {
                        btn.click();
                        return true;
                    }
                }
                return false;
            }""")
            if done_clicked:
                logger.info("Scoot: closed calendar via Done button")
                await asyncio.sleep(0.5)
        except Exception:
            pass

    # ── Search submit ───────────────────────────────────────────────────────

    async def _click_search(self, page) -> None:
        """Click the 'Let's Go!' search button via JS."""
        clicked = await page.evaluate("""() => {
            const labels = ["Let's Go!", "Search flights", "Search", "SEARCH", "Find flights"];
            const btns = document.querySelectorAll('button, a[role="button"], div[role="button"]');
            for (const label of labels) {
                for (const btn of btns) {
                    if (btn.offsetHeight > 0 && btn.textContent.trim() === label) {
                        btn.click();
                        return label;
                    }
                }
            }
            // Fallback: submit button
            const submit = document.querySelector('button[type="submit"]');
            if (submit && submit.offsetHeight > 0) {
                submit.click();
                return 'submit';
            }
            return null;
        }""")
        if clicked:
            logger.info("Scoot: clicked search button '%s'", clicked)
        else:
            await page.keyboard.press("Enter")
            logger.info("Scoot: pressed Enter as search fallback")

    # ── DOM extraction fallback ─────────────────────────────────────────────

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight offers from DOM elements on the results page."""
        try:
            await asyncio.sleep(3)
            offers_data = await page.evaluate("""() => {
                const results = [];
                const cardSelectors = [
                    '[class*="flight-card"]', '[class*="flight-row"]',
                    '[class*="journey-card"]', '[class*="flight-result"]',
                    '[class*="fare-card"]', '[class*="flight-select"]',
                    '[data-test*="flight"]', '[class*="availability-row"]',
                ];
                for (const sel of cardSelectors) {
                    const cards = document.querySelectorAll(sel);
                    if (cards.length > 0) {
                        cards.forEach(card => {
                            const text = card.innerText || '';
                            const priceMatch = text.match(/(?:SGD|USD|EUR|\\$)\\s*([\\d,]+\\.?\\d*)/i)
                                || text.match(/([\\d,]+\\.?\\d*)\\s*(?:SGD|USD|EUR)/i);
                            const timeMatch = text.match(/(\\d{1,2}:\\d{2})\\s*(?:am|pm)?/gi);
                            const flightMatch = text.match(/(?:TR|TZ|3K)\\s*\\d+/i);
                            if (priceMatch || timeMatch)
                                results.push({
                                    price: priceMatch ? parseFloat(priceMatch[1].replace(',', '')) : null,
                                    times: timeMatch || [],
                                    flightNo: flightMatch ? flightMatch[0] : null,
                                    fullText: text.substring(0, 300),
                                });
                        });
                        break;
                    }
                }
                const bundleScript = document.querySelector(
                    'script#bundle-data-v2, script#bundle-data');
                if (bundleScript) {
                    try { return { type: 'bundle', data: JSON.parse(bundleScript.textContent) }; }
                    catch {}
                }
                return { type: 'dom', cards: results };
            }""")

            if not offers_data:
                return []

            data_type = offers_data.get("type", "")
            if data_type == "bundle":
                return self._parse_navitaire_response(offers_data.get("data", {}), req)
            if data_type == "dom":
                return self._parse_dom_cards(offers_data.get("cards", []), req)
            return []
        except Exception as e:
            logger.debug("Scoot: DOM extraction error: %s", e)
            return []

    async def _extract_from_page_data(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight data from Angular component state or inline scripts."""
        try:
            data = await page.evaluate("""() => {
                const scripts = document.querySelectorAll('script');
                for (const s of scripts) {
                    const t = s.textContent || '';
                    const m = t.match(/FlightData\\s*=\\s*'([\\s\\S]*?)';/);
                    if (m) return { type: 'flightdata', raw: m[1] };
                    const m2 = t.match(/(?:availability|flightSearch)\\s*=\\s*({[\\s\\S]*?});/);
                    if (m2) return { type: 'json', raw: m2[1] };
                }
                if (window.FlightData) return { type: 'global', data: window.FlightData };
                if (window.AvailabilityV2) return { type: 'global', data: window.AvailabilityV2 };
                return null;
            }""")
            if not data:
                return []
            if data.get("type") == "flightdata":
                import html as html_mod
                raw = html_mod.unescape(data["raw"])
                return self._parse_navitaire_response(json.loads(raw), req)
            if data.get("type") == "json":
                return self._parse_navitaire_response(json.loads(data["raw"]), req)
            if data.get("type") == "global" and data.get("data"):
                return self._parse_navitaire_response(data["data"], req)
            return []
        except Exception:
            return []

    # ── Response parsing ────────────────────────────────────────────────────

    def _parse_navitaire_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Scoot Navitaire availability API response.

        Structure: data.trips[].journeys[] with pricing in data.faresAvailable[].
        Each journey.fares[].fareAvailabilityKey maps to faresAvailable[].totals.fareTotal.
        """
        if not isinstance(data, dict):
            return []

        offers: list[FlightOffer] = []
        currency = data.get("currencyCode", "SGD")
        booking_url = self._build_booking_url(req)

        # Build fare price lookup: fareAvailabilityKey → lowest fareTotal
        fare_lookup: dict[str, float] = {}
        for fa in data.get("faresAvailable", []):
            key = fa.get("fareAvailabilityKey", "")
            totals = fa.get("totals", {})
            price = totals.get("fareTotal")
            if key and price is not None:
                fare_lookup[key] = float(price)

        trips = data.get("trips", [])
        for trip in trips:
            if not isinstance(trip, dict):
                continue
            for journey in trip.get("journeys", []):
                if not isinstance(journey, dict):
                    continue

                # Find cheapest fare for this journey
                best_price = float("inf")
                for fare in journey.get("fares", []):
                    fkey = fare.get("fareAvailabilityKey", "")
                    if fkey in fare_lookup:
                        p = fare_lookup[fkey]
                        if 0 < p < best_price:
                            best_price = p
                if best_price == float("inf"):
                    continue

                # Parse segments
                segments: list[FlightSegment] = []
                for seg in journey.get("segments", []):
                    ident = seg.get("identifier", {})
                    desig = seg.get("designator", {})
                    carrier = ident.get("carrierCode", "TR")
                    flight_num = str(ident.get("identifier", "")).strip()
                    flight_no = f"{carrier}{flight_num}" if flight_num else ""
                    segments.append(FlightSegment(
                        airline=carrier,
                        airline_name="Scoot",
                        flight_no=flight_no,
                        origin=desig.get("origin", req.origin),
                        destination=desig.get("destination", req.destination),
                        departure=self._parse_dt(desig.get("departure", "")),
                        arrival=self._parse_dt(desig.get("arrival", "")),
                        cabin_class="M",
                    ))

                if not segments:
                    # Fallback: use journey-level designator
                    desig = journey.get("designator", {})
                    if desig:
                        segments.append(FlightSegment(
                            airline="TR", airline_name="Scoot",
                            flight_no="",
                            origin=desig.get("origin", req.origin),
                            destination=desig.get("destination", req.destination),
                            departure=self._parse_dt(desig.get("departure", "")),
                            arrival=self._parse_dt(desig.get("arrival", "")),
                            cabin_class="M",
                        ))
                    if not segments:
                        continue

                total_dur = 0
                if segments[0].departure and segments[-1].arrival:
                    delta = segments[-1].arrival - segments[0].departure
                    total_dur = max(int(delta.total_seconds()), 0)

                route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=total_dur,
                    stopovers=max(len(segments) - 1, 0),
                )

                journey_key = journey.get("journeyKey", f"{req.origin}{req.destination}{time.monotonic()}")

                offers.append(FlightOffer(
                    id=f"tr_{hashlib.md5(str(journey_key).encode()).hexdigest()[:12]}",
                    price=round(best_price, 2),
                    currency=currency,
                    price_formatted=f"{best_price:.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=["Scoot"],
                    owner_airline="TR",
                    booking_url=booking_url,
                    is_locked=False,
                    source="scoot_direct",
                    source_tier="free",
                ))

        return offers

    def _parse_single_flight(self, flight: dict, currency: str,
                             req: FlightSearchRequest,
                             booking_url: str) -> Optional[FlightOffer]:
        best_price = self._extract_best_price(flight)
        if best_price is None or best_price <= 0:
            return None

        segments = self._parse_segments(flight, req)
        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            delta = segments[-1].arrival - segments[0].departure
            total_dur = max(int(delta.total_seconds()), 0)

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = (
            flight.get("JourneySellKey") or flight.get("journeySellKey")
            or flight.get("journeyKey") or flight.get("id")
            or f"{req.origin}{req.destination}{time.monotonic()}"
        )

        return FlightOffer(
            id=f"tr_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=["Scoot"], owner_airline="TR",
            booking_url=booking_url, is_locked=False,
            source="scoot_direct", source_tier="free",
        )

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        best = float("inf")
        # Navitaire Bundles (like Jetstar)
        bundles = flight.get("Bundles") or flight.get("bundles") or []
        for bundle in bundles:
            if isinstance(bundle, dict):
                for key in [
                    "RegularInclusiveAmount", "regularInclusiveAmount",
                    "CjInclusiveAmount", "cjInclusiveAmount",
                    "TotalAmount", "totalAmount",
                    "Amount", "amount", "Price", "price",
                ]:
                    val = bundle.get(key)
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        # Navitaire Fares
        fares = (flight.get("Fares") or flight.get("fares")
                 or flight.get("fareProducts") or [])
        for fare in fares:
            if isinstance(fare, dict):
                for key in ["price", "amount", "totalPrice", "basePrice",
                            "fareAmount", "totalAmount",
                            "PassengerFare", "passengerFare"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = (val.get("Amount") or val.get("amount")
                               or val.get("TotalAmount"))
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        # Direct price fields
        for key in ["price", "lowestFare", "totalPrice", "farePrice",
                     "amount", "lowestPrice", "Price", "LowestFare", "TotalPrice"]:
            p = flight.get(key)
            if p is not None:
                try:
                    v = (float(p) if not isinstance(p, dict)
                         else float(p.get("Amount") or p.get("amount") or 0))
                    if 0 < v < best:
                        best = v
                except (TypeError, ValueError):
                    pass
        return best if best < float("inf") else None

    def _parse_segments(self, flight: dict, req: FlightSearchRequest) -> list[FlightSegment]:
        segments: list[FlightSegment] = []
        segs_raw = (
            flight.get("Segments") or flight.get("segments")
            or flight.get("Legs") or flight.get("legs")
            or flight.get("flights") or []
        )
        if segs_raw and isinstance(segs_raw, list):
            for seg in segs_raw:
                if isinstance(seg, dict):
                    segments.append(self._build_segment(seg, req.origin, req.destination))
            if segments:
                return segments

        # Parse from JourneySellKey
        journey_key = flight.get("JourneySellKey") or flight.get("journeySellKey") or ""
        if journey_key and "~" in journey_key:
            seg = self._parse_journey_sell_key(journey_key, req)
            if seg:
                return [seg]

        # Build from flight-level fields
        dep_str = (flight.get("DepartureDateTime") or flight.get("departureDateTime")
                   or flight.get("departure") or flight.get("departureDate")
                   or flight.get("STD") or "")
        arr_str = (flight.get("ArrivalDateTime") or flight.get("arrivalDateTime")
                   or flight.get("arrival") or flight.get("arrivalDate")
                   or flight.get("STA") or "")
        flight_no_raw = (flight.get("FlightNumber") or flight.get("flightNumber")
                         or flight.get("FlightDesignator", {}).get("FlightNumber", "")
                         or "")
        carrier = (flight.get("CarrierCode") or flight.get("carrierCode")
                   or flight.get("FlightDesignator", {}).get("CarrierCode", "")
                   or "TR")
        origin = (flight.get("Origin") or flight.get("origin")
                  or flight.get("DepartureStation") or req.origin)
        dest = (flight.get("Destination") or flight.get("destination")
                or flight.get("ArrivalStation") or req.destination)

        flight_no = str(flight_no_raw).replace(" ", "")
        if flight_no and not flight_no.startswith(carrier):
            flight_no = f"{carrier}{flight_no}"

        segments.append(FlightSegment(
            airline=carrier, airline_name="Scoot",
            flight_no=flight_no, origin=origin, destination=dest,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        ))
        return segments

    @staticmethod
    def _parse_journey_sell_key(key: str, req: FlightSearchRequest) -> Optional[FlightSegment]:
        """Parse Navitaire JourneySellKey: TR~ 501~ ~~SIN~06/15/2026 06:00~BKK~..."""
        try:
            parts = key.split("~")
            if len(parts) < 7:
                return None
            carrier = parts[0].strip() or "TR"
            flight_no_raw = parts[1].strip()
            flight_no = f"{carrier}{flight_no_raw}" if flight_no_raw else ""
            origin = dest = ""
            dep_str = arr_str = ""
            for i, part in enumerate(parts):
                part = part.strip()
                if len(part) == 3 and part.isalpha() and part.isupper():
                    if not origin:
                        origin = part
                        if i + 1 < len(parts):
                            dep_str = parts[i + 1].strip()
                    elif not dest:
                        dest = part
                        if i + 1 < len(parts):
                            arr_str = parts[i + 1].strip()
            return FlightSegment(
                airline=carrier, airline_name="Scoot",
                flight_no=flight_no,
                origin=origin or req.origin,
                destination=dest or req.destination,
                departure=ScootConnectorClient._parse_dt(dep_str),
                arrival=ScootConnectorClient._parse_dt(arr_str),
                cabin_class="M",
            )
        except Exception:
            return None

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = (seg.get("DepartureDateTime") or seg.get("departureDateTime")
                   or seg.get("departure") or seg.get("STD") or seg.get("std") or "")
        arr_str = (seg.get("ArrivalDateTime") or seg.get("arrivalDateTime")
                   or seg.get("arrival") or seg.get("STA") or seg.get("sta") or "")
        flight_no_raw = str(
            seg.get("FlightNumber") or seg.get("flightNumber")
            or seg.get("FlightDesignator", {}).get("FlightNumber", "")
            or ""
        ).replace(" ", "")
        carrier = (seg.get("CarrierCode") or seg.get("carrierCode")
                   or seg.get("FlightDesignator", {}).get("CarrierCode", "") or "TR")
        origin = (seg.get("Origin") or seg.get("origin")
                  or seg.get("DepartureStation") or default_origin)
        dest = (seg.get("Destination") or seg.get("destination")
                or seg.get("ArrivalStation") or default_dest)

        flight_no = flight_no_raw
        if flight_no and not flight_no.startswith(carrier):
            flight_no = f"{carrier}{flight_no}"

        return FlightSegment(
            airline=carrier, airline_name="Scoot",
            flight_no=flight_no, origin=origin, destination=dest,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _parse_dom_cards(self, cards: list, req: FlightSearchRequest) -> list[FlightOffer]:
        offers = []
        booking_url = self._build_booking_url(req)
        for card in cards:
            price = card.get("price")
            if not price or price <= 0:
                continue
            flight_no = card.get("flightNo", "")
            times = card.get("times", [])
            dep_time = self._parse_dt(times[0]) if times else datetime(2000, 1, 1)
            arr_time = self._parse_dt(times[1]) if len(times) > 1 else datetime(2000, 1, 1)
            total_dur = 0
            if dep_time.year > 2000 and arr_time.year > 2000:
                total_dur = max(int((arr_time - dep_time).total_seconds()), 0)
            seg = FlightSegment(
                airline="TR", airline_name="Scoot",
                flight_no=flight_no or "", origin=req.origin,
                destination=req.destination,
                departure=dep_time, arrival=arr_time, cabin_class="M",
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=total_dur,
                                stopovers=0)
            offers.append(FlightOffer(
                id=f"tr_{hashlib.md5(f'{flight_no}{price}'.encode()).hexdigest()[:12]}",
                price=round(price, 2), currency="SGD",
                price_formatted=f"{price:.2f} SGD",
                outbound=route, inbound=None,
                airlines=["Scoot"], owner_airline="TR",
                booking_url=booking_url, is_locked=False,
                source="scoot_direct", source_tier="free",
            ))
        return offers

    # ── Utilities ───────────────────────────────────────────────────────────

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest,
                        elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Scoot %s->%s returned %d offers in %.1fs (CDP Chrome)",
                    req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(
            f"scoot{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s).strip()
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in (
            "%m/%d/%Y %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M",
            "%H:%M",
        ):
            try:
                return datetime.strptime(s[:len(fmt) + 4], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://booking.flyscoot.com/"
            f"?origin={req.origin}&destination={req.destination}&depart={dep}"
            f"&pax={req.adults or 1}&type=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"scoot{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
