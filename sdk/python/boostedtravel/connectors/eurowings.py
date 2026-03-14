"""
Eurowings cookie-farm hybrid connector.

Strategy (converted Mar 2026):
1. Farm Cloudflare cookies via CDP Chrome (once per ~25 min, just visit homepage)
2. POST to apps.eurowings.com/flightsearch/v1/booking/flight-data via curl_cffi (~0.7s)
3. Parse QUERY_FLIGHT_DATA JSON → FlightOffers
4. Fall back to full Playwright browser automation if API fails
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

from curl_cffi import requests as cffi_requests

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import stealth_args, stealth_popen_kwargs

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-DE"]
_TIMEZONES = ["Europe/London", "Europe/Berlin", "Europe/Paris", "Europe/Vienna"]

_CDP_PORT = 9455
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", "/tmp"), "eurowings_cdp_data")

_API_URL = "https://apps.eurowings.com/flightsearch/v1/booking/flight-data?action=QUERY_FLIGHT_DATA"
_IMPERSONATE = "chrome136"
_COOKIE_MAX_AGE = 25 * 60  # 25 minutes

# ── Module-level singletons ──────────────────────────────────────────────────
_chrome_proc: subprocess.Popen | None = None
_pw_instance = None
_cdp_browser = None
_browser_lock: Optional[asyncio.Lock] = None

_farm_lock: Optional[asyncio.Lock] = None
_farmed_cookies: list[dict] = []
_farm_timestamp: float = 0


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _get_browser():
    """Shared real Chrome via CDP (launched once, reused across searches)."""
    global _pw_instance, _cdp_browser, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _cdp_browser and _cdp_browser.is_connected():
            return _cdp_browser
        from connectors.browser import get_or_launch_cdp
        _cdp_browser, _chrome_proc = await get_or_launch_cdp(_CDP_PORT, _USER_DATA_DIR)
        logger.info("Eurowings: Chrome ready via CDP (port %d)", _CDP_PORT)
        return _cdp_browser


class EurowingsConnectorClient:
    """Eurowings hybrid scraper — cookie-farm + curl_cffi direct API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Eurowings flights via cookie-farm + curl_cffi direct API.

        Fast path (~0.7s): curl_cffi with farmed CF cookies → POST QUERY_FLIGHT_DATA.
        Slow path (~15s): Playwright farms cookies first, then curl_cffi.
        Fallback: Full Playwright interception if API fails.
        """
        t0 = time.monotonic()

        try:
            # Fast path: try with existing farmed cookies
            cookies = await self._ensure_cookies()
            data = None
            if cookies:
                data = await self._api_search(req, cookies)

            # Re-farm once if stale / rejected
            if data is None:
                logger.info("Eurowings: API search failed, re-farming cookies")
                cookies = await self._farm_cookies()
                if cookies:
                    data = await self._api_search(req, cookies)

            if data:
                elapsed = time.monotonic() - t0
                offers = self._parse_response(data, req)
                if offers:
                    offers.sort(key=lambda o: o.price)
                    logger.info(
                        "Eurowings %s→%s returned %d offers in %.1fs (hybrid API)",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    return self._build_response(offers, req, elapsed)

            # Last resort: full Playwright browser fallback
            logger.warning("Eurowings: API returned no data, falling back to Playwright")
            return await self._playwright_fallback(req, t0)

        except Exception as e:
            logger.error("Eurowings hybrid error: %s", e)
            return self._empty(req)

    # ------------------------------------------------------------------
    # Cookie farm — Playwright generates CF cookies
    # ------------------------------------------------------------------

    async def _ensure_cookies(self) -> list[dict]:
        """Return valid farmed cookies, farming new ones if needed."""
        global _farmed_cookies, _farm_timestamp
        lock = _get_farm_lock()
        async with lock:
            age = time.monotonic() - _farm_timestamp
            if _farmed_cookies and age < _COOKIE_MAX_AGE:
                return _farmed_cookies
            return await self._farm_cookies()

    async def _farm_cookies(self) -> list[dict]:
        """Visit Eurowings homepage to harvest Cloudflare cookies."""
        global _farmed_cookies, _farm_timestamp

        browser = await _get_browser()
        ctx = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        try:
            page = await ctx.new_page()
            logger.info("Eurowings: farming cookies via homepage visit")
            await page.goto(
                "https://www.eurowings.com/en.html",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await asyncio.sleep(random.uniform(2.5, 4))
            await self._dismiss_cookies(page)
            await asyncio.sleep(1)

            cookies = await ctx.cookies()
            _farmed_cookies = cookies
            _farm_timestamp = time.monotonic()
            logger.info("Eurowings: farmed %d cookies", len(cookies))
            return cookies
        except Exception as e:
            logger.error("Eurowings: cookie farm error: %s", e)
            return []
        finally:
            await ctx.close()

    # ------------------------------------------------------------------
    # Direct API via curl_cffi
    # ------------------------------------------------------------------

    async def _api_search(
        self, req: FlightSearchRequest, cookies: list[dict],
    ) -> Optional[dict]:
        """POST QUERY_FLIGHT_DATA via curl_cffi with farmed cookies."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._api_search_sync, req, cookies)

    def _api_search_sync(
        self, req: FlightSearchRequest, cookies: list[dict],
    ) -> Optional[dict]:
        """Synchronous curl_cffi QUERY_FLIGHT_DATA search."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)

        for c in cookies:
            domain = c.get("domain", "")
            sess.cookies.set(c["name"], c["value"], domain=domain)

        body = json.dumps(self._build_api_body(req))

        try:
            r = sess.post(
                _API_URL,
                headers={
                    "content-type": "application/json",
                    "accept": "application/json, text/plain, */*",
                    "referer": "https://www.eurowings.com/",
                },
                data=body,
                timeout=15,
            )
        except Exception as e:
            logger.error("Eurowings: API request failed: %s", e)
            return None

        if r.status_code != 200:
            logger.warning("Eurowings: API returned %d", r.status_code)
            return None

        try:
            data = r.json()
        except Exception:
            logger.warning("Eurowings: API response not JSON")
            return None

        status = data.get("_payload", {}).get("_status", "")
        if status != "SUCCESS":
            logger.warning("Eurowings: API status=%s", status)
            return None

        return data

    @staticmethod
    def _build_api_body(req: FlightSearchRequest) -> dict:
        """Build the QUERY_FLIGHT_DATA request payload."""
        return {
            "_payload": {
                "_type": "UPDATE_COMPONENT",
                "_updates": [{
                    "_type": "ew/components/booking/shoppingexperience",
                    "_path": "/content/eurowings/en/booking/flights/flight-search/shopping/select/jcr:content/main/flightselect",
                    "_action": "QUERY_FLIGHT_DATA",
                    "_parameters": {
                        "origin": req.origin,
                        "destination": req.destination,
                        "outwardDate": req.date_from.strftime("%Y-%m-%d"),
                        "adultCount": req.adults,
                        "childCount": req.children,
                        "infantCount": req.infants,
                        "tripType": "ONE_WAY",
                        "locale": "en-GB",
                        "systemType": "SYS_1N",
                    },
                }],
            }
        }

    # ------------------------------------------------------------------
    # Playwright browser fallback
    # ------------------------------------------------------------------

    async def _playwright_fallback(
        self, req: FlightSearchRequest, t0: float,
    ) -> FlightSearchResponse:
        """Full browser automation fallback — fill form + intercept API."""
        browser = await _get_browser()
        vp = random.choice(_VIEWPORTS)
        ctx = await browser.new_context(
            viewport=vp,
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()

        captured: dict = {}

        async def on_response(response):
            url = response.url
            if any(kw in url for kw in (
                "QUERY_FLIGHT_DATA", "flight-data", "flightdata",
                "search-results", "availability", "offers",
                "api/flights", "api/search", "graphql",
            )):
                try:
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        body = await response.json()
                        captured["data"] = body
                        captured["url"] = response.url
                        logger.info("Eurowings: captured API response from %s", response.url[:120])
                except Exception:
                    pass

        page.on("response", on_response)

        try:
            logger.info("Eurowings %s→%s: Playwright fallback …", req.origin, req.destination)
            await page.goto("https://www.eurowings.com/en.html", wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(random.uniform(2.5, 4))

            await self._dismiss_cookies(page)
            await asyncio.sleep(1)
            await self._remove_overlays(page)
            await asyncio.sleep(0.5)

            await self._fill_search_form(page, req)
            start_url = page.url
            await self._click_search(page)

            deadline = time.monotonic() + 25
            while time.monotonic() < deadline:
                if captured.get("data"):
                    break
                if page.url != start_url:
                    logger.info("Eurowings: SPA navigated to %s", page.url[:120])
                    try:
                        await page.wait_for_load_state("networkidle", timeout=10_000)
                    except Exception:
                        pass
                    break
                await asyncio.sleep(0.5)

            offers: list[FlightOffer] = []
            if captured.get("data"):
                offers = self._parse_response(captured["data"], req)

            if not offers:
                offers = await self._extract_from_page(page, req)

            elapsed = time.monotonic() - t0
            return self._build_response(offers, req, elapsed) if offers else self._empty(req)
        except Exception as exc:
            logger.warning("Eurowings Playwright fallback error: %s", exc)
            return self._empty(req)
        finally:
            try:
                await ctx.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Cookie / overlay helpers
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        for sel in (
            "#onetrust-accept-btn-handler",
            'button:has-text("Accept All")',
            'button:has-text("Alle akzeptieren")',
        ):
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and await btn.is_visible(timeout=2000):
                    await btn.click(timeout=3000)
                    await asyncio.sleep(1)
                    logger.info("Eurowings: dismissed cookies via %s", sel)
                    return
            except Exception:
                continue

    async def _remove_overlays(self, page) -> None:
        """Remove overlay and login panel that block pointer events on the form."""
        await page.evaluate("""() => {
            document.querySelectorAll('.o-layer__overlay').forEach(o => o.remove());
            document.querySelectorAll('.o-layer-myewlogin, [class*="myew-login"]').forEach(l => {
                l.style.display = 'none';
            });
        }""")

    async def _close_popover(self, page) -> None:
        """Close any open popover overlay (date picker / airport selector)."""
        await page.evaluate("""() => {
            document.querySelectorAll('[class*="_popoverOverlay_"]').forEach(o => o.remove());
        }""")

    # ------------------------------------------------------------------
    # Form filling
    # ------------------------------------------------------------------

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> None:
        await self._fill_airport(page, "Departure airport", req.origin)
        await asyncio.sleep(0.8)
        await self._remove_overlays(page)
        await self._fill_airport(page, "Destination airport", req.destination)
        await asyncio.sleep(0.8)
        await self._remove_overlays(page)
        await self._fill_date(page, req.date_from)
        await asyncio.sleep(0.5)

    async def _fill_airport(self, page, label: str, code: str) -> None:
        """Click trigger button → dialog opens → type in autocomplete → select matching option."""
        logger.info("Eurowings: filling %s → %s", label, code)

        # Click the trigger button (has aria-haspopup="dialog")
        trigger = page.locator(f'button:has-text("{label}")').first
        try:
            await trigger.click(timeout=5000)
        except Exception:
            # Overlay may have reappeared — remove and retry
            await self._remove_overlays(page)
            await trigger.click(timeout=5000)
        await asyncio.sleep(1)

        # The dialog contains an autocomplete input (not readonly)
        dialog_input = page.locator(
            f'[role="dialog"] input[type="text"][placeholder="{label}"]'
        ).first
        if await dialog_input.count() == 0:
            dialog_input = page.locator(
                f'input[aria-label="{label}"]:not([readonly])'
            ).first
        if await dialog_input.count() == 0:
            # Fallback: any non-readonly text input inside a dialog
            dialog_input = page.locator(
                '[role="dialog"] input[type="text"]:not([readonly])'
            ).first

        await dialog_input.fill("")
        await asyncio.sleep(0.2)
        await dialog_input.fill(code)
        logger.info("Eurowings: typed '%s' in %s field", code, label)
        await asyncio.sleep(1.5)

        # Find and click the matching suggestion
        # Suggestions are button.list-option inside the dialog/popover
        dialog = page.locator('[role="dialog"]')
        dialog_count = await dialog.count()
        clicked = False
        for di in range(dialog_count):
            opts = dialog.nth(di).locator("button.list-option")
            opt_count = await opts.count()
            if opt_count == 0:
                continue
            # Look for option containing the IATA code
            for i in range(min(opt_count, 15)):
                text = await opts.nth(i).inner_text()
                if code.upper() in text.upper():
                    await opts.nth(i).click(timeout=3000)
                    logger.info("Eurowings: selected '%s'", text.strip()[:40])
                    clicked = True
                    break
            if clicked:
                break

        if not clicked:
            # Fallback: click first list-option anywhere
            first_opt = page.locator("button.list-option").first
            if await first_opt.count() > 0:
                await first_opt.click(timeout=3000)
                logger.info("Eurowings: selected first option (fallback)")

        await asyncio.sleep(1)
        # Verify the readonly input got the value
        val = await page.evaluate(
            f'() => document.querySelector(\'input[aria-label="{label}"]\')?.value || ""'
        )
        logger.info("Eurowings: %s = '%s'", label, val)

    async def _fill_date(self, page, target) -> None:
        """Click date trigger → type DD/MM/YY in the date input → press Enter."""
        logger.info("Eurowings: filling date %s", target)

        # Click the "Outgoing flight" trigger button
        date_trigger = page.locator('button:has-text("Outgoing flight")').first
        try:
            await date_trigger.click(timeout=5000)
        except Exception:
            await self._remove_overlays(page)
            await date_trigger.click(timeout=5000)
        await asyncio.sleep(1)

        # The date input inside the popover has placeholder "DD/MM/YY"
        date_input = page.locator('input[placeholder="DD/MM/YY"]').first
        if await date_input.count() == 0:
            date_input = page.locator('input[aria-label="Outgoing flight"]:not([readonly])').first
        if await date_input.count() == 0:
            date_input = page.locator('[data-modal-input] input[type="text"]').first

        if await date_input.count() > 0:
            date_str = target.strftime("%d/%m/%y")
            await date_input.fill("")
            await asyncio.sleep(0.2)
            await date_input.fill(date_str)
            logger.info("Eurowings: typed date '%s'", date_str)
            await asyncio.sleep(0.5)
            await date_input.press("Enter")
            await asyncio.sleep(1)
        else:
            logger.warning("Eurowings: date input not found, trying calendar click")
            await self._fill_date_calendar(page, target)

        # Close the date popover overlay
        await self._close_popover(page)

    async def _fill_date_calendar(self, page, target) -> None:
        """Fallback: navigate calendar and click the day."""
        for _ in range(6):
            try:
                header = await page.locator(
                    "[class*='calendar'] [class*='month'], [class*='datepicker'] [class*='header']"
                ).first.inner_text(timeout=2000)
                if target.strftime("%B") in header and str(target.year) in header:
                    break
            except Exception:
                pass
            try:
                next_btn = page.locator(
                    "button[aria-label*='next' i], button[aria-label*='Next' i]"
                ).first
                if await next_btn.count() > 0:
                    await next_btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
            except Exception:
                break

        day = target.day
        for sel in [
            f"[data-date='{target.isoformat()}']",
            f"button[aria-label*='{day} {target.strftime('%B')}']",
        ]:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click(timeout=3000)
                    return
            except Exception:
                continue

    async def _click_search(self, page) -> None:
        """Click the "Search for flight" submit button."""
        # Ensure no overlays block the button
        await self._remove_overlays(page)
        await self._close_popover(page)
        await asyncio.sleep(0.3)

        # Primary: the submit button with data attribute
        submit = page.locator("button[data-flight-search-submit-button]").first
        if await submit.count() > 0:
            try:
                await submit.click(timeout=5000)
                logger.info("Eurowings: clicked search (data-attr)")
                return
            except Exception:
                pass

        # Fallback: by text
        for label in ["Search for flight", "Search", "Flug suchen"]:
            try:
                btn = page.locator(f'button:has-text("{label}")').first
                if await btn.count() > 0:
                    await self._remove_overlays(page)
                    await self._close_popover(page)
                    await btn.click(timeout=5000)
                    logger.info("Eurowings: clicked search (%s)", label)
                    return
            except Exception:
                continue

        # Last resort: submit button
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    # ------------------------------------------------------------------
    # Results extraction
    # ------------------------------------------------------------------

    async def _extract_from_page(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Wait for results page and try multiple extraction methods."""
        await asyncio.sleep(5)

        url = page.url
        logger.info("Eurowings: results page URL: %s", url)

        # Method 1: Try __NUXT__ / appData
        data = await page.evaluate("""() => {
            if (window.__NUXT__) return window.__NUXT__;
            if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
            if (window.appData) return window.appData;
            const scripts = document.querySelectorAll('script[type="application/json"]');
            for (const s of scripts) {
                try {
                    const d = JSON.parse(s.textContent);
                    if (d && (d.flights || d.outbound || d.offers || d.fares)) return d;
                } catch {}
            }
            return null;
        }""")
        if data:
            offers = self._parse_response(data, req)
            if offers:
                return offers

        # Method 2: Scrape flight cards from DOM
        return await self._scrape_flight_cards(page, req)

    async def _scrape_flight_cards(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight data from rendered result cards."""
        cards_data = await page.evaluate(r"""() => {
            const results = [];
            const cards = document.querySelectorAll(
                '[class*="flight-card"], [class*="FlightCard"], [class*="result-card"], ' +
                '[class*="fare-card"], [data-flight], [class*="journey"]'
            );
            cards.forEach(card => {
                const text = card.textContent;
                const priceMatch = text.match(/(\d+[.,]\d{2})\s*€|€\s*(\d+[.,]\d{2})|EUR\s*(\d+[.,]\d{2})/);
                const timeMatches = text.match(/(\d{1,2}:\d{2})/g);
                const fnMatch = text.match(/EW\s*\d{3,4}/);
                if (priceMatch || timeMatches) {
                    results.push({
                        price: (priceMatch?.[1] || priceMatch?.[2] || priceMatch?.[3] || '').replace(',', '.'),
                        times: timeMatches || [],
                        flightNo: fnMatch?.[0] || '',
                        text: text.trim().substring(0, 200),
                    });
                }
            });
            return results;
        }""")

        offers: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)
        for card in cards_data:
            try:
                price = float(card["price"]) if card.get("price") else None
                if not price or price <= 0:
                    continue
                dep_time = card["times"][0] if len(card.get("times", [])) >= 1 else None
                arr_time = card["times"][1] if len(card.get("times", [])) >= 2 else None
                flight_no = card.get("flightNo", "").replace(" ", "")

                dep_dt = self._parse_time_on_date(dep_time, req.date_from) if dep_time else datetime(2000, 1, 1)
                arr_dt = self._parse_time_on_date(arr_time, req.date_from) if arr_time else datetime(2000, 1, 1)

                seg = FlightSegment(
                    airline="EW", airline_name="Eurowings", flight_no=flight_no,
                    origin=req.origin, destination=req.destination,
                    departure=dep_dt, arrival=arr_dt, cabin_class="M",
                )
                dur = int((arr_dt - dep_dt).total_seconds()) if dep_dt.year > 2000 and arr_dt.year > 2000 else 0
                route = FlightRoute(segments=[seg], total_duration_seconds=max(dur, 0), stopovers=0)
                fkey = f"{flight_no}_{dep_dt.isoformat()}"
                offers.append(FlightOffer(
                    id=f"ew_{hashlib.md5(fkey.encode()).hexdigest()[:12]}",
                    price=round(price, 2), currency=req.currency or "EUR",
                    price_formatted=f"{price:.2f} EUR",
                    outbound=route, inbound=None,
                    airlines=["Eurowings"], owner_airline="EW",
                    booking_url=booking_url, is_locked=False,
                    source="eurowings_direct", source_tier="free",
                ))
            except Exception:
                continue
        return offers

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the QUERY_FLIGHT_DATA response format.

        Structure: _payload._updates[0]._resultData.flights[0].schedules[0].journeys[]
        Each journey has segments[] and fares[] with farePrices[].
        """
        if not isinstance(data, dict):
            return []
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Navigate to the journeys array
        journeys = self._extract_journeys(data)
        if journeys:
            for journey in journeys:
                offer = self._parse_journey(journey, req, booking_url)
                if offer:
                    offers.append(offer)
            return offers

        # Fallback: try legacy keys
        currency = data.get("currency", req.currency or "EUR")

        flights_raw = (
            data.get("outboundFlights") or data.get("outbound")
            or data.get("flights") or data.get("offers")
            or (data.get("journeys", {}).get("outbound") if isinstance(data.get("journeys"), dict) else None)
            or []
        )
        if not isinstance(flights_raw, list):
            flights_raw = []

        for flight in flights_raw:
            offer = self._parse_single_flight(flight, currency, req, booking_url)
            if offer:
                offers.append(offer)

        if not offers:
            fares_raw = data.get("fares") or data.get("results") or []
            if isinstance(fares_raw, list):
                for fare in fares_raw:
                    offer = self._parse_single_flight(fare, currency, req, booking_url)
                    if offer:
                        offers.append(offer)

        return offers

    @staticmethod
    def _extract_journeys(data: dict) -> list:
        """Navigate the nested QUERY_FLIGHT_DATA response to find the journeys list."""
        try:
            updates = data.get("_payload", {}).get("_updates", [])
            for upd in updates:
                rd = upd.get("_resultData", {})
                flights = rd.get("flights", [])
                if flights:
                    schedules = flights[0].get("schedules", [])
                    if schedules:
                        return schedules[0].get("journeys", [])
        except (KeyError, IndexError, TypeError):
            pass
        return []

    def _parse_journey(
        self, journey: dict, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        """Parse a single journey from the QUERY_FLIGHT_DATA response."""
        dep_dt_str = journey.get("departureDateTime", "")
        arr_dt_str = journey.get("arrivalDateTime", "")
        duration_str = journey.get("duration", "")
        is_nonstop = journey.get("nonstop", False)

        # Parse segments
        segments: list[FlightSegment] = []
        for seg_raw in journey.get("segments", []):
            dep_info = seg_raw.get("departure", {})
            arr_info = seg_raw.get("arrival", {})
            op_info = seg_raw.get("operatorInformation", {})

            origin = dep_info.get("station", {}).get("tlc", req.origin)
            dest = arr_info.get("station", {}).get("tlc", req.destination)
            dep_time = dep_info.get("dateTime", dep_dt_str)
            arr_time = arr_info.get("dateTime", arr_dt_str)
            airline_code = op_info.get("airlineCode", "EW")
            flight_no = op_info.get("flightNo", "")
            airline_name = op_info.get("operatingAirlineName", "Eurowings")

            segments.append(FlightSegment(
                airline=airline_code,
                airline_name=airline_name,
                flight_no=f"{airline_code}{flight_no}" if flight_no else "",
                origin=origin,
                destination=dest,
                departure=self._parse_dt(dep_time),
                arrival=self._parse_dt(arr_time),
                cabin_class="M",
            ))

        if not segments:
            return None

        # Extract cheapest fare price
        price = self._extract_cheapest_from_fares(journey.get("fares", []))
        if price is None or price <= 0:
            return None

        # Build duration
        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        # Parse HH:MM duration string as fallback
        if total_dur <= 0 and duration_str:
            try:
                parts = duration_str.split(":")
                total_dur = int(parts[0]) * 3600 + int(parts[1]) * 60
            except (ValueError, IndexError):
                pass

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = f"{dep_dt_str}_{arr_dt_str}_{segments[0].flight_no}"
        airlines = list({s.airline_name for s in segments})

        return FlightOffer(
            id=f"ew_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency="EUR",
            price_formatted=f"{price:.2f} EUR",
            outbound=route,
            inbound=None,
            airlines=airlines,
            owner_airline="EW",
            booking_url=booking_url,
            is_locked=False,
            source="eurowings_direct",
            source_tier="free",
        )

    @staticmethod
    def _extract_cheapest_from_fares(fares: list) -> Optional[float]:
        """Find the cheapest fare price from QUERY_FLIGHT_DATA fare structures."""
        prices: list[float] = []
        for fare in fares:
            for fp in fare.get("farePrices", []):
                # Try price.value first (may be promo), then originalPrice.value
                for price_key in ("price", "originalPrice", "totalPrice"):
                    p_obj = fp.get(price_key, {})
                    if isinstance(p_obj, dict) and "value" in p_obj:
                        try:
                            val = float(p_obj["value"])
                            if val > 0:
                                prices.append(val)
                                break
                        except (TypeError, ValueError):
                            continue
        return min(prices) if prices else None

    def _parse_single_flight(
        self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        price = (
            flight.get("price") or flight.get("totalPrice") or flight.get("lowestPrice")
            or flight.get("farePrice") or self._extract_cheapest_fare(flight)
        )
        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination))
        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments, total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("flightKey") or flight.get("id") or flight.get("flightId")
            or flight.get("flightNumber", "") + "_" + segments[0].departure.isoformat()
        )
        return FlightOffer(
            id=f"ew_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=["Eurowings"], owner_airline="EW",
            booking_url=booking_url, is_locked=False,
            source="eurowings_direct", source_tier="free",
        )

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = seg.get("departure") or seg.get("departureDate") or seg.get("departureTime") or seg.get("std") or ""
        arr_str = seg.get("arrival") or seg.get("arrivalDate") or seg.get("arrivalTime") or seg.get("sta") or ""
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("flightNo") or seg.get("number") or "").replace(" ", "")
        origin = seg.get("origin") or seg.get("departureAirport") or seg.get("departureStation") or seg.get("departureCode") or default_origin
        destination = seg.get("destination") or seg.get("arrivalAirport") or seg.get("arrivalStation") or seg.get("arrivalCode") or default_dest
        return FlightSegment(
            airline="EW", airline_name="Eurowings", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    @staticmethod
    def _extract_cheapest_fare(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareBundles") or flight.get("fareOptions") or []
        prices: list[float] = []
        for f in fares:
            p = f.get("price") or f.get("amount") or f.get("totalPrice") or f.get("lowestPrice")
            if p is not None:
                try:
                    prices.append(float(p))
                except (TypeError, ValueError):
                    continue
        return min(prices) if prices else None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Eurowings %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        search_hash = hashlib.md5(f"eurowings{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[: len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _parse_time_on_date(time_str: str, date) -> datetime:
        """Parse HH:MM time string and combine with search date."""
        try:
            h, m = time_str.split(":")
            return datetime(date.year, date.month, date.day, int(h), int(m))
        except Exception:
            return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.eurowings.com/en/booking/flights/search.html"
            f"?origin={req.origin}&destination={req.destination}"
            f"&outboundDate={dep}&adultCount={req.adults}"
            f"&childCount={req.children}&infantCount={req.infants}"
            f"&isOneWay=true"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"eurowings{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=req.currency or "EUR", offers=[], total_results=0,
        )
