"""
Batik Air scraper — nodriver CF bypass + Playwright DOM extraction.

Batik Air (IATA: ID) is an Indonesian full-service carrier (Lion Air Group).
Website: www.batikair.com.my (Malaysia hub).

Strategy (nodriver + Playwright hybrid, discovered Mar 2026):
1. nodriver (headed, off-screen) launches Chrome → auto-bypasses Cloudflare Turnstile (~6s)
2. Playwright connects to the same Chrome via CDP for reliable DOM interaction
3. Fill Ant Design combobox airports (type IATA → JS click [title] option)
4. Pick date from Ant Design calendar table
5. Click search → navigates to /book/flight-search
6. Extract flight cards from results DOM

CF Turnstile note: Only nodriver headed (headless=False) passes Turnstile.
Playwright persistent context, CDP headless, curl_cffi all fail.

API note: search.batikair.com.my/flightrr_api/api/get/Flights uses
encrypted payloads — not replayable. DOM extraction is the reliable approach.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
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

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]

# ── nodriver + Playwright hybrid browser ───────────────────────────────
_nd_browser = None          # nodriver browser instance (owns the Chrome process)
_pw_instance = None         # Playwright async API instance
_pw_browser = None          # Playwright CDP browser (connected to nodriver's Chrome)
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _ensure_browser():
    """Launch nodriver Chrome, bypass CF Turnstile, connect Playwright via CDP.

    Returns a Playwright BrowserContext connected to the nodriver Chrome.
    Reuses existing browser if still alive.
    """
    global _nd_browser, _pw_instance, _pw_browser
    lock = _get_lock()
    async with lock:
        # Check if existing connection is still alive
        if _pw_browser:
            try:
                if _pw_browser.is_connected():
                    return _pw_browser.contexts[0]
            except Exception:
                pass
            _pw_browser = None

        # Clean up old Playwright instance
        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass
            _pw_instance = None

        # Clean up old nodriver browser
        if _nd_browser:
            try:
                _nd_browser.stop()
            except Exception:
                pass
            _nd_browser = None

        import nodriver as uc

        # Launch nodriver headed (MUST be headless=False for CF Turnstile bypass)
        _nd_browser = await uc.start(
            headless=False,
            browser_args=[
                "--window-size=1366,768",
                "--window-position=-2400,-2400",
            ],
        )

        # Navigate to homepage and wait for CF Turnstile to pass
        page = await _nd_browser.get("https://www.batikair.com.my/")
        for i in range(25):
            await asyncio.sleep(1)
            title = await page.evaluate("document.title")
            if "Just a moment" not in str(title):
                logger.info("BatikAir: Cloudflare passed in %ds", i + 1)
                break
        else:
            logger.warning("BatikAir: Cloudflare Turnstile did not pass after 25s")
            _nd_browser.stop()
            _nd_browser = None
            return None

        # Let React SPA hydrate after CF pass
        await asyncio.sleep(3)

        # Connect Playwright to nodriver's Chrome via CDP
        from playwright.async_api import async_playwright

        _pw_instance = await async_playwright().start()
        port = _nd_browser.config.port
        host = _nd_browser.config.host or "127.0.0.1"
        _pw_browser = await _pw_instance.chromium.connect_over_cdp(
            f"http://{host}:{port}"
        )
        logger.info("BatikAir: Playwright connected via CDP to %s:%s", host, port)

        ctx = _pw_browser.contexts[0]
        return ctx


class BatikAirConnectorClient:
    """Batik Air scraper — Playwright form-fill + DOM card extraction."""

    def __init__(self, timeout: float = 50.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        context = await _ensure_browser()
        if context is None:
            logger.warning("BatikAir: browser launch / CF bypass failed")
            return self._empty(req)

        page = None
        try:
            page = await context.new_page()

            # --- API response interception (in case response is unencrypted) ---
            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    if response.status == 200 and (
                        "flightrr_api/api/get/flights" in url
                        or "availability" in url
                        or "/api/flights" in url
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, (dict, list)):
                                captured_data["json"] = data
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("BatikAir: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto("https://www.batikair.com.my/",
                            wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2.0)

            # CF Turnstile already passed by nodriver during _ensure_browser(),
            # but new pages may still hit a brief challenge. Wait if so.
            for _ in range(15):
                title = await page.title()
                if "just a moment" not in title.lower():
                    break
                await asyncio.sleep(1)

            # Wait for the search form to render (origin + destination selects)
            # .ant-select.w-full identifies the airport selects (language select has no w-full)
            form_ok = False
            for _ in range(20):
                count = await page.locator(".ant-select.w-full").count()
                if count >= 2:
                    form_ok = True
                    break
                await asyncio.sleep(0.5)
            if not form_ok:
                logger.warning("BatikAir: search form did not render (ant-selects: %d)", count)

            # Remove overlay popups
            await self._remove_overlays(page)
            await asyncio.sleep(0.3)

            # One-way
            await self._set_one_way(page)
            await asyncio.sleep(0.3)
            await self._remove_overlays(page)

            # Origin
            await self._remove_overlays(page)
            ok = await self._fill_airport(page, req.origin, is_origin=True)
            if not ok:
                logger.warning("BatikAir: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Destination
            await self._remove_overlays(page)
            ok = await self._fill_airport(page, req.destination, is_origin=False)
            if not ok:
                logger.warning("BatikAir: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("BatikAir: date fill failed")
                return self._empty(req)
            await asyncio.sleep(0.3)
            await self._remove_overlays(page)

            # Search
            await self._click_search(page)
            # Wait for results page
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await page.wait_for_url("**/book/flight-search**", timeout=remaining * 1000)
            except Exception:
                pass
            await asyncio.sleep(2)

            # Try API capture first
            try:
                await asyncio.wait_for(api_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass

            if captured_data.get("json"):
                offers = self._parse_api_response(captured_data["json"], req)
                if offers:
                    return self._build_response(offers, req, time.monotonic() - t0)

            # Fallback: DOM extraction from results page
            offers = await self._extract_from_dom(page, req)
            return self._build_response(offers, req, time.monotonic() - t0)

        except Exception as e:
            logger.error("BatikAir error: %s", e)
            # If browser disconnected, invalidate it so next call re-launches
            global _pw_browser
            if _pw_browser and not _pw_browser.is_connected():
                _pw_browser = None
            return self._empty(req)
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

    # ── Overlay removal ──

    async def _remove_overlays(self, page) -> None:
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '#__st_overlay_popup, [smtmsgid], .ant-modal-wrap, .ant-modal-root, '
                    + '.ant-modal-mask, [class*="cookie"], [id*="cookie"], '
                    + '[class*="consent"], [id*="consent"], [class*="onetrust"]'
                ).forEach(el => el.remove());
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ── One-way toggle ──

    async def _set_one_way(self, page) -> None:
        """Click One-way radio via JS to bypass overlay interception."""
        clicked = await page.evaluate("""() => {
            const labels = document.querySelectorAll('.ant-radio-wrapper');
            for (const l of labels) {
                if (/one.?way/i.test(l.textContent)) {
                    l.click();
                    return l.textContent.trim();
                }
            }
            return null;
        }""")
        if clicked:
            logger.debug("BatikAir: one-way selected via JS: %s", clicked)
        else:
            logger.debug("BatikAir: one-way radio not found")

    # ── Airport fill (Ant Design Select combobox) ──

    async def _fill_airport(self, page, iata: str, is_origin: bool) -> bool:
        """Fill Ant Design Select combobox — force-click, keyboard type, JS-click option.

        Uses context-aware identification (aria-labels, placeholder, parent containers)
        to locate the correct origin/destination Ant Design Select, then types the IATA
        code and picks the matching dropdown option via ``.ant-select-item-option[title]``.
        """
        try:
            # Strategy 1: Find Ant Design select inputs via class selector
            cb_input = await self._find_ant_select_input(page, is_origin)

            if not cb_input:
                logger.debug("BatikAir: no Ant Design select found for %s",
                             "origin" if is_origin else "destination")
                return False

            # Force-click to open dropdown (bypasses Ant Design overlay interception)
            await cb_input.click(force=True, timeout=5000)
            await asyncio.sleep(0.5)

            # Clear existing text with keyboard shortcuts
            await page.keyboard.press("Control+a")
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.3)

            # Type IATA character by character (triggers React keyboard events)
            await cb_input.type(iata, delay=100)
            await asyncio.sleep(1.5)

            # JS-click the dropdown option using Ant Design v4/v5 selectors
            clicked = await page.evaluate("""(iata) => {
                // Ant Design v4/v5: .ant-select-item-option with title attribute
                const antOpts = document.querySelectorAll(
                    '.ant-select-item-option[title], .ant-select-item[title]'
                );
                for (const opt of antOpts) {
                    const t = opt.getAttribute('title') || '';
                    if (t.includes('(' + iata + ')') || t.includes(iata)) {
                        opt.click();
                        return t;
                    }
                }
                // Broader title-based fallback
                const titled = document.querySelectorAll('[title]');
                for (const opt of titled) {
                    const t = opt.title || '';
                    if (t.includes('(' + iata + ')')) {
                        opt.click();
                        return t;
                    }
                }
                // Try role="option" elements (Ant Design accessible markup)
                const options = document.querySelectorAll('[role="option"]');
                for (const opt of options) {
                    if (opt.textContent.includes(iata)) {
                        opt.click();
                        return opt.textContent.substring(0, 80);
                    }
                }
                return null;
            }""", iata)

            if clicked:
                logger.debug("BatikAir: selected airport %s → %s", iata, clicked)
                return True

            # Last resort: Enter key to accept first suggestion
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.3)
            logger.debug("BatikAir: airport %s — pressed Enter fallback", iata)
            return True

        except Exception as e:
            logger.debug("BatikAir: airport fill error for %s: %s", iata, e)
            return False

    async def _find_ant_select_input(self, page, is_origin: bool):
        """Locate the correct Ant Design Select input for origin or destination.

        Batik Air uses Ant Design Selects with class ``w-full`` for airport fields.
        The language selector (small, borderless, no ``w-full``) is at combobox
        index 0. Origin is index 1, destination is index 2.
        """
        label = "origin" if is_origin else "destination"

        # Strategy 1: Use .ant-select.w-full to target airport selects (skips language)
        airport_selects = page.locator(".ant-select.w-full")
        as_count = await airport_selects.count()
        if as_count >= 2:
            target = 0 if is_origin else 1
            inp = airport_selects.nth(target).locator("input")
            if await inp.count() > 0:
                logger.debug("BatikAir: found ant-select.w-full[%d] input for %s", target, label)
                return inp.first

        # Strategy 2: Positional indexing of all combobox inputs
        # Index 0 = language, 1 = origin, 2 = destination
        all_combos = page.locator("[role='combobox']")
        count = await all_combos.count()
        if count >= 3:
            target_idx = 1 if is_origin else 2
        elif count >= 2:
            target_idx = 0 if is_origin else 1
        elif count == 1 and is_origin:
            target_idx = 0
        else:
            return None

        logger.debug("BatikAir: using combobox[%d/%d] for %s", target_idx, count, label)
        return all_combos.nth(target_idx)

    # ── Date picker (Ant Design Calendar) ──

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from
        try:
            await asyncio.sleep(1.0)
            await self._remove_overlays(page)

            # Click the .ant-picker container to open the calendar
            clicked_cal = await page.evaluate("""() => {
                const pickers = document.querySelectorAll('.ant-picker');
                for (const pk of pickers) {
                    const inp = pk.querySelector('input');
                    if (inp) { pk.click(); return true; }
                }
                return false;
            }""")
            if not clicked_cal:
                # Fallback: click readonly input with force
                f = page.locator(".ant-picker input").first
                if await f.count() > 0:
                    await f.click(force=True, timeout=5000)
                else:
                    return False

            await asyncio.sleep(1.0)

            # Navigate to target month
            target_month = target.strftime("%b")  # "Mar", "Apr" etc.
            target_year = str(target.year)

            for _ in range(12):
                header_text = await page.evaluate("""() => {
                    const hv = document.querySelector('.ant-picker-header-view');
                    return hv ? hv.innerText : '';
                }""")

                if target_month in header_text and target_year in header_text:
                    break

                next_clicked = await page.evaluate("""() => {
                    const btn = document.querySelector(
                        '.ant-picker-header-next-btn, button[aria-label*="Next month"]'
                    );
                    if (btn) { btn.click(); return true; }
                    return false;
                }""")

                if not next_clicked:
                    break
                await asyncio.sleep(0.5)

            # Click the target day cell
            day = str(target.day)

            clicked = await page.evaluate("""(day) => {
                // Use in-view cells (current month only)
                const cells = document.querySelectorAll('td.ant-picker-cell-in-view');
                for (const cell of cells) {
                    if (cell.classList.contains('ant-picker-cell-disabled')) continue;
                    const inner = cell.querySelector('.ant-picker-cell-inner');
                    if (inner && inner.textContent.trim() === day) {
                        cell.click();
                        return true;
                    }
                }
                // Broader fallback
                const allCells = document.querySelectorAll('td[class*="picker-cell"]');
                for (const cell of allCells) {
                    if (cell.classList.contains('ant-picker-cell-disabled')) continue;
                    const inner = cell.querySelector('[class*="inner"]');
                    if (!inner) continue;
                    const text = inner.textContent.trim();
                    if (text === day || text.startsWith(day + ' ') || text.startsWith(day + '\\n')) {
                        cell.click();
                        return true;
                    }
                }
                return false;
            }""", day)

            if clicked:
                logger.debug("BatikAir: selected date day %s", day)
                return True

            return False

        except Exception as e:
            logger.warning("BatikAir: date error: %s", e)
            return False

    # ── Search button ──

    async def _click_search(self, page) -> None:
        await self._remove_overlays(page)
        # Use JS click to bypass any overlay interception
        clicked = await page.evaluate("""() => {
            const btn = document.querySelector('#search_btn');
            if (btn) { btn.click(); return 'search_btn'; }
            // Fallback: button with search text
            const btns = document.querySelectorAll('button');
            for (const b of btns) {
                const text = (b.innerText || '').trim().toLowerCase();
                if (text === 'search' || text === 'search flights') {
                    b.click(); return text;
                }
            }
            return null;
        }""")
        if clicked:
            logger.debug("BatikAir: search clicked via JS: %s", clicked)
            return
        # Playwright force-click fallback
        try:
            btn = page.locator("#search_btn")
            if await btn.count() > 0:
                await btn.click(force=True, timeout=5000)
                return
        except Exception:
            pass

    # ── DOM extraction (results page) ──

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight cards from /book/flight-search results page.

        Batik Air results page uses Ant Design cards with class ``hover:scale-102``
        containing airline, times, duration, stops and prices in MYR.
        """
        try:
            await asyncio.sleep(2)

            # Wait for flight cards to appear
            try:
                await page.wait_for_selector(".shadow-2", timeout=10000)
            except Exception:
                pass

            flights = await page.evaluate("""(params) => {
                const body = document.body.innerText || '';
                const results = [];

                // Match flight blocks: "Batik Air, MY(OD306)" pattern
                const pattern = /([\\w\\s,]+?)\\(([A-Z]{2}\\d{1,5})\\)/g;
                let match;
                const matches = [];
                while ((match = pattern.exec(body)) !== null) {
                    // Skip non-flight matches
                    const code = match[2];
                    if (!/^[A-Z]{2}\\d{2,5}$/.test(code)) continue;
                    matches.push({
                        airline: match[1].replace(/,\\s*MY$/i, '').trim(),
                        flightNo: code,
                        carrier: code.substring(0, 2),
                        index: match.index,
                    });
                }

                for (let i = 0; i < matches.length; i++) {
                    const m = matches[i];
                    const nextIdx = (i + 1 < matches.length) ? matches[i + 1].index : body.length;
                    const block = body.substring(m.index, Math.min(nextIdx, m.index + 600));

                    // Times: HH:MM
                    const times = block.match(/(\\d{2}:\\d{2})/g) || [];
                    const depTime = times[0] || '';
                    const arrTime = times[1] || '';

                    // Duration: "3HR" or "3HR 10M"
                    const durMatch = block.match(/(\\d+)\\s*HR\\s*(?:(\\d+)\\s*M)?/i);
                    let durSec = 0;
                    if (durMatch) {
                        durSec = parseInt(durMatch[1]) * 3600;
                        if (durMatch[2]) durSec += parseInt(durMatch[2]) * 60;
                    }

                    // Stops
                    const isNonstop = /NON[\\s-]*STOP/i.test(block);
                    const stopMatch = block.match(/(\\d+)\\s*STOP/i);
                    const stops = isNonstop ? 0 : (stopMatch ? parseInt(stopMatch[1]) : 0);

                    // Price: "RM 449.00" or "RM 1,449.00"
                    const econIdx = block.search(/ECONOMY/i);
                    let priceBlock = econIdx >= 0 ? block.substring(econIdx) : block;
                    const priceMatch = priceBlock.match(/RM\\s*([\\d,]+\\.\\d{2})/);
                    let price = 0;
                    if (priceMatch) {
                        price = parseFloat(priceMatch[1].replace(/,/g, ''));
                    }
                    // Fallback: first RM price after the flight number
                    if (price <= 0) {
                        const fallback = block.match(/RM\\s*([\\d,]+\\.\\d{2})/);
                        if (fallback) price = parseFloat(fallback[1].replace(/,/g, ''));
                    }

                    if (price <= 0) continue;

                    results.push({
                        flightNo: m.flightNo,
                        carrier: m.carrier,
                        airline: m.airline || 'Batik Air',
                        depTime, arrTime, durationSec: durSec,
                        stops, price,
                        origin: params.origin,
                        destination: params.destination,
                    });
                }

                return results;
            }""", {"origin": req.origin, "destination": req.destination})

            if not flights:
                return []

            booking_url = self._build_booking_url(req)
            offers: list[FlightOffer] = []
            date_str = req.date_from.strftime("%Y-%m-%d")

            for f in flights:
                dep_dt = self._parse_time(date_str, f.get("depTime", ""))
                arr_dt = self._parse_time(date_str, f.get("arrTime", ""))
                dur = f.get("durationSec", 0)
                if not dur and dep_dt and arr_dt:
                    delta = arr_dt - dep_dt
                    dur = max(int(delta.total_seconds()), 0)
                    if dur < 0:
                        dur += 86400  # crosses midnight

                seg = FlightSegment(
                    airline=f.get("carrier", "ID"),
                    airline_name=f.get("airline", "Batik Air"),
                    flight_no=f.get("flightNo", ""),
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_dt or datetime(2000, 1, 1),
                    arrival=arr_dt or datetime(2000, 1, 1),
                    duration_seconds=dur,
                    cabin_class="economy",
                    aircraft="",
                )

                route = FlightRoute(
                    segments=[seg],
                    total_duration_seconds=dur,
                    stopovers=f.get("stops", 0),
                )

                price = f.get("price", 0)
                fid = hashlib.md5(
                    f"id_{req.origin}{req.destination}{f.get('flightNo','')}{price}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"id_{fid}",
                    price=float(price),
                    currency="MYR",
                    price_formatted=f"RM {price:,.2f}",
                    outbound=route,
                    inbound=None,
                    airlines=[f.get("airline", "Batik Air")],
                    owner_airline=f.get("carrier", "OD"),
                    booking_url=booking_url,
                    is_locked=False,
                    source="batikair_direct",
                    source_tier="free",
                ))

            return offers

        except Exception as e:
            logger.warning("BatikAir DOM extraction error: %s", e)
            return []

    # ── API response parser (if interception works) ──

    def _parse_api_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if not isinstance(data, (dict, list)):
            return []
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Inspect common JSON shapes
        flights_raw = []
        if isinstance(data, dict):
            for key in ("flights", "outboundFlights", "data", "journeys", "availability"):
                v = data.get(key)
                if isinstance(v, list):
                    flights_raw = v
                    break
                if isinstance(v, dict):
                    for k2 in ("flights", "journeys", "outbound"):
                        v2 = v.get(k2)
                        if isinstance(v2, list):
                            flights_raw = v2
                            break
                    if flights_raw:
                        break
        elif isinstance(data, list):
            flights_raw = data

        for flight in flights_raw:
            if not isinstance(flight, dict):
                continue
            price = None
            for pk in ("price", "totalPrice", "lowestFare", "farePrice", "amount"):
                pv = flight.get(pk)
                if pv is not None:
                    try:
                        price = float(pv if not isinstance(pv, dict) else pv.get("amount", 0))
                        break
                    except (TypeError, ValueError):
                        pass
            if not price or price <= 0:
                continue

            fno = str(flight.get("flightNumber", "") or flight.get("flightNo", ""))
            carrier = flight.get("carrierCode", "") or flight.get("carrier", "") or "ID"

            dep_str = flight.get("departureDateTime", "") or flight.get("departure", "")
            arr_str = flight.get("arrivalDateTime", "") or flight.get("arrival", "")
            dep_dt = self._parse_dt(dep_str)
            arr_dt = self._parse_dt(arr_str)
            dur = max(int((arr_dt - dep_dt).total_seconds()), 0) if dep_dt.year > 2000 and arr_dt.year > 2000 else 0

            seg = FlightSegment(
                airline=carrier, airline_name="Batik Air", flight_no=fno,
                origin=flight.get("origin", req.origin),
                destination=flight.get("destination", req.destination),
                departure=dep_dt, arrival=arr_dt, duration_seconds=dur,
                cabin_class="economy", aircraft="",
            )

            route = FlightRoute(segments=[seg], total_duration_seconds=dur, stopovers=0)
            fid = hashlib.md5(f"id_{req.origin}{req.destination}{fno}{price}".encode()).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"id_{fid}", price=round(price, 2), currency="MYR",
                price_formatted=f"RM {price:,.2f}", outbound=route, inbound=None,
                airlines=["Batik Air"], owner_airline=carrier,
                booking_url=booking_url, is_locked=False,
                source="batikair_direct", source_tier="free",
            ))

        return offers

    # ── Helpers ──

    @staticmethod
    def _parse_time(date_str: str, time_str: str) -> Optional[datetime]:
        if not time_str or not date_str:
            return None
        try:
            return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            return None

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("BatikAir %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"batikair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="MYR", offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return f"https://www.batikair.com.my/book/flight-search"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"batikair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="MYR", offers=[], total_results=0,
        )
