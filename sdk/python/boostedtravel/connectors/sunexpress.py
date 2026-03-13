"""
SunExpress CDP Chrome scraper — direct URL navigation + API interception + DOM extraction.

SunExpress (IATA: XQ) is a Turkish-German low-cost carrier, a joint venture
of Turkish Airlines and Lufthansa, operating from Turkey and Germany to
leisure destinations across Europe, Middle East and North Africa.

Strategy (updated Mar 2026):
1. Launch real system Chrome via CDP (persistent, avoids launch overhead)
2. Navigate directly to sunexpress.com/en-gb/booking/select/?... deep link
   (bypasses homepage form fill — selectors break with site redesigns)
3. Dismiss OneTrust cookie banner ("Accept All Cookies" button)
4. Intercept API responses from Angular SPA for structured flight data
5. Fall back to DOM text extraction if API interception finds nothing
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import subprocess
import time
from datetime import datetime
from typing import Any, Optional

from boostedtravel.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from boostedtravel.connectors.browser import stealth_args, stealth_popen_kwargs

logger = logging.getLogger(__name__)

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-IE", "en-AU"]
_TIMEZONES = [
    "Europe/Istanbul", "Europe/Berlin", "Europe/London",
    "Europe/Paris", "Europe/Vienna", "Europe/Zurich",
]

_TURKEY_AIRPORTS = {
    "IST", "SAW", "AYT", "ESB", "ADB", "DLM", "BJV", "TZX", "GZT",
    "VAN", "ERZ", "DIY", "SZF", "KYA", "MLX", "ASR", "EZS", "MQM",
    "HTY", "NAV", "KCM", "EDO", "ONQ", "BAL", "CKZ", "MSR", "IGD",
    "NKT", "GNY", "USQ", "DNZ", "ERC", "AOE", "KSY", "ISE", "YEI",
    "TEQ", "OGU", "BZI", "SFQ",
}

# Month abbreviations used by the SunExpress calendar cells
_MONTH_ABBRS = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

# ── CDP Chrome singleton ──────────────────────────────────────────────
_CDP_PORT = 9453
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", "/tmp"), "sunexpress_cdp_data")
_CHROME_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]

_chrome_proc: subprocess.Popen | None = None
_pw_instance = None
_cdp_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _find_chrome() -> str:
    for p in _CHROME_PATHS:
        if os.path.isfile(p):
            return p
    raise FileNotFoundError("Chrome not found")


def _launch_chrome():
    global _chrome_proc
    if _chrome_proc and _chrome_proc.poll() is None:
        return
    os.makedirs(_USER_DATA_DIR, exist_ok=True)
    chrome = _find_chrome()
    _chrome_proc = subprocess.Popen(
        [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--disable-blink-features=AutomationControlled",
            "--no-first-run", "--no-default-browser-check",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            *stealth_args(),
        ],
        **stealth_popen_kwargs(),
    )
    logger.info("SunExpress: Chrome launched on CDP port %d (pid=%d)", _CDP_PORT, _chrome_proc.pid)


async def _get_browser():
    """Shared real Chrome via CDP (launched once, reused across searches)."""
    global _pw_instance, _cdp_browser
    lock = _get_lock()
    async with lock:
        if _cdp_browser and _cdp_browser.is_connected():
            return _cdp_browser
        _launch_chrome()
        await asyncio.sleep(2)
        from playwright.async_api import async_playwright
        if not _pw_instance:
            _pw_instance = await async_playwright().start()
        for attempt in range(5):
            try:
                _cdp_browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{_CDP_PORT}"
                )
                logger.info("SunExpress: connected to Chrome via CDP")
                return _cdp_browser
            except Exception:
                if attempt < 4:
                    await asyncio.sleep(1)
        raise RuntimeError(f"SunExpress: cannot connect to Chrome CDP on port {_CDP_PORT}")


class SunExpressConnectorClient:
    """SunExpress Playwright scraper — homepage form fill + DOM extraction."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )

        try:
            page = await context.new_page()

            # ── Step 1: Intercept API responses from Angular SPA ────────
            api_data: list[dict] = []

            async def _on_response(response):
                try:
                    url = response.url
                    if response.status != 200:
                        return
                    # Capture JSON responses from flight search/availability APIs
                    if any(kw in url for kw in (
                        "/api/", "/availability", "/flight",
                        "/offer", "/search", "/booking",
                    )):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            body = await response.json()
                            api_data.append({"url": url, "data": body})
                except Exception:
                    pass

            page.on("response", _on_response)

            # ── Step 2: Navigate directly to booking results URL ────────
            # Deep link bypasses homepage form fill (selectors break with
            # site redesigns). The Angular SPA router handles the URL and
            # fires its own API calls to fetch flight data.
            booking_url = self._build_booking_url(req)
            logger.info("SunExpress: navigating to %s→%s via %s",
                        req.origin, req.destination, booking_url)
            await page.goto(
                booking_url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(2.5)

            # ── Step 3: Dismiss cookie banner ──────────────────────────
            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)

            # ── Step 4: Wait for prices to load ────────────────────────
            remaining = max(self.timeout - (time.monotonic() - t0), 8)
            prices_loaded = False
            for _ in range(int(remaining)):
                await asyncio.sleep(1)
                has_prices = await page.evaluate("""() => {
                    const els = document.querySelectorAll(
                        '[class*="price"], [class*="fare"], [class*="amount"]'
                    );
                    return Array.from(els).some(e => /[1-9]/.test(e.textContent));
                }""")
                if has_prices:
                    prices_loaded = True
                    break

            if not prices_loaded:
                logger.warning("SunExpress: prices never loaded on results page")
                return self._empty(req)

            await asyncio.sleep(1.5)

            # ── Step 5: Try structured API data first ──────────────────
            if api_data:
                offers = self._parse_api_data(api_data, req)
                if offers:
                    elapsed = time.monotonic() - t0
                    logger.info("SunExpress: parsed %d offers from API interception", len(offers))
                    return self._build_response(offers, req, elapsed)

            # ── Step 6: Fall back to DOM extraction ────────────────────
            offers = await self._extract_from_dom(page, req)
            elapsed = time.monotonic() - t0
            if offers:
                return self._build_response(offers, req, elapsed)
            return self._empty(req)

        except Exception as e:
            logger.error("SunExpress Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ── Cookie dismissal ───────────────────────────────────────────────

    async def _dismiss_cookies(self, page) -> None:
        try:
            btn = page.get_by_role(
                "button", name=re.compile(r"Accept All Cookies", re.IGNORECASE)
            )
            if await btn.count() > 0:
                await btn.first.click(timeout=3000)
                await asyncio.sleep(0.5)
                return
        except Exception:
            pass
        # JS fallback: remove OneTrust / cookie overlays
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="onetrust"], [id*="onetrust"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ── Form filling ───────────────────────────────────────────────────

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        # 1. Set one-way
        await self._set_one_way(page)
        await asyncio.sleep(0.3)

        # 2. Fill origin — click "From" button → combobox → option
        ok = await self._fill_airport(page, "From", req.origin)
        if not ok:
            logger.warning("SunExpress: origin fill failed for %s", req.origin)
            return False

        # Close any overlay (origin picker may stay open, blocking To button)
        await page.keyboard.press("Escape")
        await asyncio.sleep(1)

        # 3. Fill destination
        ok = await self._fill_airport(page, "To", req.destination)
        if not ok:
            logger.warning("SunExpress: destination fill failed for %s", req.destination)
            return False
        await asyncio.sleep(0.8)

        # 4. Fill date
        ok = await self._fill_date(page, req)
        if not ok:
            logger.warning("SunExpress: date fill failed")
            return False
        return True

    async def _fill_airport(self, page, label: str, iata: str) -> bool:
        """Click the airport button, type IATA in the combobox, click the option.

        SunExpress has two picker styles:
          - Origin: single-panel, airports shown directly as role="option"
          - Destination: dual-panel, countries on left (role="option") +
            airport buttons on right (button.station-control-list_item_link)
        """
        try:
            # Always click the specific button to ensure the right picker is open
            btn = page.locator(f'button.control_field_button:has-text("{label}")')
            if await btn.count() > 0:
                await btn.first.click(timeout=5000)
                logger.info("SunExpress: clicked %s button", label)
            else:
                btn2 = page.locator(f'button:has-text("{label}")').first
                if await btn2.count() > 0:
                    await btn2.click(timeout=5000)
                    logger.info("SunExpress: clicked %s button (fallback)", label)
                else:
                    logger.warning("SunExpress: no %s button found", label)
                    return False
            await asyncio.sleep(1)

            # Fill the combobox with the IATA code
            cb = page.get_by_role("combobox").first
            try:
                await cb.wait_for(state="visible", timeout=3000)
            except Exception:
                logger.warning("SunExpress: combobox not visible for %s/%s", label, iata)
                return False

            await cb.fill(iata)
            await asyncio.sleep(2)

            # Strategy 1: Airport button in the station-control-list (destination style)
            # These are <button class="station-control-list_item_link"> with IATA text
            airport_btn = page.locator(
                'button.station-control-list_item_link'
            ).filter(has_text=re.compile(rf"\b{re.escape(iata)}\b", re.IGNORECASE)).first
            if await airport_btn.count() > 0:
                await airport_btn.click(timeout=3000)
                logger.info("SunExpress: selected %s for %s (airport button)", iata, label)
                return True

            # Strategy 2: role="option" with exact IATA match (origin style)
            opt = page.get_by_role("option", name=re.compile(
                rf"\b{re.escape(iata)}\b", re.IGNORECASE
            )).first
            if await opt.count() > 0:
                await opt.click(timeout=3000)
                logger.info("SunExpress: selected %s for %s (option)", iata, label)
                return True

            # Strategy 3: any listitem with IATA text
            li = page.locator('li.station-control-list_item').filter(
                has_text=re.compile(rf"\b{re.escape(iata)}\b", re.IGNORECASE)
            ).first
            if await li.count() > 0:
                await li.click(timeout=3000)
                logger.info("SunExpress: selected %s for %s (listitem)", iata, label)
                return True

            logger.warning("SunExpress: no option found for %s/%s", label, iata)
        except Exception as e:
            logger.warning("SunExpress: airport fill error for %s/%s: %s", label, iata, e)
        return False

    async def _set_one_way(self, page) -> None:
        try:
            ow = page.get_by_text("One-way", exact=False).first
            if await ow.count() > 0:
                await ow.click(timeout=3000)
                return
        except Exception:
            pass

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from
        target_month_abbr = _MONTH_ABBRS[target.month]
        target_month_full = target.strftime("%b %Y")  # "Apr 2026"
        target_day = target.day

        try:
            # Open the date picker
            date_field = page.locator(
                '.ibe-search_date-control, .date-control, input-date-picker-custom'
            ).first
            if await date_field.count() > 0:
                await date_field.click(timeout=3000)
                await asyncio.sleep(0.8)

            # Navigate calendar until target month is visible (max 12 clicks)
            for _ in range(12):
                visible = await page.evaluate("""(targetMonth) => {
                    const headings = document.querySelectorAll(
                        '[class*="cal"] [class*="heading"], [class*="cal"] [class*="title"], ' +
                        '[class*="month-name"], [class*="cal"] h3, [class*="cal"] h4'
                    );
                    for (const h of headings) {
                        if (h.offsetHeight > 0 && h.textContent.includes(targetMonth)) return true;
                    }
                    // Also check the full calendar text for "Apr 2026" style headings
                    const calText = document.querySelector('[class*="calendar"]')?.textContent || '';
                    return calText.includes(targetMonth);
                }""", target_month_full)

                if visible:
                    break

                # Click next arrow
                next_btn = page.locator(
                    '[class*="next"]:visible, [aria-label*="next" i]:visible'
                ).first
                if await next_btn.count() > 0:
                    await next_btn.click(timeout=2000)
                    await asyncio.sleep(0.4)
                else:
                    break

            # Click the exact day cell in the correct month via JS
            clicked = await page.evaluate("""(args) => {
                const [monthAbbr, day] = args;
                // SunExpress calendar cells have text like "Apr  15" with month prefix
                const cells = document.querySelectorAll(
                    '[role="gridcell"], [class*="cal__day"], td[class*="day"]'
                );
                // First pass: find cells whose text content matches "MonthAbbr <space> day"
                for (const cell of cells) {
                    if (cell.offsetHeight === 0) continue;
                    const text = cell.textContent.trim();
                    // Match patterns like "Apr  15", "Apr 15", or just checking
                    // if the cell belongs to the right month section
                    if (text.includes(monthAbbr) && text.includes(String(day))) {
                        // Verify it's exactly our day (not day 15 in "Apr 1  5")
                        const nums = text.match(/\\d+/g);
                        if (nums && nums.includes(String(day))) {
                            cell.click();
                            return true;
                        }
                    }
                }
                // Second pass: find by navigating month sections in the calendar
                const calMonths = document.querySelectorAll('[class*="v7-cal__month"], [class*="month"]');
                for (const monthEl of calMonths) {
                    const heading = monthEl.querySelector('[class*="heading"], [class*="title"], h3, h4');
                    if (!heading || !heading.textContent.includes(monthAbbr)) continue;
                    const dayCells = monthEl.querySelectorAll('[role="gridcell"], td[class*="day"], [class*="cal__day"]');
                    for (const dc of dayCells) {
                        if (dc.offsetHeight === 0) continue;
                        const nums = dc.textContent.trim().match(/\\d+/g);
                        if (nums && nums.includes(String(day))) {
                            dc.click();
                            return true;
                        }
                    }
                }
                return false;
            }""", [target_month_abbr, target_day])

            if clicked:
                logger.info("SunExpress: selected date %s", target.strftime("%Y-%m-%d"))
                await asyncio.sleep(0.5)
                return True

            # Last resort: gridcell with exact name
            gc = page.get_by_role("gridcell", name=str(target_day)).first
            if await gc.count() > 0:
                await gc.click(timeout=3000)
                await asyncio.sleep(0.5)
                return True

            return False
        except Exception as e:
            logger.warning("SunExpress: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        btn = page.get_by_role(
            "button", name=re.compile(r"^Search flights$", re.IGNORECASE)
        )
        if await btn.count() > 0:
            await btn.first.click(timeout=5000)
            logger.info("SunExpress: clicked Search flights")
            return
        btn2 = page.get_by_role(
            "button", name=re.compile(r"Search", re.IGNORECASE)
        )
        if await btn2.count() > 0:
            await btn2.first.click(timeout=5000)
            return
        await page.locator("button[type='submit']").first.click(timeout=3000)

    # ── API response parsing ──────────────────────────────────────────

    def _parse_api_data(self, api_data: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse structured flight data from intercepted API responses.

        The Angular SPA fetches availability data via XHR.  We inspect each
        captured JSON blob for arrays of flight/offer/journey objects and
        extract price, times, and route information.
        """
        booking_url = self._build_booking_url(req)
        date_str = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []
        seen_keys: set[str] = set()

        for entry in api_data:
            data = entry.get("data")
            if not data:
                continue

            # Walk the response looking for iterable collections of flights
            candidates = self._find_flight_arrays(data)
            for flights_list in candidates:
                for item in flights_list:
                    if not isinstance(item, dict):
                        continue
                    offer = self._api_item_to_offer(item, req, date_str, booking_url, seen_keys)
                    if offer:
                        offers.append(offer)

        return offers

    def _find_flight_arrays(self, obj: Any, depth: int = 0) -> list[list]:
        """Recursively find lists that look like flight collections."""
        if depth > 6:
            return []
        results: list[list] = []

        if isinstance(obj, list) and len(obj) > 0:
            # Check if this list contains flight-like dicts
            if any(
                isinstance(item, dict) and any(
                    k in item for k in (
                        "price", "totalPrice", "total_price", "amount",
                        "fare", "fares", "departureTime", "departure_time",
                        "departure", "segments", "flights", "legs",
                        "journeyKey", "flightNumber", "flight_number",
                    )
                )
                for item in obj[:5]
            ):
                results.append(obj)
            else:
                for item in obj[:20]:
                    results.extend(self._find_flight_arrays(item, depth + 1))
        elif isinstance(obj, dict):
            for v in obj.values():
                results.extend(self._find_flight_arrays(v, depth + 1))

        return results

    def _api_item_to_offer(
        self, item: dict, req: FlightSearchRequest,
        date_str: str, booking_url: str, seen: set,
    ) -> FlightOffer | None:
        """Convert a single API flight item into a FlightOffer (or None)."""
        # Extract price from common field names
        price = None
        for key in ("price", "totalPrice", "total_price", "amount", "fare",
                     "basePrice", "base_price", "totalAmount", "total_amount"):
            val = item.get(key)
            if isinstance(val, (int, float)) and val > 0:
                price = float(val)
                break
            if isinstance(val, dict):
                for sub in ("amount", "value", "total", "price"):
                    sv = val.get(sub)
                    if isinstance(sv, (int, float)) and sv > 0:
                        price = float(sv)
                        break
                if price:
                    break

        if not price or price <= 0:
            return None

        # Currency
        currency = req.currency or "EUR"
        for key in ("currency", "currencyCode", "currency_code"):
            val = item.get(key)
            if isinstance(val, str) and len(val) == 3:
                currency = val.upper()
                break

        # Departure / arrival times
        dep_time = self._extract_time(item, ("departureTime", "departure_time", "departure", "depTime", "dep"))
        arr_time = self._extract_time(item, ("arrivalTime", "arrival_time", "arrival", "arrTime", "arr"))

        dep_iata = self._extract_str(item, ("origin", "departureStation", "departure_station", "from")) or req.origin
        arr_iata = self._extract_str(item, ("destination", "arrivalStation", "arrival_station", "to")) or req.destination

        # Duration
        duration_sec = 0
        for key in ("duration", "durationSeconds", "duration_seconds", "travelTime", "travel_time"):
            val = item.get(key)
            if isinstance(val, (int, float)) and val > 0:
                duration_sec = int(val) if val > 300 else int(val * 60)
                break

        stops = 0
        for key in ("stops", "stopovers", "numberOfStops"):
            val = item.get(key)
            if isinstance(val, int):
                stops = val
                break

        # Deduplicate
        flight_key = f"XQ_{dep_iata}_{arr_iata}_{dep_time}_{price}"
        if flight_key in seen:
            return None
        seen.add(flight_key)

        dep_dt = self._parse_dt(f"{date_str}T{dep_time}" if dep_time and "T" not in dep_time else dep_time or f"{date_str}T00:00")
        arr_dt = self._parse_dt(f"{date_str}T{arr_time}" if arr_time and "T" not in arr_time else arr_time or f"{date_str}T00:00")
        if arr_dt <= dep_dt:
            from datetime import timedelta
            arr_dt = arr_dt + timedelta(days=1)
        if duration_sec == 0 and dep_dt and arr_dt:
            duration_sec = max(int((arr_dt - dep_dt).total_seconds()), 0)

        segment = FlightSegment(
            airline="XQ", airline_name="SunExpress",
            flight_no=self._extract_str(item, ("flightNumber", "flight_number", "flightNo", "flight_no")) or "",
            origin=dep_iata, destination=arr_iata,
            departure=dep_dt, arrival=arr_dt,
            cabin_class="M",
        )
        route = FlightRoute(segments=[segment], total_duration_seconds=duration_sec, stopovers=stops)

        return FlightOffer(
            id=f"xq_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=["SunExpress"], owner_airline="XQ",
            booking_url=booking_url, is_locked=False,
            source="sunexpress_direct", source_tier="free",
        )

    @staticmethod
    def _extract_time(item: dict, keys: tuple) -> str:
        """Extract a time string from a dict, trying multiple key names."""
        for k in keys:
            val = item.get(k)
            if isinstance(val, str) and val:
                # Handle full ISO datetime → extract time portion
                if "T" in val:
                    return val
                m = re.search(r"(\d{1,2}:\d{2})", val)
                if m:
                    return m.group(1)
        return ""

    @staticmethod
    def _extract_str(item: dict, keys: tuple) -> str:
        """Extract a string value from a dict, trying multiple key names."""
        for k in keys:
            val = item.get(k)
            if isinstance(val, str) and val:
                return val
        return ""

    # ── DOM extraction (results page) ──────────────────────────────────

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight cards from the SunExpress booking/select results page.

        Multiple extraction strategies are tried in order:
        1. Structured DOM elements (data attributes, card containers)
        2. Original text regex (Departure/Arrival/Duration pattern)
        3. Broad time + price pairing fallback
        """
        raw = await page.evaluate(r"""() => {
            const results = [];
            const body = document.body.innerText;

            // Extract currency from the page (£, €, TRY, etc.)
            const currMatch = body.match(/(?:GBP|EUR|USD|TRY|PLN|CHF|SEK|NOK|DKK)/);
            const currency = currMatch ? currMatch[0] : 'EUR';

            // ── Strategy 1: structured DOM cards ──────────────────────
            // Many Angular SPAs render flight cards with data attributes or
            // well-known class names.  Try to read them directly.
            const cardSels = [
                '[class*="flight-card"]', '[class*="flightCard"]',
                '[class*="journey-item"]', '[class*="journeyItem"]',
                '[class*="flight-row"]',  '[class*="flightRow"]',
                '[class*="select-flight"]',
                '[data-flight]', '[data-journey]',
            ];
            for (const sel of cardSels) {
                const cards = document.querySelectorAll(sel);
                if (cards.length === 0) continue;
                cards.forEach(card => {
                    const txt = card.innerText || '';
                    const timeMatches = txt.match(/\b([01]?\d|2[0-3]):[0-5]\d\b/g) || [];
                    const priceMatch = txt.match(/(?:[£€$₺]\s*)([\d,.]+)|([\d,.]+)\s*(?:GBP|EUR|USD|TRY)/);
                    const price = priceMatch
                        ? parseFloat((priceMatch[1] || priceMatch[2]).replace(/,/g, ''))
                        : 0;
                    if (timeMatches.length >= 2 && price > 0) {
                        const durMatch = txt.match(/(\d+)\s*h\s*(\d+)?\s*m?/);
                        const hours = durMatch ? parseInt(durMatch[1]) : 0;
                        const mins = durMatch && durMatch[2] ? parseInt(durMatch[2]) : 0;
                        const iataMatches = txt.match(/\b([A-Z]{3})\b/g) || [];
                        results.push({
                            depTime: timeMatches[0],
                            arrTime: timeMatches[1],
                            depIata: iataMatches.length >= 2 ? iataMatches[0] : '',
                            arrIata: iataMatches.length >= 2 ? iataMatches[1] : '',
                            duration: hours * 3600 + mins * 60,
                            stops: 0, price, currency,
                        });
                    }
                });
                if (results.length > 0) return results;
            }

            // ── Strategy 2: original text regex ──────────────────────
            // Pattern: "Departure: HH:MM ... Arrival: HH:MM ... Duration: Xh Ym ... PRICE"
            const flightPattern = /Departure:\s*(\d{1,2}:\d{2})\s*.*?(\w{3})\s+(\w{3})\s*.*?(?:Return|Arrival):\s*(\d{1,2}:\d{2})\s*.*?(\w{3})\s+(\w{3})\s*.*?Duration:\s*([\dhm\s]+?)(?:Nonstop|(\d+)\s*stop).*?(?:From\s*)?(?:[£€$₺]|GBP|EUR|USD|TRY)?\s*([\d,.]+)/gs;

            let m;
            while ((m = flightPattern.exec(body)) !== null) {
                const depTime = m[1];
                const depIata = m[3];   // 3-letter code after city name
                const arrTime = m[4];
                const arrIata = m[6];
                const durText = m[7].trim();
                const stops = m[8] ? parseInt(m[8]) : 0;
                const priceStr = m[9].replace(/,/g, '');
                const price = parseFloat(priceStr);
                if (isNaN(price) || price <= 0) continue;

                // Parse duration to seconds
                const hMatch = durText.match(/(\d+)\s*h/);
                const mMatch = durText.match(/(\d+)\s*m/);
                const hours = hMatch ? parseInt(hMatch[1]) : 0;
                const mins = mMatch ? parseInt(mMatch[1]) : 0;
                const durationSec = hours * 3600 + mins * 60;

                results.push({
                    depTime, arrTime,
                    depIata, arrIata,
                    duration: durationSec,
                    stops, price, currency
                });
            }
            if (results.length > 0) return results;

            // ── Strategy 3: relaxed time-arrow-time + price pattern ──
            // Catches "08:15 → 12:30 ... £89" or "08:15 - 12:30 ... 89.00 EUR"
            const relaxed = /(\d{1,2}:\d{2})\s*(?:→|->|–|-|―)\s*(\d{1,2}:\d{2})[\s\S]*?(?:[£€$₺]\s*)([\d,.]+)/g;
            let rm;
            while ((rm = relaxed.exec(body)) !== null) {
                const price = parseFloat(rm[3].replace(/,/g, ''));
                if (isNaN(price) || price <= 0) continue;
                results.push({
                    depTime: rm[1], arrTime: rm[2],
                    depIata: '', arrIata: '',
                    duration: 0, stops: 0, price, currency,
                });
            }
            if (results.length > 0) return results;

            // ── Strategy 4: broad time + price fallback ──────────────
            const times = (body.match(/\b([01]?\d|2[0-3]):[0-5]\d\b/g) || []);
            const prices = (body.match(/(?:[£€$₺]\s*)([\d,.]+)/g) || [])
                .map(p => parseFloat(p.replace(/[£€$₺\s,]/g, '')))
                .filter(p => p > 5 && p < 50000);
            // Pair times and prices
            for (let i = 0; i < Math.min(Math.floor(times.length / 2), prices.length); i++) {
                results.push({
                    depTime: times[i * 2],
                    arrTime: times[i * 2 + 1],
                    depIata: '', arrIata: '',
                    duration: 0, stops: 0,
                    price: prices[i],
                    currency: currency
                });
            }

            return results;
        }""")

        if not raw:
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        date_str = req.date_from.strftime("%Y-%m-%d")

        for i, flight in enumerate(raw):
            price = flight.get("price", 0)
            if not price or price <= 0:
                continue

            dep_time = flight.get("depTime", "00:00")
            arr_time = flight.get("arrTime", "00:00")
            dep_iata = flight.get("depIata") or req.origin
            arr_iata = flight.get("arrIata") or req.destination
            duration_sec = flight.get("duration", 0)
            stops = flight.get("stops", 0)
            currency = flight.get("currency", req.currency or "EUR")

            dep_dt = self._parse_dt(f"{date_str}T{dep_time}")
            arr_dt = self._parse_dt(f"{date_str}T{arr_time}")
            # Handle overnight flights
            if arr_dt <= dep_dt:
                from datetime import timedelta
                arr_dt = arr_dt + timedelta(days=1)

            if duration_sec == 0 and dep_dt and arr_dt:
                duration_sec = max(int((arr_dt - dep_dt).total_seconds()), 0)

            segment = FlightSegment(
                airline="XQ", airline_name="SunExpress",
                flight_no="",
                origin=dep_iata, destination=arr_iata,
                departure=dep_dt, arrival=arr_dt,
                cabin_class="M",
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=duration_sec,
                stopovers=stops,
            )

            flight_key = f"XQ_{dep_iata}_{arr_iata}_{dep_time}_{price}"
            offers.append(FlightOffer(
                id=f"xq_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["SunExpress"],
                owner_airline="XQ",
                booking_url=booking_url,
                is_locked=False,
                source="sunexpress_direct",
                source_tier="free",
            ))

        return offers

    # ── Helpers ────────────────────────────────────────────────────────

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("SunExpress %s→%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        search_hash = hashlib.md5(f"sunexpress{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "EUR"),
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
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.sunexpress.com/en-gb/booking/select/"
            f"?origin1={req.origin}&destination1={req.destination}"
            f"&departure1={dep}&adult={getattr(req, 'adults', 1) or 1}"
            f"&child={getattr(req, 'children', 0) or 0}"
            f"&infant={getattr(req, 'infants', 0) or 0}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"sunexpress{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=req.currency or "EUR", offers=[], total_results=0,
        )
