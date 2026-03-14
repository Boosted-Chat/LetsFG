"""
Norwegian Air hybrid scraper — cookie-farm + curl_cffi direct API.

Norwegian's booking engine (booking.norwegian.com) is an Angular 18 SPA that
calls api-des.norwegian.com (Amadeus Digital Experience Suite). The search API
is behind Incapsula, which blocks raw HTTP clients. The token API is NOT
behind Incapsula.

Strategy (hybrid cookie-farm):
1. ONCE per ~25 min: Playwright opens homepage, fills search form, submits.
   This generates valid Incapsula cookies (reese84, visid_incap, etc.).
   Extract all cookies via context.cookies().
2. For each search: curl_cffi uses farmed cookies to:
   a) POST token/initialization → get Bearer access_token (~0.4s)
   b) POST airlines/DY/v2/search/air-bounds → get flight data (~0.6s)
3. Parse airBoundGroups → FlightOffers

Result: ~1s per search instead of ~45s with full Playwright.

API details (discovered Mar 2026):
  Token: POST api-des.norwegian.com/v1/security/oauth2/token/initialization
    Body: client_id, client_secret, grant_type=client_credentials, fact (JSON)
  Search: POST api-des.norwegian.com/airlines/DY/v2/search/air-bounds
    Body: {commercialFareFamilies, itineraries, travelers, searchPreferences}
  Response: {data: {airBoundGroups: [{boundDetails, airBounds: [{prices, ...}]}]}}
  Prices are in CENTS (divide by 100)
  flightId format: SEG-DY1303-LGWOSL-2026-04-15-0920
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
from datetime import datetime, timedelta
from typing import Optional

from curl_cffi import requests as cffi_requests

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import stealth_popen_kwargs, find_chrome, _launched_procs

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-IE"]
_TIMEZONES = ["Europe/London", "Europe/Berlin", "Europe/Oslo", "Europe/Paris"]

_CLIENT_ID = "YnF1uDBnJMWsGEmAndoGljO0DgkBeWaE"
_CLIENT_SECRET = "mrYaim0FdBrNRRZf"
_TOKEN_URL = "https://api-des.norwegian.com/v1/security/oauth2/token/initialization"
_SEARCH_URL = "https://api-des.norwegian.com/airlines/DY/v2/search/air-bounds"
_IMPERSONATE = "chrome131"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
_COOKIE_MAX_AGE = 25 * 60  # Re-farm cookies after 25 minutes
_DEBUG_PORT = 9460
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", ".norwegian_chrome_profile"
)

# Shared cookie farm state
_farm_lock: Optional[asyncio.Lock] = None
_farmed_cookies: list[dict] = []
_farm_timestamp: float = 0.0
_pw_instance = None
_browser = None
_chrome_proc = None


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _get_browser():
    """Launch real headed Chrome via CDP for cookie farming (Incapsula blocks headless)."""
    global _pw_instance, _browser, _chrome_proc
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
        logger.info("Norwegian: connected to existing Chrome on port %d", _DEBUG_PORT)
        return _browser
    except Exception:
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass

    # Launch Chrome HEADED (no --headless) — Incapsula blocks headless Chrome.
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
    logger.info("Norwegian: Chrome launched headed on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)
    return _browser


class NorwegianConnectorClient:
    """Norwegian hybrid scraper — cookie-farm + curl_cffi direct API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Norwegian flights via cookie-farm + curl_cffi direct API.

        Fast path (~1s): curl_cffi with farmed Incapsula cookies.
        Slow path (~18s): Playwright farms cookies first, then curl_cffi.
        """
        t0 = time.monotonic()

        try:
            cookies = await self._ensure_cookies(req)
            if not cookies:
                logger.warning("Norwegian: cookie farm failed, no cookies")
                return self._empty(req)

            data = await self._api_search(req, cookies)

            # If search failed (expired cookies), re-farm once and retry
            if data is None:
                logger.info("Norwegian: API search failed, re-farming cookies")
                cookies = await self._farm_cookies(req)
                if cookies:
                    data = await self._api_search(req, cookies)

            if not data:
                logger.warning("Norwegian: no data after search")
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_air_bounds(data, req)
            offers.sort(key=lambda o: o.price)

            logger.info(
                "Norwegian %s→%s returned %d offers in %.1fs (hybrid API)",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"norwegian{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else req.currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Norwegian hybrid error: %s", e)
            return self._empty(req)

    # ------------------------------------------------------------------
    # Cookie farm — Playwright generates Incapsula cookies
    # ------------------------------------------------------------------

    async def _ensure_cookies(self, req: FlightSearchRequest) -> list[dict]:
        """Return valid farmed cookies, farming new ones if needed."""
        global _farmed_cookies, _farm_timestamp
        lock = _get_farm_lock()
        async with lock:
            age = time.monotonic() - _farm_timestamp
            if _farmed_cookies and age < _COOKIE_MAX_AGE:
                return _farmed_cookies
            return await self._farm_cookies(req)

    async def _farm_cookies(self, req: FlightSearchRequest) -> list[dict]:
        """Visit booking.norwegian.com to get valid Incapsula cookies."""
        global _farmed_cookies, _farm_timestamp

        browser = await _get_browser()
        context = browser.contexts[0] if browser.contexts else await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
        )

        try:
            page = await context.new_page()

            logger.info("Norwegian: farming Incapsula cookies from booking.norwegian.com")
            await page.goto(
                "https://booking.norwegian.com/booking/",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await asyncio.sleep(3)

            cookies = await context.cookies()
            if cookies:
                _farmed_cookies = cookies
                _farm_timestamp = time.monotonic()
                incap = [c for c in cookies if "incap" in c["name"].lower() or "reese84" in c["name"].lower()]
                logger.info("Norwegian: farmed %d cookies (%d Incapsula)", len(cookies), len(incap))
            return cookies

        except Exception as e:
            logger.error("Norwegian: cookie farm error: %s", e)
            return []
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Direct API via curl_cffi
    # ------------------------------------------------------------------

    async def _api_search(
        self, req: FlightSearchRequest, cookies: list[dict]
    ) -> Optional[dict]:
        """Get token + search via curl_cffi with farmed cookies."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._api_search_sync, req, cookies)

    def _api_search_sync(
        self, req: FlightSearchRequest, cookies: list[dict]
    ) -> Optional[dict]:
        """Synchronous curl_cffi token + search."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)

        # Load farmed cookies into session
        for c in cookies:
            domain = c.get("domain", "")
            sess.cookies.set(c["name"], c["value"], domain=domain)

        # Step 1: Get OAuth2 token
        date_str = req.date_from.strftime("%Y-%m-%dT00:00:00")
        fact = json.dumps({
            "keyValuePairs": [
                {"key": "originLocationCode1", "value": req.origin},
                {"key": "destinationLocationCode1", "value": req.destination},
                {"key": "departureDateTime1", "value": date_str},
                {"key": "market", "value": "EN"},
                {"key": "channel", "value": "B2C"},
            ]
        })

        try:
            r_token = sess.post(
                _TOKEN_URL,
                data={
                    "client_id": _CLIENT_ID,
                    "client_secret": _CLIENT_SECRET,
                    "grant_type": "client_credentials",
                    "fact": fact,
                },
                headers={
                    "User-Agent": _UA,
                    "Origin": "https://booking.norwegian.com",
                    "Referer": "https://booking.norwegian.com/",
                },
                timeout=15,
            )
        except Exception as e:
            logger.error("Norwegian: token request failed: %s", e)
            return None

        if r_token.status_code != 200:
            logger.warning("Norwegian: token returned %d", r_token.status_code)
            return None

        access_token = r_token.json().get("access_token")
        if not access_token:
            logger.warning("Norwegian: no access_token in response")
            return None

        # Step 2: Search flights
        search_body = {
            "commercialFareFamilies": ["DYSTD"],
            "itineraries": [{
                "originLocationCode": req.origin,
                "destinationLocationCode": req.destination,
                "departureDateTime": f"{date_str}.000",
                "directFlights": False,
                "originLocationType": "airport",
                "destinationLocationType": "airport",
                "isRequestedBound": True,
            }],
            "travelers": self._build_travelers(req),
            "searchPreferences": {"showSoldOut": True, "showMilesPrice": False},
        }

        try:
            r_search = sess.post(
                _SEARCH_URL,
                json=search_body,
                headers={
                    "User-Agent": _UA,
                    "Authorization": f"Bearer {access_token}",
                    "Origin": "https://booking.norwegian.com",
                    "Referer": "https://booking.norwegian.com/",
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/plain, */*",
                },
                timeout=30,
            )
        except Exception as e:
            logger.error("Norwegian: search request failed: %s", e)
            return None

        if r_search.status_code != 200:
            logger.warning("Norwegian: search returned %d", r_search.status_code)
            return None

        return r_search.json()

    @staticmethod
    def _build_travelers(req: FlightSearchRequest) -> list[dict]:
        travelers = []
        for _ in range(req.adults):
            travelers.append({"passengerTypeCode": "ADT"})
        for _ in range(req.children or 0):
            travelers.append({"passengerTypeCode": "CHD"})
        for _ in range(req.infants or 0):
            travelers.append({"passengerTypeCode": "INF"})
        return travelers or [{"passengerTypeCode": "ADT"}]

    # ------------------------------------------------------------------
    # Form interaction for cookie farming (selectors verified Mar 2026)
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        """Remove OneTrust cookie banner — click accept first, then JS cleanup."""
        try:
            for label in ["Accept All Cookies", "Accept all", "Accept"]:
                btn = page.get_by_role("button", name=label)
                if await btn.count() > 0:
                    await btn.first.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    break
        except Exception:
            pass
        try:
            await page.evaluate("""() => {
                const ot = document.getElementById('onetrust-consent-sdk');
                if (ot) ot.remove();
                document.querySelectorAll('[class*="cookie"], [id*="cookie"], [class*="consent"]')
                    .forEach(el => { if (el.offsetHeight > 0) el.remove(); });
            }""")
        except Exception:
            pass

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> None:
        """Fill the Norwegian homepage search form (one-way, airports, date)."""
        # Wait for the search form to be interactive
        try:
            await page.get_by_role("combobox", name="From").wait_for(
                state="visible", timeout=10000
            )
        except Exception:
            logger.debug("Norwegian: From combobox not found, trying anyway")

        # Select one-way — click the text label (radio input is covered by label)
        try:
            await page.get_by_text("One-way").click(timeout=3000)
            await asyncio.sleep(0.3)
        except Exception:
            logger.debug("Norwegian: could not click One-way")

        # Fill 'From' airport
        await self._fill_airport_field(page, "From", req.origin)
        await asyncio.sleep(0.5)

        # Fill 'To' airport
        await self._fill_airport_field(page, "To", req.destination)
        await asyncio.sleep(0.5)

        # Fill departure date via calendar picker
        await self._fill_date(page, req)

    async def _fill_airport_field(self, page, label: str, iata: str) -> None:
        """Fill an airport combobox and pick the matching option.

        The Norwegian form exposes ``combobox "From"`` / ``combobox "To"``.
        Typing the IATA code filters the listbox; each option renders as
        ``button "CityName (IATA) Country"`` inside the listbox.
        """
        try:
            combo = page.get_by_role("combobox", name=label)
            await combo.click(timeout=3000)
            await asyncio.sleep(0.3)
            await combo.fill(iata)
            await asyncio.sleep(1.5)

            # Click the first option button whose name contains "(IATA)"
            option_btn = page.get_by_role("button", name=re.compile(
                rf"\({re.escape(iata)}\)", re.IGNORECASE
            )).first
            await option_btn.click(timeout=5000)
        except Exception as e:
            logger.debug("Norwegian: %s field error: %s", label, e)

    async def _fill_date(self, page, req: FlightSearchRequest) -> None:
        """Open the calendar picker, navigate to the correct month, click the day."""
        target_year = req.date_from.year
        target_month = req.date_from.month
        target_day = req.date_from.day

        try:
            # Click the "Outbound flight" textbox to open the calendar
            date_box = page.get_by_role("textbox", name="Outbound flight")
            await date_box.click(timeout=3000)
            await asyncio.sleep(0.5)

            # Navigate months using the <select> inside the datepicker.
            # Option values follow the pattern "YYYY-MM-01Txx:xx:xx.xxxZ".
            target_prefix = f"{target_year}-{target_month:02d}-01T"
            changed = await page.evaluate(f"""() => {{
                const sel = document.querySelector('.nas-datepicker select');
                if (!sel) return 'no select';
                for (const opt of sel.options) {{
                    if (opt.value.startsWith('{target_prefix}')) {{
                        sel.value = opt.value;
                        sel.dispatchEvent(new Event('change', {{bubbles: true}}));
                        return 'ok';
                    }}
                }}
                return 'month not found';
            }}""")
            if changed != "ok":
                logger.debug("Norwegian: month select result: %s", changed)
            await asyncio.sleep(0.5)

            # Click the day button inside the calendar table
            # The calendar renders buttons with just the day number as name.
            # Use a narrow locator: table cell button with exact day text.
            day_btn = page.locator(
                f".nas-datepicker table button"
            ).filter(has_text=re.compile(rf"^{target_day}$")).first
            await day_btn.click(timeout=3000)
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.debug("Norwegian: Date error: %s", e)

    async def _click_search(self, page) -> None:
        """Click 'Search and book' (enabled only after form is filled)."""
        try:
            btn = page.get_by_role("button", name="Search and book")
            await btn.click(timeout=5000)
        except Exception:
            # Fallback: try any submit button
            try:
                await page.locator("button[type='submit']").first.click(timeout=3000)
            except Exception:
                await page.keyboard.press("Enter")

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_air_bounds(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Amadeus DES air-bounds response into FlightOffers."""
        offers: list[FlightOffer] = []
        groups = data.get("data", {}).get("airBoundGroups", [])
        booking_url = self._build_booking_url(req)

        for group in groups:
            bound_details = group.get("boundDetails", {})
            segments_raw = bound_details.get("segments", [])
            duration = bound_details.get("duration", 0)  # seconds

            # Parse segments from flightIds
            segments = self._parse_segments(segments_raw)
            if not segments:
                continue

            # Fix arrival times using bound duration
            self._fix_arrival_times(segments, duration)

            stopovers = max(len(segments) - 1, 0)

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=max(duration, 0),
                stopovers=stopovers,
            )

            # Get cheapest fare from airBounds (LOWFARE < LOWPLUS < FLEX)
            for air_bound in group.get("airBounds", []):
                fare_family = air_bound.get("fareFamilyCode", "")
                if fare_family != "LOWFARE":
                    continue  # Only take cheapest fare family

                total_prices = air_bound.get("prices", {}).get("totalPrices", [])
                if not total_prices:
                    continue

                price_obj = total_prices[0]
                total_cents = price_obj.get("total", 0)
                currency = price_obj.get("currencyCode", "EUR")
                price = total_cents / 100.0  # Prices are in cents

                if price <= 0:
                    continue

                flight_ids = "_".join(s.get("flightId", "") for s in segments_raw)
                key = f"{flight_ids}_{fare_family}_{total_cents}"

                offer = FlightOffer(
                    id=f"dy_{hashlib.md5(key.encode()).hexdigest()[:12]}",
                    price=round(price, 2),
                    currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=["Norwegian"],
                    owner_airline="DY",
                    booking_url=booking_url,
                    is_locked=False,
                    source="norwegian_api",
                    source_tier="free",
                )
                offers.append(offer)
                break  # Only one offer per group (cheapest)

        return offers

    def _parse_segments(self, segments_raw: list) -> list[FlightSegment]:
        """Parse segments from flightId strings.

        flightId format: SEG-DY1303-LGWOSL-2026-04-15-0920
        → carrier=DY, number=1303, origin=LGW, dest=OSL, date=2026-04-15, time=09:20
        """
        segments: list[FlightSegment] = []

        for seg_info in segments_raw:
            flight_id = seg_info.get("flightId", "")
            match = re.match(
                r"SEG-([A-Z0-9]{2})(\d+)-([A-Z]{3})([A-Z]{3})-(\d{4}-\d{2}-\d{2})-(\d{4})",
                flight_id,
            )
            if not match:
                logger.debug("Norwegian: could not parse flightId: %s", flight_id)
                continue

            carrier = match.group(1)
            number = match.group(2)
            origin = match.group(3)
            dest = match.group(4)
            date_str = match.group(5)
            time_str = match.group(6)

            dep_dt = datetime.strptime(
                f"{date_str} {time_str[:2]}:{time_str[2:]}", "%Y-%m-%d %H:%M"
            )

            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Norwegian",
                flight_no=f"{carrier}{number}",
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=dep_dt,  # Placeholder — fixed by _fix_arrival_times
                cabin_class="M",
            ))

        return segments

    def _fix_arrival_times(self, segments: list[FlightSegment], duration_seconds: int) -> None:
        """Fix placeholder arrival times using total bound duration."""
        if len(segments) == 1 and duration_seconds > 0:
            segments[0] = FlightSegment(
                airline=segments[0].airline,
                airline_name=segments[0].airline_name,
                flight_no=segments[0].flight_no,
                origin=segments[0].origin,
                destination=segments[0].destination,
                departure=segments[0].departure,
                arrival=segments[0].departure + timedelta(seconds=duration_seconds),
                cabin_class=segments[0].cabin_class,
            )
        elif len(segments) > 1 and duration_seconds > 0:
            # For multi-segment: set last segment's arrival from total duration
            segments[-1] = FlightSegment(
                airline=segments[-1].airline,
                airline_name=segments[-1].airline_name,
                flight_no=segments[-1].flight_no,
                origin=segments[-1].origin,
                destination=segments[-1].destination,
                departure=segments[-1].departure,
                arrival=segments[0].departure + timedelta(seconds=duration_seconds),
                cabin_class=segments[-1].cabin_class,
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        date_str = req.date_from.strftime("%d/%m/%Y")
        return (
            f"https://www.norwegian.com/en/"
            f"?D_City={req.origin}&A_City={req.destination}"
            f"&TripType=1&D_Day={date_str}"
            f"&AdultCount={req.adults}"
            f"&ChildCount={req.children or 0}"
            f"&InfantCount={req.infants or 0}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"norwegian{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
