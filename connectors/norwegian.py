"""
Norwegian Air hybrid scraper — cookie-farm + curl_cffi direct API.

Norwegian's booking engine (booking.norwegian.com) is an Angular 18 SPA that
calls api-des.norwegian.com (Amadeus Digital Experience Suite). The search API
is behind Incapsula, which blocks raw HTTP clients. The token API is NOT
behind Incapsula.

Strategy (three-tier):
1. FAST: curl_cffi with impersonate alone (no cookies) — works when Incapsula
   accepts the TLS fingerprint without a prior browser session (~1s).
2. COOKIE-FARM: Playwright opens booking.norwegian.com deep-link, generates
   valid Incapsula cookies (reese84, visid_incap, etc.), then curl_cffi
   replays them for the token + search API calls (~3-5s first time, ~1s cached).
3. FALLBACK: Full Playwright browser interception — navigates to the booking
   deep-link and intercepts the air-bounds API response directly (~15-25s).

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
import random
import re
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
from connectors.browser import stealth_args

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
_BOOKING_ORIGIN = "https://booking.norwegian.com"
_IMPERSONATE = "chrome124"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_SEC_CH_UA = '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'
_COOKIE_MAX_AGE = 15 * 60  # Re-farm cookies after 15 minutes

# Shared cookie farm state
_farm_lock: Optional[asyncio.Lock] = None
_farmed_cookies: list[dict] = []
_farm_timestamp: float = 0.0
_pw_instance = None
_browser = None


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _get_browser():
    """Shared headed Chromium for cookie farming (launched once, reused)."""
    global _pw_instance, _browser
    if _browser and _browser.is_connected():
        return _browser
    from connectors.browser import launch_headed_browser
    _browser = await launch_headed_browser()
    logger.info("Norwegian: browser launched for cookie farming")
    return _browser


class NorwegianConnectorClient:
    """Norwegian hybrid scraper — cookie-farm + curl_cffi direct API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Norwegian flights via three-tier strategy.

        Tier 1 (~1s): curl_cffi cookieless — impersonation alone.
        Tier 2 (~1-3s): curl_cffi with farmed Incapsula cookies.
        Tier 3 (~15-25s): Full Playwright interception fallback.
        """
        t0 = time.monotonic()

        try:
            # Tier 1: try cookieless API first (curl_cffi impersonation only)
            data = await self._api_search(req, cookies=[])
            if data:
                logger.info("Norwegian: cookieless API succeeded")
            else:
                # Tier 2: farm cookies and retry
                cookies = await self._ensure_cookies(req)
                if cookies:
                    data = await self._api_search(req, cookies)

                # Re-farm once if stale
                if data is None and cookies:
                    logger.info("Norwegian: API search failed, re-farming cookies")
                    cookies = await self._farm_cookies(req)
                    if cookies:
                        data = await self._api_search(req, cookies)

                # Tier 3: full Playwright interception
                if not data:
                    logger.warning("Norwegian: API returned no data, falling back to Playwright")
                    return await self._playwright_fallback(req, t0)

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
        """Open Playwright, navigate to booking deep-link, extract Incapsula cookies."""
        global _farmed_cookies, _farm_timestamp

        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )

        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            search_done = asyncio.Event()

            async def on_response(response):
                try:
                    if "air-bounds" in response.url and response.status == 200:
                        search_done.set()
                except Exception:
                    pass

            page.on("response", on_response)

            # Navigate to booking.norwegian.com (the Angular SPA that calls
            # api-des.norwegian.com).  This is the domain whose Incapsula
            # cookies the API actually checks — www.norwegian.com cookies are
            # for a different Incapsula site-id and are not accepted by the API.
            date_str = req.date_from.strftime("%Y-%m-%d")
            deep_url = (
                f"{_BOOKING_ORIGIN}/en/offer?"
                f"D_City={req.origin}&A_City={req.destination}"
                f"&TripType=1&D_Day={date_str}"
                f"&AdultCount={req.adults}"
                f"&ChildCount={req.children or 0}"
                f"&InfantCount={req.infants or 0}"
            )
            logger.info("Norwegian: farming cookies via booking deep-link %s→%s", req.origin, req.destination)
            await page.goto(
                deep_url,
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await asyncio.sleep(3)
            await self._dismiss_cookies(page)

            remaining = max(self.timeout - 5, 15)
            try:
                await asyncio.wait_for(search_done.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Norwegian: cookie farm search timed out, using page-load cookies")

            cookies = await context.cookies()
            if cookies:
                _farmed_cookies = cookies
                _farm_timestamp = time.monotonic()
                logger.info("Norwegian: farmed %d cookies", len(cookies))
            return cookies

        except Exception as e:
            logger.error("Norwegian: cookie farm error: %s", e)
            return []
        finally:
            await context.close()

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

        # Load farmed cookies into session (may be empty for cookieless path)
        for c in cookies:
            domain = c.get("domain", "")
            sess.cookies.set(c["name"], c["value"], domain=domain)

        # Common browser-like headers to satisfy Incapsula
        _browser_headers = {
            "User-Agent": _UA,
            "Origin": _BOOKING_ORIGIN,
            "Referer": f"{_BOOKING_ORIGIN}/",
            "sec-ch-ua": _SEC_CH_UA,
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Accept-Language": "en-US,en;q=0.9",
        }

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
                    **_browser_headers,
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "*/*",
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
                    **_browser_headers,
                    "Authorization": f"Bearer {access_token}",
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
    # Playwright fallback (full browser flow, used if API fails)
    # ------------------------------------------------------------------

    async def _playwright_fallback(
        self, req: FlightSearchRequest, t0: float,
    ) -> FlightSearchResponse:
        """Full Playwright interception flow as fallback."""
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )

        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    ct = response.headers.get("content-type", "")
                    if response.status != 200 or "json" not in ct:
                        return
                    if "air-bounds" in url or "airbounds" in url:
                        data = await response.json()
                        if data and isinstance(data, dict):
                            captured_data["json"] = data
                            api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            date_str = req.date_from.strftime("%Y-%m-%d")
            deep_url = (
                f"{_BOOKING_ORIGIN}/en/offer?"
                f"D_City={req.origin}&A_City={req.destination}"
                f"&TripType=1&D_Day={date_str}"
                f"&AdultCount={req.adults}"
                f"&ChildCount={req.children or 0}"
                f"&InfantCount={req.infants or 0}"
            )
            logger.info("Norwegian: Playwright fallback for %s→%s", req.origin, req.destination)
            await page.goto(
                deep_url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(2.0)
            await self._dismiss_cookies(page)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Norwegian: fallback timed out waiting for API response")
                return self._empty(req)

            # Also update cookie farm from this successful browser session
            global _farmed_cookies, _farm_timestamp
            _farmed_cookies = await context.cookies()
            _farm_timestamp = time.monotonic()

            data = captured_data.get("json", {})
            if not data:
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_air_bounds(data, req)
            offers.sort(key=lambda o: o.price)

            logger.info(
                "Norwegian %s→%s returned %d offers in %.1fs (Playwright fallback)",
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
            logger.error("Norwegian Playwright fallback error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Cookie/consent helpers
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        """Remove OneTrust cookie banner via JS (avoids click-interception)."""
        try:
            await page.evaluate("""() => {
                const ot = document.getElementById('onetrust-consent-sdk');
                if (ot) ot.remove();
                document.querySelectorAll('[class*="cookie"], [id*="cookie"], [class*="consent"]')
                    .forEach(el => { if (el.offsetHeight > 0) el.remove(); });
            }""")
        except Exception:
            pass

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
