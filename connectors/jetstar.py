"""
Jetstar hybrid scraper — curl_cffi direct API + SSR fetch + Playwright fallback.

Jetstar (IATA: JQ) is an Australian low-cost airline in the Qantas Group,
operating domestic/international flights across Asia-Pacific.

Strategy (hybrid — API first, Kasada-resilient):
1. FAST PATH: curl_cffi with Chrome TLS impersonation → Navitaire dotREZ API.
   POST token + POST availability/search — bypasses Kasada by not loading a
   full browser page. ~1-3s, no browser needed.
2. SSR FALLBACK: curl_cffi GET of booking URL → extract bundle-data-v2 JSON
   from <script> tag in SSR-rendered page. ~2-5s, no browser needed.
3. BROWSER FALLBACK: CDP Chrome + bundle-data-v2/DOM extraction (original
   approach, used only when curl_cffi paths fail). ~10-25s.

Booking engine observations (Mar 2026):
- Navitaire dotREZ platform at booking.jetstar.com
- dotREZ API: POST /api/nsk/v1/token (anonymous session) +
  POST /api/nsk/v4/availability/search (flight search)
- SSR page embeds <script id="bundle-data-v2" type="application/json">
  with Trips[].Flights[] structure (~327KB)
- JourneySellKey format: "JQ~ 501~ ~~SYD~04/15/2026 06:00~MEL~04/15/2026 07:40~~"
- Bundles[].RegularInclusiveAmount = regular price, CjInclusiveAmount = member price
- Kasada WAF blocks Playwright Chromium; curl_cffi TLS impersonation bypasses it
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

try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL = True
except ImportError:
    HAS_CURL = False

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import stealth_args, stealth_position_arg, stealth_popen_kwargs

logger = logging.getLogger(__name__)

# ── curl_cffi API constants ────────────────────────────────────────────────
_IMPERSONATE = "chrome124"
_API_BASE = "https://booking.jetstar.com"
_TOKEN_URLS = [
    "https://booking.jetstar.com/api/nsk/v1/token",
    "https://booking.jetstar.com/au/en/api/nsk/v1/token",
]
_SEARCH_URLS = [
    "https://booking.jetstar.com/api/nsk/v4/availability/search",
    "https://booking.jetstar.com/au/en/api/nsk/v4/availability/search",
    "https://booking.jetstar.com/api/nsk/v2/availability/search",
]
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_TOKEN_MAX_AGE = 10 * 60  # Re-acquire token every 10 minutes

# ── Shared token state ─────────────────────────────────────────────────────
_token_lock: Optional[asyncio.Lock] = None
_cached_token: Optional[str] = None
_token_timestamp: float = 0.0
_working_token_url: Optional[str] = None
_working_search_url: Optional[str] = None


def _get_token_lock() -> asyncio.Lock:
    global _token_lock
    if _token_lock is None:
        _token_lock = asyncio.Lock()
    return _token_lock

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-AU", "en-NZ", "en-GB", "en-US", "en-SG"]
_TIMEZONES = [
    "Australia/Sydney", "Australia/Melbourne", "Australia/Brisbane",
    "Pacific/Auckland", "Asia/Singapore",
]

_MAX_ATTEMPTS = 3
_DEBUG_PORT = 9444
_USER_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".jetstar_chrome_data")

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
    """Launch real Chrome via CDP, or fall back to Playwright headed.

    Uses a persistent user-data-dir so Kasada clearance persists across runs.
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

        try:
            from connectors.browser import get_or_launch_cdp
            _browser, _chrome_proc = await get_or_launch_cdp(_DEBUG_PORT, _USER_DATA_DIR)
            logger.info("Jetstar: Chrome ready via CDP (port %d)", _DEBUG_PORT)
            return _browser
        except Exception as e:
            logger.warning("Jetstar: CDP failed: %s, falling back to Playwright", e)

        from connectors.browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("Jetstar: Playwright browser launched (fallback)")
        return _browser


class JetstarConnectorClient:
    """Jetstar hybrid scraper — curl_cffi direct API + SSR fetch + Playwright fallback."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        # ── Fast path 1: curl_cffi direct Navitaire API ──────────────
        if HAS_CURL:
            try:
                result = await self._try_direct_api(req, t0)
                if result and result.total_results > 0:
                    return result
            except Exception as e:
                logger.debug("Jetstar: direct API path failed: %s", e)

        # ── Fast path 2: curl_cffi SSR page fetch ────────────────────
        if HAS_CURL:
            try:
                result = await self._try_ssr_fetch(req, t0)
                if result and result.total_results > 0:
                    return result
            except Exception as e:
                logger.debug("Jetstar: SSR fetch path failed: %s", e)

        # ── Slow fallback: Playwright CDP browser ────────────────────
        logger.info("Jetstar: curl_cffi paths exhausted, falling back to Playwright")
        adults = getattr(req, "adults", 1) or 1
        dep = req.date_from.strftime("%Y-%m-%d")
        search_url = (
            f"https://booking.jetstar.com/au/en/booking/search-flights"
            f"?origin1={req.origin}&destination1={req.destination}"
            f"&departuredate1={dep}&ADT={adults}"
        )

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                offers = await self._attempt_search(search_url, req)
                if offers is not None:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)
                logger.warning(
                    "Jetstar: attempt %d/%d got no results or blocked",
                    attempt, _MAX_ATTEMPTS,
                )
            except Exception as e:
                logger.warning("Jetstar: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    # ------------------------------------------------------------------
    # Fast path 1: curl_cffi → Navitaire dotREZ API
    # ------------------------------------------------------------------

    async def _try_direct_api(
        self, req: FlightSearchRequest, t0: float
    ) -> Optional[FlightSearchResponse]:
        """Try Navitaire dotREZ availability API via curl_cffi.

        1. Acquire anonymous session token from /api/nsk/v1/token
        2. POST /api/nsk/v4/availability/search with token
        3. Parse Navitaire availability response → FlightOffers
        """
        token = await self._ensure_api_token()
        if not token:
            logger.debug("Jetstar: no API token, skipping direct API path")
            return None

        data = await self._api_search(req, token)

        # If token might be stale, re-acquire once and retry
        if data is None:
            logger.debug("Jetstar: API search failed, re-acquiring token")
            global _cached_token, _token_timestamp
            _cached_token = None
            _token_timestamp = 0.0
            token = await self._ensure_api_token()
            if token:
                data = await self._api_search(req, token)

        if not data:
            return None

        offers = self._parse_api_availability(data, req)
        if not offers:
            return None

        elapsed = time.monotonic() - t0
        logger.info(
            "Jetstar %s→%s returned %d offers in %.1fs (direct API)",
            req.origin, req.destination, len(offers), elapsed,
        )
        return self._build_response(offers, req, elapsed)

    async def _ensure_api_token(self) -> Optional[str]:
        """Return cached Navitaire token, acquiring a fresh one if expired."""
        global _cached_token, _token_timestamp
        lock = _get_token_lock()
        async with lock:
            age = time.monotonic() - _token_timestamp
            if _cached_token and age < _TOKEN_MAX_AGE:
                return _cached_token
            return await self._acquire_api_token()

    async def _acquire_api_token(self) -> Optional[str]:
        """Fetch a fresh anonymous session token from Navitaire."""
        global _cached_token, _token_timestamp, _working_token_url
        loop = asyncio.get_event_loop()
        token = await loop.run_in_executor(None, self._acquire_api_token_sync)
        if token:
            _cached_token = token
            _token_timestamp = time.monotonic()
            logger.info("Jetstar: acquired Navitaire API token")
        return token

    @staticmethod
    def _acquire_api_token_sync() -> Optional[str]:
        """Synchronous token acquisition via curl_cffi."""
        global _working_token_url
        urls = ([_working_token_url] + _TOKEN_URLS) if _working_token_url else _TOKEN_URLS
        seen = set()

        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            try:
                sess = cffi_requests.Session(impersonate=_IMPERSONATE)
                r = sess.post(
                    url,
                    json={},
                    headers={
                        "Content-Type": "application/json",
                        "Accept": "application/json, text/plain, */*",
                        "Origin": _API_BASE,
                        "Referer": f"{_API_BASE}/au/en/booking/search-flights",
                        "User-Agent": _UA,
                    },
                    timeout=10,
                    allow_redirects=True,
                )
                if r.status_code == 200:
                    data = r.json()
                    token = (
                        data.get("token")
                        or data.get("accessToken")
                        or data.get("data", {}).get("token")
                    )
                    if not token:
                        for v in data.values():
                            if isinstance(v, str) and len(v) > 30:
                                token = v
                                break
                    if token:
                        _working_token_url = url
                        return token
                logger.debug("Jetstar: token endpoint %s returned %d", url, r.status_code)
            except Exception as e:
                logger.debug("Jetstar: token endpoint %s error: %s", url, e)
        return None

    async def _api_search(
        self, req: FlightSearchRequest, token: str
    ) -> Optional[dict]:
        """Execute Navitaire availability search via curl_cffi."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._api_search_sync, req, token)

    def _api_search_sync(self, req: FlightSearchRequest, token: str) -> Optional[dict]:
        """Synchronous Navitaire availability search."""
        global _working_search_url
        date_str = req.date_from.strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1
        children = getattr(req, "children", 0) or 0
        infants = getattr(req, "infants", 0) or 0
        currency = req.currency if req.currency and req.currency != "EUR" else "AUD"

        body = {
            "criteria": [
                {
                    "stations": {
                        "originStationCodes": [req.origin],
                        "destinationStationCodes": [req.destination],
                    },
                    "dates": {
                        "beginDate": date_str,
                        "endDate": date_str,
                    },
                }
            ],
            "passengers": {
                "types": [
                    {"type": "ADT", "count": adults},
                ],
            },
            "codes": {
                "currencyCode": currency,
            },
        }
        if children:
            body["passengers"]["types"].append({"type": "CHD", "count": children})
        if infants:
            body["passengers"]["types"].append({"type": "INF", "count": infants})

        headers = {
            "Authorization": f"Bearer {token}" if not token.startswith("Bearer") else token,
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": _API_BASE,
            "Referer": f"{_API_BASE}/au/en/booking/search-flights",
            "User-Agent": _UA,
        }

        urls = ([_working_search_url] + _SEARCH_URLS) if _working_search_url else _SEARCH_URLS
        seen = set()

        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            try:
                sess = cffi_requests.Session(impersonate=_IMPERSONATE)
                r = sess.post(url, json=body, headers=headers, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, dict) and (
                        data.get("data") or data.get("trips") or data.get("journeysAvailable")
                    ):
                        _working_search_url = url
                        return data
                logger.debug(
                    "Jetstar: search endpoint %s returned %d", url, r.status_code
                )
            except Exception as e:
                logger.debug("Jetstar: search endpoint %s error: %s", url, e)
        return None

    def _parse_api_availability(
        self, data: dict, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Parse Navitaire dotREZ availability response."""
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Standard dotREZ: data.trips[].journeysAvailable[]
        trips = (
            data.get("data", {}).get("trips", [])
            if isinstance(data.get("data"), dict) else
            data.get("trips", [])
        )
        for trip in trips:
            journeys = (
                trip.get("journeysAvailable")
                or trip.get("journeys")
                or trip.get("Flights")
                or trip.get("flights")
                or []
            )
            for journey in journeys:
                offer = self._parse_api_journey(journey, req, booking_url)
                if offer:
                    offers.append(offer)

        if not offers:
            # Fallback: flatter structures
            journeys = (
                data.get("journeysAvailable")
                or data.get("journeys")
                or data.get("flights")
                or data.get("outboundFlights")
                or []
            )
            for journey in journeys:
                offer = self._parse_api_journey(journey, req, booking_url)
                if offer:
                    offers.append(offer)

        return offers

    def _parse_api_journey(
        self, journey: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        """Parse a single journey from the Navitaire API response."""
        # Extract price from fares
        price = self._extract_api_fare_price(journey)
        if price is None or price <= 0:
            # Try bundle-data style pricing
            price = self._extract_bundle_price(journey)
        if price is None or price <= 0:
            price = self._extract_best_price(journey)
        if price is None or price <= 0:
            return None

        designator = journey.get("designator", {})
        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []

        for seg in segments_raw:
            seg_des = seg.get("designator", {})
            identifier = seg.get("identifier", {})
            carrier = identifier.get("carrierCode", "JQ")
            flight_no = str(identifier.get("identifier", ""))

            seg_obj = FlightSegment(
                airline=carrier,
                airline_name="Jetstar",
                flight_no=flight_no,
                origin=seg_des.get("origin", req.origin),
                destination=seg_des.get("destination", req.destination),
                departure=self._parse_dt(seg_des.get("departure", "")),
                arrival=self._parse_dt(seg_des.get("arrival", "")),
                cabin_class="M",
            )
            segments.append(seg_obj)

        if not segments:
            # Build segment from designator
            dep_str = designator.get("departure", "")
            arr_str = designator.get("arrival", "")
            origin = designator.get("origin", req.origin)
            destination = designator.get("destination", req.destination)
            segments.append(
                FlightSegment(
                    airline="JQ",
                    airline_name="Jetstar",
                    flight_no="",
                    origin=origin,
                    destination=destination,
                    departure=self._parse_dt(dep_str),
                    arrival=self._parse_dt(arr_str),
                    cabin_class="M",
                )
            )

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        journey_key = (
            journey.get("journeyKey")
            or journey.get("JourneySellKey")
            or journey.get("journeySellKey")
            or f"{req.origin}_{req.destination}_{time.monotonic()}"
        )
        currency = req.currency if req.currency and req.currency != "EUR" else "AUD"

        return FlightOffer(
            id=f"jq_{hashlib.md5(str(journey_key).encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"${price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Jetstar"],
            owner_airline="JQ",
            booking_url=booking_url,
            is_locked=False,
            source="jetstar_api",
            source_tier="free",
        )

    @staticmethod
    def _extract_api_fare_price(journey: dict) -> Optional[float]:
        """Extract price from Navitaire dotREZ fares structure."""
        fares = journey.get("fares", {})
        best = float("inf")

        if isinstance(fares, dict):
            for fare_key, fare_info in fares.items():
                if not isinstance(fare_info, dict):
                    continue
                # passengerFares[].fareAmount
                pax_fares = fare_info.get("passengerFares", [])
                for pf in pax_fares:
                    for key in ["fareAmount", "publishedFare", "amount", "serviceChargeTotal"]:
                        val = pf.get(key)
                        if val is not None:
                            try:
                                v = float(val)
                                if 0 < v < best:
                                    best = v
                            except (TypeError, ValueError):
                                pass
        elif isinstance(fares, list):
            for fare in fares:
                if not isinstance(fare, dict):
                    continue
                for key in ["price", "amount", "totalPrice", "fareAmount"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("value")
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass

        return best if best < float("inf") else None

    # ------------------------------------------------------------------
    # Fast path 2: SSR page fetch via curl_cffi
    # ------------------------------------------------------------------

    async def _try_ssr_fetch(
        self, req: FlightSearchRequest, t0: float
    ) -> Optional[FlightSearchResponse]:
        """Fetch booking page via curl_cffi, extract bundle-data-v2 from HTML."""
        loop = asyncio.get_event_loop()
        html = await loop.run_in_executor(None, self._ssr_fetch_sync, req)
        if not html:
            return None

        # Extract bundle-data-v2 JSON from HTML
        flight_data = self._extract_bundle_from_html(html)
        if flight_data:
            offers = self._parse_bundle_data_v2(flight_data, req)
            if offers:
                elapsed = time.monotonic() - t0
                logger.info(
                    "Jetstar %s→%s returned %d offers in %.1fs (SSR fetch)",
                    req.origin, req.destination, len(offers), elapsed,
                )
                return self._build_response(offers, req, elapsed)

        # Try Navitaire JSON from inline scripts
        flight_data = self._extract_flight_json_from_html(html)
        if flight_data:
            offers = self._parse_navitaire_data(flight_data, req)
            if offers:
                elapsed = time.monotonic() - t0
                logger.info(
                    "Jetstar %s→%s returned %d offers in %.1fs (SSR inline JSON)",
                    req.origin, req.destination, len(offers), elapsed,
                )
                return self._build_response(offers, req, elapsed)

        return None

    def _ssr_fetch_sync(self, req: FlightSearchRequest) -> Optional[str]:
        """Fetch the booking page HTML via curl_cffi."""
        booking_url = self._build_booking_url(req)
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)
        try:
            r = sess.get(
                booking_url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-AU,en;q=0.9",
                    "User-Agent": _UA,
                },
                timeout=20,
                allow_redirects=True,
            )
            if r.status_code == 200 and len(r.text) > 1000:
                logger.debug("Jetstar: SSR fetch got %d bytes", len(r.text))
                return r.text
            logger.debug("Jetstar: SSR fetch returned %d (%d bytes)", r.status_code, len(r.text))
        except Exception as e:
            logger.debug("Jetstar: SSR fetch error: %s", e)
        return None

    @staticmethod
    def _extract_bundle_from_html(html: str) -> Optional[dict]:
        """Extract bundle-data-v2 JSON from raw HTML string."""
        # Look for <script id="bundle-data-v2" type="application/json">...</script>
        m = re.search(
            r'<script\s+id=["\']bundle-data(?:-v2)?["\']\s+type=["\']application/json["\']>\s*(.*?)\s*</script>',
            html,
            re.DOTALL,
        )
        if not m:
            # Broader fallback
            m = re.search(
                r'<script\s+id=["\']bundle-data[^"\']*["\'][^>]*>\s*(.*?)\s*</script>',
                html,
                re.DOTALL,
            )
        if m:
            try:
                return json.loads(m.group(1))
            except (json.JSONDecodeError, ValueError):
                pass
        return None

    @staticmethod
    def _extract_flight_json_from_html(html: str) -> Optional[dict]:
        """Extract Navitaire FlightData or availability JSON from inline scripts."""
        import html as html_mod
        # FlightData = '...';
        m = re.search(r"FlightData\s*=\s*'([\s\S]*?)';", html)
        if m:
            try:
                return json.loads(html_mod.unescape(m.group(1)))
            except (json.JSONDecodeError, ValueError):
                pass
        # availability = {...};
        m = re.search(r"(?:availability|AvailabilityV2|flightSearch)\s*=\s*(\{[\s\S]*?\});", html)
        if m:
            try:
                return json.loads(m.group(1))
            except (json.JSONDecodeError, ValueError):
                pass
        return None

    # ------------------------------------------------------------------
    # Playwright fallback (original browser approach, demoted)
    # ------------------------------------------------------------------

    async def _attempt_search(
        self, url: str, req: FlightSearchRequest
    ) -> Optional[list[FlightOffer]]:
        browser = await _get_browser()

        # CDP browsers use default context — don't call new_context()
        is_cdp = hasattr(browser, 'contexts') and browser.contexts
        if is_cdp:
            context = browser.contexts[0]
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

            logger.info("Jetstar: loading %s", url[:120])
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(4)

            # Handle deeplink redirect page ("Continue to booking" button)
            await self._handle_deeplink_redirect(page)

            title = await page.title()
            if "not found" in title.lower() or "error" in title.lower():
                logger.warning("Jetstar: got error page: %s", title)
                return None

            # Dismiss privacy notices and "Multiple booking" overlay
            await self._dismiss_overlays(page)

            # Wait for flight results (bundle-data-v2 script or flight cards)
            try:
                await page.wait_for_selector(
                    "script#bundle-data-v2, [class*='flight-row'], "
                    "div[aria-label*='Departure'], div[aria-label*='price']",
                    timeout=20000,
                )
            except Exception:
                pass
            await asyncio.sleep(2)

            # Strategy 1: Extract bundle-data-v2 JSON (full structured flight data)
            flight_data = await self._extract_bundle_data_v2(page)
            if flight_data:
                offers = self._parse_bundle_data_v2(flight_data, req)
                if offers:
                    logger.info("Jetstar: extracted %d offers from bundle-data-v2", len(offers))
                    return offers

            # Strategy 2: Try Navitaire FlightData JSON from inline scripts
            flight_data = await self._extract_flight_data(page)
            if flight_data:
                offers = self._parse_navitaire_data(flight_data, req)
                if offers:
                    return offers

            # Strategy 3: DOM extraction from flight cards
            offers = await self._extract_from_dom(page, req)
            if offers:
                return offers

            return None
        finally:
            await page.close()
            if not is_cdp:
                await context.close()

    async def _handle_deeplink_redirect(self, page) -> None:
        """Handle the deeplinksv2 interim page that shows 'Continue to booking'."""
        try:
            current_url = page.url
            if "deeplink" in current_url.lower() or "continue" in (await page.title()).lower():
                # Click "Continue to booking" or similar button
                for selector in [
                    "button:has-text('Continue')",
                    "a:has-text('Continue to booking')",
                    "a:has-text('Continue')",
                    "button[type='submit']",
                ]:
                    try:
                        btn = page.locator(selector).first
                        if await btn.count() > 0 and await btn.is_visible():
                            await btn.click(timeout=5000)
                            await page.wait_for_load_state("domcontentloaded", timeout=15000)
                            await asyncio.sleep(3)
                            logger.info("Jetstar: clicked through deeplink redirect")
                            return
                    except Exception:
                        continue
        except Exception:
            pass

    async def _dismiss_overlays(self, page) -> None:
        """Dismiss privacy notice, cookie banners, and 'Multiple booking' overlay."""
        # Click close buttons on banners and overlays
        for selector in [
            "img[alt*='close']",
            "[class*='privacy'] img[role='button']",
            "[class*='notice'] button",
            "[class*='banner'] button",
            "button:has-text('OK')",
            "button:has-text('Got it')",
            "button:has-text('No thanks')",
            "button:has-text('Continue')",
            "[class*='modal'] button[class*='close']",
            "[class*='multiple-booking'] button",
        ]:
            try:
                el = page.locator(selector).first
                if await el.count() > 0 and await el.is_visible():
                    await el.click(timeout=2000)
                    await asyncio.sleep(0.3)
            except Exception:
                continue

        # JS-remove overlays that intercept pointer events
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="gdpr"], [class*="consent"], [class*="cookie"], [class*="onetrust"], ' +
                    '[class*="privacy-notice"], [class*="modal-overlay"], [class*="popup"], ' +
                    '[class*="multiple-booking"], [class*="overlay-backdrop"]'
                ).forEach(el => { if (el.offsetHeight > 0 && el.offsetHeight < 400) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _extract_bundle_data_v2(self, page) -> Optional[dict]:
        """Extract bundle-data-v2 JSON — full structured flight data from Navitaire.

        The Jetstar select-flights page embeds a ~327KB JSON blob in:
        <script id="bundle-data-v2" type="application/json">{...}</script>

        Structure: { Trips: [{ Flights: [...] }], ... }
        """
        raw_json = await page.evaluate(r"""() => {
            const el = document.querySelector('script#bundle-data-v2');
            if (el) return el.textContent;
            // Fallback: look for bundle-data (v1)
            const el2 = document.querySelector('script#bundle-data');
            if (el2) return el2.textContent;
            return null;
        }""")
        if not raw_json:
            return None
        try:
            data = json.loads(raw_json)
            logger.debug("Jetstar: bundle-data-v2 extracted (%d bytes)", len(raw_json))
            return data
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug("Jetstar: bundle-data-v2 parse error: %s", e)
            return None

    async def _extract_flight_data(self, page) -> Optional[dict]:
        """Fallback: try to extract Navitaire FlightData JSON from inline <script> tags."""
        import html as html_mod

        raw = await page.evaluate(r"""() => {
            const scripts = document.querySelectorAll('script');
            for (const s of scripts) {
                const t = s.textContent || '';
                const m = t.match(/FlightData\s*=\s*'([\s\S]*?)';/);
                if (m) return { type: 'flightdata', data: m[1] };
                const m2 = t.match(/(?:availability|AvailabilityV2|flightSearch)\s*=\s*({[\s\S]*?});/);
                if (m2) return { type: 'json', data: m2[1] };
            }
            if (window.FlightData) return { type: 'global', data: JSON.stringify(window.FlightData) };
            if (window.AvailabilityV2) return { type: 'global', data: JSON.stringify(window.AvailabilityV2) };
            return null;
        }""")
        if not raw:
            return None

        data_str = raw.get("data", "")
        if raw.get("type") == "flightdata":
            data_str = html_mod.unescape(data_str)

        try:
            return json.loads(data_str)
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug("Jetstar: FlightData parse error: %s", e)
            return None

    def _parse_bundle_data_v2(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse bundle-data-v2 JSON from Jetstar's Navitaire booking engine.

        Structure:
          { Trips: [{ Flights: [{ JourneySellKey, Bundles, Segments, ... }] }] }

        JourneySellKey format:
          "JQ~ 501~ ~~SYD~04/15/2026 06:00~MEL~04/15/2026 07:40~~"

        Bundles[].RegularInclusiveAmount = regular price
        Bundles[].CjInclusiveAmount = Club Jetstar (member) price
        """
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        trips = data.get("Trips") or data.get("trips") or []
        for trip in trips:
            flights = trip.get("Flights") or trip.get("flights") or []
            for flight in flights:
                offer = self._parse_bundle_flight(flight, req, booking_url)
                if offer:
                    offers.append(offer)

        return offers

    def _parse_bundle_flight(
        self, flight: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        """Parse a single flight from bundle-data-v2 Trips[].Flights[]."""
        # Extract price from Bundles — prefer RegularInclusiveAmount (non-member)
        price = self._extract_bundle_price(flight)
        if price is None or price <= 0:
            return None

        # Parse JourneySellKey for flight number, airports, times
        sell_key = flight.get("JourneySellKey") or flight.get("journeySellKey") or ""
        key_info = self._parse_journey_sell_key(sell_key)

        # Build segments from Segments array or from sell key
        segments_raw = flight.get("Segments") or flight.get("segments") or []
        segments: list[FlightSegment] = []

        if segments_raw:
            for seg_raw in segments_raw:
                legs = seg_raw.get("Legs") or seg_raw.get("legs") or [seg_raw]
                for leg in legs:
                    seg = self._build_bundle_segment(leg, req)
                    if seg:
                        segments.append(seg)

        # Fallback: build segment from sell key info
        if not segments and key_info:
            seg = FlightSegment(
                airline=key_info.get("carrier", "JQ"),
                airline_name="Jetstar",
                flight_no=key_info.get("flight_no", ""),
                origin=key_info.get("origin", req.origin),
                destination=key_info.get("destination", req.destination),
                departure=key_info.get("departure", datetime(2000, 1, 1)),
                arrival=key_info.get("arrival", datetime(2000, 1, 1)),
                cabin_class="M",
            )
            segments.append(seg)

        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = sell_key or f"{req.origin}_{req.destination}_{time.monotonic()}"
        return FlightOffer(
            id=f"jq_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
            price=round(price, 2), currency="AUD",
            price_formatted=f"${price:.2f} AUD",
            outbound=route, inbound=None,
            airlines=["Jetstar"], owner_airline="JQ",
            booking_url=booking_url, is_locked=False,
            source="jetstar_direct", source_tier="free",
        )

    @staticmethod
    def _extract_bundle_price(flight: dict) -> Optional[float]:
        """Extract best price from Bundles[].RegularInclusiveAmount."""
        bundles = flight.get("Bundles") or flight.get("bundles") or []
        best = float("inf")
        for bundle in bundles:
            # Regular (non-member) price
            for key in ["RegularInclusiveAmount", "regularInclusiveAmount",
                        "InclusiveAmount", "inclusiveAmount",
                        "CjInclusiveAmount", "cjInclusiveAmount"]:
                val = bundle.get(key)
                if val is not None:
                    try:
                        v = float(val)
                        if 0 < v < best:
                            best = v
                    except (TypeError, ValueError):
                        pass
            # Nested price object
            price_obj = bundle.get("Price") or bundle.get("price") or {}
            if isinstance(price_obj, dict):
                for key in ["Amount", "amount", "Value", "value"]:
                    val = price_obj.get(key)
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        return best if best < float("inf") else None

    @staticmethod
    def _parse_journey_sell_key(sell_key: str) -> Optional[dict]:
        """Parse JourneySellKey: 'JQ~ 501~ ~~SYD~04/15/2026 06:00~MEL~04/15/2026 07:40~~'"""
        if not sell_key or "~" not in sell_key:
            return None
        parts = sell_key.split("~")
        # parts[0] = carrier (JQ), parts[1] = flight number (space + 501)
        # parts[4] = origin (SYD), parts[5] = dep datetime (04/15/2026 06:00)
        # parts[6] = destination (MEL), parts[7] = arr datetime (04/15/2026 07:40)
        try:
            carrier = parts[0].strip() if len(parts) > 0 else "JQ"
            flight_no = parts[1].strip() if len(parts) > 1 else ""
            origin = parts[4].strip() if len(parts) > 4 else ""
            dep_str = parts[5].strip() if len(parts) > 5 else ""
            destination = parts[6].strip() if len(parts) > 6 else ""
            arr_str = parts[7].strip() if len(parts) > 7 else ""

            dep_dt = datetime.strptime(dep_str, "%m/%d/%Y %H:%M") if dep_str else datetime(2000, 1, 1)
            arr_dt = datetime.strptime(arr_str, "%m/%d/%Y %H:%M") if arr_str else datetime(2000, 1, 1)

            full_flight_no = f"{carrier}{flight_no}" if flight_no else ""

            return {
                "carrier": carrier,
                "flight_no": full_flight_no,
                "origin": origin,
                "destination": destination,
                "departure": dep_dt,
                "arrival": arr_dt,
            }
        except (ValueError, IndexError) as e:
            logger.debug("Jetstar: JourneySellKey parse error: %s", e)
            return None

    def _build_bundle_segment(self, leg: dict, req: FlightSearchRequest) -> Optional[FlightSegment]:
        """Build a FlightSegment from a bundle-data-v2 Segments[].Legs[] entry."""
        carrier = leg.get("CarrierCode") or leg.get("carrierCode") or leg.get("Carrier") or "JQ"
        flight_no_raw = leg.get("FlightNumber") or leg.get("flightNumber") or leg.get("FlightDesignator") or ""
        if isinstance(flight_no_raw, dict):
            flight_no = str(flight_no_raw.get("FlightNumber", ""))
            carrier = flight_no_raw.get("CarrierCode", carrier)
        else:
            flight_no = str(flight_no_raw)

        full_flight_no = f"{carrier}{flight_no}" if flight_no else ""

        origin = (leg.get("DepartureStation") or leg.get("departureStation")
                  or leg.get("Origin") or leg.get("origin") or req.origin)
        destination = (leg.get("ArrivalStation") or leg.get("arrivalStation")
                       or leg.get("Destination") or leg.get("destination") or req.destination)

        dep_str = (leg.get("STD") or leg.get("std") or leg.get("DepartureDateTime")
                   or leg.get("departureDateTime") or leg.get("DepartureDate") or "")
        arr_str = (leg.get("STA") or leg.get("sta") or leg.get("ArrivalDateTime")
                   or leg.get("arrivalDateTime") or leg.get("ArrivalDate") or "")

        return FlightSegment(
            airline=carrier,
            airline_name="Jetstar",
            flight_no=full_flight_no,
            origin=origin,
            destination=destination,
            departure=self._parse_dt(dep_str),
            arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _parse_navitaire_data(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: parse Navitaire-style FlightData JSON."""
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        journeys = data.get("journeys") or data.get("trips") or []
        for journey in journeys:
            flights = journey.get("flights") or journey.get("segments") or []
            for flight in flights:
                offer = self._parse_navitaire_flight(flight, req, booking_url)
                if offer:
                    offers.append(offer)

        if not offers:
            flights = data.get("flights") or data.get("outboundFlights") or []
            for flight in flights:
                offer = self._parse_navitaire_flight(flight, req, booking_url)
                if offer:
                    offers.append(offer)

        return offers

    def _parse_navitaire_flight(
        self, flight: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        price = self._extract_best_price(flight)
        if price is None or price <= 0:
            return None

        legs_raw = flight.get("legs") or flight.get("segments") or []
        segments: list[FlightSegment] = []
        for leg in legs_raw:
            segments.append(self._build_segment(leg, req.origin, req.destination))
        if not segments:
            segments.append(self._build_segment(flight, req.origin, req.destination))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("journeyKey") or flight.get("standardFareKey")
            or flight.get("id") or f"{req.origin}_{req.destination}_{time.monotonic()}"
        )
        return FlightOffer(
            id=f"jq_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency="AUD",
            price_formatted=f"${price:.2f} AUD",
            outbound=route, inbound=None,
            airlines=["Jetstar"], owner_airline="JQ",
            booking_url=booking_url, is_locked=False,
            source="jetstar_direct", source_tier="free",
        )

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight data from DOM flight cards on the select-flights page.
        
        Navitaire renders flight cards with:
        - Departure/arrival times (e.g. "6:00am", "7:40am")
        - Airport codes (e.g. "SYD - Departure", "MEL - Arrival")
        - Duration (e.g. "Direct flight - 1hr 40mins travel")
        - Prices (e.g. "Regular price from 83 AUD")
        """
        flights_data = await page.evaluate(r"""() => {
            const flights = [];
            
            // Find all flight row containers - they are clickable divs containing
            // departure/arrival info and price buttons
            const allButtons = document.querySelectorAll('button[aria-label]');
            const priceButtons = [];
            for (const btn of allButtons) {
                const label = btn.getAttribute('aria-label') || '';
                if (label.includes('AUD') && label.includes('price')) {
                    priceButtons.push(btn);
                }
            }
            
            for (const priceBtn of priceButtons) {
                // Walk up to find the flight row container
                let row = priceBtn.closest('[class]');
                // Keep walking up until we find a container with time info
                for (let i = 0; i < 10 && row; i++) {
                    const text = row.textContent || '';
                    if (/\d{1,2}:\d{2}(am|pm)/i.test(text) && text.includes('Departure') && text.includes('Arrival')) {
                        break;
                    }
                    row = row.parentElement;
                }
                if (!row) continue;
                
                const text = row.textContent || '';
                
                // Extract times: "6:00am" pattern
                const timeMatches = text.match(/(\d{1,2}:\d{2}(?:am|pm))/gi);
                if (!timeMatches || timeMatches.length < 2) continue;
                
                const depTime = timeMatches[0];
                const arrTime = timeMatches[1];
                
                // Extract airport codes: "SYD - Departure" / "MEL - Arrival" 
                const depAirportMatch = text.match(/([A-Z]{3})\s*-\s*Departure/);
                const arrAirportMatch = text.match(/([A-Z]{3})\s*-\s*Arrival/);
                const depAirport = depAirportMatch ? depAirportMatch[1] : '';
                const arrAirport = arrAirportMatch ? arrAirportMatch[1] : '';
                
                // Extract duration: "1hr 40mins" or "Direct flight - 1hr 40mins travel"
                const durMatch = text.match(/(\d+)hr\s*(\d+)min/i);
                const durationMins = durMatch ? parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]) : 0;
                
                // Direct vs stops
                const isDirect = /direct\s*flight/i.test(text);
                const stopsMatch = text.match(/(\d+)\s*stop/i);
                const stops = isDirect ? 0 : (stopsMatch ? parseInt(stopsMatch[1]) : 0);
                
                // Extract prices from the button aria-label
                // Format: "Club Jetstar price from 76 AUD Regular price from 83 AUD"
                // or: "1 left at this price Club Jetstar price from 96 AUD Regular price from 103 AUD"
                const label = priceBtn.getAttribute('aria-label') || '';
                const regularPriceMatch = label.match(/Regular\s+price\s+from\s+(\d+(?:\.\d+)?)\s+AUD/i);
                const clubPriceMatch = label.match(/Club\s+Jetstar\s+price\s+from\s+(\d+(?:\.\d+)?)\s+AUD/i);
                
                // Use regular price (non-member price)
                const price = regularPriceMatch ? parseFloat(regularPriceMatch[1]) :
                              (clubPriceMatch ? parseFloat(clubPriceMatch[1]) : 0);
                
                if (price <= 0) continue;
                
                // Dedup check: don't add same dep+arr time twice
                const key = depTime + '_' + arrTime;
                if (flights.some(f => f.key === key)) continue;
                
                flights.push({
                    key: key,
                    depTime: depTime,
                    arrTime: arrTime,
                    depAirport: depAirport,
                    arrAirport: arrAirport,
                    durationMins: durationMins,
                    stops: stops,
                    price: price,
                    currency: 'AUD'
                });
            }
            
            return flights;
        }""")

        if not flights_data:
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        dep_date = req.date_from

        for fd in flights_data:
            try:
                dep_dt = self._parse_time_ampm(fd["depTime"], dep_date)
                arr_dt = self._parse_time_ampm(fd["arrTime"], dep_date)
                # Handle overnight: if arrival is before departure, add a day
                if arr_dt < dep_dt:
                    from datetime import timedelta
                    arr_dt = arr_dt + timedelta(days=1)

                origin = fd.get("depAirport") or req.origin
                destination = fd.get("arrAirport") or req.destination

                seg = FlightSegment(
                    airline="JQ", airline_name="Jetstar",
                    flight_no="",
                    origin=origin, destination=destination,
                    departure=dep_dt, arrival=arr_dt,
                    cabin_class="M",
                )
                dur = fd.get("durationMins", 0) * 60
                if dur == 0 and dep_dt and arr_dt:
                    dur = int((arr_dt - dep_dt).total_seconds())

                route = FlightRoute(
                    segments=[seg],
                    total_duration_seconds=max(dur, 0),
                    stopovers=fd.get("stops", 0),
                )
                price = fd["price"]
                flight_key = f"{origin}_{destination}_{fd['depTime']}_{fd['arrTime']}"
                offers.append(FlightOffer(
                    id=f"jq_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
                    price=round(price, 2), currency="AUD",
                    price_formatted=f"${price:.2f} AUD",
                    outbound=route, inbound=None,
                    airlines=["Jetstar"], owner_airline="JQ",
                    booking_url=booking_url, is_locked=False,
                    source="jetstar_direct", source_tier="free",
                ))
            except Exception as e:
                logger.debug("Jetstar: DOM flight parse error: %s", e)
                continue

        return offers

    @staticmethod
    def _parse_time_ampm(time_str: str, base_date: datetime) -> datetime:
        """Parse '6:00am' / '7:40pm' into datetime on the given date."""
        m = re.match(r"(\d{1,2}):(\d{2})(am|pm)", time_str, re.IGNORECASE)
        if not m:
            return datetime(2000, 1, 1)
        hour = int(m.group(1))
        minute = int(m.group(2))
        ampm = m.group(3).lower()
        if ampm == "pm" and hour != 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
        return datetime(base_date.year, base_date.month, base_date.day, hour, minute)

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareProducts") or flight.get("bundles") or []
        best = float("inf")
        for fare in fares:
            if isinstance(fare, dict):
                for key in ["price", "amount", "totalPrice", "basePrice", "fareAmount", "standardFare"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("value")
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        for key in ["price", "lowestFare", "totalPrice", "farePrice", "amount", "standardFare"]:
            p = flight.get(key)
            if p is not None:
                try:
                    v = float(p) if not isinstance(p, dict) else float(p.get("amount", 0))
                    if 0 < v < best:
                        best = v
                except (TypeError, ValueError):
                    pass
        return best if best < float("inf") else None

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = seg.get("departureDateTime") or seg.get("departure") or seg.get("departureDate") or seg.get("std") or ""
        arr_str = seg.get("arrivalDateTime") or seg.get("arrival") or seg.get("arrivalDate") or seg.get("sta") or ""
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("number") or "").replace(" ", "")
        origin = seg.get("origin") or seg.get("departureStation") or seg.get("departureAirport") or default_origin
        destination = seg.get("destination") or seg.get("arrivalStation") or seg.get("arrivalAirport") or default_dest
        carrier = seg.get("carrierCode") or seg.get("carrier") or seg.get("airline") or "JQ"
        return FlightSegment(
            airline=carrier, airline_name="Jetstar", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        h = hashlib.md5(f"jetstar{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="AUD", offers=offers, total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1
        return (
            f"https://booking.jetstar.com/au/en/booking/search-flights"
            f"?origin1={req.origin}&destination1={req.destination}"
            f"&departuredate1={dep}&ADT={adults}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"jetstar{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="AUD", offers=[], total_results=0,
        )
