"""
Jet2 hybrid scraper — cookie-farm + curl_cffi direct page fetch.

Jet2 (IATA: LS) is a British low-cost leisure airline operating from
14 UK airports to 75+ destinations across Europe and beyond.

Strategy (hybrid cookie-farm, updated Mar 2026):
  1. ONCE per ~25 min: Playwright opens jet2.com homepage, accepts cookies.
     This generates valid Akamai WAF cookies. Extract all cookies via context.cookies().
  2. For each search: curl_cffi fetches two endpoints with farmed cookies:
     a) /client/api/search-panels/flight-schedules/outbound?departures={iata}&arrivals={iata}
        → Returns which dates have flights (no prices)
     b) /en/cheap-flights/{origin-slug}/{dest-slug}?from=YYYY-MM-DD&...
        → HTML page with embedded £ prices in calendar cells
  3. Parse prices from HTML via regex + optional schedule API cross-check

  Result: ~1-3s per search instead of ~20-30s with full Playwright.

  URL routing: /en/cheap-flights/{origin-slug}/{dest-slug}?from=YYYY-MM-DD&to=...&adults=N
  Cookie banner: OneTrust ("Accept All Cookies")
  Anti-bot: Akamai Bot Manager — cookies farmed via headed Playwright
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from datetime import date, datetime
from typing import Any, Optional

from curl_cffi import requests as cffi_requests

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
_IMPERSONATE = "chrome124"
_COOKIE_MAX_AGE = 25 * 60  # Re-farm cookies after 25 minutes

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-IE"]
_TIMEZONES = [
    "Europe/London", "Europe/Dublin", "Europe/Berlin",
    "Europe/Paris", "Europe/Madrid",
]

# ── Persistent headed browser context ─────────────────────────────────────
import os as _os
_USER_DATA_DIR = _os.path.join(_os.environ.get("TEMP", "/tmp"), "jet2_pw_data")
_pw_instance = None
_pw_context = None
_context_lock: Optional[asyncio.Lock] = None

# ── Cookie farm state ─────────────────────────────────────────────────────
_farm_lock: Optional[asyncio.Lock] = None
_farmed_cookies: list[dict] = []
_farm_timestamp: float = 0.0

# ── Airport slug cache (IATA → slug, populated from allairportinformation) ─
_airport_slug_cache: dict[str, str] = {}
_slug_cache_lock: Optional[asyncio.Lock] = None

# ── Hardcoded IATA → Jet2 URL slug mapping (fallback) ─────────────────────
# Jet2 URLs use city/island names, not IATA codes.
# UK departure airports
_STATIC_SLUGS: dict[str, str] = {
    "MAN": "manchester", "LBA": "leeds-bradford", "EMA": "east-midlands",
    "BHX": "birmingham", "NCL": "newcastle", "EDI": "edinburgh",
    "GLA": "glasgow", "BFS": "belfast-international", "BRS": "bristol",
    "STN": "london-stansted", "EXT": "exeter",
    # Popular holiday destinations
    "PMI": "majorca", "TFS": "tenerife", "LPA": "gran-canaria",
    "AGP": "malaga", "ALC": "alicante", "FAO": "faro",
    "IBZ": "ibiza", "MAH": "menorca", "FUE": "fuerteventura",
    "ACE": "lanzarote", "HER": "crete-heraklion", "RHO": "rhodes",
    "CFU": "corfu", "ZTH": "zante", "DLM": "dalaman",
    "AYT": "antalya", "BJV": "bodrum", "PFO": "paphos",
    "LCA": "larnaca", "SKG": "thessaloniki", "CHQ": "crete-chania",
    "KGS": "kos", "JSI": "skiathos", "SPU": "split",
    "DBV": "dubrovnik", "MJT": "lesvos", "JMK": "mykonos",
    "JTR": "santorini", "SPC": "la-palma", "VRN": "verona",
    "NAP": "naples", "PSA": "pisa", "BRI": "bari",
    "OLB": "sardinia", "CTA": "catania", "GRO": "girona",
    "REU": "reus", "BUD": "budapest", "KRK": "krakow",
    "GDN": "gdansk", "PRG": "prague", "RAK": "marrakech",
    "SSH": "sharm-el-sheikh", "HRG": "hurghada", "TIV": "tivat",
}


def _get_lock() -> asyncio.Lock:
    global _context_lock
    if _context_lock is None:
        _context_lock = asyncio.Lock()
    return _context_lock


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


def _get_slug_lock() -> asyncio.Lock:
    global _slug_cache_lock
    if _slug_cache_lock is None:
        _slug_cache_lock = asyncio.Lock()
    return _slug_cache_lock


async def _get_context():
    """Persistent headed Chrome context — off-screen to bypass Akamai headless detection."""
    global _pw_instance, _pw_context
    lock = _get_lock()
    async with lock:
        if _pw_context:
            try:
                _pw_context.pages
                return _pw_context
            except Exception:
                _pw_context = None

        from playwright.async_api import async_playwright

        _os.makedirs(_USER_DATA_DIR, exist_ok=True)
        _pw_instance = await async_playwright().start()

        _pw_context = await _pw_instance.chromium.launch_persistent_context(
            _USER_DATA_DIR,
            channel="chrome",
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
            ],
            viewport={"width": 1366, "height": 768},
            locale="en-GB",
            timezone_id="Europe/London",
            service_workers="block",
        )
        logger.info("Jet2: persistent headed Chrome context ready")
        return _pw_context


async def _reset_context():
    """Close and reset context (used when PX blocks the session)."""
    global _pw_instance, _pw_context
    lock = _get_lock()
    async with lock:
        if _pw_context:
            try:
                await _pw_context.close()
            except Exception:
                pass
            _pw_context = None
        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass
            _pw_instance = None


class Jet2ConnectorClient:
    """Jet2 hybrid scraper — cookie-farm + curl_cffi direct fetch."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Jet2 flights via cookie-farm + curl_cffi.

        Fast path (~1-3s): curl_cffi fetches the cheap-flights page with farmed cookies.
        Slow path (~15s): Playwright farms cookies first, then curl_cffi.
        Fallback: Full Playwright interception if API/HTML fails.
        """
        t0 = time.monotonic()

        try:
            cookies = await self._ensure_cookies()
            if not cookies:
                logger.warning("Jet2: cookie farm failed, falling back to Playwright")
                return await self._playwright_fallback(req, t0)

            offers = await self._api_search(req, cookies)

            # If failed (expired cookies), re-farm once and retry
            if offers is None:
                logger.info("Jet2: API search failed, re-farming cookies")
                cookies = await self._farm_cookies()
                if cookies:
                    offers = await self._api_search(req, cookies)

            # If API still fails, fall back to full Playwright
            if offers is None:
                logger.warning("Jet2: API search returned no data, falling back to Playwright")
                return await self._playwright_fallback(req, t0)

            elapsed = time.monotonic() - t0
            if offers:
                offers.sort(key=lambda o: o.price)

            logger.info(
                "Jet2 %s→%s returned %d offers in %.1fs (hybrid API)",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"jet2{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else "GBP",
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.error("Jet2: hybrid search error: %s", e)
            return await self._playwright_fallback(req, t0)

    # ------------------------------------------------------------------
    # Cookie farm
    # ------------------------------------------------------------------

    async def _ensure_cookies(self) -> list[dict]:
        """Return cached cookies or farm fresh ones."""
        global _farmed_cookies, _farm_timestamp
        if _farmed_cookies and (time.monotonic() - _farm_timestamp) < _COOKIE_MAX_AGE:
            return _farmed_cookies
        return await self._farm_cookies()

    async def _farm_cookies(self) -> list[dict]:
        """Load Jet2 homepage in persistent context to farm Akamai cookies."""
        global _farmed_cookies, _farm_timestamp
        lock = _get_farm_lock()
        async with lock:
            # Double-check after acquiring lock
            if _farmed_cookies and (time.monotonic() - _farm_timestamp) < _COOKIE_MAX_AGE:
                return _farmed_cookies

            ctx = await _get_context()
            page = None
            try:
                page = await ctx.new_page()

                # Also capture airport info API during homepage load
                async def on_response(response):
                    try:
                        if "allairportinformation" in response.url.lower() and response.status == 200:
                            ct = response.headers.get("content-type", "")
                            if "json" in ct:
                                data = await response.json()
                                if data:
                                    self._update_slug_cache(data)
                    except Exception:
                        pass

                page.on("response", on_response)

                logger.info("Jet2: farming cookies via homepage load")
                await page.goto(
                    "https://www.jet2.com/",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                await asyncio.sleep(2.0)

                # Dismiss OneTrust cookie banner
                await self._dismiss_overlays(page)
                await asyncio.sleep(1.5)

                # Extract cookies from persistent context
                cookies = await ctx.cookies()
                if cookies:
                    _farmed_cookies = cookies
                    _farm_timestamp = time.monotonic()
                    logger.info("Jet2: farmed %d cookies", len(cookies))
                    return cookies
                return []
            except Exception as e:
                logger.error("Jet2: cookie farm error: %s", e)
                return []
            finally:
                if page:
                    try:
                        await page.close()
                    except Exception:
                        pass

    # ------------------------------------------------------------------
    # Direct API via curl_cffi
    # ------------------------------------------------------------------

    async def _api_search(
        self, req: FlightSearchRequest, cookies: list[dict],
    ) -> Optional[list[FlightOffer]]:
        """Fetch Jet2 cheap-flights page via curl_cffi and extract prices."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._api_search_sync, req, cookies)

    def _api_search_sync(
        self, req: FlightSearchRequest, cookies: list[dict],
    ) -> Optional[list[FlightOffer]]:
        """Synchronous curl_cffi: fetch cheap-flights HTML page + extract prices."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)

        # Load farmed cookies
        for c in cookies:
            domain = c.get("domain", "")
            sess.cookies.set(c["name"], c["value"], domain=domain)

        # Resolve slugs
        origin_slug = self._resolve_slug_sync(req.origin)
        dest_slug = self._resolve_slug_sync(req.destination)
        if not origin_slug or not dest_slug:
            logger.warning("Jet2: could not resolve slugs for %s→%s", req.origin, req.destination)
            return None

        dep = req.date_from.strftime("%Y-%m-%d")
        children_param = f"children={req.children}" if req.children > 0 else "children"
        search_url = (
            f"https://www.jet2.com/en/cheap-flights/{origin_slug}/{dest_slug}"
            f"?from={dep}&to={dep}"
            f"&adults={req.adults}&{children_param}&infants={req.infants}"
            f"&preselect=false"
        )

        try:
            r = sess.get(
                search_url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-GB,en;q=0.9",
                    "Referer": "https://www.jet2.com/",
                },
                timeout=15,
            )
        except Exception as e:
            logger.error("Jet2: curl_cffi request failed: %s", e)
            return None

        if r.status_code != 200:
            logger.warning("Jet2: cheap-flights page returned %d", r.status_code)
            return None

        # Extract prices from HTML
        html = r.text
        if "Access Denied" in html or "PerimeterX" in html:
            logger.warning("Jet2: Akamai/PX block on cheap-flights page")
            return None

        offers = self._parse_html_prices(html, req)
        if offers is not None:
            return offers

        # Try schedule API as extra signal (no prices, just availability)
        try:
            r2 = sess.get(
                f"https://www.jet2.com/client/api/search-panels/flight-schedules/outbound"
                f"?departures={req.origin.lower()}&arrivals={req.destination.lower()}&xmsversion=2",
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.jet2.com/",
                },
                timeout=10,
            )
            if r2.status_code == 200:
                ct = r2.headers.get("content-type", "")
                if "json" in ct:
                    sched = r2.json()
                    year_str = str(req.date_from.year)
                    month_str = str(req.date_from.month)
                    day_str = str(req.date_from.day)
                    year_data = sched.get(year_str, {})
                    month_data = year_data.get(month_str, {})
                    days = month_data.get("days", {})
                    if day_str not in days:
                        logger.info("Jet2: schedule API says no flight on %s", req.date_from)
                        return []  # Confirmed: no flights
        except Exception:
            pass

        return None  # signal to fall back

    def _parse_html_prices(
        self, html: str, req: FlightSearchRequest,
    ) -> Optional[list[FlightOffer]]:
        """Parse £ prices from the Jet2 cheap-flights calendar HTML."""
        # Find all £XX or £XX.XX prices in the page
        all_prices = re.findall(r'£(\d+(?:\.\d{2})?)', html)
        if not all_prices:
            return None

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        seen_prices: set[float] = set()

        for price_str in all_prices:
            price = float(price_str)
            if price <= 0 or price in seen_prices:
                continue
            seen_prices.add(price)

            dep_dt = datetime(req.date_from.year, req.date_from.month, req.date_from.day, 0, 0)
            segment = FlightSegment(
                airline="LS", airline_name="Jet2",
                flight_no="",
                origin=req.origin, destination=req.destination,
                departure=dep_dt, arrival=dep_dt,
                cabin_class="M",
            )
            route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
            offer_id = f"ls_{hashlib.md5(f'api_{req.date_from}_{price}'.encode()).hexdigest()[:12]}"
            offers.append(FlightOffer(
                id=offer_id,
                price=round(price, 2),
                currency="GBP",
                price_formatted=f"£{price:.2f}",
                outbound=route,
                inbound=None,
                airlines=["Jet2"],
                owner_airline="LS",
                booking_url=booking_url,
                is_locked=False,
                source="jet2_direct",
                source_tier="free",
            ))

        return offers if offers else None

    # ------------------------------------------------------------------
    # Playwright fallback (full browser flow, used if API fails)
    # ------------------------------------------------------------------

    async def _playwright_fallback(
        self, req: FlightSearchRequest, t0: float,
    ) -> FlightSearchResponse:
        """Full Playwright interception flow as fallback."""
        last_error = None
        for attempt in range(2):
            if attempt > 0:
                logger.info("Jet2: retry %d with fresh context", attempt)
                await _reset_context()
                await asyncio.sleep(3.0)
            try:
                return await self._do_search(req, t0)
            except Exception as e:
                last_error = e
                if "ERR_HTTP2" in str(e) or "ERR_CONNECTION" in str(e):
                    continue
                raise
        logger.error("Jet2: all attempts failed: %s", last_error)
        return self._empty(req)

    async def _do_search(self, req: FlightSearchRequest, t0: float) -> FlightSearchResponse:
        ctx = await _get_context()
        page = await ctx.new_page()

        try:

            # ── Set up API interception ────────────────────────────────
            captured: dict[str, Any] = {}
            schedule_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url
                    ct = response.headers.get("content-type", "")
                    if response.status != 200 or "json" not in ct:
                        return
                    url_lower = url.lower()

                    # Airport information API → build slug cache
                    if "allairportinformation" in url_lower:
                        data = await response.json()
                        if data:
                            captured["airports"] = data
                            self._update_slug_cache(data)

                    # Flight schedule API → calendar data
                    if "flight-schedules" in url_lower:
                        data = await response.json()
                        if data:
                            captured["schedules"] = data
                            schedule_event.set()

                    # Flight search results / GTM registration
                    if "flightsearchresults" in url_lower or "registerdefault" in url_lower:
                        data = await response.json()
                        if data:
                            captured["flight_details"] = data

                    # Generic flight/availability APIs
                    if any(k in url_lower for k in [
                        "availability", "/api/flights", "offers",
                        "air-bounds", "fares", "low-fare",
                    ]):
                        data = await response.json()
                        if data and isinstance(data, (dict, list)):
                            captured["generic"] = data
                            schedule_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            # ── Step 1: Navigate to homepage to establish session + get airport mapping
            logger.info("Jet2: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.jet2.com/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(2.0)

            # Dismiss cookie consent + PerimeterX overlays
            await self._dismiss_overlays(page)

            # Wait briefly for allairportinformation API
            await asyncio.sleep(1.0)

            # ── Step 2: Try direct API fetch for flight schedules (uses IATA codes)
            direct_data = await self._try_direct_api(page, req)
            if direct_data:
                captured["schedules"] = direct_data
                schedule_event.set()

            # ── Step 3: Resolve IATA → slug
            origin_slug = await self._resolve_slug(req.origin)
            dest_slug = await self._resolve_slug(req.destination)

            if not origin_slug or not dest_slug:
                logger.warning("Jet2: could not resolve slugs for %s→%s (got %s→%s)",
                               req.origin, req.destination, origin_slug, dest_slug)
                return self._empty(req)

            # ── Step 3: Navigate to search results page
            dep = req.date_from.strftime("%Y-%m-%d")
            # Jet2 URL format: children param has no value when 0 children
            children_param = f"children={req.children}" if req.children > 0 else "children"
            search_url = (
                f"https://www.jet2.com/en/cheap-flights/{origin_slug}/{dest_slug}"
                f"?from={dep}&to={dep}"
                f"&adults={req.adults}&{children_param}&infants={req.infants}"
                f"&preselect=false"
            )
            logger.info("Jet2: navigating to %s", search_url)
            await page.goto(
                search_url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(1.0)
            await self._dismiss_overlays(page)

            # ── Step 5: Wait for calendar prices to render in DOM
            try:
                await page.wait_for_selector(
                    "table td, [class*='calendar'], [class*='price']",
                    timeout=10000,
                )
                await asyncio.sleep(2.0)  # Extra time for prices to populate
            except Exception:
                logger.debug("Jet2: no calendar table found after results page load")
                await asyncio.sleep(3.0)

            await self._dismiss_overlays(page)  # Re-dismiss any new overlays

            # ── Step 6: Parse results
            elapsed = time.monotonic() - t0
            offers: list[FlightOffer] = []

            # Try flight_details first (most detailed)
            if "flight_details" in captured:
                offers = self._parse_flight_details(captured["flight_details"], req)

            # Try schedule data
            if not offers and "schedules" in captured:
                offers = self._parse_schedule_data(captured["schedules"], req)

            # Try generic API data
            if not offers and "generic" in captured:
                offers = self._parse_generic_api(captured["generic"], req)

            # Fallback: parse DOM calendar
            if not offers:
                offers = await self._parse_dom_calendar(page, req)

            if offers:
                return self._build_response(offers, req, elapsed)
            return self._empty(req)

        except Exception as e:
            # Let HTTP/2 and connection errors propagate for retry
            if "ERR_HTTP2" in str(e) or "ERR_CONNECTION" in str(e):
                raise
            logger.error("Jet2 Playwright error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ── Direct API fetch ─────────────────────────────────────────────

    async def _try_direct_api(self, page, req: FlightSearchRequest) -> Optional[Any]:
        """Try to fetch the flight-schedules API directly from the page context."""
        origin = req.origin.lower()
        dest = req.destination.lower()
        try:
            data = await page.evaluate("""async ({origin, dest}) => {
                try {
                    const url = `/client/api/search-panels/flight-schedules/outbound?departures=${origin}&arrivals=${dest}&xmsversion=2`;
                    const resp = await fetch(url, {
                        headers: { 'Accept': 'application/json' },
                        credentials: 'same-origin',
                    });
                    if (!resp.ok) return null;
                    const ct = resp.headers.get('content-type') || '';
                    if (!ct.includes('json')) return null;
                    return await resp.json();
                } catch { return null; }
            }""", {"origin": origin, "dest": dest})
            if data:
                logger.info("Jet2: direct API fetch succeeded for %s→%s", req.origin, req.destination)
            return data
        except Exception as e:
            logger.debug("Jet2: direct API fetch failed: %s", e)
            return None

    # ── Overlay / cookie dismissal ─────────────────────────────────────

    async def _dismiss_overlays(self, page) -> None:
        """Dismiss OneTrust cookie banner + PerimeterX captcha overlay."""
        # OneTrust cookie banner
        for selector in [
            "#onetrust-accept-btn-handler",
            "button[id*='accept']",
        ]:
            try:
                btn = page.locator(selector).first
                if await btn.count() > 0:
                    await btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    break
            except Exception:
                continue

        # Also try by text
        for label in ["Accept All Cookies", "Accept all cookies", "Accept", "I agree"]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    break
            except Exception:
                continue

        # Force-remove blocking overlays (OneTrust + PX)
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '#onetrust-consent-sdk, [class*="onetrust"], ' +
                    '#px-captcha-modal, iframe[id*="px-captcha"], ' +
                    '[class*="cookie-consent"], [class*="consent-overlay"]'
                ).forEach(el => el.remove());
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ── Airport slug resolution ────────────────────────────────────────

    def _update_slug_cache(self, data: Any) -> None:
        """Parse allairportinformation response and update IATA→slug cache."""
        global _airport_slug_cache
        if not data:
            return
        airports = data if isinstance(data, list) else data.get("airports", data.get("data", []))
        if not isinstance(airports, list):
            return
        for airport in airports:
            if not isinstance(airport, dict):
                continue
            iata = (
                airport.get("iataCode") or airport.get("code")
                or airport.get("airportCode") or airport.get("iata") or ""
            ).upper().strip()
            slug = (
                airport.get("seoUrl") or airport.get("slug")
                or airport.get("urlSlug") or airport.get("seoName")
                or airport.get("name", "")
            ).strip().lower().replace(" ", "-")
            if iata and slug:
                # Strip leading/trailing slashes
                slug = slug.strip("/")
                # Take last path segment if it's a full path
                if "/" in slug:
                    slug = slug.rsplit("/", 1)[-1]
                _airport_slug_cache[iata] = slug
        if _airport_slug_cache:
            logger.info("Jet2: cached %d airport slugs", len(_airport_slug_cache))

    async def _resolve_slug(self, iata: str) -> Optional[str]:
        """Resolve IATA code to Jet2 URL slug."""
        return self._resolve_slug_sync(iata)

    def _resolve_slug_sync(self, iata: str) -> Optional[str]:
        """Resolve IATA code to Jet2 URL slug (sync)."""
        iata = iata.upper().strip()
        # Dynamic cache from allairportinformation API
        if iata in _airport_slug_cache:
            return _airport_slug_cache[iata]
        # Static fallback mapping
        if iata in _STATIC_SLUGS:
            return _STATIC_SLUGS[iata]
        # Last resort: lowercase IATA (rarely works)
        return iata.lower()

    # ── Parse flight details (registerdefaultflightwithgtm) ────────────

    def _parse_flight_details(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the GTM registration response for detailed flight info."""
        if not isinstance(data, dict):
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Try various known structures
        for key in ["flights", "outboundFlights", "outbound", "results", "items"]:
            flights = data.get(key, [])
            if isinstance(flights, list) and flights:
                for f in flights:
                    offer = self._parse_single_flight(f, req, booking_url)
                    if offer:
                        offers.append(offer)
                if offers:
                    return offers

        # Single flight object
        offer = self._parse_single_flight(data, req, booking_url)
        if offer:
            return [offer]

        return []

    # ── Parse schedule data (flight-schedules endpoint) ────────────────

    def _parse_schedule_data(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the flight-schedules API response.
        
        Structure: {year: {month: {pof: bool, days: {day: {pof: bool}}}}}
        This only indicates availability (which days have flights), not prices.
        Returns empty — prices come from DOM calendar.
        We use this to validate the route has flights on the target date.
        """
        if not data or not isinstance(data, dict):
            return []

        year_str = str(req.date_from.year)
        month_str = str(req.date_from.month)
        day_str = str(req.date_from.day)

        year_data = data.get(year_str)
        if not isinstance(year_data, dict):
            logger.info("Jet2: no schedule data for year %s", year_str)
            return []

        month_data = year_data.get(month_str)
        if not isinstance(month_data, dict):
            logger.info("Jet2: no schedule data for %s/%s", year_str, month_str)
            return []

        days = month_data.get("days", {})
        if day_str in days:
            day_info = days[day_str]
            pof = day_info.get("pof", False) if isinstance(day_info, dict) else False
            logger.info("Jet2: flight available on %s-%s-%s (package_only=%s)", year_str, month_str, day_str, pof)
        else:
            logger.info("Jet2: no flight on target date %s-%s-%s", year_str, month_str, day_str)

        return []  # No prices in schedule data — DOM calendar needed

    # ── Parse generic API response ─────────────────────────────────────

    def _parse_generic_api(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback parser for any intercepted JSON with flight data."""
        if isinstance(data, list):
            data = {"flights": data}
        if not isinstance(data, dict):
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        outbound_raw = (
            data.get("outboundFlights") or data.get("outbound")
            or (data.get("journeys", {}).get("outbound") if isinstance(data.get("journeys"), dict) else None)
            or data.get("flights", [])
        )
        if not isinstance(outbound_raw, list):
            outbound_raw = []

        for flight in outbound_raw:
            offer = self._parse_single_flight(flight, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    # ── Parse DOM calendar ─────────────────────────────────────────────

    async def _parse_dom_calendar(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the calendar table in the DOM for the target date's price."""
        try:
            # Parse prices from elements containing £ sign
            calendar_data = await page.evaluate("""(targetDay) => {
                const results = [];
                
                // Strategy 1: Find links/buttons with data-day attribute
                const dayEls = document.querySelectorAll('a[data-day], button[data-day], [data-day]');
                for (const el of dayEls) {
                    const day = parseInt(el.getAttribute('data-day'), 10);
                    if (isNaN(day)) continue;
                    const text = el.textContent || '';
                    const prices = text.match(/£(\\d+(?:\\.\\d{2})?)/g);
                    if (prices && day === targetDay) {
                        const price = parseFloat(prices[prices.length - 1].replace('£', ''));
                        if (!isNaN(price) && price > 0)
                            results.push({day, price, method: 'data-day'});
                    }
                }
                if (results.length > 0) return results;
                
                // Strategy 2: Parse table cells
                const cells = document.querySelectorAll('table td, [class*="calendar"] td');
                for (const cell of cells) {
                    const text = cell.textContent || '';
                    // Look for day number
                    const supEl = cell.querySelector('sup, superscript, [class*="day-num"], [class*="dayNum"]');
                    let dayStr = supEl ? supEl.textContent.trim() : '';
                    if (!dayStr) {
                        const m = text.match(/^\\s*(\\d{1,2})\\b/);
                        if (m) dayStr = m[1];
                    }
                    if (!dayStr) continue;
                    const day = parseInt(dayStr, 10);
                    if (isNaN(day) || day < 1 || day > 31 || day !== targetDay) continue;
                    
                    const prices = text.match(/£(\\d+(?:\\.\\d{2})?)/g);
                    if (!prices) continue;
                    const price = parseFloat(prices[prices.length - 1].replace('£', ''));
                    if (!isNaN(price) && price > 0)
                        results.push({day, price, method: 'table-cell'});
                }
                if (results.length > 0) return results;
                
                // Strategy 3: Walk all elements with £ sign and nearby day numbers
                const allEls = document.querySelectorAll('*');
                for (const el of allEls) {
                    if (el.children.length > 3) continue;  // skip containers
                    const text = el.textContent || '';
                    if (!text.includes('£')) continue;
                    // Check if this element or its parent has day info
                    const dayAttr = el.getAttribute('data-day') || 
                                    el.closest('[data-day]')?.getAttribute('data-day') || '';
                    let day = dayAttr ? parseInt(dayAttr, 10) : NaN;
                    if (isNaN(day)) {
                        const m = text.match(/^\\s*(\\d{1,2})\\s/);
                        if (m) day = parseInt(m[1], 10);
                    }
                    if (day !== targetDay) continue;
                    
                    const prices = text.match(/£(\\d+(?:\\.\\d{2})?)/g);
                    if (!prices) continue;
                    const price = parseFloat(prices[prices.length - 1].replace('£', ''));
                    if (!isNaN(price) && price > 0)
                        results.push({day, price, method: 'walk'});
                }
                return results;
            }""", req.date_from.day)

            if calendar_data:
                logger.debug("Jet2: DOM calendar found %d price entries for day %d", len(calendar_data), req.date_from.day)

            if not calendar_data:
                return []

            booking_url = self._build_booking_url(req)
            offers: list[FlightOffer] = []
            seen_prices: set[float] = set()
            for entry in calendar_data:
                price = entry.get("price", 0)
                if price <= 0 or price in seen_prices:
                    continue
                seen_prices.add(price)

                dep_dt = datetime(req.date_from.year, req.date_from.month, req.date_from.day, 0, 0)
                segment = FlightSegment(
                    airline="LS", airline_name="Jet2",
                    flight_no="",
                    origin=req.origin, destination=req.destination,
                    departure=dep_dt, arrival=dep_dt,
                    cabin_class="M",
                )
                route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)
                offer_id = f"ls_{hashlib.md5(f'cal_{req.date_from}_{price}'.encode()).hexdigest()[:12]}"
                offers.append(FlightOffer(
                    id=offer_id,
                    price=round(price, 2),
                    currency="GBP",
                    price_formatted=f"£{price:.2f}",
                    outbound=route,
                    inbound=None,
                    airlines=["Jet2"],
                    owner_airline="LS",
                    booking_url=booking_url,
                    is_locked=False,
                    source="jet2_direct",
                    source_tier="free",
                ))
            return offers
        except Exception as e:
            logger.debug("Jet2: DOM calendar parse error: %s", e)
            return []

    # ── Single flight parser ───────────────────────────────────────────

    def _parse_single_flight(
        self, flight: dict, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        if not isinstance(flight, dict):
            return None
        price = (
            flight.get("price") or flight.get("totalPrice")
            or flight.get("farePrice") or flight.get("lowestPrice")
            or flight.get("adultPrice") or self._extract_cheapest_fare(flight)
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
                segments.append(self._build_segment(seg, req.origin, req.destination, req.date_from))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination, req.date_from))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival and segments[-1].arrival > segments[0].departure:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("flightKey") or flight.get("id") or flight.get("scheduleId")
            or flight.get("flightNumber", "") + "_" + str(price)
        )
        return FlightOffer(
            id=f"ls_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency="GBP",
            price_formatted=f"£{price:.2f}",
            outbound=route,
            inbound=None,
            airlines=["Jet2"],
            owner_airline="LS",
            booking_url=booking_url,
            is_locked=False,
            source="jet2_direct",
            source_tier="free",
        )

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str, target_date: date) -> FlightSegment:
        dep_str = seg.get("departure") or seg.get("departureDate") or seg.get("departureTime") or seg.get("std") or ""
        arr_str = seg.get("arrival") or seg.get("arrivalDate") or seg.get("arrivalTime") or seg.get("sta") or ""
        flight_no = str(
            seg.get("flightNumber") or seg.get("flight_no") or seg.get("flightNo") or seg.get("number") or ""
        ).replace(" ", "")
        origin = seg.get("origin") or seg.get("departureAirport") or seg.get("departureStation") or default_origin
        destination = seg.get("destination") or seg.get("arrivalAirport") or seg.get("arrivalStation") or default_dest

        dep_dt = self._parse_dt(dep_str) if dep_str else self._build_datetime(target_date, "")
        arr_dt = self._parse_dt(arr_str) if arr_str else dep_dt

        return FlightSegment(
            airline="LS", airline_name="Jet2", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=dep_dt, arrival=arr_dt,
            cabin_class="M",
        )

    @staticmethod
    def _extract_cheapest_fare(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareBundles") or flight.get("bundles") or []
        prices: list[float] = []
        for f in fares:
            p = f.get("price") or f.get("amount") or f.get("totalPrice") or f.get("basePrice")
            if p is not None:
                try:
                    prices.append(float(p))
                except (TypeError, ValueError):
                    continue
        return min(prices) if prices else None

    # ── Helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _build_datetime(d: date, time_str: str) -> datetime:
        """Combine a date with an optional time string like '14:30'."""
        if time_str:
            time_str = str(time_str).strip()
            match = re.match(r"(\d{1,2}):(\d{2})", time_str)
            if match:
                return datetime(d.year, d.month, d.day, int(match.group(1)), int(match.group(2)))
        return datetime(d.year, d.month, d.day, 0, 0)

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Jet2 %s→%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        search_hash = hashlib.md5(f"jet2{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else "GBP",
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
            f"https://www.jet2.com/en/cheap-flights"
            f"?from={dep}&to={dep}"
            f"&adults={req.adults}&children={req.children}&infants={req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"jet2{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency="GBP", offers=[], total_results=0,
        )
