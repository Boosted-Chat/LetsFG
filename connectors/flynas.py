"""
Flynas hybrid scraper — CDP Chrome + persistent page + curl_cffi fast path.

Flynas (IATA: XY) is a Saudi low-cost carrier.
Website: booking.flynas.com — custom booking engine protected by Akamai WAF.

Strategy (Enhanced Hybrid — curl_cffi fast path + CDP Chrome fallback):
1. Launch REAL system Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP. Keep ONE persistent page on booking.flynas.com.
3. page.evaluate(fetch('/api/SessionCreate')) → establishes API session
4. Extract Akamai cookies (_abck, bm_sz, ak_bmsc, session cookies) from warm page.
5. FAST PATH: curl_cffi (impersonate="chrome131") POSTs to /api/FlightSearch
   with extracted cookies. ~0.5-1s when cookies are fresh (<5 min).
6. FALLBACK: page.evaluate(fetch('/api/FlightSearch', body)) if curl_cffi
   fails (403 = Akamai blocked). ~2-4s warm.
7. After successful in-browser search, refresh cookie cache from warm page.

Real Chrome bypasses Akamai fingerprinting where bundled Chromium fails.
curl_cffi fast path works ~50% of the time but is 5-10x faster when it does.
The page is kept warm with periodic pings (every 2 min) to keep Akamai fresh.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
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
from connectors.browser import stealth_args, stealth_position_arg, stealth_popen_kwargs

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_DEBUG_PORT = 9449
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".flynas_chrome_data"
)

_IMPERSONATE = "chrome131"
_COOKIE_MAX_AGE = 5 * 60  # Akamai _abck validity ~5-10 min; use 5 min as safe threshold
_KEEPALIVE_INTERVAL = 2 * 60  # Ping warm page every 2 min to keep Akamai cookies fresh
_SEARCH_URL = "https://booking.flynas.com/api/FlightSearch"

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
# Warm page: keeps one page with Akamai cookies alive for fast reuse
_warm_page = None
_warm_page_lock: Optional[asyncio.Lock] = None
_warm_ready = False

# Akamai cookie cache — extracted from warm page after Akamai resolves
_akamai_cookies: list[dict] | None = None
_akamai_cookies_ts: float = 0

# Keepalive task reference
_keepalive_task: asyncio.Task | None = None
_keepalive_shutdown = False


def _find_chrome() -> Optional[str]:
    """Find Chrome executable on the system."""
    candidates = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium-browser",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _get_page_lock() -> asyncio.Lock:
    global _warm_page_lock
    if _warm_page_lock is None:
        _warm_page_lock = asyncio.Lock()
    return _warm_page_lock


async def _get_browser():
    """Launch real Chrome via subprocess + connect via CDP."""
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

        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass
        _pw_instance = await async_playwright().start()

        chrome_path = _find_chrome()
        if chrome_path:
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
            # Try connecting to already-running Chrome
            try:
                _browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://localhost:{_DEBUG_PORT}"
                )
                logger.info("Flynas: connected to existing Chrome via CDP")
                return _browser
            except Exception:
                pass

            # Launch new Chrome
            vp = random.choice(_VIEWPORTS)
            _chrome_proc = subprocess.Popen(
                [
                    chrome_path,
                    f"--remote-debugging-port={_DEBUG_PORT}",
                    f"--user-data-dir={_USER_DATA_DIR}",
                    f"--window-size={vp['width']},{vp['height']}",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-background-networking",
                    *stealth_position_arg(),
                    "about:blank",
                ],
                **stealth_popen_kwargs(),
            )
            await asyncio.sleep(2.5)
            try:
                _browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://localhost:{_DEBUG_PORT}"
                )
                logger.info("Flynas: CDP Chrome connected (port %d)", _DEBUG_PORT)
                return _browser
            except Exception as e:
                logger.warning("Flynas: CDP connect failed: %s, falling back", e)
                if _chrome_proc:
                    _chrome_proc.terminate()
                    _chrome_proc = None

        # Fallback: regular Playwright headed
        try:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled", *stealth_args()],
            )
        except Exception:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox", *stealth_args()],
            )
        logger.info("Flynas: Playwright browser launched (headed fallback)")
        return _browser


async def _extract_cookies_from_page(page) -> None:
    """Extract all cookies from the warm page's browser context into the global cache."""
    global _akamai_cookies, _akamai_cookies_ts
    try:
        ctx = page.context
        cookies = await ctx.cookies()
        if cookies:
            _akamai_cookies = cookies
            _akamai_cookies_ts = time.monotonic()
            akamai_names = {c["name"] for c in cookies if c["name"] in (
                "_abck", "bm_sz", "ak_bmsc",
            )}
            logger.info(
                "Flynas: cached %d cookies (Akamai keys: %s)",
                len(cookies), ", ".join(sorted(akamai_names)) or "none",
            )
            if not akamai_names:
                logger.warning("Flynas: no critical Akamai cookies found — curl_cffi path may fail")
    except Exception as e:
        logger.warning("Flynas: cookie extraction failed: %s", e)


def _are_cookies_fresh() -> bool:
    """Check if cached Akamai cookies exist and are within the max age threshold."""
    return bool(_akamai_cookies) and (time.monotonic() - _akamai_cookies_ts) < _COOKIE_MAX_AGE


async def _keepalive_loop():
    """Periodically ping the warm page to keep Akamai cookies fresh."""
    global _warm_page, _warm_ready
    while not _keepalive_shutdown:
        await asyncio.sleep(_KEEPALIVE_INTERVAL)
        if _keepalive_shutdown:
            break
        try:
            if not _warm_ready or not _warm_page or _warm_page.is_closed():
                continue
            # Simple JS eval to keep the page connection alive
            await _warm_page.evaluate("1")
            # Re-extract cookies to keep the cache fresh
            await _extract_cookies_from_page(_warm_page)
            logger.debug("Flynas: keepalive ping OK")
        except Exception as e:
            logger.warning("Flynas: keepalive ping failed: %s — marking page stale", e)
            _warm_ready = False


def _start_keepalive():
    """Start the keepalive background task if not already running."""
    global _keepalive_task, _keepalive_shutdown
    _keepalive_shutdown = False
    if _keepalive_task is None or _keepalive_task.done():
        try:
            _keepalive_task = asyncio.get_event_loop().create_task(_keepalive_loop())
        except RuntimeError:
            _keepalive_task = asyncio.ensure_future(_keepalive_loop())


async def _ensure_warm_page():
    """Ensure a warm page exists with valid Akamai session."""
    global _warm_page, _warm_ready
    lock = _get_page_lock()
    async with lock:
        # Check if existing page is still usable
        if _warm_ready and _warm_page and not _warm_page.is_closed():
            try:
                await _warm_page.evaluate("1")
                return _warm_page
            except Exception:
                _warm_ready = False

        # Get browser and use first context's first page, or create one
        browser = await _get_browser()
        contexts = browser.contexts
        if contexts and contexts[0].pages:
            _warm_page = contexts[0].pages[0]
        else:
            ctx = contexts[0] if contexts else await browser.new_context()
            _warm_page = await ctx.new_page()

        logger.info("Flynas: loading booking page (Akamai warm-up)...")
        await _warm_page.goto(
            "https://booking.flynas.com/",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await asyncio.sleep(5)

        # Create API session
        sess = await _warm_page.evaluate("""async () => {
            const r = await fetch('/api/SessionCreate', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({session: {channel: 'web'}})
            });
            return {status: r.status};
        }""")
        logger.info("Flynas: SessionCreate status=%s", sess.get("status"))

        # Extract Akamai cookies after warm-up
        await _extract_cookies_from_page(_warm_page)

        _warm_ready = True
        _start_keepalive()
        return _warm_page


class FlynasConnectorClient:
    """Flynas hybrid scraper — curl_cffi fast path + Playwright warm page fallback."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ------------------------------------------------------------------
    # curl_cffi fast path
    # ------------------------------------------------------------------

    async def _search_via_api(self, search_body: dict) -> dict | None:
        """
        POST /api/FlightSearch via curl_cffi with cached Akamai cookies.

        Returns parsed JSON dict on success, None on failure (403 = Akamai block).
        """
        if not _are_cookies_fresh():
            return None

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._search_via_api_sync, search_body,
        )

    def _search_via_api_sync(self, search_body: dict) -> dict | None:
        """Synchronous curl_cffi POST to /api/FlightSearch.

        Uses a shorter timeout than in-browser path since this is the fast path —
        we want to fail quickly and fall back to the browser if needed.
        """
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)

        # Load all Akamai cookies into session
        for c in _akamai_cookies:
            domain = c.get("domain", "")
            sess.cookies.set(c["name"], c["value"], domain=domain)

        try:
            r = sess.post(
                _SEARCH_URL,
                json=search_body,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Origin": "https://booking.flynas.com",
                    "Referer": "https://booking.flynas.com/",
                },
                timeout=min(self.timeout / 3, 10),
            )
        except Exception as e:
            logger.warning("Flynas: curl_cffi request failed: %s", e)
            return None

        if r.status_code in (403, 429):
            logger.info("Flynas: curl_cffi blocked by Akamai (%d)", r.status_code)
            return None

        if r.status_code not in (200, 201):
            logger.warning("Flynas: curl_cffi returned %d", r.status_code)
            return None

        try:
            return r.json()
        except Exception:
            logger.warning("Flynas: curl_cffi non-JSON response")
            return None

    # ------------------------------------------------------------------
    # In-browser fetch fallback (existing path)
    # ------------------------------------------------------------------

    async def _search_in_browser(self, page, search_body: dict) -> dict | None:
        """Execute FlightSearch via page.evaluate(fetch(...)) — inherits Akamai cookies."""
        global _warm_ready

        try:
            result = await page.evaluate(
                """async (body) => {
                    const r = await fetch('/api/FlightSearch', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(body)
                    });
                    return {status: r.status, body: await r.text()};
                }""",
                search_body,
            )
        except Exception as e:
            logger.warning("Flynas: page.evaluate failed: %s — resetting warm page", e)
            _warm_ready = False
            return None

        status = result.get("status", 0)

        if status == 429:
            logger.info("Flynas: 429 challenge, waiting 3s and retrying...")
            await asyncio.sleep(3)
            try:
                result = await page.evaluate(
                    """async (body) => {
                        const r = await fetch('/api/FlightSearch', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify(body)
                        });
                        return {status: r.status, body: await r.text()};
                    }""",
                    search_body,
                )
                status = result.get("status", 0)
            except Exception:
                _warm_ready = False
                return None

        if status not in (200, 201):
            logger.warning("Flynas API returned %d: %s", status, result.get("body", "")[:300])
            if status in (403, 429):
                _warm_ready = False
            return None

        try:
            return json.loads(result["body"])
        except Exception:
            logger.warning("Flynas: non-JSON response from in-browser fetch")
            return None

    # ------------------------------------------------------------------
    # Main search — curl_cffi fast path -> in-browser fallback
    # ------------------------------------------------------------------

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        date_str = req.date_from.strftime("%Y-%m-%d")

        flights_data = [{"origin": req.origin, "destination": req.destination, "date": date_str}]
        if req.return_from:
            ret_str = req.return_from.strftime("%Y-%m-%d")
            flights_data.append({"origin": req.destination, "destination": req.origin, "date": ret_str})

        search_body = {
            "flightSearch": {
                "flights": flights_data,
                "adultCount": str(req.adults),
                "childCount": str(req.children or 0),
                "infantCount": str(req.infants or 0),
                "selectedCurrencyCode": req.currency or "SAR",
                "flightMode": "return" if req.return_from else "oneway",
                "clickId": "",
                "reqSource": "",
                "custID": "",
                "isStopoverbooking": False,
            }
        }

        # ── Fast path: curl_cffi with cached Akamai cookies ───────────
        data = None
        search_path = "none"

        if _are_cookies_fresh():
            data = await self._search_via_api(search_body)
            if data is not None:
                search_path = "curl_cffi"
                logger.info(
                    "Flynas: curl_cffi fast path succeeded for %s->%s in %.1fs",
                    req.origin, req.destination, time.monotonic() - t0,
                )

        # ── Fallback: in-browser fetch via warm page ──────────────────
        if data is None:
            try:
                page = await _ensure_warm_page()
            except Exception as e:
                logger.error("Flynas: warm page setup failed: %s", e)
                return self._empty(req)

            data = await self._search_in_browser(page, search_body)
            if data is not None:
                search_path = "in_browser"
                logger.info(
                    "Flynas: in-browser fallback succeeded for %s->%s in %.1fs",
                    req.origin, req.destination, time.monotonic() - t0,
                )
                # Refresh cookie cache after successful in-browser search
                await _extract_cookies_from_page(page)

        if data is None:
            return self._empty(req)

        # ── Parse response ────────────────────────────────────────────
        trips = data.get("flightsAvailability", {}).get("trips", [])
        outbound_flights = []
        return_flights = []

        for i, trip in enumerate(trips):
            for flight in trip.get("flights", []):
                parsed = self._parse_flight(flight)
                if parsed:
                    if i == 0:
                        outbound_flights.append(parsed)
                    else:
                        return_flights.append(parsed)

        offers = []
        booking_url = self._build_booking_url(req)
        currency = req.currency or "SAR"

        if req.return_from and return_flights:
            outbound_flights.sort(key=lambda x: x["price"])
            return_flights.sort(key=lambda x: x["price"])

            for ob in outbound_flights[:15]:
                for rt in return_flights[:10]:
                    total = ob["price"] + rt["price"]
                    offer = FlightOffer(
                        id=f"xy_{hashlib.md5((ob['key'] + rt['key']).encode()).hexdigest()[:12]}",
                        price=round(total, 2),
                        currency=currency,
                        price_formatted=f"{total:.2f} {currency}",
                        outbound=ob["route"],
                        inbound=rt["route"],
                        airlines=["flynas"],
                        owner_airline="XY",
                        booking_url=booking_url,
                        is_locked=False,
                        source="flynas_direct",
                        source_tier="free",
                    )
                    offers.append(offer)
        else:
            for ob in outbound_flights:
                offer = FlightOffer(
                    id=f"xy_{hashlib.md5(ob['key'].encode()).hexdigest()[:12]}",
                    price=round(ob["price"], 2),
                    currency=currency,
                    price_formatted=f"{ob['price']:.2f} {currency}",
                    outbound=ob["route"],
                    inbound=None,
                    airlines=["flynas"],
                    owner_airline="XY",
                    booking_url=booking_url,
                    is_locked=False,
                    source="flynas_direct",
                    source_tier="free",
                )
                offers.append(offer)

        offers.sort(key=lambda o: o.price)
        elapsed = time.monotonic() - t0

        logger.info(
            "Flynas %s->%s returned %d offers in %.1fs (path=%s)",
            req.origin, req.destination, len(offers), elapsed, search_path,
        )

        search_hash = hashlib.md5(
            f"flynas{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=currency,
            offers=offers,
            total_results=len(offers),
        )

    def _parse_flight(self, flight: dict) -> Optional[dict]:
        """Parse a single flight from the flynas FlightSearch response."""
        best_price = None
        fares = flight.get("fares", [])
        for fare in fares:
            p = fare.get("price")
            if p is not None and p > 0:
                if best_price is None or p < best_price:
                    best_price = p
        if best_price is None or best_price <= 0:
            return None

        legs = flight.get("legs", [])
        segments = []
        for leg in legs:
            flight_no = str(leg.get("flightNumber", "")).strip()
            carrier = leg.get("carrierCode") or leg.get("operatedBy") or "XY"
            if flight_no and not flight_no.startswith(("XY", "xy")):
                flight_no = f"XY{flight_no}"
            segments.append(FlightSegment(
                airline=carrier,
                airline_name="flynas",
                flight_no=flight_no,
                origin=leg.get("origin", ""),
                destination=leg.get("destination", ""),
                departure=self._parse_dt(leg.get("departureDate", "")),
                arrival=self._parse_dt(leg.get("arrivalDate", "")),
                cabin_class="M",
            ))

        if not segments:
            # Fallback: build from flight-level data
            flight_no = str(flight.get("flightNumber", "")).strip()
            if flight_no and not flight_no.startswith(("XY", "xy")):
                flight_no = f"XY{flight_no}"
            segments.append(FlightSegment(
                airline="XY",
                airline_name="flynas",
                flight_no=flight_no,
                origin=flight.get("origin", ""),
                destination=flight.get("destination", ""),
                departure=self._parse_dt(flight.get("departureDate", "")),
                arrival=self._parse_dt(flight.get("arrivalDate", "")),
                cabin_class="M",
            ))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = max(
                int((segments[-1].arrival - segments[0].departure).total_seconds()), 0
            )

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=flight.get("numberOfStops", max(len(segments) - 1, 0)),
        )

        flight_key = str(
            flight.get("journeyKey")
            or flight.get("id")
            or f"{segments[0].flight_no}_{segments[0].departure.isoformat()}"
        )

        return {
            "price": float(best_price),
            "key": flight_key,
            "route": route,
        }

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

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://booking.flynas.com/#/booking/search-redirect?"
            f"origin={req.origin}&destination={req.destination}"
            f"&departureDate={dep}&flightMode=oneway&adultCount={req.adults}"
            f"&childCount={req.children or 0}&infantCount={req.infants or 0}&culture=en-US"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"flynas{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
