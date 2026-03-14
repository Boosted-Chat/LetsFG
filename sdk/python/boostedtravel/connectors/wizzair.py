"""
Wizzair scraper — CDP Chrome + persistent page + page.evaluate(fetch) hybrid.

Kasada (KPSDK) protects all Wizzair API endpoints. Playwright's bundled
Chromium gets fingerprinted → 429 on the KPSDK /fp endpoint. SPA hash
navigation is unreliable (Usercentrics overlay, Vue router lazy-loading).

Strategy (CDP Chrome + in-browser fetch):
1. Launch REAL system Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP.  Keep ONE persistent page on wizzair.com.
3. On first load KPSDK JS solves the challenge; cookies persist across runs.
4. Discover API version from intercepted /Api/asset/* calls (e.g. "28.1.0").
5. For each search: page.evaluate(fetch) POSTs to /Api/search/search.
   KPSDK JS hooks the fetch and injects x-kpsdk-h / x-kpsdk-ct headers.
6. Parse JSON response directly — no SPA navigation per search.

Result: First search ~8-12s (KPSDK challenge), subsequent ~1-3s (cookies cached).
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

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import find_chrome, stealth_popen_kwargs

logger = logging.getLogger(__name__)

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
    {"width": 1600, "height": 900},
]
_LOCALES = ["en-GB", "en-US", "en-IE", "en-AU", "en-CA"]
_TIMEZONES = [
    "Europe/Warsaw", "Europe/London", "Europe/Berlin",
    "Europe/Paris", "Europe/Rome", "Europe/Madrid",
]

_MAX_ATTEMPTS = 2
_DEBUG_PORT = 9446
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".wizzair_chrome_data"
)

# ── Shared state ──────────────────────────────────────────────────────────
_browser_lock: Optional[asyncio.Lock] = None
_pw_instance = None
_browser = None
_chrome_proc = None
_persistent_page = None          # stays on wizzair.com — KPSDK JS active
_api_version: Optional[str] = None  # e.g. "28.1.0"
_page_ready = False              # True once KPSDK is loaded


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch real Chrome via CDP (headed — KPSDK detects --headless=new)."""
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

        # Try connecting to already-running Chrome on the debug port
        try:
            _browser = await _pw_instance.chromium.connect_over_cdp(
                f"http://localhost:{_DEBUG_PORT}"
            )
            logger.info("Wizzair: connected to existing Chrome via CDP")
            return _browser
        except Exception:
            pass

        # Launch real Chrome WITHOUT --headless=new (Kasada fingerprints it)
        chrome_path = find_chrome()
        if chrome_path:
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
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
                    "--window-position=-2400,-2400",
                    "about:blank",
                ],
                **stealth_popen_kwargs(),
            )
            await asyncio.sleep(2.5)
            try:
                _browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://localhost:{_DEBUG_PORT}"
                )
                logger.info("Wizzair: CDP Chrome connected (port %d)", _DEBUG_PORT)
                return _browser
            except Exception as e:
                logger.warning("Wizzair: CDP connect failed: %s, falling back", e)
                if _chrome_proc:
                    _chrome_proc.terminate()
                    _chrome_proc = None

        # Fallback: Playwright headed (no headless — KPSDK needs it)
        try:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled",
                      "--window-position=-2400,-2400"],
            )
        except Exception:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled",
                      "--no-sandbox",
                      "--window-position=-2400,-2400"],
            )
        logger.info("Wizzair: Playwright browser launched (headed fallback)")
        return _browser


async def _ensure_persistent_page():
    """Return a persistent page sitting on wizzair.com with KPSDK loaded.

    On first call: navigates to homepage, waits for KPSDK JS to solve the
    challenge, and intercepts asset API calls to discover the version.
    On subsequent calls: returns the cached page if still alive.
    """
    global _persistent_page, _api_version, _page_ready

    # Reuse existing page if alive (no lock needed for read check)
    if _persistent_page and _page_ready:
        try:
            await _persistent_page.evaluate("1")
            return _persistent_page, _api_version
        except Exception:
            _persistent_page = None
            _page_ready = False

    # Get browser first (acquires lock internally)
    browser = await _get_browser()

    is_cdp = hasattr(browser, "contexts") and browser.contexts
    if is_cdp:
        context = browser.contexts[0]
        page = await context.new_page()
    else:
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
        )
        page = await context.new_page()
        try:
            from playwright_stealth import stealth_async
            await stealth_async(page)
        except ImportError:
            pass

    # Intercept asset API calls to discover the version
    version_found = asyncio.Event()

    async def on_response(response):
        global _api_version
        try:
            url = response.url
            if "/Api/asset/" in url and response.status == 200:
                # Extract version from URL: be.wizzair.com/28.1.0/Api/...
                import re
                m = re.search(r"be\.wizzair\.com/(\d+\.\d+\.\d+)/Api/", url)
                if m and not _api_version:
                    _api_version = m.group(1)
                    logger.info("Wizzair: API version discovered: %s", _api_version)
                    version_found.set()
        except Exception:
            pass

    page.on("response", on_response)

    logger.info("Wizzair: loading homepage (KPSDK init)...")
    try:
        await page.goto(
            "https://wizzair.com/en-gb",
            wait_until="domcontentloaded",
            timeout=30000,
        )
    except Exception as e:
        logger.debug("Wizzair: homepage goto: %s", e)

    # Wait for KPSDK to solve challenge (Kasada page disappears)
    for _ in range(20):
        title = await page.title()
        if title and "verification" not in title.lower():
            break
        await asyncio.sleep(1)

    # Dismiss Usercentrics cookie consent
    await page.evaluate("""
        () => {
            if (window.__ucCmp) __ucCmp.acceptAllConsents();
            else if (window.UC_UI) UC_UI.acceptAllConsents();
        }
    """)

    # Wait for version discovery from asset API calls
    try:
        await asyncio.wait_for(version_found.wait(), timeout=15)
    except asyncio.TimeoutError:
        if not _api_version:
            # Fallback: try /buildnumber
            try:
                resp = await page.evaluate("""
                    async () => {
                        const r = await fetch('https://wizzair.com/buildnumber');
                        return await r.text();
                    }
                """)
                if resp and "." in resp:
                    _api_version = resp.strip()
                    logger.info("Wizzair: version from buildnumber: %s", _api_version)
            except Exception:
                _api_version = "28.1.0"  # known-good fallback
                logger.warning("Wizzair: using fallback API version %s", _api_version)

    _persistent_page = page
    _page_ready = True
    # Give KPSDK JS time to complete its challenge-response cycle.
    # Without this delay, the first fetch gets 429'd.
    await asyncio.sleep(5)
    logger.info("Wizzair: persistent page ready (version %s)", _api_version)
    return page, _api_version


class WizzairConnectorClient:
    """Wizzair scraper — CDP Chrome + SPA navigation + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        search_url = self._build_booking_url(req)

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                data = await self._attempt_search(search_url, req)
                if data is not None:
                    elapsed = time.monotonic() - t0
                    outbound = self._parse_flights(data.get("outboundFlights") or [])
                    inbound = self._parse_flights(data.get("returnFlights") or [])
                    offers = self._build_offers(req, outbound, inbound)
                    logger.info(
                        "Wizzair %s→%s returned %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    search_hash_id = hashlib.md5(
                        f"wizzair{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{search_hash_id}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency,
                        offers=offers,
                        total_results=len(offers),
                    )
                logger.warning(
                    "Wizzair: attempt %d/%d got no results",
                    attempt, _MAX_ATTEMPTS,
                )
            except Exception as e:
                logger.warning(
                    "Wizzair: attempt %d/%d error: %s",
                    attempt, _MAX_ATTEMPTS, e,
                )

        return self._empty(req)

    async def _attempt_search(
        self, url: str, req: FlightSearchRequest
    ) -> Optional[dict]:
        """Use persistent page + page.evaluate(fetch) to call the search API.

        KPSDK JS running in the page hooks the fetch and automatically
        injects x-kpsdk-h, x-kpsdk-ct, x-kpsdk-v headers.
        """
        global _persistent_page, _page_ready

        page, version = await _ensure_persistent_page()

        # Build the API request body (protocol: wizzair.json)
        flight_list = [
            {
                "departureStation": req.origin,
                "arrivalStation": req.destination,
                "departureDate": req.date_from.isoformat(),
            }
        ]
        if req.return_from:
            flight_list.append({
                "departureStation": req.destination,
                "arrivalStation": req.origin,
                "departureDate": req.return_from.isoformat(),
            })

        body = {
            "flightList": flight_list,
            "adultCount": req.adults,
            "childCount": req.children,
            "infantCount": req.infants,
            "wdc": True,
            "isFlightChange": False,
            "isSeniorOrStudent": False,
            "rescueFareCode": "",
            "priceType": "regular",
        }

        api_url = f"https://be.wizzair.com/{version}/Api/search/search"
        body_json = json.dumps(body)

        logger.info(
            "Wizzair: fetch %s→%s via page.evaluate (v%s)",
            req.origin, req.destination, version,
        )

        try:
            result = await page.evaluate("""
                async ([url, bodyJson]) => {
                    try {
                        // Read RequestVerificationToken from cookies
                        const rvt = document.cookie.split('; ')
                            .find(c => c.startsWith('RequestVerificationToken='));
                        const rvtVal = rvt ? rvt.split('=').slice(1).join('=') : '';

                        const resp = await fetch(url, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json;charset=UTF-8',
                                'Accept': 'application/json, text/plain, */*',
                                'X-RequestVerificationToken': rvtVal,
                            },
                            body: bodyJson,
                            credentials: 'include',
                        });
                        const status = resp.status;
                        if (status === 200) {
                            const data = await resp.json();
                            return {ok: true, status, data};
                        }
                        const text = await resp.text().catch(() => '');
                        return {ok: false, status, error: text.slice(0, 500)};
                    } catch (e) {
                        return {ok: false, status: 0, error: e.message};
                    }
                }
            """, [api_url, body_json])

            if result and result.get("ok"):
                return result["data"]

            status = result.get("status", 0) if result else 0
            error = result.get("error", "") if result else ""
            logger.warning("Wizzair: API returned %d: %s", status, error[:200])

            # 429 = KPSDK challenge expired → reset page, re-farm
            if status == 429:
                logger.info("Wizzair: 429 — KPSDK expired, re-farming...")
                _persistent_page = None
                _page_ready = False
                try:
                    await page.close()
                except Exception:
                    pass

            return None

        except Exception as e:
            logger.error("Wizzair: page.evaluate failed: %s", e)
            _persistent_page = None
            _page_ready = False
            return None

    def _build_offers(
        self,
        req: FlightSearchRequest,
        outbound_parsed: list[dict],
        return_parsed: list[dict],
    ) -> list[FlightOffer]:
        """Build FlightOffer objects from parsed flight data."""
        offers = []

        if req.return_from and return_parsed:
            outbound_parsed.sort(key=lambda x: x["price"])
            return_parsed.sort(key=lambda x: x["price"])

            for ob in outbound_parsed[:15]:
                for rt in return_parsed[:10]:
                    total = ob["price"] + rt["price"]
                    offer = FlightOffer(
                        id=f"w6_{hashlib.md5((ob['key'] + rt['key']).encode()).hexdigest()[:12]}",
                        price=round(total, 2),
                        currency=ob.get("currency", req.currency),
                        price_formatted=f"{total:.2f} {ob.get('currency', req.currency)}",
                        outbound=ob["route"],
                        inbound=rt["route"],
                        airlines=["Wizz Air"],
                        owner_airline="W6",
                        booking_url=self._build_booking_url(req),
                        is_locked=False,
                        source="wizzair_api",
                        source_tier="free",
                    )
                    offers.append(offer)
        else:
            for ob in outbound_parsed:
                offer = FlightOffer(
                    id=f"w6_{hashlib.md5(ob['key'].encode()).hexdigest()[:12]}",
                    price=round(ob["price"], 2),
                    currency=ob.get("currency", req.currency),
                    price_formatted=f"{ob['price']:.2f} {ob.get('currency', req.currency)}",
                    outbound=ob["route"],
                    inbound=None,
                    airlines=["Wizz Air"],
                    owner_airline="W6",
                    booking_url=self._build_booking_url(req),
                    is_locked=False,
                    source="wizzair_api",
                    source_tier="free",
                )
                offers.append(offer)

        offers.sort(key=lambda o: o.price)
        return offers

    def _parse_flights(self, flights: list[dict]) -> list[dict]:
        """Parse Wizzair flight entries into intermediate format."""
        results = []
        for flight in flights:
            fares = flight.get("fares", [])
            if not fares:
                continue

            # Get the basic fare (cheapest bundle)
            best_price = float("inf")
            best_currency = "EUR"
            for fare in fares:
                bundle = fare.get("bundle", "")
                base = fare.get("basePrice", {})
                amount = float(base.get("amount", 0))
                currency = base.get("currencyCode", "EUR")

                # Also check discounted price (WDC price)
                disc = fare.get("discountedPrice", {})
                disc_amount = float(disc.get("amount", 0)) if disc else 0

                effective = disc_amount if disc_amount > 0 else amount
                if 0 < effective < best_price:
                    best_price = effective
                    best_currency = currency

            if best_price == float("inf") or best_price <= 0:
                continue

            # Build segments
            dep_str = flight.get("departureDateTime", "")
            arr_str = flight.get("arrivalDateTime", "")
            flight_num = flight.get("flightNumber", "").replace(" ", "")

            dep_dt = self._parse_dt(dep_str)
            arr_dt = self._parse_dt(arr_str)

            dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

            route = FlightRoute(
                segments=[FlightSegment(
                    airline="W6",
                    airline_name="Wizz Air",
                    flight_no=flight_num,
                    origin=flight.get("departureStation", ""),
                    destination=flight.get("arrivalStation", ""),
                    departure=dep_dt,
                    arrival=arr_dt,
                    cabin_class="M",
                )],
                total_duration_seconds=max(dur, 0),
                stopovers=0,
            )

            key = f"{flight_num}_{dep_str}"

            results.append({
                "price": best_price,
                "currency": best_currency,
                "key": key,
                "route": route,
            })

        return results

    def _parse_dt(self, s: str) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        date_out = req.date_from.isoformat()
        date_in = req.return_from.isoformat() if req.return_from else ""
        return (
            f"https://wizzair.com/en-gb#/booking/select-flight/"
            f"{req.origin}/{req.destination}/{date_out}/{date_in}/"
            f"{req.adults}/{req.children}/{req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"wizzair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
