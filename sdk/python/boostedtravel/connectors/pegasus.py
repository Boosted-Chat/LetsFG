"""
Pegasus Airlines CDP scraper -- direct URL navigation + availability API interception.

Pegasus (IATA: PC) is Turkey's largest low-cost carrier, operating from
Istanbul Sabiha Gökçen (SAW) and Ankara (ESB) to domestic and international
destinations across Europe, Middle East and North Africa.

Strategy:
1. Launch Chrome via subprocess with specific flags that bypass Akamai
   (standard Playwright launch / headless / off-screen are all detected)
2. Connect via CDP (remote-debugging-port)
3. Navigate directly to the booking URL (skips form filling)
4. Intercept /pegasus/availability response
5. Parse departureRouteList -> dailyFlightList -> flightList -> FlightOffers
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import subprocess
import platform
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

# ── Chrome launch flags that bypass Akamai Bot Manager ────────────────────
# These match Playwright MCP's launch config. Standard Playwright launch()
# or headless modes are all detected by Akamai and served "Sayfa Bulunamadı".
_CHROME_FLAGS = [
    "--disable-field-trial-config",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-back-forward-cache",
    "--disable-breakpad",
    "--disable-client-side-phishing-detection",
    "--disable-component-extensions-with-background-pages",
    "--disable-component-update",
    "--no-default-browser-check",
    "--disable-default-apps",
    "--disable-dev-shm-usage",
    "--disable-features=AvoidUnnecessaryBeforeUnloadCheckSync,"
    "BoundaryEventDispatchTracksNodeRemoval,DestroyProfileOnBrowserClose,"
    "DialMediaRouteProvider,GlobalMediaControls,HttpsUpgrades,LensOverlay,"
    "MediaRouter,PaintHolding,ThirdPartyStoragePartitioning,Translate,"
    "AutoDeElevate,RenderDocument,OptimizationHints,AutomationControlled",
    "--enable-features=CDPScreenshotNewSurface",
    "--allow-pre-commit-input",
    "--disable-hang-monitor",
    "--disable-ipc-flooding-protection",
    "--disable-popup-blocking",
    "--disable-prompt-on-repost",
    "--disable-renderer-backgrounding",
    "--force-color-profile=srgb",
    "--metrics-recording-only",
    "--no-first-run",
    "--password-store=basic",
    "--no-service-autorun",
    "--disable-search-engine-choice-screen",
    "--disable-infobars",
    "--disable-sync",
    "--enable-unsafe-swiftshader",
    "--window-position=-2400,-2400",
    "--window-size=1366,768",
]

_TURKEY_AIRPORTS = {
    "IST", "SAW", "AYT", "ESB", "ADB", "DLM", "BJV", "TZX", "GZT",
    "VAN", "ERZ", "DIY", "SZF", "KYA", "MLX", "ASR", "EZS", "MQM",
    "HTY", "NAV", "KCM", "EDO", "ONQ", "BAL", "CKZ", "MSR", "IGD",
    "NKT", "GNY", "USQ", "DNZ", "ERC", "AOE", "KSY", "ISE", "YEI",
    "TEQ", "OGU", "BZI", "SFQ",
}

# ── Shared browser singleton via CDP ────────────────────────────────────
_CDP_PORT = 9454
_chrome_proc: Optional[subprocess.Popen] = None
_browser = None
_pw_instance = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch Chrome with Akamai-bypassing flags and connect via CDP."""
    global _chrome_proc, _browser, _pw_instance
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser

        from connectors.browser import find_chrome

        chrome = find_chrome()
        user_data = os.path.join(
            os.environ.get("TEMP", "/tmp"), "chrome-cdp-pegasus"
        )
        os.makedirs(user_data, exist_ok=True)

        args = [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={user_data}",
            *_CHROME_FLAGS,
            "about:blank",
        ]

        popen_kw: dict = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
        if platform.system() == "Windows":
            si = subprocess.STARTUPINFO()
            si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            si.wShowWindow = 7  # SW_SHOWMINNOACTIVE
            popen_kw["startupinfo"] = si
            popen_kw["creationflags"] = subprocess.CREATE_NO_WINDOW

        _chrome_proc = subprocess.Popen(args, **popen_kw)
        await asyncio.sleep(2.5)

        from playwright.async_api import async_playwright

        _pw_instance = await async_playwright().start()
        _browser = await _pw_instance.chromium.connect_over_cdp(
            f"http://127.0.0.1:{_CDP_PORT}"
        )
        logger.info(
            "Pegasus: Chrome ready via CDP (port %d, pid %d)",
            _CDP_PORT,
            _chrome_proc.pid,
        )
        return _browser


class PegasusConnectorClient:
    """Pegasus Airlines scraper -- direct URL + availability API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()

        try:
            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url
                    if "flypgs.com" not in url:
                        return
                    if response.status != 200:
                        return
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    body = await response.body()
                    if len(body) < 200:
                        return

                    import json as _json

                    data = _json.loads(body)
                    if not isinstance(data, dict):
                        return

                    if "pegasus/availability" in url and "departureRouteList" in data:
                        captured_data["availability"] = data
                        api_event.set()
                        logger.info(
                            "Pegasus: captured availability (%d bytes)",
                            len(body),
                        )
                except Exception:
                    pass

            page.on("response", on_response)

            dep = req.date_from.strftime("%Y-%m-%d")
            search_url = (
                f"https://web.flypgs.com/booking?"
                f"language=en&adultCount={req.adults}&childCount={req.children or 0}"
                f"&infantCount={req.infants or 0}&departurePort={req.origin}"
                f"&arrivalPort={req.destination}&currency={req.currency or 'EUR'}"
                f"&dateOption=1&departureDate={dep}"
            )
            logger.info(
                "Pegasus: searching %s->%s on %s",
                req.origin,
                req.destination,
                req.date_from.strftime("%Y-%m-%d"),
            )
            await page.goto(
                search_url,
                wait_until="load",
                timeout=int(self.timeout * 1000),
            )

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Pegasus: timed out waiting for availability API")

            data = captured_data.get("availability")
            if data:
                elapsed = time.monotonic() - t0
                offers = self._parse_response(data, req)
                if offers:
                    return self._build_response(offers, req, elapsed)

            return self._empty(req)

        except Exception as e:
            logger.error("Pegasus error: %s", e)
            return self._empty(req)
        finally:
            try:
                page.remove_listener("response", on_response)
                await page.goto("about:blank", wait_until="commit", timeout=5000)
            except Exception:
                pass

    # ── Response parsing ───────────────────────────────────────────────

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if isinstance(data, list):
            data = {"flights": data}
        currency = self._resolve_currency(data, req)
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Pegasus availability API returns departureRouteList → dailyFlightList → flightList
        if "departureRouteList" in data:
            routes = data["departureRouteList"]
            if isinstance(routes, list):
                for route in routes:
                    if not isinstance(route, dict):
                        continue
                    daily_flights = route.get("dailyFlightList") or []
                    if not isinstance(daily_flights, list):
                        continue
                    for daily in daily_flights:
                        if not isinstance(daily, dict):
                            continue
                        # cheapestFare at day level as price fallback
                        day_cheapest = None
                        day_currency = currency
                        cf = daily.get("cheapestFare")
                        if isinstance(cf, dict):
                            day_cheapest = cf.get("amount")
                            day_currency = cf.get("currency") or currency
                        # Each day has flightList with individual flights
                        flight_list = daily.get("flightList") or []
                        if isinstance(flight_list, list):
                            for flight in flight_list:
                                offer = self._parse_pegasus_flight(
                                    flight, day_currency, req, booking_url,
                                    fallback_price=day_cheapest,
                                )
                                if offer:
                                    offers.append(offer)
            if offers:
                return offers

        outbound_raw = (
            data.get("outboundFlights")
            or data.get("outbound")
            or (data.get("journeys", {}).get("outbound") if isinstance(data.get("journeys"), dict) else None)
            or data.get("departureDateFlights")
            or data.get("flights", [])
        )
        if not isinstance(outbound_raw, list):
            outbound_raw = []

        for flight in outbound_raw:
            offer = self._parse_single_flight(flight, currency, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    def _resolve_currency(self, data: dict, req: FlightSearchRequest) -> str:
        if data.get("currency"):
            return data["currency"]
        if req.origin in _TURKEY_AIRPORTS and req.destination in _TURKEY_AIRPORTS:
            return "TRY"
        return req.currency or "EUR"

    def _parse_pegasus_flight(
        self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str,
        fallback_price: float | None = None,
    ) -> Optional[FlightOffer]:
        """Parse an individual flight from Pegasus's flightList inside dailyFlightList."""
        if not isinstance(flight, dict):
            return None

        # ── Extract price ────────────────────────────────────────────
        price = None

        # 1) fareBundleList (some endpoints)
        fare_bundles = flight.get("fareBundleList") or flight.get("fareList") or flight.get("fares") or []
        if isinstance(fare_bundles, list) and fare_bundles:
            prices = []
            for fb in fare_bundles:
                if isinstance(fb, dict):
                    p = (fb.get("price") or fb.get("amount") or fb.get("totalPrice")
                         or fb.get("basePrice") or fb.get("adultPrice"))
                    if isinstance(p, dict):
                        p = p.get("amount") or p.get("value")
                    if p is not None:
                        try:
                            prices.append(float(p))
                        except (TypeError, ValueError):
                            pass
            if prices:
                price = min(prices)

        # 2) Single fare object (availability endpoint structure)
        if price is None:
            fare = flight.get("fare") or {}
            if isinstance(fare, dict):
                for key in ("amount", "price", "totalPrice", "basePrice", "adultPrice"):
                    val = fare.get(key)
                    if val is not None:
                        try:
                            price = float(val)
                            break
                        except (TypeError, ValueError):
                            pass
                # Extract currency from fare
                fc = fare.get("currency") or fare.get("currencyCode")
                if fc:
                    currency = str(fc)

        # 3) Direct fields on the flight object
        if price is None:
            price = (flight.get("price") or flight.get("totalPrice")
                     or flight.get("lowestFare") or flight.get("cheapestFare"))
            if isinstance(price, dict):
                currency = price.get("currency") or currency
                price = price.get("amount") or price.get("value")

        # 4) Fallback to day-level cheapestFare
        if price is None and fallback_price is not None:
            price = fallback_price

        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        # Extract currency from fare bundles if available
        for fb in (fare_bundles if isinstance(fare_bundles, list) else []):
            if isinstance(fb, dict):
                fc = fb.get("currency") or fb.get("currencyCode")
                if isinstance(fc, dict):
                    fc = fc.get("code")
                if fc:
                    currency = str(fc)
                    break

        # ── Build segments ───────────────────────────────────────────
        seg_raw = (flight.get("segmentList") or flight.get("segments")
                   or flight.get("legs") or [])
        segments: list[FlightSegment] = []
        if isinstance(seg_raw, list) and seg_raw:
            for seg in seg_raw:
                if isinstance(seg, dict):
                    segments.append(self._build_segment(seg, req.origin, req.destination))

        # If no explicit segments, the flight itself IS the segment
        if not segments:
            # Build from departure/arrival locations specific to Pegasus
            dep_loc = flight.get("departureLocation") or {}
            arr_loc = flight.get("arrivalLocation") or {}
            origin = dep_loc.get("portCode") or flight.get("origin") or req.origin
            dest = arr_loc.get("portCode") or flight.get("destination") or req.destination
            dep_dt = flight.get("departureDateTime") or flight.get("departure") or ""
            arr_dt = flight.get("arrivalDateTime") or flight.get("arrival") or ""
            flight_no = str(flight.get("flightNo") or flight.get("flightNumber") or "").strip()
            airline = flight.get("airline") or "PC"

            segments.append(FlightSegment(
                airline=airline,
                airline_name="Pegasus Airlines",
                flight_no=f"{airline}{flight_no}" if flight_no and not flight_no.startswith(airline) else flight_no,
                origin=origin,
                destination=dest,
                departure=self._parse_dt(dep_dt),
                arrival=self._parse_dt(arr_dt),
                cabin_class="M",
            ))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        # Duration from API data
        if total_dur <= 0:
            fd = flight.get("flightDuration")
            if isinstance(fd, dict):
                vals = fd.get("values") or []
                if isinstance(vals, list) and len(vals) >= 2:
                    try:
                        total_dur = int(vals[0]) * 3600 + int(vals[1]) * 60
                    except (TypeError, ValueError):
                        pass

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("segmentId") or flight.get("flightKey") or flight.get("id")
            or (flight.get("flightNo", "") + "_" + str(flight.get("departureDateTime", "")))
        )
        return FlightOffer(
            id=f"pc_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Pegasus Airlines"],
            owner_airline="PC",
            booking_url=booking_url,
            is_locked=False,
            source="pegasus_direct",
            source_tier="free",
        )

    def _parse_single_flight(
        self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        price = (
            flight.get("price") or flight.get("totalPrice")
            or flight.get("farePrice") or flight.get("lowestFare")
            or self._extract_cheapest_fare(flight)
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

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("flightKey") or flight.get("id")
            or flight.get("flightNumber", "") + "_" + segments[0].departure.isoformat()
        )
        return FlightOffer(
            id=f"pc_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Pegasus Airlines"],
            owner_airline="PC",
            booking_url=booking_url,
            is_locked=False,
            source="pegasus_direct",
            source_tier="free",
        )

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = seg.get("departure") or seg.get("departureDate") or seg.get("departureTime") or seg.get("std") or ""
        arr_str = seg.get("arrival") or seg.get("arrivalDate") or seg.get("arrivalTime") or seg.get("sta") or ""
        flight_no = str(
            seg.get("flightNumber") or seg.get("flight_no") or seg.get("flightNo") or seg.get("number") or ""
        ).replace(" ", "")
        origin = seg.get("origin") or seg.get("departureAirport") or seg.get("departureStation") or default_origin
        destination = seg.get("destination") or seg.get("arrivalAirport") or seg.get("arrivalStation") or default_dest
        return FlightSegment(
            airline="PC", airline_name="Pegasus Airlines", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
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

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Pegasus %s→%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        search_hash = hashlib.md5(f"pegasus{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
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
            f"https://www.flypgs.com/en/booking?from={req.origin}"
            f"&to={req.destination}&departure={dep}"
            f"&adults={req.adults}&children={req.children}&infants={req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"pegasus{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=req.currency or "EUR", offers=[], total_results=0,
        )
