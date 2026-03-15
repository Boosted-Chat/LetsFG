"""
Qatar Airways (QR) CDP Chrome connector — booking URL + availability API interception.

Qatar's booking engine is an Angular SPA (NBX — Next-gen Booking eXperience)
built on Navitaire NSP.  The homepage widget uses Shadow DOM which is fragile,
so we bypass it entirely by navigating directly to the booking URL with
pre-filled search parameters.  The NBX engine fires availability API calls
which we intercept and parse.

Strategy (CDP Chrome + response interception):
1.  Launch REAL Chrome (--remote-debugging-port, --user-data-dir).
2.  Connect via Playwright CDP.  Re-use context across searches.
3.  Each search: new page → navigate to booking URL with params.
4.  The NBX engine loads and fires availability/search API calls.
5.  Intercept the first 200 response containing flight data.
6.  Parse into FlightOffers.

Booking URL pattern:
  https://booking.qatarairways.com/nsp/sale/flightbooking
    ?widget=QR&searchType=F&addTax498To1=1&flexibleDate=off
    &bookingClass=E&tripType=O&from={origin}&to={destination}
    &departing={date}&adults={n}&children={n}&infants={n}
    &teenager=0&ofw=0&promoCode=&currency=USD
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, date as date_type, timedelta
from typing import Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import find_chrome, stealth_popen_kwargs, _launched_procs

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9454
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".qr_chrome_data"
)

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent browser context (headed — bot protection)."""
    global _browser, _context, _pw_instance, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    if _context:
                        try:
                            _ = _context.pages
                            return _context
                        except Exception:
                            pass
                    contexts = _browser.contexts
                    if contexts:
                        _context = contexts[0]
                        return _context
            except Exception:
                pass

        from playwright.async_api import async_playwright

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("QR: connected to existing Chrome on port %d", _DEBUG_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

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
                "--window-size=1400,900",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(2.0)

            pw = await async_playwright().start()
            _pw_instance = pw
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            logger.info(
                "QR: Chrome launched on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    """Wipe Chrome profile when session is corrupted."""
    global _browser, _context, _pw_instance, _chrome_proc
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
    _browser = None
    _context = None
    _pw_instance = None
    _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("QR: deleted stale Chrome profile")
        except Exception:
            pass


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


def _parse_datetime(s: str) -> datetime:
    """Parse ISO-ish datetime strings from Qatar's API responses."""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M",
    ):
        try:
            return datetime.strptime(s[:len(fmt) + 3], fmt)
        except (ValueError, IndexError):
            continue
    return datetime.strptime(s[:10], "%Y-%m-%d")


# ── Keywords for API interception ────────────────────────────────────────────

_AVAIL_KEYWORDS = (
    "flightservlet", "availability", "avail", "search", "offer",
    "flights", "air-bound", "airbound", "journey", "fare",
)

_SKIP_KEYWORDS = (
    "analytics", "google", "facebook", "doubleclick", "fonts.",
    "gtm.", "pixel", "amplitude", ".css", ".png", ".jpg", ".svg",
    ".gif", ".woff", ".ico", "demdex", "omtrdc", "appdynamics",
    "newrelic", "nr-data", "medallia", "adobedtm", "qualtrics",
    "tealium", "mparticle", "segment", "fullstory", "hotjar",
    "snapchat", "tiktok", "twitter", "quantummetric",
    "applicationinsights", "onetrust",
)


class QatarConnectorClient:
    """Qatar Airways CDP Chrome connector — booking URL + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()

        avail_data: dict = {}
        blocked = False

        async def _on_response(response):
            nonlocal blocked
            url_lower = response.url.lower()

            # Skip noise
            if any(s in url_lower for s in _SKIP_KEYWORDS):
                return

            status = response.status

            # Detect blocking
            if status in (403, 429):
                if any(k in url_lower for k in _AVAIL_KEYWORDS):
                    blocked = True
                    logger.warning("QR: %d on %s", status, response.url[:120])
                return

            if status != 200:
                return

            # Check if this is an availability/flight response
            is_avail = any(k in url_lower for k in _AVAIL_KEYWORDS)
            if not is_avail:
                return

            try:
                ct = response.headers.get("content-type", "")
                if "json" not in ct and "javascript" not in ct:
                    return
                body = await response.text()
                if len(body) < 50:
                    return
                data = json.loads(body)
                if not isinstance(data, dict):
                    return

                # Look for flight data signatures
                if self._looks_like_flights(data):
                    if not avail_data:
                        avail_data.update(data)
                        logger.info(
                            "QR: captured availability (%d bytes) from %s",
                            len(body), response.url[:100],
                        )
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            # ── Navigate to booking URL with pre-filled params ──
            url = self._build_booking_url(req)
            logger.info("QR: loading %s->%s via booking URL", req.origin, req.destination)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3.0)

            # Accept cookies if prompted
            await self._dismiss_cookies(page)

            # Wait for availability API response
            remaining = max(self.timeout - (time.monotonic() - t0), 20)
            deadline = time.monotonic() + remaining
            while not avail_data and not blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            # ── Fallback: try homepage form fill if URL approach failed ──
            if not avail_data and not blocked:
                logger.info("QR: URL approach got no data, trying homepage form")
                avail_data, blocked = await self._try_homepage_form(page, req, t0)

            if blocked:
                logger.warning("QR: session blocked, resetting profile")
                await _reset_profile()
                return self._empty(req)

            if not avail_data:
                logger.warning("QR: no availability data captured")
                return self._empty(req)

            offers = self._parse_availability(avail_data, req)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "QR %s->%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"qr{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            currency = self._extract_currency(avail_data, req)

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("QR CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # URL construction
    # ------------------------------------------------------------------

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        """Build the NSP booking URL with pre-filled search params."""
        dt = _to_datetime(req.date_from)
        date_str = dt.strftime("%Y-%m-%d")
        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0

        return (
            "https://booking.qatarairways.com/nsp/sale/flightbooking"
            f"?widget=QR&searchType=F&addTax498To1=1&flexibleDate=off"
            f"&bookingClass=E&tripType=O"
            f"&from={req.origin}&to={req.destination}"
            f"&departing={date_str}"
            f"&adults={adults}&children={children}&infants={infants}"
            f"&teenager=0&ofw=0&promoCode=&currency=USD"
        )

    # ------------------------------------------------------------------
    # Cookie dismissal
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        for sel in (
            "#onetrust-accept-btn-handler",
            "button:has-text('Accept')",
            "#acceptAllCookies",
            "button:has-text('I agree')",
        ):
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and await btn.is_visible(timeout=1000):
                    await btn.click(timeout=2000)
                    logger.info("QR: cookies accepted")
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue

    # ------------------------------------------------------------------
    # Fallback: homepage form fill (Shadow DOM)
    # ------------------------------------------------------------------

    async def _try_homepage_form(
        self, page, req: FlightSearchRequest, t0: float
    ) -> tuple[dict, bool]:
        """Fallback: navigate to homepage, fill Angular Shadow DOM form, submit."""
        avail_data: dict = {}
        blocked = False

        try:
            await page.goto(
                "https://www.qatarairways.com/en/homepage.html",
                wait_until="domcontentloaded",
                timeout=20000,
            )
            await asyncio.sleep(5.0)
            await self._dismiss_cookies(page)

            # Select One Way via shadow DOM
            try:
                await page.evaluate("""() => {
                    const host = document.querySelector('app-nbx-explore');
                    if (!host || !host.shadowRoot) return;
                    const radios = host.shadowRoot.querySelectorAll(
                        'mat-radio-button, label, [class*="mat-radio"]'
                    );
                    for (const r of radios) {
                        if (r.textContent?.trim()?.toLowerCase().includes('one way')) {
                            r.click(); return;
                        }
                    }
                }""")
                await asyncio.sleep(0.5)
            except Exception:
                pass

            # Fill origin
            await self._fill_shadow_input(page, 0, req.origin)
            await asyncio.sleep(1.0)

            # Fill destination
            await self._fill_shadow_input(page, 1, req.destination)
            await asyncio.sleep(1.0)

            # Fill date
            dt = _to_datetime(req.date_from)
            date_str = dt.strftime("%d %b %y")
            try:
                await page.evaluate(f"""() => {{
                    const host = document.querySelector('app-nbx-explore');
                    if (!host || !host.shadowRoot) return;
                    const dp = host.shadowRoot.querySelector('#dpFromDate, #datepicker');
                    if (dp) {{
                        dp.value = '{date_str}';
                        dp.dispatchEvent(new Event('input', {{bubbles: true}}));
                        dp.dispatchEvent(new Event('change', {{bubbles: true}}));
                    }}
                }}""")
                await asyncio.sleep(0.5)
            except Exception:
                pass

            # Click search button
            try:
                await page.evaluate("""() => {
                    const host = document.querySelector('app-nbx-explore');
                    if (!host || !host.shadowRoot) return;
                    const btns = host.shadowRoot.querySelectorAll('button');
                    for (const b of btns) {
                        const text = b.textContent?.trim()?.toLowerCase() || '';
                        if (text.includes('search') && text.includes('flight')) {
                            b.click(); return;
                        }
                    }
                }""")
                logger.info("QR: clicked search button via shadow DOM")
            except Exception:
                pass

            # Wait for availability response
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            deadline = time.monotonic() + remaining
            while not avail_data and not blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.warning("QR: homepage form fallback error: %s", e)

        return avail_data, blocked

    async def _fill_shadow_input(self, page, index: int, value: str) -> None:
        """Fill an input field inside the Shadow DOM booking widget."""
        try:
            await page.evaluate(f"""() => {{
                const host = document.querySelector('app-nbx-explore');
                if (!host || !host.shadowRoot) return;
                const inputs = host.shadowRoot.querySelectorAll('input[type="text"], input[matinput]');
                const inp = inputs[{index}];
                if (!inp) return;
                inp.focus();
                inp.value = '';
                inp.dispatchEvent(new Event('input', {{bubbles: true}}));
                inp.value = '{value}';
                inp.dispatchEvent(new Event('input', {{bubbles: true}}));
            }}""")
            await asyncio.sleep(2.0)

            # Click first dropdown option
            await page.evaluate("""() => {
                const host = document.querySelector('app-nbx-explore');
                if (!host || !host.shadowRoot) return;
                const opts = host.shadowRoot.querySelectorAll(
                    '[role="option"], mat-option, [class*="mat-option"]'
                );
                if (opts.length > 0) opts[0].click();
            }""")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Flight data detection
    # ------------------------------------------------------------------

    @staticmethod
    def _looks_like_flights(data: dict) -> bool:
        """Heuristic: does this JSON look like flight availability data?"""
        s = json.dumps(data)[:5000].lower()

        # Check for common availability data signatures
        signatures = [
            "journeysavailablebymarket",   # Navitaire NSK
            "faresavailable",              # Navitaire NSK
            "origindestinationoption",     # Amadeus/Sabre
            "flightoffers",               # Various
            "flightsegment",              # Various
            "departuretime",              # Generic
            "airbound",                   # Generic
            "flightlist",                 # Various
            "itineraries",               # Various
            '"fare"',                     # Fare data
        ]

        has_flight_data = any(sig in s for sig in signatures)
        has_price = '"price"' in s or '"amount"' in s or '"fare"' in s or '"total"' in s
        has_airports = bool(re.search(r'"[A-Z]{3}"', s))

        return has_flight_data and (has_price or has_airports)

    # ------------------------------------------------------------------
    # Response parsing — multi-format
    # ------------------------------------------------------------------

    def _parse_availability(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse availability from multiple possible response formats."""
        offers: list[FlightOffer] = []

        # Try Navitaire NSK format (like Jazeera/Akasa)
        offers = self._parse_navitaire(data, req)
        if offers:
            return offers

        # Try NBX/generic format
        offers = self._parse_nbx(data, req)
        if offers:
            return offers

        # Try DOM-like structured format
        offers = self._parse_generic(data, req)
        return offers

    def _parse_navitaire(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Navitaire NSK/dotREZ availability format."""
        offers: list[FlightOffer] = []

        av = data
        for key in ("response", "data", "availabilityv4", "availability"):
            if key in av and isinstance(av[key], dict):
                av = av[key]

        currency = av.get("currencyCode", "USD")

        # Build fare lookup
        fare_lookup: dict[str, float] = {}
        fares_available = av.get("faresAvailable", av.get("FaresAvailable", []))
        if isinstance(fares_available, list):
            for fa_entry in fares_available:
                fak = fa_entry.get("key", "")
                val = fa_entry.get("value", fa_entry)
                if not isinstance(val, dict):
                    continue
                for fare in val.get("fares", val.get("Fares", [])):
                    for pf in fare.get("passengerFares", fare.get("PassengerFares", [])):
                        amount = pf.get("fareAmount", pf.get("FareAmount", 0))
                        if amount and amount > 0:
                            if fak not in fare_lookup or amount < fare_lookup[fak]:
                                fare_lookup[fak] = amount
        elif isinstance(fares_available, dict):
            for fak, val in fares_available.items():
                if not isinstance(val, dict):
                    continue
                for fare in val.get("fares", val.get("Fares", [])):
                    for pf in fare.get("passengerFares", fare.get("PassengerFares", [])):
                        amount = pf.get("fareAmount", pf.get("FareAmount", 0))
                        if amount and amount > 0:
                            if fak not in fare_lookup or amount < fare_lookup[fak]:
                                fare_lookup[fak] = amount

        # Parse journeys
        journeys_by_market_ = av.get(
            "journeysAvailableByMarket",
            av.get("JourneysAvailableByMarket", {}),
        )
        if isinstance(journeys_by_market_, list):
            for entry in journeys_by_market_:
                journeys = entry.get("value", entry.get("journeys", []))
                if isinstance(journeys, list):
                    for j in journeys:
                        offer = self._parse_navitaire_journey(j, fare_lookup, currency, req)
                        if offer:
                            offers.append(offer)
        elif isinstance(journeys_by_market_, dict):
            for market_key, journeys in journeys_by_market_.items():
                if isinstance(journeys, list):
                    for j in journeys:
                        offer = self._parse_navitaire_journey(j, fare_lookup, currency, req)
                        if offer:
                            offers.append(offer)

        return offers

    def _parse_navitaire_journey(
        self, journey: dict, fare_lookup: dict, currency: str, req: FlightSearchRequest
    ) -> Optional[FlightOffer]:
        """Parse a single Navitaire journey into a FlightOffer."""
        if not isinstance(journey, dict):
            return None

        segments_raw = journey.get("segments", journey.get("Segments", []))
        if not segments_raw:
            return None

        # Find cheapest applicable fare
        fares_keys = journey.get("fares", journey.get("Fares", []))
        price = None
        for fk in fares_keys:
            key = fk.get("fareAvailabilityKey", fk.get("FareAvailabilityKey", "")) if isinstance(fk, dict) else str(fk)
            if key in fare_lookup:
                candidate = fare_lookup[key]
                if price is None or candidate < price:
                    price = candidate

        if not price or price <= 0:
            return None

        segments = []
        for seg in segments_raw:
            dep_str = seg.get("designator", seg.get("Designator", {})).get(
                "departure", seg.get("departureTime", "")
            ) if isinstance(seg.get("designator", seg.get("Designator")), dict) else seg.get("departureTime", "")
            arr_str = seg.get("designator", seg.get("Designator", {})).get(
                "arrival", seg.get("arrivalTime", "")
            ) if isinstance(seg.get("designator", seg.get("Designator")), dict) else seg.get("arrivalTime", "")
            origin = seg.get("designator", seg.get("Designator", {})).get(
                "origin", seg.get("origin", req.origin)
            ) if isinstance(seg.get("designator", seg.get("Designator")), dict) else seg.get("origin", req.origin)
            dest = seg.get("designator", seg.get("Designator", {})).get(
                "destination", seg.get("destination", req.destination)
            ) if isinstance(seg.get("designator", seg.get("Designator")), dict) else seg.get("destination", req.destination)

            identifier = seg.get("identifier", seg.get("Identifier", {}))
            carrier = identifier.get("carrierCode", identifier.get("CarrierCode", "QR")) if isinstance(identifier, dict) else "QR"
            flight_num = identifier.get("identifier", identifier.get("Identifier", "")) if isinstance(identifier, dict) else ""

            dep_dt = _parse_datetime(dep_str) if dep_str else _to_datetime(req.date_from)
            arr_dt = _parse_datetime(arr_str) if arr_str else dep_dt + timedelta(hours=2)

            dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Qatar Airways" if carrier == "QR" else carrier,
                flight_no=f"{carrier}{flight_num}",
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur,
                cabin_class="economy",
            ))

        if not segments:
            return None

        total_dur = int(
            (segments[-1].arrival - segments[0].departure).total_seconds()
        ) if len(segments) > 0 else 0

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=max(len(segments) - 1, 0),
        )

        offer_key = f"qr_{req.origin}_{req.destination}_{segments[0].departure.isoformat()}_{price}"
        offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]

        all_airlines = list({s.airline for s in segments})

        return FlightOffer(
            id=f"qr_{offer_id}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:,.2f} {currency}",
            outbound=route,
            airlines=[("Qatar Airways" if a == "QR" else a) for a in all_airlines],
            owner_airline="QR",
            booking_url=self._user_booking_url(req),
            is_locked=False,
            source="qatar_direct",
            source_tier="free",
        )

    def _parse_nbx(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse NBX booking engine response format."""
        offers: list[FlightOffer] = []

        # NBX wraps data in response.data or just data
        inner = data
        for key in ("response", "data", "result"):
            if key in inner and isinstance(inner[key], dict):
                inner = inner[key]

        # Look for flight offers array
        flight_list = (
            inner.get("flightOffers")
            or inner.get("flights")
            or inner.get("offers")
            or inner.get("flightList")
            or inner.get("originDestinationOptionList")
            or []
        )
        if not isinstance(flight_list, list):
            return offers

        currency = (
            inner.get("currency")
            or inner.get("currencyCode")
            or inner.get("originalCurrency")
            or "USD"
        )

        for flight in flight_list:
            if not isinstance(flight, dict):
                continue
            if flight.get("soldOut"):
                continue

            # Price extraction
            price = self._extract_price(flight)
            if not price or price <= 0:
                continue

            # Segments
            seg_list = (
                flight.get("segments")
                or flight.get("segmentList")
                or flight.get("legs")
                or flight.get("flightSegments")
                or []
            )

            segments = []
            for seg in seg_list:
                if not isinstance(seg, dict):
                    continue

                dep_str = (
                    seg.get("departureDateTime")
                    or seg.get("departureTime")
                    or seg.get("departure")
                    or seg.get("localDepartureDateTime")
                    or ""
                )
                arr_str = (
                    seg.get("arrivalDateTime")
                    or seg.get("arrivalTime")
                    or seg.get("arrival")
                    or seg.get("localArrivalDateTime")
                    or ""
                )
                origin = (
                    seg.get("departureAirportCode")
                    or seg.get("origin")
                    or seg.get("from")
                    or req.origin
                )
                dest = (
                    seg.get("arrivalAirportCode")
                    or seg.get("destination")
                    or seg.get("to")
                    or req.destination
                )
                carrier = (
                    seg.get("airlineCode")
                    or seg.get("carrierCode")
                    or seg.get("airline")
                    or seg.get("operatingCarrier")
                    or "QR"
                )
                fno = (
                    seg.get("flightNumber")
                    or seg.get("flightNo")
                    or seg.get("flight_no")
                    or ""
                )

                dep_dt = _parse_datetime(dep_str) if dep_str else _to_datetime(req.date_from)
                arr_dt = _parse_datetime(arr_str) if arr_str else dep_dt + timedelta(hours=2)
                dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

                segments.append(FlightSegment(
                    airline=carrier,
                    airline_name="Qatar Airways" if carrier == "QR" else carrier,
                    flight_no=f"{carrier}{fno}" if fno and not fno.startswith(carrier) else fno,
                    origin=origin,
                    destination=dest,
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=dur,
                    cabin_class="economy",
                ))

            if not segments:
                continue

            total_dur = int(
                (segments[-1].arrival - segments[0].departure).total_seconds()
            ) if segments else 0

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur,
                stopovers=max(len(segments) - 1, 0),
            )

            offer_key = f"qr_{req.origin}_{req.destination}_{segments[0].departure.isoformat()}_{price}"
            offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]
            all_airlines = list({s.airline for s in segments})

            offers.append(FlightOffer(
                id=f"qr_{offer_id}",
                price=round(price, 2),
                currency=currency,
                outbound=route,
                airlines=[("Qatar Airways" if a == "QR" else a) for a in all_airlines],
                owner_airline="QR",
                booking_url=self._user_booking_url(req),
                is_locked=False,
                source="qatar_direct",
                source_tier="free",
            ))

        return offers

    def _parse_generic(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Last-resort parser for unknown response structures."""
        # Walk JSON recursively looking for arrays of objects with price + airport fields
        offers: list[FlightOffer] = []
        self._walk_json(data, req, offers, depth=0)
        return offers

    def _walk_json(
        self, obj, req: FlightSearchRequest, offers: list, depth: int
    ) -> None:
        if depth > 6 or len(offers) >= 50:
            return
        if isinstance(obj, list) and len(obj) >= 2:
            # Check if this looks like a flight list
            sample = obj[0] if isinstance(obj[0], dict) else None
            if sample and self._extract_price(sample):
                for item in obj[:50]:
                    if not isinstance(item, dict):
                        continue
                    price = self._extract_price(item)
                    if not price or price <= 0:
                        continue
                    offer_key = f"qr_generic_{len(offers)}_{price}"
                    offers.append(FlightOffer(
                        id=f"qr_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
                        price=round(price, 2),
                        currency="USD",
                        outbound=FlightRoute(
                            segments=[FlightSegment(
                                airline="QR",
                                airline_name="Qatar Airways",
                                origin=req.origin,
                                destination=req.destination,
                                departure=_to_datetime(req.date_from),
                                arrival=_to_datetime(req.date_from) + timedelta(hours=5),
                            )],
                        ),
                        airlines=["Qatar Airways"],
                        owner_airline="QR",
                        booking_url=self._user_booking_url(req),
                        is_locked=False,
                        source="qatar_direct",
                        source_tier="free",
                    ))
                return
        if isinstance(obj, dict):
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    self._walk_json(v, req, offers, depth + 1)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_price(obj: dict) -> Optional[float]:
        """Try to extract a price from various field names."""
        for key in (
            "startingPrice", "price", "totalPrice", "amount",
            "fareAmount", "total", "adultFare", "baseFare",
        ):
            val = obj.get(key)
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
            if isinstance(val, dict):
                for inner_key in ("amount", "total", "value", "grossPrice"):
                    inner = val.get(inner_key)
                    if isinstance(inner, (int, float)) and inner > 0:
                        return float(inner)
        return None

    @staticmethod
    def _extract_currency(data: dict, req: FlightSearchRequest) -> str:
        """Extract currency from availability data."""
        for key in ("currencyCode", "currency", "originalCurrency", "CurrencyCode"):
            val = data.get(key)
            if isinstance(val, str) and len(val) == 3:
                return val
        # Walk one level
        for v in data.values():
            if isinstance(v, dict):
                for key in ("currencyCode", "currency"):
                    val = v.get(key)
                    if isinstance(val, str) and len(val) == 3:
                        return val
        return req.currency or "USD"

    @staticmethod
    def _user_booking_url(req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        return (
            f"https://www.qatarairways.com/en/booking.html"
            f"?from={req.origin}&to={req.destination}"
            f"&departing={dt.strftime('%Y-%m-%d')}"
            f"&adults={req.adults or 1}"
            f"&tripType=O&bookingClass=E"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"qr{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "USD",
            offers=[],
            total_results=0,
        )
