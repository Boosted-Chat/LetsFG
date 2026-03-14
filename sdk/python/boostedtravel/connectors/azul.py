"""
Azul Brazilian Airlines scraper — headed Chrome + route interception + API capture.

Azul (IATA: AD) is Brazil's third-largest airline with the widest domestic network.
Website: www.voeazul.com.br — English version at /us/en/home.

Architecture:
- React SPA frontend with Navitaire/New Skies backend
- Akamai Bot Manager blocks headless Chrome and non-browser HTTP
- SPA sends empty criteria to availability API regardless of URL params
- Route interception rewrites the empty payload with correct NSK-format criteria

Strategy:
1. Launch persistent headed Chrome (Akamai blocks headless)
2. Per search: set up route interception → navigate to booking URL
3. Route handler rewrites SPA's empty criteria with correct payload
4. Capture availability response → parse Navitaire format → FlightOffer objects

Performance: ~5-8s first search (Chrome launch), ~3-5s subsequent searches.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
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

# ── Constants ────────────────────────────────────────────────────────────

_AVAIL_API = "reservationavailability/api/reservation/availability"
_MAX_ATTEMPTS = 3
_API_WAIT = 30  # seconds to wait for availability API per attempt

_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", ".azul_chrome_data"
)

# ── Persistent browser context (headed to bypass Akamai) ────────────────

_pw_instance = None
_pw_context = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """
    Persistent headed Chrome context — cookies survive across searches
    so the Akamai challenge only needs to pass once.
    """
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

        os.makedirs(os.path.abspath(_USER_DATA_DIR), exist_ok=True)
        _pw_instance = await async_playwright().start()

        _pw_context = await _pw_instance.chromium.launch_persistent_context(
            os.path.abspath(_USER_DATA_DIR),
            channel="chrome",
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
            ],
            viewport={"width": 1366, "height": 768},
            locale="en-US",
            timezone_id="America/Sao_Paulo",
            service_workers="block",
        )
        logger.info("Azul: persistent Chrome context ready")
        return _pw_context


class AzulConnectorClient:
    """Azul scraper — headed Chrome + route interception + API capture.

    Uses persistent headed Chrome (Akamai blocks headless). Browser launched once,
    reused across searches. SPA sends empty criteria which route interception
    rewrites with the correct NSK-format payload. ~3-5s per search after launch.
    """

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                result = await self._attempt_search(req, t0)
                if result is not None:
                    return result
            except Exception as e:
                logger.warning("Azul: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    async def _attempt_search(
        self, req: FlightSearchRequest, t0: float
    ) -> Optional[FlightSearchResponse]:
        """Single attempt: fresh page + route interception → navigate → capture API."""
        ctx = await _get_context()

        booking_url = self._build_booking_url(req)
        dep = req.date_from.strftime("%Y-%m-%d")

        # Build correct NSK criteria payload (SPA sends empty criteria)
        correct_payload = json.dumps({
            "criteria": [{
                "DepartureStation": req.origin,
                "ArrivalStation": req.destination,
                "Std": dep + "T00:00:00",
            }],
            "passengers": [{"type": "ADT", "count": req.adults}],
            "flexibleDays": {"daysToLeft": 0, "daysToRight": 0},
            "currencyCode": "BRL",
        })

        # Fresh page per search
        page = await ctx.new_page()

        # Route interception: rewrite empty criteria with correct payload
        async def intercept_avail(route):
            request = route.request
            if request.method == "POST" and "v5/availability" in request.url:
                post_data = request.post_data
                if post_data:
                    try:
                        pd = json.loads(post_data)
                        if not pd.get("criteria"):
                            await route.continue_(post_data=correct_payload)
                            return
                    except Exception:
                        pass
                await route.continue_()
            else:
                await route.continue_()

        await page.route("**/b2c-api.voeazul.com.br/**/availability**", intercept_avail)

        # Capture API response
        captured: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                if "v5/availability" not in response.url:
                    return
                if response.status != 200:
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                body = await response.json()
                if isinstance(body, dict) and body:
                    captured["avail"] = body
                    api_event.set()
            except Exception:
                pass

        page.on("response", on_response)

        logger.info("Azul: searching %s→%s on %s", req.origin, req.destination, dep)

        try:
            await page.goto(
                booking_url,
                wait_until="commit",
                timeout=60000,
            )

            # Wait for availability API response
            await asyncio.wait_for(api_event.wait(), timeout=_API_WAIT)

        except asyncio.TimeoutError:
            logger.warning("Azul: availability API timed out")
            return None
        except Exception as e:
            logger.warning("Azul: navigation error: %s", e)
            return None
        finally:
            try:
                page.remove_listener("response", on_response)
            except Exception:
                pass
            try:
                await page.unroute("**/b2c-api.voeazul.com.br/**/availability**")
            except Exception:
                pass
            try:
                await page.close()
            except Exception:
                pass

        data = captured.get("avail")
        if data is None:
            return None

        elapsed = time.monotonic() - t0
        offers = self._parse_availability(data, req)
        return self._build_response(offers, req, elapsed)

    # ── Navitaire availability parsing ───────────────────────────────────

    def _parse_availability(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        trips = data.get("data", {}).get("trips") or data.get("trips") or []
        for trip in trips:
            journeys = trip.get("journeys") or trip.get("journeysAvailable") or []
            if not isinstance(journeys, list):
                continue
            for journey in journeys:
                offer = self._parse_journey(journey, req, booking_url)
                if offer:
                    offers.append(offer)

        return offers

    def _parse_journey(
        self, journey: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        """Parse a single Navitaire journey into a FlightOffer."""
        best_price = self._extract_journey_price(journey)
        if best_price is None or best_price <= 0:
            return None

        currency = self._extract_currency(journey) or "BRL"

        # Azul v5 uses "identifier" instead of "designator"
        identifier = journey.get("identifier") or journey.get("designator") or {}
        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []

        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._parse_segment(seg, req))
        else:
            dep_str = (
                identifier.get("std") or identifier.get("departure")
                or journey.get("departureDateTime") or ""
            )
            arr_str = (
                identifier.get("sta") or identifier.get("arrival")
                or journey.get("arrivalDateTime") or ""
            )
            origin = (
                identifier.get("departureStation") or identifier.get("origin")
                or req.origin
            )
            dest = (
                identifier.get("arrivalStation") or identifier.get("destination")
                or req.destination
            )
            carrier = identifier.get("carrierCode") or "AD"
            flight_num = str(identifier.get("flightNumber") or "")
            segments.append(FlightSegment(
                airline=carrier, airline_name="Azul",
                flight_no=f"{carrier}{flight_num}" if flight_num else "",
                origin=origin, destination=dest,
                departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
                cabin_class="M",
            ))

        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            diff = (segments[-1].arrival - segments[0].departure).total_seconds()
            total_dur = int(diff) if diff > 0 else 0

        connections = identifier.get("connections")
        stops = len(connections) if isinstance(connections, list) else max(len(segments) - 1, 0)
        route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=stops)

        journey_key = journey.get("journeyKey") or ""
        if not journey_key and segments:
            journey_key = f"{segments[0].departure.isoformat()}_{segments[0].flight_no}"

        return FlightOffer(
            id=f"ad_{hashlib.md5(journey_key.encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=list(set(s.airline for s in segments)) or ["AD"],
            owner_airline="AD", booking_url=booking_url,
            is_locked=False, source="azul_direct", source_tier="free",
        )

    def _parse_segment(self, seg: dict, req: FlightSearchRequest) -> FlightSegment:
        """Parse a Navitaire segment."""
        # Azul v5 uses "identifier" for segment-level info
        identifier = seg.get("identifier") or seg.get("designator") or {}
        flight_des = seg.get("flightDesignator") or {}

        dep_str = (
            identifier.get("std") or identifier.get("departure")
            or seg.get("departureDateTime") or seg.get("std") or ""
        )
        arr_str = (
            identifier.get("sta") or identifier.get("arrival")
            or seg.get("arrivalDateTime") or seg.get("sta") or ""
        )
        origin = (
            identifier.get("departureStation") or identifier.get("origin")
            or seg.get("departureStation") or req.origin
        )
        dest = (
            identifier.get("arrivalStation") or identifier.get("destination")
            or seg.get("arrivalStation") or req.destination
        )
        carrier = (
            identifier.get("carrierCode") or flight_des.get("carrierCode")
            or seg.get("carrierCode") or "AD"
        )
        flight_num = str(
            identifier.get("flightNumber") or flight_des.get("flightNumber")
            or seg.get("flightNumber") or ""
        )

        dep = self._parse_dt(dep_str)
        arr = self._parse_dt(arr_str)

        return FlightSegment(
            airline=carrier, airline_name="Azul",
            flight_no=f"{carrier}{flight_num}" if flight_num else "",
            origin=origin, destination=dest,
            departure=dep, arrival=arr,
            cabin_class="M",
        )

    @staticmethod
    def _extract_journey_price(journey: dict) -> Optional[float]:
        """Extract the cheapest fare price from a Navitaire journey."""
        best = float("inf")

        for fare in journey.get("fares", []):
            if not isinstance(fare, dict):
                continue
            # Azul v5 uses "paxFares" with "totalAmount"
            pax_fares = fare.get("paxFares") or fare.get("passengerFares") or []
            for pf in pax_fares:
                for key in ("totalAmount", "originalAmount", "fareAmount"):
                    val = pf.get(key)
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
                # serviceCharges fallback
                total_charge = 0.0
                for charge in pf.get("serviceCharges", []):
                    try:
                        total_charge += float(charge.get("amount", 0))
                    except (TypeError, ValueError):
                        pass
                if total_charge > 0 and total_charge < best:
                    best = total_charge

        return best if best < float("inf") else None

    @staticmethod
    def _extract_currency(journey: dict) -> Optional[str]:
        for fare in journey.get("fares", []):
            if not isinstance(fare, dict):
                continue
            for pf in fare.get("paxFares") or fare.get("passengerFares") or []:
                cc = pf.get("currencyCode")
                if cc:
                    return cc
                for charge in pf.get("serviceCharges", []):
                    cc = charge.get("currencyCode")
                    if cc:
                        return cc
        return None

    # ── Helpers ──────────────────────────────────────────────────────────

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
        return (
            f"https://www.voeazul.com.br/us/en/home/selecao-voo"
            f"?c0={req.origin}&c1={req.destination}&d1={dep}"
            f"&dt=ow&p1=ADT{req.adults}&px={req.adults}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"azul{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Azul %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(
            f"azul{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers, total_results=len(offers),
        )
