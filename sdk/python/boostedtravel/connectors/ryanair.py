"""
Ryanair direct API scraper — queries Ryanair's public REST API.

Ryanair exposes a rich internal REST API used by their SPA frontend.
These endpoints are publicly accessible without authentication.
This is the DEFINITIVE source for Ryanair pricing — no middleman markup.

Implements the Ryanair AIP (api/protocols/ryanair.json).
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any, Optional

import httpx

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

RYANAIR_API = "https://www.ryanair.com/api"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}


class RyanairConnectorClient:
    """Direct scraper for Ryanair's public API — zero auth, real-time prices."""

    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout,
                headers=_HEADERS,
                follow_redirects=True,
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Ryanair availability API.

        GET /booking/v4/en-gb/availability
        Returns real-time fares directly from Ryanair — no middleman.
        """
        client = await self._client()

        params: dict[str, Any] = {
            "ADT": req.adults,
            "CHD": req.children,
            "INF": req.infants,
            "DateOut": req.date_from.isoformat(),
            "Origin": req.origin,
            "Destination": req.destination,
            "RoundTrip": "true" if req.return_from else "false",
            "ToUs": "AGREED",
            "IncludeConnectingFlights": "true",
            "FlexDaysBeforeOut": 0,
            "FlexDaysOut": 0,
            "FlexDaysBeforeIn": 0,
            "FlexDaysIn": 0,
            "promoCode": "",
        }

        if req.return_from:
            params["DateIn"] = req.return_from.isoformat()
        else:
            params["DateIn"] = ""

        t0 = time.monotonic()

        try:
            resp = await client.get(
                f"{RYANAIR_API}/booking/v4/en-gb/availability",
                params=params,
            )
        except httpx.TimeoutException:
            logger.warning("Ryanair API timed out")
            return self._empty(req)
        except Exception as e:
            logger.error("Ryanair API error: %s", e)
            return self._empty(req)

        elapsed = time.monotonic() - t0

        if resp.status_code != 200:
            logger.warning(
                "Ryanair API returned %d: %s",
                resp.status_code,
                resp.text[:300],
            )
            return self._empty(req)

        try:
            data = resp.json()
        except Exception:
            logger.warning("Ryanair returned non-JSON response")
            return self._empty(req)

        currency = data.get("currency", req.currency)
        trips = data.get("trips", [])

        # Parse outbound and return trip data
        outbound_flights = []
        return_flights = []

        for trip in trips:
            for date_entry in trip.get("dates", []):
                for flight in date_entry.get("flights", []):
                    if not flight.get("regularFare"):
                        continue  # Skip sold-out flights
                    parsed = self._parse_flight(flight, currency, trip)
                    if parsed:
                        # Determine if outbound or return based on trip index
                        if trip == trips[0]:
                            outbound_flights.append(parsed)
                        else:
                            return_flights.append(parsed)

        # Build offers: if round trip, combine outbound × return (cheapest combos)
        offers = []
        if req.return_from and return_flights:
            # Combine best outbound with best return
            outbound_flights.sort(key=lambda x: x["price"])
            return_flights.sort(key=lambda x: x["price"])

            # Take top outbound and return (limit combos to avoid explosion)
            for ob in outbound_flights[:15]:
                for rt in return_flights[:10]:
                    total = ob["price"] + rt["price"]
                    offer = FlightOffer(
                        id=f"ry_{hashlib.md5((ob['key'] + rt['key']).encode()).hexdigest()[:12]}",
                        price=round(total, 2),
                        currency=currency,
                        price_formatted=f"{total:.2f} {currency}",
                        outbound=ob["route"],
                        inbound=rt["route"],
                        airlines=["Ryanair"],
                        owner_airline="FR",
                        booking_url=self._build_booking_url(req),
                        is_locked=False,
                        source="ryanair_direct",
                        source_tier="free",
                    )
                    offers.append(offer)
        else:
            # One-way: each outbound is an offer
            for ob in outbound_flights:
                offer = FlightOffer(
                    id=f"ry_{hashlib.md5(ob['key'].encode()).hexdigest()[:12]}",
                    price=round(ob["price"], 2),
                    currency=currency,
                    price_formatted=f"{ob['price']:.2f} {currency}",
                    outbound=ob["route"],
                    inbound=None,
                    airlines=["Ryanair"],
                    owner_airline="FR",
                    booking_url=self._build_booking_url(req),
                    is_locked=False,
                    source="ryanair_direct",
                    source_tier="free",
                )
                offers.append(offer)

        # Sort by price
        offers.sort(key=lambda o: o.price)

        logger.info(
            "Ryanair direct %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"ryanair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=currency,
            offers=offers,
            total_results=len(offers),
        )

    def _parse_flight(
        self, flight: dict, currency: str, trip: dict
    ) -> Optional[dict]:
        """Parse a single Ryanair flight entry."""
        regular_fare = flight.get("regularFare")
        if not regular_fare:
            return None

        # Get total adult fare
        fares = regular_fare.get("fares", [])
        total_price = 0.0
        for fare in fares:
            total_price += float(fare.get("amount", 0)) * int(fare.get("count", 1))

        if total_price <= 0:
            return None

        # Parse segments
        segments_data = flight.get("segments", [])
        time_arr = flight.get("timeUTC", flight.get("time", []))

        segments = []
        for seg in segments_data:
            seg_time = seg.get("time", [])
            dep_str = seg_time[0] if len(seg_time) > 0 else ""
            arr_str = seg_time[1] if len(seg_time) > 1 else ""

            dep_dt = self._parse_dt(dep_str)
            arr_dt = self._parse_dt(arr_str)

            flight_no = seg.get("flightNumber", "").replace(" ", "")

            segments.append(FlightSegment(
                airline="FR",
                airline_name="Ryanair",
                flight_no=flight_no,
                origin=seg.get("origin", trip.get("origin", "")),
                destination=seg.get("destination", trip.get("destination", "")),
                departure=dep_dt,
                arrival=arr_dt,
                cabin_class="M",
            ))

        if not segments:
            # Fallback: build from trip-level time
            dep_str = time_arr[0] if len(time_arr) > 0 else ""
            arr_str = time_arr[1] if len(time_arr) > 1 else ""

            segments.append(FlightSegment(
                airline="FR",
                airline_name="Ryanair",
                flight_no=flight.get("flightNumber", ""),
                origin=trip.get("origin", ""),
                destination=trip.get("destination", ""),
                departure=self._parse_dt(dep_str),
                arrival=self._parse_dt(arr_str),
                cabin_class="M",
            ))

        total_dur = 0
        if len(segments) >= 1:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = flight.get("flightKey", f"{segments[0].flight_no}_{segments[0].departure.isoformat()}")

        return {
            "price": total_price,
            "key": flight_key,
            "route": route,
            "seats_left": flight.get("faresLeft", -1),
        }

    def _parse_dt(self, s: str) -> datetime:
        """Parse Ryanair datetime string."""
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
        """Build a direct Ryanair booking URL."""
        date_out = req.date_from.isoformat()
        date_in = req.return_from.isoformat() if req.return_from else ""
        is_return = "true" if req.return_from else "false"
        return (
            f"https://www.ryanair.com/gb/en/trip/flights/select"
            f"?adults={req.adults}&teens=0&children={req.children}"
            f"&infants={req.infants}&dateOut={date_out}&dateIn={date_in}"
            f"&isConnectedFlight=false&discount=0&isReturn={is_return}"
            f"&promoCode=&originIata={req.origin}&destinationIata={req.destination}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"ryanair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
