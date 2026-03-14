"""
Nok Air direct API scraper — queries Sabre EzyCommerce REST API.

Nok Air (IATA: DD) is a Thai low-cost carrier based at Don Mueang (DMK).
Their booking site (booking.nokair.com) is a Vue.js SPA backed by the
Sabre EzyCommerce availability API. The API is publicly accessible
with static headers — no auth tokens, cookies, or sessions required.

Endpoint: POST nokair-api.ezycommerce.sabre.com/api/v1/Availability/SearchShop
Discovered via network interception, Mar 2026.
Rewritten from ~560-line Playwright scraper to direct httpx API client.
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

_API_BASE = "https://nokair-api.ezycommerce.sabre.com/api/v1"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/plain",
    "Content-Type": "application/json;charset=UTF-8",
    "channel": "web",
    "appcontext": "ibe",
    "tenant-identifier": (
        "FkDcDjsr3Po6GAHFnBh48dHff8MvWpCMfkKyXJ3WVQ7frJ68bD2ubXZDx6sPFRTW"
    ),
    "x-clientversion": "0.5.3937",
    "languagecode": "en-us",
    "Origin": "https://booking.nokair.com",
    "Referer": "https://booking.nokair.com/",
}


class NokAirConnectorClient:
    """Direct scraper for Nok Air's Sabre EzyCommerce API — zero auth, real-time prices."""

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
        Search Nok Air availability via the Sabre EzyCommerce API.

        POST /api/v1/Availability/SearchShop
        Returns real-time fares — no browser, no auth.
        """
        client = await self._client()
        date_str = req.date_from.strftime("%Y-%m-%d")

        passengers = [{"code": "ADT", "count": req.adults}]
        if req.children:
            passengers.append({"code": "CHD", "count": req.children})
        if req.infants:
            passengers.append({"code": "INF", "count": req.infants})

        routes = [{
            "fromAirport": req.origin,
            "toAirport": req.destination,
            "startDate": date_str,
            "endDate": date_str,
            "departureDate": None,
            "segmentKey": None,
            "cabin": None,
        }]

        if req.return_from:
            ret_str = req.return_from.strftime("%Y-%m-%d")
            routes.append({
                "fromAirport": req.destination,
                "toAirport": req.origin,
                "startDate": ret_str,
                "endDate": ret_str,
                "departureDate": None,
                "segmentKey": None,
                "cabin": None,
            })

        body = {
            "languageCode": "en-us",
            "currency": "THB",
            "passengers": passengers,
            "routes": routes,
            "promoCode": "",
            "filterMethod": "102",
            "fareTypeCategories": [1],
            "isManageBooking": False,
            "sanlamSubscriptionId": None,
            "externalProfileId": None,
            "fareTypeFilters": [],
            "fareClass": None,
        }

        t0 = time.monotonic()

        try:
            resp = await client.post(
                f"{_API_BASE}/Availability/SearchShop",
                json=body,
            )
        except httpx.TimeoutException:
            logger.warning("NokAir API timed out")
            return self._empty(req)
        except Exception as e:
            logger.error("NokAir API error: %s", e)
            return self._empty(req)

        elapsed = time.monotonic() - t0

        if resp.status_code != 200:
            logger.warning(
                "NokAir API returned %d: %s",
                resp.status_code,
                resp.text[:300],
            )
            return self._empty(req)

        try:
            data = resp.json()
        except Exception:
            logger.warning("NokAir returned non-JSON response")
            return self._empty(req)

        currency = data.get("currency", "THB")
        api_routes = data.get("routes", [])

        outbound_flights = []
        return_flights = []

        for i, route in enumerate(api_routes):
            flights = route.get("flights", [])
            for flight in flights:
                parsed = self._parse_flight(flight, currency)
                if parsed:
                    if i == 0:
                        outbound_flights.append(parsed)
                    else:
                        return_flights.append(parsed)

        offers = []
        booking_url = self._build_booking_url(req)

        if req.return_from and return_flights:
            outbound_flights.sort(key=lambda x: x["price"])
            return_flights.sort(key=lambda x: x["price"])

            for ob in outbound_flights[:15]:
                for rt in return_flights[:10]:
                    total = ob["price"] + rt["price"]
                    offer = FlightOffer(
                        id=f"dd_{hashlib.md5((ob['key'] + rt['key']).encode()).hexdigest()[:12]}",
                        price=round(total, 2),
                        currency=currency,
                        price_formatted=f"{total:.2f} {currency}",
                        outbound=ob["route"],
                        inbound=rt["route"],
                        airlines=["Nok Air"],
                        owner_airline="DD",
                        booking_url=booking_url,
                        is_locked=False,
                        source="nokair_direct",
                        source_tier="free",
                    )
                    offers.append(offer)
        else:
            for ob in outbound_flights:
                offer = FlightOffer(
                    id=f"dd_{hashlib.md5(ob['key'].encode()).hexdigest()[:12]}",
                    price=round(ob["price"], 2),
                    currency=currency,
                    price_formatted=f"{ob['price']:.2f} {currency}",
                    outbound=ob["route"],
                    inbound=None,
                    airlines=["Nok Air"],
                    owner_airline="DD",
                    booking_url=booking_url,
                    is_locked=False,
                    source="nokair_direct",
                    source_tier="free",
                )
                offers.append(offer)

        offers.sort(key=lambda o: o.price)

        logger.info(
            "NokAir direct %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"nokair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=currency,
            offers=offers,
            total_results=len(offers),
        )

    def _parse_flight(self, flight: dict, currency: str) -> Optional[dict]:
        """Parse a single flight from the Sabre SearchShop response."""
        if flight.get("soldOut") or flight.get("soldout"):
            return None

        lowest_price = flight.get("lowestPriceTotal") or flight.get("lowestPriceSinglePax")
        if not lowest_price or lowest_price <= 0:
            for ft in flight.get("fareTypes", []):
                for fare in ft.get("fares", []):
                    if not fare.get("soldOut"):
                        p = fare.get("price", 0)
                        if p > 0 and (lowest_price is None or p < lowest_price):
                            lowest_price = p
            if not lowest_price or lowest_price <= 0:
                return None

        legs = flight.get("legs", [])
        segments = []
        for leg in legs:
            dep_str = leg.get("departureDate", "")
            arr_str = leg.get("arrivalDate", "")
            segments.append(FlightSegment(
                airline=leg.get("carrierCode", "DD"),
                airline_name="Nok Air",
                flight_no=f"DD{leg.get('flightNumber', flight.get('flightNumber', ''))}",
                origin=leg.get("from", {}).get("code", ""),
                destination=leg.get("to", {}).get("code", ""),
                departure=self._parse_dt(dep_str),
                arrival=self._parse_dt(arr_str),
                cabin_class="M",
            ))

        if not segments:
            segments.append(FlightSegment(
                airline=flight.get("carrierCode", "DD"),
                airline_name="Nok Air",
                flight_no=f"DD{flight.get('flightNumber', '')}",
                origin="",
                destination="",
                departure=self._parse_dt(flight.get("departureDate", "")),
                arrival=self._parse_dt(flight.get("arrivalDate", "")),
                cabin_class="M",
            ))

        flight_time = flight.get("flightTime", 0)
        total_dur = flight_time * 60 if flight_time else 0
        if not total_dur and len(segments) >= 1:
            total_dur = max(
                int((segments[-1].arrival - segments[0].departure).total_seconds()),
                0,
            )

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = str(
            flight.get("key")
            or flight.get("id")
            or f"{segments[0].flight_no}_{segments[0].departure.isoformat()}"
        )

        return {
            "price": float(lowest_price),
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
            f"https://booking.nokair.com/en/search?origin={req.origin}"
            f"&destination={req.destination}&departure={dep}&adults={req.adults}&tripType=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"nokair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
