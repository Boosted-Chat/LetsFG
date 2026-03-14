"""
Spring Airlines direct API scraper -- queries en.ch.com REST endpoints.

Spring Airlines (IATA: 9C) is a Chinese LCC headquartered in Shanghai.
Website: en.ch.com (English), flights.ch.com (Chinese).

API backend: en.ch.com (publicly accessible, no auth key required)
  - Flight search:     POST /Flights/SearchByTime
  - Low price calendar: POST /Flights/MinPriceTrends
  - City/routes:       GET  /Default/GetReRoutesByCity?CityCode={code}&Lang=en-us
  - City detection:    GET  /Default/GetCity

Parameters (form-encoded):
  Departure, Arrival (city codes), DepartureDate (YYYY-MM-DD),
  Currency, AdtNum, ChdNum, InfNum, IsIJFlight, SType.

Discovered via Playwright network interception + JS analysis, Mar 2026.
Rewritten from 539-line Playwright scraper to direct httpx API client.
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

_BASE = "https://en.ch.com"
_BASE_CN = "https://flights.ch.com"
_SEARCH_URL = f"{_BASE}/Flights/SearchByTime"
_SEARCH_URL_CN = f"{_BASE_CN}/Flights/SearchByTime"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{_BASE}/flights",
    "Origin": _BASE,
}

_HEADERS_CN = {
    **_HEADERS,
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": f"{_BASE_CN}/",
    "Origin": _BASE_CN,
}


class SpringConnectorClient:
    """Spring Airlines scraper -- direct httpx API client for SearchByTime."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")

        form_data = {
            "Departure": req.origin,
            "Arrival": req.destination,
            "DepartureDate": date_str,
            "ReturnDate": "",
            "IsIJFlight": "false",
            "SType": "0",
            "Currency": req.currency or "CNY",
            "AdtNum": str(req.adults),
            "ChdNum": str(req.children),
            "InfNum": str(req.infants),
        }

        # Try English site first, then Chinese site for domestic routes
        for base, search_url, headers, cookies in (
            (_BASE, _SEARCH_URL, _HEADERS, {"lang": "en-us"}),
            (_BASE_CN, _SEARCH_URL_CN, _HEADERS_CN, {"lang": "zh-cn"}),
        ):
            data = await self._api_call(base, search_url, form_data, headers, cookies)
            if data and data.get("Code") == "0" and data.get("Route"):
                offers = self._parse_routes(data, req)
                if offers:
                    elapsed = time.monotonic() - t0
                    offers.sort(key=lambda o: o.price)
                    logger.info(
                        "Spring %s->%s returned %d offers in %.1fs (via %s)",
                        req.origin, req.destination, len(offers), elapsed, base,
                    )
                    search_id = hashlib.md5(
                        f"spring{req.origin}{req.destination}{req.date_from}".encode()
                    ).hexdigest()[:12]
                    return FlightSearchResponse(
                        search_id=f"fs_{search_id}",
                        origin=req.origin,
                        destination=req.destination,
                        currency=req.currency or "CNY",
                        offers=offers,
                        total_results=len(offers),
                    )

        # Neither site returned results
        logger.info("Spring %s->%s: no results from either site", req.origin, req.destination)
        return self._empty(req)

    async def _api_call(
        self, base: str, search_url: str, form_data: dict,
        headers: dict, cookies: dict,
    ) -> dict | None:
        """Make a single API call to a Spring Airlines endpoint."""
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout, follow_redirects=True,
                cookies=cookies,
            ) as client:
                # Get session cookie
                await client.get(
                    base,
                    headers={"User-Agent": headers["User-Agent"], "Accept": "text/html,*/*"},
                )
                resp = await client.post(search_url, data=form_data, headers=headers)
        except httpx.HTTPError as exc:
            logger.debug("Spring API call to %s failed: %s", base, exc)
            return None

        if resp.status_code != 200:
            logger.debug("Spring %s returned %d", base, resp.status_code)
            return None

        try:
            return resp.json()
        except Exception:
            return None

    def _parse_routes(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        raw_routes = data.get("Route", [])
        if not raw_routes:
            return []

        booking_url = self._booking_url(req)
        offers: list[FlightOffer] = []
        currency = req.currency or "CNY"

        for route in raw_routes:
            if not isinstance(route, list) or not route:
                continue

            # Each route is a list of segment dicts (1 for direct, 2+ for connecting)
            first = route[0]
            price = first.get("MinCabinPrice") or first.get("MinCabinPriceForDisplay") or 0
            if price <= 0:
                continue

            tax = first.get("RouteTotalTax", 0) or 0
            total_price = price + tax

            segments: list[FlightSegment] = []
            stopovers_list = first.get("Stopovers", [])

            if stopovers_list:
                # Connecting flight — segments come from Stopovers
                for stop in stopovers_list:
                    segments.append(self._build_segment(stop, req))
            else:
                # Direct flight — build from the route itself
                segments.append(self._build_segment(first, req))

            if not segments:
                continue

            # Calculate total duration
            total_dur = 0
            flight_time = first.get("FlightsTime", "") or first.get("FlightTime", "")
            if flight_time:
                total_dur = self._parse_duration(flight_time)
            elif segments[0].departure and segments[-1].arrival:
                delta = segments[-1].arrival - segments[0].departure
                total_dur = max(int(delta.total_seconds()), 0)

            route_obj = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur,
                stopovers=max(len(segments) - 1, 0),
            )

            seg_id = first.get("SegmentId", "")
            fid = hashlib.md5(
                f"9c_{req.origin}{req.destination}{seg_id}{price}".encode()
            ).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"9c_{fid}",
                price=round(total_price, 2),
                currency=currency,
                price_formatted=f"{total_price:.0f} {currency}",
                outbound=route_obj,
                inbound=None,
                airlines=["Spring Airlines"],
                owner_airline="9C",
                booking_url=booking_url,
                is_locked=False,
                source="spring_direct",
                source_tier="free",
            ))

        return offers

    def _build_segment(self, seg: dict, req: FlightSearchRequest) -> FlightSegment:
        flight_no = seg.get("No", "") or ""
        origin_code = seg.get("DepartureCode") or seg.get("DepartureAirportCode") or req.origin
        dest_code = seg.get("ArrivalCode") or seg.get("ArrivalAirportCode") or req.destination
        dep_str = seg.get("DepartureTime") or seg.get("DepartureTimeBJ") or ""
        arr_str = seg.get("ArrivalTime") or seg.get("ArrivalTimeBJ") or ""
        aircraft = seg.get("Type", "") or ""

        dep_dt = self._parse_dt(dep_str)
        arr_dt = self._parse_dt(arr_str)
        dur = 0
        if dep_dt and arr_dt:
            dur = max(int((arr_dt - dep_dt).total_seconds()), 0)

        return FlightSegment(
            airline="9C",
            airline_name="Spring Airlines",
            flight_no=flight_no,
            origin=origin_code,
            destination=dest_code,
            departure=dep_dt or datetime(2000, 1, 1),
            arrival=arr_dt or datetime(2000, 1, 1),
            duration_seconds=dur,
            cabin_class="economy",
            aircraft=aircraft,
        )

    @staticmethod
    def _parse_duration(s: str) -> int:
        """Parse '2 H 40 M' or '2H40M' to seconds."""
        import re
        m = re.search(r'(\d+)\s*[Hh]', s)
        mins_match = re.search(r'(\d+)\s*[Mm]', s)
        hours = int(m.group(1)) if m else 0
        mins = int(mins_match.group(1)) if mins_match else 0
        return hours * 3600 + mins * 60

    @staticmethod
    def _parse_dt(raw: str) -> Optional[datetime]:
        if not raw:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return None

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://en.ch.com/flights/{req.origin}-{req.destination}.html"
            f"?departure={dep}&adults={req.adults}&tripType=OW"
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        search_id = hashlib.md5(
            f"spring{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_id}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "CNY",
            offers=[],
            total_results=0,
        )
