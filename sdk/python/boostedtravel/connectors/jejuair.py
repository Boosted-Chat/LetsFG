"""
Jeju Air direct API scraper -- queries sec.jejuair.net REST API.

Jeju Air (IATA: 7C) is a South Korean LCC based at Jeju International Airport.
Website: www.jejuair.net

API backend: sec.jejuair.net (publicly accessible, no auth/session required)
  - Lowest fare calendar: POST /en/ibe/booking/searchlowestFareCalendarInPeriod.json
    Headers: Content-Type: application/json, Channel-Code: WPC
  - Station list: POST /en/ibe/booking/selectDepartureStations.json
  - Arrival stations: POST /en/ibe/booking/selectArrivalStations.json

Discovered via network interception probes, Mar 2026.
Rewritten from 1000+ line Playwright scraper to direct httpx API client.
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timedelta
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

_BASE = "https://sec.jejuair.net"
_LOWFARE_URL = f"{_BASE}/en/ibe/booking/searchlowestFareCalendarInPeriod.json"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/json",
    "Channel-Code": "WPC",
    "Origin": "https://www.jejuair.net",
    "Referer": "https://www.jejuair.net/",
}


class JejuAirConnectorClient:
    """Jeju Air scraper -- direct httpx API client for lowest fare calendar."""

    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")
        end_str = (req.date_from + timedelta(days=6)).strftime("%Y-%m-%d")

        payload = {
            "tripRoute": [{
                "searchStartDate": date_str,
                "searchEndDate": end_str,
                "originAirport": req.origin,
                "destinationAirport": req.destination,
            }],
            "passengers": [{"type": "ADT", "count": str(req.adults)}],
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    _LOWFARE_URL, headers=_HEADERS, json=payload,
                )
        except httpx.HTTPError as exc:
            logger.error("JejuAir API request failed: %s", exc)
            return self._empty(req)

        if resp.status_code != 200:
            logger.warning("JejuAir API returned %d", resp.status_code)
            return self._empty(req)

        try:
            data = resp.json()
        except Exception:
            logger.warning("JejuAir API returned non-JSON response")
            return self._empty(req)

        if data.get("code") != "0000":
            logger.warning("JejuAir API error: %s", data.get("message", "unknown"))
            return self._empty(req)

        offers = self._parse_lowfare(data, req)
        elapsed = time.monotonic() - t0
        offers.sort(key=lambda o: o.price)
        logger.info(
            "JejuAir %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_id = hashlib.md5(
            f"jejuair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_id}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=offers,
            total_results=len(offers),
        )

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_lowfare(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        lowfares_obj = data.get("data", {}).get("lowfares", {})
        if isinstance(lowfares_obj, list):
            # Old format: data.lowfares is a list directly
            markets = lowfares_obj
        else:
            # Current format: data.lowfares.lowFareDateMarkets
            markets = lowfares_obj.get("lowFareDateMarkets", [])

        if not markets:
            return []

        booking_url = self._booking_url(req)
        offers: list[FlightOffer] = []

        for item in markets:
            if not isinstance(item, dict):
                continue
            if item.get("noFlights"):
                continue

            fare_info = item.get("lowestFareAmount", {})
            if not fare_info:
                continue

            fare_amount = fare_info.get("fareAmount", 0) or 0
            tax_amount = fare_info.get("taxesAndFeesAmount", 0) or 0
            total = fare_amount + tax_amount
            if total <= 0:
                continue

            dep_date_raw = item.get("departureDate", "")
            dep_date_str = dep_date_raw[:10] if dep_date_raw else ""
            if not dep_date_str:
                continue

            currency = (
                lowfares_obj.get("currencyCode")
                if isinstance(lowfares_obj, dict)
                else fare_info.get("currencyCode", req.currency)
            ) or req.currency

            dep_dt = self._parse_dt(dep_date_raw)
            origin = item.get("origin", req.origin)
            dest = item.get("destination", req.destination)

            fid = hashlib.md5(
                f"7c_{origin}{dest}{dep_date_str}{total}".encode()
            ).hexdigest()[:12]

            seg = FlightSegment(
                airline="7C",
                airline_name="Jeju Air",
                flight_no="",
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=dep_dt,  # exact time not available from lowfare calendar
                cabin_class="M",
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)

            offers.append(FlightOffer(
                id=f"7c_{fid}",
                price=round(total, 2),
                currency=currency,
                price_formatted=f"{total:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Jeju Air"],
                owner_airline="7C",
                booking_url=booking_url,
                is_locked=False,
                source="jejuair_direct",
                source_tier="free",
            ))

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s).strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.jejuair.net/en/ibe/booking/Availability.do"
            f"?origin={req.origin}&destination={req.destination}"
            f"&departure={dep}&adults={req.adults}&tripType=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"jejuair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
