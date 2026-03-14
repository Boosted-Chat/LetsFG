"""
Air India Express direct API scraper -- queries api.airindiaexpress.com REST API.

Air India Express (IATA: IX) is an Indian low-cost carrier (subsidiary of Air India).
Website: www.airindiaexpress.com

API backend: api.airindiaexpress.com (publicly accessible, subscription key required)
  - Low fare calendar: POST /b2c-flightsearch/v2/lowFares
    Headers: ocp-apim-subscription-key, Content-Type: application/json
  - Station list:   GET /b2c-flightsearch/v3/station/getSources
  - Destinations:   GET /b2c-flightsearch/v3/station/getDestinations/{IATA}

Discovered via headed-Chrome network interception, Mar 2026.
Rewritten from 496-line Playwright scraper to direct httpx API client.
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

_BASE = "https://api.airindiaexpress.com"
_LOWFARE_URL = f"{_BASE}/b2c-flightsearch/v2/lowFares"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Content-Type": "application/json",
    "ocp-apim-subscription-key": "fe65ec9eec2445d9802be1d6c0295158",
    "client-id": "AIRASIA-WEB-APP",
    "Origin": "https://www.airindiaexpress.com",
    "Referer": "https://www.airindiaexpress.com/",
}


class AirIndiaExpressConnectorClient:
    """Air India Express scraper -- direct httpx API client for low fare calendar."""

    def __init__(self, timeout: float = 20.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")
        end_str = (req.date_from + timedelta(days=6)).strftime("%Y-%m-%d")

        payload = {
            "startDate": date_str,
            "endDate": end_str,
            "origin": req.origin,
            "destination": req.destination,
            "currencyCode": req.currency or "INR",
            "includeTaxesAndFees": True,
            "numberOfPassengers": req.adults,
            "fareType": "None",
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    _LOWFARE_URL, headers=_HEADERS, json=payload,
                )
        except httpx.HTTPError as exc:
            logger.error("AirIndiaExpress API request failed: %s", exc)
            return self._empty(req)

        if resp.status_code != 200:
            logger.warning("AirIndiaExpress API returned %d", resp.status_code)
            return self._empty(req)

        try:
            data = resp.json()
        except Exception:
            logger.warning("AirIndiaExpress API returned non-JSON response")
            return self._empty(req)

        offers = self._parse_lowfares(data, req)
        elapsed = time.monotonic() - t0
        offers.sort(key=lambda o: o.price)
        logger.info(
            "AirIndiaExpress %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_id = hashlib.md5(
            f"aie{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_id}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "INR",
            offers=offers,
            total_results=len(offers),
        )

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_lowfares(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        fares = data.get("lowFares", [])
        if not fares:
            return []

        booking_url = self._booking_url(req)
        offers: list[FlightOffer] = []

        for item in fares:
            if not isinstance(item, dict):
                continue
            if item.get("noFlights") or item.get("soldOut"):
                continue

            price = item.get("price", 0) or 0
            if price <= 0:
                continue

            taxes = item.get("taxesAndFees", 0) or 0
            date_raw = item.get("date", "")
            date_str = date_raw[:10] if date_raw else ""
            if not date_str:
                continue

            dep_dt = self._parse_dt(date_raw)

            fid = hashlib.md5(
                f"ix_{req.origin}{req.destination}{date_str}{price}".encode()
            ).hexdigest()[:12]

            seg = FlightSegment(
                airline="IX",
                airline_name="Air India Express",
                flight_no="",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                cabin_class="M",
            )
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=0,
                stopovers=0,
            )
            offers.append(FlightOffer(
                id=f"aie_{fid}",
                price=round(price, 2),
                currency=req.currency or "INR",
                price_formatted=f"{price:.0f} {req.currency or 'INR'}",
                outbound=route,
                inbound=None,
                airlines=["Air India Express"],
                owner_airline="IX",
                booking_url=booking_url,
                is_locked=False,
                source="airindiaexpress_direct",
                source_tier="free",
                availability_seats=item.get("available"),
            ))

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        d = req.date_from.strftime("%d/%m/%Y")
        return (
            f"https://www.airindiaexpress.com/booking?"
            f"origin={req.origin}&destination={req.destination}"
            f"&date={d}&adults={req.adults}&children={req.children}"
            f"&infants={req.infants}&tripType=O"
        )

    @staticmethod
    def _parse_dt(raw: str) -> Optional[datetime]:
        if not raw:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw[:19] if "T" in fmt else raw[:10], fmt)
            except (ValueError, IndexError):
                continue
        return None

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        search_id = hashlib.md5(
            f"aie{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_id}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "INR",
            offers=[],
            total_results=0,
        )