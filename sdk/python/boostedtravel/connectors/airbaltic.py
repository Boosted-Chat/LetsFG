"""
airBaltic httpx scraper -- calendar fare data from airbaltic.com public API.

airBaltic (IATA: BT) is a Latvian flag carrier based in Riga, operating
short/medium-haul flights across Europe, the Middle East, and Central Asia.
Default currency EUR.

Strategy:
1. Call /api/fsf/outbound with origin, destination, month -> daily prices
2. Each day entry has price + isDirect flag
3. Build one FlightOffer per day with the cheapest calendar price
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

import httpx

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.airbaltic.com/en/book-flight",
    "Origin": "https://www.airbaltic.com",
}

_API_BASE = "https://www.airbaltic.com/api/fsf"


class AirbalticConnectorClient:
    """airBaltic calendar-fare scraper via public API."""

    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        date_from = req.date_from
        date_to = req.date_to or date_from

        # Collect all months in the search range
        months: list[str] = []
        current = date_from.replace(day=1)
        end_month = date_to.replace(day=1)
        while current <= end_month:
            months.append(current.strftime("%Y-%m"))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        all_days: list[dict] = []
        try:
            async with httpx.AsyncClient(
                timeout=self.timeout,
                follow_redirects=True,
                headers=_HEADERS,
            ) as client:
                for month in months:
                    params = {
                        "origin": req.origin,
                        "destin": req.destination,
                        "tripType": "oneway",
                        "numAdt": str(req.adults),
                        "numChd": str(req.children),
                        "numInf": str(req.infants),
                        "departureMonth": month,
                        "flightMode": "oneway",
                    }
                    resp = await client.get(f"{_API_BASE}/outbound", params=params)
                    if resp.status_code != 200:
                        logger.warning(
                            "airBaltic outbound %s: HTTP %d", month, resp.status_code
                        )
                        continue

                    body = resp.json()
                    if not body.get("success"):
                        logger.warning(
                            "airBaltic outbound %s: %s", month, body.get("error", "")
                        )
                        continue

                    data = body.get("data", [])
                    if isinstance(data, list):
                        all_days.extend(data)

        except Exception as e:
            logger.error("airBaltic API error: %s", e)
            return self._empty(req)

        # Filter to requested date range and build offers
        offers = self._build_offers(all_days, req, date_from, date_to)
        elapsed = time.monotonic() - t0

        offers.sort(key=lambda o: o.price)
        logger.info(
            "airBaltic %s->%s returned %d offers in %.1fs (httpx)",
            req.origin, req.destination, len(offers), elapsed,
        )

        h = hashlib.md5(
            f"bt{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"bt_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _build_offers(
        days: list[dict],
        req: FlightSearchRequest,
        date_from,
        date_to,
    ) -> list[FlightOffer]:
        from_str = date_from.isoformat()
        to_str = date_to.isoformat()
        offers: list[FlightOffer] = []

        for day in days:
            price = day.get("price")
            if not price or price <= 0:
                continue

            dep_date = day.get("date", "")
            if not dep_date:
                continue

            # Filter to requested date range
            if dep_date < from_str or dep_date > to_str:
                continue

            is_direct = day.get("isDirect", False)

            dep_dt = datetime(2000, 1, 1)
            try:
                dep_dt = datetime.strptime(dep_date, "%Y-%m-%d")
            except ValueError:
                pass

            segment = FlightSegment(
                airline="BT",
                airline_name="airBaltic",
                flight_no="",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class="economy",
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=0,
                stopovers=0 if is_direct else 1,
            )

            fid = hashlib.md5(
                f"bt_{req.origin}{req.destination}{dep_date}{price}".encode()
            ).hexdigest()[:12]

            booking_url = (
                f"https://www.airbaltic.com/en/book-flight"
                f"?originCode={req.origin}&destinCode={req.destination}"
                f"&tripType=oneway&numAdt={req.adults}"
                f"&numChd={req.children}&numInf={req.infants}"
            )

            offers.append(FlightOffer(
                id=f"bt_{fid}",
                price=round(price, 2),
                currency="EUR",
                price_formatted=f"{price:.2f} EUR",
                outbound=route,
                inbound=None,
                airlines=["airBaltic"],
                owner_airline="BT",
                booking_url=booking_url,
                is_locked=False,
                source="airbaltic_direct",
                source_tier="free",
            ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"bt{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"bt_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "EUR",
            offers=[],
            total_results=0,
        )
