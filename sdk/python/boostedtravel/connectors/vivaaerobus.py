"""
VivaAerobus direct API scraper — zero auth, pure httpx.

VivaAerobus (IATA: VB) is Mexico's largest ultra-low-cost carrier.
Website: www.vivaaerobus.com — English at /en-us.

Strategy (discovered Mar 2026):
The lowfares calendar API is open — requires only a static x-api-key header.
POST api.vivaaerobus.com/web/vb/v1/availability/lowfares
Returns 7 days of lowest fares as structured JSON. No browser needed.

Note: the full /web/v1/availability/search endpoint IS Akamai-protected (403),
but the lowfares endpoint works fine with plain httpx.
"""

from __future__ import annotations

import hashlib
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Any

from curl_cffi.requests import AsyncSession

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_API_BASE = "https://api.vivaaerobus.com"
_API_KEY = "zasqyJdSc92MhWMxYu6vW3hqhxLuDwKog3mqoYkf"
_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": "https://www.vivaaerobus.com",
    "Referer": "https://www.vivaaerobus.com/",
    "x-api-key": _API_KEY,
    "X-Channel": "web",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
}

_http_client: AsyncSession | None = None


def _get_client() -> AsyncSession:
    global _http_client
    if _http_client is None:
        _http_client = AsyncSession(impersonate="chrome136", headers=_HEADERS, timeout=30)
    return _http_client


class VivaAerobusConnectorClient:
    """VivaAerobus scraper — pure direct API, zero auth, ~0.5s searches."""

    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout

    async def close(self):
        global _http_client
        if _http_client:
            _http_client.close()
            _http_client = None

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        client = _get_client()

        start_date = req.date_from.strftime("%Y-%m-%d")
        end_date = (req.date_from + timedelta(days=6)).strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1

        body = {
            "currencyCode": req.currency or "USD",
            "promoCode": None,
            "bookingType": None,
            "referralCode": "",
            "passengers": [{"code": "ADT", "count": adults}],
            "routes": [{
                "startDate": start_date,
                "endDate": end_date,
                "origin": {"code": req.origin, "type": "Airport"},
                "destination": {"code": req.destination, "type": "Airport"},
            }],
            "sessionID": str(uuid.uuid4()),
            "language": "en-US",
        }

        logger.info("VivaAerobus API: %s→%s %s–%s", req.origin, req.destination, start_date, end_date)

        try:
            resp = await client.post(f"{_API_BASE}/web/vb/v1/availability/lowfares", json=body, headers=_HEADERS)
            elapsed = time.monotonic() - t0

            if resp.status_code != 200:
                logger.warning("VivaAerobus API HTTP %d: %s", resp.status_code, resp.text[:300])
                return self._empty(req)

            api_json = resp.json()
            offers = self._parse_lowfares(api_json, req)
            if offers:
                return self._build_response(offers, req, elapsed)
            return self._empty(req)

        except Exception as e:
            logger.error("VivaAerobus API error: %s", e)
            return self._empty(req)

    # ------------------------------------------------------------------ #
    #  Lowfares API parsing                                                #
    # ------------------------------------------------------------------ #

    def _parse_lowfares(self, api_json: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the lowfares API response into FlightOffer objects."""
        data = api_json.get("data", {})
        low_fares = data.get("lowFares", [])
        currency = data.get("currencyCode", req.currency)
        if not low_fares:
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for fare in low_fares:
            if not isinstance(fare, dict):
                continue
            dep_date = fare.get("departureDate", "")
            fare_obj = fare.get("fare", {})
            fare_with_tua = fare.get("fareWithTua", {})
            # Prefer fareWithTua (includes taxes) over base fare
            price = (fare_with_tua.get("amount") if fare_with_tua else None) or fare_obj.get("amount")
            if price is None or price <= 0:
                continue

            origin_obj = fare.get("origin", {})
            dest_obj = fare.get("destination", {})
            origin_code = origin_obj.get("code", req.origin) if isinstance(origin_obj, dict) else req.origin
            dest_code = dest_obj.get("code", req.destination) if isinstance(dest_obj, dict) else req.destination
            origin_name = origin_obj.get("name", "") if isinstance(origin_obj, dict) else ""
            dest_name = dest_obj.get("name", "") if isinstance(dest_obj, dict) else ""
            carrier = fare.get("carrierCode", "VB")
            avail = fare.get("availableCount")
            fare_class = fare.get("fareProductClass", "")

            # Build a segment for the date (VB calendar shows one fare per day)
            dep_dt = self._parse_dt(dep_date)
            segment = FlightSegment(
                airline=carrier,
                airline_name="VivaAerobus",
                flight_no="",
                origin=origin_code,
                destination=dest_code,
                origin_city=origin_name,
                destination_city=dest_name,
                departure=dep_dt,
                arrival=dep_dt,
                cabin_class=fare_class or "M",
            )
            route = FlightRoute(segments=[segment], total_duration_seconds=0, stopovers=0)

            offer_key = f"vb_{origin_code}{dest_code}_{dep_date}_{price}"
            offer = FlightOffer(
                id=f"vb_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
                price=round(float(price), 2),
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=[carrier],
                owner_airline="VB",
                availability_seats=avail,
                booking_url=booking_url,
                is_locked=False,
                source="vivaaerobus_direct",
                source_tier="free",
            )
            offers.append(offer)

        return offers

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("VivaAerobus %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"vivaaerobus{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S",
                     "%Y-%m-%d", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y%m%d")
        adults = getattr(req, "adults", 1) or 1
        return (
            f"https://www.vivaaerobus.com/en-us/book/options?itineraryCode="
            f"{req.origin}_{req.destination}_{dep}&passengers=A{adults}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"vivaaerobus{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
