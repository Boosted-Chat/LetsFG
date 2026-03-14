"""
Akasa Air direct API scraper — no browser required.

Akasa Air (IATA: QP) is an Indian low-cost carrier (launched 2022).
Website: www.akasaair.com — React (Next.js) SPA booking engine.

Backend: Navitaire New Skies (NSK) via prod-bl.qp.akasaair.com.

Strategy (Pure Direct API):
1. Token: POST /api/ibe/token/generateToken — no auth, returns encrypted token.
   Cached ~10 min. MUST send Accept: application/json header.
2. Search: POST /api/ibe/availability/search with token + curl_cffi (TLS
   impersonation). ~0.3-0.7s per search. No browser needed at all.

Key API details (discovered Mar 2026, direct API conversion Jul 2026):
- Token: POST prod-bl.qp.akasaair.com/api/ibe/token/generateToken
  Body: {"deviceType":"WEB","bookingType":"BOOKING","userType":"GUEST"}
  Returns: {"data":{"token":"P6gel5Cb/Xte..."}}
  Auth header uses raw token string (NO "Bearer " prefix).
- Availability: POST prod-bl.qp.akasaair.com/api/ibe/availability/search
  Body: criteria[].stations (IATA codes), criteria[].dates.beginDate
  ("YYYY-MM-DDT00:00:00"), passengers, codes, productClasses, fareTypes.
  Response: {data: {results[0].trips[0].journeysAvailableByMarket[0].value}}
  Fare lookup: {data: {faresAvailable: [{key, value: {totals: {fareTotal}}}]}}
  Prices in whole currency units (INR), fareTotal includes taxes+fees.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime
from typing import Any, Optional

from curl_cffi import requests as cffi_requests

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

# --- API constants ---
_BASE = "https://prod-bl.qp.akasaair.com"
_TOKEN_URL = f"{_BASE}/api/ibe/token/generateToken"
_SEARCH_URL = f"{_BASE}/api/ibe/availability/search"
_IMPERSONATE = "chrome131"
_TOKEN_MAX_AGE = 10 * 60  # Re-acquire token every 10 minutes

_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.akasaair.com",
    "Referer": "https://www.akasaair.com/",
}

# --- Shared token state ---
_token_lock: Optional[asyncio.Lock] = None
_cached_token: Optional[str] = None
_token_timestamp: float = 0.0


def _get_token_lock() -> asyncio.Lock:
    global _token_lock
    if _token_lock is None:
        _token_lock = asyncio.Lock()
    return _token_lock


async def _acquire_token(sess: cffi_requests.Session) -> str:
    """Acquire a fresh Navitaire API token (cached for ~10 min)."""
    global _cached_token, _token_timestamp
    now = time.monotonic()
    if _cached_token and (now - _token_timestamp) < _TOKEN_MAX_AGE:
        return _cached_token

    lock = _get_token_lock()
    async with lock:
        # Double-check after acquiring lock
        if _cached_token and (time.monotonic() - _token_timestamp) < _TOKEN_MAX_AGE:
            return _cached_token

        body = json.dumps({
            "deviceType": "WEB",
            "bookingType": "BOOKING",
            "userType": "GUEST",
        })
        r = await asyncio.to_thread(
            sess.post, _TOKEN_URL, data=body, headers=_HEADERS, timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        token = data["data"]["token"]
        _cached_token = token
        _token_timestamp = time.monotonic()
        logger.info("Akasa: acquired fresh API token")
        return token


class AkasaConnectorClient:
    """Akasa Air direct API scraper — pure HTTP, no browser."""

    def __init__(self, timeout: float = 15.0):
        self.timeout = timeout
        self._sess = cffi_requests.Session(impersonate=_IMPERSONATE)

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        try:
            token = await _acquire_token(self._sess)
        except Exception as e:
            logger.error("Akasa: token acquisition failed: %s", e)
            return self._empty(req)

        dep_date = req.date_from.strftime("%Y-%m-%dT00:00:00")
        search_body = json.dumps({
            "criteria": [{
                "stations": {
                    "originStationCodes": [req.origin],
                    "destinationStationCodes": [req.destination],
                    "searchDestinationMacs": True,
                    "searchOriginMacs": True,
                },
                "dates": {"beginDate": dep_date},
                "filters": {
                    "compressionType": 1,
                    "maxConnections": 8,
                    "productClasses": ["NB", "LB", "EC", "AV"],
                    "fareTypes": ["NB", "LB", "R", "V"],
                },
            }],
            "passengers": {
                "types": [{"type": "ADT", "count": max(req.adults, 1)}],
                "residentCountry": "",
            },
            "codes": {
                "currencyCode": req.currency,
                "promotionCode": "",
                "currentSourceOrganization": None,
            },
            "numberOfFaresPerJourney": 10,
            "taxesAndFees": 1,
        })

        try:
            hdrs = {**_HEADERS, "Authorization": token}
            r = await asyncio.to_thread(
                self._sess.post, _SEARCH_URL, data=search_body,
                headers=hdrs, timeout=self.timeout,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            logger.error("Akasa: search API error %s→%s: %s", req.origin, req.destination, e)
            return self._empty(req)

        elapsed = time.monotonic() - t0
        offers = self._parse_navitaire_response(data, req)
        offers.sort(key=lambda o: o.price)

        logger.info(
            "Akasa %s→%s returned %d offers in %.1fs (direct API)",
            req.origin, req.destination, len(offers), elapsed,
        )

        search_hash = hashlib.md5(
            f"akasa{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers,
            total_results=len(offers),
        )

    # ------------------------------------------------------------------
    # Navitaire response parsing
    # ------------------------------------------------------------------

    def _parse_navitaire_response(
        self, data: dict, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Parse Navitaire NSK availability/search response into FlightOffers.

        Response structure:
          data.results[0].trips[0].journeysAvailableByMarket[0].value → [journey]
          data.faresAvailable → [{key, value: {totals: {fareTotal}, fares: [...]}}]

        Each journey has:
          - designator: {origin, destination, departure, arrival}
          - segments[].identifier: {carrierCode, identifier (flight number)}
          - segments[].designator: {origin, dest, departure, arrival}
          - segments[].legs[].legInfo: {departureTerminal, arrivalTerminal, equipmentType}
          - fares[].fareAvailabilityKey → links to faresAvailable lookup
          - journeyKey, stops, flightType
        """
        offers: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)

        inner = data.get("data", {})
        results = inner.get("results", [])
        if not results:
            return offers

        # Build fare lookup: fareAvailabilityKey → {totals, fares, productClass}
        fare_lookup: dict[str, dict] = {}
        for fare_entry in inner.get("faresAvailable", []):
            key = fare_entry.get("key", "")
            value = fare_entry.get("value", {})
            if key and value:
                fare_lookup[key] = value

        # Walk results → trips → journeysAvailableByMarket → journeys
        for result_block in results:
            for trip in result_block.get("trips", []):
                for market in trip.get("journeysAvailableByMarket", []):
                    journeys = market.get("value", [])
                    for journey in journeys:
                        offer = self._parse_journey(
                            journey, fare_lookup, req, booking_url
                        )
                        if offer:
                            offers.append(offer)

        return offers

    def _parse_journey(
        self,
        journey: dict,
        fare_lookup: dict[str, dict],
        req: FlightSearchRequest,
        booking_url: str,
    ) -> Optional[FlightOffer]:
        """Convert a single Navitaire journey into a FlightOffer."""
        # Find the cheapest fare for this journey
        best_price = float("inf")
        best_currency = req.currency
        best_product = ""

        for fare_ref in journey.get("fares", []):
            fare_key = fare_ref.get("fareAvailabilityKey", "")
            fare_data = fare_lookup.get(fare_key)
            if not fare_data:
                continue
            totals = fare_data.get("totals", {})
            fare_total = totals.get("fareTotal", 0)  # Includes taxes+fees
            if 0 < fare_total < best_price:
                best_price = fare_total
                # Determine currency from service charges
                for fare in fare_data.get("fares", []):
                    for pf in fare.get("passengerFares", []):
                        for sc in pf.get("serviceCharges", []):
                            if sc.get("currencyCode"):
                                best_currency = sc["currencyCode"]
                                break
                best_product = fare_data.get("fares", [{}])[0].get("productClass", "")

        if best_price == float("inf") or best_price <= 0:
            return None

        # Parse segments
        segments = self._parse_nsk_segments(journey.get("segments", []))
        if not segments:
            return None

        designator = journey.get("designator", {})
        dep_str = designator.get("departure", "")
        arr_str = designator.get("arrival", "")
        dep_dt = self._parse_dt(dep_str)
        arr_dt = self._parse_dt(arr_str)
        total_dur = int((arr_dt - dep_dt).total_seconds()) if dep_dt and arr_dt else 0

        stops = journey.get("stops", max(len(segments) - 1, 0))
        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=stops,
        )

        journey_key = journey.get("journeyKey", "")
        offer_key = f"{journey_key}_{best_price}"

        # Map productClass to cabin: EC=Economy, AV=Akasa Value (premium economy equivalent)
        cabin_map = {"EC": "M", "AV": "W", "NB": "M", "LB": "M"}
        cabin = cabin_map.get(best_product, "M")
        for seg in segments:
            seg.cabin_class = cabin

        return FlightOffer(
            id=f"qp_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=best_currency,
            price_formatted=f"{best_price:.2f} {best_currency}",
            outbound=route,
            inbound=None,
            airlines=["Akasa Air"],
            owner_airline="QP",
            booking_url=booking_url,
            is_locked=False,
            source="akasa_direct",
            source_tier="free",
        )

    def _parse_nsk_segments(self, segments_raw: list) -> list[FlightSegment]:
        """Parse Navitaire segments.

        Each segment has:
          identifier: {carrierCode: "QP", identifier: "1819"}
          designator: {origin, destination, departure, arrival}
          legs[0].legInfo: {departureTerminal, arrivalTerminal, equipmentType}
        """
        segments: list[FlightSegment] = []

        for seg_info in segments_raw:
            ident = seg_info.get("identifier", {})
            design = seg_info.get("designator", {})
            carrier = ident.get("carrierCode", "QP")
            number = ident.get("identifier", "")

            origin = design.get("origin", "")
            dest = design.get("destination", "")
            dep_dt = self._parse_dt(design.get("departure", ""))
            arr_dt = self._parse_dt(design.get("arrival", ""))

            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Akasa Air",
                flight_no=f"{carrier}{number}",
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=arr_dt,
                cabin_class="M",
            ))

        return segments

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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
            f"https://www.akasaair.com/booking?origin={req.origin}"
            f"&destination={req.destination}&date={dep}"
            f"&adults={req.adults}&tripType=O"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"akasa{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
