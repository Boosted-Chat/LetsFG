"""
SpiceJet direct API scraper — no browser required.

SpiceJet (IATA: SG) is an Indian low-cost carrier (Navitaire/dotREZ platform).
Website: www.spicejet.com — React Native Web SPA with REST API backend.

Strategy (Pure Direct API):
1. Token: POST /api/v1/token — no auth, returns JWT. Cached ~10 min.
2. Search: POST /api/v3/search/availability with JWT + curl_cffi (TLS impersonation).
   ~1-1.5s per search. No browser needed at all.

Key API details (discovered March 2026, body field fix April 2026):
- Token: POST /api/v1/token — empty JSON body, returns {"data":{"token":"eyJ..."}}
  Headers: os: desktop, Content-Type: application/json
- Availability: POST /api/v3/search/availability
  Body: {"originStationCode":"DEL","destinationStationCode":"BOM",
         "onWardDate":"2026-04-15","currency":"INR",
         "pax":{"journeyClass":"ff","adult":1,"child":0,"infant":0,"srCitizen":0}}
  IMPORTANT: the date field is "onWardDate" (not beginDate/endDate).
  Response: {"data":{"trips":[{"journeysAvailable":[...segments, fares...]}]}}
- Fare pricing encoded in base64url fareAvailabilityKey: first number / 10 = INR base fare
"""

from __future__ import annotations

import asyncio
import base64
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
_TOKEN_URL = "https://www.spicejet.com/api/v1/token"
_SEARCH_URL = "https://www.spicejet.com/api/v3/search/availability"
_IMPERSONATE = "chrome131"
_TOKEN_MAX_AGE = 10 * 60  # Re-acquire token every 10 minutes

# --- Shared token state ---
_token_lock: Optional[asyncio.Lock] = None
_cached_token: Optional[str] = None
_token_timestamp: float = 0.0


def _get_token_lock() -> asyncio.Lock:
    global _token_lock
    if _token_lock is None:
        _token_lock = asyncio.Lock()
    return _token_lock


def _decode_fare_price(fare_key: str) -> Optional[float]:
    """Extract base fare from Navitaire fareAvailabilityKey (base64url encoded).

    Key format decoded: '...!journeyIdx:baseFare:tax1:tax2:...'
    The number after '!0:' divided by 10 gives the base fare in INR.
    """
    try:
        padded = fare_key + "=" * (4 - len(fare_key) % 4)
        decoded = base64.urlsafe_b64decode(padded).decode("utf-8", errors="replace")
        if "!" not in decoded:
            return None
        pricing = decoded.split("!")[-1]
        parts = pricing.split(":")
        if len(parts) >= 2:
            raw_value = int(parts[1])
            return raw_value / 10.0
    except Exception:
        pass
    return None


class SpiceJetConnectorClient:
    """SpiceJet direct API scraper — curl_cffi with TLS impersonation."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    async def _ensure_token(self) -> Optional[str]:
        """Return cached JWT token, acquiring a fresh one if expired."""
        global _cached_token, _token_timestamp
        lock = _get_token_lock()
        async with lock:
            age = time.monotonic() - _token_timestamp
            if _cached_token and age < _TOKEN_MAX_AGE:
                return _cached_token
            return await self._acquire_token()

    async def _acquire_token(self) -> Optional[str]:
        """Fetch a fresh JWT from the token endpoint."""
        global _cached_token, _token_timestamp
        loop = asyncio.get_event_loop()
        try:
            token = await loop.run_in_executor(None, self._acquire_token_sync)
            if token:
                _cached_token = token
                _token_timestamp = time.monotonic()
                logger.info("SpiceJet: acquired fresh JWT token")
            return token
        except Exception as e:
            logger.error("SpiceJet: token acquisition failed: %s", e)
            return None

    @staticmethod
    def _acquire_token_sync() -> Optional[str]:
        """Synchronous token acquisition via curl_cffi."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)
        r = sess.post(
            _TOKEN_URL,
            json={},
            headers={"Content-Type": "application/json", "os": "desktop"},
            timeout=10,
        )
        if r.status_code != 200:
            logger.warning("SpiceJet: token endpoint returned %d", r.status_code)
            return None
        data = r.json()
        return data.get("data", {}).get("token")

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search SpiceJet flights via direct API (~1-1.5s per search)."""
        t0 = time.monotonic()

        try:
            token = await self._ensure_token()
            if not token:
                logger.warning("SpiceJet: no token available")
                return self._empty(req)

            data = await self._api_search(req, token)

            # If failed (e.g. expired token), re-acquire once and retry
            if data is None:
                logger.info("SpiceJet: search failed, re-acquiring token")
                token = await self._acquire_token()
                if token:
                    data = await self._api_search(req, token)

            if data:
                elapsed = time.monotonic() - t0
                offers = self._parse_availability(data, req)
                logger.info(
                    "SpiceJet %s→%s returned %d offers in %.1fs (direct API)",
                    req.origin, req.destination, len(offers), elapsed,
                )
                return self._build_response(offers, req, elapsed)

            logger.warning("SpiceJet: no data for %s→%s", req.origin, req.destination)
            return self._empty(req)

        except Exception as e:
            logger.error("SpiceJet error: %s", e)
            return self._empty(req)

    async def _api_search(self, req: FlightSearchRequest, token: str) -> Optional[dict]:
        """Execute availability search via curl_cffi."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._api_search_sync, req, token)

    def _api_search_sync(self, req: FlightSearchRequest, token: str) -> Optional[dict]:
        """Synchronous availability search."""
        currency = req.currency if req.currency != "EUR" else "INR"
        date_str = req.date_from.strftime("%Y-%m-%d")

        body = {
            "originStationCode": req.origin,
            "destinationStationCode": req.destination,
            "onWardDate": date_str,
            "currency": currency,
            "pax": {
                "journeyClass": "ff",
                "adult": req.adults,
                "child": req.children or 0,
                "infant": req.infants or 0,
                "srCitizen": 0,
            },
        }

        sess = cffi_requests.Session(impersonate=_IMPERSONATE)
        try:
            r = sess.post(
                _SEARCH_URL,
                json=body,
                headers={
                    "Authorization": token,
                    "Content-Type": "application/json",
                    "os": "desktop",
                    "Origin": "https://www.spicejet.com",
                    "Referer": f"https://www.spicejet.com/search?from={req.origin}&to={req.destination}",
                },
                timeout=15,
            )
        except Exception as e:
            logger.error("SpiceJet: API request failed: %s", e)
            return None

        if r.status_code == 401:
            logger.warning("SpiceJet: API returned 401 (token expired)")
            return None
        if r.status_code != 200:
            logger.warning("SpiceJet: API returned %d", r.status_code)
            return None

        try:
            return r.json()
        except Exception:
            logger.error("SpiceJet: failed to parse API response")
            return None

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_availability(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse /api/v3/search/availability response into FlightOffer list."""
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        trips = data.get("data", {}).get("trips", [])
        for trip in trips:
            journeys = trip.get("journeysAvailable", [])
            for journey in journeys:
                offer = self._parse_journey(journey, req, booking_url)
                if offer:
                    offers.append(offer)
        return offers

    def _parse_journey(
        self, journey: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        """Parse a single journey from the availability response."""
        fares = journey.get("fares", {})
        best_price = float("inf")
        for fare_key, fare_info in fares.items():
            if isinstance(fare_info, dict):
                key_str = fare_info.get("fareAvailabilityKey", "")
                decoded_price = _decode_fare_price(key_str)
                if decoded_price and 0 < decoded_price < best_price:
                    best_price = decoded_price

        if best_price == float("inf") or best_price <= 0:
            return None

        designator = journey.get("designator", {})
        is_international = journey.get("isInternational", False)
        tax_rate = 0.12 if is_international else 0.19
        total_price = round(best_price * (1 + tax_rate))

        segments_raw = journey.get("segments", [])
        segments: list[FlightSegment] = []
        for seg in segments_raw:
            seg_obj = self._parse_segment(seg, req)
            if seg_obj:
                segments.append(seg_obj)

        if not segments:
            dep_str = designator.get("departure", "")
            arr_str = designator.get("arrival", "")
            carrier_str = journey.get("carrierString", "SG ???")
            parts = carrier_str.split()
            carrier = parts[0] if parts else "SG"
            flight_no = f"{carrier}{parts[1]}" if len(parts) > 1 else carrier

            segments.append(
                FlightSegment(
                    airline=carrier,
                    airline_name="SpiceJet",
                    flight_no=flight_no,
                    origin=designator.get("origin", req.origin),
                    destination=designator.get("destination", req.destination),
                    departure=self._parse_dt(dep_str),
                    arrival=self._parse_dt(arr_str),
                    cabin_class="economy",
                )
            )

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        journey_key = journey.get("journeyKey", "")
        currency = req.currency if req.currency != "EUR" else "INR"

        return FlightOffer(
            id=f"sg_{hashlib.md5(journey_key.encode()).hexdigest()[:12]}",
            price=total_price,
            currency=currency,
            price_formatted=f"{total_price:.0f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["SpiceJet"],
            owner_airline="SG",
            booking_url=booking_url,
            is_locked=False,
            source="spicejet_direct_api",
            source_tier="protocol",
        )

    def _parse_segment(self, seg: dict, req: FlightSearchRequest) -> Optional[FlightSegment]:
        """Parse a segment from the availability response."""
        designator = seg.get("designator", {})
        identifier = seg.get("identifier", {})
        carrier = identifier.get("carrierCode", "SG")
        flight_no_raw = str(identifier.get("identifier", ""))
        flight_no = f"{carrier}{flight_no_raw}" if flight_no_raw and not flight_no_raw.startswith(carrier) else flight_no_raw

        dep_str = designator.get("departure", "")
        arr_str = designator.get("arrival", "")
        origin = designator.get("origin", req.origin)
        destination = designator.get("destination", req.destination)

        aircraft = ""
        legs = seg.get("legs", [])
        if legs:
            leg_info = legs[0].get("legInfo", {})
            aircraft = leg_info.get("equipmentType", "")

        dep_dt = self._parse_dt(dep_str)
        arr_dt = self._parse_dt(arr_str)
        dur = int((arr_dt - dep_dt).total_seconds()) if dep_dt and arr_dt else 0

        return FlightSegment(
            airline=carrier,
            airline_name="SpiceJet",
            flight_no=flight_no,
            origin=origin,
            destination=destination,
            origin_city=designator.get("originFullName", ""),
            destination_city=designator.get("destinationFullName", ""),
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=max(dur, 0),
            cabin_class="economy",
            aircraft=aircraft,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        h = hashlib.md5(f"spicejet{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=offers,
            total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s[: len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.spicejet.com/search?from={req.origin}&to={req.destination}"
            f"&tripType=1&departure={dep}&adult={req.adults}&child={req.children}"
            f"&srCitizen=0&infant={req.infants}&currency=INR"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"spicejet{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
