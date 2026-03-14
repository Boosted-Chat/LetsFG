"""
Jazeera Airways direct API scraper — pure curl_cffi, no browser needed.

Jazeera Airways (IATA: J9) is a Kuwaiti low-cost carrier.
Booking engine: booking.jazeeraairways.com (Angular SPA, Navitaire/dotREZ platform).

Direct API strategy (Mar 2026):
1. Token: POST j9api.jazeeraairways.com/api/Postman/api/nsk/v1/token
   - No body needed, returns JWT (RS256, ~20min TTL)
   - Claims: WebAnonymous agent, J9 org, DigitalAPI channel, KWD currency
2. Availability: POST j9api.jazeeraairways.com/api/jz/v1/availability
   - JSON body with criteria/passengers/codes (exact format from Angular bundle)
   - Returns trips with journeys + faresAvailable with pricing

Body format extracted from Angular bundle (main.04c03bcb4fc47d8f.js):
  compressionType: "CompressByProductClass"
  exclusionType: "Default"
  maxConnections: 10
  passengers.types: [{type: "ADT", count: N}]
  codes.currencyCode: "KWD" (default, overridden by request)
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
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

# ── API constants ─────────────────────────────────────────
_API_BASE = "https://j9api.jazeeraairways.com"
_TOKEN_URL = f"{_API_BASE}/api/Postman/api/nsk/v1/token"
_AVAIL_URL = f"{_API_BASE}/api/jz/v1/availability"
_BOOKING_BASE = "https://booking.jazeeraairways.com"
_TOKEN_MARGIN = 120  # refresh token 2min before expiry (20min TTL)

# Cached token state
_token: str | None = None
_token_expiry: float = 0  # monotonic timestamp

_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Content-Type": "application/json",
    "Origin": _BOOKING_BASE,
    "Referer": f"{_BOOKING_BASE}/en/search-flight",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
}


async def _ensure_token() -> str | None:
    """Get a valid JWT token, refreshing if needed."""
    global _token, _token_expiry
    if _token and time.monotonic() < _token_expiry:
        return _token
    try:
        from curl_cffi import requests as cffi_requests

        ses = cffi_requests.Session(impersonate="chrome131")
        r = ses.post(_TOKEN_URL, headers=_HEADERS, json={}, timeout=15)
        if r.status_code not in (200, 201):
            logger.warning("Jazeera auth failed: %s %s", r.status_code, r.text[:200])
            return None
        data = r.json()
        # Response: {"data": {"token": "eyJ...", "idToken": null, ...}}
        inner = data.get("data", data)
        tok = None
        if isinstance(inner, dict):
            tok = inner.get("token")
        if not tok:
            for v in data.values():
                if isinstance(v, str) and len(v) > 50 and v.startswith("eyJ"):
                    tok = v
                    break
        if tok:
            # Parse TTL from JWT claims
            ttl = 1200  # default 20min
            try:
                parts = tok.split(".")
                if len(parts) >= 2:
                    payload = parts[1] + "=" * (4 - len(parts[1]) % 4)
                    claims = json.loads(base64.b64decode(payload))
                    exp = claims.get("exp")
                    if exp:
                        ttl = max(int(exp - time.time()), 60)
            except Exception:
                pass
            _token = tok
            _token_expiry = time.monotonic() + ttl - _TOKEN_MARGIN
            logger.info("Jazeera: auth token acquired (TTL=%ss)", ttl)
            return tok
        logger.warning("Jazeera: no token in auth response")
        return None
    except Exception as e:
        logger.warning("Jazeera: auth error: %s", e)
        return None


class JazeeraConnectorClient:
    """Jazeera Airways direct API scraper — curl_cffi only, no browser."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ── Main entry point ─────────────────────────────────
    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        token = await _ensure_token()
        if not token:
            logger.warning("Jazeera: could not acquire token")
            return self._empty(req)

        try:
            from curl_cffi import requests as cffi_requests

            currency = req.currency or "KWD"
            body = self._build_request_body(req, currency)
            headers = {**_HEADERS, "Authorization": token}

            ses = cffi_requests.Session(impersonate="chrome131")
            r = ses.post(
                _AVAIL_URL,
                headers=headers,
                json=body,
                timeout=int(self.timeout),
            )

            if r.status_code != 200:
                logger.warning(
                    "Jazeera availability HTTP %s: %s",
                    r.status_code, r.text[:300],
                )
                return self._empty(req)

            data = r.json()
            elapsed = time.monotonic() - t0
            offers = self._parse_availability(data, req)
            logger.info(
                "Jazeera API: %d offers %s->%s in %.2fs",
                len(offers), req.origin, req.destination, elapsed,
            )
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.warning("Jazeera API error: %s", e)
            return self._empty(req)

    # ── Request body builder ─────────────────────────────
    def _build_request_body(self, req: FlightSearchRequest, currency: str) -> dict:
        """Build the exact request body format from Angular bundle."""
        passenger_types = [{"type": "ADT", "count": req.adults}]
        if req.children:
            passenger_types.append({"type": "CHD", "count": req.children})
        if req.infants:
            passenger_types.append({"type": "INF", "count": req.infants})

        return {
            "criteria": [
                {
                    "stations": {
                        "destinationStationCodes": [req.destination],
                        "originStationCodes": [req.origin],
                        "searchDestinationMacs": True,
                        "searchOriginMacs": True,
                    },
                    "dates": {
                        "beginDate": req.date_from.strftime("%Y-%m-%d"),
                    },
                    "filters": {
                        "maxConnections": 10,
                        "compressionType": "CompressByProductClass",
                        "exclusionType": "Default",
                    },
                }
            ],
            "passengers": {
                "types": passenger_types,
                "residentCountry": "",
            },
            "codes": {
                "promotionCode": "",
                "currencyCode": currency,
            },
            "numberOfFaresPerJourney": 10,
            "hasUmnr": False,
        }

    # ── Response parser ──────────────────────────────────
    def _parse_availability(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Parse availability response into FlightOffers.

        Structure:
        - data.availabilityv4.results[].trips[].journeysAvailableByMarket[{key, value}]
          Each journey: designator (local times), segments[].legs[].legInfo, fares[]
        - data.availabilityv4.faresAvailable[{key, value}]
          Maps fareAvailabilityKey -> productClass, fareAmount, publishedFare, serviceCharges
        """
        # Navigate to availability data. Some responses wrap in "data" or "response.data"
        av = data
        if "response" in av:
            av = av["response"]
        if "data" in av:
            av = av["data"]
        if "availabilityv4" in av:
            av = av["availabilityv4"]
        elif "availability" in av:
            av = av["availability"]
        else:
            logger.warning("Jazeera: no availability data in response")
            return []

        currency = av.get("currencyCode", req.currency or "KWD")

        # Build fare price lookup: fareAvailabilityKey -> {amount, productClass}
        fare_lookup: dict[str, dict] = {}
        for fa_entry in av.get("faresAvailable", []):
            fak = fa_entry.get("key", "")
            val = fa_entry.get("value", {})
            if not fak or not val:
                continue
            for fare in val.get("fares", []):
                pc = fare.get("productClass", "")
                for pf in fare.get("passengerFares", []):
                    amount = pf.get("fareAmount")
                    if amount is not None and amount > 0:
                        existing = fare_lookup.get(fak, {}).get("amount")
                        if existing is None or amount < existing:
                            fare_lookup[fak] = {
                                "amount": amount,
                                "publishedFare": pf.get("publishedFare", amount),
                                "productClass": pc,
                            }

        # Extract journeys
        results = av.get("results", [])
        if not results:
            logger.warning("Jazeera: no results in availability")
            return []

        target_key = f"{req.origin}|{req.destination}"
        journeys: list[dict] = []

        for result in results:
            for trip in result.get("trips", []):
                for market in trip.get("journeysAvailableByMarket", []):
                    mk = market.get("key", "")
                    if mk == target_key or not journeys:
                        jlist = market.get("value", [])
                        if mk == target_key:
                            journeys = jlist
                            break
                        elif not journeys:
                            journeys = jlist

        if not journeys:
            logger.warning(
                "Jazeera: no journeys for %s|%s", req.origin, req.destination,
            )
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for journey in journeys:
            des = journey.get("designator", {})
            origin = des.get("origin", req.origin)
            destination = des.get("destination", req.destination)

            # Find cheapest fare
            best_price: float | None = None
            best_class = ""
            for fare in journey.get("fares", []):
                fak = fare.get("fareAvailabilityKey", "")
                info = fare_lookup.get(fak)
                if info and info["amount"] > 0:
                    if best_price is None or info["amount"] < best_price:
                        best_price = info["amount"]
                        best_class = info.get("productClass", "")

            if best_price is None:
                continue

            # Build segments from journey data
            segments_data = journey.get("segments", [])
            flight_segments: list[FlightSegment] = []
            flight_numbers: list[str] = []

            for seg in segments_data:
                # Segment-level designator has carrier info
                seg_ident = seg.get("identifier", {})
                carrier = seg_ident.get("carrierCode", "J9") if seg_ident else "J9"
                flight_num = seg_ident.get("identifier", "") if seg_ident else ""

                # Get times from legs (UTC times available in legInfo)
                legs = seg.get("legs", [])
                if legs:
                    leg_info = legs[0].get("legInfo", {})
                    dep_utc = leg_info.get("departureTimeUtc", "")
                    arr_utc = leg_info.get("arrivalTimeUtc", "")
                    dep_terminal = leg_info.get("departureTerminal", "")
                    arr_terminal = leg_info.get("arrivalTerminal", "")
                    equipment = leg_info.get("equipmentType", "")
                else:
                    dep_utc = ""
                    arr_utc = ""
                    equipment = ""

                # Use journey designator for origin/dest per segment
                seg_des = seg.get("designator", {})
                seg_origin = seg_des.get("origin", origin) if seg_des else origin
                seg_dest = seg_des.get("destination", destination) if seg_des else destination

                # Prefer local times from designator, fall back to UTC
                seg_dep = seg_des.get("departure", "") if seg_des else ""
                seg_arr = seg_des.get("arrival", "") if seg_des else ""
                if not seg_dep and dep_utc:
                    seg_dep = dep_utc
                if not seg_arr and arr_utc:
                    seg_arr = arr_utc

                fn = f"J9{flight_num}" if flight_num and not flight_num.startswith("J9") else (flight_num or f"J9")
                flight_numbers.append(fn)

                cabin = self._map_cabin_class(best_class)

                flight_segments.append(FlightSegment(
                    airline="J9",
                    airline_name="Jazeera Airways",
                    flight_no=fn,
                    origin=seg_origin,
                    destination=seg_dest,
                    departure=self._parse_dt(seg_dep),
                    arrival=self._parse_dt(seg_arr),
                    cabin_class=cabin,
                    aircraft=equipment,
                ))

            if not flight_segments:
                # Fallback: use journey-level designator
                dep_str = des.get("departure", "")
                arr_str = des.get("arrival", "")
                cabin = self._map_cabin_class(best_class)
                flight_segments.append(FlightSegment(
                    airline="J9",
                    airline_name="Jazeera Airways",
                    flight_no="J9",
                    origin=origin,
                    destination=destination,
                    departure=self._parse_dt(dep_str),
                    arrival=self._parse_dt(arr_str),
                    cabin_class=cabin,
                ))

            stopovers = max(len(segments_data) - 1, 0)
            if journey.get("stops") is not None:
                stopovers = journey["stops"]

            route = FlightRoute(
                segments=flight_segments,
                total_duration_seconds=0,
                stopovers=stopovers,
            )

            dep_str = des.get("departure", "")
            offer_key = "_".join(flight_numbers) + f"_{dep_str[:10]}"
            price = round(best_price, 2)

            offers.append(FlightOffer(
                id=f"j9_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
                price=price,
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Jazeera Airways"],
                owner_airline="J9",
                booking_url=booking_url,
                is_locked=False,
                source="jazeera_direct",
                source_tier="free",
            ))

        logger.info(
            "Jazeera: found %d offers for %s->%s on %s",
            len(offers), req.origin, req.destination,
            req.date_from.strftime("%Y-%m-%d"),
        )
        return offers

    # ── Helpers ───────────────────────────────────────────

    @staticmethod
    def _map_cabin_class(product_class: str) -> str:
        """Map Jazeera product class codes to standard cabin classes."""
        pc = product_class.upper() if product_class else ""
        if pc.startswith("E"):
            return "economy"
        if pc.startswith("B"):
            return "business"
        return "economy"

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float,
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "Jazeera %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        search_hash = hashlib.md5(
            f"jazeera{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "KWD"),
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
        for fmt in (
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
        ):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://booking.jazeeraairways.com/en/search-flight"
            f"?origin={req.origin}&destination={req.destination}"
            f"&departureDate={dep}&adults={req.adults}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"jazeera{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "KWD",
            offers=[],
            total_results=0,
        )
