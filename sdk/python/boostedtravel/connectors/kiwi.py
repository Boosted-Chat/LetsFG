"""
Kiwi.com website connector — LCC flights + virtual interlining.

Scrapes the kiwi.com frontend GraphQL API (umbrella/v2/graphql) which powers
their search results page. Zero auth required — just needs the Referer header.

The old Skypicker REST API (api.skypicker.com/flights) and Tequila API both
require paid API keys. This connector uses the same GraphQL endpoint that the
kiwi.com website uses, which is free and rate-limit-friendly.

Supports one-way and return itineraries, airport or city-level searches,
and all the Kiwi virtual interlining magic (combining LCC one-way fares).
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import date, datetime
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

KIWI_GRAPHQL_URL = "https://api.skypicker.com/umbrella/v2/graphql"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.kiwi.com/",
    "Origin": "https://www.kiwi.com",
}

# Minimal GraphQL query for one-way flights
_ONEWAY_QUERY = """query SearchOnewayItinerariesQuery(
  $search: SearchOnewayInput
  $filter: ItinerariesFilterInput
  $options: ItinerariesOptionsInput
) {
  onewayItineraries(search: $search, filter: $filter, options: $options) {
    __typename
    ... on AppError { error: message }
    ... on Itineraries {
      metadata { itinerariesCount hasMorePending }
      itineraries {
        __typename
        ... on ItineraryOneWay {
          id
          price { amount }
          priceEur { amount }
          provider { name code }
          duration
          sector {
            sectorSegments {
              segment {
                source { localTime utcTimeIso station { code name city { name } } }
                destination { localTime utcTimeIso station { code name city { name } } }
                duration
                type
                code
                carrier { name code }
                operatingCarrier { name code }
                cabinClass
              }
              layover { duration }
            }
            duration
          }
          bookingOptions { edges { node { bookingUrl price { amount } } } }
          travelHack { isVirtualInterlining isThrowawayTicket isTrueHiddenCity }
        }
      }
    }
  }
}"""

# Minimal GraphQL query for return flights
_RETURN_QUERY = """query SearchReturnItinerariesQuery(
  $search: SearchReturnInput
  $filter: ItinerariesFilterInput
  $options: ItinerariesOptionsInput
) {
  returnItineraries(search: $search, filter: $filter, options: $options) {
    __typename
    ... on AppError { error: message }
    ... on Itineraries {
      metadata { itinerariesCount hasMorePending }
      itineraries {
        __typename
        ... on ItineraryReturn {
          id
          price { amount }
          priceEur { amount }
          provider { name code }
          duration
          outbound {
            sectorSegments {
              segment {
                source { localTime utcTimeIso station { code name city { name } } }
                destination { localTime utcTimeIso station { code name city { name } } }
                duration
                type
                code
                carrier { name code }
                operatingCarrier { name code }
                cabinClass
              }
              layover { duration }
            }
            duration
          }
          inbound {
            sectorSegments {
              segment {
                source { localTime utcTimeIso station { code name city { name } } }
                destination { localTime utcTimeIso station { code name city { name } } }
                duration
                type
                code
                carrier { name code }
                operatingCarrier { name code }
                cabinClass
              }
              layover { duration }
            }
            duration
          }
          bookingOptions { edges { node { bookingUrl price { amount } } } }
          travelHack { isVirtualInterlining isThrowawayTicket isTrueHiddenCity }
        }
      }
    }
  }
}"""

# Cabin class mapping
_CABIN_MAP = {"M": "ECONOMY", "W": "PREMIUM_ECONOMY", "C": "BUSINESS", "F": "FIRST"}


class KiwiConnectorClient:
    """
    Kiwi.com website connector — scrapes their frontend GraphQL API.

    No API key required. Uses the same endpoint as kiwi.com website.
    Supports IATA codes (e.g. STN, BCN) and city-level searches.
    """

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    @property
    def available(self) -> bool:
        return True

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

    _CURRENCY_MARKET = {
        "PLN": "pl", "EUR": "de", "GBP": "gb", "USD": "us",
        "CZK": "cz", "HUF": "hu", "SEK": "se", "NOK": "no",
        "DKK": "dk", "CHF": "ch", "RON": "ro", "BGN": "bg",
        "HRK": "hr", "TRY": "tr", "RUB": "ru", "UAH": "ua",
    }

    def _guess_market(self, currency: str) -> str:
        return self._CURRENCY_MARKET.get(currency.upper(), "gb")

    def _location_id(self, code: str) -> str:
        """Convert IATA code to Kiwi location ID."""
        code = code.strip().upper()
        if len(code) == 3 and code.isalpha():
            return f"Station:airport:{code}"
        return code

    def _build_variables(self, req: FlightSearchRequest, is_return: bool) -> dict:
        """Build GraphQL variables from FlightSearchRequest."""
        date_str = f"{req.date_from.isoformat()}T00:00:00"
        date_end = f"{req.date_from.isoformat()}T23:59:59"

        itinerary: dict[str, Any] = {
            "source": {"ids": [self._location_id(req.origin)]},
            "destination": {"ids": [self._location_id(req.destination)]},
            "outboundDepartureDate": {"start": date_str, "end": date_end},
        }

        if is_return and req.return_from:
            ret_str = f"{req.return_from.isoformat()}T00:00:00"
            ret_end = f"{req.return_from.isoformat()}T23:59:59"
            itinerary["inboundDepartureDate"] = {"start": ret_str, "end": ret_end}

        cabin = _CABIN_MAP.get(req.cabin_class, "ECONOMY") if req.cabin_class else "ECONOMY"

        return {
            "search": {
                "itinerary": itinerary,
                "passengers": {
                    "adults": req.adults,
                    "children": req.children,
                    "infants": req.infants,
                    "adultsHoldBags": [0] * req.adults,
                    "adultsHandBags": [0] * req.adults,
                    "childrenHoldBags": [0] * req.children,
                    "childrenHandBags": [0] * req.children,
                },
                "cabinClass": {
                    "cabinClass": cabin,
                    "applyMixedClasses": False,
                },
            },
            "filter": {
                "transportTypes": ["FLIGHT"],
                "limit": min(req.limit or 100, 100),
                "enableSelfTransfer": True,
                "enableThrowAwayTicketing": True,
                "enableTrueHiddenCity": True,
            },
            "options": {
                "currency": req.currency.lower(),
                "locale": req.locale.split("-")[0] if req.locale else "en",
                "market": self._guess_market(req.currency),
                "partner": "skypicker",
                "sortBy": "PRICE",
            },
        }

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search flights via Kiwi.com's frontend GraphQL API."""
        client = await self._client()
        is_return = bool(req.return_from)

        query = _RETURN_QUERY if is_return else _ONEWAY_QUERY
        feature = "SearchReturnItinerariesQuery" if is_return else "SearchOnewayItinerariesQuery"
        variables = self._build_variables(req, is_return)

        t0 = time.monotonic()

        try:
            resp = await client.post(
                f"{KIWI_GRAPHQL_URL}?featureName={feature}",
                json={"query": query, "variables": variables},
            )
        except httpx.TimeoutException:
            logger.warning("Kiwi.com GraphQL timed out")
            return self._empty(req)
        except Exception as e:
            logger.error("Kiwi.com GraphQL error: %s", e)
            return self._empty(req)

        elapsed = time.monotonic() - t0

        if resp.status_code == 429:
            logger.warning("Kiwi.com rate limited (429)")
            return self._empty(req)

        if resp.status_code != 200:
            logger.warning("Kiwi.com returned %d: %s", resp.status_code, resp.text[:300])
            return self._empty(req)

        try:
            raw = resp.json()
        except Exception:
            logger.warning("Kiwi.com returned non-JSON")
            return self._empty(req)

        # Extract itineraries from response
        data = raw.get("data", {})
        root_key = "returnItineraries" if is_return else "onewayItineraries"
        result = data.get(root_key, {})

        if result.get("__typename") == "AppError":
            logger.warning("Kiwi.com error: %s", result.get("error", "unknown"))
            return self._empty(req)

        itineraries = result.get("itineraries", [])
        total = result.get("metadata", {}).get("itinerariesCount", len(itineraries))

        logger.info(
            "Kiwi.com %s→%s returned %d offers (total %d) in %.1fs",
            req.origin, req.destination, len(itineraries), total, elapsed,
        )

        offers = []
        for itin in itineraries:
            try:
                offer = self._parse_itinerary(itin, req, is_return)
                if offer:
                    offers.append(offer)
            except Exception as e:
                logger.debug("Failed to parse Kiwi itinerary: %s", e)
                continue

        search_hash = hashlib.md5(
            f"kiwiscrape{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=offers,
            total_results=total,
        )

    def _parse_itinerary(
        self, itin: dict, req: FlightSearchRequest, is_return: bool
    ) -> Optional[FlightOffer]:
        """Parse a Kiwi.com GraphQL itinerary into a FlightOffer."""
        price = float(itin.get("price", {}).get("amount", 0))
        if price <= 0:
            return None

        currency = req.currency

        # Parse outbound
        if is_return:
            outbound_data = itin.get("outbound", {})
            inbound_data = itin.get("inbound", {})
        else:
            outbound_data = itin.get("sector", {})
            inbound_data = None

        outbound = self._parse_sector(outbound_data, req) if outbound_data else None
        inbound = self._parse_sector(inbound_data, req) if inbound_data else None

        if not outbound:
            return None

        # Collect airlines
        all_segs = outbound.segments + (inbound.segments if inbound else [])
        airlines = list({s.airline for s in all_segs if s.airline})

        # Extract booking URL
        booking_url = ""
        booking_options = itin.get("bookingOptions", {}).get("edges", [])
        if booking_options:
            booking_url = booking_options[0].get("node", {}).get("bookingUrl", "")

        # Travel hack info
        travel_hack = itin.get("travelHack", {}) or {}
        conditions = {}
        if travel_hack.get("isVirtualInterlining"):
            conditions["virtual_interlining"] = "Different airlines combined for best price"
        if travel_hack.get("isThrowawayTicket"):
            conditions["throwaway_ticket"] = "Only using first leg of ticket"
        if travel_hack.get("isTrueHiddenCity"):
            conditions["hidden_city"] = "Hidden city ticketing"

        itin_id = itin.get("id", "")
        offer_id = f"ks_{hashlib.md5(itin_id.encode()).hexdigest()[:12]}" if itin_id else f"ks_{hashlib.md5(f'{price}{airlines}'.encode()).hexdigest()[:12]}"

        return FlightOffer(
            id=offer_id,
            price=price,
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=outbound,
            inbound=inbound,
            airlines=airlines,
            owner_airline=airlines[0] if airlines else "",
            booking_url=booking_url,
            is_locked=False,
            conditions=conditions,
            source="kiwi_connector",
            source_tier="free",
        )

    def _parse_sector(self, sector: dict, req: FlightSearchRequest) -> Optional[FlightRoute]:
        """Parse a sector (outbound/inbound) into a FlightRoute."""
        sector_segments = sector.get("sectorSegments", [])
        if not sector_segments:
            return None

        segments = []
        for ss in sector_segments:
            seg = ss.get("segment", {})
            if not seg:
                continue

            source = seg.get("source", {})
            dest = seg.get("destination", {})

            dep_dt = self._parse_dt(source.get("localTime", ""))
            arr_dt = self._parse_dt(dest.get("localTime", ""))

            carrier = seg.get("carrier", {}) or {}
            op_carrier = seg.get("operatingCarrier", {}) or {}

            segments.append(FlightSegment(
                airline=carrier.get("code", ""),
                airline_name=carrier.get("name", ""),
                flight_no=f"{carrier.get('code', '')}{seg.get('code', '')}",
                origin=source.get("station", {}).get("code", ""),
                destination=dest.get("station", {}).get("code", ""),
                origin_city=source.get("station", {}).get("city", {}).get("name", ""),
                destination_city=dest.get("station", {}).get("city", {}).get("name", ""),
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=int(seg.get("duration", 0)),
                cabin_class=seg.get("cabinClass", "ECONOMY"),
            ))

        total_dur = int(sector.get("duration", 0))
        if not total_dur and segments:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        return FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

    def _parse_dt(self, s: str) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"kiwiscrape{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
