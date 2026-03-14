"""
Air Arabia direct API scraper — queries FeaturedOffers REST endpoint via httpx.

Air Arabia (IATA: G9) is a UAE low-cost carrier based in Sharjah.

The reservation IBE (reservations.airarabia.com) is protected by:
  - WAF that blocks direct httpx/curl (403 AAHDR0015)
  - HAProxy CAPTCHA challenge for fresh sessions
  - Cloudflare Turnstile that does not solve in automated browsers

Strategy (httpx, no browser):
  The www.airarabia.com marketing site exposes a FeaturedOffers API that
  returns best-priced flights per route for a given origin country and month.
  This endpoint requires only a static api-key header — no cookies, no
  Turnstile, no browser.

  Limitations:
    - Returns best-deal flights only (cheapest per featured route per month)
    - Route filtering not supported server-side (we filter client-side)
    - ~6-8 featured routes per country, 1 offer per route per month
    - Not a full search engine — provides real pricing for promoted routes

API endpoint:
  POST https://www.airarabia.com/api/bestoffers/FeaturedOffers
  Header: api-key: 2a109656-f0c7-4362-a123-6c61e18af314

Request body:
  {currency, includeTax: true, countryCode, month, count}

Response structure:
  BestOffersGroups[].OriginAirportCode / DestinationAirportCode
  BestOffersGroups[].BestOffers[]:
    DepartureDateTimeLocal, ArrivalDateTimeLocal, FlightNumber,
    Origin.airportcode, Destination.airportcode, AircraftModel,
    Fare, Surcharge, Tax, Total, CurrencyCode, SegmentCode
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

_API_URL = "https://www.airarabia.com/api/bestoffers/FeaturedOffers"
_API_KEY = "2a109656-f0c7-4362-a123-6c61e18af314"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Content-Type": "application/json",
    "api-key": _API_KEY,
    "Origin": "https://www.airarabia.com",
    "Referer": "https://www.airarabia.com/",
}

# Map origin IATA code → ISO-2 country code (used for countryCode param)
_ORIGIN_COUNTRY: dict[str, str] = {
    # UAE
    "SHJ": "AE", "AUH": "AE", "DXB": "AE", "RKT": "AE",
    # Egypt
    "CAI": "EG", "HBE": "EG", "ATZ": "EG", "SPX": "EG",
    # Morocco
    "CMN": "MA", "FEZ": "MA", "RBA": "MA", "TNG": "MA", "NDR": "MA",
    # Jordan / Gulf
    "AMM": "JO", "MCT": "OM", "SLL": "OM", "OHS": "OM",
    "BAH": "BH", "KWI": "KW", "DOH": "QA",
    # Pakistan
    "ISB": "PK", "LHE": "PK", "KHI": "PK", "MUX": "PK", "SKT": "PK",
    # Bangladesh
    "DAC": "BD", "CGP": "BD",
    # India
    "DEL": "IN", "BOM": "IN", "BLR": "IN", "MAA": "IN", "COK": "IN",
    "HYD": "IN", "AMD": "IN", "JAI": "IN", "CCJ": "IN", "TRV": "IN",
    # Africa
    "NBO": "KE", "ADD": "ET", "KRT": "SD",
    # Turkey
    "SAW": "TR", "IST": "TR",
    # Europe
    "LGW": "GB", "CDG": "FR", "FCO": "IT", "BCN": "ES", "BER": "DE",
    "WAW": "PL", "KRK": "PL", "VIE": "AT", "BRU": "BE",
}


class AirArabiaConnectorClient:
    """Air Arabia scraper — httpx calls to FeaturedOffers API, no browser."""

    def __init__(self, timeout: float = 15.0):
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
        """Search Air Arabia via FeaturedOffers API.

        Determines the origin country, calls the API for relevant month(s),
        and filters results to the requested route.
        """
        t0 = time.monotonic()
        country = _ORIGIN_COUNTRY.get(req.origin)
        if not country:
            # Fallback: resolve via shared airport→country map
            from connectors.airline_routes import get_country
            country = get_country(req.origin)
        if not country:
            logger.debug(
                "AirArabia: origin %s not in country map, skipping", req.origin
            )
            return self._empty(req)

        currency = req.currency or "AED"

        # Determine which months to query (date_from month, and date_to month if different)
        months: list[str] = [str(req.date_from.month)]
        if req.date_to and req.date_to.month != req.date_from.month:
            months.append(str(req.date_to.month))

        client = await self._client()
        all_offers: list[FlightOffer] = []

        for month in months:
            payload = {
                "currency": currency,
                "includeTax": True,
                "countryCode": country,
                "month": month,
                "count": 30,
            }
            try:
                resp = await client.post(_API_URL, json=payload)
            except httpx.TimeoutException:
                logger.warning("AirArabia FeaturedOffers timed out for %s month %s", country, month)
                continue
            except Exception as e:
                logger.error("AirArabia FeaturedOffers error: %s", e)
                continue

            if resp.status_code != 200:
                logger.warning(
                    "AirArabia FeaturedOffers %d for %s month %s: %s",
                    resp.status_code, country, month, resp.text[:200],
                )
                continue

            try:
                data = resp.json()
            except Exception:
                logger.warning("AirArabia returned non-JSON for %s month %s", country, month)
                continue

            if not data.get("Success"):
                logger.debug("AirArabia FeaturedOffers Success=false for %s month %s", country, month)
                continue

            offers = self._parse_featured(data, req, currency)
            all_offers.extend(offers)

        elapsed = time.monotonic() - t0

        # Deduplicate by offer ID
        seen: set[str] = set()
        unique: list[FlightOffer] = []
        for o in all_offers:
            if o.id not in seen:
                seen.add(o.id)
                unique.append(o)

        unique.sort(key=lambda o: o.price)

        logger.info(
            "AirArabia %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(unique), elapsed,
        )

        search_hash = hashlib.md5(
            f"airarabia{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=currency,
            offers=unique,
            total_results=len(unique),
        )

    def _parse_featured(
        self, data: dict, req: FlightSearchRequest, currency: str
    ) -> list[FlightOffer]:
        """Parse FeaturedOffers response, filtering to the requested route.

        FeaturedOffers returns ~1 best-deal per route per month.  The departure
        date is whichever day is cheapest, so we accept any date in the queried
        month(s) rather than filtering to the exact requested date range.
        """
        offers: list[FlightOffer] = []

        for group in data.get("BestOffersGroups", []):
            origin_code = group.get("OriginAirportCode", "")
            dest_code = group.get("DestinationAirportCode", "")

            # Filter to requested route
            if origin_code != req.origin or dest_code != req.destination:
                continue

            for bo in group.get("BestOffers", []):
                dep_str = bo.get("DepartureDateTimeLocal", "")
                arr_str = bo.get("ArrivalDateTimeLocal", "")
                dep_dt = self._parse_dt(dep_str)
                arr_dt = self._parse_dt(arr_str)

                total = bo.get("Total")
                if total is None or float(total) <= 0:
                    continue
                price = round(float(total), 2)

                flight_no = bo.get("FlightNumber", "")
                cur = bo.get("CurrencyCode") or currency
                seg_code = bo.get("SegmentCode", "")  # e.g. "SHJ/DEL"
                aircraft = bo.get("AircraftModel", "")

                # Build origin/dest from nested objects or fallback to group codes
                seg_origin = origin_code
                seg_dest = dest_code
                if isinstance(bo.get("Origin"), dict):
                    seg_origin = bo["Origin"].get("airportcode", origin_code)
                if isinstance(bo.get("Destination"), dict):
                    seg_dest = bo["Destination"].get("airportcode", dest_code)

                # Duration from departure/arrival
                duration_secs = 0
                if dep_dt.year > 2000 and arr_dt.year > 2000:
                    delta = arr_dt - dep_dt
                    duration_secs = max(int(delta.total_seconds()), 0)

                segment = FlightSegment(
                    airline="G9",
                    airline_name="Air Arabia",
                    flight_no=flight_no,
                    origin=seg_origin,
                    destination=seg_dest,
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=duration_secs,
                    cabin_class="economy",
                )

                route = FlightRoute(
                    segments=[segment],
                    total_duration_seconds=duration_secs,
                    stopovers=0,
                )

                offer_id = (
                    f"g9_{hashlib.md5(f'{flight_no}_{dep_str}_{price}'.encode()).hexdigest()[:12]}"
                )

                offers.append(FlightOffer(
                    id=offer_id,
                    price=price,
                    currency=cur,
                    price_formatted=f"{price:.2f} {cur}",
                    outbound=route,
                    inbound=None,
                    airlines=["Air Arabia"],
                    owner_airline="G9",
                    booking_url=self._build_booking_url(req),
                    is_locked=False,
                    source="airarabia_direct",
                    source_tier="free",
                ))

        return offers

    @staticmethod
    def _parse_dt(s: str) -> datetime:
        """Parse datetime strings from the FeaturedOffers API."""
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s[:19], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        """Build a direct Air Arabia booking URL."""
        date_str = req.date_from.strftime("%d-%m-%Y")
        country = _ORIGIN_COUNTRY.get(req.origin, "AE")
        return (
            f"https://reservations.airarabia.com/service-app/ibe/reservation.html"
            f"#/fare/en/{req.currency or 'AED'}/{country}/{req.origin}/{req.destination}/"
            f"{date_str}/N/{req.adults or 1}/{req.children or 0}/{req.infants or 0}/Y//Y/Y"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"airarabia{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "AED",
            offers=[],
            total_results=0,
        )
