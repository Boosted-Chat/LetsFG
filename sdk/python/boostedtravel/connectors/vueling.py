"""
Vueling hybrid scraper — direct API first, Playwright fallback.

Vueling (IATA: VY) is a Spanish low-cost carrier.
Booking engine: tickets.vueling.com (Angular 17 SPA).

Hybrid strategy (Mar 2026):
1. Try direct HTTP via curl_cffi (Chrome TLS fingerprint):
   a. Auth: POST ams.vueling.com/asm/v1/Auth → Bearer JWT (1199s TTL)
   b. GQL:  POST ams.vueling.com/avy/v1/graphql → all flights for date
   - Token is cached and reused until close to expiry
   - ~0.7s total (auth + search), no browser needed
2. If direct API fails → fall back to full Playwright browser flow
   - Navigate to tickets.vueling.com, capture GQL request, replay in-page

GQL query extracted from Angular bundle (chunk-EXIIY677.js):
  query GetAvy($requestAVY: AvailabilityRequestGraphQLInput!) {
    amsAvy(amsAvailabilityRequest: $requestAVY) { ... }
  }
  Variables: requestAVY.request.{criteria, passengers, flightType, ...}
  Returns: trips (journeys with segments) + faresAvailable (prices per fare class)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
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

# ── Direct API constants ─────────────────────────────────
_AUTH_URL = "https://ams.vueling.com/asm/v1/Auth"
_GQL_URL = "https://ams.vueling.com/avy/v1/graphql"
_PROFILE_ID = "e8ffa738-cb67-4a02-b501-9bfd975a4b65"
_TOKEN_MARGIN = 60  # refresh token 60s before expiry

# Cached token state
_token: str | None = None
_token_expiry: float = 0  # monotonic timestamp

_GQL_QUERY = """query GetAvy($requestAVY:AvailabilityRequestGraphQLInput!){ amsAvy (amsAvailabilityRequest  : $requestAVY) {
      currencyCode,
      trips {
        trips {
        journeysAvailableByMarket  {
            key,
          value{
                journeyKey,
                segments {
                    carrierCodeShare,
                    segmentType,
                    segmentKey,
                    identifier {
                        carrierCode,
                        identifier
                    },
                    designator{
                        arrival,
                        departure,
                        destination,
                        origin
                    },
                    segmentDuration,
                    legs {
                        legKey,
                        legInfo {
                            operatingCarrier,
                            operatedByText,
                            departureTerminal,
                            arrivalTerminal,
                            arrivalTimeUtc,
                            departureTimeUtc,
                            departureTimeVariant,
                            lid,
                            sold
                        },
                        ssrs {
                            available,
                            ssrNestCode
                        }
                    },
                },designator{
                    arrival,
                    departure,
                    destination,
                    origin
                },
                fares{
                    fareAvailabilityKey,
                    fareSellKey,
                    details {
                        serviceBundleSetCode,
                        bundleReferences,
                        reference,
                        availableCount
                    }
                },
                flightType,
                duration,
                connectionTime
            }
        }
      }
    },
    faresAvailable {
      value {
            fares {
                productClass,
                classOfService,
                fareBasisCode,
                passengerFares {
                    amsFareAmount
                },
                reference,
                ruleNumber
            },
            fareAvailabilityKey
      }
    },
    bundleOffers {
        key,
        value {
            bundlePrices{
                totalPrice, taxTotal, feePrice, passengerType,
                program {
                    code, level
                },
                bundleSsrPrices {
                    bundleSsrPrice, taxTotal, ssrCode
                }
            }, bundleCode
        }
    }
 }
}"""

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_TIMEZONES = ["Europe/London", "Europe/Berlin", "Europe/Paris", "Europe/Madrid"]

_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Shared headed Chromium (launched once, reused across searches)."""
    global _pw_instance, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from connectors.browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("Vueling: browser launched")
        return _browser


async def _ensure_token() -> str | None:
    """Get a valid Bearer token, refreshing if needed."""
    global _token, _token_expiry
    if _token and time.monotonic() < _token_expiry:
        return _token
    try:
        from curl_cffi import requests as cffi_requests
        ses = cffi_requests.Session(impersonate="chrome")
        r = ses.post(_AUTH_URL, json={"profileId": _PROFILE_ID}, timeout=10)
        if r.status_code != 200:
            logger.warning("Vueling auth failed: %s %s", r.status_code, r.text[:200])
            return None
        data = r.json()
        tok = data.get("token") or data.get("accessToken")
        if not tok:
            for v in data.values():
                if isinstance(v, str) and len(v) > 50:
                    tok = v
                    break
        if tok:
            ttl = data.get("expiresIn", data.get("expires_in", 1199))
            _token = tok
            _token_expiry = time.monotonic() + ttl - _TOKEN_MARGIN
            logger.info("Vueling: auth token acquired (TTL=%ss)", ttl)
            return tok
        logger.warning("Vueling: no token in auth response")
        return None
    except Exception as e:
        logger.warning("Vueling: auth error: %s", e)
        return None


class VuelingConnectorClient:
    """Vueling hybrid scraper — direct API first, Playwright fallback."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    # ── Main entry point ─────────────────────────────────
    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Try direct API first (no browser)
        result = await self._search_via_api(req)
        if result and result.total_results > 0:
            return result
        logger.info("Vueling: API path returned 0 results, trying Playwright fallback")
        return await self._search_via_browser(req)

    # ── Direct API path (curl_cffi) ──────────────────────
    async def _search_via_api(self, req: FlightSearchRequest) -> FlightSearchResponse | None:
        t0 = time.monotonic()
        token = await _ensure_token()
        if not token:
            return None
        try:
            from curl_cffi import requests as cffi_requests

            variables = {
                "requestAVY": {
                    "request": {
                        "criteria": [
                            {
                                "origin": req.origin,
                                "destination": req.destination,
                                "date": req.date_from.strftime("%Y-%m-%d"),
                            }
                        ],
                        "cultureCode": "en-GB",
                        "currencyCode": req.currency or "EUR",
                        "flightType": "ALL",
                        "itineraryType": "ONE_WAY",
                        "maxConnections": 10,
                        "passengers": [
                            {"count": req.adults, "type": "Adult"},
                        ],
                        "serviceCode": "Search1Day",
                        "servicesToRequest": [],
                        "bundleControlFilter": 2,
                    },
                    "trackingPoint": "BBV",
                }
            }

            # Add children if requested
            if req.children:
                variables["requestAVY"]["request"]["passengers"].append(
                    {"count": req.children, "type": "Child"}
                )
            if req.infants:
                variables["requestAVY"]["request"]["servicesToRequest"].append(
                    {"count": req.infants, "serviceCode": "INFANT"}
                )

            payload = json.dumps({"query": _GQL_QUERY, "variables": variables})
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }

            ses = cffi_requests.Session(impersonate="chrome")
            r = ses.post(_GQL_URL, data=payload, headers=headers, timeout=15)

            if r.status_code != 200:
                logger.warning("Vueling GQL HTTP %s: %s", r.status_code, r.text[:300])
                return None

            data = r.json()
            if "errors" in data:
                logger.warning("Vueling GQL errors: %s", data["errors"])
                return None

            elapsed = time.monotonic() - t0
            offers = self._parse_graphql(data, req)
            logger.info(
                "Vueling API: %d offers %s->%s in %.2fs",
                len(offers), req.origin, req.destination, elapsed,
            )
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.warning("Vueling API error: %s", e)
            return None

    # ── Playwright fallback ──────────────────────────────
    async def _search_via_browser(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            browser = await _get_browser()
        except Exception as e:
            logger.error("Vueling: browser launch failed: %s", e)
            return self._empty(req)
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-GB",
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )

        try:
            try:
                from playwright_stealth import stealth_async

                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            # Intercept the GraphQL REQUEST to capture auth + query template
            gql_request: dict = {}
            gql_event = asyncio.Event()

            async def on_request(request):
                if (
                    "graphql" in request.url.lower()
                    and "vueling" in request.url.lower()
                    and request.method == "POST"
                    and not gql_request
                ):
                    body = request.post_data
                    if body:
                        gql_request["body"] = body
                        gql_request["headers"] = dict(request.headers)
                        gql_event.set()

            page.on("request", on_request)

            logger.info(
                "Vueling: searching %s->%s on %s",
                req.origin, req.destination, req.date_from.strftime("%Y-%m-%d"),
            )

            # Step 1: Navigate directly to Angular SPA
            await page.goto(
                "https://tickets.vueling.com/booking/flightSearch",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(4)
            await self._dismiss_cookies(page)

            # Step 2: Fill destination to establish session + trigger allFlights
            dest_input = page.locator("#id-destination")
            if await dest_input.count() > 0 and await dest_input.is_visible():
                await dest_input.click()
                await dest_input.fill(req.destination)
                await asyncio.sleep(1.5)
                opts = page.locator("[role='option']")
                for i in range(await opts.count()):
                    txt = await opts.nth(i).text_content() or ""
                    if req.destination in txt:
                        await opts.nth(i).click()
                        break
                else:
                    if await opts.count() > 0:
                        await opts.first.click()
                await asyncio.sleep(1)

            # Step 3: Click Search — triggers GraphQL with default date
            # (we only need the request headers + query template)
            search_btn = page.locator(
                "button:has-text('Search'), "
                "button:has-text('SEARCH'), "
                "button:has-text('Buscar'), "
                "button.btn--full-width"
            )
            if await search_btn.count() > 0:
                await search_btn.first.click(timeout=5000)
                logger.info("Vueling: clicked Search to capture template")
            else:
                logger.warning("Vueling: no Search button found")
                return self._empty(req)

            # Wait for GQL request to be captured
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(gql_event.wait(), timeout=min(remaining, 15))
            except asyncio.TimeoutError:
                logger.warning("Vueling: timed out capturing GQL request template")
                return self._empty(req)

            await asyncio.sleep(1)

            # Step 4: Replay GraphQL with correct parameters
            original = json.loads(gql_request["body"])
            query = original["query"]
            date_str = req.date_from.strftime("%Y-%m-%dT00:00:00.000Z")

            new_variables = {
                "requestAVY": {
                    "request": {
                        "criteria": [
                            {
                                "origin": req.origin,
                                "destination": req.destination,
                                "date": date_str,
                            },
                            {
                                "origin": req.destination,
                                "destination": req.origin,
                                "date": date_str,
                            },
                        ],
                        "cultureCode": "en-GB",
                        "currencyCode": req.currency or "EUR",
                        "flightType": "ALL",
                        "itineraryType": "ROUND_TRIP",
                        "maxConnections": 10,
                        "passengers": [
                            {"count": req.adults, "type": "Adult"},
                        ],
                        "serviceCode": "Search1Day",
                        "servicesToRequest": [],
                        "bundleControlFilter": 2,
                    },
                    "trackingPoint": "BBV",
                }
            }

            payload = json.dumps({"query": query, "variables": new_variables})
            headers = gql_request["headers"]

            result_text = await page.evaluate(
                """async ([url, headers, body]) => {
                    const h = {};
                    for (const [k, v] of Object.entries(headers)) {
                        if (!['host','content-length',':method',':path',':scheme',':authority']
                            .includes(k.toLowerCase())) {
                            h[k] = v;
                        }
                    }
                    h['content-type'] = 'application/json';
                    const resp = await fetch(url, {
                        method: 'POST', headers: h, body: body, credentials: 'include'
                    });
                    return resp.text();
                }""",
                ["https://ams.vueling.com/avy/v1/graphql", headers, payload],
            )

            data = json.loads(result_text)
            elapsed = time.monotonic() - t0
            offers = self._parse_graphql(data, req)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Vueling Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Cookie dismissal
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        try:
            btn = page.locator("#onetrust-accept-btn-handler")
            if await btn.count() > 0:
                await btn.click(timeout=3000)
                await asyncio.sleep(0.5)
                return
        except Exception:
            pass

        for label in [
            "Accept all cookies", "Accept All", "ACCEPT ALL COOKIES",
        ]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue

    # ------------------------------------------------------------------
    # GraphQL response parsing
    # ------------------------------------------------------------------

    def _parse_graphql(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Parse the GraphQL response into FlightOffers.

        Structure:
        - data.amsAvy.trips[0].trips[0].journeysAvailableByMarket[{key, value}]
          Each journey: designator, segments, fareAvailabilityKey
        - data.amsAvy.faresAvailable[{value: {fareAvailabilityKey, fares}}]
          Maps fareAvailabilityKey → product-class prices (BA/OP/TF/FF)
        """
        ams = data.get("data", {}).get("amsAvy", {})
        if not ams:
            logger.warning("Vueling: no amsAvy in GraphQL response")
            return []

        currency = ams.get("currencyCode", req.currency or "EUR")

        # Build fare price lookup: fareAvailabilityKey -> cheapest {amount, productClass}
        fare_lookup: dict[str, dict] = {}
        for fa_entry in ams.get("faresAvailable", []):
            val = fa_entry.get("value", {})
            fak = val.get("fareAvailabilityKey", "")
            if not fak:
                continue
            for fare in val.get("fares", []):
                for pf in fare.get("passengerFares", []):
                    amount = pf.get("amsFareAmount")
                    if amount is not None:
                        existing = fare_lookup.get(fak, {}).get("amount")
                        if existing is None or amount < existing:
                            fare_lookup[fak] = {
                                "amount": amount,
                                "productClass": fare.get("productClass", ""),
                            }

        # Extract outbound journeys
        trips_list = ams.get("trips", [])
        if not trips_list:
            logger.warning("Vueling: no trips in GraphQL response")
            return []

        journeys: list[dict] = []
        inner_trips = trips_list[0].get("trips", [])
        if inner_trips:
            target_key = f"{req.origin}|{req.destination}"
            for market in inner_trips[0].get("journeysAvailableByMarket", []):
                if market.get("key", "") == target_key:
                    journeys = market.get("value", [])
                    break
            if not journeys and inner_trips[0].get("journeysAvailableByMarket"):
                journeys = inner_trips[0]["journeysAvailableByMarket"][0].get(
                    "value", []
                )

        if not journeys:
            logger.warning(
                "Vueling: no journeys for %s|%s", req.origin, req.destination
            )
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for journey in journeys:
            if journey.get("notForSale") or journey.get("isSoldOut"):
                continue

            des = journey.get("designator", {})
            origin = des.get("origin", req.origin)
            destination = des.get("destination", req.destination)

            # Find cheapest fare: journey.fares[] has fareAvailabilityKey entries
            best_price: float | None = None
            best_class = ""
            for fare in journey.get("fares", []):
                fak = fare.get("fareAvailabilityKey", "")
                info = fare_lookup.get(fak)
                if info and info["amount"] is not None and info["amount"] > 0:
                    if best_price is None or info["amount"] < best_price:
                        best_price = info["amount"]
                        best_class = info.get("productClass", "")

            if best_price is None:
                continue

            # Build segments
            segments_data = journey.get("segments", [])
            flight_segments: list[FlightSegment] = []
            flight_numbers: list[str] = []

            for seg in segments_data:
                ident = seg.get("identifier", {})
                carrier = ident.get("carrierCode", "VY")
                flight_num = ident.get("identifier", "")
                seg_des = seg.get("designator", {})

                flight_numbers.append(f"{carrier}{flight_num}")
                flight_segments.append(FlightSegment(
                    airline=carrier,
                    airline_name="Vueling",
                    flight_no=f"{carrier}{flight_num}",
                    origin=seg_des.get("origin", origin),
                    destination=seg_des.get("destination", destination),
                    departure=self._parse_dt(seg_des.get("departure", "")),
                    arrival=self._parse_dt(seg_des.get("arrival", "")),
                    cabin_class=best_class or "M",
                ))

            if not flight_segments:
                continue

            stopovers = max(len(segments_data) - 1, 0)

            route = FlightRoute(
                segments=flight_segments,
                total_duration_seconds=0,
                stopovers=stopovers,
            )

            dep_str = des.get("departure", "")
            offer_key = "_".join(flight_numbers) + f"_{dep_str[:10]}"
            price = round(best_price, 2)

            offers.append(FlightOffer(
                id=f"vy_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
                price=price,
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Vueling"],
                owner_airline="VY",
                booking_url=booking_url,
                is_locked=False,
                source="vueling_direct",
                source_tier="free",
            ))

        logger.info(
            "Vueling: found %d offers for %s->%s on %s",
            len(offers), req.origin, req.destination,
            req.date_from.strftime("%Y-%m-%d"),
        )
        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float,
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "Vueling %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        search_hash = hashlib.md5(
            f"vueling{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "EUR"),
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
            f"https://tickets.vueling.com/ScheduleSelect.aspx"
            f"?culture=en-GB&origin={req.origin}&destination={req.destination}"
            f"&departureDate={dep}&adults={req.adults}"
            f"&children={req.children}&infants={req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"vueling{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "EUR",
            offers=[],
            total_results=0,
        )
