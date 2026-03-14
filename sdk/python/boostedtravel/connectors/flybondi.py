"""
Flybondi hybrid scraper -- curl_cffi SSR extraction (primary) + Playwright fallback.

Flybondi (IATA: FO) is an Argentine low-cost carrier operating domestic
and regional routes from Buenos Aires (EZE/AEP/BUE) and other Argentine cities.
Default currency ARS.

Strategy (hybrid — API first, browser fallback):
1. (Primary) Fetch SSR page via curl_cffi — extract viewer.flights.edges from
   inline <script> tag. ~1.5s, zero browser, zero RAM.
2. (Fallback) Playwright headed Chrome — navigate to results URL, extract SSR
   from JS context, or intercept GraphQL response.
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

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL = True
except ImportError:
    HAS_CURL = False

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import stealth_args

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["es-AR", "es-UY", "es-PY", "en-US"]
_TIMEZONES = [
    "America/Argentina/Buenos_Aires", "America/Argentina/Cordoba",
    "America/Montevideo", "America/Asuncion",
]

_MAX_ATTEMPTS = 2

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
        logger.info("Flybondi: browser launched")
        return _browser


class FlybondiConnectorClient:
    """Flybondi hybrid scraper -- curl_cffi SSR extraction + Playwright fallback."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    def _build_search_url(self, req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1
        children = getattr(req, "children", 0) or 0
        infants = getattr(req, "infants", 0) or 0
        currency = req.currency or "ARS"
        return (
            f"https://flybondi.com/ar/search/results"
            f"?departureDate={dep}"
            f"&adults={adults}&children={children}&infants={infants}"
            f"&currency={currency}"
            f"&fromCityCode={req.origin}&toCityCode={req.destination}"
        )

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        search_url = self._build_search_url(req)

        # ── Primary: curl_cffi SSR extraction (fast, no browser) ──
        if HAS_CURL:
            try:
                offers = await asyncio.to_thread(self._search_via_api, search_url, req)
                if offers:
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "Flybondi API %s->%s: %d offers in %.1fs",
                        req.origin, req.destination, len(offers), elapsed,
                    )
                    return self._build_response(offers, req, elapsed)
                logger.warning("Flybondi API: no offers, falling back to Playwright")
            except Exception as e:
                logger.warning("Flybondi API error: %s — falling back to Playwright", e)

        # ── Fallback: Playwright browser ──
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                offers = await self._attempt_search(search_url, req)
                if offers is not None:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)
                logger.warning(
                    "Flybondi PW: attempt %d/%d returned no results",
                    attempt, _MAX_ATTEMPTS,
                )
            except Exception as e:
                logger.warning("Flybondi PW: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    def _search_via_api(self, url: str, req: FlightSearchRequest) -> list[FlightOffer] | None:
        """Fetch SSR page via curl_cffi (TLS fingerprint) and extract Relay edges."""
        r = curl_requests.get(url, impersonate="chrome131", timeout=int(self.timeout))
        if r.status_code != 200:
            logger.warning("Flybondi API: HTTP %d", r.status_code)
            return None

        # Find the large inline <script> containing the Relay store
        scripts = re.findall(r'<script[^>]*>(.*?)</script>', r.text, re.DOTALL)
        for s in scripts:
            s = s.strip()
            if len(s) < 50000 or 'viewer' not in s:
                continue
            try:
                data = json.loads(s)
                edges = (
                    data.get("viewer", {})
                    .get("flights", {})
                    .get("edges", [])
                )
                if edges:
                    return self._parse_edges(edges, req)
            except (json.JSONDecodeError, TypeError):
                continue

        # Check for GraphQL error in SSR data
        for s in scripts:
            s = s.strip()
            if len(s) > 5000 and '"error"' in s:
                try:
                    data = json.loads(s)
                    err = data.get("error", {})
                    if err.get("graphqlError"):
                        logger.warning("Flybondi API: GraphQL error: %s",
                                       err.get("errorMessage", "")[:120])
                except (json.JSONDecodeError, TypeError):
                    pass

        return None

    async def _attempt_search(
        self, url: str, req: FlightSearchRequest
    ) -> Optional[list[FlightOffer]]:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
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

            # Set up GraphQL interception as fallback
            captured_flights: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    if response.status != 200:
                        return
                    rq = response.request
                    if rq.method != "POST" or "graphql" not in response.url.lower():
                        return
                    post = rq.post_data or ""
                    if "FlightSearchContainerQuery" not in post:
                        return
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    data = await response.json()
                    edges = (
                        data.get("data", {})
                        .get("viewer", {})
                        .get("flights", {})
                        .get("edges", [])
                    )
                    if edges:
                        captured_flights["edges"] = edges
                        api_event.set()
                        logger.info("Flybondi: captured %d flights from GraphQL", len(edges))
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("Flybondi: loading %s", url[:120])
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )

            # Dismiss cookies if present
            await self._dismiss_cookies(page)

            # Wait for page to finish rendering
            await page.wait_for_load_state("networkidle", timeout=15000)

            # Strategy 1: Extract SSR data from inline script tag
            edges = await self._extract_ssr_edges(page)
            if edges:
                logger.info("Flybondi: extracted %d flights from SSR", len(edges))
                return self._parse_edges(edges, req)

            # Strategy 2: Wait for GraphQL interception (e.g. if user interaction triggered it)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=10)
            except asyncio.TimeoutError:
                pass

            gql_edges = captured_flights.get("edges", [])
            if gql_edges:
                return self._parse_edges(gql_edges, req)

            logger.warning("Flybondi: no flight data found via SSR or GraphQL")
            return None
        finally:
            await context.close()

    async def _extract_ssr_edges(self, page) -> Optional[list[dict]]:
        """Extract flight edges from SSR data embedded in a script tag."""
        ssr_data = await page.evaluate(r"""() => {
            const scripts = document.querySelectorAll('script');
            for (const s of scripts) {
                const t = s.textContent || '';
                if (t.length > 50000 && t.includes('viewer')) {
                    try {
                        const data = JSON.parse(t);
                        if (data && data.viewer && data.viewer.flights) {
                            return data.viewer.flights.edges || null;
                        }
                    } catch(e) {}
                }
            }
            return null;
        }""")
        if ssr_data and isinstance(ssr_data, list) and len(ssr_data) > 0:
            return ssr_data
        return None

    async def _dismiss_cookies(self, page) -> None:
        try:
            btn = page.locator("button:has-text('Aceptar')")
            if await btn.count() > 0:
                await btn.first.click(timeout=3000)
                await asyncio.sleep(0.5)
        except Exception:
            pass

    def _parse_edges(self, edges: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        currency = req.currency or "ARS"
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for edge in edges:
            node = edge.get("node", {})
            if not node:
                continue
            # Only include outbound flights
            direction = node.get("direction", "OUTBOUND")
            if direction != "OUTBOUND":
                continue

            offer = self._parse_flight_node(node, currency, req, booking_url)
            if offer:
                offers.append(offer)

        return offers

    def _parse_flight_node(
        self, node: dict, currency: str, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        price = self._extract_best_price(node)
        if price is None or price <= 0:
            return None

        # Use node currency if available
        currency = node.get("currency", currency) or currency

        # Build segments from legs
        legs_raw = node.get("legs", [])
        segments: list[FlightSegment] = []
        for leg in legs_raw:
            segments.append(FlightSegment(
                airline="FO",
                airline_name="Flybondi",
                flight_no=str(leg.get("flightNo", node.get("flightNo", ""))),
                origin=leg.get("origin", node.get("origin", req.origin)),
                destination=leg.get("destination", node.get("destination", req.destination)),
                departure=self._parse_dt(leg.get("departureDate", "")),
                arrival=self._parse_dt(leg.get("arrivalDate", "")),
                cabin_class="M",
            ))

        if not segments:
            # Fallback: build segment from top-level node fields
            segments.append(FlightSegment(
                airline="FO",
                airline_name="Flybondi",
                flight_no=str(node.get("segmentFlightNo", node.get("flightNo", ""))),
                origin=node.get("origin", req.origin),
                destination=node.get("destination", req.destination),
                departure=self._parse_dt(node.get("departureDate", "")),
                arrival=self._parse_dt(node.get("arrivalDate", "")),
                cabin_class="M",
            ))

        # Total duration from node or compute from segments
        dur_min = node.get("flightTimeMinutes", 0)
        total_dur = dur_min * 60 if dur_min else 0
        if not total_dur and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=node.get("stops", max(len(segments) - 1, 0)),
        )

        flight_key = f"FO{node.get('flightNo', '')}_{node.get('departureDate', '')}_{node.get('origin', '')}"
        return FlightOffer(
            id=f"fo_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:,.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Flybondi"],
            owner_airline="FO",
            booking_url=booking_url,
            is_locked=False,
            source="flybondi_direct",
            source_tier="free",
        )

    @staticmethod
    def _extract_best_price(node: dict) -> Optional[float]:
        """Extract cheapest STANDARD fare price (afterTax) from fares list."""
        fares = node.get("fares", [])
        best = float("inf")
        for fare in fares:
            # Prefer STANDARD fares over CLUB (member-only) fares
            fare_type = fare.get("type", "")
            prices = fare.get("prices", {})
            after_tax = prices.get("afterTax")
            if after_tax is not None and fare_type == "STANDARD":
                try:
                    val = float(after_tax)
                    if 0 < val < best:
                        best = val
                except (TypeError, ValueError):
                    pass
        # If no STANDARD fare found, try any fare
        if best == float("inf"):
            for fare in fares:
                prices = fare.get("prices", {})
                after_tax = prices.get("afterTax")
                if after_tax is not None:
                    try:
                        val = float(after_tax)
                        if 0 < val < best:
                            best = val
                    except (TypeError, ValueError):
                        pass
        return best if best < float("inf") else None

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "Flybondi %s->%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        h = hashlib.md5(
            f"flybondi{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "ARS"),
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
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1
        children = getattr(req, "children", 0) or 0
        infants = getattr(req, "infants", 0) or 0
        return (
            f"https://flybondi.com/ar/search/results"
            f"?departureDate={dep}"
            f"&adults={adults}&children={children}&infants={infants}"
            f"&currency={req.currency or 'ARS'}"
            f"&fromCityCode={req.origin}&toCityCode={req.destination}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"flybondi{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "ARS",
            offers=[],
            total_results=0,
        )
