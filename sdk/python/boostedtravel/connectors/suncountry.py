"""
Sun Country Airlines direct connector — Playwright lowfare API.

Sun Country Airlines (IATA: SY) is a US low-cost carrier based in Minneapolis/
St. Paul (MSP). Operates 90+ routes across the US, Mexico, Caribbean, and
Central America.

Strategy:
  1. Launch Playwright browser → load suncountry.com (gets Incapsula WAF cookies
     and Navitaire JWT from sessionStorage).
  2. Use in-browser fetch() to call the lowfare/outbound API, which returns
     the cheapest fare per day with availability counts.
  3. Build one FlightOffer per requested date.

Note: The Navitaire availability/search endpoint (ext/v1/availability/search)
is unreachable — it times out from both browser and direct httpx, confirmed
2026-03 testing. The lowfare endpoint is the only working pricing source.
Direct httpx calls to the API are blocked by Incapsula WAF (403).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]

_SUB_KEY = os.environ.get("SUNCOUNTRY_SUB_KEY", "")

_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    global _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright
        pw = await async_playwright().start()
        launch_kw: dict = {
            "headless": False,
            "channel": "chrome",
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                "--window-size=1366,768",
            ],
        }
        try:
            _browser = await pw.chromium.launch(**launch_kw)
        except Exception:
            launch_kw.pop("channel", None)
            _browser = await pw.chromium.launch(**launch_kw)
        logger.info("SunCountry: headed Chrome launched")
        return _browser


class SunCountryConnectorClient:
    """Sun Country Airlines connector — Playwright + lowfare API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            offers = await self._search_via_browser(req)
            elapsed = time.monotonic() - t0
            logger.info(
                "SunCountry: %s→%s on %s — %d offers in %.1fs",
                req.origin, req.destination, req.date_from, len(offers), elapsed,
            )
            return FlightSearchResponse(
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=offers,
                total_results=len(offers),
                search_id=f"suncountry_{req.origin}_{req.destination}_{req.date_from}",
            )
        except Exception as e:
            logger.error("SunCountry search error: %s", e)
            return self._empty(req)

    async def _search_via_browser(self, req: FlightSearchRequest) -> list[FlightOffer]:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
            timezone_id="America/Chicago",
            service_workers="block",
        )
        try:
            page = await context.new_page()
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            token_holder: dict = {}
            token_event = asyncio.Event()

            async def on_response(response):
                try:
                    if "/nsk/v1/token" in response.url and response.status in (200, 201):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            tok = (data.get("data") or data).get("token", "")
                            if tok and not token_holder.get("token"):
                                token_holder["token"] = tok
                                token_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            # Step 1: Load homepage to get WAF cookies and Navitaire token
            logger.info("SunCountry: loading homepage for WAF cookies")
            await page.goto(
                "https://www.suncountry.com/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )

            # Wait for the SPA to obtain the Navitaire JWT
            try:
                await asyncio.wait_for(token_event.wait(), timeout=15)
            except asyncio.TimeoutError:
                pass
            token = token_holder.get("token")
            if not token:
                logger.warning("SunCountry: no Navitaire token found")
                return []

            # Step 2: Call lowfare/outbound via in-browser fetch
            # Request a ±3 day window centred on the target date
            start_dt = req.date_from - timedelta(days=3)
            end_dt = req.date_from + timedelta(days=3)
            body = {
                "request": {
                    "origin": req.origin,
                    "destination": req.destination,
                    "currencyCode": "USD",
                    "includeTaxesAndFees": True,
                    "isRoundTrip": False,
                    "numberOfPassengers": req.adults,
                    "startDate": start_dt.strftime("%m/%d/%Y"),
                    "endDate": end_dt.strftime("%m/%d/%Y"),
                }
            }

            result = await page.evaluate(
                """async ([token, subKey, body]) => {
                    try {
                        const resp = await fetch(
                            'https://syprod-api.suncountry.com/ext/v1/lowfare/outbound',
                            {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                    'Ocp-Apim-Subscription-Key': subKey,
                                    'Authorization': token
                                },
                                body: JSON.stringify(body)
                            }
                        );
                        if (!resp.ok) return { error: resp.status };
                        return await resp.json();
                    } catch (e) {
                        return { error: e.message };
                    }
                }""",
                [token, _SUB_KEY, body],
            )

            if not result or result.get("error"):
                logger.warning("SunCountry: lowfare error: %s", result)
                return []

            return self._parse_lowfare(result, req)
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_lowfare(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        fares = data.get("lowfares") or []
        target = req.date_from.strftime("%Y-%m-%d")
        booking_url = "https://www.suncountry.com/booking/select"
        offers: list[FlightOffer] = []

        for fare in fares:
            fare_date = (fare.get("date") or "")[:10]  # "2026-04-13T00:00:00" → "2026-04-13"
            if fare_date != target:
                continue
            if fare.get("noFlights") or fare.get("soldOut"):
                continue
            price = fare.get("price")
            if not price or price <= 0:
                continue

            seats = fare.get("available")
            dep_dt = datetime.fromisoformat(fare["date"])

            seg = FlightSegment(
                airline="SY",
                airline_name="Sun Country Airlines",
                flight_no="SY",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class="economy",
            )
            route = FlightRoute(segments=[seg])

            key = f"{req.origin}{req.destination}{fare_date}{price}"
            offer_id = f"sy_{hashlib.md5(key.encode()).hexdigest()[:12]}"

            offers.append(FlightOffer(
                id=offer_id,
                price=round(price, 2),
                currency="USD",
                price_formatted=f"${price:.2f}",
                outbound=route,
                inbound=None,
                airlines=["Sun Country Airlines"],
                owner_airline="SY",
                booking_url=booking_url,
                is_locked=False,
                source="suncountry_direct",
                source_tier="free",
                availability_seats=seats if isinstance(seats, int) else None,
            ))

        return offers

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
            search_id=f"suncountry_{req.origin}_{req.destination}_{req.date_from}",
        )
