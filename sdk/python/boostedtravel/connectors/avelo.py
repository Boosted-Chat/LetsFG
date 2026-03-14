"""
Avelo Airlines direct connector — deep link URL + Playwright DOM scraping.

Avelo (IATA: XP) is a US ultra-low-cost carrier with ~42 routes across the
US, operating from bases at New Haven (HVN), Hollywood/Burbank (BUR), and
other cities to leisure destinations.

Strategy:
  1. Navigate to deep link URL which triggers server-side flight search:
     /flight-search/deeplink/searchflights/oneway/{origin}/{dest}/{date}/{adults}/{children}/{infants_seat}/{infants_lap}/?calendar=false
  2. Wait for Blazor WASM app to render results.
  3. Parse flight cards from the DOM (times, prices, duration, stops).

Avelo's booking engine is a Blazor WebAssembly app that uses gRPC and
session-scoped APIs. Direct API access requires session management,
so Playwright DOM scraping is the simplest reliable approach.

The route data API is public (no auth):
  GET https://api.aveloair.com/proxies/proxy-refdata/v1.0/read/location/route/all
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta
from typing import Any, Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_DEEPLINK_TPL = (
    "https://www.aveloair.com/flight-search/deeplink/searchflights/oneway"
    "/{origin}/{dest}/{date}/{adults}/{children}/0/0/?calendar=false"
)
_BOOKING_URL_TPL = (
    "https://www.aveloair.com/flight-search/deeplink/searchflights/oneway"
    "/{origin}/{dest}/{date}/{adults}/0/0/0/?calendar=false"
)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]

# Shared browser singleton
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_proxy_url() -> str:
    return os.environ.get("AVELO_PROXY", "").strip()


def _get_pw_proxy() -> Optional[dict]:
    raw = _get_proxy_url()
    if not raw:
        return None
    from urllib.parse import urlparse
    p = urlparse(raw)
    result: dict[str, str] = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username:
        result["username"] = p.username
    if p.password:
        result["password"] = p.password
    return result


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
        proxy = _get_pw_proxy()
        if proxy:
            launch_kw["proxy"] = proxy
        try:
            _browser = await pw.chromium.launch(**launch_kw)
        except Exception:
            launch_kw.pop("channel", None)
            _browser = await pw.chromium.launch(**launch_kw)
        logger.info("Avelo: headed Chrome launched (proxy=%s)", bool(proxy))
        return _browser


def _parse_duration(text: str) -> int:
    """Parse '2h 55m nonstop' or '4h 10m 1 stop' to seconds."""
    m = re.search(r'(\d+)h\s*(\d+)m', text)
    if m:
        return int(m.group(1)) * 3600 + int(m.group(2)) * 60
    m = re.search(r'(\d+)h', text)
    if m:
        return int(m.group(1)) * 3600
    m = re.search(r'(\d+)m', text)
    if m:
        return int(m.group(1)) * 60
    return 0


def _parse_stops(text: str) -> int:
    """Parse 'nonstop' or '1 stop' from duration text."""
    if "nonstop" in text.lower():
        return 0
    m = re.search(r'(\d+)\s*stop', text.lower())
    return int(m.group(1)) if m else 0


def _parse_time(time_str: str, date_str: str) -> Optional[datetime]:
    """Parse '8:30 am' or '8:30 AM' + '2026-04-04' to datetime."""
    try:
        return datetime.strptime(f"{date_str} {time_str.strip().upper()}", "%Y-%m-%d %I:%M %p")
    except ValueError:
        return None


def _parse_price(text: str) -> Optional[float]:
    """Extract price from '$175' or '$1,234'."""
    m = re.search(r'\$([0-9,]+(?:\.\d{2})?)', text)
    if m:
        return float(m.group(1).replace(",", ""))
    return None


class AveloConnectorClient:
    """Avelo Airlines connector — deep link URL + Playwright DOM scraping."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            offers = await self._search_via_playwright(req)
            return FlightSearchResponse(
                search_id=f"avelo_{req.origin}_{req.destination}_{req.date_from.strftime('%Y%m%d')}",
                origin=req.origin,
                destination=req.destination,
                currency="USD",
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.error("Avelo search error: %s", e)
            return self._empty(req)

    async def _search_via_playwright(self, req: FlightSearchRequest) -> list[FlightOffer]:
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="en-US",
            service_workers="block",
        )
        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            date_str = req.date_from.strftime("%Y-%m-%d")
            url = _DEEPLINK_TPL.format(
                origin=req.origin,
                dest=req.destination,
                date=date_str,
                adults=req.adults,
                children=0,
            )

            logger.info("Avelo: navigating to %s", url)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)

            # Wait for the results page (Blazor WASM app loads)
            try:
                await page.wait_for_url(
                    "**/flight-search/select**",
                    timeout=25000,
                )
            except Exception:
                logger.warning("Avelo: did not redirect to select page")
                return []

            # Wait for flight cards to appear — Blazor renders div[role=button]
            # elements with class flight-block-wrapper-v2
            try:
                await page.wait_for_selector(
                    '.flight-block-wrapper-v2',
                    timeout=20000,
                )
            except Exception:
                logger.warning("Avelo: no flight cards found")
                return []

            # Small extra wait for Blazor rendering
            await asyncio.sleep(1.5)

            # Extract flight data from DOM
            return await self._extract_flights(page, req, date_str)

        finally:
            await context.close()

    async def _extract_flights(
        self, page, req: FlightSearchRequest, date_str: str,
    ) -> list[FlightOffer]:
        """Extract flight offers from Blazor-rendered DOM."""
        booking_url = _BOOKING_URL_TPL.format(
            origin=req.origin,
            dest=req.destination,
            date=date_str,
            adults=req.adults,
        )

        raw_flights = await page.evaluate("""() => {
            const cards = document.querySelectorAll('.flight-block-wrapper-v2');
            if (cards.length === 0) return {flights: []};

            const flights = [];
            for (const card of cards) {
                const text = card.innerText || card.textContent || '';
                const lines = text.split(/\\r?\\n|\\r/).map(l => l.trim()).filter(Boolean);
                if (lines.length < 3) continue;

                const times = lines.filter(l => /^\\d{1,2}:\\d{2}\\s*[ap]m$/i.test(l));
                // Airport codes may be standalone "HVN" or in "New Haven, CT (HVN)"
                const codeLines = [];
                for (const l of lines) {
                    const m3 = l.match(/^([A-Z]{3})$/);
                    if (m3) { codeLines.push(m3[1]); continue; }
                    const m4 = l.match(/\\(([A-Z]{3})\\)/);
                    if (m4) codeLines.push(m4[1]);
                }
                const durLine = lines.find(l => /\\d+h\\s*\\d+m/i.test(l));
                const prices = lines.filter(l => /^\\$\\d/.test(l))
                    .map(l => parseFloat(l.replace('$', '').replace(',', '')));

                if (times.length < 2 || codeLines.length < 2 || !durLine || prices.length === 0) continue;

                flights.push({
                    departure_time: times[0],
                    arrival_time: times[1],
                    origin: codeLines[0],
                    destination: codeLines[1],
                    duration_text: durLine,
                    prices: prices,
                });
            }
            return {flights: flights};
        }""")

        if isinstance(raw_flights, dict):
            raw_flights = raw_flights.get("flights", [])

        offers: list[FlightOffer] = []
        for flight in raw_flights:
            dep_time = _parse_time(flight["departure_time"], date_str)
            arr_time = _parse_time(flight["arrival_time"], date_str)
            duration_secs = _parse_duration(flight["duration_text"])
            stops = _parse_stops(flight["duration_text"])

            # Use the "Standard" fare price (last/highest), which is the base fare
            # Avelo shows "Avelo PLUS" (member) and "Standard" (regular)
            prices = flight.get("prices", [])
            if not prices:
                continue

            # Standard price is typically the higher one (non-member)
            price = max(prices) if prices else 0
            if price <= 0:
                continue

            if not dep_time:
                dep_time = datetime.strptime(date_str, "%Y-%m-%d")
            if not arr_time:
                arr_time = dep_time + timedelta(seconds=duration_secs) if duration_secs else dep_time

            seg = FlightSegment(
                airline="XP",
                airline_name="Avelo Airlines",
                flight_no="",
                origin=flight.get("origin", req.origin),
                destination=flight.get("destination", req.destination),
                departure=dep_time,
                arrival=arr_time,
                cabin_class="M",
            )
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=duration_secs,
                stopovers=stops,
            )
            dep_key = flight["departure_time"]
            offer_id = f"xp_{hashlib.md5(f'{req.origin}{req.destination}{date_str}{price}{dep_key}'.encode()).hexdigest()[:12]}"
            offers.append(FlightOffer(
                id=offer_id,
                price=round(price, 2),
                currency="USD",
                price_formatted=f"${price:.2f}",
                outbound=route,
                inbound=None,
                airlines=["Avelo"],
                owner_airline="XP",
                booking_url=booking_url,
                is_locked=False,
                source="avelo_direct",
                source_tier="free",
                availability_seats=None,
            ))

        logger.info("Avelo: extracted %d offers for %s->%s", len(offers), req.origin, req.destination)
        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            search_id=f"avelo_{req.origin}_{req.destination}_{req.date_from.strftime('%Y%m%d')}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
