"""
Lucky Air browser-based scraper — Playwright form fill + response interception.

Lucky Air (IATA: 8L) is a Chinese LCC headquartered in Kunming, Yunnan.
Hub: KMG (Kunming Changshui). Domestic network: 40+ Chinese cities.
Some international routes (Bangkok, Phuket, etc).

Website: www.luckyair.net (Chinese, micro-app SPA on Vue/Ant Design).
NOT available in GDS — must be scraped directly.

Strategy (browser-based, validated Mar 2026):
  1. Launch Playwright headed browser
  2. Navigate to https://www.luckyair.net/micro/main/flight/search
  3. Fill destination city → UI auto-calls calendar API
  4. Intercept POST /api/flight/query/price/calendar/fix response
  5. Parse cheapest daily prices → FlightOffers

The calendar API is called automatically by the Vue app when a destination
city is selected in the form. Calling it manually via fetch() returns 500.
We must trigger it via UI interaction and capture it via response interception.

Calendar API response:
  {"status": "success", "code": "200",
   "data": [{"date": "2026-03-13", "price": "1010", "currency": "CNY"}, ...]}

Discovered via Playwright interception probes, Mar 2026.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from datetime import datetime
from typing import Optional

from boostedtravel.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
logger = logging.getLogger(__name__)

# ── Module-level browser state (cleaned up by engine.py) ───────────────────
_browser = None
_pw_instance = None

_BASE_URL = "https://www.luckyair.net"
_SEARCH_URL = f"{_BASE_URL}/micro/main/flight/search"

_VIEWPORTS = [
    {"width": 1280, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
]


async def _get_browser():
    """Get or create the shared Playwright browser."""
    global _browser, _pw_instance
    if _browser and _browser.is_connected():
        return _browser

    from playwright.async_api import async_playwright
    from boostedtravel.connectors.browser import stealth_args
    _pw_instance = await async_playwright().start()
    _args = [*stealth_args(), "--lang=zh-CN"]
    try:
        _browser = await _pw_instance.chromium.launch(
            headless=True,
            channel="chrome",
            args=_args,
        )
    except Exception:
        _browser = await _pw_instance.chromium.launch(
            headless=True,
            args=_args,
        )
    logger.info("Lucky Air: browser launched")
    return _browser


class LuckyAirConnectorClient:
    """Lucky Air scraper — browser session + calendar API."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout

    async def close(self):
        pass  # Cleaned up by engine.py via module globals

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        try:
            browser = await _get_browser()
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                locale="zh-CN",
                viewport=random.choice(_VIEWPORTS),
            )

            try:
                page = await context.new_page()
                offers = await self._search_via_calendar(page, req, t0)

                elapsed = time.monotonic() - t0
                if offers:
                    offers.sort(key=lambda o: o.price)
                logger.info(
                    "Lucky Air %s→%s returned %d offers in %.1fs",
                    req.origin, req.destination, len(offers), elapsed,
                )

                search_hash = hashlib.md5(
                    f"luckyair{req.origin}{req.destination}{req.date_from}".encode()
                ).hexdigest()[:12]
                return FlightSearchResponse(
                    search_id=f"fs_{search_hash}",
                    origin=req.origin,
                    destination=req.destination,
                    currency=req.currency or "CNY",
                    offers=offers,
                    total_results=len(offers),
                )
            finally:
                await context.close()

        except Exception as exc:
            logger.error("Lucky Air error: %s", exc)
            return self._empty(req)

    async def _search_via_calendar(
        self, page, req: FlightSearchRequest, t0: float,
    ) -> list[FlightOffer]:
        """Fill form to trigger calendar API, intercept the response."""

        remaining = lambda: max(self.timeout - (time.monotonic() - t0), 1)

        captured_data: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                url = response.url
                if response.status == 200 and any(k in url for k in (
                    "price/calendar", "calendar/fix",
                    "searchflight", "availability",
                )):
                    ct = response.headers.get("content-type", "")
                    if "json" in ct:
                        data = await response.json()
                        if data and isinstance(data, dict):
                            # Must have a list "data" field (calendar prices)
                            inner = data.get("data")
                            if isinstance(inner, list) and inner:
                                captured_data["calendar"] = data
                                api_event.set()
            except Exception:
                pass

        page.on("response", on_response)

        # Step 1: Navigate to search page
        logger.info("Lucky Air: loading search page for %s→%s", req.origin, req.destination)
        try:
            await page.goto(
                _SEARCH_URL,
                wait_until="domcontentloaded",
                timeout=int(min(remaining(), 15) * 1000),
            )
        except Exception as exc:
            logger.warning("Lucky Air: failed to load search page: %s", exc)
            return []

        # Wait for the Vue SPA to mount and render the search form
        try:
            await page.wait_for_selector(
                "input[placeholder='出发城市'], input[placeholder='到达城市']",
                timeout=10000,
            )
        except Exception:
            logger.debug("Lucky Air: form inputs not found within 10s")
        await asyncio.sleep(1.0)

        # Step 2: Change departure city if not KMG (default)
        if req.origin != "KMG":
            await self._fill_city(page, "出发城市", req.origin)

        # Step 3: Fill destination city
        logger.info("Lucky Air: selecting destination %s", req.destination)
        await self._fill_city(page, "到达城市", req.destination)

        # Step 4: Click the date input to open the calendar picker
        # The calendar API fires when the date picker is opened/navigated,
        # NOT when cities are selected.
        try:
            date_input = page.locator(
                "input[placeholder='出发日期'], input[placeholder*='日期'], "
                ".ant-calendar-picker input, .ant-picker-input input"
            ).first
            await date_input.click(timeout=3000)
            await asyncio.sleep(1.5)
        except Exception:
            logger.debug("Lucky Air: could not open date picker")

        # Step 5: Navigate to target month — triggers calendar API
        entries = await self._navigate_calendar_to_month(
            page, captured_data, api_event, req, remaining,
        )

        return self._parse_calendar(entries, req)

    async def _navigate_calendar_to_month(
        self, page, captured_data, api_event, req, remaining,
    ) -> list[dict]:
        """Navigate the Ant Design calendar to the target month.

        Each month navigation triggers a calendar API call.
        Returns the entries for the best available month.
        """
        target_month = req.date_from.replace(day=1)
        next_btn_sel = (
            ".ant-calendar-next-month-btn, .ant-picker-header-next-btn, "
            "button[class*=next-month], a[class*=next-month], "
            "[class*=calendar] [class*=next]:not([class*=year])"
        )
        best_entries: list[dict] = []

        # Try navigating up to 12 months forward from current calendar position
        for i in range(13):
            # Wait for any calendar data that might have been triggered
            if not api_event.is_set():
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=min(remaining(), 4))
                except asyncio.TimeoutError:
                    pass

            data = captured_data.get("calendar", {})
            entries = data.get("data", []) if isinstance(data, dict) else []
            if not isinstance(entries, list):
                entries = []

            if entries:
                best_entries = entries
                # Check if we've reached the target month
                dated = [e.get("date", "") for e in entries if isinstance(e, dict) and e.get("price")]
                if dated:
                    try:
                        entry_month = datetime.strptime(min(dated), "%Y-%m-%d").date().replace(day=1)
                        if entry_month >= target_month:
                            logger.info("Lucky Air: reached target month %s", entry_month.strftime("%Y-%m"))
                            return entries
                    except ValueError:
                        pass

            # Break if overall timeout exhausted
            if remaining() <= 1:
                logger.debug("Lucky Air: timeout reached at month step %d", i + 1)
                break

            # Click next month
            api_event.clear()
            try:
                btn = page.locator(next_btn_sel).first
                if await btn.count() == 0:
                    break
                await btn.click(timeout=2000)
                await asyncio.sleep(0.5)
            except Exception:
                logger.debug("Lucky Air: next-month click failed at step %d", i + 1)
                break

        if not best_entries:
            logger.warning("Lucky Air: no calendar data found after navigation")
        return best_entries

    async def _fill_city(self, page, placeholder: str, code: str) -> bool:
        """Fill a city input field using the Ant Design Select dropdown."""

        # Strategy 1: exact placeholder match
        matched_input = None
        for sel in (
            f"input[placeholder='{placeholder}']",
            f"input[placeholder*='{placeholder[:2]}']",  # partial match (first 2 chars)
        ):
            try:
                inp = page.locator(sel).first
                if await inp.count() > 0:
                    matched_input = inp
                    break
            except Exception:
                continue

        # Strategy 2: positional — departure inputs are typically first, arrival second
        if not matched_input:
            try:
                is_departure = "出发" in placeholder or "departure" in placeholder.lower()
                idx = 0 if is_departure else 1
                inputs = page.locator(".ant-select input, [class*=city] input, [class*=search] input")
                if await inputs.count() > idx:
                    matched_input = inputs.nth(idx)
            except Exception:
                pass

        # Strategy 3: any visible text input
        if not matched_input:
            try:
                inputs = page.locator("input[type='text'], input:not([type])")
                is_departure = "出发" in placeholder or "departure" in placeholder.lower()
                idx = 0 if is_departure else 1
                if await inputs.count() > idx:
                    matched_input = inputs.nth(idx)
            except Exception:
                pass

        if not matched_input:
            logger.warning("Lucky Air: no input found for '%s'", placeholder)
            return False

        try:
            await matched_input.click(timeout=5000)
            await asyncio.sleep(0.5)
            await matched_input.fill("")
            await asyncio.sleep(0.3)
            await matched_input.type(code, delay=80)
            await asyncio.sleep(2.0)

            # Click the first suggestion in the dropdown
            for sel in (
                ".ant-select-dropdown-menu-item",
                "[class*=dropdown] li", "[class*=option]",
                "[role=option]", "[role=listbox] li",
                "[class*=city-item]", "[class*=suggest] li",
            ):
                try:
                    items = page.locator(sel)
                    if await items.count() > 0 and await items.first.is_visible():
                        await items.first.click()
                        await asyncio.sleep(1.0)
                        return True
                except Exception:
                    continue

            # Fallback: press Enter
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.5)
            return True
        except Exception as exc:
            logger.debug("Lucky Air: failed to fill %s: %s", placeholder, exc)
            return False

    def _parse_calendar(
        self, entries: list[dict], req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Convert calendar price entries into FlightOffers."""
        offers: list[FlightOffer] = []
        currency = "CNY"
        booking_base = self._booking_url(req)

        priced = [e for e in entries if e.get("price")]
        logger.info("Lucky Air: calendar returned %d priced days", len(priced))

        # First pass: collect all valid priced entries
        valid_entries: list[tuple] = []  # (date, price, date_str)
        for entry in entries:
            price_str = entry.get("price")
            if not price_str:
                continue
            try:
                price = float(price_str)
            except (ValueError, TypeError):
                continue
            if price <= 0:
                continue
            flight_date_str = entry.get("date", "")
            if not flight_date_str:
                continue
            try:
                flight_date = datetime.strptime(flight_date_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            valid_entries.append((flight_date, price, flight_date_str))

        if not valid_entries:
            return []

        # Filter to requested date(s)
        if req.date_to:
            # Date range: include all in range
            filtered = [
                (d, p, s) for d, p, s in valid_entries
                if req.date_from <= d <= req.date_to
            ]
        else:
            # Single date: exact match
            filtered = [
                (d, p, s) for d, p, s in valid_entries if d == req.date_from
            ]

        # Fallback: if no exact match, use closest date to prove route exists
        if not filtered:
            closest = min(
                valid_entries,
                key=lambda e: abs((e[0] - req.date_from).days),
            )
            filtered = [closest]
            logger.info(
                "Lucky Air: no exact date match for %s, using closest %s",
                req.date_from, closest[2],
            )

        for flight_date, price, flight_date_str in filtered:
            fid = hashlib.md5(
                f"8l_{req.origin}{req.destination}{flight_date_str}{price}".encode()
            ).hexdigest()[:12]

            # Build a minimal segment — calendar gives price only, not times
            dep_dt = datetime(flight_date.year, flight_date.month, flight_date.day, 0, 0)
            segment = FlightSegment(
                airline="8L",
                airline_name="Lucky Air",
                flight_no="8L",
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=dep_dt,
                duration_seconds=0,
                cabin_class="economy",
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=0,
                stopovers=0,
            )

            offers.append(FlightOffer(
                id=f"8l_{fid}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{price:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Lucky Air"],
                owner_airline="8L",
                booking_url=booking_base,
                is_locked=False,
                source="luckyair_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.luckyair.net/micro/main/flight/search"
            f"?depCode={req.origin}&arrCode={req.destination}&flightDate={dep}"
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"luckyair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "CNY",
            offers=[],
            total_results=0,
        )
