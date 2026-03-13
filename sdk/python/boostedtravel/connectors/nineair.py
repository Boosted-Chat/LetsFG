"""
9 Air browser-based scraper — Playwright form fill + response interception.

9 Air (IATA: AQ) is a Chinese LCC headquartered in Guangzhou, Guangdong.
Hub: CAN (Guangzhou Baiyun). Domestic network: ~30 Chinese cities.
Website: www.9air.com (Chinese, Next.js SPA).

NOT available in GDS — must be scraped directly.
Direct API calls fail with anti-spider verification ("检验失败").
Must use browser-based approach: fill form → click search → intercept.

Strategy (browser-based, validated Mar 2026):
  1. Launch Playwright headed browser
  2. Navigate to https://www.9air.com/zh-CN (homepage with search form)
  3. Fill the search form (departure, arrival, date)
  4. Click search → page navigates to results
  5. Intercept /shop/api/shopping/b2c/searchflight response
  6. Parse flight results → FlightOffers

Search API endpoint (captured via interception):
  POST /shop/api/shopping/b2c/searchflight?language=zh_CN&currency=CNY

City dictionary (static, works directly):
  GET /frontendfile/cityDict.js

Discovered via Playwright probes, Mar 2026.
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

_BASE_URL = "https://www.9air.com"
_SEARCH_PAGE = f"{_BASE_URL}/zh-CN"

_VIEWPORTS = [
    {"width": 1280, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
]

# IATA code → Chinese city name (for 9 Air's city grid picker)
_IATA_CN: dict[str, str] = {
    "PEK": "北京", "PKX": "北京", "NAY": "北京",
    "SHA": "上海", "PVG": "上海",
    "CAN": "广州", "SZX": "深圳", "CTU": "成都", "TFU": "成都",
    "CKG": "重庆", "WUH": "武汉", "HGH": "杭州", "NKG": "南京",
    "KMG": "昆明", "XIY": "西安", "DLC": "大连", "HAK": "海口",
    "CGO": "郑州", "URC": "乌鲁木齐", "HET": "呼和浩特",
    "LHW": "兰州", "CGQ": "长春", "WUX": "无锡", "WNZ": "温州",
    "TNA": "济南", "SYX": "三亚", "TAO": "青岛", "CSX": "长沙",
    "HRB": "哈尔滨", "KWE": "贵阳", "FOC": "福州", "XMN": "厦门",
    "NNG": "南宁", "HFE": "合肥", "TSN": "天津", "SHE": "沈阳",
    "SJW": "石家庄", "TYN": "太原", "LPF": "六盘水", "HSN": "舟山",
    "BKK": "曼谷", "HAN": "河内", "KUL": "吉隆坡", "KIX": "大阪",
    "VTE": "万象",
}


async def _get_browser():
    """Get or create the shared Playwright browser."""
    global _browser, _pw_instance
    if _browser and _browser.is_connected():
        return _browser

    from playwright.async_api import async_playwright
    from boostedtravel.connectors.browser import stealth_args
    _pw_instance = await async_playwright().start()
    _args = [*stealth_args(), "--lang=zh-CN", "--disable-blink-features=AutomationControlled"]
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
    logger.info("9 Air: browser launched")
    return _browser


class NineAirConnectorClient:
    """9 Air scraper — browser form fill + API response interception."""

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
                offers = await self._search_with_interception(page, req, t0)

                elapsed = time.monotonic() - t0
                if offers:
                    offers.sort(key=lambda o: o.price)
                logger.info(
                    "9 Air %s→%s returned %d offers in %.1fs",
                    req.origin, req.destination, len(offers), elapsed,
                )

                search_hash = hashlib.md5(
                    f"9air{req.origin}{req.destination}{req.date_from}".encode()
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
            logger.error("9 Air error: %s", exc)
            return self._empty(req)

    async def _search_with_interception(
        self, page, req: FlightSearchRequest, t0: float,
    ) -> list[FlightOffer]:
        """Navigate to search page, fill form, intercept API response."""

        remaining = lambda: max(self.timeout - (time.monotonic() - t0), 1)

        captured_data: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                url = response.url.lower()
                if response.status == 200 and any(k in url for k in (
                    "searchflight", "calendarshopping", "availability",
                    "flightsearch", "search/flights", "shopping/b2c",
                )):
                    ct = response.headers.get("content-type", "")
                    if "json" in ct or "javascript" in ct:
                        data = await response.json()
                        if data and isinstance(data, dict):
                            # Look for successful flight data
                            if data.get("status") not in ("500",) and "data" in data:
                                captured_data["json"] = data
                                api_event.set()
            except Exception:
                pass

        page.on("response", on_response)

        # Step 1: Load homepage
        logger.info("9 Air: loading homepage for %s→%s", req.origin, req.destination)
        try:
            await page.goto(
                _SEARCH_PAGE,
                wait_until="domcontentloaded",
                timeout=int(min(remaining(), 15) * 1000),
            )
        except Exception as exc:
            logger.warning("9 Air: failed to load page: %s", exc)
            return []

        await asyncio.sleep(3.0)

        # Dismiss cookie/privacy banners
        await self._dismiss_popups(page)

        # Step 2: Select one-way
        try:
            await page.click("text=单程", timeout=3000)
            await asyncio.sleep(0.5)
        except Exception:
            pass

        # Step 3: Fill departure city
        ok = await self._fill_city(page, "departure", req.origin)
        if not ok:
            logger.warning("9 Air: could not fill departure city %s", req.origin)
            return []

        # Step 4: Fill arrival city
        ok = await self._fill_city(page, "arrival", req.destination)
        if not ok:
            logger.warning("9 Air: could not fill arrival city %s", req.destination)
            return []

        # Step 5: Select date
        await self._select_date(page, req.date_from)

        # Step 6: Click search
        await self._click_search(page)

        # Step 7: Wait for API response
        try:
            await asyncio.wait_for(api_event.wait(), timeout=remaining())
        except asyncio.TimeoutError:
            logger.warning("9 Air: timed out waiting for search results")
            # Try DOM extraction as fallback
            return await self._extract_from_dom(page, req)

        data = captured_data.get("json", {})
        if not data:
            return []

        return self._parse_response(data, req)

    async def _fill_city(self, page, field_type: str, code: str) -> bool:
        """Fill a departure or arrival city field.

        Departure: type IATA in .cityInput → click .panel-search-body suggestion.
        Arrival:   Escape city grid → type Chinese name → click suggestion.
        """
        idx = 0 if field_type == "departure" else 1
        cn_name = _IATA_CN.get(code)

        # Strategy 1: type in .cityInput → use search suggestions
        try:
            city_inputs = page.locator(".cityInput:visible")
            if await city_inputs.count() > idx:
                inp = city_inputs.nth(idx)
                await inp.click(timeout=5000)
                await asyncio.sleep(0.5)
                # Close popup if arrival grid is covering things
                if field_type == "arrival":
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.3)
                    await inp.click(timeout=3000)
                    await asyncio.sleep(0.3)
                # Try Chinese name first for arrival (IATA doesn't show suggestions)
                terms = [cn_name, code] if (field_type == "arrival" and cn_name) else [code, cn_name] if cn_name else [code]
                for term in terms:
                    if not term:
                        continue
                    await inp.fill(term)
                    await asyncio.sleep(2.0)
                    if await self._click_city_suggestion(page):
                        return True
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.5)
                return True
        except Exception:
            pass

        return False

    async def _click_city_suggestion(self, page) -> bool:
        """Click the first visible city suggestion in a dropdown."""
        for sel in (".panel-search-body:visible li",
                    ".city-item", "[class*=city-option]",
                    "[class*=city-content-list]:visible li",
                    "[class*=dropdown] li", "[class*=suggest] li",
                    ".ant-select-dropdown-menu-item",
                    "[class*=option]", "[class*=list] li",
                    "[role=option]", "[role=listbox] li"):
            try:
                item = page.locator(sel).first
                if await item.count() > 0 and await item.is_visible():
                    await item.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return True
            except Exception:
                continue
        return False

    async def _select_date(self, page, target_date) -> None:
        """Select the departure date using 9 Air's custom calendar.

        The calendar uses input.dateInput and a .date-panel with two months.
        Clicking the date input opens the panel. Day cells are clickable divs.
        """
        try:
            # Click the date input to open the calendar panel
            date_inp = page.locator("input.dateInput, .cell.deptdate, .fly-date-date").first
            if await date_inp.count() > 0:
                await date_inp.click(timeout=3000)
                await asyncio.sleep(0.8)

            day = target_date.day
            target_month_label = f"{target_date.year}年{target_date.month}月"

            # The calendar shows two months in .date-main panels with .date-head headers.
            # Find the panel whose header matches our target month.
            clicked = await page.evaluate("""(args) => {
                const [day, monthLabel] = args;
                const panels = document.querySelectorAll('.date-main');
                for (const panel of panels) {
                    const head = panel.querySelector('.date-head');
                    if (head && head.textContent.trim().includes(monthLabel)) {
                        // Find the day cell that matches (not disabled, not from other month)
                        const cells = panel.querySelectorAll('.date-cell:not(.disabled):not(.other-month)');
                        for (const cell of cells) {
                            const txt = cell.textContent.trim().split('\\n')[0].trim();
                            if (txt === String(day)) {
                                cell.click();
                                return true;
                            }
                        }
                        // Fallback: try all child elements with the day number
                        const all = panel.querySelectorAll('div, td, span');
                        for (const el of all) {
                            if (el.children.length === 0 && el.textContent.trim() === String(day)) {
                                el.click();
                                return true;
                            }
                        }
                    }
                }
                return false;
            }""", [day, target_month_label])

            if clicked:
                await asyncio.sleep(0.5)
                logger.debug("9 Air: selected date %s", target_date)
            else:
                # Fallback: just set the date value directly
                await page.evaluate(
                    """(dateStr) => {
                        const inp = document.querySelector('input.dateInput');
                        if (inp) {
                            const nativeSet = Object.getOwnPropertyDescriptor(
                                HTMLInputElement.prototype, 'value').set;
                            nativeSet.call(inp, dateStr);
                            inp.dispatchEvent(new Event('input', {bubbles: true}));
                            inp.dispatchEvent(new Event('change', {bubbles: true}));
                        }
                    }""",
                    target_date.strftime("%Y-%m-%d"),
                )
                await asyncio.sleep(0.3)
                logger.debug("9 Air: set date via JS fallback %s", target_date)
        except Exception as exc:
            logger.debug("9 Air: date selection failed: %s", exc)

    async def _click_search(self, page) -> None:
        """Click the search button."""
        for sel in ("button:has-text('查询')", "button.flyway-btn",
                    "button:has-text('搜索')", "button:has-text('Search')",
                    "button[type='submit']", "[class*=search-btn]"):
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click(timeout=3000)
                    await asyncio.sleep(1.0)
                    return
            except Exception:
                continue

    async def _dismiss_popups(self, page) -> None:
        """Dismiss cookie banners and popups."""
        for sel in ("text=接受", "text=同意", "text=Accept", "text=OK",
                    "[class*=cookie] button", "[class*=modal] [class*=close]"):
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click(timeout=1500)
                    await asyncio.sleep(0.3)
            except Exception:
                continue

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: extract flight info from DOM if API interception failed."""
        try:
            cards = await page.evaluate("""() => {
                const results = [];
                const cards = document.querySelectorAll(
                    '[class*=flight-card], [class*=flight-item], [class*=result-item], ' +
                    '[class*=flight-row], [class*=itinerary]'
                );
                for (const card of [...cards].slice(0, 20)) {
                    const text = card.textContent || '';
                    const priceMatch = text.match(/[¥￥](\\d+)/);
                    const timeMatch = text.match(/(\\d{2}:\\d{2})/g);
                    const flightMatch = text.match(/([A-Z0-9]{2}\\d{3,4})/);
                    if (priceMatch) {
                        results.push({
                            price: parseInt(priceMatch[1]),
                            times: timeMatch || [],
                            flight_no: flightMatch ? flightMatch[0] : '',
                            text: text.slice(0, 200),
                        });
                    }
                }
                return results;
            }""")

            if not cards:
                return []

            offers = []
            for card in cards:
                price = card.get("price", 0)
                if not price or price <= 0:
                    continue

                flight_no = card.get("flight_no", "AQ")
                times = card.get("times", [])

                dep_dt = datetime(
                    req.date_from.year, req.date_from.month, req.date_from.day,
                    int(times[0].split(":")[0]) if times else 0,
                    int(times[0].split(":")[1]) if times else 0,
                ) if times else datetime(req.date_from.year, req.date_from.month, req.date_from.day)

                arr_dt = datetime(
                    req.date_from.year, req.date_from.month, req.date_from.day,
                    int(times[1].split(":")[0]) if len(times) > 1 else 0,
                    int(times[1].split(":")[1]) if len(times) > 1 else 0,
                ) if len(times) > 1 else dep_dt

                dur = max(int((arr_dt - dep_dt).total_seconds()), 0) if arr_dt > dep_dt else 0

                fid = hashlib.md5(
                    f"aq_{req.origin}{req.destination}{flight_no}{price}".encode()
                ).hexdigest()[:12]

                segment = FlightSegment(
                    airline="AQ",
                    airline_name="9 Air",
                    flight_no=flight_no,
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=dur,
                    cabin_class="economy",
                )

                offers.append(FlightOffer(
                    id=f"aq_{fid}",
                    price=float(price),
                    currency="CNY",
                    price_formatted=f"{price} CNY",
                    outbound=FlightRoute(
                        segments=[segment],
                        total_duration_seconds=dur,
                        stopovers=0,
                    ),
                    inbound=None,
                    airlines=["9 Air"],
                    owner_airline="AQ",
                    booking_url=self._booking_url(req),
                    is_locked=False,
                    source="9air_direct",
                    source_tier="free",
                ))

            return offers
        except Exception as exc:
            logger.debug("9 Air: DOM extraction failed: %s", exc)
            return []

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the searchflight API response."""
        offers: list[FlightOffer] = []
        booking_url = self._booking_url(req)

        # 9 Air API response format varies — try common patterns
        flights = data.get("data", [])
        if isinstance(flights, dict):
            flights = flights.get("flightList", []) or flights.get("flights", []) or flights.get("results", [])

        if not isinstance(flights, list):
            return []

        for flight in flights:
            if not isinstance(flight, dict):
                continue

            price = (
                flight.get("price") or flight.get("minPrice") or
                flight.get("lowestPrice") or flight.get("salePrice") or 0
            )
            try:
                price = float(price)
            except (ValueError, TypeError):
                continue
            if price <= 0:
                continue

            flight_no = (
                flight.get("flightNo") or flight.get("flightNumber") or
                flight.get("no") or "AQ"
            )
            dep_time = (
                flight.get("depTime") or flight.get("departureTime") or
                flight.get("deptTime") or ""
            )
            arr_time = (
                flight.get("arrTime") or flight.get("arrivalTime") or
                flight.get("destTime") or ""
            )
            dep_airport = flight.get("depAirportCode") or flight.get("depCode") or req.origin
            arr_airport = flight.get("arrAirportCode") or flight.get("arrCode") or req.destination

            dep_dt = self._parse_time(dep_time, req.date_from)
            arr_dt = self._parse_time(arr_time, req.date_from)
            dur = max(int((arr_dt - dep_dt).total_seconds()), 0) if arr_dt > dep_dt else 0

            fid = hashlib.md5(
                f"aq_{req.origin}{req.destination}{flight_no}{price}".encode()
            ).hexdigest()[:12]

            segment = FlightSegment(
                airline="AQ",
                airline_name="9 Air",
                flight_no=flight_no,
                origin=dep_airport,
                destination=arr_airport,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur,
                cabin_class="economy",
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=dur,
                stopovers=0,
            )

            offers.append(FlightOffer(
                id=f"aq_{fid}",
                price=round(price, 2),
                currency="CNY",
                price_formatted=f"{price:.0f} CNY",
                outbound=route,
                inbound=None,
                airlines=["9 Air"],
                owner_airline="AQ",
                booking_url=booking_url,
                is_locked=False,
                source="9air_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _parse_time(raw: str, fallback_date) -> datetime:
        """Parse time string into datetime."""
        if not raw:
            return datetime(fallback_date.year, fallback_date.month, fallback_date.day)

        # Try full datetime formats
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M",
                    "%Y-%m-%dT%H:%M"):
            try:
                return datetime.strptime(raw[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue

        # Try time-only format "HH:MM" — combine with search date
        m = re.match(r"(\d{2}):(\d{2})", raw)
        if m:
            return datetime(
                fallback_date.year, fallback_date.month, fallback_date.day,
                int(m.group(1)), int(m.group(2)),
            )

        return datetime(fallback_date.year, fallback_date.month, fallback_date.day)

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.9air.com/zh-CN/booking/search"
            f"?depCity={req.origin}&arrCity={req.destination}&goDate={dep}"
            f"&adtCount=1&chdCount=0&infCount=0"
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"9air{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "CNY",
            offers=[],
            total_results=0,
        )
