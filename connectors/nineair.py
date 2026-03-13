"""
9 Air browser-based scraper — Playwright form fill + response interception.

9 Air (IATA: AQ) is a Chinese LCC headquartered in Guangzhou, Guangdong.
Hub: CAN (Guangzhou Baiyun). Domestic Chinese network (~10 routes from CAN).
Website: www.9air.com (Chinese, Vue 2 + Element-UI SPA).

NOT available in GDS — must be scraped directly.

Strategy (browser-based, validated Mar 2026):
  1. Launch Playwright browser, navigate to homepage
  2. Set up route interception for /searchflight — replace TongDun
     black_box with _fmOpt.sign() in Anti-Headers (bypasses anti-bot)
  3. Fill search form atomically via Vue 2 reactive model ($set)
  4. Trigger SPA navigation via parent.toSearch() → /zh-CN/book/booking
  5. Intercept modified searchflight response → parse flights

Search API endpoint (GET, with Anti-Headers):
  GET /shop/api/shopping/b2c/searchflight?language=zh_CN&currency=CNY&...

Anti-bot solution:
  The server requires an "Anti-Headers" HTTP header with JSON payload.
  The standard flow uses a TongDun fingerprint (black_box via _fmOpt)
  plus a NetEase YiDun token (dunVm). YiDun fails in Playwright.
  Solution: strip black_box, use _fmOpt.sign({path, body}) instead.
  The server accepts sign-only anti-headers. Discovered via probes 29-34.
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
    "JHG": "西双版纳", "WUT": "忻州", "XNN": "西宁",
    "BKK": "曼谷", "HAN": "河内", "KUL": "吉隆坡", "KIX": "大阪",
    "VTE": "万象",
}


async def _get_browser():
    """Get or create the shared Playwright browser."""
    global _browser, _pw_instance
    if _browser and _browser.is_connected():
        return _browser
    from connectors.browser import launch_headed_browser
    _browser = await launch_headed_browser(extra_args=[
        "--lang=zh-CN",
        "--disable-blink-features=AutomationControlled",
    ])
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
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => false});
                window.chrome = { runtime: { connect: function(){}, sendMessage: function(){} } };
            """)

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
        """Navigate, fill form via Vue model, intercept search API response.

        The anti-bot bypass works by intercepting outgoing searchflight requests
        and replacing the TongDun ``black_box`` in Anti-Headers with a
        ``_fmOpt.sign()`` signature. The server accepts sign-only payloads.
        """

        remaining = lambda: max(self.timeout - (time.monotonic() - t0), 1)

        captured_data: dict = {}
        api_event = asyncio.Event()

        # ── Route interception: modify Anti-Headers on outgoing requests ───
        async def _modify_anti_headers(route):
            """Strip black_box, inject _fmOpt.sign() into Anti-Headers."""
            request = route.request
            url = request.url
            headers = dict(request.headers)
            anti = headers.get("anti-headers", "")

            if anti:
                try:
                    parsed = json.loads(anti)
                    parsed.pop("black_box", None)

                    rel_url = url.replace(_BASE_URL, "")
                    sign_result = await page.evaluate(
                        r"""(urlPath) => {
                            try {
                                if (typeof _fmOpt === 'undefined' || !_fmOpt.sign) return {error: 'no _fmOpt'};
                                const parts = urlPath.split('?');
                                const r = _fmOpt.sign({path: parts[0], body: parts[1] || ''});
                                return {code: r.code, sign: r.sign};
                            } catch(e) { return {error: e.message}; }
                        }""",
                        rel_url,
                    )

                    if sign_result.get("code") == 0:
                        parts = rel_url.split("?")
                        parsed["path"] = parts[0]
                        parsed["body"] = parts[1] if len(parts) > 1 else ""
                        parsed["sign"] = sign_result["sign"]

                    headers["anti-headers"] = json.dumps(parsed)
                except Exception:
                    pass

            resp = await route.fetch(headers=headers)
            body_text = await resp.text()

            if "searchflight" in url.lower() and resp.status == 200:
                try:
                    data = json.loads(body_text)
                    if data.get("status") == 200 and data.get("data"):
                        captured_data["json"] = data
                        api_event.set()
                except Exception:
                    pass

            try:
                await route.fulfill(response=resp)
            except Exception:
                pass  # Context may already be closed

        await page.route("**/searchflight**", _modify_anti_headers)
        await page.route("**/calendarshopping**", _modify_anti_headers)

        # Step 1: Load homepage
        logger.info("9 Air: loading homepage for %s->%s", req.origin, req.destination)
        try:
            await page.goto(
                _SEARCH_PAGE,
                wait_until="domcontentloaded",
                timeout=int(min(remaining(), 15) * 1000),
            )
        except Exception as exc:
            logger.warning("9 Air: failed to load page: %s", exc)
            return []

        await asyncio.sleep(4.0)

        # Clear black_box to force sign-only flow
        await page.evaluate("() => { sessionStorage.removeItem('currentBlackBox'); }")

        # Step 2: Atomic form fill via Vue model + SPA navigation
        dep_cn = _IATA_CN.get(req.origin, req.origin)
        arr_cn = _IATA_CN.get(req.destination, req.destination)
        date_str = req.date_from.strftime("%Y-%m-%d")

        nav_ok = await page.evaluate(
            r"""([depCode, depName, arrCode, arrName, dateStr]) => {
                const depCity = { name: depName, code: depCode, type: "CITY" };
                const arrCity = { name: arrName, code: arrCode, type: "CITY" };
                /* Close any open popovers */
                document.querySelectorAll('.el-popover, .el-popper').forEach(el => {
                    el.style.display = 'none';
                    if (el.__vue__) try { el.__vue__.doClose(); } catch(e) {}
                });
                const formEl = document.querySelector('.flight-way');
                if (!formEl || !formEl.__vue__) return false;
                const fvm = formEl.__vue__;
                fvm.tripType = 'OW';
                fvm.$set(fvm.form, 's_address', depCity);
                fvm.$set(fvm.form, 'e_address', arrCity);
                fvm.$set(fvm.form, 's_date', dateStr);
                fvm.$set(fvm.form, 'e_date', '');
                document.querySelectorAll('.fly-input').forEach(el => {
                    if (!el.__vue__) return;
                    const vm = el.__vue__;
                    if ((vm.$props || {}).icon === 'dept') {
                        vm.showCityValue = depName; vm.defaultCity = depCity;
                    }
                    if ((vm.$props || {}).icon === 'dest') {
                        vm.showCityValue = arrName; vm.defaultCity = arrCity;
                    }
                });
                const searchData = { tripType: 'OW', form: fvm.form };
                fvm.$mstore.save(searchData, 'searchForm');
                fvm.$parent.toSearch(searchData);
                return true;
            }""",
            [req.origin, dep_cn, req.destination, arr_cn, date_str],
        )

        if not nav_ok:
            logger.warning("9 Air: Vue form fill / navigation failed")
            return []

        # Step 3: Wait for intercepted API response
        try:
            await asyncio.wait_for(api_event.wait(), timeout=remaining())
        except asyncio.TimeoutError:
            logger.warning("9 Air: timed out waiting for search results")
            return []

        data = captured_data.get("json", {})
        if not data:
            return []

        return self._parse_response(data, req)

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

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse the searchflight API response.

        Response structure::

            data.flights[0][] — array of flight objects, each with:
              .segments[] — leg details (departDate, departTime, arrivalTime, etc.)
              .fares[]    — cabin/price options (price, ticketPrice, taxPrice, cabinClass)
        """
        offers: list[FlightOffer] = []
        booking_url = self._booking_url(req)

        outer = data.get("data", {})
        if not isinstance(outer, dict):
            return []

        flights_wrapper = outer.get("flights", [])
        if not flights_wrapper or not isinstance(flights_wrapper, list):
            return []

        # flights is [[flight1, flight2, ...]] — first element is array of flights
        flight_list = flights_wrapper[0] if flights_wrapper else []
        if not isinstance(flight_list, list):
            return []

        for flight in flight_list:
            if not isinstance(flight, dict):
                continue
            if flight.get("salesClosed"):
                continue

            segments_raw = flight.get("segments", [])
            fares = flight.get("fares", [])
            if not segments_raw or not fares:
                continue

            # Find cheapest ADT fare
            adt_fares = [f for f in fares if f.get("paxType") == "ADT"]
            if not adt_fares:
                adt_fares = fares
            cheapest = min(adt_fares, key=lambda f: float(f.get("price", 99999)))
            price = float(cheapest.get("price", 0))
            if price <= 0:
                continue

            currency = cheapest.get("currency", "CNY")

            # Build segments
            parsed_segments: list[FlightSegment] = []
            total_duration = 0

            for seg in segments_raw:
                dep_date = seg.get("departDate", "")
                dep_time = seg.get("departTime", "")
                arr_date = seg.get("arrivalDate", "")
                arr_time = seg.get("arrivalTime", "")

                dep_dt = self._parse_datetime(dep_date, dep_time, req.date_from)
                arr_dt = self._parse_datetime(arr_date, arr_time, req.date_from)

                dur = seg.get("flightTime", 0) or 0
                if dur == 0 and arr_dt > dep_dt:
                    dur = int((arr_dt - dep_dt).total_seconds())
                total_duration += dur

                flight_no = seg.get("marketFlightNo", "") or seg.get("carrierFlightNo", "")
                airline_code = seg.get("marketAirlineCode", "AQ")
                airline_name = seg.get("marketAirlineName", "9 Air")

                parsed_segments.append(FlightSegment(
                    airline=airline_code,
                    airline_name=airline_name or "9 Air",
                    flight_no=f"{airline_code}{flight_no}" if flight_no and not flight_no.startswith(airline_code) else (flight_no or "AQ"),
                    origin=seg.get("departAirportCode", req.origin),
                    destination=seg.get("arrivalAirportCode", req.destination),
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=dur,
                    cabin_class=cheapest.get("cabinClass", "Y"),
                ))

            if not parsed_segments:
                continue

            stopovers = sum(s.get("stopoverCount", 0) for s in segments_raw)

            fid = hashlib.md5(
                f"aq_{req.origin}{req.destination}{flight.get('flightId', '')}{price}".encode()
            ).hexdigest()[:12]

            route = FlightRoute(
                segments=parsed_segments,
                total_duration_seconds=total_duration,
                stopovers=stopovers,
            )

            offers.append(FlightOffer(
                id=f"aq_{fid}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{price:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=list({s.airline_name for s in parsed_segments}),
                owner_airline="AQ",
                booking_url=booking_url,
                is_locked=False,
                source="9air_direct",
                source_tier="free",
            ))

            # Also add higher fare classes as separate offers
            for fare in adt_fares:
                fp = float(fare.get("price", 0))
                if fp <= 0 or fp == price:
                    continue

                fare_fid = hashlib.md5(
                    f"aq_{req.origin}{req.destination}{flight.get('flightId', '')}{fp}".encode()
                ).hexdigest()[:12]

                fare_segments = []
                for ps in parsed_segments:
                    fare_segments.append(FlightSegment(
                        airline=ps.airline,
                        airline_name=ps.airline_name,
                        flight_no=ps.flight_no,
                        origin=ps.origin,
                        destination=ps.destination,
                        departure=ps.departure,
                        arrival=ps.arrival,
                        duration_seconds=ps.duration_seconds,
                        cabin_class=fare.get("cabinClass", ps.cabin_class),
                    ))

                offers.append(FlightOffer(
                    id=f"aq_{fare_fid}",
                    price=round(fp, 2),
                    currency=currency,
                    price_formatted=f"{fp:.0f} {currency}",
                    outbound=FlightRoute(
                        segments=fare_segments,
                        total_duration_seconds=total_duration,
                        stopovers=stopovers,
                    ),
                    inbound=None,
                    airlines=list({s.airline_name for s in fare_segments}),
                    owner_airline="AQ",
                    booking_url=booking_url,
                    is_locked=False,
                    source="9air_direct",
                    source_tier="free",
                ))

        return offers

    @staticmethod
    def _parse_datetime(date_str: str, time_str: str, fallback_date) -> datetime:
        """Parse separate date and time strings into a datetime."""
        try:
            if date_str and time_str:
                return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass

        if time_str:
            m = re.match(r"(\d{2}):(\d{2})", time_str)
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
            f"https://www.9air.com/zh-CN/book/booking"
            f"?tripType=OW&flightCondition=index%3A0%3BdepCity%3A{req.origin}"
            f"%3BarrCity%3A{req.destination}%3Bdate%3A{dep}"
            f"&ADT=1&CHD=0&INF=0"
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
