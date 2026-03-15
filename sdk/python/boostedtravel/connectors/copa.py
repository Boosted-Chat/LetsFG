"""
Copa Airlines (CM) CDP Chrome connector — search URL + DOM scraping.

Copa Airlines is Panama's flag carrier (Star Alliance), with a major hub
at Tocumen International Airport (PTY) connecting North and South America.
Their booking engine lives at shopping.copaair.com — a React SPA that
renders flight results as richly-labelled ARIA DOM elements.

Strategy (CDP Chrome + DOM scraping):
1.  Launch REAL Chrome via CDP.
2.  Navigate to shopping.copaair.com with pre-filled search parameters.
3.  Try API interception first; if no JSON captured, scrape DOM ARIA labels.
4.  Parse flight offers from ARIA descriptions containing times/prices/routes.

Search URL:
  https://shopping.copaair.com/?roundtrip=false&area1={origin}&area2={destination}
    &date1={YYYY-MM-DD}&adults={n}&children={n}&infants={n}&langid=en
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, date as date_type, timedelta
from typing import Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import find_chrome, stealth_popen_kwargs, _launched_procs

logger = logging.getLogger(__name__)

_DEBUG_PORT = 9457
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".cm_chrome_data"
)

_browser = None
_context = None
_pw_instance = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    global _browser, _context, _pw_instance, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    if _context:
                        try:
                            _ = _context.pages
                            return _context
                        except Exception:
                            pass
                    contexts = _browser.contexts
                    if contexts:
                        _context = contexts[0]
                        return _context
            except Exception:
                pass

        from playwright.async_api import async_playwright

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("CM: connected to existing Chrome on port %d", _DEBUG_PORT)
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

            chrome = find_chrome()
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
            args = [
                chrome,
                f"--remote-debugging-port={_DEBUG_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",
                "--window-position=-2400,-2400",
                "--window-size=1400,900",
                "about:blank",
            ]
            _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
            _launched_procs.append(_chrome_proc)
            await asyncio.sleep(2.0)

            pw = await async_playwright().start()
            _pw_instance = pw
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            logger.info(
                "CM: Chrome launched on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    global _browser, _context, _pw_instance, _chrome_proc
    for obj, method in [(_browser, "close"), (_pw_instance, "stop")]:
        if obj:
            try:
                await getattr(obj, method)()
            except Exception:
                pass
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
    _browser = _context = _pw_instance = _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
        except Exception:
            pass


def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


def _parse_dt(s: str) -> datetime:
    for fmt in (
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(s[:len(fmt) + 3], fmt)
        except (ValueError, IndexError):
            continue
    return datetime.strptime(s[:10], "%Y-%m-%d")


_SKIP = frozenset((
    "analytics", "google", "facebook", "doubleclick", "fonts.",
    "gtm.", "pixel", "amplitude", ".css", ".png", ".jpg", ".svg",
    ".gif", ".woff", ".ico", "newrelic", "nr-data", "medallia",
    "adobedtm", "onetrust", "cookiebot",
))

_AVAIL_KEYS = (
    "booking/plan", "availability", "flights", "offers", "search", "air-bound",
    "itinerar", "fare", "journey", "graphql",
)


class CopaConnectorClient:
    """Copa Airlines CDP Chrome connector — search URL + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        context = await _get_context()
        page = await context.new_page()

        avail_data: dict = {}
        blocked = False

        async def _on_response(response):
            nonlocal blocked
            url_lower = response.url.lower()
            if any(s in url_lower for s in _SKIP):
                return

            status = response.status
            if status in (403, 429):
                if any(k in url_lower for k in _AVAIL_KEYS):
                    blocked = True
                return
            if status != 200:
                return

            is_avail = any(k in url_lower for k in _AVAIL_KEYS)
            if not is_avail:
                return

            try:
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                body = await response.text()
                if len(body) < 100:
                    return
                data = json.loads(body)
                # booking/plan returns a list — unwrap first element
                if isinstance(data, list) and data and isinstance(data[0], dict):
                    data = data[0]
                if isinstance(data, dict) and self._looks_like_flights(data):
                    if not avail_data:
                        avail_data.update(data)
                        logger.info("CM: captured flights from %s", response.url[:100])
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            url = self._build_search_url(req)
            logger.info("CM: loading %s->%s", req.origin, req.destination)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(4.0)

            # Dismiss overlays
            for sel in (
                "#onetrust-accept-btn-handler",
                "button:has-text('Accept')",
                "button:has-text('Aceptar')",
            ):
                try:
                    btn = page.locator(sel).first
                    if await btn.count() > 0 and await btn.is_visible(timeout=1000):
                        await btn.click(timeout=2000)
                        break
                except Exception:
                    continue

            # Wait for API response or DOM results
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while not avail_data and not blocked and time.monotonic() < deadline:
                # Check if DOM has flight results (faster than waiting for timeout)
                try:
                    has_results = await page.locator("[role='complementary']").count()
                    if has_results > 0:
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.5)

            if blocked:
                await _reset_profile()
                return self._empty(req)

            # Try API data first, then DOM scraping
            offers = []
            currency = "USD"
            if avail_data:
                offers = self._parse_flights(avail_data, req)
                currency = self._get_currency(avail_data, req)

            if not offers:
                # DOM scraping fallback — Copa renders ARIA-labelled flight cards
                dom_offers, dom_currency = await self._scrape_dom(page, req)
                if dom_offers:
                    offers = dom_offers
                    currency = dom_currency

            if not offers:
                return self._empty(req)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info("CM %s->%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            h = hashlib.md5(f"cm{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("CM CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    def _build_search_url(self, req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0
        return (
            f"https://shopping.copaair.com/"
            f"?roundtrip=false&area1={req.origin}&area2={req.destination}"
            f"&date1={dt.strftime('%Y-%m-%d')}&date2="
            f"&flexible_dates_v2=false&adults={adults}"
            f"&children={children}&infants={infants}"
            f"&isMiles=false&advanced_air_search=false"
            f"&stopover=false&sf=gs&langid=en"
        )

    @staticmethod
    def _looks_like_flights(data: dict) -> bool:
        s = json.dumps(data)[:5000].lower()
        flight_sigs = (
            "flightoffer", "segment", "departuretime", "departuredatetime",
            "itinerar", "flightleg", "bounddetail", "airbound",
            "flightnumber", "solutions",
        )
        price_sigs = ('"price"', '"amount"', '"total"', '"fare"', '"lowestpricecoachcabin"')
        return any(sig in s for sig in flight_sigs) and any(sig in s for sig in price_sigs)

    def _parse_flights(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []

        # Copa booking/plan format: data.solutions[]
        solutions = data.get("solutions")
        if isinstance(solutions, list) and solutions:
            return self._parse_plan_solutions(data, req)

        inner = data
        for key in ("data", "response", "result"):
            if key in inner and isinstance(inner[key], (dict, list)):
                val = inner[key]
                inner = val if isinstance(val, dict) else {"items": val}

        currency = self._get_currency(data, req)

        flight_list = None
        for key in (
            "flights", "flightOffers", "offers", "results", "items",
            "itineraries", "boundGroups", "originDestinationOptionList",
        ):
            candidate = inner.get(key)
            if isinstance(candidate, list) and len(candidate) > 0:
                flight_list = candidate
                break

        if not flight_list:
            for v in inner.values():
                if isinstance(v, list) and len(v) >= 2 and isinstance(v[0], dict):
                    if self._get_price(v[0]):
                        flight_list = v
                        break

        if not flight_list:
            return offers

        for flight in flight_list[:50]:
            if not isinstance(flight, dict):
                continue

            price = self._get_price(flight)
            if not price or price <= 0:
                continue

            segments = self._extract_segments(flight, req)
            if not segments:
                continue

            total_dur = int(
                (segments[-1].arrival - segments[0].departure).total_seconds()
            ) if segments[-1].arrival > segments[0].departure else 0

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur,
                stopovers=max(len(segments) - 1, 0),
            )

            offer_key = f"cm_{req.origin}_{req.destination}_{segments[0].departure.isoformat()}_{price}"
            offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]
            all_airlines = list({s.airline for s in segments})

            offers.append(FlightOffer(
                id=f"cm_{offer_id}",
                price=round(price, 2),
                currency=currency,
                outbound=route,
                airlines=[("Copa Airlines" if a == "CM" else a) for a in all_airlines],
                owner_airline="CM",
                booking_url=self._user_url(req),
                is_locked=False,
                source="copa_direct",
                source_tier="free",
            ))

        return offers

    def _parse_plan_solutions(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Copa booking/plan API response (solutions[] format)."""
        offers: list[FlightOffer] = []
        cur_obj = data.get("currency", {})
        currency = cur_obj.get("code", "USD") if isinstance(cur_obj, dict) else "USD"

        solutions = data.get("solutions", [])
        for sol in solutions[:50]:
            if not isinstance(sol, dict):
                continue

            price = sol.get("lowestPriceCoachCabin") or sol.get("totalPrice") or 0
            if not price or price <= 0:
                continue

            flights = sol.get("flights", [])
            if not flights:
                continue

            segments: list[FlightSegment] = []
            for fl in flights:
                if not isinstance(fl, dict):
                    continue
                mc = fl.get("marketingCarrier") or {}
                dep = fl.get("departure") or {}
                arr = fl.get("arrival") or {}

                dep_date = dep.get("flightDate", "")
                dep_time = dep.get("flightTime", "")
                arr_date = arr.get("flightDate", dep_date)
                arr_time = arr.get("flightTime", "")

                dep_dt = _parse_dt(f"{dep_date}T{dep_time}:00") if dep_date and dep_time else _to_datetime(req.date_from)
                arr_dt = _parse_dt(f"{arr_date}T{arr_time}:00") if arr_date and arr_time else dep_dt + timedelta(hours=3)

                carrier = mc.get("airlineCode", "CM")
                fno = str(mc.get("flightNumber", ""))

                segments.append(FlightSegment(
                    airline=carrier,
                    airline_name=mc.get("airlineName", "Copa Airlines"),
                    flight_no=f"{carrier}{fno}" if fno else f"{carrier}?",
                    origin=dep.get("airportCode", req.origin),
                    destination=arr.get("airportCode", req.destination),
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0,
                    cabin_class="economy",
                ))

            if not segments:
                continue

            jt = sol.get("journeyTime", "")
            dur = 0
            m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?", jt)
            if m:
                dur = int(m.group(1) or 0) * 3600 + int(m.group(2) or 0) * 60

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=dur or int((segments[-1].arrival - segments[0].departure).total_seconds()),
                stopovers=sol.get("numberOfLayovers", max(len(segments) - 1, 0)),
            )

            offer_key = f"cm_{req.origin}_{req.destination}_{segments[0].departure.isoformat()}_{price}"
            offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]

            offers.append(FlightOffer(
                id=f"cm_{offer_id}",
                price=round(float(price), 2),
                currency=currency,
                outbound=route,
                airlines=["Copa Airlines"],
                owner_airline="CM",
                booking_url=self._user_url(req),
                is_locked=False,
                source="copa_direct",
                source_tier="free",
            ))

        return offers

    def _extract_segments(self, flight: dict, req: FlightSearchRequest) -> list[FlightSegment]:
        segments: list[FlightSegment] = []

        seg_list = None
        for key in ("segments", "segmentList", "legs", "flightSegments"):
            candidate = flight.get(key)
            if isinstance(candidate, list) and candidate:
                seg_list = candidate
                break

        if not seg_list:
            dep = flight.get("departureDateTime") or flight.get("departure") or ""
            if dep:
                seg_list = [flight]

        if not seg_list:
            return segments

        for seg in seg_list:
            if not isinstance(seg, dict):
                continue

            dep_str = seg.get("departureDateTime") or seg.get("departureTime") or seg.get("departure") or ""
            arr_str = seg.get("arrivalDateTime") or seg.get("arrivalTime") or seg.get("arrival") or ""
            origin = seg.get("departureAirportCode") or seg.get("origin") or seg.get("from") or req.origin
            dest = seg.get("arrivalAirportCode") or seg.get("destination") or seg.get("to") or req.destination
            carrier = seg.get("airlineCode") or seg.get("carrierCode") or seg.get("operatingCarrier") or "CM"
            fno = seg.get("flightNumber") or seg.get("flightNo") or ""

            dep_dt = _parse_dt(dep_str) if dep_str else _to_datetime(req.date_from)
            arr_dt = _parse_dt(arr_str) if arr_str else dep_dt + timedelta(hours=3)
            dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

            segments.append(FlightSegment(
                airline=carrier,
                airline_name="Copa Airlines" if carrier == "CM" else carrier,
                flight_no=f"{carrier}{fno}" if fno and not fno.startswith(carrier) else (fno or f"{carrier}?"),
                origin=origin,
                destination=dest,
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=dur,
                cabin_class="economy",
            ))

        return segments

    @staticmethod
    def _get_price(obj: dict) -> Optional[float]:
        for key in ("price", "totalPrice", "amount", "fareAmount", "total", "lowestPrice", "displayPrice"):
            val = obj.get(key)
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
            if isinstance(val, dict):
                for ik in ("amount", "total", "value"):
                    iv = val.get(ik)
                    if isinstance(iv, (int, float)) and iv > 0:
                        return float(iv)
        fares = obj.get("fareFamilies") or obj.get("cabins") or []
        if isinstance(fares, list):
            for fare in fares:
                if isinstance(fare, dict):
                    p = fare.get("price") or fare.get("amount")
                    if isinstance(p, (int, float)) and p > 0:
                        return float(p)
                    if isinstance(p, dict):
                        a = p.get("amount") or p.get("total")
                        if isinstance(a, (int, float)) and a > 0:
                            return float(a)
        return None

    @staticmethod
    def _get_currency(data: dict, req: FlightSearchRequest) -> str:
        for key in ("currencyCode", "currency"):
            val = data.get(key)
            if isinstance(val, str) and len(val) == 3:
                return val
            if isinstance(val, dict):
                code = val.get("code")
                if isinstance(code, str) and len(code) == 3:
                    return code
        for v in data.values():
            if isinstance(v, dict):
                for key in ("currencyCode", "currency"):
                    val = v.get(key)
                    if isinstance(val, str) and len(val) == 3:
                        return val
        return "USD"

    @staticmethod
    def _user_url(req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        return (
            f"https://shopping.copaair.com/"
            f"?roundtrip=false&area1={req.origin}&area2={req.destination}"
            f"&date1={dt.strftime('%Y-%m-%d')}&adults={req.adults or 1}"
            f"&langid=en"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"cm{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "USD",
            offers=[],
            total_results=0,
        )

    # ------------------------------------------------------------------
    # DOM scraping — Copa renders ARIA-labelled flight cards
    # ------------------------------------------------------------------

    async def _scrape_dom(self, page, req: FlightSearchRequest) -> tuple[list[FlightOffer], str]:
        """Parse flight offers from Copa's DOM ARIA labels.

        Each flight card has role='complementary' with an aria-label like:
          "Direct flight from Panama (PTY) to Bogota (BOG) with departure time
           07:50 am and arrival at 09:29 am; from 15, April, 2026, operated by
           Copa Airlines. Total flight duration is 1 hour 39 minutes."

        Price buttons have aria-labels like:
          "Economy cabin. Price from 129.25 USD per adult"
        """
        offers: list[FlightOffer] = []
        currency = "USD"

        try:
            # Wait for flight cards to appear
            await page.wait_for_selector("[role='complementary']", timeout=15000)
        except Exception:
            return offers, currency

        cards = await page.query_selector_all("[role='complementary']")

        for card in cards[:50]:
            try:
                label = await card.get_attribute("aria-label") or ""
                if not label or "flight" not in label.lower():
                    continue

                # Extract route info from label
                dep_match = re.search(r"departure time (\d{1,2}:\d{2}\s*[ap]m)", label, re.I)
                arr_match = re.search(r"arrival (?:at )?(\d{1,2}:\d{2}\s*[ap]m)", label, re.I)
                dur_match = re.search(r"(\d+)\s*hour[s]?\s*(\d+)?\s*minute", label, re.I)
                origin_match = re.search(r"from\s+\w[\w\s]*\((\w{3})\)", label)
                dest_match = re.search(r"to\s+\w[\w\s]*\((\w{3})\)", label)
                nonstop = "nonstop" in label.lower() or "direct" in label.lower()

                dep_time = dep_match.group(1) if dep_match else None
                arr_time = arr_match.group(1) if arr_match else None

                dur_seconds = 0
                if dur_match:
                    hours = int(dur_match.group(1))
                    minutes = int(dur_match.group(2) or 0)
                    dur_seconds = hours * 3600 + minutes * 60

                origin_code = origin_match.group(1) if origin_match else req.origin
                dest_code = dest_match.group(1) if dest_match else req.destination

                # Parse departure/arrival datetime
                base_date = _to_datetime(req.date_from)
                dep_dt = self._parse_time(dep_time, base_date) if dep_time else base_date
                arr_dt = self._parse_time(arr_time, base_date) if arr_time else dep_dt + timedelta(seconds=dur_seconds or 7200)
                if arr_dt <= dep_dt:
                    arr_dt += timedelta(days=1)

                # Extract flight number from card text
                inner_text = await card.inner_text()
                fno_match = re.search(r"\b(CM)\s*(\d{2,4})\b", inner_text)
                flight_no = f"CM{fno_match.group(2)}" if fno_match else "CM?"

                # Extract cheapest price from price buttons inside the card
                price = None
                price_buttons = await card.query_selector_all("button")
                for btn in price_buttons:
                    btn_label = await btn.get_attribute("aria-label") or await btn.inner_text()
                    price_match = re.search(r"(\d[\d,.]+)\s*([A-Z]{3})", btn_label)
                    if price_match:
                        try:
                            p = float(price_match.group(1).replace(",", ""))
                            cur = price_match.group(2)
                            if price is None or p < price:
                                price = p
                                currency = cur
                        except ValueError:
                            pass

                if not price or price <= 0:
                    continue

                segment = FlightSegment(
                    airline="CM",
                    airline_name="Copa Airlines",
                    flight_no=flight_no,
                    origin=origin_code,
                    destination=dest_code,
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=dur_seconds,
                    cabin_class="economy",
                )

                route = FlightRoute(
                    segments=[segment],
                    total_duration_seconds=dur_seconds or int((arr_dt - dep_dt).total_seconds()),
                    stopovers=0 if nonstop else 1,
                )

                offer_key = f"cm_{origin_code}_{dest_code}_{dep_dt.isoformat()}_{price}"
                offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"cm_{offer_id}",
                    price=round(price, 2),
                    currency=currency,
                    outbound=route,
                    airlines=["Copa Airlines"],
                    owner_airline="CM",
                    booking_url=self._user_url(req),
                    is_locked=False,
                    source="copa_direct",
                    source_tier="free",
                ))
            except Exception:
                continue

        logger.info("CM DOM scrape: %d offers found", len(offers))
        return offers, currency

    @staticmethod
    def _parse_time(time_str: str, base: datetime) -> datetime:
        """Parse '07:50 am' or '9:29am' into a datetime on the given date."""
        clean = time_str.strip().lower().replace(" ", "")
        m = re.match(r"(\d{1,2}):(\d{2})(am|pm)", clean)
        if not m:
            return base
        hour, minute, ampm = int(m.group(1)), int(m.group(2)), m.group(3)
        if ampm == "pm" and hour != 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
        return base.replace(hour=hour, minute=minute, second=0, microsecond=0)
