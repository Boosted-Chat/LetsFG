"""
LOT Polish Airlines (LO) CDP Chrome connector — search URL + API interception.

LOT Polish Airlines is Poland's flag carrier (Star Alliance), hub at Warsaw
Chopin (WAW).  LOT uses Amadeus Altéa NDC + own frontend; the booking SPA
calls internal availability APIs.

Strategy (CDP Chrome + response interception):
1.  Launch REAL Chrome via CDP.
2.  Navigate to LOT search results URL with pre-filled params.
3.  Intercept availability API responses.
4.  Parse flight offers.

Search URL:
  https://www.lot.com/us/en/offer/flights
    ?departureAirport={origin}&arrivalAirport={dest}
    &departureDate={DD.MM.YYYY}&adults={n}&children={n}
    &infants={n}&cabinClass=ECONOMY&tripType=ONE_WAY
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

_DEBUG_PORT = 9459
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".lo_chrome_data"
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
            logger.info("LO: connected to existing Chrome on port %d", _DEBUG_PORT)
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
            logger.info("LO: Chrome launched on CDP port %d (pid %d)", _DEBUG_PORT, _chrome_proc.pid)

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
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s[:len(fmt) + 3], fmt)
        except (ValueError, IndexError):
            continue
    return datetime.strptime(s[:10], "%Y-%m-%d")


_SKIP = frozenset((
    "analytics", "google", "facebook", "doubleclick", "fonts.",
    "gtm.", "pixel", "amplitude", ".css", ".png", ".jpg", ".svg",
    ".gif", ".woff", ".ico", "newrelic", "nr-data", "adobedtm",
    "onetrust", "cookiebot", "sentry",
))

_AVAIL_KEYS = (
    "availability", "flights", "offers", "search", "air-bound",
    "itinerar", "fare", "journey", "shopping",
)


class LotConnectorClient:
    """LOT Polish Airlines CDP Chrome connector."""

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
                if isinstance(data, dict) and self._looks_like_flights(data):
                    if not avail_data:
                        avail_data.update(data)
                        logger.info("LO: captured flights from %s", response.url[:100])
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            url = self._build_search_url(req)
            logger.info("LO: loading %s->%s", req.origin, req.destination)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3.0)

            for sel in (
                "#onetrust-accept-btn-handler",
                "button:has-text('Accept All')",
                "button:has-text('Accept')",
                "button:has-text('Akceptuję')",
                "button:has-text('Zaakceptuj')",
            ):
                try:
                    btn = page.locator(sel).first
                    if await btn.count() > 0 and await btn.is_visible(timeout=1000):
                        await btn.click(timeout=2000)
                        break
                except Exception:
                    continue

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while not avail_data and not blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            if blocked:
                await _reset_profile()
                return self._empty(req)
            if not avail_data:
                return self._empty(req)

            offers = self._parse_flights(avail_data, req)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info("LO %s->%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

            h = hashlib.md5(f"lo{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
            currency = self._get_currency(avail_data, req)

            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.error("LO CDP error: %s", e)
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
            f"https://www.lot.com/us/en/offer/flights"
            f"?departureAirport={req.origin}&arrivalAirport={req.destination}"
            f"&departureDate={dt.strftime('%d.%m.%Y')}"
            f"&adults={adults}&children={children}&infants={infants}"
            f"&cabinClass=ECONOMY&tripType=ONE_WAY"
        )

    @staticmethod
    def _looks_like_flights(data: dict) -> bool:
        s = json.dumps(data)[:5000].lower()
        flight_sigs = (
            "flightoffer", "segment", "departuretime", "departuredatetime",
            "itinerar", "flightleg", "bounddetail", "recommendation",
        )
        price_sigs = ('"price"', '"amount"', '"total"', '"fare"')
        return any(sig in s for sig in flight_sigs) and any(sig in s for sig in price_sigs)

    def _parse_flights(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []

        inner = data
        for key in ("data", "response", "result"):
            if key in inner and isinstance(inner[key], (dict, list)):
                val = inner[key]
                inner = val if isinstance(val, dict) else {"items": val}

        currency = self._get_currency(data, req)

        flight_list = None
        for key in (
            "flights", "flightOffers", "offers", "results", "recommendations",
            "itineraries", "items", "originDestinationOptionList",
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

            offer_key = f"lo_{req.origin}_{req.destination}_{segments[0].departure.isoformat()}_{price}"
            offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]
            all_airlines = list({s.airline for s in segments})

            offers.append(FlightOffer(
                id=f"lo_{offer_id}",
                price=round(price, 2),
                currency=currency,
                outbound=route,
                airlines=[("LOT Polish Airlines" if a == "LO" else a) for a in all_airlines],
                owner_airline="LO",
                booking_url=self._user_url(req),
                is_locked=False,
                source="lot_direct",
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
            carrier = seg.get("airlineCode") or seg.get("carrierCode") or seg.get("operatingCarrier") or "LO"
            fno = seg.get("flightNumber") or seg.get("flightNo") or ""

            dep_dt = _parse_dt(dep_str) if dep_str else _to_datetime(req.date_from)
            arr_dt = _parse_dt(arr_str) if arr_str else dep_dt + timedelta(hours=3)
            dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

            segments.append(FlightSegment(
                airline=carrier,
                airline_name="LOT Polish Airlines" if carrier == "LO" else carrier,
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
        for key in ("price", "totalPrice", "amount", "fareAmount", "total", "lowestPrice"):
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
        for v in data.values():
            if isinstance(v, dict):
                for key in ("currencyCode", "currency"):
                    val = v.get(key)
                    if isinstance(val, str) and len(val) == 3:
                        return val
        return "PLN"

    @staticmethod
    def _user_url(req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        return (
            f"https://www.lot.com/us/en/offer/flights"
            f"?departureAirport={req.origin}&arrivalAirport={req.destination}"
            f"&departureDate={dt.strftime('%d.%m.%Y')}&adults={req.adults or 1}"
            f"&cabinClass=ECONOMY&tripType=ONE_WAY"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"lo{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "PLN",
            offers=[],
            total_results=0,
        )
