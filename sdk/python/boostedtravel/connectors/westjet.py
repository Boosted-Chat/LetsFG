"""
WestJet (WS) CDP Chrome connector — search URL + availability API interception.

WestJet's booking engine is an Angular SPA at flightbooking.westjet.com.
We navigate to the booking URL with pre-filled search params and intercept
the availability API response.

Strategy (CDP Chrome + response interception):
1.  Launch REAL Chrome via CDP (WestJet uses Akamai bot protection).
2.  Navigate to the WestJet flight search page with query params.
3.  Intercept availability API responses (POST /api/flights or GraphQL).
4.  Parse flight data into FlightOffers.

Booking URL pattern (example):
  https://www.westjet.com/en-ca/flights/search
    ?orig={origin}&dest={destination}&depart={YYYY-MM-DD}
    &adt={adults}&chd={children}&inf={infants}&type=one-way
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

_DEBUG_PORT = 9455
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".ws_chrome_data"
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
    """Get or create a persistent browser context."""
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
            logger.info("WS: connected to existing Chrome on port %d", _DEBUG_PORT)
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
                "WS: Chrome launched on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _reset_profile():
    """Wipe Chrome profile on persistent bot detection."""
    global _browser, _context, _pw_instance, _chrome_proc
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    try:
        if _pw_instance:
            await _pw_instance.stop()
    except Exception:
        pass
    if _chrome_proc:
        try:
            _chrome_proc.terminate()
        except Exception:
            pass
    _browser = None
    _context = None
    _pw_instance = None
    _chrome_proc = None
    if os.path.isdir(_USER_DATA_DIR):
        try:
            shutil.rmtree(_USER_DATA_DIR)
            logger.info("WS: deleted stale Chrome profile")
        except Exception:
            pass


# ── Helpers ──────────────────────────────────────────────────────────────────

def _to_datetime(val) -> datetime:
    if isinstance(val, datetime):
        return val
    if isinstance(val, date_type):
        return datetime(val.year, val.month, val.day)
    return datetime.strptime(str(val), "%Y-%m-%d")


def _parse_dt(s: str) -> datetime:
    """Parse datetime from WestJet API responses."""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ):
        try:
            return datetime.strptime(s[:len(fmt) + 3], fmt)
        except (ValueError, IndexError):
            continue
    return datetime.strptime(s[:10], "%Y-%m-%d")


_SKIP = frozenset((
    "analytics", "google", "facebook", "doubleclick", "fonts.",
    "gtm.", "pixel", "amplitude", ".css", ".png", ".jpg", ".svg",
    ".gif", ".woff", ".ico", "demdex", "omtrdc",
    "newrelic", "nr-data", "medallia", "adobedtm",
    "tealium", "mparticle", "segment", "fullstory", "hotjar",
    "onetrust", "cookiebot", "snapchat",
))

_AVAIL_KEYS = (
    "availability", "flights", "offers", "search", "air-bound",
    "itinerar", "fare", "journey", "lowfare",
)


class WestjetConnectorClient:
    """WestJet CDP Chrome connector — search + API interception."""

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
                    logger.warning("WS: %d on %s", status, response.url[:120])
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
                if not isinstance(data, dict):
                    return

                if self._looks_like_flights(data):
                    if not avail_data:
                        avail_data.update(data)
                        logger.info(
                            "WS: captured flights (%d bytes) from %s",
                            len(body), response.url[:100],
                        )
            except Exception:
                pass

        page.on("response", _on_response)

        try:
            # ── Strategy 1: Direct search URL ──
            url = self._build_search_url(req)
            logger.info("WS: loading %s->%s", req.origin, req.destination)
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3.0)

            # Dismiss cookie / promo banners
            await self._dismiss_overlays(page)

            # Wait for availability API response
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while not avail_data and not blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            # ── Strategy 2: Fallback to homepage form fill ──
            if not avail_data and not blocked:
                logger.info("WS: URL approach failed, trying form fill")
                avail_data, blocked = await self._form_search(page, req, t0)

            if blocked:
                logger.warning("WS: Akamai blocked, resetting profile")
                await _reset_profile()
                return self._empty(req)

            if not avail_data:
                logger.warning("WS: no flight data captured")
                return self._empty(req)

            offers = self._parse_flights(avail_data, req)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "WS %s->%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"ws{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            currency = self._get_currency(avail_data, req)

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("WS CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # URL / form
    # ------------------------------------------------------------------

    def _build_search_url(self, req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0
        return (
            f"https://www.westjet.com/en-ca/flights/search"
            f"?orig={req.origin}&dest={req.destination}"
            f"&depart={dt.strftime('%Y-%m-%d')}"
            f"&adt={adults}&chd={children}&inf={infants}"
            f"&type=one-way"
        )

    async def _dismiss_overlays(self, page) -> None:
        for sel in (
            "#onetrust-accept-btn-handler",
            "button:has-text('Accept')",
            "button:has-text('Got it')",
            "button:has-text('Close')",
            "[data-testid='close-button']",
        ):
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and await btn.is_visible(timeout=1000):
                    await btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
            except Exception:
                continue

    async def _form_search(
        self, page, req: FlightSearchRequest, t0: float
    ) -> tuple[dict, bool]:
        """Fallback: load homepage, fill form, submit."""
        avail_data: dict = {}
        blocked = False

        try:
            await page.goto(
                "https://www.westjet.com/en-ca/flights",
                wait_until="domcontentloaded",
                timeout=20000,
            )
            await asyncio.sleep(4.0)
            await self._dismiss_overlays(page)

            # Select one-way
            try:
                ow = page.locator("label:has-text('One-way'), button:has-text('One-way')").first
                if await ow.count() > 0:
                    await ow.click(timeout=3000)
                    await asyncio.sleep(0.5)
            except Exception:
                pass

            # Fill origin
            ok = await self._fill_airport(page, "From", req.origin)
            if not ok:
                return avail_data, blocked
            await asyncio.sleep(0.8)

            # Fill destination
            ok = await self._fill_airport(page, "To", req.destination)
            if not ok:
                return avail_data, blocked
            await asyncio.sleep(0.8)

            # Fill date
            dt = _to_datetime(req.date_from)
            date_input = page.locator(
                "input[name*='depart'], input[aria-label*='Depart'], "
                "input[placeholder*='Depart']"
            ).first
            if await date_input.count() > 0:
                try:
                    await date_input.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    await date_input.fill(dt.strftime("%Y-%m-%d"))
                    await asyncio.sleep(0.5)
                except Exception:
                    pass

            # Click search
            try:
                btn = page.locator(
                    "button:has-text('Search flights'), "
                    "button:has-text('Search'), "
                    "button[type='submit']"
                ).first
                if await btn.count() > 0:
                    await btn.click(timeout=5000)
                    logger.info("WS: clicked search")
            except Exception:
                pass

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            deadline = time.monotonic() + remaining
            while not avail_data and not blocked and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.warning("WS: form search error: %s", e)

        return avail_data, blocked

    async def _fill_airport(self, page, label: str, iata: str) -> bool:
        """Fill an airport typeahead."""
        try:
            field = page.get_by_role("textbox", name=re.compile(label, re.I)).first
            if await field.count() == 0:
                field = page.locator(
                    f"input[aria-label*='{label}'], input[placeholder*='{label}']"
                ).first
            if await field.count() == 0:
                return False

            await field.click(timeout=3000)
            await asyncio.sleep(0.3)
            await field.fill("")
            await field.type(iata, delay=80)
            await asyncio.sleep(2.0)

            opt = page.locator("[role='option']").first
            if await opt.count() > 0:
                await opt.click(timeout=3000)
                return True

            await field.press("ArrowDown")
            await asyncio.sleep(0.2)
            await field.press("Enter")
            return True
        except Exception as e:
            logger.warning("WS: airport fill '%s' error: %s", label, e)
            return False

    # ------------------------------------------------------------------
    # Detection + parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _looks_like_flights(data: dict) -> bool:
        s = json.dumps(data)[:5000].lower()
        flight_sigs = (
            "flightoffer", "flightresult", "bounddetail", "segment",
            "departuretime", "departuredatetime", "airbound",
            "journeypair", "itinerar", "flightleg",
        )
        price_sigs = ('"price"', '"amount"', '"total"', '"fare"')
        return any(sig in s for sig in flight_sigs) and any(sig in s for sig in price_sigs)

    def _parse_flights(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight data from various WestJet API formats."""
        offers: list[FlightOffer] = []

        inner = data
        for key in ("data", "response", "result"):
            if key in inner and isinstance(inner[key], (dict, list)):
                inner = inner[key] if isinstance(inner[key], dict) else {"items": inner[key]}

        currency = self._get_currency(data, req)

        # Look for flight arrays
        flight_list = None
        for key in (
            "flightOffers", "flights", "offers", "results",
            "flightResults", "boundGroups", "items",
            "journeyPairs", "originDestinationOptionList",
        ):
            candidate = inner.get(key)
            if isinstance(candidate, list) and len(candidate) > 0:
                flight_list = candidate
                break

        if not flight_list:
            # Try to find any list of dicts with price data
            for v in inner.values():
                if isinstance(v, list) and len(v) >= 2:
                    if isinstance(v[0], dict) and self._get_price(v[0]):
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
            ) if len(segments) > 0 and segments[-1].arrival > segments[0].departure else 0

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur,
                stopovers=max(len(segments) - 1, 0),
            )

            offer_key = f"ws_{req.origin}_{req.destination}_{segments[0].departure.isoformat()}_{price}"
            offer_id = hashlib.md5(offer_key.encode()).hexdigest()[:12]
            all_airlines = list({s.airline for s in segments})

            offers.append(FlightOffer(
                id=f"ws_{offer_id}",
                price=round(price, 2),
                currency=currency,
                outbound=route,
                airlines=[("WestJet" if a == "WS" else a) for a in all_airlines],
                owner_airline="WS",
                booking_url=self._user_url(req),
                is_locked=False,
                source="westjet_direct",
                source_tier="free",
            ))

        return offers

    def _extract_segments(self, flight: dict, req: FlightSearchRequest) -> list[FlightSegment]:
        """Extract segments from a flight object."""
        segments: list[FlightSegment] = []

        seg_list = None
        for key in ("segments", "segmentList", "legs", "flightSegments", "boundDetails"):
            candidate = flight.get(key)
            if isinstance(candidate, list) and len(candidate) > 0:
                seg_list = candidate
                break
            if isinstance(candidate, dict):
                inner_segs = candidate.get("segments", candidate.get("legs", []))
                if isinstance(inner_segs, list) and len(inner_segs) > 0:
                    seg_list = inner_segs
                    break

        if not seg_list:
            # Maybe the flight object itself is a single segment
            dep = flight.get("departureDateTime") or flight.get("departureTime") or ""
            if dep:
                seg_list = [flight]

        if not seg_list:
            return segments

        for seg in seg_list:
            if not isinstance(seg, dict):
                continue

            dep_str = (
                seg.get("departureDateTime") or seg.get("departureTime")
                or seg.get("departure") or seg.get("localDepartureDateTime") or ""
            )
            arr_str = (
                seg.get("arrivalDateTime") or seg.get("arrivalTime")
                or seg.get("arrival") or seg.get("localArrivalDateTime") or ""
            )
            origin = (
                seg.get("departureAirportCode") or seg.get("origin")
                or seg.get("from") or seg.get("departureStation") or req.origin
            )
            dest = (
                seg.get("arrivalAirportCode") or seg.get("destination")
                or seg.get("to") or seg.get("arrivalStation") or req.destination
            )
            carrier = (
                seg.get("airlineCode") or seg.get("carrierCode")
                or seg.get("operatingCarrier") or seg.get("airline") or "WS"
            )
            fno = seg.get("flightNumber") or seg.get("flightNo") or ""

            dep_dt = _parse_dt(dep_str) if dep_str else _to_datetime(req.date_from)
            arr_dt = _parse_dt(arr_str) if arr_str else dep_dt + timedelta(hours=3)
            dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

            segments.append(FlightSegment(
                airline=carrier,
                airline_name="WestJet" if carrier == "WS" else carrier,
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
        for key in (
            "price", "totalPrice", "startingPrice", "amount",
            "fareAmount", "total", "adultFare", "baseFare", "displayPrice",
        ):
            val = obj.get(key)
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
            if isinstance(val, dict):
                for ik in ("amount", "total", "value", "grossPrice", "displayAmount"):
                    iv = val.get(ik)
                    if isinstance(iv, (int, float)) and iv > 0:
                        return float(iv)
            if isinstance(val, str):
                try:
                    fv = float(re.sub(r"[^\d.]", "", val))
                    if fv > 0:
                        return fv
                except (ValueError, TypeError):
                    pass
        return None

    @staticmethod
    def _get_currency(data: dict, req: FlightSearchRequest) -> str:
        for key in ("currencyCode", "currency", "originalCurrency"):
            val = data.get(key)
            if isinstance(val, str) and len(val) == 3:
                return val
        for v in data.values():
            if isinstance(v, dict):
                for key in ("currencyCode", "currency"):
                    val = v.get(key)
                    if isinstance(val, str) and len(val) == 3:
                        return val
        return "CAD"

    @staticmethod
    def _user_url(req: FlightSearchRequest) -> str:
        dt = _to_datetime(req.date_from)
        return (
            f"https://www.westjet.com/en-ca/flights/search"
            f"?orig={req.origin}&dest={req.destination}"
            f"&depart={dt.strftime('%Y-%m-%d')}"
            f"&adt={req.adults or 1}&type=one-way"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"ws{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "CAD",
            offers=[],
            total_results=0,
        )
