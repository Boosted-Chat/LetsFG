"""
ZIPAIR BFF API scraper — direct curl_cffi, no auth needed.

ZIPAIR Tokyo (IATA: ZG) is a Japanese LCC (JAL group).
BFF API: bff.zipair.net — completely open, no cookies/tokens required.

Strategy (updated Mar 2026):
1. GET bff.zipair.net/v2/flights with curl_cffi (impersonate Chrome)
2. Parse JSON → FlightOffer objects
3. Playwright fallback if API fails

API details:
- GET https://bff.zipair.net/v2/flights
- Params: routes=NRT,ICN&departureDateFrom=YYYY-MM-DD&adult=1&childA=0&childB=0
           &childC=0&infant=0&currency=USD&language=en
- No auth required — BFF is fully open
- Response: {flights: [[{origin, destination, logicalFlightId,
    scheduledDepartureArrivalDateTime: {departureDate, arrivalDate},
    flightTime (min), flightNumber, carrierCode,
    fares: [{fareBasisCode, passengerType, amounts: {originalAmount},
             cabinCode (STANDARD|ZIPFULLFLAT), availableSeat,
             taxes: [{id, amount}]}]}]],
  taxDetails: [{id, taxCode, name}]}
- Price = float(originalAmount) + sum(float(tax.amount))
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import re
import time
from datetime import datetime
from typing import Any, Optional

from curl_cffi import requests as creq

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_BFF_URL = "https://bff.zipair.net/v2/flights"

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
    {"width": 1600, "height": 900},
]
_LOCALES = ["en-US", "en-GB", "en-JP", "ja-JP"]
_TIMEZONES = [
    "Asia/Tokyo", "Asia/Seoul", "America/Los_Angeles",
    "Asia/Bangkok", "Pacific/Honolulu",
]

# ------- Playwright fallback globals -------
_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    global _pw_instance, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from connectors.browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("Zipair: browser launched")
        return _browser


class ZipairConnectorClient:
    """ZIPAIR scraper — direct BFF API via curl_cffi, Playwright fallback."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ── primary: direct API (no auth) ──────────────────────────

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        try:
            offers = await asyncio.get_event_loop().run_in_executor(
                None, self._api_search_sync, req,
            )
            if offers is not None:
                elapsed = time.monotonic() - t0
                return self._build_response(offers, req, elapsed)
        except Exception as e:
            logger.warning("Zipair: API search failed, falling back to Playwright: %s", e)

        return await self._playwright_fallback(req, t0)

    def _api_search_sync(self, req: FlightSearchRequest) -> list[FlightOffer] | None:
        dep_str = req.date_from.strftime("%Y-%m-%d")
        currency = req.currency if req.currency != "EUR" else "USD"
        params = {
            "routes": f"{req.origin},{req.destination}",
            "departureDateFrom": dep_str,
            "adult": str(req.adults),
            "childA": "0", "childB": "0", "childC": "0", "infant": "0",
            "currency": currency,
            "language": "en",
        }

        logger.info("Zipair: API %s→%s on %s", req.origin, req.destination, dep_str)
        sess = creq.Session(impersonate="chrome124")
        try:
            r = sess.get(_BFF_URL, params=params, headers={
                "Accept": "application/json",
            }, timeout=15)
        except Exception as e:
            logger.warning("Zipair: API request failed: %s", e)
            return None

        if r.status_code == 400:
            # 400 = invalid route (not served by ZIPAIR) — no fallback needed
            logger.info("Zipair: route %s→%s not served (400)", req.origin, req.destination)
            return []

        if r.status_code != 200:
            logger.warning("Zipair: API returned %d", r.status_code)
            return None

        try:
            data = r.json()
        except Exception:
            return None

        return self._parse_flights(data, req, currency)

    # ── Playwright fallback ────────────────────────────────────

    async def _playwright_fallback(self, req: FlightSearchRequest, t0: float) -> FlightSearchResponse:
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

            logger.info("Zipair: Playwright fallback for %s→%s", req.origin, req.destination)
            await page.goto("https://www.zipair.net/en",
                            wait_until="load", timeout=int(self.timeout * 1000))
            await asyncio.sleep(3.0)

            # Dismiss cookie consent
            for label in ["Agree", "Accept", "OK", "Got it"]:
                try:
                    btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                    if await btn.count() > 0:
                        await btn.first.click(timeout=3000)
                        await asyncio.sleep(0.5)
                        break
                except Exception:
                    continue

            try:
                await page.evaluate(
                    "() => window.$nuxt.$store.dispatch('token/createToken')"
                )
                await asyncio.sleep(2.0)
            except Exception as e:
                logger.warning("Zipair: token/createToken failed: %s", e)

            dep_str = req.date_from.strftime("%Y-%m-%d")
            currency = req.currency if req.currency != "EUR" else "USD"
            api_url = (
                f"https://bff.zipair.net/v2/flights?"
                f"routes={req.origin},{req.destination}"
                f"&departureDateFrom={dep_str}"
                f"&adult={req.adults}"
                f"&childA=0&childB=0&childC=0&infant=0"
                f"&currency={currency}&language=en"
            )

            result = await page.evaluate("""async (url) => {
                try {
                    const resp = await fetch(url, {
                        method: 'GET',
                        headers: {'Accept': 'application/json'},
                        credentials: 'include',
                    });
                    if (!resp.ok) return {error: `HTTP ${resp.status}`, status: resp.status};
                    return await resp.json();
                } catch(e) {
                    return {error: e.message};
                }
            }""", api_url)

            if not result or result.get("error"):
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_flights(result, req, currency)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Zipair Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    def _parse_flights(self, data: dict, req: FlightSearchRequest, currency: str) -> list[FlightOffer]:
        raw_flights = data.get("flights", [])
        if not isinstance(raw_flights, list):
            return []

        target_date = req.date_from.strftime("%Y-%m-%d")
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for itinerary in raw_flights:
            if not isinstance(itinerary, list) or not itinerary:
                continue

            # Check if departure date matches requested date
            first_seg = itinerary[0]
            dep_dt_str = first_seg.get("scheduledDepartureArrivalDateTime", {}).get("departureDate", "")
            if not dep_dt_str.startswith(target_date):
                continue

            # Build segments
            segments: list[FlightSegment] = []
            all_fares: list[dict] = []
            for seg in itinerary:
                segments.append(self._build_segment(seg))
                all_fares.extend(seg.get("fares", []))

            if not segments or not all_fares:
                continue

            # Find cheapest adult fare (base + taxes)
            best_price, best_cabin = self._cheapest_fare(all_fares)
            if best_price is None or best_price <= 0:
                continue

            total_dur = 0
            if segments[0].departure and segments[-1].arrival:
                total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
            if total_dur == 0:
                # Fallback: sum flightTime (minutes)
                total_dur = sum(s.get("flightTime", 0) for s in itinerary) * 60

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=max(total_dur, 0),
                stopovers=max(len(segments) - 1, 0),
            )

            flight_id = first_seg.get("logicalFlightId", time.monotonic())
            offers.append(FlightOffer(
                id=f"zg_{hashlib.md5(str(flight_id).encode()).hexdigest()[:12]}",
                price=round(best_price, 2),
                currency=currency,
                price_formatted=f"{best_price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["ZIPAIR"],
                owner_airline="ZG",
                booking_url=booking_url,
                is_locked=False,
                source="zipair_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _cheapest_fare(fares: list[dict]) -> tuple:
        best_price = None
        best_cabin = "STANDARD"

        for fare in fares:
            if fare.get("passengerType") != "adult":
                continue
            if fare.get("availableSeat", 0) <= 0:
                continue

            base = fare.get("amounts", {}).get("originalAmount")
            if base is None:
                continue
            try:
                base_f = float(base)
            except (TypeError, ValueError):
                continue

            tax_total = sum(
                float(t.get("amount", 0))
                for t in fare.get("taxes", [])
            )
            total = base_f + tax_total

            if total > 0 and (best_price is None or total < best_price):
                best_price = total
                best_cabin = fare.get("cabinCode", "STANDARD")

        return best_price, best_cabin

    def _build_segment(self, seg: dict) -> FlightSegment:
        times = seg.get("scheduledDepartureArrivalDateTime", {})
        dep_str = times.get("departureDate", "")
        arr_str = times.get("arrivalDate", "")

        carrier = seg.get("carrierCode", "ZG")
        flight_no_raw = str(seg.get("flightNumber", ""))
        flight_no = f"{carrier}{flight_no_raw}" if flight_no_raw and not flight_no_raw.startswith(carrier) else flight_no_raw

        return FlightSegment(
            airline=carrier,
            airline_name="ZIPAIR",
            flight_no=flight_no,
            origin=seg.get("origin", ""),
            destination=seg.get("destination", ""),
            departure=self._parse_dt(dep_str),
            arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Zipair %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"zipair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.zipair.net/en/booking/flight?"
            f"origin={req.origin}&destination={req.destination}"
            f"&departureDate={dep}&adult={req.adults}&tripType=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"zipair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
