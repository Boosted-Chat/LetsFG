"""
AirAsia Playwright scraper -- navigates to airasia.com and searches flights.

AirAsia (IATA: AK) is a Malaysian super-LCC operating across Asia-Pacific.
Uses a Navitaire-based booking engine. Heavy Akamai/Datadome bot protection.

Strategy:
1. Navigate to airasia.com/en/gb homepage (English version)
2. Dismiss cookie consent banner
3. Fill search form (origin, destination, date, one-way)
4. Intercept API responses (Navitaire availability/search/fares endpoints)
5. Parse results -> FlightOffers

Homepage observations (Mar 2026):
- Cookie banner: "We use cookies..." dismiss via button then JS removal
- Search form: "From" / "To" / "Depart" / "Return" fields
- Trip type: "Round-trip" default -- need to switch to "One-way"
- Search button: "Search Flights" (accessible button)
- Autocomplete: Dropdown with airport suggestions after typing IATA
- API: Navitaire-style calls across multiple endpoint patterns:
    aggregated-results, availability, flights/search, fares, offers,
    /api/nsk, navi, nskts, booking/search, _next/data, bifrost
- Response: JSON with searchResults.trips[].flightsList[] or pageProps envelope
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import subprocess
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

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-MY", "en-SG"]
_TIMEZONES = [
    "Asia/Kuala_Lumpur", "Asia/Singapore", "Asia/Bangkok",
    "Asia/Jakarta", "Asia/Manila",
]

# ── Shared browser singleton via CDP ────────────────────────────────────
_CDP_PORT = 9457
_chrome_proc = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Connect to a real Chrome instance via CDP (launched once, reused)."""
    global _chrome_proc, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright

        from boostedtravel.connectors.browser import find_chrome, stealth_args, stealth_popen_kwargs
        chrome_path = find_chrome()
        user_data = os.path.join(os.environ.get("TEMP", "/tmp"), "chrome-cdp-airasia")
        _chrome_proc = subprocess.Popen([
            chrome_path,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={user_data}",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            *stealth_args(),
        ], **stealth_popen_kwargs())
        await asyncio.sleep(1.5)

        pw = await async_playwright().start()
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
        logger.info("AirAsia: Connected to real Chrome via CDP (port %d)", _CDP_PORT)
        return _browser


class AirAsiaConnectorClient:
    """AirAsia Playwright scraper -- homepage form search + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
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

            try:
                cdp = await context.new_cdp_session(page)
                await cdp.send("Network.setCacheDisabled", {"cacheDisabled": True})
            except Exception:
                pass

            captured_data: dict = {}
            api_event = asyncio.Event()

            # Navitaire / AirAsia API endpoint patterns (aligned with cebupacific/scoot)
            _URL_PATTERNS = (
                "aggregated-results",
                "availability",
                "search/flights",
                "flights/search",
                "flightsearch",
                "low-fare",
                "lowest-fare",
                "fares",
                "offers",
                "/api/search",
                "/api/nsk",
                "navi",
                "nskts",
                "booking/search",
                "air-bounds",
                "search-results",
                "_next/data",
                "bifrost",
            )
            _FLIGHT_KEYS = frozenset({
                "searchResults", "trips", "flights", "journeys",
                "outboundFlights", "availability",
                "lowFareAvailability", "outbound", "fares", "flightsList",
                "offers", "airBoundGroups",
            })

            async def on_response(response):
                try:
                    if response.status != 200:
                        return
                    url = response.url.lower()
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    if not any(k in url for k in _URL_PATTERNS):
                        return
                    data = await response.json()
                    if not data or not isinstance(data, (dict, list)):
                        return
                    # Unwrap Next.js pageProps envelope if present
                    if isinstance(data, dict) and "pageProps" in data:
                        inner = data["pageProps"]
                        if isinstance(inner, dict):
                            data = inner.get("aggregatorResponse") or inner
                    # Accept lists directly (array of flights)
                    if isinstance(data, list):
                        captured_data["json"] = data
                        captured_data["url"] = response.url
                        api_event.set()
                        logger.info("AirAsia: captured flight data from %s (status=%d)", response.url[:120], response.status)
                        return
                    # Accept dicts that contain flight-related keys
                    if any(k in data for k in _FLIGHT_KEYS):
                        captured_data["json"] = data
                        captured_data["url"] = response.url
                        api_event.set()
                        logger.info("AirAsia: captured flight data from %s (status=%d)", response.url[:120], response.status)
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("AirAsia: loading homepage for %s->%s", req.origin, req.destination)
            await page.goto(
                "https://www.airasia.com/en/gb",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3.0)

            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await self._dismiss_cookies(page)

            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            ok = await self._fill_airport_field(page, "From", req.origin, 0)
            if not ok:
                logger.warning("AirAsia: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_airport_field(page, "To", req.destination, 1)
            if not ok:
                logger.warning("AirAsia: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("AirAsia: date fill failed")
                return self._empty(req)
            await asyncio.sleep(0.3)

            await self._click_search(page)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("AirAsia: timed out waiting for API response")
                offers = await self._extract_from_dom(page, req)
                if offers:
                    return self._build_response(offers, req, time.monotonic() - t0)
                return self._empty(req)

            data = captured_data.get("json", {})
            if not data:
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_response(data, req)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("AirAsia Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    async def _dismiss_cookies(self, page) -> None:
        for label in [
            "Accept all cookies", "Accept All", "Accept", "I agree",
            "Got it", "OK", "Close", "Dismiss", "Agree",
        ]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="Cookie"], [id*="Cookie"], [class*="onetrust"], [id*="onetrust"], ' +
                    '[class*="modal-overlay"], [class*="popup"], [id*="popup"], ' +
                    '[class*="privacy"], [id*="privacy"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        """Click trip-type dropdown (#home_triptype) and select 'One-way'."""
        try:
            # Close calendar overlay if open (it intercepts pointer events)
            close_btn = page.locator("[class*='CloseCalendar']")
            if await close_btn.count() > 0:
                await close_btn.first.click(timeout=1000)
                await asyncio.sleep(0.3)
        except Exception:
            pass
        try:
            await page.click("#home_triptype", timeout=3000)
            await asyncio.sleep(0.5)
            # Find "One-way" text inside the dropdown
            ow = page.locator("#home_triptype").get_by_text("One-way", exact=True)
            if await ow.count() > 0:
                await ow.first.click(timeout=3000)
                logger.info("AirAsia: set one-way via dropdown")
                return
            # Fallback: any element below triptype with One-way text
            ow = page.locator("#home_triptype p, #home_triptype li, #home_triptype div").filter(
                has_text=re.compile(r"One.?way", re.IGNORECASE)
            ).last
            if await ow.count() > 0:
                await ow.click(timeout=3000)
                return
        except Exception as e:
            logger.debug("AirAsia: one-way toggle error: %s", e)

    async def _fill_airport_field(self, page, label: str, iata: str, index: int) -> bool:
        """Fill origin (index=0) or destination (index=1) using specific AirAsia input IDs."""
        try:
            if index == 0:
                field = page.locator("input#flight-place-picker")
            else:
                field = page.locator("input#home_flyingfrom")
            await field.click(timeout=3000)
            await asyncio.sleep(0.3)
            await field.fill("")
            await asyncio.sleep(0.2)
            await field.fill(iata)
            await asyncio.sleep(2.5)
            # Click autocomplete suggestion -- LI with Dropdown__Option class
            suggestion = page.locator("li[class*='Dropdown__Option']").filter(
                has_text=re.compile(rf"{re.escape(iata)}", re.IGNORECASE)
            ).first
            if await suggestion.count() > 0:
                await suggestion.click(timeout=3000)
                logger.info("AirAsia: selected %s from autocomplete for %s", iata, label)
                return True
            # Fallback: any visible LI containing the IATA code
            suggestion = page.locator("li:visible").filter(
                has_text=re.compile(rf"{re.escape(iata)}", re.IGNORECASE)
            ).first
            if await suggestion.count() > 0:
                await suggestion.click(timeout=3000)
                return True
            await page.keyboard.press("Enter")
            return True
        except Exception as e:
            logger.debug("AirAsia: %s field error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Open AirAsia calendar via #departclick-handle, navigate months, click day div.

        Calendar day cells use id='div-YYYY-M-D' where M is 0-indexed (Jan=0).
        Month headers are in calendarInstance__CalendarHeaderItem divs.
        Navigation arrows: #lefticon (prev) / #righticon (next).
        """
        target = req.date_from
        try:
            await page.click("#departclick-handle", timeout=3000)
            await asyncio.sleep(1.0)

            target_my = target.strftime("%B %Y")  # e.g. "April 2026"
            for _ in range(12):
                headers = await page.locator(
                    "[class*='CalendarHeaderItem']"
                ).all_text_contents()
                if any(target_my in h for h in headers):
                    break
                try:
                    await page.click("#righticon", timeout=2000)
                    await asyncio.sleep(0.5)
                except Exception:
                    break

            # Calendar uses 0-indexed months: Jan=0, Feb=1, Mar=2 ...
            month_0 = target.month - 1
            day_id = f"div-{target.year}-{month_0}-{target.day}"
            await page.click(f"#{day_id}", timeout=3000)
            await asyncio.sleep(0.5)
            logger.info("AirAsia: selected date %s via calendar", target.strftime("%Y-%m-%d"))

            # Close the calendar -- it stays open after date pick and blocks search
            try:
                close = page.locator("[class*='CloseCalendar']")
                if await close.count() > 0:
                    await close.first.click(timeout=2000)
                    await asyncio.sleep(0.3)
            except Exception:
                try:
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.3)
                except Exception:
                    pass

            return True
        except Exception as e:
            logger.warning("AirAsia: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        """Click search -- AirAsia uses <a id='home_Search'> not a <button>."""
        # Ensure calendar overlay is closed first
        try:
            close = page.locator("[class*='CloseCalendar']")
            if await close.count() > 0:
                await close.first.click(timeout=1000)
                await asyncio.sleep(0.3)
        except Exception:
            pass
        try:
            await page.click("a#home_Search", timeout=5000)
            logger.info("AirAsia: clicked search")
            return
        except Exception:
            pass
        # Fallback: try by aria-label
        try:
            link = page.locator("[aria-label*='Search Flights' i]")
            if await link.count() > 0:
                await link.first.click(timeout=5000)
                return
        except Exception:
            pass
        # Last resort
        try:
            await page.locator("a:has-text('Search'), button:has-text('Search')").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: extract from __NEXT_DATA__ (AirAsia uses Next.js SSR)."""
        try:
            await asyncio.sleep(5)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) {
                    const pr = window.__NEXT_DATA__?.props?.pageProps;
                    if (pr?.aggregatorResponse) return pr.aggregatorResponse;
                    return window.__NEXT_DATA__;
                }
                if (window.__NUXT__) return window.__NUXT__;
                const scripts = document.querySelectorAll('script[type="application/json"], script[id="__NEXT_DATA__"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.journeys || d.fares || d.availability || d.searchResults)) return d;
                        if (d?.props?.pageProps?.aggregatorResponse) return d.props.pageProps.aggregatorResponse;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
        return []

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if isinstance(data, list):
            data = {"flights": data}
        # Unwrap Next.js pageProps / aggregatorResponse envelope
        if isinstance(data, dict) and "pageProps" in data:
            inner = data["pageProps"]
            if isinstance(inner, dict):
                data = inner.get("aggregatorResponse") or inner
        currency = req.currency
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # AirAsia aggregated-results: searchResults.trips[].flightsList[]
        search_results = data.get("searchResults")
        if isinstance(search_results, dict):
            trips = search_results.get("trips", [])
            if isinstance(trips, list):
                for trip in trips:
                    for flight in trip.get("flightsList", []):
                        offer = self._parse_airasia_flight(flight, currency, req, booking_url)
                        if offer:
                            offers.append(offer)
        if offers:
            return offers

        # Fallback: generic format
        flights_raw = (
            data.get("outboundFlights")
            or data.get("outbound")
            or data.get("journeys")
            or data.get("flights")
            or data.get("availability", {}).get("trips", [])
            or data.get("data", {}).get("flights", [])
            or data.get("data", {}).get("journeys", [])
            or data.get("lowFareAvailability", {}).get("outboundOptions", [])
            or []
        )
        if isinstance(flights_raw, dict):
            flights_raw = flights_raw.get("outbound", []) or flights_raw.get("journeys", [])
        if not isinstance(flights_raw, list):
            flights_raw = []

        for flight in flights_raw:
            offer = self._parse_single_flight(flight, currency, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    def _parse_airasia_flight(self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        """Parse a single flight from AirAsia's aggregated-results format."""
        # Price: prefer convertedPrice (user currency USD), else price (local MYR)
        price = None
        flight_currency = currency
        converted = flight.get("convertedPrice")
        if converted is not None:
            try:
                price = float(converted)
                flight_currency = flight.get("userCurrencyCode") or currency
            except (TypeError, ValueError):
                pass
        if not price or price <= 0:
            try:
                price = float(flight.get("price", 0))
                flight_currency = flight.get("currencyCode") or currency
            except (TypeError, ValueError):
                pass
        if not price or price <= 0:
            return None

        details = flight.get("flightDetails", {})
        designator = details.get("designator", {})
        segments_raw = details.get("segments", [])

        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                seg_des = seg.get("designator", {})
                segments.append(FlightSegment(
                    airline=seg.get("carrierCode") or seg.get("airline") or "AK",
                    airline_name="AirAsia",
                    flight_no=seg.get("marketingFlightNo") or seg.get("flightNumber") or "",
                    origin=seg_des.get("departureStation", req.origin),
                    destination=seg_des.get("arrivalStation", req.destination),
                    departure=self._parse_dt(seg_des.get("departureTime", "")),
                    arrival=self._parse_dt(seg_des.get("arrivalTime", "")),
                    cabin_class="M",
                ))
        else:
            segments.append(FlightSegment(
                airline="AK", airline_name="AirAsia", flight_no="",
                origin=designator.get("departureStation", req.origin),
                destination=designator.get("arrivalStation", req.destination),
                departure=self._parse_dt(designator.get("departureTime", "")),
                arrival=self._parse_dt(designator.get("arrivalTime", "")),
                cabin_class="M",
            ))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = flight.get("tripId") or f"{designator.get('departureTime', '')}_{time.monotonic()}"
        return FlightOffer(
            id=f"ak_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency=flight_currency,
            price_formatted=f"{price:.2f} {flight_currency}",
            outbound=route, inbound=None,
            airlines=["AirAsia"], owner_airline="AK",
            booking_url=booking_url, is_locked=False,
            source="airasia_direct", source_tier="free",
        )

    def _parse_single_flight(self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        best_price = self._extract_best_price(flight)
        if best_price is None or best_price <= 0:
            return None
        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination))
        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
        route = FlightRoute(segments=segments, total_duration_seconds=max(total_dur, 0), stopovers=max(len(segments) - 1, 0))
        flight_key = flight.get("journeyKey") or flight.get("id") or f"{flight.get('departureDate', '')}_{time.monotonic()}"
        return FlightOffer(
            id=f"ak_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=["AirAsia"], owner_airline="AK",
            booking_url=booking_url, is_locked=False,
            source="airasia_direct", source_tier="free",
        )

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareProducts") or flight.get("bundles") or flight.get("fareBundles") or []
        best = float("inf")
        for fare in fares:
            if isinstance(fare, dict):
                for key in ["price", "amount", "totalPrice", "basePrice", "fareAmount", "totalAmount"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("value")
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        for key in ["price", "lowestFare", "totalPrice", "farePrice", "amount", "lowestPrice"]:
            p = flight.get(key)
            if p is not None:
                try:
                    v = float(p) if not isinstance(p, dict) else float(p.get("amount", 0))
                    if 0 < v < best:
                        best = v
                except (TypeError, ValueError):
                    pass
        return best if best < float("inf") else None

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = seg.get("departureDateTime") or seg.get("departure") or seg.get("departureDate") or seg.get("std") or ""
        arr_str = seg.get("arrivalDateTime") or seg.get("arrival") or seg.get("arrivalDate") or seg.get("sta") or ""
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("number") or "").replace(" ", "")
        origin = seg.get("origin") or seg.get("departureStation") or seg.get("departureAirport") or default_origin
        destination = seg.get("destination") or seg.get("arrivalStation") or seg.get("arrivalAirport") or default_dest
        carrier = seg.get("carrierCode") or seg.get("carrier") or seg.get("airline") or "AK"
        return FlightSegment(
            airline=carrier, airline_name="AirAsia", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("AirAsia %s->%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"airasia{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
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
            f"https://www.airasia.com/flights/search?origin={req.origin}"
            f"&destination={req.destination}&departDate={dep}&pax={req.adults}&tripType=O"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"airasia{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
