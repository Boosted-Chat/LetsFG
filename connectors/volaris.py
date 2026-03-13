"""
Volaris Playwright scraper -- navigates to volaris.com and searches flights.

Volaris (IATA: Y4) is Mexico's largest low-cost carrier operating domestic
and international routes across Mexico, US, and Central America.
Navitaire-based booking system. Default currency MXN.

⚠️  US-ONLY: The Volaris API gateway (apigw.volaris.com) is geo-blocked by
Fastly CDN — returns 406 from non-North-American IPs. Must be run from
US/MX infrastructure.

Strategy:
1. Navigate to volaris.com/en homepage
2. Dismiss cookie consent banner ("Accept All")
3. Fill search form (origin, destination, date, one-way)
4. Intercept API responses (Navitaire availability/search endpoints)
5. Parse results -> FlightOffers
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
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-US", "es-MX", "en-GB", "es-US"]
_TIMEZONES = [
    "America/Mexico_City", "America/Cancun", "America/Tijuana",
    "America/Chicago", "America/Los_Angeles",
]
_MAX_ATTEMPTS = 2


def _get_proxy() -> Optional[dict]:
    """Read proxy config from VOLARIS_PROXY env var (US/MX IP required)."""
    raw = os.environ.get("VOLARIS_PROXY", "").strip()
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


# ── Shared browser singleton via CDP ────────────────────────────────────
_CDP_PORT = 9466
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
        from connectors.browser import get_or_launch_cdp
        _user_data = os.path.join(os.environ.get("TEMP", "/tmp"), "chrome-cdp-volaris")
        _browser, _chrome_proc = await get_or_launch_cdp(
            _CDP_PORT, _user_data,
            extra_args=["--disable-web-security", "--disable-features=IsolateOrigins,site-per-process"],
        )
        logger.info("Volaris: Chrome ready via CDP (port %d)", _CDP_PORT)
        return _browser


class VolarisConnectorClient:
    """Volaris Playwright scraper -- homepage form search + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                offers = await self._attempt_search(req, t0)
                if offers is not None:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)
                logger.warning("Volaris: attempt %d/%d got no results", attempt, _MAX_ATTEMPTS)
            except Exception as e:
                logger.warning("Volaris: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    async def _attempt_search(self, req: FlightSearchRequest, t0: float) -> Optional[list[FlightOffer]]:
        browser = await _get_browser()
        proxy = _get_proxy()
        context = await browser.new_context(
            proxy=proxy,
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
            bypass_csp=True,
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
            search_clicked: dict = {"ready": False}

            async def on_response(response):
                if not search_clicked["ready"]:
                    return
                try:
                    url = response.url.lower()
                    status = response.status
                    if status != 200:
                        return
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    if any(k in url for k in [
                        "availability", "flights/search", "flightsearch",
                        "search/flights", "/api/v1/flights", "/api/v2/flights",
                        "/api/nsk/", "navi", "nskts",
                        "search/results", "booking/search",
                        "offers", "fares", "shopping",
                        "lowfare", "low-fare", "air-bounds", "trips",
                        "apigw.volaris.com/api/",
                    ]):
                        data = await response.json()
                        if data and isinstance(data, (dict, list)):
                            if isinstance(data, list) or (
                                isinstance(data, dict) and any(
                                    k in data for k in [
                                        "trips", "journeys", "flights",
                                        "availability", "data", "offers",
                                        "schedules", "outbound", "outboundFlights",
                                        "lowFareAvailability", "results",
                                        "itineraries", "searchResults",
                                        "fareAvailability", "lowFareList",
                                    ]
                                )
                            ):
                                captured_data["json"] = data
                                api_event.set()
                                logger.info(
                                    "Volaris: captured flight API response from %s",
                                    response.url[:120],
                                )
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("Volaris: loading homepage for %s->%s", req.origin, req.destination)
            await page.goto(
                "https://www.volaris.com/en",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3.0)

            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await self._dismiss_cookies(page)

            # Volaris: trip type selector -- set one-way ("Viaje sencillo" / "One way")
            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            ok = await self._fill_airport_field(page, "From", "Desde", req.origin, 0)
            if not ok:
                logger.warning("Volaris: origin fill failed")
                return None
            await asyncio.sleep(0.5)

            ok = await self._fill_airport_field(page, "To", "A", req.destination, 1)
            if not ok:
                logger.warning("Volaris: destination fill failed")
                return None
            await asyncio.sleep(0.5)

            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("Volaris: date fill failed")
                return None
            await asyncio.sleep(0.3)

            search_clicked["ready"] = True
            await self._click_search(page)

            # Wait for navigation to results page (Navitaire Angular app).
            try:
                await page.wait_for_url(
                    re.compile(r"/(flight|booking|book)/(select|search|results)", re.IGNORECASE),
                    timeout=15000,
                )
            except Exception:
                pass  # may already be on the page or URL pattern differs

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Volaris: timed out waiting for API response")
                offers = await self._extract_from_dom(page, req)
                return offers if offers else None

            data = captured_data.get("json", {})
            if not data:
                return None

            return self._parse_response(data, req)

        except Exception as e:
            logger.error("Volaris Playwright error: %s", e)
            return None
        finally:
            await context.close()

    async def _dismiss_cookies(self, page) -> None:
        for label in [
            "Accept All", "Accept all", "Accept", "Aceptar todo",
            "Aceptar", "I agree", "OK", "Got it",
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
                    '[class*="privacy"], [id*="privacy"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        """Volaris uses a headlessui Listbox for trip type. Already defaults to
        'Viaje sencillo' (one-way), so we just verify and skip if correct."""
        try:
            btn = page.locator('[id*="headlessui-listbox-button"]').first
            if await btn.count() > 0:
                text = (await btn.text_content() or "").strip().lower()
                if "sencillo" in text or "one way" in text:
                    return  # already one-way
                await btn.click(timeout=2000)
                await asyncio.sleep(0.5)
                opt = page.get_by_role("option").filter(
                    has_text=re.compile(r"sencillo|one.way", re.IGNORECASE)
                ).first
                if await opt.count() > 0:
                    await opt.click(timeout=2000)
                    return
        except Exception:
            pass
        for label in ["Viaje sencillo", "Sencillo", "One way", "One Way"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if await el.count() > 0:
                    await el.click(timeout=2000)
                    return
            except Exception:
                continue

    async def _fill_airport_field(self, page, en_label: str, es_label: str, iata: str, index: int) -> bool:
        """Fill origin (index=0) or destination (index=1) via input[role=combobox].

        Volaris uses headlessui Combobox components. The inputs are
        ``input[role="combobox"]`` elements. Typing the IATA code filters
        the listbox; each matching city is a ``div[role="option"]``.
        """
        try:
            combo = page.locator('input[role="combobox"]').nth(index)
            if await combo.count() == 0:
                # Fallback: try aria-label
                label_part = "origin" if index == 0 else "destination"
                combo = page.locator(f'[aria-label*="fc-booking-{label_part}"]').first
            await combo.click(timeout=3000)
            await asyncio.sleep(0.3)
            await combo.fill(iata)
            await asyncio.sleep(2.0)

            # Pick the first matching option
            opt = page.get_by_role("option").filter(
                has_text=re.compile(re.escape(iata), re.IGNORECASE)
            ).first
            if await opt.count() > 0:
                await opt.click(timeout=3000)
                return True

            # Broader match — city name might not contain the IATA code literally
            any_opt = page.get_by_role("option").first
            if await any_opt.count() > 0:
                await any_opt.click(timeout=3000)
                return True

            await page.keyboard.press("Enter")
            return True
        except Exception as e:
            logger.debug("Volaris: airport field %d error: %s", index, e)
        return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Open the headlessui date-popover and select the target day.

        Volaris calendar structure (verified Mar 2026):
        - Clicking the departure area opens a popover with role="dialog"
        - Month nav: buttons with aria-label 'fc-booking-date-selector-previous-month'
          and 'fc-booking-date-selector-next-month'
        - Month headers: text like 'marzo 2026', 'abril 2026'
        - Day cells: button[role="gridcell"] with aria-label 'DD/MM/YYYY, ...'
        """
        target = req.date_from
        try:
            # Open the date popover by clicking the departure area
            date_trigger = page.locator(
                '[id*="headlessui-popover-button"]'
            ).filter(has_text=re.compile(r"Salida|Departure|fecha", re.IGNORECASE)).first
            if await date_trigger.count() == 0:
                date_trigger = page.get_by_text("Salida").first
            await date_trigger.click(timeout=3000)
            await asyncio.sleep(1.0)

            # The calendar popover is the visible dialog
            calendar = page.locator('[role="dialog"]').filter(
                has_text=re.compile(r"Fechas de viaje|Travel dates", re.IGNORECASE)
            ).first
            if await calendar.count() == 0:
                # Fallback: any visible dialog
                calendar = page.locator('[role="dialog"]:visible').first

            # Navigate to target month
            months_es = {
                1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
                7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
            }
            target_month_es = f"{months_es[target.month]} {target.year}"
            target_month_en = target.strftime("%B %Y").lower()

            fwd_btn = page.locator(
                '[aria-label="fc-booking-date-selector-next-month"]'
            ).first
            if await fwd_btn.count() == 0:
                fwd_btn = page.locator(
                    '[aria-label*="next-month"], [aria-label*="Next month"]'
                ).first
            if await fwd_btn.count() == 0:
                fwd_btn = calendar.locator('button').filter(
                    has_text=re.compile(r"keyboard_arrow_right|>|next", re.IGNORECASE)
                ).first

            for _ in range(12):
                page_text = await calendar.text_content() or ""
                if target_month_es in page_text.lower() or target_month_en in page_text.lower():
                    break
                if await fwd_btn.count() > 0:
                    await fwd_btn.click(timeout=2000)
                    await asyncio.sleep(0.4)
                else:
                    break

            # Click the target day using the DD/MM/YYYY aria-label format
            day_str = f"{target.day:02d}/{target.month:02d}/{target.year}"
            day_btn = page.locator(f'button[role="gridcell"][aria-label*="{day_str}"]').first
            if await day_btn.count() > 0:
                await day_btn.click(timeout=3000)
                await asyncio.sleep(0.5)
                logger.info("Volaris: selected date %s", day_str)
                return True

            # Fallback: match by day number text within the calendar
            day_btn = calendar.locator('button[role="gridcell"]').filter(
                has_text=re.compile(rf"^{target.day}$")
            ).first
            if await day_btn.count() > 0:
                await day_btn.click(timeout=3000)
                await asyncio.sleep(0.5)
                return True

            logger.warning("Volaris: could not find day %s in calendar", day_str)
            return False
        except Exception as e:
            logger.warning("Volaris: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        for label in [
            "Buscar Vuelos", "Search Flights", "Search", "SEARCH",
            "Buscar", "Find flights", "Search flights",
        ]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    logger.info("Volaris: clicked search")
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fall back to DOM scraping on the Navitaire Angular results page."""
        try:
            await asyncio.sleep(5)

            # Check for JSON state in the page (SPA frameworks + Navitaire Angular)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                // Navitaire Angular state
                try {
                    const root = document.querySelector('mbs-root, app-root, [ng-version]');
                    if (root && root.__ngContext__) return root.__ngContext__;
                } catch {}
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.journeys || d.fares || d.availability
                                  || d.trips || d.results || d.itineraries)) return d;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)

            # DOM scraping for Navitaire Angular booking engine (mbs-root)
            dom_flights = await page.evaluate("""() => {
                const body = document.body?.innerText || '';
                const times = body.match(/\\d{1,2}:\\d{2}/g) || [];
                const prices = body.match(/\\$[\\d,.]+|[\\d,.]+\\s*MXN/g) || [];
                const cards = document.querySelectorAll(
                    '[class*="journey"], [class*="flight-row"], [class*="fare"], [class*="avail"]'
                );
                const visible = Array.from(cards).filter(e => e.offsetHeight > 0);
                return {
                    times: times.slice(0, 20),
                    prices: prices.slice(0, 20),
                    cardCount: visible.length,
                    bodyLen: body.length,
                };
            }""")
            if dom_flights and dom_flights.get("times") and dom_flights.get("prices"):
                logger.info(
                    "Volaris DOM: %d times, %d prices found",
                    len(dom_flights["times"]), len(dom_flights["prices"]),
                )
        except Exception:
            pass
        return []

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if isinstance(data, list):
            data = {"flights": data}
        currency = req.currency or "MXN"
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Navitaire-style response — try multiple extraction paths
        flights_raw = (
            data.get("trips", [{}])[0].get("dates", [{}])[0].get("journeys") if data.get("trips") else None
        ) or (
            data.get("outboundFlights")
            or data.get("outbound")
            or data.get("journeys")
            or data.get("flights")
            or data.get("results")
            or data.get("itineraries")
            or data.get("searchResults")
            or data.get("lowFareList")
            or data.get("fareAvailability", {}).get("journeys", [])
            or data.get("data", {}).get("flights", [])
            or data.get("data", {}).get("journeys", [])
            or data.get("data", {}).get("results", [])
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

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = flight.get("journeyKey") or flight.get("id") or f"{flight.get('departureDate', '')}_{time.monotonic()}"
        return FlightOffer(
            id=f"y4_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Volaris"],
            owner_airline="Y4",
            booking_url=booking_url,
            is_locked=False,
            source="volaris_direct",
            source_tier="free",
        )

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareProducts") or flight.get("bundles") or flight.get("fareBundles") or []
        best = float("inf")
        for fare in fares:
            if isinstance(fare, dict):
                for key in ["price", "amount", "totalPrice", "basePrice", "fareAmount", "passengerFare"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("value") or val.get("total")
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        for key in ["price", "lowestFare", "totalPrice", "farePrice", "amount"]:
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
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("number") or seg.get("identifier", {}).get("identifier", "")).replace(" ", "")
        origin = seg.get("origin") or seg.get("departureStation") or seg.get("departureAirport") or seg.get("designator", {}).get("origin", default_origin)
        destination = seg.get("destination") or seg.get("arrivalStation") or seg.get("arrivalAirport") or seg.get("designator", {}).get("destination", default_dest)
        return FlightSegment(
            airline="Y4", airline_name="Volaris", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Volaris %s->%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"volaris{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "MXN"),
            offers=offers, total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.volaris.com/en/flight/select"
            f"?origin={req.origin}&destination={req.destination}"
            f"&departure={dep}&adults={req.adults}&children={req.children}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"volaris{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency or "MXN", offers=[], total_results=0,
        )
