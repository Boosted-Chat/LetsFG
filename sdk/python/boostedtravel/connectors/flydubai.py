"""
FlyDubai hybrid scraper — direct API first, Playwright fallback.

FlyDubai (IATA: FZ) is Dubai's low-cost carrier.
Booking engine: flights2.flydubai.com (Angular SPA).

Hybrid strategy (Mar 2026):
1. Try direct HTTP to flights2.flydubai.com/api/flights/7 (calendar API)
   - Uses curl_cffi with Chrome TLS fingerprint to bypass Akamai
   - Returns 7-day calendar with lowest fares per day (~1s)
   - No browser needed
2. If direct API fails → fall back to full Playwright browser flow
   - Navigate to flydubai.com/en/, fill form, intercept API response

API details (discovered Mar 2026):
  POST https://flights2.flydubai.com/api/flights/7
  Payload: {searchCriteria: [{date, dest, direction, origin, isOriginMetro, isDestMetro}],
            paxInfo: {adultCount, childCount, infantCount},
            cabinClass: "Economy", variant: "1"}
  Date format: "MM/DD/YYYY 12:00 AM"
  Response: {segments: [{route, origin, dest, lowestAdultFarePerPax, currencyCode, departureDate, ...}]}
  /api/flights/1 (detailed) is WAF-blocked; /api/flights/7 (calendar) is not.
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

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import stealth_args

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
    {"width": 1600, "height": 900},
]
_LOCALES = ["en-GB", "en-US", "en-AE", "en-AU"]
_TIMEZONES = [
    "Asia/Dubai", "Asia/Riyadh", "Europe/London",
    "Europe/Berlin", "Asia/Kolkata",
]

_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Shared headed Chromium (launched once, reused)."""
    global _pw_instance, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from connectors.browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("FlyDubai: browser launched")
        return _browser


class FlydubaiConnectorClient:
    """FlyDubai hybrid scraper — direct API first, Playwright fallback."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # ── Hybrid: try direct API first (no browser) ──
        try:
            result = await self._try_direct_api(req)
            if result and result.total_results > 0:
                return result
            logger.info("FlyDubai: direct API returned no results, falling back to Playwright")
        except Exception as e:
            logger.info("FlyDubai: direct API failed (%s), falling back to Playwright", e)

        # ── Fallback: full Playwright browser flow ──
        return await self._search_via_playwright(req)

    async def _try_direct_api(self, req: FlightSearchRequest) -> FlightSearchResponse | None:
        """Try direct HTTP to flights2.flydubai.com/api/flights/7 using curl_cffi."""
        from curl_cffi.requests import AsyncSession

        t0 = time.monotonic()
        date_str = req.date_from.strftime("%m/%d/%Y 12:00 AM")

        payload = {
            "promoCode": "",
            "campaignCode": "",
            "cabinClass": "Economy",
            "isDestMetro": "false",
            "isOriginMetro": "false",
            "paxInfo": {
                "adultCount": req.adults,
                "childCount": req.children or 0,
                "infantCount": req.infants or 0,
            },
            "searchCriteria": [{
                "date": date_str,
                "dest": req.destination,
                "direction": "outBound",
                "origin": req.origin,
                "isOriginMetro": True,
                "isDestMetro": False,
            }],
            "variant": "1",
        }

        async with AsyncSession(impersonate="chrome131") as session:
            r = await session.post(
                "https://flights2.flydubai.com/api/flights/7",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json, text/plain, */*",
                    "Origin": "https://flights2.flydubai.com",
                    "Referer": f"https://flights2.flydubai.com/en/results/ow/a{req.adults}c{req.children or 0}i{req.infants or 0}/{req.origin}_{req.destination}/{req.date_from.strftime('%Y%m%d')}",
                },
                json=payload,
                timeout=15,
            )

            if r.status_code != 200:
                logger.warning("FlyDubai API: HTTP %d", r.status_code)
                return None

            data = r.json()
            if not data or not data.get("segments"):
                return None

            elapsed = time.monotonic() - t0
            offers = self._parse_calendar_segments(
                data["segments"], req, self._build_booking_url(req)
            )
            if offers:
                for o in offers:
                    o.source = "flydubai_api"
                logger.info(
                    "FlyDubai %s→%s: %d offers in %.1fs (direct API)",
                    req.origin, req.destination, len(offers), elapsed,
                )
            return self._build_response(offers, req, elapsed)

    async def _search_via_playwright(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Full Playwright browser fallback."""
        t0 = time.monotonic()
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
            color_scheme=random.choice(["light", "dark", "no-preference"]),
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

            async def on_response(response):
                try:
                    url = response.url.lower()
                    ct = response.headers.get("content-type", "")
                    # FlyDubai results page calls /api/flights/7 (calendar)
                    # and /api/flights/1 (single day).  Akamai often blocks /1
                    # with 403 so capture whichever succeeds first.
                    if response.status == 200 and "json" in ct and (
                        "/api/flights/" in url
                        or "availability" in url
                        or "/api/search" in url
                        or "search/flights" in url
                        or "flights/search" in url
                        or "offers" in url
                        or "low-fare" in url
                        or "flight-search" in url
                        or "flightsearch" in url
                    ):
                        data = await response.json()
                        if data and isinstance(data, (dict, list)):
                            # Prefer /api/flights/1 (full details) over /7
                            if "/api/flights/1" in url:
                                captured_data["json"] = data
                                api_event.set()
                            elif "json" not in captured_data:
                                captured_data["json"] = data
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("FlyDubai: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.flydubai.com/en/",
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
                logger.warning("FlyDubai: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_airport_field(page, "To", req.destination, 1)
            if not ok:
                logger.warning("FlyDubai: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("FlyDubai: date fill failed")
                return self._empty(req)
            await asyncio.sleep(0.3)

            await self._click_search(page)

            # Wait for navigation to the results page (flights2.flydubai.com)
            try:
                await page.wait_for_url("**/results/**", timeout=15000)
                logger.info("FlyDubai: navigated to results page")
            except Exception:
                logger.debug("FlyDubai: results URL not detected, continuing")
            await asyncio.sleep(3.0)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("FlyDubai: timed out waiting for API response")
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
            logger.error("FlyDubai Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    async def _dismiss_cookies(self, page) -> None:
        for label in [
            "Accept All", "Accept all", "Accept", "I agree",
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
        for label in ["One-way", "One Way", "One way", "ONE WAY", "one-way"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if el and await el.count() > 0:
                    await el.click(timeout=3000)
                    return
            except Exception:
                continue
        for label in ["One-way", "One Way", "One way"]:
            try:
                radio = page.get_by_role("radio", name=re.compile(rf"{re.escape(label)}", re.IGNORECASE))
                if await radio.count() > 0:
                    await radio.first.click(timeout=2000)
                    return
            except Exception:
                continue
        try:
            toggle = page.locator("[data-testid*='one-way'], [data-testid*='oneway'], [class*='one-way']").first
            if await toggle.count() > 0:
                await toggle.click(timeout=2000)
        except Exception:
            pass

    async def _fill_airport_field(self, page, label: str, iata: str, index: int) -> bool:
        try:
            for role in ["combobox", "textbox"]:
                field = page.get_by_role(role, name=re.compile(rf"{label}", re.IGNORECASE))
                if await field.count() > 0:
                    await field.first.click(timeout=3000)
                    await asyncio.sleep(0.3)
                    await field.first.fill("")
                    await asyncio.sleep(0.2)
                    await field.first.fill(iata)
                    await asyncio.sleep(2.5)
                    for role2 in ["option", "button", "listitem", "link"]:
                        try:
                            option = page.get_by_role(role2, name=re.compile(rf"{re.escape(iata)}", re.IGNORECASE)).first
                            if await option.count() > 0:
                                await option.click(timeout=3000)
                                return True
                        except Exception:
                            continue
                    item = page.locator(
                        "[class*='suggestion'], [class*='option'], [class*='result'], "
                        "[class*='autocomplete'] li, [class*='dropdown'] li, "
                        "[class*='airport'] li, [class*='station'] li"
                    ).filter(has_text=re.compile(rf"{re.escape(iata)}", re.IGNORECASE)).first
                    if await item.count() > 0:
                        await item.click(timeout=3000)
                        return True
                    await page.keyboard.press("Enter")
                    return True
        except Exception as e:
            logger.debug("FlyDubai: %s field error: %s", label, e)
        try:
            inputs = page.locator("input[type='text'], input[type='search'], input[placeholder]")
            if await inputs.count() > index:
                field = inputs.nth(index)
                await field.click(timeout=3000)
                await field.fill("")
                await asyncio.sleep(0.2)
                await field.fill(iata)
                await asyncio.sleep(2.5)
                await page.keyboard.press("Enter")
                return True
        except Exception:
            pass
        return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill departure date using the Litepicker calendar widget.

        The calendar lives inside ``span#calendar-container`` and renders as a
        two-column scrollable grid with ``div.month-item`` containers holding
        ``div.day-item`` cells.  Month headers follow the pattern
        ``"March2026"`` (no space).  Navigation arrows are
        ``button.button-previous-month`` / ``button.button-next-month``.
        """
        target = req.date_from
        try:
            # Open the calendar by clicking the departure date input
            date_field = page.locator("#start-date")
            if await date_field.count() == 0:
                # Fallback: any date-like field
                date_field = page.locator(
                    "[class*='date'], [data-testid*='date'], [id*='date']"
                ).first
            await date_field.click(timeout=3000)
            await asyncio.sleep(1.0)

            # The Litepicker calendar renders inside #calendar-container
            cal = page.locator("#calendar-container .litepicker, .litepicker")
            if await cal.count() == 0:
                logger.warning("FlyDubai: Litepicker calendar not found")
                return False

            # Navigate to the correct month using Litepicker's own arrows.
            # Month headers are "MonthYYYY" with no space (e.g. "April2026").
            target_month_label = f"{target.strftime('%B')}{target.year}"
            for _ in range(12):
                visible_text = await cal.first.inner_text()
                if target_month_label in visible_text.replace(" ", ""):
                    break
                # Click Litepicker's forward arrow
                fwd = page.locator(
                    ".litepicker .button-next-month, "
                    ".litepicker [class*='next-month'], "
                    ".litepicker [aria-label='Next']"
                ).first
                if await fwd.count() > 0:
                    await fwd.click(timeout=2000)
                    await asyncio.sleep(0.4)
                else:
                    logger.debug("FlyDubai: no forward arrow found")
                    break

            # Click the correct day-item inside the target month.
            # Each month-item contains a header and container__days with
            # div.day-item children.  We match the day number text.
            day = str(target.day)
            clicked = await page.evaluate(
                """([targetMonthLabel, day]) => {
                    const months = document.querySelectorAll('.month-item');
                    for (const m of months) {
                        const hdr = m.querySelector('.month-item-header, [class*="month-item-name"]');
                        const headerText = (hdr ? hdr.textContent : m.textContent).replace(/\\s/g, '');
                        if (!headerText.includes(targetMonthLabel)) continue;
                        const days = m.querySelectorAll('.day-item:not(.is-locked)');
                        for (const d of days) {
                            if (d.textContent.trim() === day) {
                                d.click();
                                return true;
                            }
                        }
                    }
                    return false;
                }""",
                [target_month_label, day],
            )
            if clicked:
                await asyncio.sleep(0.5)
                return True

            # Fallback: try any visible unlocked day with matching text
            fallback = page.locator(
                ".day-item:not(.is-locked)"
            ).filter(has_text=re.compile(rf"^{day}$"))
            if await fallback.count() > 0:
                await fallback.first.click(timeout=3000)
                await asyncio.sleep(0.5)
                return True

            logger.warning("FlyDubai: day %s not found in calendar", day)
            return False
        except Exception as e:
            logger.warning("FlyDubai: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        for label in ["SEARCH", "Search", "Search Flights", "Search flights", "Find flights"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    logger.info("FlyDubai: clicked search")
                    return
            except Exception:
                continue
        for label in ["Search", "SEARCH", "Search Flights"]:
            try:
                link = page.get_by_role("link", name=re.compile(rf"{re.escape(label)}", re.IGNORECASE))
                if await link.count() > 0:
                    await link.first.click(timeout=5000)
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: extract flight data from the Angular results page DOM."""
        try:
            await asyncio.sleep(5)
            # The results page is an Angular app on flights2.flydubai.com.
            # The calendar strip (fz-calendar-tab) always renders even when
            # loading is incomplete.  Extract per-date prices from tabs.
            data = await page.evaluate("""() => {
                const result = { tabs: [], sct: [] };
                // Calendar tabs with date + price
                document.querySelectorAll('fz-calendar-tab').forEach(tab => {
                    const text = tab.textContent.replace(/\\s+/g, ' ').trim();
                    const priceEl = tab.querySelector('#lblAmount');
                    const currEl = tab.querySelector('#lblCurrency');
                    result.tabs.push({
                        text: text,
                        price: priceEl ? priceEl.textContent.trim() : null,
                        currency: currEl ? currEl.textContent.trim() : null,
                    });
                });
                // Station code / time (if flight details loaded)
                document.querySelectorAll('fz-station-code-time').forEach(el => {
                    const t = el.textContent.replace(/\\s+/g, ' ').trim();
                    if (t) result.sct.push(t);
                });
                return result;
            }""")
            if not data or not data.get("tabs"):
                return []

            booking_url = self._build_booking_url(req)
            target_str = req.date_from.strftime("%-d %B").lstrip("0")
            # Windows strftime uses # instead of -
            try:
                target_str2 = req.date_from.strftime("%#d %B")
            except ValueError:
                target_str2 = target_str
            offers = []
            for tab in data["tabs"]:
                price_str = tab.get("price")
                if not price_str:
                    continue
                try:
                    price = float(price_str)
                except (TypeError, ValueError):
                    continue
                if price <= 0:
                    continue
                currency = tab.get("currency") or req.currency
                text = tab.get("text", "")
                # Check if this tab matches our target date
                is_target = target_str in text or target_str2 in text
                dep = req.date_from if is_target else datetime(2000, 1, 1)
                offer = FlightOffer(
                    id=f"fz_{hashlib.md5(text.encode()).hexdigest()[:12]}",
                    price=round(price, 2),
                    currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=FlightRoute(
                        segments=[FlightSegment(
                            airline="FZ", airline_name="flydubai",
                            flight_no="", origin=req.origin,
                            destination=req.destination,
                            departure=dep,
                            arrival=datetime(2000, 1, 1),
                            cabin_class="M",
                        )],
                        total_duration_seconds=0,
                        stopovers=0,
                    ),
                    inbound=None,
                    airlines=["flydubai"],
                    owner_airline="FZ",
                    booking_url=booking_url,
                    is_locked=False,
                    source="flydubai_direct",
                    source_tier="free",
                )
                if is_target:
                    return [offer]
                offers.append(offer)
            return offers
        except Exception:
            pass
        return []

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if isinstance(data, list):
            data = {"flights": data}
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # FlyDubai /api/flights/7 returns calendar segments with per-date fares
        segments = data.get("segments")
        if segments and isinstance(segments, list) and segments:
            first = segments[0] if isinstance(segments[0], dict) else {}
            if "lowestAdultFarePerPax" in first:
                return self._parse_calendar_segments(segments, req, booking_url)

        flights_raw = (
            data.get("outboundFlights")
            or data.get("outbound")
            or data.get("journeys")
            or data.get("flights")
            or data.get("availability", {}).get("trips", [])
            or data.get("data", {}).get("flights", [])
            or data.get("data", {}).get("journeys", [])
            or data.get("lowFareAvailability", {}).get("outboundOptions", [])
            or data.get("flightList", [])
            or []
        )
        if isinstance(flights_raw, dict):
            flights_raw = flights_raw.get("outbound", []) or flights_raw.get("journeys", [])
        if not isinstance(flights_raw, list):
            flights_raw = []

        for flight in flights_raw:
            offer = self._parse_single_flight(flight, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    def _parse_single_flight(self, flight: dict, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
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
            id=f"fz_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=req.currency,
            price_formatted=f"{best_price:.2f} {req.currency}",
            outbound=route,
            inbound=None,
            airlines=["flydubai"],
            owner_airline="FZ",
            booking_url=booking_url,
            is_locked=False,
            source="flydubai_direct",
            source_tier="free",
        )

    def _parse_calendar_segments(
        self, segments: list, req: FlightSearchRequest, booking_url: str
    ) -> list[FlightOffer]:
        """Parse FlyDubai /api/flights/7 calendar segments into offers.

        Each segment represents one date with the lowest available fare.
        We return an offer for the target date only.  If the target date
        is unavailable we return the closest available date in the window.
        """
        target_str = req.date_from.strftime("%Y-%m-%d")
        offers: list[FlightOffer] = []

        for seg in segments:
            if not isinstance(seg, dict):
                continue
            price_str = seg.get("lowestAdultFarePerPax", "0")
            try:
                price = float(price_str)
            except (TypeError, ValueError):
                continue
            if price <= 0 or seg.get("isSoldOut"):
                continue
            dep_date = (seg.get("departureDate") or "")[:10]
            currency = seg.get("currencyCode") or req.currency
            cabin = "M"
            cabin_list = seg.get("lowestFareByCabin") or []
            if cabin_list and isinstance(cabin_list[0], dict):
                cab = (cabin_list[0].get("lowestAdultFareCabin") or "").upper()
                if "BUSINESS" in cab:
                    cabin = "C"

            offer = FlightOffer(
                id=f"fz_{hashlib.md5(f'{seg.get('route', '')}_{dep_date}'.encode()).hexdigest()[:12]}",
                price=round(price, 2),
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=FlightRoute(
                    segments=[
                        FlightSegment(
                            airline="FZ",
                            airline_name="flydubai",
                            flight_no="",
                            origin=seg.get("origin") or req.origin,
                            destination=seg.get("dest") or req.destination,
                            departure=self._parse_dt(dep_date),
                            arrival=datetime(2000, 1, 1),
                            cabin_class=cabin,
                        )
                    ],
                    total_duration_seconds=0,
                    stopovers=0,
                ),
                inbound=None,
                airlines=["flydubai"],
                owner_airline="FZ",
                booking_url=booking_url,
                is_locked=False,
                source="flydubai_direct",
                source_tier="free",
            )
            offers.append(offer)

        # Prefer the offer matching the requested date
        target_offers = [o for o in offers if o.outbound.segments[0].departure.strftime("%Y-%m-%d") == target_str]
        if target_offers:
            return target_offers
        # Fall back to all available dates in the 7-day window
        return offers

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
        carrier = seg.get("carrierCode") or seg.get("carrier") or seg.get("airline") or "FZ"
        return FlightSegment(
            airline=carrier, airline_name="flydubai", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        source_label = "API" if offers and offers[0].source == "flydubai_api" else "Playwright"
        logger.info("FlyDubai %s→%s returned %d offers in %.1fs (%s)", req.origin, req.destination, len(offers), elapsed, source_label)
        h = hashlib.md5(f"flydubai{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
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
            f"https://www.flydubai.com/en/flight-search?origin={req.origin}"
            f"&destination={req.destination}&departure={dep}&pax={req.adults}&trip=oneway"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"flydubai{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
