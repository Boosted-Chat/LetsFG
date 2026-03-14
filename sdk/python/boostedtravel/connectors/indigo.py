"""
IndiGo direct scraper — uses Playwright to scrape flight data.

IndiGo (IATA: 6E) is India's largest airline by market share.
Website: www.goindigo.in — custom React SPA with Module Federation micro-frontends.

Strategy:
1. Navigate to goindigo.in homepage
2. Dismiss cookie/session banners
3. Fill search form (From/To/Departure, One Way)
4. Intercept flight search API via page.route() + route.fetch()
5. Parse JSON → FlightOffer objects

Key technical details:
- API endpoint: api-prod-flight-skyplus6e.goindigo.in/v1/flight/search (POST)
- page.on("response") cannot see cross-origin API calls from micro-frontends
- page.route() + route.fetch() intercepts at the Playwright proxy level
- Akamai Bot Manager protects the API; may return 403 on some requests
- response is text/plain with JSON body (728KB+ for popular routes)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
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

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
    {"width": 1600, "height": 900},
]
_LOCALES = ["en-IN", "en-US", "en-GB"]
_TIMEZONES = [
    "Asia/Kolkata", "Asia/Dubai", "Europe/London",
    "Asia/Singapore", "Asia/Bangkok",
]

# ── Shared browser context (headed Chrome, persistent) ──────────────────
_ctx = None
_ctx_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _ctx_lock
    if _ctx_lock is None:
        _ctx_lock = asyncio.Lock()
    return _ctx_lock


async def _get_context():
    """Launch a persistent headed-Chrome context (reused across searches)."""
    global _ctx
    lock = _get_lock()
    async with lock:
        if _ctx and _ctx.browser:
            return _ctx
        from playwright.async_api import async_playwright
        pw = await async_playwright().start()
        user_data = os.path.join(os.environ.get("TEMP", "/tmp"), "chrome-indigo-ctx")
        os.makedirs(user_data, exist_ok=True)
        vp = random.choice(_VIEWPORTS)
        _ctx = await pw.chromium.launch_persistent_context(
            user_data, channel="chrome", headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--window-position=-2400,-2400",
                f"--window-size={vp['width']},{vp['height']}",
            ],
            viewport=vp,
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )
        logger.info("IndiGo: headed Chrome context ready")
        return _ctx


class IndiGoConnectorClient:
    """IndiGo Playwright scraper — React SPA form search + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Retry up to 2 times (Akamai may block the first attempt)
        for attempt in range(2):
            result = await self._try_search(req)
            if result.total_results > 0:
                return result
            if attempt == 0:
                logger.info("IndiGo: retrying search (attempt %d failed)", attempt + 1)
                await asyncio.sleep(2.0)
        return result

    async def _try_search(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        context = await _get_context()

        try:
            page = await context.new_page()

            captured_data: dict = {}
            api_event = asyncio.Event()
            import json as _json

            # Intercept the flight search API at the Playwright proxy level.
            # page.on("response") can't see this cross-origin API call, but
            # page.route() + route.fetch() intercepts at the proxy layer.
            async def _intercept_flight(route):
                if route.request.method == "OPTIONS":
                    await route.continue_()
                    return
                try:
                    resp = await route.fetch()
                    body = await resp.text()
                    if resp.status == 200 and body.strip():
                        data = _json.loads(body)
                        if isinstance(data, dict) and "data" in data:
                            captured_data["json"] = data["data"]
                        else:
                            captured_data["json"] = data
                        logger.info("IndiGo: captured flight/search API (%d bytes)", len(body))
                        api_event.set()
                    else:
                        logger.warning("IndiGo: API returned status %d (Akamai may be blocking)", resp.status)
                    await route.fulfill(
                        status=resp.status,
                        headers=resp.headers,
                        body=body,
                    )
                except Exception as exc:
                    logger.debug("IndiGo route intercept error: %s", exc)
                    try:
                        await route.continue_()
                    except Exception:
                        pass

            await page.route("**/v1/flight/search**", _intercept_flight)

            logger.info("IndiGo: loading homepage for %s->%s", req.origin, req.destination)
            await page.goto(
                "https://www.goindigo.in/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(6.0)

            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await self._dismiss_cookies(page)

            # Select One Way trip type
            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            # Fill origin
            ok = await self._fill_airport_field(page, "From", req.origin, 0)
            if not ok:
                logger.warning("IndiGo: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill destination - IndiGo uses "Going to?" placeholder
            ok = await self._fill_airport_field(page, "To", req.destination, 1)
            if not ok:
                logger.warning("IndiGo: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("IndiGo: date fill failed")
                return self._empty(req)
            await asyncio.sleep(0.3)

            # Click search
            await self._click_search(page)

            # Wait for the flight search API response via route interceptor
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("IndiGo: API event timed out, trying DOM fallback")

            data = captured_data.get("json")
            if data:
                elapsed = time.monotonic() - t0
                offers = self._parse_response(data, req)
                return self._build_response(offers, req, elapsed)

            # DOM fallback
            logger.info("IndiGo: trying DOM extraction fallback")
            offers = await self._extract_from_dom(page, req)
            if offers:
                return self._build_response(offers, req, time.monotonic() - t0)
            return self._empty(req)

        except Exception as e:
            logger.error("IndiGo Playwright error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _dismiss_cookies(self, page) -> None:
        # IndiGo has a minimal cookie notice — try to close it
        for label in [
            "Accept", "Accept All", "Accept all", "I agree",
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
        # Close dialog windows if any
        try:
            close_link = page.locator("a:has-text('Close this dialog window')")
            if await close_link.count() > 0:
                await close_link.first.click(timeout=2000)
                await asyncio.sleep(0.3)
        except Exception:
            pass
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="Cookie"], [id*="Cookie"], [class*="onetrust"], [id*="onetrust"], ' +
                    '[class*="modal-overlay"], [class*="popup"], [id*="popup"], ' +
                    '[class*="privacy"], [id*="privacy"], [class*="dialog"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        # IndiGo radio inputs are hidden; click the visible label/wrapper instead.
        # One Way is the default selection, but click it to be safe.
        try:
            radio = page.locator("input#radio-input-triptype-oneWay")
            if await radio.count() > 0 and await radio.is_checked():
                return  # already selected
        except Exception:
            pass
        # Click the wrapper div that contains the radio + label text
        try:
            wrapper = page.locator("label[for='radio-input-triptype-oneWay'], input#radio-input-triptype-oneWay + *")
            if await wrapper.count() > 0:
                await wrapper.first.click(timeout=3000)
                return
        except Exception:
            pass
        try:
            ow = page.get_by_text("One Way", exact=True).first
            if ow and await ow.count() > 0:
                await ow.click(timeout=3000)
        except Exception:
            pass

    async def _fill_airport_field(self, page, label: str, iata: str, index: int) -> bool:
        """
        IndiGo DOM: inputs are hidden inside .popover__wrapper divs.
        Click the container (aria-label 'sourceCity'/'destinationCity') to reveal
        a combobox input, type the IATA code, then click the matching suggestion.
        """
        try:
            # Step 1: Click the city selector container to reveal the combobox
            if index == 0:
                container = page.locator("[aria-label*='sourceCity'], .popover__wrapper.search-widget-form-body__from")
            else:
                container = page.locator("[aria-label*='destinationCity'], .popover__wrapper.search-widget-form-body__to")
            await container.first.click(timeout=5000, no_wait_after=True)
            await asyncio.sleep(0.8)

            # Step 2: Find the now-visible combobox input and type the IATA code
            combo = page.locator("input[role='combobox']")
            visible_combos = []
            for i in range(await combo.count()):
                if await combo.nth(i).is_visible():
                    visible_combos.append(combo.nth(i))
            if not visible_combos:
                logger.warning("IndiGo: no visible combobox after clicking %s container", label)
                return False
            inp = visible_combos[0]
            await inp.fill("")
            await asyncio.sleep(0.2)
            await inp.press_sequentially(iata, delay=80)
            await asyncio.sleep(1.5)

            # Step 3: Click the matching suggestion.
            # IndiGo now uses .city-selection__list-item-wrapper for suggestion items.
            # Each item has the IATA code as text in a child element.
            sugg = page.locator(".city-selection__list-item-wrapper")
            if await sugg.count() > 0:
                # Try to find the exact IATA match
                for i in range(min(await sugg.count(), 20)):
                    item = sugg.nth(i)
                    try:
                        if not await item.is_visible():
                            continue
                        text = await item.inner_text()
                        if iata in text:
                            await item.click(timeout=3000)
                            return True
                    except Exception:
                        continue
                # If no exact match, click the first visible item
                await sugg.first.click(timeout=3000)
                return True

            # Fallback: keyboard selection (ArrowDown + Enter)
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(0.3)
            await page.keyboard.press("Enter")
            return True

        except Exception as e:
            logger.warning("IndiGo: %s airport field error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """
        IndiGo uses react-date-range calendar (rdrCalendarWrapper).
        Click departure button → navigate months → click day by aria-label.
        Day aria-labels: "Wednesday, 11 March 2026" etc.
        Navigate via .rdrNextButton / .rdrPprevButton.
        """
        target = req.date_from
        try:
            # Click departure date button to open calendar
            dep_btn = page.locator("button[class*='departureDate'], .popover__wrapper.search-widget-form-body__departure")
            if await dep_btn.count() == 0:
                dep_btn = page.locator("[aria-label*='departureDate']")
            await dep_btn.first.click(timeout=5000)
            await asyncio.sleep(0.8)

            # Navigate to target month using react-date-range next button
            target_month_year = target.strftime("%B %Y")  # e.g. "April 2026"
            for _ in range(14):
                header = page.locator(".rdrMonthAndYearPickers, .rdrMonthName")
                if await header.count() > 0:
                    text = await header.first.inner_text()
                    if target_month_year.lower() in text.lower():
                        break
                # Also check all month name elements (multi-month view)
                month_names = page.locator(".rdrMonthName")
                for i in range(await month_names.count()):
                    mn_text = await month_names.nth(i).inner_text()
                    if target_month_year.lower() in mn_text.lower():
                        break
                else:
                    # Click next month button
                    nxt = page.locator(".rdrNextButton")
                    if await nxt.count() > 0:
                        await nxt.first.click(timeout=2000)
                        await asyncio.sleep(0.4)
                        continue
                    break
                break  # found in month_names loop

            # Click the target day
            # IndiGo day spans have aria-label like "Wednesday, 11 March 2026"
            day_num = target.day
            day_name = target.strftime("%A")  # e.g. "Wednesday"
            month_name = target.strftime("%B")  # e.g. "March"
            aria_label = f"{day_name}, {day_num} {month_name} {target.year}"

            day_el = page.locator(f"span[aria-label='{aria_label}']")
            if await day_el.count() > 0:
                # Wait for calendar animation to settle, then force-click
                await asyncio.sleep(0.5)
                await day_el.first.click(timeout=5000, force=True)
                await asyncio.sleep(0.5)
                return True

            # Fallback: match partial aria-label
            day_el = page.locator(f"span[aria-label*='{day_num} {month_name} {target.year}']")
            if await day_el.count() > 0:
                await asyncio.sleep(0.5)
                await day_el.first.click(timeout=5000, force=True)
                await asyncio.sleep(0.5)
                return True

            # Last fallback: click rdrDay button matching the day number
            day_btns = page.locator(".rdrDay:not(.rdrDayDisabled) .rdrDayNumber span")
            for i in range(await day_btns.count()):
                btn = day_btns.nth(i)
                txt = (await btn.inner_text()).strip()
                if txt == str(day_num):
                    await btn.click(timeout=3000)
                    return True

            logger.warning("IndiGo: could not find day %s in calendar", day_num)
            return False
        except Exception as e:
            logger.warning("IndiGo: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        # IndiGo search button starts disabled; wait for it to become enabled
        try:
            search_btn = page.locator("button:has-text('Search'):not([disabled])")
            await search_btn.first.wait_for(state="visible", timeout=5000)
            await search_btn.first.click(timeout=5000)
            logger.info("IndiGo: clicked search")
            return
        except Exception:
            pass
        for label in ["Search", "SEARCH", "Search Flights", "Search flights", "Find flights"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000, force=True)
                    logger.info("IndiGo: clicked search (force)")
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.journeys || d.fares || d.availability)) return d;
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
        """
        Parse IndiGo v1/flight/search API response.
        Structure: data.trips[0].journeysAvailable[] → each journey has:
          - designator: {origin, destination, departure, arrival}
          - passengerFares[]: [{productClass, totalFareAmount, totalTax, ...}]
          - segments[]: [{designator: {...}, identifier: {identifier, carrierCode}}]
          - stops, flightType, journeyKey, segKey
        Currency at data.currencyCode (default INR).
        """
        if isinstance(data, list):
            data = {"trips": [{"journeysAvailable": data}]}
        booking_url = self._build_booking_url(req)
        currency = data.get("currencyCode") or req.currency
        offers: list[FlightOffer] = []

        trips = data.get("trips") or []
        if not trips:
            return offers
        trip = trips[0]
        journeys = trip.get("journeysAvailable") or []

        for journey in journeys:
            if journey.get("isSold"):
                continue
            offer = self._parse_journey(journey, req, booking_url, currency)
            if offer:
                offers.append(offer)
        return offers

    def _parse_journey(self, journey: dict, req: FlightSearchRequest, booking_url: str, currency: str) -> Optional[FlightOffer]:
        # Get cheapest fare from passengerFares (productClass R = economy regular)
        passenger_fares = journey.get("passengerFares") or []
        best_price = float("inf")
        for pf in passenger_fares:
            amt = pf.get("totalFareAmount")
            if amt is not None:
                try:
                    v = float(amt)
                    if 0 < v < best_price:
                        best_price = v
                except (TypeError, ValueError):
                    pass
        if best_price == float("inf"):
            return None

        # Build segments from journey.segments[]
        segments_raw = journey.get("segments") or []
        segments: list[FlightSegment] = []
        for seg in segments_raw:
            desig = seg.get("designator") or {}
            ident = seg.get("identifier") or {}
            segments.append(FlightSegment(
                airline=ident.get("carrierCode") or "6E",
                airline_name="IndiGo",
                flight_no=str(ident.get("identifier") or ""),
                origin=desig.get("origin") or req.origin,
                destination=desig.get("destination") or req.destination,
                departure=self._parse_dt(desig.get("departure") or ""),
                arrival=self._parse_dt(desig.get("arrival") or ""),
                cabin_class="M",
            ))
        if not segments:
            # Fallback: use journey-level designator
            desig = journey.get("designator") or {}
            segments.append(FlightSegment(
                airline="6E", airline_name="IndiGo", flight_no="",
                origin=desig.get("origin") or req.origin,
                destination=desig.get("destination") or req.destination,
                departure=self._parse_dt(desig.get("departure") or ""),
                arrival=self._parse_dt(desig.get("arrival") or ""),
                cabin_class="M",
            ))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = journey.get("journeyKey") or journey.get("segKey") or f"{time.monotonic()}"
        return FlightOffer(
            id=f"6e_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["IndiGo"],
            owner_airline="6E",
            booking_url=booking_url,
            is_locked=False,
            source="indigo_direct",
            source_tier="free",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("IndiGo %s→%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"indigo{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
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
        dep = req.date_from.strftime("%d/%m/%Y")
        return (
            f"https://www.goindigo.in/flight-booking?origin={req.origin}"
            f"&destination={req.destination}&date={dep}&adults={req.adults}&tripType=O"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"indigo{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
