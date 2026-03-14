"""
VietJet Air Playwright scraper -- deep-link URL + API response interception.

VietJet (IATA: VJ) is Vietnam's largest low-cost carrier operating domestic
and regional flights across Southeast Asia, India, and beyond.

Strategy (Deep-Link URL + API Interception, Form-Fill Fallback):
1. Primary: Navigate to vietjetair.com/en?departAirport=X&arrivalAirport=Y&departDate=D&tripType=oneway
   → The SPA auto-triggers encrypted PATCH search-flight API calls
2. Fallback: If deep-link yields nothing, do a form-fill on the homepage
3. Intercept PATCH vietjet-api.vietjetair.com/booking/api/v1/search-flight responses
4. Parse travelOption[cityPair] → flights[] + fareOptions[] → FlightOffer objects

API details (discovered Mar 2026):
- Endpoint: PATCH vietjet-api.vietjetair.com/booking/api/v1/search-flight
- Request body: {"encrypted":"..."} (client-side encrypted, no replay possible)
- Response: {status, message, travelOption: {"ORIG-DEST": [options]},
             lowestFares, sessionId, sessionExpIn}
- Intelisys/Navitaire backend
- reCAPTCHA v3 auto-verified (score 0.7), no manual solve needed
- AWS WAF (ap-southeast-1) token auto-managed by browser

Response structure:
  travelOption["SGN-HAN"] = [
    {departureDate, enRouteHours, numberOfStops,
     flights: [{airlineCode, flightNumber, departure{localScheduledTime, airport{code}},
                arrival{...}, availability, aircraftModel{identifier}}],
     fareOptions: [{fareValidity{valid, soldOut}, fareType{identifier},
       cabinClass{code, description},
       fareCharges: [{chargeType{code:"FA"}, passengerApplicability{adult:true},
                      currencyAmounts: [{totalAmount, currency{code}}]}]}]
    }
  ]
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
_LOCALES = ["en-US", "en-GB", "en-VN", "en-SG"]
_TIMEZONES = [
    "Asia/Ho_Chi_Minh", "Asia/Bangkok", "Asia/Singapore",
    "Asia/Kuala_Lumpur", "Asia/Jakarta",
]

# ── Shared browser singleton via CDP ────────────────────────────────────
_CDP_PORT = 9465
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
        _user_data = os.path.join(os.environ.get("TEMP", "/tmp"), "chrome-cdp-vietjet")
        _browser, _chrome_proc = await get_or_launch_cdp(_CDP_PORT, _user_data)
        logger.info("VietJet: Chrome ready via CDP (port %d)", _CDP_PORT)
        return _browser


class VietJetConnectorClient:
    """VietJet scraper -- deep-link URL + PATCH /booking/api/v1/search-flight interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ==================================================================
    # Main entry point
    # ==================================================================

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Try deep-link URL first, then form-fill fallback
        for attempt in range(2):
            use_form = attempt > 0
            result = await self._try_search(req, use_form=use_form)
            if result.total_results > 0:
                return result
            if attempt == 0:
                logger.info("VietJet: deep-link yielded 0 results, trying form-fill fallback")
                await asyncio.sleep(2.0)
        return result

    async def _try_search(self, req: FlightSearchRequest, use_form: bool = False) -> FlightSearchResponse:
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

            route_key = f"{req.origin}-{req.destination}"
            target_date = req.date_from.strftime("%Y-%m-%d")
            currency = req.currency if req.currency != "EUR" else "USD"

            all_travel_options: list[dict] = []
            lowest_fares_captured: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url
                    if (
                        response.status == 200
                        and "vietjet-api" in url
                        and "search-flight" in url
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" not in ct:
                            return
                        data = await response.json()
                        if not isinstance(data, dict) or not data.get("status"):
                            return
                        travel = data.get("travelOption", {})
                        # Try exact route key first, then any key containing our airports
                        options = travel.get(route_key, [])
                        if not options:
                            for key in travel:
                                if req.origin in key and req.destination in key:
                                    options = travel[key]
                                    break
                        if options:
                            if isinstance(options, list):
                                all_travel_options.extend(options)
                            else:
                                all_travel_options.append(options)
                            lf = data.get("lowestFares")
                            if lf:
                                lowest_fares_captured.update(lf if isinstance(lf, dict) else {})
                            api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            if use_form:
                # Form-fill approach
                await self._do_form_search(page, req, t0)
            else:
                # Deep-link URL approach
                dep_str = req.date_from.strftime("%Y-%m-%d")
                deep_url = (
                    f"https://www.vietjetair.com/en?"
                    f"departAirport={req.origin}&arrivalAirport={req.destination}"
                    f"&departDate={dep_str}&tripType=oneway"
                    f"&currency={currency}&languageCode=en"
                )
                logger.info("VietJet: deep-link search %s->%s on %s", req.origin, req.destination, dep_str)
                await page.goto(deep_url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
                await self._nuke_overlays(page)

            # Wait for search API responses
            remaining = max(self.timeout - (time.monotonic() - t0), 12)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
                # Wait a bit more for parallel API calls to arrive
                await asyncio.sleep(3.0)
            except asyncio.TimeoutError:
                logger.warning("VietJet: timed out waiting for search-flight API (%s)", "form" if use_form else "deep-link")

            if not all_travel_options:
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_travel_options(all_travel_options, req, currency, lowest_fares_captured)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("VietJet Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ==================================================================
    # Form-fill fallback (homepage → fill form → click "Let's go")
    # ==================================================================

    async def _do_form_search(self, page, req: FlightSearchRequest, t0: float) -> None:
        """Load homepage, fill form, click search. API interception is already set up."""
        logger.info("VietJet: form-fill search %s->%s", req.origin, req.destination)
        await page.goto(
            "https://www.vietjetair.com/en",
            wait_until="domcontentloaded",
            timeout=int(self.timeout * 1000),
        )
        await asyncio.sleep(4.0)
        await self._nuke_overlays(page)
        await asyncio.sleep(0.5)

        # Set one-way
        await self._set_one_way(page)
        await asyncio.sleep(0.5)

        # Fill origin
        ok = await self._fill_airport(page, "origin", req.origin)
        if not ok:
            logger.warning("VietJet: origin fill failed for %s", req.origin)
            return
        await asyncio.sleep(1.0)

        # Fill destination (dropdown may auto-open after origin)
        ok = await self._fill_airport(page, "destination", req.destination)
        if not ok:
            logger.warning("VietJet: destination fill failed for %s", req.destination)
            return
        await asyncio.sleep(1.0)

        # Fill date
        await self._fill_date(page, req)
        await asyncio.sleep(0.5)

        # Click "Let's go"
        await self._nuke_overlays(page)
        await self._click_search(page)

    # ------------------------------------------------------------------
    # Overlay / popup removal
    # ------------------------------------------------------------------

    async def _nuke_overlays(self, page) -> None:
        """Aggressively remove all overlays, popups, cookie banners."""
        for txt in ["Accept", "Agree", "Close", "Later", "Got it", "OK", "Dismiss"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(txt)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=1500)
                    await asyncio.sleep(0.3)
            except Exception:
                continue
        try:
            await page.evaluate("""() => {
                const sels = '[class*="cookie"],[id*="cookie"],[class*="popup"],[id*="popup"],' +
                    '[class*="modal"],[class*="notification"],[class*="smartech"],[id*="smartech"],' +
                    '[class*="Overlay"],[class*="overlay"],.MuiBackdrop-root,' +
                    '[class*="consent"],[id*="consent"]';
                document.querySelectorAll(sels).forEach(el => {
                    if (el.offsetHeight > 0) el.remove();
                });
                document.body.style.overflow = 'auto';
                document.body.style.pointerEvents = 'auto';
            }""")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # One-way toggle
    # ------------------------------------------------------------------

    async def _set_one_way(self, page) -> None:
        for sel in [
            "input#oneway", "input[value='oneway']", "input[value='OW']",
            "label[for='oneway']", "label[for='one-way']",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click(timeout=2000)
                    return
            except Exception:
                continue
        for label in ["One-way", "One Way", "One way"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if el and await el.count() > 0:
                    await el.click(timeout=2000)
                    return
            except Exception:
                continue

    # ------------------------------------------------------------------
    # Airport field fill (MUI ExpansionPanel dropdown)
    # ------------------------------------------------------------------

    async def _fill_airport(self, page, field_type: str, iata: str) -> bool:
        """Fill origin or destination airport in the MUI booking widget.

        The dropdown is an MUI ExpansionPanel with airports grouped by country.
        Each airport shows the IATA code as a separate text element.
        """
        is_origin = field_type == "origin"

        # Step 1: Click the input to open the dropdown
        clicked = False
        if is_origin:
            for sel in [
                "[class*='departurePlaceDesktop'] input",
                "input#departurePlaceDesktop",
                ".booking-widget input:first-of-type",
                "[class*='MuiInputBase'] input",
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        clicked = True
                        break
                except Exception:
                    continue
            if not clicked:
                try:
                    inputs = page.locator("[class*='booking'] input, [class*='search-form'] input, [class*='flight-search'] input")
                    if await inputs.count() > 0:
                        await inputs.first.click(timeout=3000)
                        clicked = True
                except Exception:
                    pass
        else:
            for sel in [
                "input#arrivalPlaceDesktop",
                "[class*='arrivalPlaceDesktop'] input",
                "[id*='arrival'] input",
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        clicked = True
                        break
                except Exception:
                    continue

        if not clicked:
            logger.debug("VietJet: %s input not found", field_type)
            return False

        await asyncio.sleep(1.0)

        # Step 2: Find and click the IATA code in the dropdown
        for _ in range(3):
            # Try text match for the IATA code
            try:
                iata_text = page.get_by_text(iata, exact=True)
                count = await iata_text.count()
                if count > 0:
                    for i in range(min(count, 5)):
                        el = iata_text.nth(i)
                        try:
                            box = await el.bounding_box()
                            if box and box["height"] > 0:
                                await el.click(timeout=2000)
                                logger.debug("VietJet: selected %s via text (idx %d)", iata, i)
                                return True
                        except Exception:
                            continue
            except Exception:
                pass

            # Try typing to filter, then recheck
            try:
                active = page.locator("input:focus")
                if await active.count() > 0:
                    await active.fill(iata)
                    await asyncio.sleep(1.5)
            except Exception:
                pass

        # JS fallback: find and click leaf text node matching IATA
        try:
            clicked_js = await page.evaluate("""(iata) => {
                const els = document.querySelectorAll('*');
                for (const el of els) {
                    if (el.children.length === 0 &&
                        el.textContent.trim() === iata &&
                        el.offsetHeight > 0 &&
                        el.getBoundingClientRect().top > 0) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }""", iata)
            if clicked_js:
                logger.debug("VietJet: selected %s via JS", iata)
                return True
        except Exception:
            pass

        logger.warning("VietJet: could not select %s in %s dropdown", iata, field_type)
        return False

    # ------------------------------------------------------------------
    # Date picker
    # ------------------------------------------------------------------

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from
        day = target.day
        day_str = str(day)

        # Open date picker if not already visible
        try:
            cal = page.locator("[class*='calendar'], [class*='Calendar'], [class*='datepicker']")
            if await cal.count() == 0:
                for sel in [
                    "input[name='DepartureDate']",
                    "[class*='departure-date'] input",
                    "[class*='depart'] input",
                ]:
                    try:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            await el.click(timeout=3000)
                            break
                    except Exception:
                        continue
            await asyncio.sleep(1.0)
        except Exception:
            pass

        # Navigate to correct month
        target_month_year = target.strftime("%B %Y")
        for _ in range(14):
            try:
                if await page.get_by_text(target_month_year).count() > 0:
                    break
            except Exception:
                pass
            for arrow_sel in [
                "button[aria-label*='next' i]",
                "button[aria-label*='Next']",
                "[class*='next']",
                "[class*='forward']",
            ]:
                try:
                    arrow = page.locator(arrow_sel).first
                    if await arrow.count() > 0:
                        await arrow.click(timeout=2000)
                        await asyncio.sleep(0.4)
                        break
                except Exception:
                    continue

        # Click the target day
        for fmt in [
            f"{day} {target.strftime('%B')} {target.year}",
            f"{target.strftime('%B')} {day}, {target.year}",
            target.strftime("%Y-%m-%d"),
        ]:
            try:
                btn = page.locator(f"[aria-label*='{fmt}']").first
                if await btn.count() > 0:
                    await btn.click(timeout=3000)
                    return True
            except Exception:
                continue

        # Calendar cell buttons
        for sel in [
            "td button", "table button", "[class*='calendar'] button",
            "[class*='Calendar'] button",
        ]:
            try:
                btns = page.locator(sel).filter(has_text=re.compile(rf"^{day_str}$"))
                if await btns.count() > 0:
                    await btns.first.click(timeout=3000)
                    return True
            except Exception:
                continue

        # JS fallback
        try:
            clicked = await page.evaluate("""(day) => {
                const cals = document.querySelectorAll(
                    '[class*="calendar"], [class*="Calendar"], [class*="datepicker"], table'
                );
                for (const cal of cals) {
                    const cells = cal.querySelectorAll('td, button, [class*="day"]');
                    for (const cell of cells) {
                        if (cell.textContent.trim() === String(day) && cell.offsetHeight > 0) {
                            cell.click();
                            return true;
                        }
                    }
                }
                return false;
            }""", day)
            if clicked:
                return True
        except Exception:
            pass

        logger.warning("VietJet: failed to click day %d", day)
        return False

    # ------------------------------------------------------------------
    # Search button ("Let's go")
    # ------------------------------------------------------------------

    async def _click_search(self, page) -> None:
        for label in ["Let's go", "Let's Go", "LETS GO", "Search", "Search flights"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"{re.escape(label)}", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(force=True, timeout=5000)
                    logger.info("VietJet: clicked '%s'", label)
                    await asyncio.sleep(3.0)
                    return
            except Exception:
                continue

        for sel in [
            "button:has-text(\"Let's go\")",
            "button:has-text('Let')",
            "button[type='submit']",
        ]:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click(force=True, timeout=5000)
                    await asyncio.sleep(3.0)
                    return
            except Exception:
                continue

        # JS fallback
        try:
            await page.evaluate("""() => {
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    const txt = b.textContent.toLowerCase();
                    if (txt.includes("let") && txt.includes("go")) {
                        b.click();
                        return true;
                    }
                }
                const submit = document.querySelector('button[type="submit"]');
                if (submit) { submit.click(); return true; }
                return false;
            }""")
            await asyncio.sleep(3.0)
        except Exception:
            logger.warning("VietJet: could not click search button")

    # ==================================================================
    # Response parsing
    # ==================================================================

    def _parse_travel_options(
        self, options: list[dict], req: FlightSearchRequest, currency: str, lowest_fares: dict
    ) -> list[FlightOffer]:
        """Parse travelOption entries into FlightOffer objects."""
        target_date = req.date_from.strftime("%Y-%m-%d")
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        seen_keys: set[str] = set()

        for opt in options:
            # Filter to target date
            dep_date = opt.get("departureDate", "")
            if dep_date and target_date not in dep_date:
                continue

            flights_raw = opt.get("flights", [])
            fare_options = opt.get("fareOptions", [])
            if not flights_raw:
                continue

            # Build segments
            segments: list[FlightSegment] = []
            for flt in flights_raw:
                seg = self._build_segment(flt, req)
                if seg:
                    segments.append(seg)
            if not segments:
                continue

            # Deduplicate by flight numbers
            seg_key = "-".join(f"{s.airline}{s.flight_no}" for s in segments)
            if seg_key in seen_keys:
                continue
            seen_keys.add(seg_key)

            # Duration
            en_route = opt.get("enRouteHours", 0)
            if en_route:
                total_dur = int(float(en_route) * 3600)
            elif segments[0].departure and segments[-1].arrival:
                total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
            else:
                total_dur = 0

            stopovers = max(len(segments) - 1, 0)
            route = FlightRoute(
                segments=segments,
                total_duration_seconds=max(total_dur, 0),
                stopovers=stopovers,
            )

            # Extract pricing from fareOptions
            if fare_options:
                best_price, best_currency, _ = self._cheapest_fare(fare_options, currency)
                if best_price is not None and best_price > 0:
                    offer_key = opt.get("key", seg_key)
                    offers.append(self._make_offer(
                        offer_key, best_price, best_currency, route, booking_url,
                    ))
                    continue

            # Fallback: try lowestFares
            if lowest_fares:
                lf_price = self._extract_lowest_fare(lowest_fares, target_date)
                if lf_price and lf_price > 0:
                    offer_key = opt.get("key", seg_key)
                    offers.append(self._make_offer(
                        offer_key, lf_price, currency, route, booking_url,
                    ))
                    continue

            # Fallback: emit offer with price=0 (flights known, price unknown)
            offer_key = opt.get("key", seg_key)
            offers.append(self._make_offer(
                offer_key, 0, currency, route, booking_url,
            ))
            logger.warning("VietJet: flights found but no pricing for %s", seg_key)

        return offers

    # ------------------------------------------------------------------
    # Fare extraction
    # ------------------------------------------------------------------

    @staticmethod
    def _cheapest_fare(fare_options: list[dict], default_currency: str) -> tuple:
        """Extract cheapest adult fare from fareOptions[].fareCharges[].currencyAmounts[].

        Returns (price, currency, cabin_class) or (None, default_currency, "Economy").
        """
        best_price = None
        best_currency = default_currency
        best_cabin = "Economy"

        for fo in fare_options:
            # Skip invalid / sold out fares
            validity = fo.get("fareValidity", {})
            if isinstance(validity, dict):
                if not validity.get("valid", True) or validity.get("soldOut", False):
                    continue

            cabin = fo.get("cabinClass", {})
            cabin_desc = cabin.get("description", "Economy") if isinstance(cabin, dict) else "Economy"

            # VietJet uses fareCharges[].currencyAmounts[].totalAmount
            for charge in fo.get("fareCharges", []):
                charge_type = charge.get("chargeType", {})
                code = charge_type.get("code", "") if isinstance(charge_type, dict) else str(charge_type)
                if code != "FA":
                    continue
                pax = charge.get("passengerApplicability", {})
                if isinstance(pax, dict) and not pax.get("adult", True):
                    continue
                for ca in charge.get("currencyAmounts", []):
                    total = ca.get("totalAmount")
                    if total is not None:
                        try:
                            v = float(total)
                            if v > 0 and (best_price is None or v < best_price):
                                best_price = v
                                cur = ca.get("currency", {})
                                best_currency = (cur.get("code", default_currency)
                                                 if isinstance(cur, dict) else str(cur or default_currency))
                                best_cabin = cabin_desc
                        except (TypeError, ValueError):
                            pass

            # Fallback: try flat price fields on the fareOption itself
            if best_price is None:
                for key in ["price", "amount", "totalPrice", "totalAmount", "displayPrice"]:
                    val = fo.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("value")
                    if val is not None:
                        try:
                            v = float(val)
                            if v > 0 and (best_price is None or v < best_price):
                                best_price = v
                                best_cabin = cabin_desc
                        except (TypeError, ValueError):
                            pass

        return best_price, best_currency, best_cabin

    @staticmethod
    def _extract_lowest_fare(lowest_fares: dict, target_date: str) -> Optional[float]:
        """Extract price from the lowestFares section keyed by date."""
        if not isinstance(lowest_fares, dict):
            return None
        # Try date-keyed lookup
        for key, val in lowest_fares.items():
            if target_date in str(key):
                if isinstance(val, (int, float)) and val > 0:
                    return float(val)
                if isinstance(val, dict):
                    for pkey in ["price", "amount", "fare", "total", "totalAmount"]:
                        p = val.get(pkey)
                        if p is not None:
                            try:
                                v = float(p)
                                if v > 0:
                                    return v
                            except (TypeError, ValueError):
                                pass
        return None

    # ------------------------------------------------------------------
    # Segment / offer builders
    # ------------------------------------------------------------------

    def _build_segment(self, flt: dict, req: FlightSearchRequest) -> Optional[FlightSegment]:
        """Parse a single flight from travelOption.flights[]."""
        dep_info = flt.get("departure", {})
        arr_info = flt.get("arrival", {})
        dep_time = dep_info.get("localScheduledTime") or dep_info.get("scheduledTime") or ""
        arr_time = arr_info.get("localScheduledTime") or arr_info.get("scheduledTime") or ""

        dep_airport = dep_info.get("airport", {})
        arr_airport = arr_info.get("airport", {})

        # airlineCode can be a string "VJ" or a dict {"code": "VJ"}
        airline_raw = flt.get("airlineCode", "VJ")
        if isinstance(airline_raw, dict):
            airline = airline_raw.get("code", "VJ")
        else:
            airline = str(airline_raw) if airline_raw else "VJ"

        flight_no_raw = str(flt.get("flightNumber", ""))
        flight_no = f"{airline}{flight_no_raw}" if flight_no_raw and not flight_no_raw.startswith(airline) else flight_no_raw

        return FlightSegment(
            airline=airline,
            airline_name="VietJet Air",
            flight_no=flight_no,
            origin=dep_airport.get("code", req.origin) if isinstance(dep_airport, dict) else req.origin,
            destination=arr_airport.get("code", req.destination) if isinstance(arr_airport, dict) else req.destination,
            departure=self._parse_dt(dep_time),
            arrival=self._parse_dt(arr_time),
            cabin_class="economy",
        )

    @staticmethod
    def _make_offer(
        key: str, price: float, currency: str, route: FlightRoute, booking_url: str,
    ) -> FlightOffer:
        offer_id = hashlib.md5(str(key).encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"vj_{offer_id}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:,.0f} {currency}" if price > 0 else f"0 {currency}",
            outbound=route,
            inbound=None,
            airlines=["VietJet"],
            owner_airline="VJ",
            booking_url=booking_url,
            is_locked=False,
            source="vietjet_direct",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # Utility methods
    # ------------------------------------------------------------------

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: (o.price if o.price > 0 else float("inf")))
        logger.info("VietJet %s->%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"vietjet{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
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
            f"https://www.vietjetair.com/en/booking?origin={req.origin}"
            f"&destination={req.destination}&departDate={dep}&adults={req.adults}&type=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"vietjet{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
