"""
Emirates (EK) CDP Chrome connector — form fill + DOM results scraping.

Emirates' booking flow at /english/book/ is a Next.js app behind Akamai WAF.
Direct API calls are blocked; the ONLY reliable path is form-triggered browsing.

Strategy (CDP Chrome + DOM scraping):
1. Launch REAL Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP.
3. Navigate to /english/book/ → remove disruption modal → dismiss cookies.
4. Click "One way" → fill departure/arrival via auto-suggest typeahead
   → select date via DayPicker calendar widget → click "Search flights".
5. Page navigates to /booking/search-results/?searchRequest=<base64>.
6. Wait for DOM with flight cards → scrape flight details (flight no,
   dep/arr times, duration, stops, airports, price, aircraft type).
7. Also capture /service/search-results/flexi-fares API as pricing fallback.

Discovered Mar 2026:
  - /english/book/ inputs: auto-suggest (typed with delay), DayPicker calendar.
  - Results page: flight cards with EK flight numbers, times, prices in AED.
  - API: /service/search-results/flexi-fares → calendar pricing for ±3 days.
  - API: /service/search-results/simplified-fare-rules → fare brand details.
  - Akamai WAF blocks headless browsers; CDP headed Chrome works.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, date
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
    os.path.dirname(os.path.abspath(__file__)), ".emirates_chrome_data"
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
    """Get or create a persistent browser context (headed — Akamai blocks headless)."""
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

        # Try existing Chrome
        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("Emirates: connected to existing Chrome on port %d", _DEBUG_PORT)
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
                "Emirates: Chrome launched on CDP port %d (pid %d)",
                _DEBUG_PORT, _chrome_proc.pid,
            )

        contexts = _browser.contexts
        _context = contexts[0] if contexts else await _browser.new_context()
        return _context


async def _dismiss_overlays(page) -> None:
    """Remove disruption modal, OneTrust cookie banner, and unstick header."""
    await page.evaluate("""() => {
        document.querySelectorAll(
            '#modal-wrapper, .disruption-modal-wrapper'
        ).forEach(el => el.remove());
        // Unstick header so it doesn't block form field clicks
        const hdr = document.querySelector('.header-popup__wrapper--sticky, header[data-auto="header"]');
        if (hdr) hdr.style.position = 'relative';
        // Remove header second-level menu that intercepts pointer events
        document.querySelectorAll('.second-level-menu').forEach(el => {
            el.style.pointerEvents = 'none';
        });
    }""")
    try:
        btn = page.locator("#onetrust-accept-btn-handler")
        if await btn.count() > 0:
            await btn.first.click(timeout=3000)
            await asyncio.sleep(0.5)
            return
    except Exception:
        pass
    # Force-remove cookie SDK
    try:
        await page.evaluate("""() => {
            document.querySelectorAll(
                '#onetrust-consent-sdk, .onetrust-pc-dark-filter, #onetrust-banner-sdk'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


async def _reset_profile():
    """Wipe Chrome profile when Akamai flags the session."""
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
            logger.info("Emirates: deleted stale Chrome profile")
        except Exception:
            pass


class EmiratesConnectorClient:
    """Emirates CDP Chrome connector — form fill + results page scraping."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()

        # Interception state
        flexi_data: dict = {}
        akamai_blocked = False

        async def _on_response(response):
            nonlocal akamai_blocked
            url = response.url
            if "accessrestricted" in url:
                akamai_blocked = True
                return
            if "flexi-fares" in url and response.status == 200:
                try:
                    data = await response.json()
                    if isinstance(data, dict) and "options" in data:
                        flexi_data.update(data)
                        logger.info("Emirates: captured flexi-fares response")
                except Exception:
                    pass

        page.on("response", _on_response)

        try:
            # Step 1: Load booking page
            logger.info("Emirates: loading /book/ for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.emirates.com/english/book/",
                wait_until="domcontentloaded",
                timeout=45000,
            )
            await asyncio.sleep(5.0)
            await _dismiss_overlays(page)
            await asyncio.sleep(0.5)

            # Step 2: Click "One way"
            await page.evaluate("""() => {
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    if (b.textContent.trim() === 'One way' && b.offsetHeight > 0) {
                        b.click(); return;
                    }
                }
            }""")
            await asyncio.sleep(1.0)

            # Step 3: Fill airport fields
            ok = await self._fill_airports(page, req.origin, req.destination)
            if not ok:
                logger.warning("Emirates: airport fill failed")
                return self._empty(req)

            # Step 4: Select date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("Emirates: date selection failed")
                return self._empty(req)

            # Step 5: Click "Search flights"
            await page.evaluate("""() => {
                const btns = document.querySelectorAll('button');
                for (const b of btns) {
                    if (b.textContent.trim() === 'Search flights' && b.offsetHeight > 0) {
                        b.click(); return;
                    }
                }
            }""")
            logger.info("Emirates: search clicked")

            # Step 6: Wait for results page
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            got_results = False
            while time.monotonic() < deadline:
                await asyncio.sleep(1.0)
                url = page.url
                if akamai_blocked:
                    logger.warning("Emirates: Akamai blocked, resetting profile")
                    await _reset_profile()
                    return self._empty(req)
                if "search-results" in url:
                    # Wait for flight data to load
                    await asyncio.sleep(6.0)
                    got_results = True
                    break

            if not got_results:
                logger.warning("Emirates: never reached results page (URL: %s)", page.url[:200])
                return self._empty(req)

            # Step 7: Scrape flight data from DOM
            flights = await self._scrape_results(page)

            if not flights:
                # Fallback: try flexi-fares calendar pricing
                if flexi_data and flexi_data.get("options"):
                    flights = self._parse_flexi_fares(flexi_data, req)
                    logger.info("Emirates: using flexi-fares fallback (%d offers)", len(flights))

            offers = []
            for f in flights:
                offer = self._build_offer(f, req)
                if offer:
                    offers.append(offer)

            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "Emirates %s→%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"emirates{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            currency = offers[0].currency if offers else "AED"
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Emirates CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Form fill helpers
    # ------------------------------------------------------------------

    async def _fill_airports(self, page, origin: str, destination: str) -> bool:
        """Fill departure and arrival auto-suggest fields."""
        inputs = page.locator("input[id^='auto-suggest_']")
        count = await inputs.count()
        if count < 2:
            logger.warning("Emirates: only %d auto-suggest inputs found", count)
            return False

        # Departure
        ok = await self._fill_auto_suggest(page, inputs.first, origin)
        if not ok:
            return False
        await asyncio.sleep(1.0)

        # Arrival
        ok = await self._fill_auto_suggest(page, inputs.nth(1), destination)
        if not ok:
            return False
        await asyncio.sleep(1.0)
        return True

    async def _fill_auto_suggest(self, page, field, iata: str) -> bool:
        """Type into an auto-suggest field and select from dropdown."""
        try:
            # JS click to bypass label / header pointer interception
            el_handle = await field.element_handle()
            await page.evaluate("el => { el.focus(); el.click(); }", el_handle)
            await asyncio.sleep(0.5)
            # Select all existing text, then type IATA
            await page.evaluate("el => el.select()", el_handle)
            await field.type(iata, delay=100)
            await asyncio.sleep(2.5)

            # Click dropdown option matching IATA code
            selected = await page.evaluate("""(iata) => {
                const items = document.querySelectorAll(
                    '[role="option"], [role="group"] div'
                );
                for (const item of items) {
                    const text = (item.textContent || '').trim();
                    if (text.includes(iata) && item.offsetHeight > 0) {
                        item.click();
                        return text.slice(0, 80);
                    }
                }
                return null;
            }""", iata)

            if not selected:
                # Keyboard fallback
                await field.press("ArrowDown")
                await asyncio.sleep(0.2)
                await field.press("Enter")

            await asyncio.sleep(1.0)
            value = await field.input_value()
            if iata.upper() in value.upper():
                logger.info("Emirates: filled airport → %s", value)
                return True

            if value and len(value) > 2:
                logger.info("Emirates: airport filled with '%s' (expected %s)", value, iata)
                return True

            logger.warning("Emirates: airport fill failed for %s (got '%s')", iata, value)
            return False

        except Exception as e:
            logger.warning("Emirates: auto-suggest error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Open calendar, navigate to target month, click target day."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            logger.warning("Emirates: invalid date_from: %s", req.date_from)
            return False

        target_month = dt.strftime("%B %Y")  # e.g. "April 2026"
        target_day = str(dt.day)

        try:
            # Unstick the header so it doesn't block clicks
            await page.evaluate("""() => {
                const hdr = document.querySelector('.header-popup__wrapper--sticky');
                if (hdr) hdr.style.position = 'relative';
            }""")

            # JS-click the date input (label/header overlaps the input)
            await page.evaluate("""() => {
                const inp = document.querySelector('#date-input0, #startDate');
                if (inp) { inp.focus(); inp.click(); }
            }""")
            await asyncio.sleep(2.0)

            # Navigate calendar to target month
            for _ in range(12):
                visible_months = await page.evaluate("""() => {
                    const caps = document.querySelectorAll('.CalendarMonth_caption strong');
                    return [...caps].map(c => c.textContent).filter(Boolean);
                }""")
                if target_month in (visible_months or []):
                    break
                # Click forward button
                await page.evaluate("""() => {
                    const next = document.querySelector(
                        'button[aria-label*="forward"], .DayPickerNavigation_button:last-of-type'
                    );
                    if (next) next.click();
                }""")
                await asyncio.sleep(0.5)

            # Click the target day in the correct month
            clicked = await page.evaluate("""(args) => {
                const [targetMonth, targetDay] = args;
                const months = document.querySelectorAll('.CalendarMonth');
                for (const m of months) {
                    const cap = m.querySelector('.CalendarMonth_caption strong');
                    if (cap && cap.textContent === targetMonth) {
                        const days = m.querySelectorAll(
                            '.CalendarDay, td[role="button"], .CalendarDay__text__default'
                        );
                        for (const d of days) {
                            const text = d.textContent.trim();
                            if (text === targetDay && d.offsetHeight > 0 &&
                                !d.classList.contains('CalendarDay__blocked') &&
                                !d.classList.contains('CalendarDay__blocked_out_of_range') &&
                                d.getAttribute('aria-disabled') !== 'true') {
                                d.click();
                                return true;
                            }
                        }
                    }
                }
                return false;
            }""", [target_month, target_day])

            if not clicked:
                logger.warning("Emirates: could not click day %s in %s", target_day, target_month)
                return False

            await asyncio.sleep(1.5)
            date_val = await page.evaluate(
                "() => document.querySelector('#date-input0, #startDate')?.value"
            )
            logger.info("Emirates: date selected → %s", date_val)
            return True

        except Exception as e:
            logger.warning("Emirates: date selection error: %s", e)
            return False

    # ------------------------------------------------------------------
    # DOM scraping
    # ------------------------------------------------------------------

    async def _scrape_results(self, page) -> list[dict]:
        """Scrape flight cards from the results page DOM."""
        flights = await page.evaluate(r"""() => {
            const body = document.body?.innerText || '';
            if (body.includes('no flight options')) return [];

            // Pre-process: join lines — merge "AED\n2,155" → "AED 2,155"
            const rawLines = body.split('\n').map(l => l.trim()).filter(Boolean);
            const lines = [];
            for (let k = 0; k < rawLines.length; k++) {
                if (/^(AED|USD|EUR|GBP)$/i.test(rawLines[k]) && k + 1 < rawLines.length) {
                    // Skip intermediate blank/whitespace-only (already filtered out)
                    let next = k + 1;
                    // Skip "Lowest price" etc between currency and amount
                    while (next < rawLines.length && !/[\d,]+/.test(rawLines[next]) && next < k + 4) next++;
                    if (next < rawLines.length && /^[\d,]+$/.test(rawLines[next])) {
                        lines.push(rawLines[k] + ' ' + rawLines[next]);
                        k = next; // skip the number line
                        continue;
                    }
                }
                lines.push(rawLines[k]);
            }

            const results = [];
            let i = 0;
            while (i < lines.length) {
                // Look for time pattern HH:MM
                const timeMatch = lines[i].match(/^(\d{1,2}:\d{2})$/);
                if (timeMatch) {
                    // Potential flight card start — look for pattern:
                    // [date] dep_time [date] arr_time duration stops origin city dest city class price aircraft flight_no
                    const flight = {};
                    const depTime = timeMatch[1];

                    // Look backwards for date
                    let dateStr = '';
                    if (i > 0 && /^\w{3}\s+\d{1,2}\s+\w{3}/.test(lines[i-1])) {
                        dateStr = lines[i-1];
                    }

                    // Look forward for arrival time
                    let j = i + 1;
                    while (j < lines.length && j < i + 3) {
                        if (/^\w{3}\s+\d{1,2}\s+\w{3}/.test(lines[j])) { j++; continue; }
                        if (/^\d{1,2}:\d{2}$/.test(lines[j])) break;
                        j++;
                    }
                    if (j >= lines.length || j >= i + 3) { i++; continue; }
                    const arrTime = lines[j];

                    // Duration line
                    j++;
                    const durLine = lines[j] || '';
                    const durMatch = durLine.match(/(\d+)\s*hrs?\s*(\d+)\s*mins?/);

                    // Stops
                    j++;
                    const stopsLine = lines[j] || '';
                    const isNonstop = stopsLine.toLowerCase().includes('non-stop');

                    // Skip "Opens a dialog" etc
                    j++;
                    while (j < lines.length && /opens|dialog/i.test(lines[j])) j++;

                    // Origin IATA
                    const originIata = lines[j] || '';
                    j++;
                    const originCity = lines[j] || '';
                    j++;

                    // Destination IATA
                    const destIata = lines[j] || '';
                    j++;
                    const destCity = lines[j] || '';
                    j++;

                    // Cabin class
                    const cabinLine = lines[j] || '';
                    j++;

                    // Price line: "from" or "AED X,XXX"
                    let priceLine = '';
                    while (j < lines.length && j < i + 25) {
                        if (/AED|USD|EUR|GBP/i.test(lines[j])) {
                            priceLine = lines[j];
                            break;
                        }
                        j++;
                    }
                    const priceMatch = priceLine.match(/(AED|USD|EUR|GBP)\s*([\d,]+)/i);

                    // Flight number — look for EK###
                    let flightNo = '';
                    for (let k = j; k < Math.min(j + 8, lines.length); k++) {
                        const fnm = lines[k].match(/^(EK\d{2,4})$/i);
                        if (fnm) { flightNo = fnm[1]; break; }
                    }

                    // Aircraft type — look nearby
                    let aircraft = '';
                    for (let k = j; k < Math.min(j + 10, lines.length); k++) {
                        if (/^(A380|A350|A340|A330|A320|B777|B787|B737|77W|77L|388)$/i.test(lines[k])) {
                            aircraft = lines[k];
                            break;
                        }
                    }

                    if (flightNo && priceMatch) {
                        results.push({
                            flightNo: flightNo.toUpperCase(),
                            depTime,
                            arrTime,
                            dateStr,
                            duration: durMatch ? parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]) : 0,
                            durationText: durLine,
                            nonstop: isNonstop,
                            stops: isNonstop ? 0 : 1,
                            origin: originIata.length === 3 ? originIata : '',
                            originCity,
                            destination: destIata.length === 3 ? destIata : '',
                            destinationCity: destCity,
                            cabin: cabinLine.toLowerCase().includes('business') ? 'business'
                                 : cabinLine.toLowerCase().includes('first') ? 'first'
                                 : cabinLine.toLowerCase().includes('premium') ? 'premium_economy'
                                 : 'economy',
                            price: parseFloat(priceMatch[2].replace(/,/g, '')),
                            currency: priceMatch[1].toUpperCase(),
                            aircraft,
                        });
                    }
                }
                i++;
            }
            return results;
        }""")
        logger.info("Emirates: scraped %d flights from DOM", len(flights) if flights else 0)
        return flights or []

    # ------------------------------------------------------------------
    # Flexi-fares fallback
    # ------------------------------------------------------------------

    def _parse_flexi_fares(self, data: dict, req: FlightSearchRequest) -> list[dict]:
        """Parse flexi-fares API response as fallback when DOM is empty."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
        except (ValueError, TypeError):
            return []

        target_date = dt.strftime("%Y-%m-%d")
        currency = data.get("currency", {}).get("sale", {}).get("code", "AED")
        options = data.get("options", [])
        results = []

        for opt in options:
            travel_date = opt.get("outbound", {}).get("travelDate", "")
            if not travel_date.startswith(target_date):
                continue
            total = opt.get("priceSummary", {}).get("total", {}).get("amount", 0)
            if total <= 0:
                continue
            dep_time = travel_date[11:16] if len(travel_date) >= 16 else "00:00"
            results.append({
                "flightNo": "EK",
                "depTime": dep_time,
                "arrTime": dep_time,
                "dateStr": "",
                "duration": 0,
                "durationText": "",
                "nonstop": True,
                "stops": 0,
                "origin": req.origin,
                "originCity": "",
                "destination": req.destination,
                "destinationCity": "",
                "cabin": "economy",
                "price": float(total),
                "currency": currency,
                "aircraft": "",
            })

        return results

    # ------------------------------------------------------------------
    # Offer construction
    # ------------------------------------------------------------------

    def _build_offer(self, flight: dict, req: FlightSearchRequest) -> Optional[FlightOffer]:
        """Build a FlightOffer from scraped flight data."""
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            dep_date = dt if isinstance(dt, date) and not isinstance(dt, datetime) else dt.date() if isinstance(dt, datetime) else dt
        except (ValueError, TypeError):
            dep_date = date.today()

        dep_time = flight.get("depTime", "00:00")
        arr_time = flight.get("arrTime", "00:00")

        try:
            hm_dep = dep_time.split(":")
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day,
                              int(hm_dep[0]), int(hm_dep[1]))
        except (ValueError, IndexError):
            dep_dt = datetime(dep_date.year, dep_date.month, dep_date.day)

        try:
            hm_arr = arr_time.split(":")
            arr_dt = datetime(dep_date.year, dep_date.month, dep_date.day,
                              int(hm_arr[0]), int(hm_arr[1]))
            # Handle overnight flights
            if arr_dt <= dep_dt:
                from datetime import timedelta
                arr_dt += timedelta(days=1)
        except (ValueError, IndexError):
            arr_dt = dep_dt

        duration_min = flight.get("duration", 0)
        flight_no = flight.get("flightNo", "EK")
        origin = flight.get("origin", "") or req.origin
        destination = flight.get("destination", "") or req.destination
        price = flight.get("price", 0)
        currency = flight.get("currency", "AED")

        if price <= 0:
            return None

        offer_id = hashlib.md5(
            f"ek_{origin}_{destination}_{dep_date}_{flight_no}_{price}".encode()
        ).hexdigest()[:12]

        segment = FlightSegment(
            airline="EK",
            airline_name="Emirates",
            flight_no=flight_no,
            origin=origin,
            destination=destination,
            origin_city=flight.get("originCity", ""),
            destination_city=flight.get("destinationCity", ""),
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=duration_min * 60,
            cabin_class=flight.get("cabin", "economy"),
            aircraft=flight.get("aircraft", ""),
        )

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=duration_min * 60,
            stopovers=flight.get("stops", 0),
        )

        return FlightOffer(
            id=f"ek_{offer_id}",
            price=price,
            currency=currency,
            price_formatted=f"{currency} {price:,.0f}",
            outbound=route,
            inbound=None,
            airlines=["Emirates"],
            owner_airline="EK",
            booking_url=self._booking_url(req),
            is_locked=False,
            source="emirates_direct",
            source_tier="free",
        )

    @staticmethod
    def _booking_url(req: FlightSearchRequest) -> str:
        """Build Emirates booking deep-link via base64 searchRequest."""
        import base64, json as _json
        try:
            dt = req.date_from if isinstance(req.date_from, (datetime, date)) else datetime.strptime(str(req.date_from), "%Y-%m-%d")
            date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, 'strftime') else str(dt)
        except (ValueError, TypeError):
            date_str = ""
        search_req = {
            "journeyType": "ONEWAY",
            "bookingType": "REVENUE",
            "passengers": [{"type": "ADT", "count": req.adults or 1}],
            "segments": [
                {"departure": req.origin, "arrival": req.destination, "departureDate": date_str}
            ],
        }
        if req.children:
            search_req["passengers"].append({"type": "CHD", "count": req.children})
        if req.infants:
            search_req["passengers"].append({"type": "INF", "count": req.infants})
        encoded = base64.b64encode(_json.dumps(search_req).encode()).decode().rstrip("=")
        return f"https://www.emirates.com/booking/search-results/?searchRequest={encoded}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"emirates{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="AED",
            offers=[],
            total_results=0,
        )
