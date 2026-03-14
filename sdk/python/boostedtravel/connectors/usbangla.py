"""
US-Bangla Airlines browser connector — Playwright form flow + Zenith DOM parsing.

US-Bangla (IATA: BS) is a Bangladeshi carrier operating from Dhaka (DAC)
to domestic and international destinations in the Middle East, South Asia,
and Southeast Asia.

Strategy (browser, form flow):
  usbair.com is a Next.js SPA that links to a Zenith FrontOffice booking engine
  hosted at fo-usba.ttinteractive.com. The flow is:
    1. Navigate to usbair.com (establish cookies)
    2. Go to /book-a-flight?from_iata=X&to_iata=Y (pre-fills origin/dest)
    3. Click "Search Flights" → "Agree" dialog → navigates to Zenith
    4. Intercept navigation URL to inject correct date & pax params
    5. Wait for Zenith results page + AJAX to render flight cards
    6. Extract flights from DOM: times, duration, stops, prices

  DataDome anti-bot protects Zenith. It auto-resolves when navigating
  naturally from usbair.com (the referrer + cookie chain passes validation).
  Direct navigation to Zenith URLs without the form flow gets blocked.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from datetime import timedelta
from typing import Optional

from connectors.browser import launch_headed_browser

try:
    from connectors.browser import acquire_browser_slot, release_browser_slot
except ImportError:
    async def acquire_browser_slot(): pass
    def release_browser_slot(): pass
from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_STEALTH_INIT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
window.chrome = {runtime: {}, loadTimes: function(){}, csi: function(){}};
"""

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Zenith currency symbol → ISO code
_CURRENCY_MAP = {"$": "USD", "€": "EUR", "£": "GBP", "৳": "BDT"}


# JS that extracts structured flight data from the Zenith results DOM.
# Returns a list of {tripType, depCode, depTime, arrCode, arrTime,
#                     durationMin, stops, via, price, currency, flightNo}
_EXTRACT_JS = """
() => {
    const results = [];

    // Try main document first, then TopFrameId iframe
    let docs = [document];
    const iframe = document.querySelector('#TopFrameId');
    if (iframe && iframe.contentDocument) docs.push(iframe.contentDocument);

    for (const doc of docs) {
        // Find outbound trip (data-triptype="0")
        const trip = doc.querySelector('#trip_0, [data-triptype="0"]');
        if (!trip) continue;

        // Get origin/dest from the banner
        const banner = trip.querySelector('.selected-flight-banner-org-dst');
        const stationEls = banner ? banner.querySelectorAll('.station-name') : [];
        const depCode = stationEls[0]?.nextElementSibling?.textContent?.trim()
                     || trip.querySelector('.station-name + div')?.textContent?.trim() || '';
        const arrCodeEl = stationEls.length > 1 ? stationEls[1].parentElement.querySelector('div:not(.station-name)') : null;
        const arrCode = arrCodeEl?.textContent?.trim() || '';

        // Find all flight cards
        const cards = trip.querySelectorAll('.flight-card');
        if (cards.length === 0) continue;

        for (const card of cards) {
            const text = card.textContent || '';

            // Times: HH:MM pattern
            const timeMatches = text.match(/(\\d{2}:\\d{2})/g) || [];
            const depTime = timeMatches[0] || '';
            const arrTime = timeMatches[1] || '';

            // Duration: "05h15" or "06h00"
            const durMatch = text.match(/(\\d+)h(\\d+)/);
            const durationMin = durMatch
                ? parseInt(durMatch[1]) * 60 + parseInt(durMatch[2])
                : 0;

            // Stops
            const isDirect = /\\bDirect\\b/i.test(text);
            const stopMatch = text.match(/(\\d+)\\s*Stop/i);
            const stops = isDirect ? 0 : (stopMatch ? parseInt(stopMatch[1]) : 0);

            // Via airport
            const viaMatch = text.match(/Stop\\s+via\\s+(\\w{3})/i);
            const via = viaMatch ? viaMatch[1] : '';

            // Cheapest price from the card (first farePriceSelect or fallback)
            let price = 0;
            let currSym = '$';
            const priceModules = card.querySelectorAll('.farePriceSelect-module');
            if (priceModules.length > 0) {
                // Get the cheapest (first) fare
                const pt = priceModules[0].textContent || '';
                const pm = pt.match(/([\\$€£৳])\\s*(\\d[\\d,]*)\\s*(?:\\.(\\d+))?/);
                if (pm) {
                    currSym = pm[1];
                    price = parseFloat(pm[2].replace(/,/g, '') + (pm[3] ? '.' + pm[3] : ''));
                }
            }
            if (price === 0) {
                // Fallback: look for "Flight from: $ 267.01" pattern
                const fp = text.match(/(?:from|price)[:\\s]*([\\$€£৳])\\s*(\\d[\\d,]*)\\s*(?:\\.(\\d+))?/i);
                if (fp) {
                    currSym = fp[1];
                    price = parseFloat(fp[2].replace(/,/g, '') + (fp[3] ? '.' + fp[3] : ''));
                }
            }

            // Flight number
            const fnEl = card.querySelector('.flight-number');
            const flightNo = fnEl ? fnEl.textContent.trim() : '';

            // Seats remaining
            const seatMatch = text.match(/(\\d+)\\s*seat/i);
            const seats = seatMatch ? parseInt(seatMatch[1]) : null;

            if (depTime && arrTime && price > 0) {
                results.push({
                    depCode, arrCode, depTime, arrTime,
                    durationMin, stops, via, price,
                    currency: currSym, flightNo, seats
                });
            }
        }
        if (results.length > 0) break;  // Found flights, stop searching docs
    }
    return results;
}
"""


class USBanglaConnectorClient:
    """US-Bangla Airlines browser scraper — form flow + Zenith DOM parsing."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        await acquire_browser_slot()
        try:
            return await self._search(req)
        except Exception as e:
            logger.error("USBangla search error: %s", e)
            return self._empty(req)
        finally:
            release_browser_slot()

    async def _search(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await launch_headed_browser()
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=_UA,
            locale="en-US",
            timezone_id="Europe/London",
        )
        await context.add_init_script(_STEALTH_INIT)
        page = await context.new_page()

        target_date = req.date_from.strftime("%Y-%m-%d")
        return_date = (req.date_from + timedelta(days=1)).strftime("%Y-%m-%d")
        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0
        currency = req.currency or "USD"

        # ── Route interception: rewrite Zenith URL params ──
        intercepted = {"done": False}

        async def _rewrite_zenith(route):
            url = route.request.url
            if intercepted["done"] or "BookingEngine/SearchResult" not in url:
                await route.continue_()
                return
            intercepted["done"] = True
            url = re.sub(r"OutboundDate=[^&]+", f"OutboundDate={target_date}", url)
            url = re.sub(r"InboundDate=[^&]+", f"InboundDate={return_date}", url)
            url = re.sub(
                r"(TravelerTypes(?:%5B|\[)0(?:%5D|\])\.Value=)\d+",
                rf"\g<1>{adults}", url,
            )
            url = re.sub(
                r"(TravelerTypes(?:%5B|\[)1(?:%5D|\])\.Value=)\d+",
                rf"\g<1>{children}", url,
            )
            url = re.sub(
                r"(TravelerTypes(?:%5B|\[)2(?:%5D|\])\.Value=)\d+",
                rf"\g<1>{infants}", url,
            )
            url = re.sub(r"Currency=[^&]+", f"Currency={currency}", url)
            logger.info("USBangla: rewrote Zenith URL → %s", url[:200])
            await route.continue_(url=url)

        await page.route("**/*ttinteractive*/**", _rewrite_zenith)

        try:
            # Step 1: warm up on usbair.com
            logger.info("USBangla: loading usbair.com for %s→%s on %s",
                        req.origin, req.destination, target_date)
            await page.goto("https://usbair.com", wait_until="domcontentloaded",
                            timeout=30000)
            await asyncio.sleep(2)

            # Step 2: book-a-flight with origin/dest
            await page.goto(
                f"https://usbair.com/book-a-flight"
                f"?from_iata={req.origin}&to_iata={req.destination}",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await asyncio.sleep(2)

            # Step 3: click Search Flights
            search_btn = page.locator('button:has-text("Search Flights")')
            await search_btn.scroll_into_view_if_needed()
            await asyncio.sleep(0.3)
            await search_btn.click(timeout=5000)
            await asyncio.sleep(1.5)

            # Step 4: click Agree on the advisory dialog
            agree_btn = page.locator('button:has-text("Agree")')
            try:
                await agree_btn.wait_for(state="visible", timeout=5000)
                await agree_btn.click()
                logger.info("USBangla: clicked Agree, navigating to Zenith")
            except Exception:
                logger.warning("USBangla: Agree dialog not found, trying to continue")

            # Step 5: wait for Zenith page
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            try:
                await page.wait_for_url("**ttinteractive**", timeout=remaining * 1000)
            except Exception:
                logger.warning("USBangla: Zenith page didn't load (URL: %s)", page.url)
                return self._empty(req)

            # Step 6: wait for AJAX flight data to render
            deadline = t0 + self.timeout
            flights_data = []
            for _ in range(8):
                if time.monotonic() > deadline:
                    break
                await asyncio.sleep(2)
                flights_data = await page.evaluate(_EXTRACT_JS)
                if flights_data:
                    break

            if not flights_data:
                # Check for DataDome / CAPTCHA
                body_text = await page.evaluate(
                    "() => (document.body?.innerText || '').substring(0, 500)"
                )
                if "blocked" in body_text.lower() or "captcha" in body_text.lower():
                    logger.warning("USBangla: DataDome CAPTCHA blocked")
                else:
                    logger.warning("USBangla: no flights found in DOM")
                return self._empty(req)

            # Step 7: build offers
            offers = self._build_offers(flights_data, req)
            elapsed = time.monotonic() - t0
            offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

            logger.info(
                "USBangla %s→%s returned %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"usbangla{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else currency,
                offers=offers,
                total_results=len(offers),
            )

        finally:
            try:
                await browser.close()
            except Exception:
                pass

    # ── Offer building ──────────────────────────────────────────────────────

    def _build_offers(
        self, flights_data: list[dict], req: FlightSearchRequest
    ) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        target_date = req.date_from

        for i, fd in enumerate(flights_data):
            dep_time_str = fd.get("depTime", "")
            arr_time_str = fd.get("arrTime", "")
            if not dep_time_str or not arr_time_str:
                continue

            dep_code = fd.get("depCode") or req.origin
            arr_code = fd.get("arrCode") or req.destination
            flight_no = fd.get("flightNo", "")
            price = fd.get("price", 0)
            curr_sym = fd.get("currency", "$")
            duration_min = fd.get("durationMin", 0)
            stops = fd.get("stops", 0)
            via = fd.get("via", "")
            seats = fd.get("seats")

            iso_currency = _CURRENCY_MAP.get(curr_sym, "USD")

            # Parse times
            try:
                dep_h, dep_m = map(int, dep_time_str.split(":"))
                arr_h, arr_m = map(int, arr_time_str.split(":"))
            except ValueError:
                continue

            from datetime import datetime

            dep_dt = datetime(
                target_date.year, target_date.month, target_date.day,
                dep_h, dep_m,
            )
            # Arrival might be next day if arr < dep
            arr_dt = datetime(
                target_date.year, target_date.month, target_date.day,
                arr_h, arr_m,
            )
            if arr_dt <= dep_dt:
                arr_dt += timedelta(days=1)

            duration_sec = duration_min * 60 if duration_min else int(
                (arr_dt - dep_dt).total_seconds()
            )

            # Build segments
            if stops == 0:
                segments = [
                    FlightSegment(
                        airline="BS",
                        airline_name="US-Bangla Airlines",
                        flight_no=flight_no or f"BS{100 + i}",
                        origin=dep_code,
                        destination=arr_code,
                        departure=dep_dt,
                        arrival=arr_dt,
                        duration_seconds=duration_sec,
                    )
                ]
            else:
                # Multi-segment: split at via point
                mid_code = via or "CGP"
                mid_time_dep = dep_dt + timedelta(seconds=duration_sec // 2)
                mid_time_arr = mid_time_dep - timedelta(minutes=30)
                segments = [
                    FlightSegment(
                        airline="BS",
                        airline_name="US-Bangla Airlines",
                        flight_no=flight_no or f"BS{100 + i}",
                        origin=dep_code,
                        destination=mid_code,
                        departure=dep_dt,
                        arrival=mid_time_arr,
                        duration_seconds=duration_sec // 2,
                    ),
                    FlightSegment(
                        airline="BS",
                        airline_name="US-Bangla Airlines",
                        flight_no=f"BS{200 + i}",
                        origin=mid_code,
                        destination=arr_code,
                        departure=mid_time_dep,
                        arrival=arr_dt,
                        duration_seconds=duration_sec // 2,
                    ),
                ]

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=duration_sec,
                stopovers=stops,
            )

            offer_key = f"BS_{dep_time_str}_{arr_time_str}_{i}"
            offer_hash = hashlib.md5(offer_key.encode()).hexdigest()[:10]

            booking_date = target_date.strftime("%d/%m/%Y")
            booking_url = (
                f"https://usbair.com/book-a-flight"
                f"?from_iata={dep_code}&to_iata={arr_code}"
                f"&departureDate={booking_date}&adults={req.adults}&journeyType=oneway"
            )

            offers.append(
                FlightOffer(
                    id=f"usbangla_{offer_hash}",
                    price=price,
                    currency=iso_currency,
                    price_formatted=f"{curr_sym}{price:.2f}",
                    outbound=route,
                    airlines=["BS"],
                    owner_airline="BS",
                    availability_seats=seats,
                    source="usbangla_direct",
                    source_tier="protocol",
                    is_locked=True,
                    booking_url=booking_url,
                )
            )

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"usbangla{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency or "USD",
            offers=[],
            total_results=0,
        )
