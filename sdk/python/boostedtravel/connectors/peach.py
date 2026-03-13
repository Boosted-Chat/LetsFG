"""
Peach Aviation CDP Chrome scraper — API interception + DOM extraction fallback.

Peach Aviation (IATA: MM) is a Japanese LCC (ANA group).
Booking site: booking.flypeach.com

Strategy (updated Mar 2026):
1. Launch real system Chrome via CDP (persistent, survives across searches)
2. Set up API response interception to capture flight JSON from XHR calls
3. Build direct search URL with JSON params (bypasses homepage form)
4. Navigate to booking.flypeach.com/en/getsearch?s=[params]
5. Click "Search by One-way" to submit
6. If API interception captured JSON flight data → parse directly
7. Fallback: extract flight data from server-rendered DOM
8. Parse → FlightOffer objects

Real Chrome passes reCAPTCHA better than Playwright's bundled Chromium.
Persistent browser avoids ~5s launch overhead per search.
API interception is faster and more reliable than DOM extraction alone.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import re
import subprocess
import time
import urllib.parse
from datetime import datetime, timedelta
from typing import Any, Optional

from boostedtravel.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from boostedtravel.connectors.browser import stealth_args, stealth_popen_kwargs

logger = logging.getLogger(__name__)

_CDP_PORT = 9468
_USER_DATA_DIR = os.path.join(os.environ.get("TEMP", "/tmp"), "peach_cdp_data")
_CHROME_PATHS = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    "/usr/bin/google-chrome",
    "/usr/bin/google-chrome-stable",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
]

_chrome_proc: subprocess.Popen | None = None
_pw_instance = None
_cdp_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _find_chrome() -> str:
    for p in _CHROME_PATHS:
        if os.path.isfile(p):
            return p
    raise FileNotFoundError("Chrome not found")


def _launch_chrome():
    global _chrome_proc
    if _chrome_proc and _chrome_proc.poll() is None:
        return
    os.makedirs(_USER_DATA_DIR, exist_ok=True)
    chrome = _find_chrome()
    _chrome_proc = subprocess.Popen(
        [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--disable-blink-features=AutomationControlled",
            "--no-first-run", "--no-default-browser-check",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            *stealth_args(),
        ],
        **stealth_popen_kwargs(),
    )
    logger.info("Peach: Chrome launched on CDP port %d (pid=%d)", _CDP_PORT, _chrome_proc.pid)


async def _get_browser():
    global _pw_instance, _cdp_browser
    lock = _get_lock()
    async with lock:
        if _cdp_browser and _cdp_browser.is_connected():
            return _cdp_browser
        _launch_chrome()
        await asyncio.sleep(2)
        from playwright.async_api import async_playwright
        if not _pw_instance:
            _pw_instance = await async_playwright().start()
        for attempt in range(5):
            try:
                _cdp_browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://127.0.0.1:{_CDP_PORT}"
                )
                logger.info("Peach: connected to Chrome via CDP")
                return _cdp_browser
            except Exception:
                if attempt < 4:
                    await asyncio.sleep(1)
        raise RuntimeError(f"Peach: cannot connect to Chrome CDP on port {_CDP_PORT}")


class PeachConnectorClient:
    """Peach Aviation scraper — API interception + DOM extraction fallback."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = None
        try:
            page = await context.new_page()

            # ── API response interception ──────────────────────────────
            api_event = asyncio.Event()
            all_captured: list[Any] = []

            async def on_response(response):
                try:
                    url = response.url.lower()
                    if response.status == 200 and (
                        "flight_search" in url
                        or "availability" in url
                        or "search_result" in url
                        or "/api/" in url
                        or "flights" in url
                        or "fare" in url
                        or "low_fare" in url
                        or ("flypeach.com" in url and "search" in url)
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct or "javascript" in ct:
                            data = await response.json()
                            if data and isinstance(data, (dict, list)):
                                logger.debug(
                                    "Peach API intercept: url=%s keys=%s",
                                    response.url[:120],
                                    list(data.keys())[:10] if isinstance(data, dict) else f"list[{len(data)}]",
                                )
                                all_captured.append(data)
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            search_url = self._build_search_url(req)
            logger.info("Peach: navigating to booking URL for %s→%s on %s",
                        req.origin, req.destination, req.date_from.strftime("%Y/%m/%d"))

            # Step 1: Navigate to getsearch URL to set session data
            # Use full timeout — the site can be slow to respond
            nav_timeout = int(self.timeout * 1000)
            for attempt in range(2):
                try:
                    await page.goto(search_url, wait_until="domcontentloaded",
                                    timeout=nav_timeout)
                    break
                except Exception as e:
                    if attempt == 0:
                        logger.warning("Peach: first navigation attempt failed (%s), retrying…", e)
                        await asyncio.sleep(random.uniform(1.0, 3.0))
                    else:
                        raise
            await asyncio.sleep(random.uniform(1.0, 2.0))

            # Step 2: Navigate to the search form page (pre-filled from session)
            await page.goto("https://booking.flypeach.com/en/search",
                            wait_until="domcontentloaded", timeout=nav_timeout)
            await asyncio.sleep(random.uniform(1.0, 2.0))

            # Step 3: Click "Search by One-way" to submit — bypasses reCAPTCHA
            try:
                one_way_link = page.get_by_role("link", name=re.compile(r"Search by One-way", re.IGNORECASE))
                await one_way_link.click(timeout=15000)
                logger.info("Peach: clicked 'Search by One-way'")
            except Exception as e:
                logger.warning("Peach: could not click one-way search (%s), trying alternate selectors", e)
                # Fallback: try other selectors for the one-way search button
                try:
                    alt = page.locator("a:has-text('One-way'), button:has-text('One-way'), [data-testid*='one-way']").first
                    await alt.click(timeout=10000)
                    logger.info("Peach: clicked one-way via fallback selector")
                except Exception:
                    logger.warning("Peach: all one-way click attempts failed")
                    return self._empty(req)

            # Wait for flight results page
            try:
                await page.wait_for_url("**/flight_search**", timeout=30000)
                logger.info("Peach: reached flight_search page")
            except Exception:
                if "flight_search" not in page.url:
                    logger.warning("Peach: did not reach flight_search (at %s)", page.url)
                    return self._empty(req)

            await asyncio.sleep(random.uniform(2.0, 3.0))

            # ── Try API-intercepted data first ─────────────────────────
            flights_data = None
            if all_captured:
                logger.info("Peach: intercepted %d API responses, parsing…", len(all_captured))
                flights_data = self._parse_api_responses(all_captured)

            # ── Fallback: DOM extraction ───────────────────────────────
            if not flights_data:
                # Wait briefly for any late API responses
                try:
                    await asyncio.wait_for(api_event.wait(), timeout=3.0)
                except asyncio.TimeoutError:
                    pass

                if all_captured:
                    flights_data = self._parse_api_responses(all_captured)

                if not flights_data:
                    logger.info("Peach: no API data captured, falling back to DOM extraction")
                    flights_data = await self._extract_flights_from_dom(page)

            if not flights_data:
                logger.warning("Peach: no flights extracted")
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._build_offers(flights_data, req)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Peach error: %s", e)
            return self._empty(req)
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # URL building
    # ------------------------------------------------------------------

    @staticmethod
    def _build_search_url(req: FlightSearchRequest) -> str:
        """Build direct booking search URL with JSON params."""
        params = [{
            "departure_date": req.date_from.strftime("%Y/%m/%d"),
            "departure_airport_code": req.origin,
            "arrival_airport_code": req.destination,
            "is_return": False,
        }]
        json_str = json.dumps(params, separators=(",", ":"))
        encoded = urllib.parse.quote(json_str)
        return f"https://booking.flypeach.com/en/getsearch?s={encoded}"

    # ------------------------------------------------------------------
    # API response parsing
    # ------------------------------------------------------------------

    def _parse_api_responses(self, captured: list[Any]) -> list[dict]:
        """Parse flight data from intercepted API JSON responses.

        Peach's booking site may return flight data in various JSON
        structures depending on the endpoint.  We look for common keys
        (flights, journeys, itineraries, segments, departure/arrival
        times, prices) and normalise to the same dict format used by
        DOM extraction.
        """
        flights: list[dict] = []
        seen: set[str] = set()

        for data in captured:
            try:
                items = self._extract_flights_from_json(data)
                for f in items:
                    key = f.get("flight_no", "") + "_" + f.get("dep_time", "")
                    if key not in seen:
                        seen.add(key)
                        flights.append(f)
            except Exception as exc:
                logger.debug("Peach: could not parse captured response: %s", exc)

        if flights:
            logger.info("Peach: parsed %d flights from API interception", len(flights))
        return flights

    def _extract_flights_from_json(self, data: Any) -> list[dict]:
        """Recursively search JSON for flight-like objects and normalise."""
        results: list[dict] = []

        if isinstance(data, list):
            for item in data:
                results.extend(self._extract_flights_from_json(item))
            return results

        if not isinstance(data, dict):
            return results

        # Try to detect a flight object — must have a flight number and times
        flight_no = ""
        for key in ("flightNumber", "flight_number", "flightNo", "flight_no",
                     "designator", "identifier", "number"):
            val = data.get(key)
            if isinstance(val, str) and re.match(r"MM\d{2,4}$", val):
                flight_no = val
                break
            if isinstance(val, dict):
                for sub in ("identifier", "carrierCode", "flightNumber"):
                    sv = val.get(sub, "")
                    if isinstance(sv, str) and re.match(r"MM\d{2,4}$", sv):
                        flight_no = sv
                        break
                carrier = val.get("carrierCode", "")
                ident = val.get("identifier", "")
                if carrier == "MM" and ident:
                    flight_no = f"MM{ident}"

        dep_time = ""
        arr_time = ""
        for dk in ("departure", "departureTime", "departure_time", "std",
                    "dep_time", "departureDate"):
            v = data.get(dk, "")
            if isinstance(v, str) and v:
                m = re.search(r"(\d{2}:\d{2})", v)
                if m:
                    dep_time = m.group(1)
                    break
        for ak in ("arrival", "arrivalTime", "arrival_time", "sta",
                    "arr_time", "arrivalDate"):
            v = data.get(ak, "")
            if isinstance(v, str) and v:
                m = re.search(r"(\d{2}:\d{2})", v)
                if m:
                    arr_time = m.group(1)
                    break

        # Check nested designator (common in Navitaire/LCC systems)
        designator = data.get("designator", {})
        if isinstance(designator, dict):
            if not dep_time:
                dv = designator.get("departure", "")
                if isinstance(dv, str):
                    m = re.search(r"(\d{2}:\d{2})", dv)
                    if m:
                        dep_time = m.group(1)
            if not arr_time:
                av = designator.get("arrival", "")
                if isinstance(av, str):
                    m = re.search(r"(\d{2}:\d{2})", av)
                    if m:
                        arr_time = m.group(1)

        # Extract prices
        prices: list[int] = []
        for pk in ("price", "amount", "totalPrice", "total_price", "fare",
                    "basePrice", "adultFare"):
            v = data.get(pk)
            if isinstance(v, (int, float)) and v > 0:
                prices.append(int(v))

        # Check nested fares/prices arrays
        for fk in ("fares", "fareAvailabilities", "prices", "farePrices"):
            farr = data.get(fk)
            if isinstance(farr, list):
                for fi in farr:
                    if isinstance(fi, dict):
                        for ppk in ("price", "amount", "total", "fareAmount",
                                     "adultFare", "passengerFare"):
                            pv = fi.get(ppk)
                            if isinstance(pv, (int, float)) and pv > 0:
                                prices.append(int(pv))

        # Duration
        duration_mins = 0
        for dur_key in ("duration", "flightDuration", "segmentDuration",
                        "totalDuration"):
            dv = data.get(dur_key, "")
            if isinstance(dv, (int, float)) and dv > 0:
                # Could be minutes or seconds
                duration_mins = int(dv) if dv < 1440 else int(dv / 60)
                break
            if isinstance(dv, str):
                hm = re.search(r"(\d+)[Hh:](\d+)", dv)
                if hm:
                    duration_mins = int(hm.group(1)) * 60 + int(hm.group(2))
                    break

        if flight_no and dep_time and arr_time:
            results.append({
                "flight_no": flight_no,
                "aircraft": data.get("aircraft", data.get("equipmentType", "")),
                "dep_time": dep_time,
                "arr_time": arr_time,
                "duration_mins": duration_mins,
                "prices": prices,
                "seats": [],
            })
            return results

        # Recurse into child arrays/dicts that may contain flights
        for key in ("flights", "journeys", "segments", "itineraries",
                     "schedules", "results", "data", "outbound",
                     "departureRouteList", "trips", "legs"):
            child = data.get(key)
            if isinstance(child, (list, dict)):
                results.extend(self._extract_flights_from_json(child))

        return results

    # ------------------------------------------------------------------
    # DOM extraction
    # ------------------------------------------------------------------

    async def _extract_flights_from_dom(self, page) -> list[dict]:
        """Extract flight data from Peach's server-rendered results page.

        DOM structure per flight row (observed from live site):
        - paragraph with flight number (MM307) and aircraft type (A320)
        - time elements: departure HH:MM, arrow, arrival HH:MM  
        - duration text (1Hour30Min(s))
        - fare cells with prices (￥3,990) and seats info (e.g. "4 seats left at this price")
        - Three fare tiers: Minimum, Standard, Standard Plus
        """
        return await page.evaluate(r"""() => {
            const results = [];
            const body = document.body.innerText || '';

            // Find all flight number occurrences (MM + 2-4 digits)
            const flightNoRegex = /MM\d{2,4}/g;
            const allMatches = [...body.matchAll(flightNoRegex)];
            const uniqueFlights = [...new Set(allMatches.map(m => m[0]))];

            // For each unique flight, find its containing element and extract data
            for (const flightNo of uniqueFlights) {
                // Find the paragraph element containing this flight number
                const fnEls = [];
                document.querySelectorAll('p').forEach(p => {
                    if (p.textContent.trim() === flightNo) fnEls.push(p);
                });
                if (fnEls.length === 0) continue;
                const fnEl = fnEls[0];

                // Walk up to find the flight row container
                let row = fnEl;
                for (let i = 0; i < 10; i++) {
                    if (!row.parentElement) break;
                    row = row.parentElement;
                    // Flight row has multiple direct children (flight info, times, fares)
                    if (row.children.length >= 4) break;
                }

                const text = row.innerText || '';

                // Aircraft type: sibling or nearby paragraph with A3XX/B7XX pattern
                let aircraft = '';
                const nearbyPs = row.querySelectorAll('p');
                nearbyPs.forEach(p => {
                    const t = p.textContent.trim();
                    if (/^[AB]\d{3}/.test(t)) aircraft = t;
                });

                // Times (HH:MM pattern)
                const timeMatches = text.match(/(\d{2}:\d{2})/g) || [];

                // Prices: ￥ followed by digits with commas
                const priceMatches = text.match(/￥([\d,]+)/g) || [];
                const prices = priceMatches.map(
                    p => parseInt(p.replace(/[￥,]/g, ''))
                );

                // Seats remaining ("N seats left")
                const seatMatches = [...text.matchAll(/(\d+)\s*seats?\s*left/gi)];
                const seats = seatMatches.map(m => parseInt(m[1]));

                // Duration (e.g. "1Hour30Min(s)")
                let durationMins = 0;
                const durMatch = text.match(/(\d+)\s*Hour\s*(\d+)\s*Min/i);
                if (durMatch) {
                    durationMins = parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]);
                }

                if (flightNo && timeMatches.length >= 2) {
                    results.push({
                        flight_no: flightNo,
                        aircraft: aircraft,
                        dep_time: timeMatches[0],
                        arr_time: timeMatches[1],
                        duration_mins: durationMins,
                        prices: prices,
                        seats: seats,
                    });
                }
            }

            return results;
        }""")

    # ------------------------------------------------------------------
    # Offer building
    # ------------------------------------------------------------------

    def _build_offers(self, flights_data: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)

        for flight in flights_data:
            prices = [p for p in flight.get("prices", []) if p > 0]
            if not prices:
                continue
            best_price = min(prices)

            flight_no = flight.get("flight_no", "")
            dep_time = flight.get("dep_time", "")
            arr_time = flight.get("arr_time", "")
            duration_mins = flight.get("duration_mins", 0)

            dep_dt = self._time_on_date(dep_time, req.date_from)
            arr_dt = self._time_on_date(arr_time, req.date_from)

            if arr_dt < dep_dt:
                arr_dt += timedelta(days=1)

            total_dur = (
                duration_mins * 60
                if duration_mins
                else max(int((arr_dt - dep_dt).total_seconds()), 0)
            )

            seg = FlightSegment(
                airline="MM",
                airline_name="Peach Aviation",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                cabin_class="M",
            )

            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=max(total_dur, 0),
                stopovers=0,
            )

            fkey = f"{flight_no}_{dep_dt.isoformat()}"
            offers.append(FlightOffer(
                id=f"mm_{hashlib.md5(fkey.encode()).hexdigest()[:12]}",
                price=round(best_price, 2),
                currency="JPY",
                price_formatted=f"¥{best_price:,.0f}",
                outbound=route,
                inbound=None,
                airlines=["Peach Aviation"],
                owner_airline="MM",
                booking_url=booking_url,
                is_locked=False,
                source="peach_direct",
                source_tier="free",
            ))

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _time_on_date(time_str: str, date) -> datetime:
        """Combine HH:MM string with a date into a datetime."""
        if not time_str:
            return datetime(2000, 1, 1)
        try:
            h, m = time_str.split(":")
            return datetime(date.year, date.month, date.day, int(h), int(m))
        except (ValueError, IndexError):
            return datetime(2000, 1, 1)

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Peach %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"peach{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="JPY", offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        params = json.dumps([{
            "departure_date": req.date_from.strftime("%Y/%m/%d"),
            "departure_airport_code": req.origin,
            "arrival_airport_code": req.destination,
            "is_return": False,
        }], separators=(",", ":"))
        return f"https://booking.flypeach.com/en/getsearch?s={urllib.parse.quote(params)}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"peach{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="JPY", offers=[], total_results=0,
        )
