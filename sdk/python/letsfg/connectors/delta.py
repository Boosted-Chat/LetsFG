"""
Delta Air Lines connector -- CDP Chrome + form fill + GraphQL API interception.

Delta (IATA: DL) is the #2 US carrier with ~5,400 daily flights to 300+
destinations in 50+ countries.  ATL hub is the world's busiest airport.

Website: delta.com -- Angular SPA with Akamai Bot Manager protection.

Strategy (CDP Chrome + API response interception):
1. Launch real Chrome via CDP (same flags as American/Turkish -- no automation detection).
2. For each search: new page -> navigate to booking form -> fill origin, destination,
   date, one-way -> click "Find Flights".
3. Page navigates to /flightsearch/search-results?cacheKeySuffix={uuid}.
4. Intercept the POST response from offer-api-prd.delta.com/prd/rm-offer-gql
   which returns full GraphQL JSON (520 KB+) with trips, segments, and pricing.
5. Parse gqlOffersSets -> trips + offers -> FlightOffer objects.

API details (discovered Mar 2026):
  POST https://offer-api-prd.delta.com/prd/rm-offer-gql
  Auth: "GUEST" (no tokens needed, but request originates from browser session)
  Response: {data: {gqlSearchOffers: {gqlOffersSets: [{trips: [...], offers: [...]}]}}}
  Price path: offers[].offerItems[0].retailItems[0].retailItemMetaData
              .fareInformation[0].farePrice[0].totalFarePrice
              .currencyEquivalentPrice.roundedCurrencyAmt
  Brand IDs: BMAIN (Basic Economy), CMAIN (Main Cabin), CDCP (Comfort+),
             CFIRST (First Class)

No geo-blocking -- works from EU without proxy.  Auto-detects locale (/eu/en).
Set DELTA_PROXY env var if needed.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import platform
import random
import subprocess
import time
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

from models.flights import (
    AirlineSummary,
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

# -- Chrome flags (Akamai-safe, no automation detection) ---------------
_CHROME_FLAGS = [
    "--disable-field-trial-config",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-back-forward-cache",
    "--disable-breakpad",
    "--disable-client-side-phishing-detection",
    "--disable-component-extensions-with-background-pages",
    "--disable-component-update",
    "--no-default-browser-check",
    "--disable-default-apps",
    "--disable-dev-shm-usage",
    "--disable-features=AvoidUnnecessaryBeforeUnloadCheckSync,"
    "BoundaryEventDispatchTracksNodeRemoval,DestroyProfileOnBrowserClose,"
    "DialMediaRouteProvider,GlobalMediaControls,HttpsUpgrades,LensOverlay,"
    "MediaRouter,PaintHolding,ThirdPartyStoragePartitioning,Translate,"
    "AutoDeElevate,RenderDocument,OptimizationHints,AutomationControlled",
    "--enable-features=CDPScreenshotNewSurface",
    "--allow-pre-commit-input",
    "--disable-hang-monitor",
    "--disable-ipc-flooding-protection",
    "--disable-popup-blocking",
    "--disable-prompt-on-repost",
    "--disable-renderer-backgrounding",
    "--force-color-profile=srgb",
    "--metrics-recording-only",
    "--no-first-run",
    "--password-store=basic",
    "--no-service-autorun",
    "--disable-search-engine-choice-screen",
    "--disable-infobars",
    "--disable-sync",
    "--enable-unsafe-swiftshader",
    "--window-position=-2400,-2400",
    "--window-size=1366,768",
]

_MAX_ATTEMPTS = 3
_RESULTS_WAIT = 25  # seconds to wait for API response after clicking search

# -- Shared CDP Chrome state -------------------------------------------
_CDP_PORT = 9472
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", "/tmp"), "chrome-cdp-delta"
)
_chrome_proc: Optional[subprocess.Popen] = None
_browser = None
_pw_instance = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_proxy() -> Optional[dict]:
    raw = os.environ.get("DELTA_PROXY", "").strip()
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


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch real Chrome via CDP (no Playwright automation flags)."""
    global _chrome_proc, _browser, _pw_instance
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    return _browser
            except Exception:
                pass
            _browser = None

        from playwright.async_api import async_playwright

        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass

        _pw_instance = await async_playwright().start()

        # Try connecting to existing Chrome first
        try:
            _browser = await _pw_instance.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_CDP_PORT}"
            )
            logger.info("Delta: connected to existing Chrome on port %d", _CDP_PORT)
            return _browser
        except Exception:
            pass

        # Launch real Chrome subprocess
        from connectors.browser import find_chrome
        chrome = find_chrome()
        os.makedirs(_USER_DATA_DIR, exist_ok=True)

        args = [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            *_CHROME_FLAGS,
            "about:blank",
        ]

        popen_kw: dict = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
        if platform.system() == "Windows":
            si = subprocess.STARTUPINFO()
            si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            si.wShowWindow = 7  # SW_SHOWMINNOACTIVE
            popen_kw["startupinfo"] = si
            popen_kw["creationflags"] = subprocess.CREATE_NO_WINDOW

        _chrome_proc = subprocess.Popen(args, **popen_kw)
        await asyncio.sleep(2.5)

        _browser = await _pw_instance.chromium.connect_over_cdp(
            f"http://127.0.0.1:{_CDP_PORT}"
        )
        logger.info(
            "Delta: Chrome ready via CDP (port %d, pid %d)",
            _CDP_PORT, _chrome_proc.pid,
        )
        return _browser


async def _reset_browser():
    """Close browser + Chrome process (called after persistent failures)."""
    global _browser, _chrome_proc, _pw_instance
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                await _browser.close()
            except Exception:
                pass
            _browser = None
        if _chrome_proc:
            try:
                _chrome_proc.terminate()
            except Exception:
                pass
            _chrome_proc = None
        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass
            _pw_instance = None


# -- Connector ---------------------------------------------------------

class DeltaConnectorClient:
    """Delta Air Lines scraper -- CDP Chrome + form fill + GraphQL API interception.

    Real Chrome launched once and reused.  Each search takes ~10-15s.
    """

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(
        self, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        t0 = time.monotonic()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                result = await self._attempt_search(req, t0)
                if result is not None:
                    return result
            except Exception as e:
                logger.warning(
                    "Delta: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e
                )
                if "closed" in str(e).lower() or "disconnected" in str(e).lower():
                    await _reset_browser()

        return self._empty(req)

    # -- Single search attempt -----------------------------------------

    async def _attempt_search(
        self, req: FlightSearchRequest, t0: float,
    ) -> Optional[FlightSearchResponse]:
        """Fresh page -> form fill -> click Find Flights -> intercept GraphQL."""
        browser = await _get_browser()
        ctx = browser.contexts[0]
        page = await ctx.new_page()

        # Block OneTrust / cookie consent resources
        async def _abort_route(route):
            await route.abort()

        for pattern in ("**/*onetrust*", "**/*cookielaw*", "**/*optanon*"):
            try:
                await page.route(pattern, _abort_route)
            except Exception:
                pass

        # Inject init script to remove OneTrust overlay
        try:
            await page.add_init_script("""
                (function() {
                    const kill = () => {
                        const sdk = document.getElementById('onetrust-consent-sdk');
                        if (sdk) sdk.remove();
                        document.querySelectorAll('.onetrust-pc-dark-filter, .ot-fade-in').forEach(e => e.remove());
                        if (document.body) document.body.style.overflow = 'auto';
                    };
                    kill();
                    const obs = new MutationObserver(kill);
                    if (document.body) {
                        obs.observe(document.body, { childList: true, subtree: true });
                    } else {
                        document.addEventListener('DOMContentLoaded', () => {
                            kill();
                            obs.observe(document.body, { childList: true, subtree: true });
                        });
                    }
                })();
            """)
        except Exception:
            pass

        # Set up passive response interception for the offer API
        api_response_body: list[str] = []
        api_response_event = asyncio.Event()

        async def _on_response(response):
            if "offer-api-prd.delta.com" in response.url:
                try:
                    body = await response.text()
                    if len(body) > 1000:
                        api_response_body.append(body)
                        api_response_event.set()
                except Exception:
                    pass

        page.on("response", _on_response)

        try:
            logger.info(
                "Delta: searching %s->%s on %s",
                req.origin, req.destination, req.date_from,
            )

            await page.goto(
                "https://www.delta.com/",
                wait_until="domcontentloaded",
                timeout=30000,
            )

            # Wait for booking form (classic Angular form with #fromAirportName)
            form_type = "none"
            for _w in range(20):
                has_classic = await page.evaluate(
                    "() => !!document.querySelector('#fromAirportName')"
                )
                if has_classic:
                    form_type = "classic"
                    break
                has_modern = await page.evaluate(
                    "() => !!document.querySelector('#findFilghtsCta')"
                )
                if has_modern:
                    form_type = "modern"
                    break
                await asyncio.sleep(1)
            await asyncio.sleep(2.0)

            if form_type == "none":
                logger.warning("Delta: no booking form found after 20s")
                return None

            # Dismiss cookie consent / overlays
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[id*="onetrust"], .onetrust-pc-dark-filter, .ot-fade-in'
                ).forEach(e => e.remove());
                if (document.body) document.body.style.overflow = 'auto';
            }""")

            # Mouse movement to build Akamai sensor data
            for _ in range(3):
                x = random.randint(200, 1000)
                y = random.randint(100, 500)
                await page.mouse.move(x, y, steps=random.randint(5, 10))
                await asyncio.sleep(random.uniform(0.2, 0.5))

            # -- Fill origin -------------------------------------------
            if form_type == "classic":
                ok = await self._fill_airport(page, "origin", req.origin)
            else:
                ok = await self._fill_airport_modern(page, "origin", req.origin)
            if not ok:
                logger.warning("Delta: origin fill failed")
                return None
            await asyncio.sleep(0.4)

            # -- Fill destination --------------------------------------
            if form_type == "classic":
                ok = await self._fill_airport(page, "destination", req.destination)
            else:
                ok = await self._fill_airport_modern(page, "destination", req.destination)
            if not ok:
                logger.warning("Delta: destination fill failed")
                return None
            await asyncio.sleep(0.4)

            # -- Select One Way trip type ------------------------------
            if form_type == "classic":
                await self._select_one_way(page)
            else:
                await self._select_one_way_modern(page)
            await asyncio.sleep(0.3)

            # -- Fill departure date -----------------------------------
            if form_type == "classic":
                await self._fill_date(page, req)
            else:
                await self._fill_date_modern(page, req)
            await asyncio.sleep(0.3)

            # -- Click SEARCH ------------------------------------------
            if form_type == "classic":
                submit = page.locator('#btn-book-submit').first
            else:
                submit = page.locator('#findFilghtsCta').first
            await submit.click(timeout=5000)

            # -- Wait for API response ---------------------------------
            try:
                await asyncio.wait_for(
                    api_response_event.wait(), timeout=_RESULTS_WAIT
                )
            except asyncio.TimeoutError:
                logger.warning("Delta: offer API response timed out")
                return None

            # Give a moment for any additional responses
            await asyncio.sleep(3.0)

            # Parse the captured API response
            if not api_response_body:
                logger.warning("Delta: no API response captured")
                return None

            raw = max(api_response_body, key=len)
            data = json.loads(raw)

        except asyncio.TimeoutError:
            logger.warning("Delta: search timed out")
            return None
        except Exception as e:
            logger.warning("Delta: search error: %s", e)
            return None
        finally:
            page.remove_listener("response", _on_response)
            try:
                await page.close()
            except Exception:
                pass

        elapsed = time.monotonic() - t0
        offers = self._parse_response(data, req)
        return self._build_response(offers, req, elapsed)

    # -- Form helpers --------------------------------------------------

    async def _select_one_way(self, page) -> None:
        """Select 'One Way' from the Trip Type combobox."""
        try:
            combo = page.get_by_role("combobox", name="Trip Type")
            text = await combo.inner_text()
            if "One Way" in text:
                return

            await combo.click(timeout=3000)
            await asyncio.sleep(0.5)
            # Click One Way within the trip-type listbox only
            await page.evaluate("""() => {
                const opts = document.querySelectorAll(
                    '#selectTripType-desc [role="option"]'
                );
                for (const o of opts) {
                    if (o.textContent.includes('One Way')) {
                        o.click(); break;
                    }
                }
            }""")
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.debug("Delta: trip type selection error: %s", e)

    async def _fill_airport(self, page, field_type: str, iata: str) -> bool:
        """Fill origin or destination airport field via modal lookup."""
        try:
            # Classic Angular form: <a id="fromAirportName"> / <a id="toAirportName">
            link_id = "fromAirportName" if field_type == "origin" else "toAirportName"
            link = page.locator(f"#{link_id}")
            await link.click(timeout=5000)
            await asyncio.sleep(1.5)

            # Modal opens with #search_input
            search_input = page.locator("#search_input")
            await search_input.wait_for(state="visible", timeout=5000)
            await search_input.fill("")
            await asyncio.sleep(0.2)
            await search_input.press_sequentially(iata, delay=120)
            await asyncio.sleep(2.5)

            # Click matching airport link in the modal popup
            airport_link = page.locator(
                "modal-container a, .airport-lookup a"
            ).filter(has_text=iata).first
            await airport_link.click(timeout=5000)
            await asyncio.sleep(0.8)
            return True
        except Exception as e:
            logger.debug("Delta: airport fill '%s' error: %s", field_type, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> None:
        """Select departure date from the dl-datepicker calendar."""
        try:
            target_month = req.date_from.strftime("%B")  # e.g. "April"
            target_year = str(req.date_from.year)         # e.g. "2026"
            target_day = str(req.date_from.day)           # e.g. "15"

            # Open the calendar
            cal_trigger = page.locator("#input_departureDate_1")
            await cal_trigger.click(timeout=5000)
            await asyncio.sleep(1.0)

            # Navigate to the correct month
            for _ in range(12):
                headers = await page.evaluate("""() => {
                    const cals = document.querySelectorAll(
                        '.dl-datepicker-calendar-cont'
                    );
                    return Array.from(cals).map(c => {
                        const ths = c.querySelectorAll('th');
                        for (const th of ths) {
                            if (th.getAttribute('colspan'))
                                return th.textContent.trim();
                        }
                        return '';
                    }).filter(h => h);
                }""")
                if any(
                    target_month in h and target_year in h for h in headers
                ):
                    break
                next_arrow = page.locator(".dl-datepicker-next").first
                if await next_arrow.count() > 0:
                    await next_arrow.click(timeout=2000)
                    await asyncio.sleep(0.5)
                else:
                    break

            # Click the target day in the correct month table
            await page.evaluate("""(args) => {
                const [tMonth, tYear, tDay] = args;
                const cals = document.querySelectorAll(
                    '.dl-datepicker-calendar-cont'
                );
                for (const cal of cals) {
                    const ths = cal.querySelectorAll('th');
                    let match = false;
                    for (const th of ths) {
                        if (th.getAttribute('colspan')
                            && th.textContent.includes(tMonth)
                            && th.textContent.includes(tYear)) {
                            match = true; break;
                        }
                    }
                    if (!match) continue;
                    const tds = cal.querySelectorAll('td');
                    for (const td of tds) {
                        if (td.textContent.trim() === tDay
                            && !td.classList.contains(
                                'dl-datepicker-other-month'
                        )) {
                            (td.querySelector('a') || td).click();
                            return;
                        }
                    }
                }
            }""", [target_month, target_year, target_day])
            await asyncio.sleep(0.5)

            # Click Done if visible
            done_btn = page.locator('button:has-text("Done")').first
            if await done_btn.count() > 0 and await done_btn.is_visible():
                await done_btn.click(timeout=2000)
                await asyncio.sleep(0.3)

        except Exception as e:
            logger.debug("Delta: date fill error: %s", e)

    # -- Modern form helpers (idp-* / mach-* web components) -----------

    async def _select_one_way_modern(self, page) -> None:
        """Select One Way from the mach-select trip-type dropdown."""
        try:
            trip_sel = page.locator("#trip-type-field")
            text = await trip_sel.inner_text()
            if "One Way" in text:
                return
            await trip_sel.click(timeout=3000)
            await asyncio.sleep(0.5)
            opt = page.locator("[role='option']").filter(has_text="One Way").first
            await opt.click(timeout=3000)
            await asyncio.sleep(0.3)
        except Exception as e:
            logger.debug("Delta: modern trip type error: %s", e)

    async def _fill_airport_modern(self, page, field_type: str, iata: str) -> bool:
        """Fill airport via the modern mach-route-picker component."""
        try:
            # The modern route picker has input fields inside shadow DOM or
            # aria-labeled inputs.  Try clicking the origin/dest area.
            if field_type == "origin":
                inp = page.locator(
                    "mach-route-picker input"
                ).first
            else:
                inp = page.locator(
                    "mach-route-picker input"
                ).last

            await inp.click(timeout=5000)
            await asyncio.sleep(0.5)
            await inp.fill("")
            await asyncio.sleep(0.2)
            await inp.press_sequentially(iata, delay=100)
            await asyncio.sleep(2.5)

            # Click the matching suggestion
            opt = page.locator("[role='option']").filter(has_text=iata).first
            await opt.click(timeout=5000)
            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            logger.debug("Delta: modern airport '%s' error: %s", field_type, e)
            return False

    async def _fill_date_modern(self, page, req: FlightSearchRequest) -> None:
        """Fill date via the modern mach-date-picker component."""
        try:
            target_label = req.date_from.strftime("%-d %B %Y")
            # Try Windows format
            try:
                target_label = req.date_from.strftime("%#d %B %Y")
            except ValueError:
                pass

            # Click the date picker trigger
            date_trigger = page.locator("mach-date-picker").first
            await date_trigger.click(timeout=5000)
            await asyncio.sleep(1.0)

            # Navigate forward in the calendar
            for _ in range(12):
                day_btn = page.locator(
                    f"[aria-label*='{target_label}']"
                ).first
                if await day_btn.count() > 0 and await day_btn.is_visible():
                    await day_btn.click(timeout=3000)
                    await asyncio.sleep(0.3)

                    done_btn = page.locator('button:has-text("Done")').first
                    if await done_btn.count() > 0:
                        await done_btn.click(timeout=2000)
                    return

                next_btn = page.locator(
                    "button[aria-label*='next'], button[aria-label*='Next']"
                ).first
                if await next_btn.count() > 0:
                    await next_btn.click(timeout=2000)
                    await asyncio.sleep(0.5)
                else:
                    break
        except Exception as e:
            logger.debug("Delta: modern date fill error: %s", e)

    # -- Response parsing ----------------------------------------------

    def _parse_response(
        self, data: dict, req: FlightSearchRequest,
    ) -> list[FlightOffer]:
        """Parse the GraphQL offer API response into FlightOffer objects.

        Structure: gqlOffersSets[] -- each set is one flight (1 trip + N fare classes).
        """
        offers: list[FlightOffer] = []
        try:
            search_offers = data.get("data", {}).get("gqlSearchOffers", {})
            offer_sets = search_offers.get("gqlOffersSets", [])
        except Exception:
            return offers

        if not offer_sets:
            return offers

        logger.debug("Delta: parsing %d offer sets", len(offer_sets))

        for set_idx, offer_set in enumerate(offer_sets):
            trips = offer_set.get("trips", [])
            raw_offers = offer_set.get("offers", [])
            if not trips or not raw_offers:
                continue

            trip = trips[0]  # Each set has exactly 1 trip

            # Find cheapest non-sold-out offer in this set
            cheapest = self._find_cheapest_offer(raw_offers)
            if cheapest is None:
                continue

            offer = self._build_offer(trip, cheapest, req, set_idx)
            if offer:
                offers.append(offer)

        return offers

    def _find_cheapest_offer(self, raw_offers: list[dict]) -> Optional[dict]:
        """Find the cheapest purchasable fare class from a list of raw offers."""
        best = None
        for raw_offer in raw_offers:
            try:
                props = raw_offer.get("additionalOfferProperties", {})
                if props.get("soldOut") or raw_offer.get("soldOut"):
                    continue
                if not props.get("offered", True):
                    continue

                price = self._extract_price(raw_offer)
                if price is None:
                    continue

                brand_id = props.get("dominantSegmentBrandId", "")
                entry = {
                    "offer_id": raw_offer.get("offerId", ""),
                    "brand_id": brand_id,
                    "price": float(price),
                    "currency": "USD",
                    "refundable": props.get("refundable", False),
                }

                if best is None or entry["price"] < best["price"]:
                    best = entry
            except Exception:
                continue
        return best

    def _extract_price(self, raw_offer: dict) -> Optional[float]:
        """Extract price from a raw offer dict."""
        items = raw_offer.get("offerItems", [])
        if not items:
            return None
        retail = items[0].get("retailItems", [])
        if not retail:
            return None
        meta = retail[0].get("retailItemMetaData", {})
        fare_info = meta.get("fareInformation", [])
        if not fare_info:
            return None
        fare_prices = fare_info[0].get("farePrice", [])
        if not fare_prices:
            return None
        total = fare_prices[0].get("totalFarePrice", {})
        curr_price = total.get("currencyEquivalentPrice", {})
        price = curr_price.get("roundedCurrencyAmt")
        formatted = curr_price.get("formattedCurrencyAmt")
        if formatted and "." in str(formatted):
            try:
                return float(formatted)
            except (ValueError, TypeError):
                pass
        return float(price) if price is not None else None

    def _build_offer(
        self, trip: dict, cheapest: dict, req: FlightSearchRequest, idx: int,
    ) -> Optional[FlightOffer]:
        """Build a FlightOffer from a trip dict and price info."""
        try:
            segments: list[FlightSegment] = []
            for seg in trip.get("flightSegment", []):
                mkt = seg.get("marketingCarrier", {})
                oper = seg.get("operatingCarrier", {})
                carrier_code = mkt.get("carrierCode", "DL")
                flight_num = str(mkt.get("carrierNum", ""))

                # Duration from the first (dominant) flight leg
                legs = seg.get("flightLeg", [])
                duration_secs = 0
                aircraft = ""
                if legs:
                    dom_leg = legs[0]
                    dur = dom_leg.get("duration", {})
                    duration_secs = (
                        dur.get("dayCnt", 0) * 86400
                        + dur.get("hourCnt", 0) * 3600
                        + dur.get("minuteCnt", 0) * 60
                    )
                    ac = dom_leg.get("aircraft", {})
                    aircraft = ac.get("fleetTypeCode", "") or ac.get("subFleetTypeCode", "")

                dep_ts = seg.get("scheduledDepartureLocalTs", "")
                arr_ts = seg.get("scheduledArrivalLocalTs", "")

                segments.append(FlightSegment(
                    airline=carrier_code,
                    airline_name=oper.get("carrierName") or _carrier_name(carrier_code),
                    flight_no=flight_num,
                    origin=seg.get("originAirportCode", ""),
                    destination=seg.get("destinationAirportCode", trip.get("destinationAirportCode", "")),
                    departure=_parse_dt(dep_ts),
                    arrival=_parse_dt(arr_ts),
                    duration_seconds=duration_secs,
                    aircraft=aircraft,
                ))

            if not segments:
                return None

            # Total trip time
            total_time = trip.get("totalTripTime", {})
            total_secs = (
                total_time.get("dayCnt", 0) * 86400
                + total_time.get("hourCnt", 0) * 3600
                + total_time.get("minuteCnt", 0) * 60
            )
            if total_secs == 0:
                total_secs = sum(s.duration_seconds for s in segments)

            outbound = FlightRoute(
                segments=segments,
                total_duration_seconds=total_secs,
                stopovers=max(trip.get("stopCnt", 0), len(segments) - 1),
            )

            airlines: list[str] = []
            for s in segments:
                if s.airline and s.airline not in airlines:
                    airlines.append(s.airline)

            price = cheapest["price"]
            currency = cheapest.get("currency", "USD")
            brand = cheapest.get("brand_id", "")
            cabin = _brand_to_cabin(brand)

            offer_id = hashlib.md5(
                f"DL-{trip.get('tripId', idx)}-{req.date_from}-{price}".encode()
            ).hexdigest()[:16]

            return FlightOffer(
                id=f"dl-{offer_id}",
                price=price,
                currency=currency,
                price_formatted=(
                    f"${price:.2f}" if currency == "USD"
                    else f"{price:.2f} {currency}"
                ),
                outbound=outbound,
                airlines=airlines,
                owner_airline="DL",
                source="delta_direct",
                source_tier="protocol",
                is_locked=True,
                booking_url=(
                    f"https://www.delta.com/flight-search/search-results"
                    f"?tripType=ONE_WAY&action=findFlights"
                    f"&originCity={req.origin}&destinationCity={req.destination}"
                    f"&departureDate={req.date_from}"
                    f"&paxCount=1&currencyCode={req.currency or 'USD'}"
                ),
            )
        except Exception as e:
            logger.debug("Delta: failed to build offer: %s", e)
            return None

    # -- Response builder ----------------------------------------------

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest,
        elapsed: float,
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)

        by_airline: dict[str, list[FlightOffer]] = defaultdict(list)
        for o in offers:
            key = o.owner_airline or (o.airlines[0] if o.airlines else "DL")
            by_airline[key].append(o)

        airlines_summary = []
        for code, al_offers in by_airline.items():
            cheapest = min(al_offers, key=lambda o: o.price)
            airlines_summary.append(
                AirlineSummary(
                    airline_code=code,
                    airline_name=(
                        _carrier_name(code) if code else "Delta Air Lines"
                    ),
                    cheapest_price=cheapest.price,
                    currency=cheapest.currency,
                    offer_count=len(al_offers),
                    cheapest_offer_id=cheapest.id,
                    sample_route=f"{req.origin}->{req.destination}",
                )
            )

        logger.info(
            "Delta: %d offers for %s->%s on %s (%.1fs)",
            len(offers), req.origin, req.destination, req.date_from, elapsed,
        )

        return FlightSearchResponse(
            search_id=hashlib.md5(
                f"dl-{req.origin}-{req.destination}-{req.date_from}-{time.time()}".encode()
            ).hexdigest()[:12],
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers[:req.limit],
            total_results=len(offers),
            airlines_summary=airlines_summary,
            search_params={
                "source": "delta_direct",
                "method": "cdp_form_fill_graphql_intercept",
                "elapsed": round(elapsed, 2),
            },
            source_tiers={
                "protocol": "Delta Air Lines direct (delta.com)"
            },
        )

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        return FlightSearchResponse(
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
            search_params={
                "source": "delta_direct",
                "error": "no_results",
            },
            source_tiers={
                "protocol": "Delta Air Lines direct (delta.com)"
            },
        )


# -- Helpers -----------------------------------------------------------

def _parse_dt(s: str) -> str:
    """Parse Delta's datetime format (2026-04-15T07:35) to ISO string."""
    if not s:
        return ""
    try:
        dt = datetime.fromisoformat(s)
        return dt.isoformat()
    except (ValueError, TypeError):
        return s


def _carrier_name(code: str) -> str:
    """Map common carrier codes to names."""
    names = {
        "DL": "Delta Air Lines",
        "AA": "American Airlines",
        "UA": "United Airlines",
        "WN": "Southwest Airlines",
        "AS": "Alaska Airlines",
        "B6": "JetBlue",
        "NK": "Spirit Airlines",
        "F9": "Frontier Airlines",
        "G4": "Allegiant Air",
        "AM": "Aeromexico",
        "AF": "Air France",
        "KL": "KLM",
        "VS": "Virgin Atlantic",
        "KE": "Korean Air",
        "LA": "LATAM Airlines",
    }
    return names.get(code, code)


def _brand_to_cabin(brand_id: str) -> str:
    """Map Delta brand IDs to cabin class names."""
    if not brand_id:
        return "economy"
    brand_upper = brand_id.upper()
    if "FIRST" in brand_upper:
        return "first"
    if "DCP" in brand_upper or "ONE" in brand_upper:
        return "premium_economy"
    if "MAIN" in brand_upper:
        return "economy"
    return "economy"
