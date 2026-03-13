"""
easyJet CDP Chrome hybrid scraper — persistent browser + form navigation
+ API response interception.

easyJet's API is behind Akamai WAF — requires browser-level session.
Direct deep-link URLs redirect to homepage without a BFF session.
The search must be initiated via the homepage form to trigger the BFF.

Strategy (CDP Chrome + persistent browser + response interception):
1. Launch REAL system Chrome (--remote-debugging-port, --user-data-dir).
2. Connect via Playwright CDP. Browser context persists across searches.
3. Each search: new page → register response listener → homepage → fill form
   → click search → intercept API JSON response.
4. Parse flight data → FlightOffers.
   Primary: intercept /api/ JSON responses containing flight/journey data.
   Fallback 1: extract from window.__NEXT_DATA__ (Next.js SSR).
   Fallback 2: extract from window.appData.searchResult (legacy path).

Real Chrome bypasses Akamai fingerprinting where bundled Chromium fails.
Persistent browser context means cookies/sessions carry over between searches,
avoiding cold-start Akamai challenges after the first search.
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
from typing import Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import stealth_args, stealth_position_arg, stealth_popen_kwargs

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]

_DEBUG_PORT = 9450
_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".easyjet_chrome_data"
)

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
_context = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_context():
    """Get or create a persistent browser context (shares cookies across searches)."""
    global _context
    browser = await _get_browser()
    if _context:
        try:
            # Check context is still alive
            if _context.pages:
                return _context
        except Exception:
            pass
    contexts = browser.contexts
    if contexts:
        _context = contexts[0]
    else:
        _context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
        )
    return _context


async def _get_browser():
    """Launch real Chrome via CDP, or fall back to Playwright headed."""
    global _pw_instance, _browser, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    return _browser
            except Exception:
                pass

        try:
            from connectors.browser import get_or_launch_cdp
            _browser, _chrome_proc = await get_or_launch_cdp(_DEBUG_PORT, _USER_DATA_DIR)
            logger.info("easyJet: Chrome ready via CDP (port %d)", _DEBUG_PORT)
            return _browser
        except Exception as e:
            logger.warning("easyJet: CDP failed: %s, falling back to Playwright", e)

        from connectors.browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("easyJet: Playwright browser launched (fallback)")
        return _browser


async def _dismiss_cookies(page) -> None:
    """Remove ensighten / cookie banners and account modals that block interaction."""
    for label in ["Accept", "Accept all", "Accept All Cookies", "I agree", "Got it", "OK"]:
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                logger.info("easyJet: clicked cookie accept button '%s'", label)
                await asyncio.sleep(0.5)
                break
        except Exception:
            continue

    try:
        await page.evaluate("""() => {
            // Remove cookie banners
            const ids = ['ensBannerBG', 'ensNotifyBanner', 'onetrust-consent-sdk',
                          'ensCloseBanner', 'ens-banner-overlay'];
            ids.forEach(id => { const el = document.getElementById(id); if (el) el.remove(); });
            document.querySelectorAll(
                '.ens-banner, [class*="cookie-banner"], [class*="consent"], ' +
                '[class*="CookieBanner"], [id*="cookie"], [id*="consent"], ' +
                '[class*="overlay"][style*="z-index"]'
            ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
            // Remove account modals that intercept pointer events
            document.querySelectorAll(
                '.modal-lightbox-wrapper, .account-modal, .modal__dialog-wrapper'
            ).forEach(el => el.remove());
        }""")
    except Exception:
        pass


class EasyjetConnectorClient:
    """easyJet CDP Chrome scraper — persistent browser + API response interception."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search easyJet using CDP Chrome persistent browser + homepage form.

        1. Get persistent browser context (cookies carry over)
        2. Open new page → register API response listener
        3. Navigate to homepage → fill search form
        4. Click "Show flights" → wait for intercepted API response
        5. Fallback: extract from window globals (__NEXT_DATA__, appData)
        6. Parse flight data → FlightOffers
        7. Close page (context stays alive for next search)
        """
        t0 = time.monotonic()

        context = await _get_context()
        page = await context.new_page()

        try:
            # ── Set up API response interception BEFORE navigation ────
            captured_data: dict = {}
            api_event = asyncio.Event()

            async def _on_response(response):
                try:
                    url = response.url.lower()
                    ct = response.headers.get("content-type", "")
                    if response.status != 200 or "json" not in ct:
                        return
                    if not (
                        any(k in url for k in (
                            "/api/query", "/api/search", "/funnel/api/",
                            "availability", "flights/search",
                        ))
                        or ("easyjet" in url and "flight" in url)
                    ):
                        return
                    data = await response.json()
                    if not data or not isinstance(data, dict):
                        return
                    # Check for known flight data structures
                    if any(k in data for k in (
                        "journeyPairs", "searchResult", "outbound",
                        "journeys", "flights", "results", "itineraries",
                    )):
                        captured_data["json"] = data
                        captured_data["url"] = response.url
                        api_event.set()
                        logger.info(
                            "easyJet: intercepted flight data from %s (keys: %s)",
                            response.url[:120],
                            list(data.keys())[:8],
                        )
                except Exception as exc:
                    logger.debug("easyJet: response handler error: %s", exc)

            page.on("response", _on_response)

            # ── Navigate + fill form ──────────────────────────────────
            logger.info("easyJet: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.easyjet.com/en/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(2.0)

            # Dismiss cookie/consent banners
            await _dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await _dismiss_cookies(page)

            # Fill the search form
            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("easyJet: form fill failed, aborting")
                return self._empty(req)

            # Click "Show flights"
            try:
                await page.get_by_role("button", name="Show flights").click(timeout=5000)
                logger.info("easyJet: clicked 'Show flights', waiting for navigation")
            except Exception as e:
                logger.warning("easyJet: could not click 'Show flights': %s", e)
                return self._empty(req)

            # Wait for navigation to /buy/flights
            try:
                await page.wait_for_url("**/buy/flights**", timeout=15000)
                logger.info("easyJet: navigated to %s", page.url)
            except Exception:
                logger.warning("easyJet: didn't navigate to /buy/flights, URL: %s", page.url)

            # Dismiss any overlays on the results page
            await _dismiss_cookies(page)

            # ── Wait for intercepted API response ─────────────────────
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.debug(
                    "easyJet: API interception timed out after %.1fs, trying page extraction",
                    time.monotonic() - t0,
                )

            # ── Extract flight data (multi-strategy) ─────────────────
            data = self._normalize_captured(captured_data.get("json"))

            if not data:
                data = await self._extract_from_page(page)

            if not data or not data.get("journeyPairs"):
                logger.warning(
                    "easyJet: no flight data found after %.1fs (URL: %s)",
                    time.monotonic() - t0, page.url,
                )
                return self._empty(req)

            # Debug: log structure of journeyPairs
            for i, pair in enumerate(data["journeyPairs"]):
                ob = pair.get("outbound", {})
                flights = ob.get("flights", {})
                logger.debug(
                    "easyJet: pair[%d] flight date keys: %s",
                    i, list(flights.keys()),
                )
                for dk, fl in flights.items():
                    avail_count = sum(
                        1 for f in fl
                        if not f.get("soldOut") and f.get("saleableStatus") == "AVAILABLE"
                    )
                    logger.debug(
                        "easyJet: pair[%d] date=%s total=%d available=%d",
                        i, dk, len(fl), avail_count,
                    )

            currency = data.get("metaData", {}).get("currencyCode", "GBP")
            offers = self._parse_journey_pairs(data["journeyPairs"], req, currency)

            elapsed = time.monotonic() - t0
            offers.sort(key=lambda o: o.price)

            logger.info(
                "easyJet %s→%s returned %d offers in %.1fs (CDP Chrome)",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"easyjet{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.error("easyJet CDP error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Data extraction helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_captured(raw: Optional[dict]) -> Optional[dict]:
        """Normalize an intercepted API response into {journeyPairs, metaData}."""
        if not raw or not isinstance(raw, dict):
            return None

        # Direct match — API returned journeyPairs at top level
        if "journeyPairs" in raw:
            return {"journeyPairs": raw["journeyPairs"], "metaData": raw.get("metaData", {})}

        # Nested under searchResult (legacy and some current responses)
        sr = raw.get("searchResult") or raw.get("SearchResult")
        if sr and isinstance(sr, dict):
            jp = sr.get("journeyPairs") or sr.get("JourneyPairs")
            if jp:
                return {"journeyPairs": jp, "metaData": sr.get("metaData", sr.get("MetaData", {}))}

        # Nested under data.searchResult
        data_field = raw.get("data")
        if isinstance(data_field, dict):
            result = EasyjetConnectorClient._normalize_captured(data_field)
            if result:
                return result

        return None

    async def _extract_from_page(self, page) -> Optional[dict]:
        """Fallback: extract flight data from window globals or __NEXT_DATA__."""
        raw = await page.evaluate(r"""() => {
            // Strategy 1: window.appData (legacy path — may still work)
            if (window.appData) {
                const sr = window.appData.searchResult || window.appData.SearchResult;
                if (sr && (sr.journeyPairs || sr.JourneyPairs)) {
                    return {
                        journeyPairs: sr.journeyPairs || sr.JourneyPairs,
                        metaData: sr.metaData || sr.MetaData || {},
                    };
                }
                // appData exists but searchResult path changed — return whole thing
                if (window.appData.journeyPairs) {
                    return {
                        journeyPairs: window.appData.journeyPairs,
                        metaData: window.appData.metaData || {},
                    };
                }
            }

            // Strategy 2: __NEXT_DATA__ (Next.js SSR)
            if (window.__NEXT_DATA__) {
                const pp = window.__NEXT_DATA__?.props?.pageProps;
                if (pp) {
                    const sr = pp.searchResult || pp.SearchResult || pp.flightData;
                    if (sr && (sr.journeyPairs || sr.JourneyPairs)) {
                        return {
                            journeyPairs: sr.journeyPairs || sr.JourneyPairs,
                            metaData: sr.metaData || sr.MetaData || {},
                        };
                    }
                    if (pp.journeyPairs) {
                        return { journeyPairs: pp.journeyPairs, metaData: pp.metaData || {} };
                    }
                    // Return full pageProps for downstream normalization
                    return pp;
                }
            }

            // Strategy 3: __NEXT_DATA__ from <script> tag
            const ndScript = document.querySelector('script#__NEXT_DATA__');
            if (ndScript) {
                try {
                    const nd = JSON.parse(ndScript.textContent);
                    const pp = nd?.props?.pageProps;
                    if (pp) return pp;
                } catch {}
            }

            // Strategy 4: scan <script type="application/json"> tags
            for (const s of document.querySelectorAll('script[type="application/json"]')) {
                try {
                    const d = JSON.parse(s.textContent);
                    if (d && (d.journeyPairs || d.searchResult || d.SearchResult)) return d;
                } catch {}
            }

            // Strategy 5: window.__EJ_DATA__ or similar easyJet-specific globals
            for (const key of Object.keys(window)) {
                if (key.startsWith('__') && key !== '__NEXT_DATA__') {
                    try {
                        const val = window[key];
                        if (val && typeof val === 'object' && !Array.isArray(val)) {
                            if (val.journeyPairs || val.searchResult || val.SearchResult) {
                                return val;
                            }
                        }
                    } catch {}
                }
            }

            return null;
        }""")

        if not raw or not isinstance(raw, dict):
            return None

        # Try to normalize whatever we got from the page
        normalized = self._normalize_captured(raw)
        if normalized:
            logger.info("easyJet: extracted flight data from page globals")
            return normalized

        # If we got journeyPairs directly (from pageProps or similar)
        if "journeyPairs" in raw:
            logger.info("easyJet: extracted journeyPairs from page globals")
            return {"journeyPairs": raw["journeyPairs"], "metaData": raw.get("metaData", {})}

        logger.debug("easyJet: page extraction returned data but no journeyPairs (keys: %s)",
                     list(raw.keys())[:10])
        return None

    # ------------------------------------------------------------------
    # Form interaction
    # ------------------------------------------------------------------

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill the easyJet homepage search form."""
        ok = await self._fill_airport_field(page, "From", req.origin)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        ok = await self._fill_airport_field(page, "To", req.destination)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        ok = await self._fill_date(page, req)
        if not ok:
            return False
        return True

    async def _fill_airport_field(self, page, label: str, iata: str) -> bool:
        """Fill an airport textbox and select the matching suggestion."""
        try:
            field = page.get_by_role("textbox", name=label)
            if label == "From":
                clear_name = "Clear selected departure airport"
            else:
                clear_name = "Clear selected destination airport"
            try:
                clear_btn = page.get_by_role("button", name=clear_name)
                if await clear_btn.count() > 0:
                    await clear_btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
            except Exception:
                pass

            await field.click(timeout=3000)
            await asyncio.sleep(0.3)
            await field.fill(iata)
            logger.info("easyJet: typed '%s' in %s field", iata, label)
            await asyncio.sleep(2.0)

            # Try multiple selector strategies for the autocomplete dropdown
            # Strategy 1: option role (listbox pattern)
            for role in ("option", "radio", "listitem"):
                try:
                    option = page.get_by_role(role, name=re.compile(
                        rf"{re.escape(iata)}", re.IGNORECASE
                    )).first
                    if await option.count() > 0:
                        await option.click(timeout=3000)
                        logger.info("easyJet: selected %s airport via %s role", iata, role)
                        return True
                except Exception:
                    continue

            # Strategy 2: data-testid or aria selectors
            for sel in (
                f'[data-testid*="airport"] >> text=/{re.escape(iata)}/i',
                f'li:has-text("{iata}")',
                f'[role="listbox"] >> text=/{re.escape(iata)}/i',
                f'ul li >> text=/{re.escape(iata)}/i',
            ):
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        logger.info("easyJet: selected %s airport via locator", iata)
                        return True
                except Exception:
                    continue

            # Strategy 3: click first visible dropdown item
            for sel in (
                '[role="listbox"] [role="option"]',
                '[class*="airport"] li',
                '[class*="dropdown"] li',
                '[class*="suggestion"] li',
                '[class*="result"] li',
            ):
                try:
                    item = page.locator(sel).first
                    if await item.count() > 0:
                        await item.click(timeout=3000)
                        logger.info("easyJet: selected first dropdown item for %s", iata)
                        return True
                except Exception:
                    continue

            logger.warning("easyJet: %s field — no matching suggestion found for %s", label, iata)
            return False
        except Exception as e:
            logger.warning("easyJet: %s field error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Open the date picker and select the outbound date."""
        target = req.date_from
        try:
            # Open the date picker
            try:
                date_field = page.get_by_role("textbox", name="Clear selected travel date")
                if await date_field.count() == 0:
                    date_field = page.get_by_placeholder("Choose your dates")
                await date_field.click(timeout=3000)
            except Exception:
                when_section = page.locator("text=When").first
                await when_section.click(timeout=3000)
            await asyncio.sleep(0.5)

            # Wait for the calendar grid to load (prices may take time)
            try:
                await page.wait_for_selector(
                    '[data-testid="month-title"]', timeout=10000
                )
            except Exception:
                logger.warning("easyJet: calendar grid didn't load in time")
                return False
            await asyncio.sleep(0.3)

            # Primary selector: data-testid="day-month-year"
            testid = f"{target.day}-{target.month}-{target.year}"
            day_btn = page.locator(f'[data-testid="{testid}"]')

            # Fallback selector: aria-label="Month day, year"
            aria_label = f"{target.strftime('%B')} {target.day}, {target.year}"
            day_btn_fallback = page.get_by_role("button", name=aria_label)

            # Navigate months until target day button is rendered
            for attempt in range(12):
                if await day_btn.count() > 0 or await day_btn_fallback.count() > 0:
                    break
                try:
                    await page.get_by_role("button", name="Next month").click(timeout=2000)
                    await asyncio.sleep(0.5)
                except Exception:
                    break

            # Try primary, then fallback
            if await day_btn.count() > 0:
                await day_btn.click(timeout=5000)
                logger.info("easyJet: clicked date %s (testid: %s)", target, testid)
            elif await day_btn_fallback.count() > 0:
                await day_btn_fallback.click(timeout=5000)
                logger.info("easyJet: clicked date %s (aria-label fallback)", target)
            else:
                # Last resort: JS-based click
                clicked = await page.evaluate("""(args) => {
                    const [testid, ariaLabel] = args;
                    let btn = document.querySelector(`[data-testid="${testid}"]`);
                    if (!btn) {
                        btn = Array.from(document.querySelectorAll('button'))
                            .find(b => b.getAttribute('aria-label') === ariaLabel);
                    }
                    if (btn) { btn.click(); return true; }
                    return false;
                }""", [testid, aria_label])
                if clicked:
                    logger.info("easyJet: clicked date %s (JS fallback)", target)
                else:
                    logger.warning("easyJet: could not find date button for %s", target)
                    return False

            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            logger.warning("easyJet: date error: %s", e)
            return False

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_journey_pairs(
        self, journey_pairs: list, req: FlightSearchRequest, currency: str
    ) -> list[FlightOffer]:
        """Parse journeyPairs from window.appData.searchResult into FlightOffers."""
        offers: list[FlightOffer] = []
        target_date = req.date_from.strftime("%Y-%m-%d")
        booking_url = self._build_booking_url(req)

        for pair in journey_pairs:
            outbound = pair.get("outbound", {})
            flights_by_date = outbound.get("flights", {})

            # flights is a dict keyed by date string
            matched_dates = [dk for dk in flights_by_date if dk == target_date]
            if not matched_dates:
                # No exact match — use all available dates (±window from search)
                available = list(flights_by_date.keys())
                logger.warning(
                    "easyJet: target %s not in flight dates %s, using all",
                    target_date, available,
                )
                matched_dates = available

            for date_key in matched_dates:
                for flight in flights_by_date[date_key]:
                    offer = self._parse_single_flight(flight, currency, booking_url)
                    if offer:
                        offers.append(offer)

        return offers

    def _parse_single_flight(
        self, flight: dict, currency: str, booking_url: str
    ) -> Optional[FlightOffer]:
        """Parse a single easyJet flight dict into a FlightOffer."""
        if flight.get("soldOut") or flight.get("saleableStatus") != "AVAILABLE":
            return None

        # Extract cheapest fare price
        fares = flight.get("fares", {})
        adt_fares = fares.get("ADT", {})
        price = None
        for fare_family in ["STANDARD", "FLEXI"]:
            fare = adt_fares.get(fare_family)
            if fare:
                unit_price = fare.get("unitPrice", {})
                gross = unit_price.get("grossPrice")
                if gross is not None:
                    if price is None or gross < price:
                        price = gross
                    break

        if price is None or price <= 0:
            return None

        flight_no = flight.get("flightNumber", "")
        carrier = flight.get("iataCarrierCode", "U2")
        if flight_no and not flight_no.startswith(carrier):
            flight_no = f"{carrier}{flight_no}"

        dep_str = flight.get("localDepartureDateTime", "")
        arr_str = flight.get("localArrivalDateTime", "")

        segment = FlightSegment(
            airline=carrier,
            airline_name="easyJet",
            flight_no=flight_no,
            origin=flight.get("departureAirportCode", ""),
            destination=flight.get("arrivalAirportCode", ""),
            departure=self._parse_dt(dep_str),
            arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

        total_dur = int((segment.arrival - segment.departure).total_seconds())

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=max(total_dur, 0),
            stopovers=0,
        )

        key = f"{flight_no}_{dep_str}_{price}"

        return FlightOffer(
            id=f"ej_{hashlib.md5(key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["easyJet"],
            owner_airline="U2",
            booking_url=booking_url,
            is_locked=False,
            source="easyjet_direct",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        date_out = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.easyjet.com/en/buy/flights"
            f"?dep={req.origin}&dest={req.destination}"
            f"&dd={date_out}&isOneWay=on"
            f"&apax={req.adults}&cpax={req.children or 0}"
            f"&ipax={req.infants or 0}"
        )

    def _parse_dt(self, s: str) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"easyjet{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
