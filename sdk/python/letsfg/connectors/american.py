"""
American Airlines connector -- CDP Chrome + form fill + ng-state extraction.

American Airlines (IATA: AA) is the world's largest airline by fleet size,
operating 6,800+ daily flights to 350+ destinations in 60+ countries.

Website: aa.com -- Angular 20 SSR/SPA with Akamai Bot Manager protection.

Strategy:
1. Launch real Chrome via CDP (same flags as Pegasus -- no automation detection)
2. For each search: navigate to booking form -> fill origin, destination, date
   -> click Search -> wait for results page
3. Extract flight data from <script id="ng-state"> (Angular transfer state,
   ~2.5 MB JSON containing SearchData.itineraryResult with full slices)
4. Parse slices -> FlightOffer objects

The ng-state approach is reliable because Angular SSR embeds the complete
API response in the HTML as transfer state.  No API interception needed.

No geo-blocking observed -- works from all regions without proxy.
Set AMERICAN_PROXY env var if your IP is flagged by Akamai.
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
from typing import Optional

from models.flights import (
    AirlineSummary,
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

# -- Chrome flags matching MCP/Pegasus pattern (Akamai-safe) ---------
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
_RESULTS_WAIT = 30  # seconds to wait for results page + ng-state

# -- Shared CDP Chrome state ------------------------------------------
_CDP_PORT = 9471
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", "/tmp"), "chrome-cdp-american"
)
_chrome_proc: Optional[subprocess.Popen] = None
_browser = None
_pw_instance = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_proxy() -> Optional[dict]:
    raw = os.environ.get("AMERICAN_PROXY", "").strip()
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
            logger.info("American: connected to existing Chrome on port %d", _CDP_PORT)
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
            "American: Chrome ready via CDP (port %d, pid %d)",
            _CDP_PORT,
            _chrome_proc.pid,
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


class AmericanConnectorClient:
    """American Airlines scraper -- CDP Chrome + form fill + ng-state extraction.

    Real Chrome launched once and reused.  Each search takes ~8-12s.
    No proxy required by default, but set AMERICAN_PROXY if Akamai blocks
    your IP.
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
                    "American: attempt %d/%d error: %s",
                    attempt,
                    _MAX_ATTEMPTS,
                    e,
                )
                if "closed" in str(e).lower() or "disconnected" in str(e).lower():
                    await _reset_browser()

        return self._empty(req)

    # -- Single search attempt -----------------------------------------

    async def _attempt_search(
        self, req: FlightSearchRequest, t0: float
    ) -> Optional[FlightSearchResponse]:
        """Fresh page -> form fill -> click Search -> extract ng-state JSON."""
        browser = await _get_browser()
        ctx = browser.contexts[0]

        page = await ctx.new_page()

        # Block OneTrust / cookie consent resources at page level
        async def _abort_route(route):
            await route.abort()

        for pattern in ("**/*onetrust*", "**/*cookielaw*", "**/*optanon*"):
            try:
                await page.route(pattern, _abort_route)
            except Exception:
                pass

        # Inject init script to remove OneTrust immediately on every navigation
        try:
            await page.add_init_script(
                """
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
                """
            )
        except Exception:
            pass

        try:
            logger.info(
                "American: searching %s->%s on %s",
                req.origin,
                req.destination,
                req.date_from,
            )

            await page.goto(
                "https://www.aa.com/booking/search/find-flights",
                wait_until="domcontentloaded",
                timeout=30000,
            )

            # Wait for Angular form to render
            try:
                await page.get_by_role("combobox", name="Departure airport").wait_for(
                    state="visible", timeout=15000
                )
            except Exception:
                await asyncio.sleep(3.0)

            # Dismiss overlays after page render
            await _dismiss_overlays(page)
            await asyncio.sleep(0.5)

            title = await page.title()
            if "access denied" in title.lower():
                logger.warning("American: Akamai blocked -- Access Denied")
                return None

            # Remove any remaining OneTrust overlay via JS
            await _dismiss_overlays(page)
            await asyncio.sleep(0.5)

            # Mouse movement to build Akamai sensor data
            for _ in range(3):
                x = random.randint(200, 1000)
                y = random.randint(100, 500)
                await page.mouse.move(x, y, steps=random.randint(5, 10))
                await asyncio.sleep(random.uniform(0.2, 0.5))

            # -- Select One way trip type ------------------------------
            await self._select_one_way(page)
            await asyncio.sleep(0.3)

            # -- Fill origin -------------------------------------------
            ok = await self._fill_airport(page, "Departure airport", req.origin)
            if not ok:
                logger.warning("American: origin fill failed")
                return None
            await asyncio.sleep(0.4)

            # -- Fill destination --------------------------------------
            ok = await self._fill_airport(page, "Arrival airport", req.destination)
            if not ok:
                logger.warning("American: destination fill failed")
                return None
            await asyncio.sleep(0.4)

            # -- Fill departure date -----------------------------------
            await self._fill_date(page, req)
            await asyncio.sleep(0.3)

            # -- Click Search ------------------------------------------
            search_btn = page.get_by_role("button", name="Search").first
            await search_btn.click(timeout=5000)

            # -- Wait for results page ---------------------------------
            await page.wait_for_url(
                "**/booking/choose-flights/**", timeout=_RESULTS_WAIT * 1000
            )
            # Wait for ng-state to appear (Angular SSR embeds it once hydrated)
            for _ in range(10):
                has_ng = await page.evaluate(
                    "() => !!document.getElementById('ng-state')"
                )
                if has_ng:
                    break
                await asyncio.sleep(1.0)

            # -- Extract ng-state transfer state -----------------------
            data = await self._extract_ng_state(page)
            if not data:
                logger.warning("American: ng-state extraction failed")
                return None

        except asyncio.TimeoutError:
            logger.warning("American: results page timed out")
            return None
        except Exception as e:
            logger.warning("American: search error: %s", e)
            return None
        finally:
            try:
                await page.close()
            except Exception:
                pass

        elapsed = time.monotonic() - t0
        offers = self._parse_response(data, req)
        return self._build_response(offers, req, elapsed)

    # -- ng-state extraction -------------------------------------------

    async def _extract_ng_state(self, page) -> Optional[dict]:
        """Extract itinerary data from Angular transfer state."""
        try:
            raw = await page.evaluate(
                """() => {
                const el = document.getElementById('ng-state');
                return el ? el.textContent : null;
            }"""
            )
            if not raw:
                return None

            state = json.loads(raw)
            if not isinstance(state, dict):
                return None

            # Path 1: state.SearchData.itineraryResult (current AA structure)
            search_data = state.get("SearchData")
            if isinstance(search_data, dict):
                ir = search_data.get("itineraryResult")
                if isinstance(ir, dict) and ir.get("slices"):
                    logger.info(
                        "American: extracted %d slices from SearchData.itineraryResult",
                        len(ir["slices"]),
                    )
                    return ir

            # Path 2: top-level key containing "itineraryResult" (legacy)
            for key, val in state.items():
                if not isinstance(val, dict):
                    continue
                if "itineraryResult" in key:
                    body = val.get("body", val)
                    if isinstance(body, dict) and body.get("slices"):
                        logger.info(
                            "American: extracted %d slices from ng-state key '%s'",
                            len(body["slices"]),
                            key[:60],
                        )
                        return body
                body = val.get("body")
                if isinstance(body, dict) and body.get("slices"):
                    logger.info(
                        "American: extracted %d slices from ng-state key '%s'",
                        len(body["slices"]),
                        key[:60],
                    )
                    return body

            # Path 3: deep search for any dict with a "slices" list
            result = _find_slices(state)
            if result:
                logger.info(
                    "American: extracted %d slices (deep search)",
                    len(result.get("slices", [])),
                )
                return result

            logger.warning("American: ng-state found but no slices in it")
            return None

        except json.JSONDecodeError as e:
            logger.warning("American: ng-state JSON parse error: %s", e)
            return None
        except Exception as e:
            logger.warning("American: ng-state extraction error: %s", e)
            return None

    # -- Form helpers --------------------------------------------------

    async def _select_one_way(self, page) -> None:
        """Select 'One way' trip type from the mat-select dropdown."""
        try:
            for name in ("Round trip", "One way", "Multi-city"):
                el = page.get_by_role("combobox", name=name)
                if await el.count() > 0:
                    if name == "One way":
                        return  # Already set
                    await el.click(timeout=5000)
                    await asyncio.sleep(0.5)
                    await page.get_by_role("option", name="One way").click(
                        timeout=5000
                    )
                    await asyncio.sleep(0.5)
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(0.3)
                    return
        except Exception as e:
            logger.debug("American: trip type selection error: %s", e)
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass

    async def _fill_airport(self, page, label: str, iata: str) -> bool:
        """Fill an airport combobox and select from autocomplete."""
        try:
            field = page.get_by_role("combobox", name=label)
            await field.wait_for(state="visible", timeout=10000)
            await field.click(timeout=5000)
            await asyncio.sleep(0.3)
            # Triple-click to select all existing text, then delete
            await field.click(click_count=3)
            await asyncio.sleep(0.1)
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.2)
            await field.press_sequentially(iata, delay=120)
            await asyncio.sleep(2.5)

            suggestion = (
                page.locator("mat-option").filter(has_text=iata).first
            )
            await suggestion.click(timeout=8000)
            await asyncio.sleep(0.3)
            return True
        except Exception as e:
            logger.debug("American: airport fill '%s' error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> None:
        """Fill departure date in mm/dd/yyyy format."""
        try:
            date_field = page.get_by_role("textbox", name="Departure date")
            await date_field.click(timeout=3000)
            await asyncio.sleep(0.2)
            await date_field.click(click_count=3)
            await asyncio.sleep(0.1)
            date_str = req.date_from.strftime("%m/%d/%Y")
            await date_field.press_sequentially(date_str, delay=50)
            await asyncio.sleep(0.3)
            await page.keyboard.press("Tab")
        except Exception as e:
            logger.debug("American: date fill error: %s", e)

    # -- Response parsing ----------------------------------------------

    def _parse_response(
        self, data: dict, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        slices = data.get("slices", [])
        offers: list[FlightOffer] = []
        for sl in slices:
            if not isinstance(sl, dict):
                continue
            offer = self._parse_slice(sl, req)
            if offer:
                offers.append(offer)
        return offers

    def _parse_slice(
        self, sl: dict, req: FlightSearchRequest
    ) -> Optional[FlightOffer]:
        try:
            cheapest = sl.get("cheapestPrice", {})
            price_info = cheapest.get("perPassengerDisplayTotal", {})
            price = price_info.get("amount")
            currency = price_info.get("currency", "USD")
            if price is None:
                return None

            segments: list[FlightSegment] = []
            for seg in sl.get("segments", []):
                if not isinstance(seg, dict):
                    continue
                flight = seg.get("flight", {})
                legs = seg.get("legs", [])

                aircraft = ""
                if legs:
                    ac = legs[0].get("aircraft", {})
                    aircraft = ac.get("name") or ac.get("code", "")

                seg_origin = seg.get("origin", {})
                seg_dest = seg.get("destination", {})
                total_mins = sum(
                    leg.get("durationInMinutes", 0) for leg in legs
                )

                segments.append(
                    FlightSegment(
                        airline=flight.get("carrierCode", "AA"),
                        airline_name=flight.get(
                            "carrierName", "American Airlines"
                        ),
                        flight_no=str(flight.get("flightNumber", "")),
                        origin=(
                            seg_origin.get("code", "")
                            if isinstance(seg_origin, dict)
                            else str(seg_origin)
                        ),
                        destination=(
                            seg_dest.get("code", "")
                            if isinstance(seg_dest, dict)
                            else str(seg_dest)
                        ),
                        origin_city=(
                            seg_origin.get("city", "")
                            if isinstance(seg_origin, dict)
                            else ""
                        ),
                        destination_city=(
                            seg_dest.get("city", "")
                            if isinstance(seg_dest, dict)
                            else ""
                        ),
                        departure=_parse_dt(seg.get("departureDateTime", "")),
                        arrival=_parse_dt(seg.get("arrivalDateTime", "")),
                        duration_seconds=total_mins * 60,
                        aircraft=aircraft,
                    )
                )

            if not segments:
                return None

            total_duration = sl.get("durationInMinutes", 0)
            outbound = FlightRoute(
                segments=segments,
                total_duration_seconds=total_duration * 60,
                stopovers=max(len(segments) - 1, 0),
            )

            airlines: list[str] = []
            for seg in segments:
                if seg.airline and seg.airline not in airlines:
                    airlines.append(seg.airline)

            offer_id = hashlib.md5(
                f"AA-{sl.get('id', '')}-{req.date_from}".encode()
            ).hexdigest()[:16]

            return FlightOffer(
                id=f"aa-{offer_id}",
                price=float(price),
                currency=currency,
                price_formatted=(
                    f"${price:.2f}"
                    if currency == "USD"
                    else f"{price:.2f} {currency}"
                ),
                outbound=outbound,
                airlines=airlines,
                owner_airline="AA",
                source="american_direct",
                source_tier="protocol",
                is_locked=True,
                booking_url=(
                    f"https://www.aa.com/booking/search?locale=en_US"
                    f"&pax=1&adult=1&type=OneWay"
                    f"&searchType=Revenue"
                    f"&origin={req.origin}&destination={req.destination}"
                    f"&departDate={req.date_from}"
                ),
            )

        except Exception as e:
            logger.debug("American: failed to parse slice: %s", e)
            return None

    # -- Response builder ----------------------------------------------

    def _build_response(
        self,
        offers: list[FlightOffer],
        req: FlightSearchRequest,
        elapsed: float,
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)

        by_airline: dict[str, list[FlightOffer]] = defaultdict(list)
        for o in offers:
            key = o.owner_airline or (o.airlines[0] if o.airlines else "AA")
            by_airline[key].append(o)

        airlines_summary = []
        for code, al_offers in by_airline.items():
            cheapest = min(al_offers, key=lambda o: o.price)
            airlines_summary.append(
                AirlineSummary(
                    airline_code=code,
                    airline_name=(
                        cheapest.airlines[0]
                        if cheapest.airlines
                        else "American Airlines"
                    ),
                    cheapest_price=cheapest.price,
                    currency=cheapest.currency,
                    offer_count=len(al_offers),
                    cheapest_offer_id=cheapest.id,
                    sample_route=f"{req.origin}->{req.destination}",
                )
            )

        logger.info(
            "American: %d offers for %s->%s on %s (%.1fs)",
            len(offers),
            req.origin,
            req.destination,
            req.date_from,
            elapsed,
        )

        return FlightSearchResponse(
            search_id=hashlib.md5(
                f"aa-{req.origin}-{req.destination}-{req.date_from}-{time.time()}".encode()
            ).hexdigest()[:12],
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "USD",
            offers=offers[: req.limit],
            total_results=len(offers),
            airlines_summary=airlines_summary,
            search_params={
                "source": "american_direct",
                "method": "cdp_form_fill_ng_state",
                "elapsed": round(elapsed, 2),
            },
            source_tiers={
                "protocol": "American Airlines direct (aa.com)"
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
                "source": "american_direct",
                "error": "no_results",
            },
            source_tiers={
                "protocol": "American Airlines direct (aa.com)"
            },
        )


# -- Helpers -----------------------------------------------------------


def _find_slices(obj, depth: int = 0) -> Optional[dict]:
    """Recursively search nested dicts for one containing a 'slices' list."""
    if depth > 5 or not isinstance(obj, dict):
        return None
    if "slices" in obj and isinstance(obj["slices"], list) and len(obj["slices"]) > 0:
        return obj
    for v in obj.values():
        if isinstance(v, dict):
            result = _find_slices(v, depth + 1)
            if result:
                return result
    return None


async def _dismiss_overlays(page) -> None:
    """Remove OneTrust cookie consent and any other blocking overlays."""
    try:
        await page.evaluate(
            """() => {
            // Click accept button if present
            const btn = document.getElementById('onetrust-accept-btn-handler');
            if (btn) btn.click();
            // Remove the SDK container entirely
            const sdk = document.getElementById('onetrust-consent-sdk');
            if (sdk) sdk.remove();
            // Remove dark filter and overlay elements
            document.querySelectorAll(
                '.onetrust-pc-dark-filter, .ot-fade-in, #onetrust-banner-sdk, #ot-sdk-btn-floating'
            ).forEach(e => e.remove());
            document.body.style.overflow = 'auto';
            // Also remove any style tags injected by OneTrust
            document.querySelectorAll('style').forEach(s => {
                if (s.textContent && s.textContent.includes('onetrust')) s.remove();
            });
        }"""
        )
    except Exception:
        pass


def _parse_dt(s: str) -> datetime:
    """Parse AA datetime like '2026-04-15T07:00:00.000-05:00'."""
    if not s:
        return datetime(2000, 1, 1)
    try:
        return datetime.fromisoformat(s)
    except (ValueError, AttributeError):
        pass
    try:
        clean = s.split(".")[0] if "." in s else s
        if "+" in clean[10:]:
            clean = clean[: clean.rindex("+")]
        elif clean.count("-") > 2:
            clean = clean[: clean.rindex("-")]
        return datetime.fromisoformat(clean)
    except Exception:
        return datetime(2000, 1, 1)