"""
Porter Airlines hybrid scraper — curl_cffi + cookie-farm + Playwright fallback.

Porter (IATA: PD) is a Canadian airline based at Billy Bishop Toronto City Airport.

Cloudflare blocks direct URL access to www.flyporter.com. This hybrid connector
uses curl_cffi with Chrome TLS fingerprint to bypass the WAF, with a cookie-farm
fallback (Playwright farms Cloudflare clearance cookies) and full browser as
last resort.

Strategy (hybrid — curl_cffi first, browser fallback):
1. (Primary) curl_cffi GET to results URL with Chrome TLS fingerprint (~2-5s).
   Parses server-rendered HTML for flight cards (h4 Departs/Arrives, fare buttons).
2. (Cookie-farm) If Cloudflare blocks curl_cffi, Playwright opens
   booking.flyporter.com (no Cloudflare), navigates to www.flyporter.com,
   extracts clearance cookies. curl_cffi retries with farmed cookies (~20s once,
   then reused for ~20 min).
3. (Fallback) Playwright CDP Chrome — navigate to results URL with farmed context,
   wait for DOM render, extract flight cards.

Result: ~2-5s per search (curl_cffi) instead of ~25s with full Playwright.
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

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL = True
except ImportError:
    HAS_CURL = False

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import stealth_args, get_or_launch_cdp

logger = logging.getLogger(__name__)

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-US", "en-CA", "en-GB"]
_TIMEZONES = [
    "America/Toronto", "America/Vancouver", "America/Edmonton",
    "America/Halifax", "America/New_York",
]

_RESULTS_URL_TPL = (
    "https://www.flyporter.com/en/flight/tickets/Select_BAF"
    "?departStation={origin}&destination={dest}&depDate={date}"
    "&paxADT={adults}&paxCHD=0&paxINF=0&trpType=OneWay&fareClass=R&bookWithPoints=0"
)
_BOOKING_URL = "https://booking.flyporter.com/en/book-travel/book-flights-online"

_IMPERSONATE = "chrome124"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_COOKIE_MAX_AGE = 20 * 60  # Re-farm cookies after 20 minutes

# ── Shared state ───────────────────────────────────────────────────────────
_farm_lock: Optional[asyncio.Lock] = None
_farmed_cookies: list[dict] = []
_farm_timestamp: float = 0.0
_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None

_USER_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".porter_chrome_data")
_DEBUG_PORT = 9333


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


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

        # Try CDP first (real Chrome avoids Cloudflare automation flags)
        try:
            _browser, _chrome_proc = await get_or_launch_cdp(_DEBUG_PORT, _USER_DATA_DIR)
            logger.info("Porter: Chrome ready via CDP (port %d)", _DEBUG_PORT)
            return _browser
        except Exception as e:
            logger.warning("Porter: CDP failed: %s, falling back to Playwright", e)

        # Fallback: regular Playwright
        from connectors.browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("Porter: Playwright browser launched (fallback)")
        return _browser


class PorterConnectorClient:
    """Porter Airlines hybrid scraper — curl_cffi + cookie-farm + Playwright fallback.

    Fast path (~2-5s): curl_cffi with Chrome TLS fingerprint → GET results URL.
    Cookie-farm (~20s once): Playwright farms Cloudflare cookies, reused ~20 min.
    Fallback: Full Playwright DOM extraction.
    """

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    # ------------------------------------------------------------------
    # Main search entry point
    # ------------------------------------------------------------------

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search Porter flights via curl_cffi (fast) → cookie-farm → Playwright."""
        t0 = time.monotonic()

        try:
            # Fast path: try curl_cffi without cookies first
            if HAS_CURL:
                html = await self._curl_search(req, cookies=[])
                offers = self._parse_html(html, req) if html else []
                if offers:
                    elapsed = time.monotonic() - t0
                    logger.info("Porter: curl_cffi cookieless succeeded (%d offers, %.1fs)",
                                len(offers), elapsed)
                    return self._build_response(offers, req, elapsed)

                # Try with farmed cookies
                cookies = await self._ensure_cookies(req)
                if cookies:
                    html = await self._curl_search(req, cookies)
                    offers = self._parse_html(html, req) if html else []
                    if offers:
                        elapsed = time.monotonic() - t0
                        logger.info("Porter: curl_cffi with cookies succeeded (%d offers, %.1fs)",
                                    len(offers), elapsed)
                        return self._build_response(offers, req, elapsed)

                # Re-farm once if stale
                if cookies:
                    logger.info("Porter: curl_cffi failed with cookies, re-farming")
                    cookies = await self._farm_cookies(req)
                    if cookies:
                        html = await self._curl_search(req, cookies)
                        offers = self._parse_html(html, req) if html else []
                        if offers:
                            elapsed = time.monotonic() - t0
                            return self._build_response(offers, req, elapsed)

            # Last resort: full Playwright
            logger.warning("Porter: curl_cffi returned no data, falling back to Playwright")
            return await self._playwright_fallback(req, t0)

        except Exception as e:
            logger.error("Porter hybrid error: %s", e)
            return self._empty(req)

    # ------------------------------------------------------------------
    # curl_cffi direct fetch
    # ------------------------------------------------------------------

    async def _curl_search(
        self, req: FlightSearchRequest, cookies: list[dict],
    ) -> Optional[str]:
        """GET results URL via curl_cffi with Chrome TLS fingerprint."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._curl_search_sync, req, cookies)

    def _curl_search_sync(
        self, req: FlightSearchRequest, cookies: list[dict],
    ) -> Optional[str]:
        """Synchronous curl_cffi fetch of results page HTML."""
        sess = curl_requests.Session(impersonate=_IMPERSONATE)

        for c in cookies:
            domain = c.get("domain", "")
            sess.cookies.set(c["name"], c["value"], domain=domain)

        results_url = _RESULTS_URL_TPL.format(
            origin=req.origin,
            dest=req.destination,
            date=req.date_from.strftime("%Y-%m-%d"),
            adults=req.adults,
        )

        try:
            r = sess.get(
                results_url,
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": "https://www.flyporter.com/en/",
                    "User-Agent": _UA,
                },
                timeout=15,
                allow_redirects=True,
            )
        except Exception as e:
            logger.error("Porter: curl_cffi request failed: %s", e)
            return None

        if r.status_code != 200:
            logger.warning("Porter: curl_cffi returned HTTP %d", r.status_code)
            return None

        html = r.text
        # Check for Cloudflare challenge page
        if _is_cloudflare_challenge(html):
            logger.info("Porter: curl_cffi got Cloudflare challenge page")
            return None

        # Check for meaningful content (flight results should have these markers)
        if "Departs" not in html and "Select_BAF" not in html and "flyporter" not in html:
            logger.info("Porter: curl_cffi response has no flight markers")
            return None

        return html

    # ------------------------------------------------------------------
    # Cookie farm — Playwright generates Cloudflare clearance cookies
    # ------------------------------------------------------------------

    async def _ensure_cookies(self, req: FlightSearchRequest) -> list[dict]:
        """Return valid farmed cookies, farming new ones if needed."""
        global _farmed_cookies, _farm_timestamp
        lock = _get_farm_lock()
        async with lock:
            age = time.monotonic() - _farm_timestamp
            if _farmed_cookies and age < _COOKIE_MAX_AGE:
                return _farmed_cookies
            return await self._farm_cookies(req)

    async def _farm_cookies(self, req: FlightSearchRequest) -> list[dict]:
        """Open Playwright, visit Porter, extract Cloudflare clearance cookies."""
        global _farmed_cookies, _farm_timestamp

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

            # Visit booking.flyporter.com first (no Cloudflare)
            logger.info("Porter: farming cookies via booking.flyporter.com")
            await page.goto(_BOOKING_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2.0)
            await self._dismiss_cookies(page)

            # Now navigate to www.flyporter.com to trigger Cloudflare and get clearance
            results_url = _RESULTS_URL_TPL.format(
                origin=req.origin,
                dest=req.destination,
                date=req.date_from.strftime("%Y-%m-%d"),
                adults=req.adults,
            )
            await page.goto(results_url, wait_until="domcontentloaded", timeout=30000)

            # Wait for Cloudflare challenge to resolve
            await self._wait_cloudflare(page, timeout=30)

            cookies = await context.cookies()
            _farmed_cookies = cookies
            _farm_timestamp = time.monotonic()
            logger.info("Porter: farmed %d cookies", len(cookies))
            return cookies

        except Exception as e:
            logger.error("Porter: cookie farm error: %s", e)
            return []
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Playwright fallback (full browser flow)
    # ------------------------------------------------------------------

    async def _playwright_fallback(
        self, req: FlightSearchRequest, t0: float,
    ) -> FlightSearchResponse:
        """Full Playwright flow: navigate to results URL, extract from DOM."""
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

            results_url = _RESULTS_URL_TPL.format(
                origin=req.origin, dest=req.destination,
                date=req.date_from.strftime("%Y-%m-%d"),
                adults=req.adults,
            )
            logger.info("Porter: Playwright fallback for %s→%s", req.origin, req.destination)
            await page.goto(results_url, wait_until="domcontentloaded", timeout=30000)

            cf_cleared = await self._wait_cloudflare(page, timeout=30)
            if not cf_cleared:
                logger.warning("Porter: Cloudflare blocked Playwright direct URL, trying booking form")
                offers = await self._strategy_booking_form(page, req, t0)
            else:
                # Wait for flight results in DOM
                try:
                    await page.wait_for_selector(
                        "h1:has-text('Select Flights'), h2:has-text('Departing flights')",
                        timeout=20000)
                except Exception:
                    logger.debug("Porter: flight headings not found, extracting DOM anyway")
                try:
                    await page.wait_for_selector("h4:has-text('Departs')", timeout=10000)
                except Exception:
                    await asyncio.sleep(5.0)
                offers = await self._extract_from_dom(page, req)

            # Update cookie farm from successful browser session
            global _farmed_cookies, _farm_timestamp
            _farmed_cookies = await context.cookies()
            _farm_timestamp = time.monotonic()

            elapsed = time.monotonic() - t0
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Porter Playwright fallback error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    async def _wait_cloudflare(self, page, timeout: int = 30) -> bool:
        """Wait for Cloudflare challenge page to resolve. Returns True if cleared."""
        for _ in range(timeout):
            try:
                title = await page.title()
            except Exception:
                await asyncio.sleep(1.0)
                return True
            if "moment" not in title.lower() and "security" not in title.lower():
                return True
            await asyncio.sleep(1.0)
        return False

    async def _strategy_booking_form(self, page, req: FlightSearchRequest, t0: float) -> list[FlightOffer]:
        """Fallback: fill booking form at booking.flyporter.com → redirect → scrape DOM."""
        logger.info("Porter: using booking form fallback for %s→%s", req.origin, req.destination)
        await page.goto(_BOOKING_URL, wait_until="domcontentloaded", timeout=30000)

        try:
            await page.wait_for_selector("#autocomplete-destination", timeout=10000)
        except Exception:
            logger.warning("Porter: search form did not load within 10s")
            return []
        await asyncio.sleep(1.0)

        await self._dismiss_cookies(page)
        await self._set_one_way(page)
        await asyncio.sleep(0.5)

        if not await self._fill_airport(page, "#autocomplete-destination", req.origin):
            logger.warning("Porter: origin fill failed for %s", req.origin)
            return []
        await asyncio.sleep(0.5)

        if not await self._fill_airport(page, "#autocomplete-arrival", req.destination):
            logger.warning("Porter: destination fill failed for %s", req.destination)
            return []
        await asyncio.sleep(0.5)

        if not await self._fill_date(page, req):
            logger.warning("Porter: date fill failed")
            return []
        await asyncio.sleep(0.3)

        await self._click_search(page)

        remaining = max(self.timeout - (time.monotonic() - t0), 10)
        try:
            await page.wait_for_url("https://www.flyporter.com/**", timeout=remaining * 1000)
        except Exception:
            pass

        await self._wait_cloudflare(page, timeout=20)

        try:
            await page.wait_for_selector(
                "h1:has-text('Select Flights'), h2:has-text('Departing flights')",
                timeout=15000)
        except Exception:
            await asyncio.sleep(3.0)

        return await self._extract_from_dom(page, req)

    # ------------------------------------------------------------------
    # HTML parsing (for curl_cffi responses)
    # ------------------------------------------------------------------

    def _parse_html(self, html: str, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight offers from server-rendered HTML (curl_cffi path).

        Extracts flight cards from <li> elements containing h4 Departs/Arrives
        headings, flight numbers, durations, and fare buttons — same data as
        the DOM extraction but using regex on raw HTML.
        """
        if not html:
            return []

        offers: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)
        dep_date = req.date_from.strftime("%Y-%m-%d")

        # Split HTML into <li> blocks and look for flight cards
        li_blocks = re.findall(r"<li[^>]*>(.*?)</li>", html, re.DOTALL | re.IGNORECASE)

        for block in li_blocks:
            # Must have both Departs and Arrives headings
            dep_match = re.search(r"<h4[^>]*>\s*Departs\s*([\d:]+\s*[APap][Mm])", block)
            arr_match = re.search(r"<h4[^>]*>\s*Arrives\s*([\d:]+\s*[APap][Mm])", block)
            if not dep_match or not arr_match:
                continue

            dep_str = dep_match.group(1).strip()
            arr_str = arr_match.group(1).strip()

            # Flight number
            fn_match = re.search(r"PD\s*\d+", block)
            flight_num = fn_match.group(0).replace(" ", "") if fn_match else ""

            # Duration
            dur_min = 0
            dur_m = re.search(r"(\d+)\s*min", block)
            if dur_m:
                dur_min = int(dur_m.group(1))
            dur_h = re.search(r"(\d+)\s*h", block)
            if dur_h:
                dur_min += int(dur_h.group(1)) * 60

            # Stops
            nonstop = bool(re.search(r"non[- ]?stop", block, re.IGNORECASE))

            # Fare buttons: "Fare category: PorterClassic ... From $169"
            fares: list[dict[str, Any]] = []
            fare_matches = re.finditer(
                r"Fare\s+category:\s*(\w+).*?\$(\d+(?:,\d{3})*(?:\.\d{2})?)",
                block, re.DOTALL,
            )
            for fm in fare_matches:
                cat = fm.group(1)
                price = float(fm.group(2).replace(",", ""))
                fares.append({"category": cat, "price": price})

            if not fares:
                continue

            dep_time = self._parse_time(dep_str, dep_date)
            arr_time = self._parse_time(arr_str, dep_date)
            dur_sec = dur_min * 60

            seg = FlightSegment(
                airline="PD",
                airline_name="Porter Airlines",
                flight_no=flight_num,
                origin=req.origin,
                destination=req.destination,
                departure=dep_time,
                arrival=arr_time,
                duration_seconds=dur_sec,
            )
            route = FlightRoute(
                segments=[seg],
                stopovers=0 if nonstop else 1,
                total_duration_seconds=dur_sec,
            )

            for fare in fares:
                price = fare["price"]
                cat = fare["category"]
                offer_id = hashlib.md5(
                    f"PD-{flight_num}-{dep_time}-{cat}-{price}".encode()
                ).hexdigest()[:12]
                offers.append(FlightOffer(
                    id=offer_id,
                    price=float(price),
                    currency="CAD",
                    outbound=route,
                    airlines=["PD"],
                    owner_airline="PD",
                    source="porter_scraper",
                    source_tier="protocol",
                    is_locked=False,
                    booking_url=booking_url,
                ))

        if offers:
            logger.info("Porter: parsed %d offers from HTML", len(offers))
        return offers

    # ------------------------------------------------------------------
    # Browser form interaction helpers
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        for label in [
            "Accept All", "Accept all", "Accept", "I agree",
            "Got it", "OK", "Close", "Dismiss", "Accept Cookies",
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
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], '
                    + '[class*="onetrust"], [id*="onetrust"], [class*="modal-overlay"], '
                    + '[class*="popup"], [id*="popup"], [class*="privacy"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        """Select One-way trip type via the combobox dropdown."""
        try:
            trip_combo = page.locator("button[role='combobox']").filter(
                has_text=re.compile(r"round|one.?way|trip", re.IGNORECASE)
            ).first
            if await trip_combo.count() > 0:
                text = (await trip_combo.inner_text()).strip().lower()
                logger.info("Porter: current trip type: '%s'", text)
                if "one" in text:
                    return
                await trip_combo.click(timeout=3000)
                await asyncio.sleep(0.5)
                oneway = page.get_by_role("option", name=re.compile(r"one.?way", re.IGNORECASE))
                if await oneway.count() > 0:
                    await oneway.first.click(timeout=3000)
                    logger.info("Porter: set trip type to One-way")
                    await asyncio.sleep(0.5)
                    return
                else:
                    logger.warning("Porter: One-way option not found in dropdown")
            else:
                logger.warning("Porter: trip type combobox not found")
        except Exception as e:
            logger.debug("Porter: trip type combobox approach failed: %s", e)
        for label in ["One-way", "One Way", "One way"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if el and await el.count() > 0:
                    await el.click(timeout=3000)
                    logger.info("Porter: set trip type via text click '%s'", label)
                    return
            except Exception:
                continue
        logger.warning("Porter: could not set one-way — will fill return date as fallback")

    async def _fill_airport(self, page, selector: str, iata: str) -> bool:
        """Fill an airport combobox by its CSS selector, type IATA, and pick from dropdown."""
        try:
            field = page.locator(selector)
            if await field.count() == 0:
                logger.warning("Porter: selector %s not found", selector)
                return False

            label_sel = f"label[for='{selector.lstrip('#')}']"
            label_el = page.locator(label_sel)
            if await label_el.count() > 0:
                await label_el.first.click(timeout=3000)
                await asyncio.sleep(0.5)
            else:
                await field.click(timeout=3000, force=True)
                await asyncio.sleep(0.5)

            try:
                clear_btn = page.get_by_role("button", name="Clear")
                if await clear_btn.count() > 0 and await clear_btn.first.is_visible():
                    await clear_btn.first.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    if await label_el.count() > 0:
                        await label_el.first.click(timeout=2000)
                    else:
                        await field.click(timeout=2000, force=True)
                    await asyncio.sleep(0.3)
            except Exception:
                pass

            await field.fill("")
            await asyncio.sleep(0.2)
            await page.keyboard.type(iata, delay=100)
            await asyncio.sleep(2.5)

            spaced_iata = " ".join(iata)
            for pattern in [iata, spaced_iata]:
                option = page.get_by_role("option").filter(has_text=re.compile(rf"{re.escape(pattern)}", re.IGNORECASE)).first
                if await option.count() > 0:
                    await option.click(timeout=3000)
                    logger.info("Porter: selected airport %s from dropdown", iata)
                    return True

            any_option = page.get_by_role("option").first
            if await any_option.count() > 0:
                await any_option.click(timeout=3000)
                logger.info("Porter: selected first available airport option for %s", iata)
                return True

            await page.keyboard.press("Enter")
            logger.info("Porter: pressed Enter to confirm airport %s", iata)
            return True

        except Exception as e:
            logger.warning("Porter: airport fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill the departure date via calendar or text input."""
        from datetime import timedelta
        target = req.date_from
        try:
            ok = await self._fill_single_date(page, target, index=0, label="departure")
            if not ok:
                return False

            return_inputs = page.locator("input[placeholder='DD/MM/YYYY']")
            count = await return_inputs.count()
            if count >= 2:
                return_date = target + timedelta(days=7)
                logger.info("Porter: form still in round-trip mode, filling return date %s",
                            return_date.strftime("%d/%m/%Y"))
                await self._fill_single_date(page, return_date, index=1, label="return")

            return True
        except Exception as e:
            logger.warning("Porter: date error: %s", e)
        return False

    async def _fill_single_date(self, page, target, index: int = 0, label: str = "departure") -> bool:
        """Fill a single date field by index (0=departure, 1=return)."""
        date_str = target.strftime("%d/%m/%Y")
        try:
            date_input = page.locator("input[placeholder='DD/MM/YYYY']").nth(index)
            if await date_input.count() == 0:
                logger.warning("Porter: %s date input not found (index %d)", label, index)
                return False

            await date_input.click(timeout=5000, force=True)
            await asyncio.sleep(1.0)

            calendar = page.locator("[class*='calendar'], [class*='Calendar'], [role='dialog'], [class*='datepicker']").first
            if await calendar.count() > 0 and await calendar.is_visible():
                logger.info("Porter: calendar popup opened for %s date", label)
                picked = await self._pick_date_from_calendar(page, target)
                if picked:
                    await asyncio.sleep(0.5)
                    for btn_name in ["Done", "Apply", "Confirm", "OK", "Select"]:
                        done_btn = page.get_by_role("button", name=re.compile(rf"^{btn_name}$", re.IGNORECASE))
                        if await done_btn.count() > 0:
                            await done_btn.first.click(timeout=2000)
                            logger.info("Porter: clicked '%s' to confirm %s date", btn_name, label)
                            await asyncio.sleep(0.5)
                            break
                    logger.info("Porter: filled %s date %s via calendar", label, date_str)
                    return True
            else:
                logger.info("Porter: no calendar popup for %s date, filling as text", label)

            await date_input.fill(date_str)
            await asyncio.sleep(0.3)
            await page.keyboard.press("Tab")
            await asyncio.sleep(0.5)

            logger.info("Porter: filled %s date %s as text", label, date_str)
            return True
        except Exception as e:
            logger.warning("Porter: %s date fill error: %s", label, e)
            return False

    async def _pick_date_from_calendar(self, page, target) -> bool:
        """Navigate a calendar popup and click the target date."""
        try:
            target_my = target.strftime("%B %Y")
            day = target.day

            for _ in range(12):
                content = await page.content()
                if target_my.lower() in content.lower():
                    break
                fwd = page.locator("[class*='next'], [aria-label*='next'], [aria-label*='Next']").first
                if await fwd.count() > 0:
                    await fwd.click(timeout=2000)
                    await asyncio.sleep(0.4)
                else:
                    break

            for fmt in [
                f"{target.strftime('%B')} {day}, {target.year}",
                f"{day} {target.strftime('%B')} {target.year}",
                f"{target.strftime('%B')} {day}",
                target.strftime("%Y-%m-%d"),
            ]:
                day_btn = page.locator(f"[aria-label*='{fmt}']").first
                if await day_btn.count() > 0:
                    await day_btn.click(timeout=3000)
                    logger.info("Porter: picked date from calendar via aria-label")
                    return True

            day_btn = page.locator(
                "[class*='calendar'] button, [class*='datepicker'] button, table button"
            ).filter(has_text=re.compile(rf"^{day}$")).first
            if await day_btn.count() > 0:
                await day_btn.click(timeout=3000)
                logger.info("Porter: picked date %d from calendar grid", day)
                return True
        except Exception as e:
            logger.debug("Porter: calendar pick failed: %s", e)
        return False

    async def _click_search(self, page) -> None:
        """Click the 'Find Flights' button."""
        try:
            btn = page.get_by_role("button", name=re.compile(r"find flights", re.IGNORECASE))
            if await btn.count() > 0:
                await btn.first.click(timeout=5000)
                logger.info("Porter: clicked Find Flights")
                return
        except Exception as e:
            logger.warning("Porter: search click error: %s", e)
        for label in ["Search", "SEARCH", "Search Flights"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    # ------------------------------------------------------------------
    # DOM extraction (Playwright path)
    # ------------------------------------------------------------------

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flight cards from the www.flyporter.com results page DOM."""
        try:
            flight_data = await page.evaluate("""() => {
                const results = [];
                const debugInfo = {
                    url: document.location.href,
                    title: document.title,
                    bodyLen: document.body.innerText.length,
                    h4Count: document.querySelectorAll('h4').length,
                    liCount: document.querySelectorAll('li').length,
                };
                const items = document.querySelectorAll('li');
                for (const item of items) {
                    const headings = item.querySelectorAll('h4');
                    let dep = null, arr = null;
                    for (const h of headings) {
                        const t = h.textContent.trim();
                        if (t.startsWith('Departs')) dep = t.replace('Departs', '').trim();
                        if (t.startsWith('Arrives')) arr = t.replace('Arrives', '').trim();
                    }
                    if (!dep || !arr) continue;

                    let flightNum = '';
                    const allText = item.innerText;
                    const fnMatch = allText.match(/PD\\s*\\d+/);
                    if (fnMatch) flightNum = fnMatch[0].replace(/\\s+/g, '');

                    let duration = '';
                    const durMatch = allText.match(/(\\d+)\\s*min/);
                    if (durMatch) duration = durMatch[0];

                    const isNonstop = /non.?stop/i.test(allText);

                    const fares = [];
                    const fareButtons = item.querySelectorAll('button');
                    for (const btn of fareButtons) {
                        const bt = btn.textContent || '';
                        if (!bt.includes('Fare category')) continue;
                        const priceMatch = bt.match(/\\$(\\d+(?:,\\d{3})*(?:\\.\\d{2})?)/);
                        const catMatch = bt.match(/Fare category:\\s*([\\w]+)/);
                        if (priceMatch && catMatch) {
                            fares.push({
                                category: catMatch[1],
                                price: parseFloat(priceMatch[1].replace(',', '')),
                            });
                        }
                    }

                    if (fares.length > 0) {
                        results.push({ dep, arr, flightNum, duration, nonstop: isNonstop, fares });
                    }
                }
                return { flights: results, debug: debugInfo };
            }""")

            if not flight_data or not flight_data.get("flights"):
                logger.info("Porter: no flight cards found in DOM (debug: %s)",
                            flight_data.get("debug") if flight_data else "null")
                return []

            flights = flight_data["flights"]
            logger.info("Porter: extracted %d flights from DOM", len(flights))

            booking_url = self._build_booking_url(req)
            dep_date = req.date_from.strftime("%Y-%m-%d")
            offers: list[FlightOffer] = []

            for f in flights:
                dep_time = self._parse_time(f.get("dep", ""), dep_date)
                arr_time = self._parse_time(f.get("arr", ""), dep_date)
                dur_min = 0
                dur_match = re.search(r"(\d+)\s*min", f.get("duration", ""))
                if dur_match:
                    dur_min = int(dur_match.group(1))
                hr_match = re.search(r"(\d+)\s*h", f.get("duration", ""))
                if hr_match:
                    dur_min += int(hr_match.group(1)) * 60

                flight_num = f.get("flightNum", "")
                nonstop = f.get("nonstop", True)
                dur_sec = dur_min * 60

                seg = FlightSegment(
                    airline="PD",
                    airline_name="Porter Airlines",
                    flight_no=flight_num,
                    origin=req.origin,
                    destination=req.destination,
                    departure=dep_time,
                    arrival=arr_time,
                    duration_seconds=dur_sec,
                )
                route = FlightRoute(
                    segments=[seg],
                    stopovers=0 if nonstop else 1,
                    total_duration_seconds=dur_sec,
                )

                for fare in f.get("fares", []):
                    price = fare.get("price", 0)
                    cat = fare.get("category", "Economy")
                    offer_id = hashlib.md5(
                        f"PD-{flight_num}-{dep_time}-{cat}-{price}".encode()
                    ).hexdigest()[:12]
                    offers.append(FlightOffer(
                        id=offer_id,
                        price=float(price),
                        currency="CAD",
                        outbound=route,
                        airlines=["PD"],
                        owner_airline="PD",
                        source="porter_scraper",
                        source_tier="protocol",
                        is_locked=False,
                        booking_url=booking_url,
                    ))

            return offers
        except Exception as e:
            logger.warning("Porter: DOM extraction error: %s", e)
        return []

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_time(time_str: str, date_str: str) -> datetime:
        """Parse '7:25AM' into a datetime object."""
        time_str = time_str.strip().upper()
        for fmt in ["%I:%M%p", "%I:%M %p"]:
            try:
                t = datetime.strptime(time_str, fmt)
                d = datetime.strptime(date_str, "%Y-%m-%d")
                return d.replace(hour=t.hour, minute=t.minute, second=0)
            except ValueError:
                continue
        return datetime.strptime(date_str, "%Y-%m-%d")

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Porter %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"porter{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.flyporter.com/en/flight-results?from={req.origin}"
            f"&to={req.destination}&departure={dep}&adults={req.adults}&tripType=oneway"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"porter{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )


def _is_cloudflare_challenge(html: str) -> bool:
    """Detect Cloudflare challenge/block pages in HTML response."""
    if not html:
        return True
    markers = [
        "Just a moment",
        "Checking your browser",
        "cf-browser-verification",
        "challenge-platform",
        "_cf_chl",
        "Attention Required",
        "Enable JavaScript and cookies",
    ]
    html_lower = html.lower()
    return any(m.lower() in html_lower for m in markers)
