"""
Transavia hybrid scraper — cookie-farm + curl_cffi direct API.

Transavia operates as HV (Transavia Netherlands) and TO (Transavia France).
Part of the Air France-KLM group.

Strategy (multi-tier, Mar 2026):
1. (Fastest ~0.5-2s) Direct POST to /api/search/oneway — semi-public API
   endpoint from the AF-KLM group. No cookies or session needed.
2. (Fast ~1-3s) curl_cffi session: GET homepage first to establish Cloudflare
   session, then GET booking page, then GET /start/api/flight-availability.
3. (Slow ~8-15s) Full Playwright browser flow as fallback.

Transavia booking page form (verified Mar 2026):
  - #one-way radio → selects one-way trip
  - #first-from-departure combobox → type IATA, select from role="option"
  - #first-to-arrival combobox → type IATA, select from role="option"
  - #flightDates_from-date-input combobox → click opens calendar (react-day-picker)
  - Calendar: td[data-day='YYYY-MM-DD'] button, [aria-label='Go to next month']
  - Search button → triggers internal API call
  Cookie banner: "Accept all cookies" button on homepage
  Booking URL pattern: transavia.com/book/en-eu/search-a-flight?ds=AMS&as=SPC
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
import time
from datetime import datetime
from typing import Any, Optional

try:
    from curl_cffi import requests as cffi_requests
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

logger = logging.getLogger(__name__)

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-NL"]
_TIMEZONES = ["Europe/London", "Europe/Amsterdam", "Europe/Paris", "Europe/Berlin"]

_AVAILABILITY_URL = "https://www.transavia.com/start/api/flight-availability"
# Direct search API endpoints (AF-KLM group semi-public APIs)
_SEARCH_API_URLS = [
    "https://www.transavia.com/api/search/oneway",
    "https://www.transavia.com/start/api/search",
]
_IMPERSONATE = "chrome131"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
_COOKIE_MAX_AGE = 25 * 60  # Re-farm cookies after 25 minutes

# ── Shared cookie farm state ──────────────────────────────────────────────
_farm_lock: Optional[asyncio.Lock] = None
_farmed_cookies: list[dict] = []
_farm_timestamp: float = 0.0

# ── Shared browser singleton (Playwright, for cookie farming + fallback) ──
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


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
    """Shared headed Chromium for cookie farming + fallback (launched once, reused)."""
    global _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser

        from connectors.browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("Transavia: browser launched for cookie farming")
        return _browser


class TransaviaConnectorClient:
    """Transavia hybrid scraper — cookie-farm + curl_cffi direct API."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Transavia flights via direct API / cookie-farm + curl_cffi.

        Fast path (~0.5-2s): Direct POST to Transavia search API (no cookies).
        Medium path (~1-3s): curl_cffi session warmed via homepage + availability API.
        Slow path (~15s): Playwright farms cookies first, then curl_cffi.
        Fallback: Full Playwright browser flow if API fails.
        """
        t0 = time.monotonic()

        try:
            if not HAS_CURL:
                return await self._playwright_fallback(req, t0)

            # Try cookieless direct API first (fastest)
            data = await self._api_search(req, cookies=[])
            if data:
                logger.info("Transavia: cookieless API succeeded")
            else:
                # Try with farmed cookies
                cookies = await self._ensure_cookies()
                if cookies:
                    data = await self._api_search(req, cookies)

                # If search failed (expired cookies), re-farm once and retry
                if data is None:
                    logger.info("Transavia: API search failed, re-farming cookies")
                    cookies = await self._farm_cookies()
                    if cookies:
                        data = await self._api_search(req, cookies)

            # If API still fails, fall back to full Playwright
            if not data:
                logger.warning("Transavia: API search returned no data, falling back to Playwright")
                return await self._playwright_fallback(req, t0)

            elapsed = time.monotonic() - t0
            offers = self._parse_response(data, req)
            offers.sort(key=lambda o: o.price)

            logger.info(
                "Transavia %s→%s returned %d offers in %.1fs (hybrid API)",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"transavia{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else (req.currency or "EUR"),
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Transavia hybrid error: %s", e)
            return self._empty(req)

    # ------------------------------------------------------------------
    # Cookie farm — Playwright generates Cloudflare cookies
    # ------------------------------------------------------------------

    async def _ensure_cookies(self) -> list[dict]:
        """Return cached cookies or farm fresh ones."""
        global _farmed_cookies, _farm_timestamp
        if _farmed_cookies and (time.monotonic() - _farm_timestamp) < _COOKIE_MAX_AGE:
            return _farmed_cookies
        return await self._farm_cookies()

    async def _farm_cookies(self) -> list[dict]:
        """Open Playwright, navigate to Transavia homepage to farm Cloudflare cookies."""
        global _farmed_cookies, _farm_timestamp
        lock = _get_farm_lock()
        async with lock:
            # Double-check after acquiring lock
            if _farmed_cookies and (time.monotonic() - _farm_timestamp) < _COOKIE_MAX_AGE:
                return _farmed_cookies

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

                logger.info("Transavia: farming cookies via homepage load")
                await page.goto(
                    "https://www.transavia.com/en-EU/home/",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                await asyncio.sleep(3.0)

                # Dismiss cookie banner
                await self._dismiss_cookies(page)
                await asyncio.sleep(1.5)

                # Navigate to booking page to warm up session cookies
                await page.goto(
                    "https://www.transavia.com/book/en-eu/search-a-flight",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                await asyncio.sleep(2.0)
                await self._dismiss_cookies(page)

                # Extract cookies
                cookies = await context.cookies()
                if cookies:
                    _farmed_cookies = cookies
                    _farm_timestamp = time.monotonic()
                    logger.info("Transavia: farmed %d cookies", len(cookies))
                    return cookies

                logger.warning("Transavia: cookie farm returned no cookies")
                return []

            except Exception as e:
                logger.error("Transavia: cookie farm error: %s", e)
                return []
            finally:
                await context.close()

    # ------------------------------------------------------------------
    # Direct API via curl_cffi
    # ------------------------------------------------------------------

    async def _api_search(
        self, req: FlightSearchRequest, cookies: list[dict],
    ) -> Optional[dict]:
        """GET /start/api/flight-availability via curl_cffi with farmed cookies."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._api_search_sync, req, cookies)

    def _api_search_sync(
        self, req: FlightSearchRequest, cookies: list[dict],
    ) -> Optional[dict]:
        """Synchronous curl_cffi: direct POST API first, then session-warmed flow."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)

        # Load farmed cookies into session
        for c in cookies:
            domain = c.get("domain", "")
            sess.cookies.set(c["name"], c["value"], domain=domain)

        # ── Strategy 1: Direct POST to search API (no session needed) ──
        data = self._try_direct_search(sess, req)
        if data:
            return data

        # ── Strategy 2: Session-warmed flow ────────────────────────────
        # GET homepage first to establish Cloudflare clearance, then
        # GET booking page to initialise server-side search session,
        # then GET /start/api/flight-availability for results.

        html_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
            "User-Agent": _UA,
        }

        # Step 1: Warm session via homepage (sets cf_clearance cookie)
        try:
            sess.get(
                "https://www.transavia.com/en-EU/home/",
                headers=html_headers,
                timeout=15,
            )
        except Exception as e:
            logger.debug("Transavia: homepage warm-up failed: %s", e)

        # Step 2: Load booking page to initialise server-side search session
        booking_url = self._build_booking_url(req)
        html_headers["Referer"] = "https://www.transavia.com/en-EU/home/"
        try:
            r = sess.get(booking_url, headers=html_headers, timeout=15)
            if r.status_code != 200:
                logger.warning("Transavia: booking page returned %d", r.status_code)
        except Exception as e:
            logger.warning("Transavia: booking page request failed: %s", e)

        # Step 3: Fetch flight availability from the API
        # Try even if booking page returned non-200 — the session cookies
        # from the homepage may be enough for the API endpoint.
        api_headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-GB,en;q=0.9",
            "User-Agent": _UA,
            "Referer": booking_url,
        }

        try:
            r = sess.get(
                _AVAILABILITY_URL,
                params={"type": "full", "update": "false"},
                headers=api_headers,
                timeout=15,
            )
        except Exception as e:
            logger.error("Transavia: API request failed: %s", e)
            return None

        if r.status_code != 200:
            logger.warning("Transavia: API returned %d", r.status_code)
            return None

        try:
            data = r.json()
        except Exception as e:
            logger.warning("Transavia: API response not JSON: %s", e)
            return None

        return self._validate_api_data(data)

    # ------------------------------------------------------------------
    # Direct POST search (AF-KLM semi-public API)
    # ------------------------------------------------------------------

    def _try_direct_search(
        self, sess: Any, req: FlightSearchRequest,
    ) -> Optional[dict]:
        """POST search params to Transavia's direct search API endpoints."""
        dep_str = req.date_from.strftime("%Y-%m-%d")
        payload = json.dumps({
            "routesList": [
                {"origin": req.origin, "destination": req.destination}
            ],
            "datesRange": {
                "startDate": dep_str,
                "endDate": dep_str,
            },
            "passengers": {
                "adults": req.adults,
                "children": req.children or 0,
                "infants": req.infants or 0,
            },
            "searchType": "oneway",
            "currency": req.currency or "EUR",
        })
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-GB,en;q=0.9",
            "Content-Type": "application/json",
            "User-Agent": _UA,
            "Origin": "https://www.transavia.com",
            "Referer": "https://www.transavia.com/en-EU/home/",
        }

        for api_url in _SEARCH_API_URLS:
            try:
                r = sess.post(api_url, data=payload, headers=headers, timeout=15)
                if r.status_code != 200:
                    logger.debug("Transavia: direct API %s returned %d", api_url, r.status_code)
                    continue
                data = r.json()
                result = self._validate_api_data(data)
                if result:
                    logger.info("Transavia: direct API hit via %s", api_url)
                    return result
            except Exception as e:
                logger.debug("Transavia: direct API %s error: %s", api_url, e)
                continue

        return None

    # ------------------------------------------------------------------
    # API response validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_api_data(data: Optional[dict]) -> Optional[dict]:
        """Check that an API response dict contains flight data."""
        if not data or not isinstance(data, dict):
            return None

        ob = data.get("outboundFlight")
        if ob and isinstance(ob, dict) and ob.get("timeSlots"):
            logger.info("Transavia: API returned %d time slots", len(ob["timeSlots"]))
            return data

        # Check for legacy / alternative response formats
        if any(data.get(k) for k in ("outboundFlights", "outbound", "flights", "availableFlights")):
            return data

        logger.debug("Transavia: API response has no flight data")
        return None

    # ------------------------------------------------------------------
    # Playwright fallback (full browser flow, used if API fails)
    # ------------------------------------------------------------------

    async def _playwright_fallback(
        self, req: FlightSearchRequest, t0: float,
    ) -> FlightSearchResponse:
        """Full Playwright browser flow as fallback."""
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

            # Set up response interception for flight-availability API
            captured_data: dict = {}

            async def on_response(response):
                try:
                    url = response.url.lower()
                    if response.status == 200 and any(k in url for k in (
                        "flight-availability", "flightavailability",
                        "api/search", "api/flights", "availability",
                    )):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, dict):
                                ob = data.get("outboundFlight")
                                if (ob and ob.get("timeSlots")) or any(
                                    data.get(k) for k in ("outboundFlights", "outbound", "flights")
                                ):
                                    captured_data["json"] = data
                                    logger.info("Transavia: intercepted flight data from %s", response.url[:120])
                except Exception:
                    pass

            page.on("response", on_response)

            # Step 1: Homepage to get cf_clearance cookie
            logger.info("Transavia: Playwright fallback for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.transavia.com/en-EU/home/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(2.0)
            await self._dismiss_cookies(page)
            await asyncio.sleep(1.0)

            # Step 2: Navigate to booking page
            logger.info("Transavia: loading booking page (fallback)")
            await page.goto(
                "https://www.transavia.com/book/en-eu/search-a-flight",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3.0)

            if "/book/" not in page.url:
                logger.warning("Transavia: booking page redirected to %s, retrying", page.url)
                await page.goto(
                    "https://www.transavia.com/book/en-eu/search-a-flight",
                    wait_until="domcontentloaded",
                    timeout=int(self.timeout * 1000),
                )
                await asyncio.sleep(3.0)

            # Dismiss cookies again (booking page may show its own banner)
            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)

            # Step 3: Fill form
            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("Transavia: fallback form fill failed")
                return self._empty(req)

            # Step 4: Click search
            await self._click_search(page)

            # Wait for intercepted response or timeout
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if captured_data.get("json"):
                    break
                await asyncio.sleep(0.5)

            # Step 5: Use intercepted data first, then in-page fetch, then DOM
            data = captured_data.get("json")
            if not data:
                data = await self._fetch_flight_availability(page)
            if not data:
                offers = await self._extract_from_dom(page, req)
                if offers:
                    return self._build_response(offers, req, time.monotonic() - t0)
                return self._empty(req)

            # Also update cookie farm from this successful browser session
            global _farmed_cookies, _farm_timestamp
            _farmed_cookies = await context.cookies()
            _farm_timestamp = time.monotonic()

            elapsed = time.monotonic() - t0
            offers = self._parse_response(data, req)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Transavia Playwright fallback error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Cookie dismissal
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        for label in [
            "Accept all cookies", "Accept All Cookies", "Accept all",
            "Accept", "Accepteer alle cookies", "Tout accepter",
        ]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue

        # Try link-style accept
        try:
            accept = page.locator("text=Accept all cookies").first
            if await accept.count() > 0:
                await accept.click(timeout=2000)
                await asyncio.sleep(0.5)
                return
        except Exception:
            pass

        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="Cookie"], [id*="Cookie"], [class*="modal"][style*="z-index"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Form filling (booking page, ID-based selectors)
    # ------------------------------------------------------------------

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        # One-way toggle
        try:
            ow = page.locator("#one-way")
            if await ow.count() > 0:
                await ow.click(force=True, timeout=3000)
                await asyncio.sleep(0.5)
                logger.info("Transavia: one-way selected")
        except Exception as e:
            logger.debug("Transavia: one-way toggle error: %s", e)

        # From airport
        ok = await self._fill_airport_by_id(page, "#first-from-departure", req.origin, "From")
        if not ok:
            ok = await self._fill_airport_field(page, "From", req.origin)
        if not ok:
            return False
        await asyncio.sleep(0.8)

        # To airport
        ok = await self._fill_airport_by_id(page, "#first-to-arrival", req.destination, "To")
        if not ok:
            ok = await self._fill_airport_field(page, "To", req.destination)
        if not ok:
            return False
        await asyncio.sleep(0.8)

        # Verify From wasn't cleared (known React issue)
        try:
            from_el = page.locator("#first-from-departure")
            if await from_el.count() > 0:
                val = await from_el.input_value()
                if not val.strip():
                    logger.info("Transavia: From field cleared after To, re-filling")
                    await self._fill_airport_by_id(page, "#first-from-departure", req.origin, "From")
                    await asyncio.sleep(0.5)
        except Exception:
            pass

        # Date
        ok = await self._fill_date(page, req)
        return ok

    async def _fill_airport_by_id(self, page, selector: str, iata: str, label: str) -> bool:
        """Fill airport field using ID selector, select from dropdown options."""
        try:
            field = page.locator(selector)
            if await field.count() == 0:
                logger.debug("Transavia: %s selector %s not found", label, selector)
                return False
            await field.click(timeout=3000)
            await asyncio.sleep(0.3)
            await field.fill(iata)
            await asyncio.sleep(2.0)

            opts = page.get_by_role("option")
            opt_count = await opts.count()
            logger.debug("Transavia: %s suggestions: %d", label, opt_count)
            for i in range(opt_count):
                txt = await opts.nth(i).text_content()
                if iata.lower() in txt.lower():
                    await opts.nth(i).click(timeout=3000)
                    logger.info("Transavia: %s = %s", label, txt.strip())
                    return True

            # Fallback: click first option if any
            if opt_count > 0:
                await opts.first.click(timeout=3000)
                logger.info("Transavia: %s = first option", label)
                return True

            # No options appeared, try typing letter by letter
            logger.debug("Transavia: %s no options from fill, trying keyboard input", label)
            await field.click(timeout=3000)
            await field.fill("")
            await asyncio.sleep(0.3)
            for ch in iata:
                await page.keyboard.type(ch, delay=100)
            await asyncio.sleep(2.0)

            opt_count = await opts.count()
            logger.debug("Transavia: %s keyboard suggestions: %d", label, opt_count)
            for i in range(opt_count):
                txt = await opts.nth(i).text_content()
                if iata.lower() in txt.lower():
                    await opts.nth(i).click(timeout=3000)
                    logger.info("Transavia: %s = %s (keyboard)", label, txt.strip())
                    return True
            if opt_count > 0:
                await opts.first.click(timeout=3000)
                return True

            await page.keyboard.press("Enter")
            return True
        except Exception as e:
            logger.debug("Transavia: %s by ID error: %s", label, e)
            return False

    async def _fill_airport_field(self, page, label: str, iata: str) -> bool:
        """Fallback: fill airport field using role-based selectors."""
        try:
            field = page.get_by_role("combobox", name=re.compile(rf"^{label}$", re.IGNORECASE))
            if await field.count() == 0:
                field = page.get_by_role("textbox", name=re.compile(rf"^{label}$", re.IGNORECASE))
            if await field.count() == 0:
                return False

            await field.first.click(timeout=3000)
            await asyncio.sleep(0.3)
            await field.first.fill(iata)
            await asyncio.sleep(1.5)

            opts = page.get_by_role("option")
            for i in range(await opts.count()):
                txt = await opts.nth(i).text_content()
                if iata.lower() in txt.lower():
                    await opts.nth(i).click(timeout=3000)
                    logger.info("Transavia: selected %s for %s", iata, label)
                    return True

            if await opts.count() > 0:
                await opts.first.click(timeout=3000)
                return True

            await page.keyboard.press("Enter")
            return True
        except Exception as e:
            logger.debug("Transavia: %s field error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill date using calendar picker on booking page."""
        target = req.date_from
        try:
            # Click the date input to open calendar
            date_el = page.locator("#flightDates_from-date-input")
            if await date_el.count() == 0:
                date_el = page.locator("[aria-label='dd-mm-yyyy']").first
            if await date_el.count() == 0:
                date_el = page.get_by_role("combobox", name=re.compile(r"Depart", re.IGNORECASE))
            if await date_el.count() == 0:
                logger.warning("Transavia: no date input found")
                return False

            await date_el.click(timeout=3000)
            await asyncio.sleep(1.0)

            expanded = await date_el.get_attribute("aria-expanded")
            if expanded != "true":
                # Try clicking again
                await date_el.click(timeout=3000)
                await asyncio.sleep(1.0)

            # Navigate calendar to target month and click the day
            target_iso = target.strftime("%Y-%m-%d")
            for _ in range(12):
                day_btn = page.locator(f"td[data-day='{target_iso}'] button")
                if await day_btn.count() > 0:
                    await day_btn.click(timeout=3000)
                    logger.info("Transavia: selected date %s", target_iso)
                    await asyncio.sleep(0.5)
                    return True
                # Click next month
                next_btn = page.locator("[aria-label='Go to next month']")
                if await next_btn.count() > 0:
                    await next_btn.click(timeout=2000)
                    await asyncio.sleep(0.5)
                else:
                    break

            # Fallback: try typing the date directly
            logger.info("Transavia: calendar day not found, typing date")
            await date_el.fill(target.strftime("%d-%m-%Y"))
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            logger.warning("Transavia: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        for label in ["Search", "search", "Search flights", "Zoeken"]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    logger.info("Transavia: clicked search")
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    # ------------------------------------------------------------------
    # Flight data retrieval via in-page fetch
    # ------------------------------------------------------------------

    async def _fetch_flight_availability(self, page) -> Optional[dict]:
        """Call flight-availability API via in-page fetch after form submission."""
        for api_url in [
            "/start/api/flight-availability?type=full&update=false",
            "/start/api/flight-availability?type=full",
        ]:
            try:
                result = await page.evaluate(f"""async () => {{
                    try {{
                        const r = await fetch('{api_url}');
                        if (r.status !== 200) return null;
                        const data = await r.json();
                        return data;
                    }} catch (e) {{
                        return null;
                    }}
                }}""")
                if result and isinstance(result, dict):
                    ob = result.get("outboundFlight")
                    if ob and ob.get("timeSlots"):
                        logger.info("Transavia: got %d time slots from %s", len(ob["timeSlots"]), api_url)
                        return result
            except Exception as e:
                logger.debug("Transavia: fetch %s error: %s", api_url, e)
                continue
        return None

    # ------------------------------------------------------------------
    # DOM fallback
    # ------------------------------------------------------------------

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                if (window.appData) return window.appData;
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.outbound || d.journeys || d.fares)) return d;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
        return []

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Primary format: outboundFlight.timeSlots (from /start/api/flight-availability)
        ob = data.get("outboundFlight")
        if ob and isinstance(ob, dict):
            slots = ob.get("timeSlots", [])
            if isinstance(slots, list):
                for slot in slots:
                    offer = self._parse_timeslot(slot, req, booking_url)
                    if offer:
                        offers.append(offer)

        if offers:
            return offers

        # Fallback: legacy API formats
        currency = data.get("currency", req.currency or "EUR")
        flights_raw = (
            data.get("outboundFlights") or data.get("outbound")
            or data.get("flights") or data.get("flightOffer")
            or (data.get("journeys", {}).get("outbound") if isinstance(data.get("journeys"), dict) else None)
            or []
        )
        if not isinstance(flights_raw, list):
            flights_raw = []

        for flight in flights_raw:
            offer = self._parse_single_flight(flight, currency, req, booking_url)
            if offer:
                offers.append(offer)

        if not offers and "availableFlights" in data:
            af = data["availableFlights"]
            if isinstance(af, list):
                for flight in af:
                    offer = self._parse_single_flight(flight, currency, req, booking_url)
                    if offer:
                        offers.append(offer)

        return offers

    def _parse_timeslot(
        self, slot: dict, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        """Parse a timeSlot from the flight-availability API."""
        price = slot.get("price")
        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        flight_no = str(slot.get("flightNumber", ""))
        carrier = "TO" if flight_no.startswith("TO") else "HV"

        dep_str = slot.get("departureDateTime", "")
        arr_str = slot.get("arrivalDateTime", "")
        dep_dt = self._parse_dt(dep_str)
        arr_dt = self._parse_dt(arr_str)

        origin = slot.get("departure") or req.origin
        destination = slot.get("arrival") or req.destination

        # Parse duration "HH:MM" format
        duration_seconds = 0
        dur_str = slot.get("duration", "")
        if dur_str and ":" in dur_str:
            parts = dur_str.split(":")
            try:
                duration_seconds = int(parts[0]) * 3600 + int(parts[1]) * 60
            except (ValueError, IndexError):
                pass
        if not duration_seconds and dep_dt.year > 2000 and arr_dt.year > 2000:
            duration_seconds = int((arr_dt - dep_dt).total_seconds())

        segment = FlightSegment(
            airline=carrier, airline_name="Transavia", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=dep_dt, arrival=arr_dt,
            cabin_class="M",
        )
        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=max(duration_seconds, 0),
            stopovers=0,
        )

        slot_key = slot.get("value") or slot.get("name") or f"{flight_no}_{dep_str}"
        return FlightOffer(
            id=f"hv_{hashlib.md5(str(slot_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency="EUR",
            price_formatted=slot.get("formattedPrice", f"{price:.2f} EUR"),
            outbound=route, inbound=None,
            airlines=["Transavia"], owner_airline=carrier,
            booking_url=booking_url, is_locked=False,
            source="transavia_direct", source_tier="free",
        )

    def _parse_single_flight(
        self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str,
    ) -> Optional[FlightOffer]:
        price = (
            flight.get("price") or flight.get("totalPrice") or flight.get("lowestFare")
            or flight.get("farePrice") or self._extract_cheapest_fare(flight)
        )
        if price is None:
            # Try nested priceDetails
            pd = flight.get("priceDetails") or flight.get("pricing") or {}
            price = pd.get("totalPrice") or pd.get("price") or pd.get("amount")
        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        cur = flight.get("currency") or currency

        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination))
        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        # Determine carrier code (HV or TO)
        carrier = "HV"
        for seg in segments:
            if seg.flight_no and seg.flight_no.startswith("TO"):
                carrier = "TO"
                break

        route = FlightRoute(
            segments=segments, total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("flightKey") or flight.get("id") or flight.get("flightId")
            or flight.get("flightNumber", "") + "_" + segments[0].departure.isoformat()
        )
        return FlightOffer(
            id=f"hv_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency=cur,
            price_formatted=f"{price:.2f} {cur}",
            outbound=route, inbound=None,
            airlines=["Transavia"], owner_airline=carrier,
            booking_url=booking_url, is_locked=False,
            source="transavia_direct", source_tier="free",
        )

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = seg.get("departure") or seg.get("departureDate") or seg.get("departureDateTime") or seg.get("departureTime") or seg.get("std") or ""
        arr_str = seg.get("arrival") or seg.get("arrivalDate") or seg.get("arrivalDateTime") or seg.get("arrivalTime") or seg.get("sta") or ""
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("flightNo") or seg.get("number") or "").replace(" ", "")
        origin = seg.get("origin") or seg.get("departureAirport") or seg.get("departureStation") or seg.get("departureCode") or default_origin
        destination = seg.get("destination") or seg.get("arrivalAirport") or seg.get("arrivalStation") or seg.get("arrivalCode") or default_dest

        carrier_code = "HV"
        if flight_no.startswith("TO"):
            carrier_code = "TO"

        return FlightSegment(
            airline=carrier_code, airline_name="Transavia", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    @staticmethod
    def _extract_cheapest_fare(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareBundles") or flight.get("fareOptions") or []
        prices: list[float] = []
        for f in fares:
            p = f.get("price") or f.get("amount") or f.get("totalPrice")
            if p is not None:
                try:
                    prices.append(float(p))
                except (TypeError, ValueError):
                    continue
        return min(prices) if prices else None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Transavia %s→%s returned %d offers in %.1fs (fallback)", req.origin, req.destination, len(offers), elapsed)
        search_hash = hashlib.md5(f"transavia{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "EUR"),
            offers=offers, total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[: len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep_m = req.date_from.month
        dep_y = req.date_from.year
        return (
            f"https://www.transavia.com/book/en-eu/search-a-flight"
            f"?ds={req.origin}&as={req.destination}"
            f"&om={dep_m}&oy={dep_y}&r=False"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"transavia{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=req.currency or "EUR", offers=[], total_results=0,
        )
