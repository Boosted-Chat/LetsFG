"""
Scoot hybrid scraper — curl_cffi direct Navitaire API + cookie-farm fallback.

Scoot (IATA: TR) is Singapore Airlines' low-cost subsidiary operating from SIN.
Uses a Navitaire New Skies booking engine at booking.flyscoot.com.
Protected by Akamai Bot Manager — curl_cffi bypasses TLS fingerprint checks.

Strategy (hybrid curl_cffi + cookie-farm):
1. FAST PATH (~1-3s): curl_cffi (impersonate=chrome124) to:
   a. Bootstrap anonymous Navitaire session (JWT token)
   b. POST availability search API
   c. Parse Navitaire trips/journeys/fares response
2. COOKIE FARM (~15-25s): If direct API fails, Playwright opens booking site,
   intercepts the JWT + Akamai _abck cookies, then curl_cffi replays.
3. PLAYWRIGHT FALLBACK: Full browser with form fill + API interception.

Key API structure (Mar 2026):
  Token endpoints (tried in order):
    POST https://booking.flyscoot.com/api/nsk/v1/token
    GET  https://booking.flyscoot.com/api/v1/account/anonymous
  Search endpoint:
    POST https://booking.flyscoot.com/api/nsk/v4/availability/search
  Auth headers: Authorization: <JWT>, x-scoot-appsource: IBE-WEB
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import time
from datetime import datetime
from typing import Any, Optional

from curl_cffi import requests as cffi_requests

from boostedtravel.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from boostedtravel.connectors.browser import stealth_args

logger = logging.getLogger(__name__)

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-SG", "en-US", "en-GB", "en-AU"]
_TIMEZONES = [
    "Asia/Singapore", "Asia/Kuala_Lumpur", "Asia/Bangkok",
    "Asia/Tokyo", "Australia/Sydney",
]

_BOOKING_BASE = "https://booking.flyscoot.com"
_TOKEN_ENDPOINTS = [
    ("POST", f"{_BOOKING_BASE}/api/nsk/v1/token"),
    ("GET", f"{_BOOKING_BASE}/api/v1/account/anonymous"),
]
_AVAIL_ENDPOINT = f"{_BOOKING_BASE}/api/nsk/v4/availability/search"
_AVAIL_ENDPOINTS_ALT = [
    f"{_BOOKING_BASE}/api/v2/availability",
    f"{_BOOKING_BASE}/api/nsk/v3/availability/search",
]
_IMPERSONATE = "chrome124"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)
_COOKIE_MAX_AGE = 25 * 60  # Re-farm cookies after 25 minutes
_MAX_ATTEMPTS = 2

# ── Shared cookie-farm state ──────────────────────────────────────────────
_farm_lock: Optional[asyncio.Lock] = None
_farmed_token: str = ""
_farmed_cookies: list[dict] = []
_farm_timestamp: float = 0.0
_pw_instance = None
_browser = None


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _get_browser():
    """Shared headed Chromium for cookie farming (launched once, reused)."""
    global _pw_instance, _browser
    if _browser and _browser.is_connected():
        return _browser
    from boostedtravel.connectors.browser import launch_headed_browser
    _browser = await launch_headed_browser()
    logger.info("Scoot: browser launched for cookie farming")
    return _browser


class ScootConnectorClient:
    """Scoot hybrid scraper — curl_cffi direct Navitaire API + cookie-farm."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Scoot flights via hybrid curl_cffi + cookie-farm.

        Fast path (~1-3s): curl_cffi direct to Navitaire API.
        Cookie farm (~15-25s): Playwright farms JWT + cookies, curl_cffi replays.
        Fallback: Full Playwright browser interception.
        """
        t0 = time.monotonic()

        try:
            # Fast path: try direct API (may work without cookies)
            data = await self._api_search(req, token="", cookies=[])
            if data:
                logger.info("Scoot: cookieless API succeeded")
            else:
                # Try with farmed session
                token, cookies = await self._ensure_session(req)
                if token or cookies:
                    data = await self._api_search(req, token=token, cookies=cookies)

                # Re-farm once if stale
                if data is None and (token or cookies):
                    logger.info("Scoot: API failed with farmed session, re-farming")
                    token, cookies = await self._farm_session(req)
                    if token or cookies:
                        data = await self._api_search(req, token=token, cookies=cookies)

                # Last resort: full Playwright
                if not data:
                    logger.warning("Scoot: API returned no data, falling back to Playwright")
                    return await self._playwright_fallback(req, t0)

            elapsed = time.monotonic() - t0
            offers = self._parse_navitaire_response(data, req)
            if not offers:
                offers = self._parse_flat_response(data, req)
            offers.sort(key=lambda o: o.price)

            logger.info(
                "Scoot %s→%s returned %d offers in %.1fs (hybrid API)",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"scoot{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else (req.currency or "SGD"),
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Scoot hybrid error: %s", e)
            return self._empty(req)

    # ------------------------------------------------------------------
    # Direct API via curl_cffi
    # ------------------------------------------------------------------

    async def _api_search(
        self, req: FlightSearchRequest, *, token: str, cookies: list[dict],
    ) -> Optional[dict]:
        """POST Navitaire availability search via curl_cffi."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._api_search_sync, req, token, cookies,
        )

    def _api_search_sync(
        self, req: FlightSearchRequest, token: str, cookies: list[dict],
    ) -> Optional[dict]:
        """Synchronous curl_cffi Navitaire availability search."""
        # Bootstrap token if not provided
        if not token:
            token = self._bootstrap_token_sync(cookies)
        if not token and not cookies:
            return None  # No credentials at all

        sess = cffi_requests.Session(impersonate=_IMPERSONATE)
        for c in cookies:
            domain = c.get("domain", "")
            sess.cookies.set(c["name"], c["value"], domain=domain)

        dep = req.date_from.strftime("%Y-%m-%dT00:00:00")
        body = {
            "criteria": [{
                "stations": {
                    "originStationCodes": [req.origin],
                    "destinationStationCodes": [req.destination],
                    "searchOriginMacs": True,
                    "searchDestinationMacs": True,
                },
                "dates": {
                    "beginDate": dep,
                    "endDate": dep,
                },
                "filters": {
                    "compressionType": "ROUND_TRIP",
                    "maxConnections": -1,
                },
                "passengers": {
                    "types": self._build_passenger_types(req),
                },
            }],
            "codes": {
                "currencyCode": req.currency or "SGD",
            },
        }

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-SG,en;q=0.9",
            "Content-Type": "application/json",
            "Referer": f"{_BOOKING_BASE}/",
            "Origin": _BOOKING_BASE,
            "x-scoot-appsource": "IBE-WEB",
        }
        if token:
            headers["Authorization"] = (
                token if token.startswith("Bearer") else f"Bearer {token}"
            )

        # Try primary endpoint, then alternatives
        for endpoint in [_AVAIL_ENDPOINT] + _AVAIL_ENDPOINTS_ALT:
            try:
                r = sess.post(
                    endpoint,
                    json=body,
                    headers=headers,
                    timeout=15,
                )
            except Exception as e:
                logger.debug("Scoot: API request to %s failed: %s", endpoint, e)
                continue

            if r.status_code == 403:
                logger.debug("Scoot: 403 from %s (Akamai block)", endpoint)
                continue
            if r.status_code not in (200, 201):
                logger.debug("Scoot: %d from %s", r.status_code, endpoint)
                continue

            try:
                data = r.json()
            except Exception:
                continue

            # Validate we got actual flight data
            if isinstance(data, dict):
                if data.get("trips") or data.get("data", {}).get("trips"):
                    return data
                if data.get("journeys") or data.get("faresAvailable"):
                    return data

        return None

    def _bootstrap_token_sync(self, cookies: list[dict]) -> str:
        """Get anonymous Navitaire JWT token via curl_cffi."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)
        for c in cookies:
            sess.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-SG,en;q=0.9",
            "Content-Type": "application/json",
            "Referer": f"{_BOOKING_BASE}/",
            "Origin": _BOOKING_BASE,
            "x-scoot-appsource": "IBE-WEB",
        }

        for method, url in _TOKEN_ENDPOINTS:
            try:
                if method == "POST":
                    r = sess.post(
                        url,
                        json={"applicationName": "IBE-WEB"},
                        headers=headers,
                        timeout=10,
                    )
                else:
                    r = sess.get(url, headers=headers, timeout=10)

                if r.status_code == 200:
                    data = r.json()
                    token = ""
                    if isinstance(data, dict):
                        token = (
                            data.get("token", "")
                            or data.get("data", {}).get("token", "")
                            or data.get("accessToken", "")
                            or data.get("id_token", "")
                        )
                    elif isinstance(data, str) and data:
                        token = data
                    if token:
                        logger.info("Scoot: got JWT token from %s", url)
                        return token
            except Exception as e:
                logger.debug("Scoot: token from %s failed: %s", url, e)
                continue

        return ""

    @staticmethod
    def _build_passenger_types(req: FlightSearchRequest) -> list[dict]:
        """Build Navitaire passenger types array."""
        types = [{"type": "ADT", "count": req.adults or 1}]
        if req.children:
            types.append({"type": "CHD", "count": req.children})
        if req.infants:
            types.append({"type": "INF", "count": req.infants})
        return types

    # ------------------------------------------------------------------
    # Cookie + token farm — Playwright generates session
    # ------------------------------------------------------------------

    async def _ensure_session(
        self, req: FlightSearchRequest,
    ) -> tuple[str, list[dict]]:
        """Return valid farmed token + cookies, farming new ones if needed."""
        global _farmed_token, _farmed_cookies, _farm_timestamp
        lock = _get_farm_lock()
        async with lock:
            age = time.monotonic() - _farm_timestamp
            if _farmed_cookies and age < _COOKIE_MAX_AGE:
                return _farmed_token, _farmed_cookies
            return await self._farm_session(req)

    async def _farm_session(
        self, req: FlightSearchRequest,
    ) -> tuple[str, list[dict]]:
        """Open Playwright, visit booking site, extract JWT + Akamai cookies."""
        global _farmed_token, _farmed_cookies, _farm_timestamp

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

            token_found = {"value": ""}

            async def on_response(response):
                try:
                    url = response.url.lower()
                    ct = response.headers.get("content-type", "")
                    if response.status != 200 or "json" not in ct:
                        return
                    # Capture JWT token from token/auth endpoints
                    if "token" in url or "anonymous" in url or "auth" in url:
                        data = await response.json()
                        t = ""
                        if isinstance(data, dict):
                            t = (
                                data.get("token", "")
                                or data.get("data", {}).get("token", "")
                                or data.get("accessToken", "")
                                or data.get("id_token", "")
                            )
                        elif isinstance(data, str):
                            t = data
                        if t:
                            token_found["value"] = t
                            logger.info("Scoot: farmed JWT from %s", response.url[:80])
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("Scoot: farming session via booking.flyscoot.com")
            await page.goto(
                f"{_BOOKING_BASE}/",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await asyncio.sleep(5)

            # Dismiss cookie consent
            await self._dismiss_cookies(page)

            # Wait for SPA to load and fire token request
            await asyncio.sleep(5)

            # Extract cookies regardless of token capture
            cookies = await context.cookies()
            _farmed_cookies = cookies
            _farmed_token = token_found["value"]
            _farm_timestamp = time.monotonic()
            logger.info(
                "Scoot: farmed %d cookies, token=%s",
                len(cookies), "yes" if _farmed_token else "no",
            )
            return _farmed_token, cookies

        except Exception as e:
            logger.error("Scoot: session farm error: %s", e)
            return "", []
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Playwright fallback (full browser flow)
    # ------------------------------------------------------------------

    async def _playwright_fallback(
        self, req: FlightSearchRequest, t0: float,
    ) -> FlightSearchResponse:
        """Full Playwright browser with form fill + API interception."""
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

            captured_data: dict = {}
            api_event = asyncio.Event()
            search_clicked = {"ready": False}

            async def on_response(response):
                if not search_clicked["ready"]:
                    return
                try:
                    url = response.url.lower()
                    if response.status != 200:
                        return
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct:
                        return
                    if "lowfare" in url:
                        return
                    if any(k in url for k in [
                        "availability", "/api/flights/search",
                        "flightsearch", "search/flights", "/api/v1/availability",
                        "/api/nsk/", "trips", "air-bounds",
                    ]):
                        data = await response.json()
                        if data and isinstance(data, (dict, list)):
                            captured_data["search"] = data
                            captured_data["search_url"] = response.url
                            api_event.set()
                            logger.info(
                                "Scoot: captured search API from %s",
                                response.url[:100],
                            )
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info(
                "Scoot: Playwright fallback for %s→%s",
                req.origin, req.destination,
            )
            await page.goto(
                f"{_BOOKING_BASE}/",
                wait_until="domcontentloaded",
                timeout=30000,
            )
            await asyncio.sleep(5)
            await self._dismiss_cookies(page)

            # Wait for SPA to render search form
            spa_ready = False
            for _wait_round in range(6):  # up to ~30s total
                spa_ready = await page.evaluate("""() => {
                    const inputs = document.querySelectorAll('input#originStation');
                    return Array.from(inputs).some(i => i.offsetHeight > 0);
                }""")
                if spa_ready:
                    break
                await asyncio.sleep(5)
            if not spa_ready:
                logger.warning("Scoot: search form never appeared (Playwright fallback)")
                # Still update cookie farm from this session
                global _farmed_cookies, _farm_timestamp
                _farmed_cookies = await context.cookies()
                _farm_timestamp = time.monotonic()
                return self._empty(req)

            # Fill form
            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            ok = await self._fill_station(page, "#originStation", req.origin)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_station(page, "#destinationStation", req.destination)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_date(page, req.date_from)
            if not ok:
                return self._empty(req)
            await asyncio.sleep(0.3)

            # Submit search
            search_clicked["ready"] = True
            await self._click_search(page)

            # Wait for API response
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Scoot: Playwright fallback timed out")
                offers = await self._extract_from_dom(page, req)
                if offers:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)
                return self._empty(req)

            # Update cookie farm from successful session
            _farmed_cookies = await context.cookies()
            _farm_timestamp = time.monotonic()

            data = captured_data.get("search")
            if not data:
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_navitaire_response(data, req)
            if not offers:
                offers = self._parse_flat_response(data, req)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Scoot Playwright fallback error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Form interaction helpers (for Playwright fallback)
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        """Dismiss cookie consent and overlay banners."""
        for selector in [
            "text='Accept all cookies'",
            "button:has-text('Accept all cookies')",
            "button:has-text('Accept All')",
            "button:has-text('Accept')",
        ]:
            try:
                el = page.locator(selector).first
                if await el.count() > 0:
                    await el.click(timeout=3000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="onetrust"], [id*="onetrust"], [class*="modal-overlay"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        """Click One-Way radio/tab if not already selected."""
        try:
            one_way = page.locator("text='One-Way'").first
            if await one_way.count() > 0:
                await one_way.click(timeout=3000)
                await asyncio.sleep(0.5)
                return
        except Exception:
            pass
        for label in ["One Way", "ONE WAY", "One-way", "one way"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if await el.count() > 0:
                    await el.click(timeout=2000)
                    return
            except Exception:
                continue

    async def _fill_station(self, page, selector: str, iata: str) -> bool:
        """Fill an airport input (#originStation or #destinationStation).

        The Angular SPA duplicates elements (visible + hidden tabs).
        We use JS to find the visible input and interact with it directly.
        """
        try:
            is_origin = "origin" in selector.lower()
            field_id = "originStation" if is_origin else "destinationStation"

            # Use JS to click the visible input (avoids duplicate-ID ambiguity)
            clicked = await page.evaluate("""(fieldId) => {
                const inputs = document.querySelectorAll('input#' + fieldId);
                for (const inp of inputs) {
                    if (inp.offsetHeight > 0) {
                        inp.click();
                        inp.focus();
                        return true;
                    }
                }
                return false;
            }""", field_id)
            if not clicked:
                logger.debug("Scoot: no visible input for %s", field_id)
                return False
            await asyncio.sleep(0.5)

            # Clear via JS + Angular event dispatch
            await page.evaluate("""(fieldId) => {
                const inputs = document.querySelectorAll('input#' + fieldId);
                for (const inp of inputs) {
                    if (inp.offsetHeight > 0) {
                        inp.value = '';
                        inp.dispatchEvent(new Event('input', {bubbles: true}));
                        inp.dispatchEvent(new Event('change', {bubbles: true}));
                        return;
                    }
                }
            }""", field_id)
            await asyncio.sleep(0.2)

            # Type IATA code character by character (triggers Angular keypress)
            await page.keyboard.type(iata, delay=100)
            await asyncio.sleep(2.5)

            # Click station suggestion from overlay via JS
            suggestion_clicked = await page.evaluate("""(iata) => {
                const overlays = document.querySelectorAll('.stations-overlay');
                for (const overlay of overlays) {
                    if (overlay.offsetHeight === 0) continue;
                    const byLabel = overlay.querySelector('div[aria-label="' + iata + '"]');
                    if (byLabel && byLabel.offsetHeight > 0) {
                        byLabel.click();
                        return 'aria-label';
                    }
                    const codes = overlay.querySelectorAll('.code');
                    for (const code of codes) {
                        if (code.textContent.trim() === iata && code.offsetHeight > 0) {
                            code.parentElement.click();
                            return 'code-parent';
                        }
                    }
                    const items = overlay.querySelectorAll(
                        'div.current-location, div.station-item, li');
                    for (const item of items) {
                        if (item.textContent.includes(iata) && item.offsetHeight > 0) {
                            item.click();
                            return 'text-match';
                        }
                    }
                }
                return null;
            }""", iata)

            if suggestion_clicked:
                logger.info("Scoot: selected station %s via JS %s", iata, suggestion_clicked)
                await asyncio.sleep(0.5)
                return True

            # Fallback: Playwright selectors
            for sel in [
                f".stations-overlay div[aria-label='{iata}']",
                f".stations-overlay :text-is('{iata}')",
            ]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        logger.info("Scoot: selected station %s via %s", iata, sel)
                        return True
                except Exception:
                    continue

            # Last resort: press Enter for top suggestion
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.5)
            logger.info("Scoot: pressed Enter for station %s", iata)
            return True

        except Exception as e:
            logger.warning("Scoot: station fill error for %s: %s", iata, e)
            return False

    async def _fill_date(self, page, target: datetime) -> bool:
        """Fill the departure date using the ngb-datepicker calendar."""
        try:
            # Click the visible #departureDate to open the calendar
            opened = await page.evaluate("""() => {
                const els = document.querySelectorAll('#departureDate');
                for (const el of els) {
                    if (el.offsetHeight > 0) { el.click(); return true; }
                }
                return false;
            }""")
            if not opened:
                logger.warning("Scoot: no visible #departureDate to click")
                return False
            await asyncio.sleep(1)

            target_month_year = target.strftime("%B %Y")  # e.g. "April 2026"

            # Navigate calendar to the target month
            for _ in range(12):
                visible_months = await page.evaluate("""() => {
                    return Array.from(document.querySelectorAll('.ngb-dp-month-name'))
                        .filter(e => e.offsetHeight > 0)
                        .map(e => e.textContent.trim());
                }""")
                if target_month_year in visible_months:
                    break
                try:
                    await page.locator(
                        "button[aria-label='Next month']"
                    ).first.click(timeout=3000)
                    await asyncio.sleep(0.5)
                except Exception:
                    logger.warning("Scoot: can't find Next month button")
                    break

            # Build the aria-label for the target day
            day_label = target.strftime("%A, %B ") + str(target.day) + target.strftime(", %Y")

            # Click the day cell via JS
            clicked = await page.evaluate("""(label) => {
                const cells = document.querySelectorAll(
                    'div.ngb-dp-day[role="gridcell"]:not(.disabled)');
                for (const cell of cells) {
                    if (cell.offsetHeight > 0 &&
                        cell.getAttribute('aria-label') === label) {
                        cell.click();
                        return true;
                    }
                }
                return false;
            }""", day_label)

            if clicked:
                logger.info("Scoot: selected date %s", target.strftime("%Y-%m-%d"))
                await asyncio.sleep(0.5)
                await self._close_calendar(page)
                return True

            # Fallback: match by day number
            day_num = str(target.day)
            clicked2 = await page.evaluate("""(dayNum) => {
                const cells = document.querySelectorAll(
                    'div.ngb-dp-day[role="gridcell"]:not(.disabled)');
                for (const cell of cells) {
                    if (cell.offsetHeight > 0 &&
                        cell.textContent.trim() === dayNum) {
                        cell.click();
                        return true;
                    }
                }
                return false;
            }""", day_num)

            if clicked2:
                logger.info("Scoot: selected date %s via day-number fallback", target.strftime("%Y-%m-%d"))
                await asyncio.sleep(0.5)
                await self._close_calendar(page)
                return True

            logger.warning("Scoot: couldn't select date %s (label=%s)", target.strftime("%Y-%m-%d"), day_label)
            return False
        except Exception as e:
            logger.warning("Scoot: date fill error: %s", e)
            return False

    async def _close_calendar(self, page) -> None:
        """Click the 'Done' button to close the calendar picker."""
        try:
            done_clicked = await page.evaluate("""() => {
                const btns = document.querySelectorAll('button, div[role="button"]');
                for (const btn of btns) {
                    if (btn.offsetHeight > 0 && btn.textContent.trim() === 'Done') {
                        btn.click();
                        return true;
                    }
                }
                return false;
            }""")
            if done_clicked:
                logger.info("Scoot: closed calendar via Done button")
                await asyncio.sleep(0.5)
        except Exception:
            pass

    async def _click_search(self, page) -> None:
        """Click the 'Let's Go!' search button via JS."""
        clicked = await page.evaluate("""() => {
            const labels = ["Let's Go!", "Search flights", "Search", "SEARCH", "Find flights"];
            const btns = document.querySelectorAll('button, a[role="button"], div[role="button"]');
            for (const label of labels) {
                for (const btn of btns) {
                    if (btn.offsetHeight > 0 && btn.textContent.trim() === label) {
                        btn.click();
                        return label;
                    }
                }
            }
            // Fallback: submit button
            const submit = document.querySelector('button[type="submit"]');
            if (submit && submit.offsetHeight > 0) {
                submit.click();
                return 'submit';
            }
            return null;
        }""")
        if clicked:
            logger.info("Scoot: clicked search button '%s'", clicked)
        else:
            await page.keyboard.press("Enter")
            logger.info("Scoot: pressed Enter as search fallback")

    # ------------------------------------------------------------------
    # DOM extraction fallback
    # ------------------------------------------------------------------

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight offers from DOM elements on the results page."""
        try:
            await asyncio.sleep(3)
            offers_data = await page.evaluate("""() => {
                const results = [];
                const cardSelectors = [
                    '[class*="flight-card"]', '[class*="flight-row"]',
                    '[class*="journey-card"]', '[class*="flight-result"]',
                    '[class*="fare-card"]', '[class*="flight-select"]',
                    '[data-test*="flight"]', '[class*="availability-row"]',
                ];
                for (const sel of cardSelectors) {
                    const cards = document.querySelectorAll(sel);
                    if (cards.length > 0) {
                        cards.forEach(card => {
                            const text = card.innerText || '';
                            const priceMatch = text.match(/(?:SGD|USD|EUR|\\$)\\s*([\\d,]+\\.?\\d*)/i)
                                || text.match(/([\\d,]+\\.?\\d*)\\s*(?:SGD|USD|EUR)/i);
                            const timeMatch = text.match(/(\\d{1,2}:\\d{2})\\s*(?:am|pm)?/gi);
                            const flightMatch = text.match(/(?:TR|TZ|3K)\\s*\\d+/i);
                            if (priceMatch || timeMatch)
                                results.push({
                                    price: priceMatch ? parseFloat(priceMatch[1].replace(',', '')) : null,
                                    times: timeMatch || [],
                                    flightNo: flightMatch ? flightMatch[0] : null,
                                    fullText: text.substring(0, 300),
                                });
                        });
                        break;
                    }
                }
                const bundleScript = document.querySelector(
                    'script#bundle-data-v2, script#bundle-data');
                if (bundleScript) {
                    try { return { type: 'bundle', data: JSON.parse(bundleScript.textContent) }; }
                    catch {}
                }
                return { type: 'dom', cards: results };
            }""")

            if not offers_data:
                return []

            data_type = offers_data.get("type", "")
            if data_type == "bundle":
                return self._parse_navitaire_response(offers_data.get("data", {}), req)
            if data_type == "dom":
                return self._parse_dom_cards(offers_data.get("cards", []), req)
            return []
        except Exception as e:
            logger.debug("Scoot: DOM extraction error: %s", e)
            return []

    async def _extract_from_page_data(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight data from Angular component state or inline scripts."""
        try:
            data = await page.evaluate("""() => {
                const scripts = document.querySelectorAll('script');
                for (const s of scripts) {
                    const t = s.textContent || '';
                    const m = t.match(/FlightData\\s*=\\s*'([\\s\\S]*?)';/);
                    if (m) return { type: 'flightdata', raw: m[1] };
                    const m2 = t.match(/(?:availability|flightSearch)\\s*=\\s*({[\\s\\S]*?});/);
                    if (m2) return { type: 'json', raw: m2[1] };
                }
                if (window.FlightData) return { type: 'global', data: window.FlightData };
                if (window.AvailabilityV2) return { type: 'global', data: window.AvailabilityV2 };
                return null;
            }""")
            if not data:
                return []
            if data.get("type") == "flightdata":
                import html as html_mod
                raw = html_mod.unescape(data["raw"])
                return self._parse_navitaire_response(json.loads(raw), req)
            if data.get("type") == "json":
                return self._parse_navitaire_response(json.loads(data["raw"]), req)
            if data.get("type") == "global" and data.get("data"):
                return self._parse_navitaire_response(data["data"], req)
            return []
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Response parsing
    # ------------------------------------------------------------------

    def _parse_navitaire_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Scoot Navitaire availability API response.

        Structure: data.trips[].journeys[] with pricing in data.faresAvailable[].
        Each journey.fares[].fareAvailabilityKey maps to faresAvailable[].totals.fareTotal.
        """
        if not isinstance(data, dict):
            return []

        offers: list[FlightOffer] = []
        currency = data.get("currencyCode", "SGD")
        booking_url = self._build_booking_url(req)

        # Build fare price lookup: fareAvailabilityKey → lowest fareTotal
        fare_lookup: dict[str, float] = {}
        for fa in data.get("faresAvailable", []):
            key = fa.get("fareAvailabilityKey", "")
            totals = fa.get("totals", {})
            price = totals.get("fareTotal")
            if key and price is not None:
                fare_lookup[key] = float(price)

        trips = data.get("trips", [])
        for trip in trips:
            if not isinstance(trip, dict):
                continue
            for journey in trip.get("journeys", []):
                if not isinstance(journey, dict):
                    continue

                # Find cheapest fare for this journey
                best_price = float("inf")
                for fare in journey.get("fares", []):
                    fkey = fare.get("fareAvailabilityKey", "")
                    if fkey in fare_lookup:
                        p = fare_lookup[fkey]
                        if 0 < p < best_price:
                            best_price = p
                if best_price == float("inf"):
                    continue

                # Parse segments
                segments: list[FlightSegment] = []
                for seg in journey.get("segments", []):
                    ident = seg.get("identifier", {})
                    desig = seg.get("designator", {})
                    carrier = ident.get("carrierCode", "TR")
                    flight_num = str(ident.get("identifier", "")).strip()
                    flight_no = f"{carrier}{flight_num}" if flight_num else ""
                    segments.append(FlightSegment(
                        airline=carrier,
                        airline_name="Scoot",
                        flight_no=flight_no,
                        origin=desig.get("origin", req.origin),
                        destination=desig.get("destination", req.destination),
                        departure=self._parse_dt(desig.get("departure", "")),
                        arrival=self._parse_dt(desig.get("arrival", "")),
                        cabin_class="M",
                    ))

                if not segments:
                    # Fallback: use journey-level designator
                    desig = journey.get("designator", {})
                    if desig:
                        segments.append(FlightSegment(
                            airline="TR", airline_name="Scoot",
                            flight_no="",
                            origin=desig.get("origin", req.origin),
                            destination=desig.get("destination", req.destination),
                            departure=self._parse_dt(desig.get("departure", "")),
                            arrival=self._parse_dt(desig.get("arrival", "")),
                            cabin_class="M",
                        ))
                    if not segments:
                        continue

                total_dur = 0
                if segments[0].departure and segments[-1].arrival:
                    delta = segments[-1].arrival - segments[0].departure
                    total_dur = max(int(delta.total_seconds()), 0)

                route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=total_dur,
                    stopovers=max(len(segments) - 1, 0),
                )

                journey_key = journey.get("journeyKey", f"{req.origin}{req.destination}{time.monotonic()}")

                offers.append(FlightOffer(
                    id=f"tr_{hashlib.md5(str(journey_key).encode()).hexdigest()[:12]}",
                    price=round(best_price, 2),
                    currency=currency,
                    price_formatted=f"{best_price:.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=["Scoot"],
                    owner_airline="TR",
                    booking_url=booking_url,
                    is_locked=False,
                    source="scoot_direct",
                    source_tier="free",
                ))

        return offers

    def _parse_flat_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse flatter Navitaire response structures (journeys at top level)."""
        if not isinstance(data, dict):
            return []
        offers: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)
        currency = data.get("currencyCode", "SGD")

        # Try various flatter structures
        journeys = (
            data.get("journeys") or data.get("flights")
            or data.get("outboundFlights")
            or data.get("data", {}).get("journeys", [])
            or data.get("flightList", []) or []
        )
        if isinstance(journeys, dict):
            journeys = journeys.get("outbound", []) or list(journeys.values())
        if not isinstance(journeys, list):
            return []

        for flight in journeys:
            if not isinstance(flight, dict):
                continue
            offer = self._parse_single_flight(flight, currency, req, booking_url)
            if offer:
                offers.append(offer)

        return offers

    def _parse_single_flight(self, flight: dict, currency: str,
                             req: FlightSearchRequest,
                             booking_url: str) -> Optional[FlightOffer]:
        best_price = self._extract_best_price(flight)
        if best_price is None or best_price <= 0:
            return None

        segments = self._parse_segments(flight, req)
        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            delta = segments[-1].arrival - segments[0].departure
            total_dur = max(int(delta.total_seconds()), 0)

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = (
            flight.get("JourneySellKey") or flight.get("journeySellKey")
            or flight.get("journeyKey") or flight.get("id")
            or f"{req.origin}{req.destination}{time.monotonic()}"
        )

        return FlightOffer(
            id=f"tr_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route, inbound=None,
            airlines=["Scoot"], owner_airline="TR",
            booking_url=booking_url, is_locked=False,
            source="scoot_direct", source_tier="free",
        )

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        best = float("inf")
        # Navitaire Bundles (like Jetstar)
        bundles = flight.get("Bundles") or flight.get("bundles") or []
        for bundle in bundles:
            if isinstance(bundle, dict):
                for key in [
                    "RegularInclusiveAmount", "regularInclusiveAmount",
                    "CjInclusiveAmount", "cjInclusiveAmount",
                    "TotalAmount", "totalAmount",
                    "Amount", "amount", "Price", "price",
                ]:
                    val = bundle.get(key)
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        # Navitaire Fares
        fares = (flight.get("Fares") or flight.get("fares")
                 or flight.get("fareProducts") or [])
        for fare in fares:
            if isinstance(fare, dict):
                for key in ["price", "amount", "totalPrice", "basePrice",
                            "fareAmount", "totalAmount",
                            "PassengerFare", "passengerFare"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = (val.get("Amount") or val.get("amount")
                               or val.get("TotalAmount"))
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        # Direct price fields
        for key in ["price", "lowestFare", "totalPrice", "farePrice",
                     "amount", "lowestPrice", "Price", "LowestFare", "TotalPrice"]:
            p = flight.get(key)
            if p is not None:
                try:
                    v = (float(p) if not isinstance(p, dict)
                         else float(p.get("Amount") or p.get("amount") or 0))
                    if 0 < v < best:
                        best = v
                except (TypeError, ValueError):
                    pass
        return best if best < float("inf") else None

    def _parse_segments(self, flight: dict, req: FlightSearchRequest) -> list[FlightSegment]:
        segments: list[FlightSegment] = []
        segs_raw = (
            flight.get("Segments") or flight.get("segments")
            or flight.get("Legs") or flight.get("legs")
            or flight.get("flights") or []
        )
        if segs_raw and isinstance(segs_raw, list):
            for seg in segs_raw:
                if isinstance(seg, dict):
                    segments.append(self._build_segment(seg, req.origin, req.destination))
            if segments:
                return segments

        # Parse from JourneySellKey
        journey_key = flight.get("JourneySellKey") or flight.get("journeySellKey") or ""
        if journey_key and "~" in journey_key:
            seg = self._parse_journey_sell_key(journey_key, req)
            if seg:
                return [seg]

        # Build from flight-level fields
        dep_str = (flight.get("DepartureDateTime") or flight.get("departureDateTime")
                   or flight.get("departure") or flight.get("departureDate")
                   or flight.get("STD") or "")
        arr_str = (flight.get("ArrivalDateTime") or flight.get("arrivalDateTime")
                   or flight.get("arrival") or flight.get("arrivalDate")
                   or flight.get("STA") or "")
        flight_no_raw = (flight.get("FlightNumber") or flight.get("flightNumber")
                         or flight.get("FlightDesignator", {}).get("FlightNumber", "")
                         or "")
        carrier = (flight.get("CarrierCode") or flight.get("carrierCode")
                   or flight.get("FlightDesignator", {}).get("CarrierCode", "")
                   or "TR")
        origin = (flight.get("Origin") or flight.get("origin")
                  or flight.get("DepartureStation") or req.origin)
        dest = (flight.get("Destination") or flight.get("destination")
                or flight.get("ArrivalStation") or req.destination)

        flight_no = str(flight_no_raw).replace(" ", "")
        if flight_no and not flight_no.startswith(carrier):
            flight_no = f"{carrier}{flight_no}"

        segments.append(FlightSegment(
            airline=carrier, airline_name="Scoot",
            flight_no=flight_no, origin=origin, destination=dest,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        ))
        return segments

    @staticmethod
    def _parse_journey_sell_key(key: str, req: FlightSearchRequest) -> Optional[FlightSegment]:
        """Parse Navitaire JourneySellKey: TR~ 501~ ~~SIN~06/15/2026 06:00~BKK~..."""
        try:
            parts = key.split("~")
            if len(parts) < 7:
                return None
            carrier = parts[0].strip() or "TR"
            flight_no_raw = parts[1].strip()
            flight_no = f"{carrier}{flight_no_raw}" if flight_no_raw else ""
            origin = dest = ""
            dep_str = arr_str = ""
            for i, part in enumerate(parts):
                part = part.strip()
                if len(part) == 3 and part.isalpha() and part.isupper():
                    if not origin:
                        origin = part
                        if i + 1 < len(parts):
                            dep_str = parts[i + 1].strip()
                    elif not dest:
                        dest = part
                        if i + 1 < len(parts):
                            arr_str = parts[i + 1].strip()
            return FlightSegment(
                airline=carrier, airline_name="Scoot",
                flight_no=flight_no,
                origin=origin or req.origin,
                destination=dest or req.destination,
                departure=ScootConnectorClient._parse_dt(dep_str),
                arrival=ScootConnectorClient._parse_dt(arr_str),
                cabin_class="M",
            )
        except Exception:
            return None

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = (seg.get("DepartureDateTime") or seg.get("departureDateTime")
                   or seg.get("departure") or seg.get("STD") or seg.get("std") or "")
        arr_str = (seg.get("ArrivalDateTime") or seg.get("arrivalDateTime")
                   or seg.get("arrival") or seg.get("STA") or seg.get("sta") or "")
        flight_no_raw = str(
            seg.get("FlightNumber") or seg.get("flightNumber")
            or seg.get("FlightDesignator", {}).get("FlightNumber", "")
            or ""
        ).replace(" ", "")
        carrier = (seg.get("CarrierCode") or seg.get("carrierCode")
                   or seg.get("FlightDesignator", {}).get("CarrierCode", "") or "TR")
        origin = (seg.get("Origin") or seg.get("origin")
                  or seg.get("DepartureStation") or default_origin)
        dest = (seg.get("Destination") or seg.get("destination")
                or seg.get("ArrivalStation") or default_dest)

        flight_no = flight_no_raw
        if flight_no and not flight_no.startswith(carrier):
            flight_no = f"{carrier}{flight_no}"

        return FlightSegment(
            airline=carrier, airline_name="Scoot",
            flight_no=flight_no, origin=origin, destination=dest,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _parse_dom_cards(self, cards: list, req: FlightSearchRequest) -> list[FlightOffer]:
        offers = []
        booking_url = self._build_booking_url(req)
        for card in cards:
            price = card.get("price")
            if not price or price <= 0:
                continue
            flight_no = card.get("flightNo", "")
            times = card.get("times", [])
            dep_time = self._parse_dt(times[0]) if times else datetime(2000, 1, 1)
            arr_time = self._parse_dt(times[1]) if len(times) > 1 else datetime(2000, 1, 1)
            total_dur = 0
            if dep_time.year > 2000 and arr_time.year > 2000:
                total_dur = max(int((arr_time - dep_time).total_seconds()), 0)
            seg = FlightSegment(
                airline="TR", airline_name="Scoot",
                flight_no=flight_no or "", origin=req.origin,
                destination=req.destination,
                departure=dep_time, arrival=arr_time, cabin_class="M",
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=total_dur,
                                stopovers=0)
            offers.append(FlightOffer(
                id=f"tr_{hashlib.md5(f'{flight_no}{price}'.encode()).hexdigest()[:12]}",
                price=round(price, 2), currency="SGD",
                price_formatted=f"{price:.2f} SGD",
                outbound=route, inbound=None,
                airlines=["Scoot"], owner_airline="TR",
                booking_url=booking_url, is_locked=False,
                source="scoot_direct", source_tier="free",
            ))
        return offers

    # ── Utilities ───────────────────────────────────────────────────────────

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest,
                        elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Scoot %s->%s returned %d offers in %.1fs",
                    req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(
            f"scoot{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s).strip()
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in (
            "%m/%d/%Y %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M",
            "%H:%M",
        ):
            try:
                return datetime.strptime(s[:len(fmt) + 4], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://booking.flyscoot.com/"
            f"?origin={req.origin}&destination={req.destination}&depart={dep}"
            f"&pax={req.adults or 1}&type=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"scoot{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
