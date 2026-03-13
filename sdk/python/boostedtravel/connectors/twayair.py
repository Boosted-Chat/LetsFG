"""
T'way Air scraper — cookie-farm hybrid: curl_cffi + Playwright cookie farm.

T'way Air (IATA: TW) is a South Korean LCC operating domestic (GMP/ICN↔CJU/PUS)
and international flights to Japan, Taiwan, Vietnam, Thailand, Philippines, Guam.

Website: www.twayair.com — Java/Spring MVC + jQuery, protected by Akamai Bot Manager.

Strategy (2-tier hybrid, Mar 2026):
  Tier 1 — curl_cffi direct (~1-3s):
    GET homepage with impersonate="chrome124" → extract CSRF from HTML meta tag →
    POST /ajax/booking/getLowestFare with session cookies + CSRF.
    On subsequent calls, reuses cached cookies + CSRF (up to 20 min).
  Tier 2 — Playwright cookie farm + curl_cffi (~8-15s):
    Shared headed Chromium navigates to homepage → Akamai resolves naturally →
    extract cookies via context.cookies() + CSRF from page meta tags →
    curl_cffi POST /ajax/booking/getLowestFare with farmed cookies.
    Cookies cached for Tier 1 reuse.

Cookie refresh: Tier 1 populates cache on first call via homepage GET;
subsequent calls reuse. If stale or blocked, Tier 2 re-farms via Playwright.

Fare data format (pipe-delimited string per date key in OW dict):
  Field 0: Date (YYYYMMDD)
  Field 1: Departure airport (IATA)
  Field 2: Arrival airport (IATA)
  Field 3: Sold out (N/Y)
  Field 4: Business sold out (N/Y)
  Field 5: Operates (Y/N)
  Field 6: Business operates (Y/N)
  Field 7: Base fare (float, e.g. 7500.0)
  Field 8: Total fare incl. taxes (float, e.g. 19200.0)
  Field 9: Fare class name (e.g. SmartFare, NormalFare)
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
from typing import Optional

try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL = True
except ImportError:
    HAS_CURL = False

from boostedtravel.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from boostedtravel.connectors.browser import stealth_args

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 2
_IMPERSONATE = "chrome124"
_COOKIE_MAX_AGE = 20 * 60  # Reuse Akamai cookies for up to 20 minutes

# Domestic routes (within South Korea) use bookingType=DOM, currency=KRW
_DOMESTIC_AIRPORTS = {"GMP", "ICN", "CJU", "PUS", "TAE", "KWJ", "RSU", "USN", "MWX", "HIN", "WJU", "YNY", "KPO", "KUV"}

# Currency mapping by destination country
_COUNTRY_CURRENCY = {
    "JP": "JPY", "KR": "KRW", "TW": "TWD", "VN": "VND",
    "TH": "THB", "PH": "PHP", "SG": "SGD", "GU": "USD",
    "HK": "HKD", "MO": "MOP", "CN": "CNY",
}

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["ko-KR", "ko", "en-US", "en-GB"]
_TIMEZONES = ["Asia/Seoul", "Asia/Tokyo", "Asia/Taipei"]

# ── curl_cffi cookie cache (populated by homepage GET / Playwright farm) ───
_tw_cookies: dict | None = None
_tw_cookies_ts: float = 0
_tw_csrf_token: str = ""
_tw_csrf_header: str = "X-CSRF-TOKEN"

# ── Playwright cookie farm state ───────────────────────────────────────────
_farm_lock: Optional[asyncio.Lock] = None
_pw_instance = None
_browser = None


def _get_farm_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _get_browser():
    """Shared headed Chromium (launched once, reused across searches)."""
    global _pw_instance, _browser
    lock = _get_farm_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from boostedtravel.connectors.browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("TwayAir: browser launched")
        return _browser


def _extract_csrf_from_html(html: str) -> tuple[str, str]:
    """Extract CSRF token and header name from HTML meta tags.

    Returns (token, header_name) or ("", "X-CSRF-TOKEN") if not found.
    """
    token = ""
    header = "X-CSRF-TOKEN"
    m = re.search(r'<meta\s+name="_csrf"\s+content="([^"]*)"', html)
    if m:
        token = m.group(1)
    m2 = re.search(r'<meta\s+name="_csrf_header"\s+content="([^"]*)"', html)
    if m2:
        header = m2.group(1)
    # Also try input hidden field
    if not token:
        m3 = re.search(r'<input[^>]+name="_csrf"[^>]+value="([^"]*)"', html)
        if m3:
            token = m3.group(1)
    return token, header


class TwayAirConnectorClient:
    """T'way Air cookie-farm hybrid: curl_cffi + Playwright cookie farm."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search T'way Air via cookie-farm hybrid.

        Fast path (~1-3s): curl_cffi with cached/fresh cookies.
        Slow path (~8-15s): Playwright farms Akamai cookies, then curl_cffi.
        Fallback: Full Playwright in-browser XHR.
        """
        t0 = time.monotonic()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                offers = await self._attempt_search(req)
                if offers is not None:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)
                logger.warning("TwayAir: attempt %d/%d got no results", attempt, _MAX_ATTEMPTS)
            except Exception as e:
                logger.warning("TwayAir: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    async def _attempt_search(self, req: FlightSearchRequest) -> Optional[list[FlightOffer]]:
        # Tier 1: curl_cffi fast path (cached cookies or fresh homepage GET)
        result = await self._search_via_curl(req)
        if result is not None:
            logger.info("TwayAir: curl_cffi path succeeded")
            return result

        # Tier 2: Playwright cookie farm + curl_cffi
        logger.info("TwayAir: curl_cffi failed, trying Playwright cookie farm")
        cookies = await self._farm_cookies(req)
        if cookies:
            result = await self._search_via_curl(req)
            if result is not None:
                logger.info("TwayAir: Playwright farm + curl_cffi succeeded")
                return result

        # Tier 2b: Playwright in-browser XHR fallback
        logger.info("TwayAir: curl_cffi with farmed cookies failed, trying in-browser fallback")
        return await self._playwright_fallback(req)

    # ------------------------------------------------------------------
    # Tier 1: curl_cffi path (homepage GET → CSRF extraction → API POST)
    # ------------------------------------------------------------------

    async def _search_via_curl(self, req: FlightSearchRequest) -> Optional[list[FlightOffer]]:
        """Search via curl_cffi: reuse cached cookies or fetch homepage first."""
        if not HAS_CURL:
            return None

        global _tw_cookies, _tw_cookies_ts, _tw_csrf_token, _tw_csrf_header

        # If no cookies or stale, try to get them from homepage GET
        need_fresh = (
            not _tw_cookies
            or not _tw_csrf_token
            or (time.monotonic() - _tw_cookies_ts) > _COOKIE_MAX_AGE
        )
        if need_fresh:
            logger.info("TwayAir: fetching homepage via curl_cffi for cookies + CSRF")
            loop = asyncio.get_event_loop()
            ok = await loop.run_in_executor(None, self._fetch_homepage_sync)
            if not ok:
                return None

        if not _tw_cookies or not _tw_csrf_token:
            return None

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._search_api_sync, req,
            dict(_tw_cookies), _tw_csrf_token, _tw_csrf_header,
        )

    def _fetch_homepage_sync(self) -> bool:
        """GET homepage via curl_cffi, extract CSRF token and cookies."""
        global _tw_cookies, _tw_cookies_ts, _tw_csrf_token, _tw_csrf_header
        try:
            sess = cffi_requests.Session(impersonate=_IMPERSONATE)
            r = sess.get(
                "https://www.twayair.com/app/main",
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                },
                timeout=15,
            )
            if r.status_code != 200:
                logger.warning("TwayAir: homepage GET returned %d", r.status_code)
                return False

            html = r.text
            if not html or "denied" in html.lower()[:500]:
                logger.warning("TwayAir: homepage blocked by Akamai")
                return False

            token, header = _extract_csrf_from_html(html)
            if not token:
                logger.warning("TwayAir: no CSRF token found in homepage HTML")
                return False

            # Extract cookies from session
            cookie_dict = {c.name: c.value for c in sess.cookies}
            if cookie_dict:
                _tw_cookies = cookie_dict
                _tw_cookies_ts = time.monotonic()
                _tw_csrf_token = token
                _tw_csrf_header = header
                logger.info("TwayAir: cached %d cookies + CSRF from homepage GET", len(cookie_dict))
                return True

            logger.warning("TwayAir: no cookies from homepage GET")
            return False
        except Exception as e:
            logger.warning("TwayAir: homepage GET failed: %s", e)
            return False

    def _search_api_sync(
        self,
        req: FlightSearchRequest,
        cookies: dict,
        csrf_token: str,
        csrf_header: str,
    ) -> Optional[list[FlightOffer]]:
        """Synchronous curl_cffi POST to getLowestFare."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)

        for name, value in cookies.items():
            sess.cookies.set(name, value, domain="www.twayair.com")

        is_domestic = req.origin in _DOMESTIC_AIRPORTS and req.destination in _DOMESTIC_AIRPORTS
        booking_type = "DOM" if is_domestic else "INT"
        currency = self._determine_currency(req, is_domestic)

        form_data = {
            "tripType": "OW",
            "bookingType": booking_type,
            "currency": currency,
            "depAirport": req.origin,
            "arrAirport": req.destination,
            "baseDeptAirportCode": req.origin,
            "_csrf": csrf_token,
        }

        try:
            r = sess.post(
                "https://www.twayair.com/ajax/booking/getLowestFare",
                data=form_data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    csrf_header: csrf_token,
                    "Referer": "https://www.twayair.com/app/main",
                    "Origin": "https://www.twayair.com",
                },
                timeout=15,
            )
        except Exception as e:
            logger.warning("TwayAir [curl_cffi]: request failed: %s", e)
            return None

        if r.status_code != 200:
            logger.warning("TwayAir [curl_cffi]: HTTP %d", r.status_code)
            # Invalidate cache on auth failure so next attempt re-fetches
            if r.status_code in (403, 401):
                global _tw_cookies, _tw_cookies_ts
                _tw_cookies = None
                _tw_cookies_ts = 0
            return None

        text = r.text
        if not text:
            logger.warning("TwayAir [curl_cffi]: empty response body")
            return None

        logger.info("TwayAir [curl_cffi]: got %d bytes, parsing", len(text))
        return self._parse_fare_response(text, req, currency)

    # ------------------------------------------------------------------
    # Tier 2: Playwright cookie farm
    # ------------------------------------------------------------------

    async def _farm_cookies(self, req: FlightSearchRequest) -> list[dict]:
        """Open Playwright, navigate to T'way homepage, extract Akamai cookies."""
        global _tw_cookies, _tw_cookies_ts, _tw_csrf_token, _tw_csrf_header

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

            logger.info("TwayAir: farming cookies via Playwright")
            await page.goto(
                "https://www.twayair.com/app/main",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3.0)

            # Check for Akamai block
            title = await page.title()
            if "denied" in title.lower():
                logger.info("TwayAir: Akamai blocked first load, retrying...")
                await asyncio.sleep(5)
                await page.goto(
                    "https://www.twayair.com/app/main",
                    wait_until="domcontentloaded",
                    timeout=int(self.timeout * 1000),
                )
                await asyncio.sleep(5)
                title = await page.title()
                if "denied" in title.lower():
                    logger.warning("TwayAir: Akamai blocked after retry")
                    return []

            # Dismiss popups
            await self._dismiss_popups_pw(page)
            await asyncio.sleep(1.0)

            # Extract CSRF token from page
            csrf_info = await page.evaluate("""() => {
                const csrfMeta = document.querySelector('meta[name="_csrf"]');
                const headerMeta = document.querySelector('meta[name="_csrf_header"]');
                const csrfInput = document.querySelector('input[name="_csrf"]');
                return {
                    token: csrfMeta ? csrfMeta.getAttribute('content') : (csrfInput ? csrfInput.value : ''),
                    header: headerMeta ? headerMeta.getAttribute('content') : 'X-CSRF-TOKEN'
                };
            }""")

            csrf_token = csrf_info.get("token", "")
            csrf_header = csrf_info.get("header", "X-CSRF-TOKEN")

            if csrf_token:
                _tw_csrf_token = csrf_token
                _tw_csrf_header = csrf_header

            # Extract cookies
            all_cookies = await context.cookies()
            cookie_dict = {c["name"]: c["value"] for c in all_cookies if "twayair" in c.get("domain", "")}
            if cookie_dict:
                _tw_cookies = cookie_dict
                _tw_cookies_ts = time.monotonic()
                logger.info("TwayAir: farmed %d cookies + CSRF from Playwright", len(cookie_dict))
                return all_cookies

            logger.warning("TwayAir: no cookies from Playwright farm")
            return []

        except Exception as e:
            logger.error("TwayAir: cookie farm error: %s", e)
            return []
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Tier 2b: Playwright in-browser XHR fallback
    # ------------------------------------------------------------------

    async def _playwright_fallback(self, req: FlightSearchRequest) -> Optional[list[FlightOffer]]:
        """In-browser XHR via Playwright — last resort when curl_cffi fails."""
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

            await page.goto(
                "https://www.twayair.com/app/main",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3.0)

            title = await page.title()
            if "denied" in title.lower():
                logger.warning("TwayAir: Akamai blocked Playwright fallback")
                return None

            await self._dismiss_popups_pw(page)
            await asyncio.sleep(0.5)

            csrf_info = await page.evaluate("""() => {
                const csrfMeta = document.querySelector('meta[name="_csrf"]');
                const headerMeta = document.querySelector('meta[name="_csrf_header"]');
                const csrfInput = document.querySelector('input[name="_csrf"]');
                return {
                    token: csrfMeta ? csrfMeta.getAttribute('content') : (csrfInput ? csrfInput.value : ''),
                    header: headerMeta ? headerMeta.getAttribute('content') : 'X-CSRF-TOKEN'
                };
            }""")

            csrf_token = csrf_info.get("token", "")
            csrf_header = csrf_info.get("header", "X-CSRF-TOKEN")

            if not csrf_token:
                logger.warning("TwayAir [Playwright]: no CSRF token found")
                return None

            is_domestic = req.origin in _DOMESTIC_AIRPORTS and req.destination in _DOMESTIC_AIRPORTS
            booking_type = "DOM" if is_domestic else "INT"
            currency = self._determine_currency(req, is_domestic)

            body = (
                f"tripType=OW&bookingType={booking_type}&currency={currency}"
                f"&depAirport={req.origin}&arrAirport={req.destination}"
                f"&baseDeptAirportCode={req.origin}&_csrf={csrf_token}"
            )

            logger.info("TwayAir [Playwright]: calling getLowestFare (%s→%s, %s, %s)",
                        req.origin, req.destination, booking_type, currency)

            result = await page.evaluate("""(args) => {
                try {
                    const xhr = new XMLHttpRequest();
                    xhr.open('POST', '/ajax/booking/getLowestFare', false);
                    xhr.setRequestHeader('Content-Type', 'application/x-www-form-urlencoded');
                    xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
                    xhr.setRequestHeader(args.csrfHeader, args.csrfToken);
                    xhr.send(args.body);
                    return {status: xhr.status, text: xhr.responseText};
                } catch(e) {
                    return {error: e.message};
                }
            }""", {"body": body, "csrfHeader": csrf_header, "csrfToken": csrf_token})

            if not result or result.get("error"):
                err = result.get("error", "null") if result else "null"
                logger.warning("TwayAir [Playwright]: getLowestFare error: %s", err)
                return None

            status = result.get("status", 0)
            text = result.get("text", "")

            if status != 200 or not text:
                logger.warning("TwayAir [Playwright]: HTTP %d, body=%d bytes", status, len(text))
                return None

            # Cache cookies for curl_cffi reuse on next call
            global _tw_cookies, _tw_cookies_ts, _tw_csrf_token, _tw_csrf_header
            try:
                all_cookies = await context.cookies()
                cookie_dict = {c["name"]: c["value"] for c in all_cookies if "twayair" in c.get("domain", "")}
                if cookie_dict:
                    _tw_cookies = cookie_dict
                    _tw_cookies_ts = time.monotonic()
                    _tw_csrf_token = csrf_token
                    _tw_csrf_header = csrf_header
                    logger.info("TwayAir: cached %d cookies from Playwright fallback", len(cookie_dict))
            except Exception:
                pass

            return self._parse_fare_response(text, req, currency)

        except Exception as e:
            logger.warning("TwayAir [Playwright]: error: %s", e)
            return None
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _determine_currency(self, req: FlightSearchRequest, is_domestic: bool) -> str:
        if is_domestic:
            return "KRW"
        try:
            from boostedtravel.connectors.airline_routes import AIRPORT_COUNTRY
            dest_country = AIRPORT_COUNTRY.get(req.destination, "")
            origin_country = AIRPORT_COUNTRY.get(req.origin, "")
            if origin_country == "KR":
                return _COUNTRY_CURRENCY.get(dest_country, "KRW")
            if dest_country == "KR":
                return _COUNTRY_CURRENCY.get(origin_country, "KRW")
        except ImportError:
            pass
        return "KRW"

    def _parse_fare_response(self, text: str, req: FlightSearchRequest, currency: str) -> list[FlightOffer]:
        """Parse JSON response from /ajax/booking/getLowestFare.

        Response: {"routeSaleYnMap": {...}, "OW": {"YYYYMMDD": "pipe|delimited|fare"}}
        Pipe format: date|dep|arr|soldOut(N/Y)|bizSoldOut|operates(Y/N)|bizOperates|baseFare|totalFare|fareClass
        """
        try:
            data = json.loads(text)
        except (json.JSONDecodeError, TypeError):
            logger.warning("TwayAir: response is not valid JSON (%d bytes)", len(text))
            return []

        ow_data = data.get("OW", {})
        if not ow_data:
            logger.warning("TwayAir: empty OW dict in response")
            return []

        target_date_str = req.date_from.strftime("%Y%m%d")
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for date_key, fare_str in ow_data.items():
            if not fare_str or "|" not in str(fare_str):
                continue

            parts = str(fare_str).split("|")
            if len(parts) < 9:
                continue

            fare_date = parts[0]        # YYYYMMDD
            dep_airport = parts[1]      # IATA
            arr_airport = parts[2]      # IATA
            sold_out = parts[3].upper() == "Y"
            operates = parts[5].upper() == "Y"
            total_fare_str = parts[8]   # float string (e.g. 19200.0)
            fare_class = parts[9] if len(parts) > 9 else ""

            if sold_out or not operates:
                continue

            # Filter to requested date range
            if fare_date != target_date_str:
                if req.date_to:
                    date_to_str = req.date_to.strftime("%Y%m%d")
                    if not (target_date_str <= fare_date <= date_to_str):
                        continue
                else:
                    continue

            try:
                total_fare = float(total_fare_str)
            except (ValueError, TypeError):
                continue

            if total_fare <= 0:
                continue

            try:
                dep_dt = datetime.strptime(fare_date, "%Y%m%d")
            except ValueError:
                continue

            segment = FlightSegment(
                airline="TW",
                airline_name="T'way Air",
                flight_no=f"TW {dep_airport}{arr_airport}",
                origin=dep_airport,
                destination=arr_airport,
                departure=dep_dt,
                arrival=dep_dt,
                cabin_class="M",
            )

            route = FlightRoute(
                segments=[segment],
                total_duration_seconds=0,
                stopovers=0,
            )

            offer_key = f"{fare_date}_{dep_airport}_{arr_airport}_{fare_class}_{total_fare}"
            offers.append(FlightOffer(
                id=f"tw_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
                price=round(total_fare, 2),
                currency=currency,
                price_formatted=f"{total_fare:,.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["T'way Air"],
                owner_airline="TW",
                booking_url=booking_url,
                is_locked=False,
                source="twayair_direct",
                source_tier="free",
            ))

        logger.info("TwayAir: parsed %d offers from %d fare days", len(offers), len(ow_data))
        return offers

    async def _dismiss_popups_pw(self, page) -> None:
        """Dismiss cookie banners and popup layers (Playwright page)."""
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [class*="popup"], [class*="modal"], '
                    + '[class*="layer_popup"], [class*="dim"], [class*="consent"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

        for label in ["\ub2eb\uae30", "Close", "\ud655\uc778", "OK", "\ub3d9\uc758", "Accept"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    return
            except Exception:
                continue

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("TwayAir %s→%s returned %d offers in %.1fs",
                     req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"twayair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.twayair.com/app/booking/search?origin={req.origin}"
            f"&destination={req.destination}&departure={dep}&adults={req.adults}&tripType=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"twayair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
