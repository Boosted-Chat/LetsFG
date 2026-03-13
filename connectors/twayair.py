"""
T'way Air scraper — cookie-farm + curl_cffi hybrid.

T'way Air (IATA: TW) is a South Korean LCC operating domestic (GMP/ICN↔CJU/PUS)
and international flights to Japan, Taiwan, Vietnam, Thailand, Philippines, Guam.

Website: www.twayair.com — Java/Spring MVC + jQuery, protected by Akamai Bot Manager.

Strategy (2-tier hybrid, Mar 2026):
  Tier 1 — curl_cffi session (~1-3s):
    Create a curl_cffi session with impersonate="chrome124".
    GET /app/main to acquire Akamai sensor cookies + CSRF token, then
    POST /ajax/booking/getLowestFare with the session cookies.
    If cookies were cached from a previous session or browser farm, reuse them.
  Tier 2 — Playwright cookie-farm + curl_cffi (~10-20s):
    Launch headed Chrome via Playwright, navigate to www.twayair.com/app/main,
    let Akamai sensor script run, extract cookies via context.cookies().
    Cache cookies and CSRF token, then retry via curl_cffi.
  Fallback — Playwright in-browser XHR:
    If curl_cffi still fails after farming, execute XHR directly inside the
    Playwright page context (page.evaluate) as a last resort.

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

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 2
_IMPERSONATE = "chrome124"
_COOKIE_MAX_AGE = 20 * 60  # Reuse farmed cookies for up to 20 minutes

# Domestic routes (within South Korea) use bookingType=DOM, currency=KRW
_DOMESTIC_AIRPORTS = {"GMP", "ICN", "CJU", "PUS", "TAE", "KWJ", "RSU", "USN", "MWX", "HIN", "WJU", "YNY", "KPO", "KUV"}

# Currency mapping by destination country
_COUNTRY_CURRENCY = {
    "JP": "JPY", "KR": "KRW", "TW": "TWD", "VN": "VND",
    "TH": "THB", "PH": "PHP", "SG": "SGD", "GU": "USD",
    "HK": "HKD", "MO": "MOP", "CN": "CNY",
}

# ── Anti-fingerprint pools ────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["ko-KR", "en-US", "en-GB"]
_TIMEZONES = ["Asia/Seoul", "Asia/Tokyo", "UTC"]

# ── Shared cookie-farm state ──────────────────────────────────────────────
_farmed_cookies: list[dict] = []
_farmed_csrf: str = ""
_farmed_csrf_header: str = "X-CSRF-TOKEN"
_farm_timestamp: float = 0.0
_farm_lock: Optional[asyncio.Lock] = None
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
    lock = _get_farm_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from connectors.browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("TwayAir: browser launched for cookie farming")
        return _browser


class TwayAirConnectorClient:
    """T'way Air hybrid scraper — curl_cffi session + Playwright cookie-farm."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                offers = await self._attempt_search(req, t0)
                if offers is not None:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)
                logger.warning("TwayAir: attempt %d/%d got no results", attempt, _MAX_ATTEMPTS)
            except Exception as e:
                logger.warning("TwayAir: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    async def _attempt_search(self, req: FlightSearchRequest, t0: float) -> Optional[list[FlightOffer]]:
        # Tier 1: curl_cffi session (fast path with cached or session-acquired cookies)
        result = await self._search_via_curl_session(req)
        if result is not None:
            logger.info("TwayAir: curl_cffi session succeeded")
            return result

        # Tier 2: cookie-farm via Playwright, then curl_cffi
        logger.info("TwayAir: curl_cffi session failed, trying cookie-farm (tier 2)")
        cookies = await self._ensure_cookies(req)
        if cookies:
            result = await self._search_via_api(req)
            if result is not None:
                logger.info("TwayAir: curl_cffi with farmed cookies succeeded")
                return result

        # Fallback: in-browser XHR via Playwright
        logger.info("TwayAir: curl_cffi failed, trying Playwright in-browser fallback")
        return await self._search_via_browser(req)

    # ------------------------------------------------------------------
    # Tier 1: curl_cffi session (GET page + POST API in one session)
    # ------------------------------------------------------------------

    async def _search_via_curl_session(self, req: FlightSearchRequest) -> Optional[list[FlightOffer]]:
        """Create a curl_cffi session, GET the page for cookies+CSRF, POST the API."""
        if not HAS_CURL:
            return None

        # If we have fresh farmed cookies, use those directly
        global _farmed_cookies, _farmed_csrf, _farmed_csrf_header, _farm_timestamp
        if _farmed_cookies and _farmed_csrf and (time.monotonic() - _farm_timestamp) < _COOKIE_MAX_AGE:
            return await self._search_via_api(req)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._curl_session_sync, req)

    def _curl_session_sync(self, req: FlightSearchRequest) -> Optional[list[FlightOffer]]:
        """Synchronous curl_cffi session: GET page → extract CSRF → POST API."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)

        # Step 1: GET the main page to acquire Akamai cookies + CSRF token
        try:
            r = sess.get(
                "https://www.twayair.com/app/main",
                headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
                },
                timeout=15,
            )
        except Exception as e:
            logger.warning("TwayAir [curl_session]: GET main page failed: %s", e)
            return None

        if r.status_code != 200:
            logger.warning("TwayAir [curl_session]: GET main page HTTP %d", r.status_code)
            return None

        html = r.text
        if not html or "denied" in html.lower()[:500]:
            logger.warning("TwayAir [curl_session]: Akamai blocked page load")
            return None

        # Step 2: Extract CSRF token from <meta> tags
        csrf_token = ""
        csrf_header = "X-CSRF-TOKEN"

        csrf_match = re.search(r'<meta\s+name="_csrf"\s+content="([^"]+)"', html)
        if csrf_match:
            csrf_token = csrf_match.group(1)
        header_match = re.search(r'<meta\s+name="_csrf_header"\s+content="([^"]+)"', html)
        if header_match:
            csrf_header = header_match.group(1)

        if not csrf_token:
            # Also try input field
            input_match = re.search(r'<input[^>]+name="_csrf"[^>]+value="([^"]+)"', html)
            if input_match:
                csrf_token = input_match.group(1)

        if not csrf_token:
            logger.warning("TwayAir [curl_session]: no CSRF token found in page")
            return None

        # Step 3: POST to getLowestFare with session cookies
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
            logger.warning("TwayAir [curl_session]: POST failed: %s", e)
            return None

        if r.status_code != 200:
            logger.warning("TwayAir [curl_session]: POST HTTP %d", r.status_code)
            return None

        text = r.text
        if not text:
            logger.warning("TwayAir [curl_session]: empty response body")
            return None

        logger.info("TwayAir [curl_session]: got %d bytes, parsing", len(text))
        return self._parse_fare_response(text, req, currency)

    # ------------------------------------------------------------------
    # Tier 1 (cached path): curl_cffi with farmed cookies
    # ------------------------------------------------------------------

    async def _search_via_api(self, req: FlightSearchRequest) -> Optional[list[FlightOffer]]:
        """POST /ajax/booking/getLowestFare via curl_cffi with farmed cookies."""
        if not HAS_CURL:
            return None

        global _farmed_cookies, _farmed_csrf, _farmed_csrf_header
        if not _farmed_cookies or not _farmed_csrf:
            return None

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._api_search_sync, req,
            list(_farmed_cookies), _farmed_csrf, _farmed_csrf_header,
        )

    def _api_search_sync(
        self,
        req: FlightSearchRequest,
        cookies: list[dict],
        csrf_token: str,
        csrf_header: str,
    ) -> Optional[list[FlightOffer]]:
        """Synchronous curl_cffi POST to getLowestFare with farmed cookies."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)

        for c in cookies:
            domain = c.get("domain", "")
            sess.cookies.set(c["name"], c["value"], domain=domain)

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
            return None

        text = r.text
        if not text:
            logger.warning("TwayAir [curl_cffi]: empty response body")
            return None

        logger.info("TwayAir [curl_cffi]: got %d bytes, parsing", len(text))
        return self._parse_fare_response(text, req, currency)

    # ------------------------------------------------------------------
    # Tier 2: Playwright cookie-farm
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
        """Open Playwright, navigate to T'way Air, extract Akamai cookies + CSRF."""
        global _farmed_cookies, _farmed_csrf, _farmed_csrf_header, _farm_timestamp

        try:
            browser = await _get_browser()
        except Exception as e:
            logger.error("TwayAir: browser launch failed: %s", e)
            return []

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
                timeout=30000,
            )
            # Wait for Akamai sensor script to complete
            await asyncio.sleep(5)

            title = await page.title()
            if "denied" in title.lower():
                logger.info("TwayAir: Akamai blocked first load, retrying...")
                await asyncio.sleep(5)
                await page.goto(
                    "https://www.twayair.com/app/main",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                await asyncio.sleep(8)
                title = await page.title()
                if "denied" in title.lower():
                    logger.warning("TwayAir: Akamai blocked after retry")
                    return []

            # Dismiss popups
            await self._dismiss_popups_pw(page)

            # Extract CSRF token
            csrf_info = await page.evaluate("""() => {
                const csrfMeta = document.querySelector('meta[name="_csrf"]');
                const headerMeta = document.querySelector('meta[name="_csrf_header"]');
                const csrfInput = document.querySelector('input[name="_csrf"]');
                return {
                    token: csrfMeta ? csrfMeta.getAttribute('content') : (csrfInput ? csrfInput.value : ''),
                    header: headerMeta ? headerMeta.getAttribute('content') : 'X-CSRF-TOKEN'
                };
            }""")

            csrf_token = csrf_info.get("token", "") if isinstance(csrf_info, dict) else ""
            csrf_header = csrf_info.get("header", "X-CSRF-TOKEN") if isinstance(csrf_info, dict) else "X-CSRF-TOKEN"

            if csrf_token:
                _farmed_csrf = csrf_token
                _farmed_csrf_header = csrf_header

            # Extract cookies
            cookies = await context.cookies()
            if cookies:
                _farmed_cookies = cookies
                _farm_timestamp = time.monotonic()
                logger.info("TwayAir: farmed %d cookies + CSRF", len(cookies))
                return cookies

            logger.warning("TwayAir: cookie farm got no cookies")
            return []

        except Exception as e:
            logger.error("TwayAir: cookie farm error: %s", e)
            return []
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Fallback: Playwright in-browser XHR
    # ------------------------------------------------------------------

    async def _search_via_browser(self, req: FlightSearchRequest) -> Optional[list[FlightOffer]]:
        """Navigate Playwright page and execute XHR directly in page context."""
        try:
            browser = await _get_browser()
        except Exception as e:
            logger.error("TwayAir: browser launch failed: %s", e)
            return None

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
            await asyncio.sleep(5)

            title = await page.title()
            if "denied" in title.lower():
                logger.warning("TwayAir [browser]: Akamai blocked page")
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
                logger.warning("TwayAir [browser]: no CSRF token found")
                return None

            # Cache for curl_cffi reuse
            global _farmed_csrf, _farmed_csrf_header, _farmed_cookies, _farm_timestamp
            _farmed_csrf = csrf_token
            _farmed_csrf_header = csrf_header
            cookies = await context.cookies()
            if cookies:
                _farmed_cookies = cookies
                _farm_timestamp = time.monotonic()

            is_domestic = req.origin in _DOMESTIC_AIRPORTS and req.destination in _DOMESTIC_AIRPORTS
            booking_type = "DOM" if is_domestic else "INT"
            currency = self._determine_currency(req, is_domestic)

            body = (
                f"tripType=OW&bookingType={booking_type}&currency={currency}"
                f"&depAirport={req.origin}&arrAirport={req.destination}"
                f"&baseDeptAirportCode={req.origin}&_csrf={csrf_token}"
            )

            logger.info("TwayAir [browser]: calling getLowestFare (%s→%s, %s, %s)",
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
                logger.warning("TwayAir [browser]: getLowestFare error: %s", err)
                return None

            status = result.get("status", 0)
            text = result.get("text", "")

            if status != 200 or not text:
                logger.warning("TwayAir [browser]: HTTP %d, body=%d bytes", status, len(text))
                return None

            return self._parse_fare_response(text, req, currency)

        except Exception as e:
            logger.warning("TwayAir [browser]: error: %s", e)
            return None
        finally:
            await context.close()

    def _determine_currency(self, req: FlightSearchRequest, is_domestic: bool) -> str:
        if is_domestic:
            return "KRW"
        try:
            from connectors.airline_routes import AIRPORT_COUNTRY
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
