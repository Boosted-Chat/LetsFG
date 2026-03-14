"""
T'way Air scraper — 2-tier hybrid: curl_cffi + CDP headed Chrome.

T'way Air (IATA: TW) is a South Korean LCC operating domestic (GMP/ICN↔CJU/PUS)
and international flights to Japan, Taiwan, Vietnam, Thailand, Philippines, Guam.

Website: www.twayair.com — Java/Spring MVC + jQuery, protected by Akamai Bot Manager.

Strategy (2-tier hybrid, discovered Mar 2026, fixed Mar 2026):
  Tier 1 — curl_cffi fast path (~0.5-1s):
    Reuses Akamai cookies + CSRF token cached from a prior browser session.
    POST /ajax/booking/getLowestFare with impersonate="chrome131".
    Skipped on first call (no cookies yet) or when cookies are stale (>5 min).
  Tier 2 — CDP headed Chrome (~5-15s):
    Persistent Chrome profile on debug port 9451.  Launched HEADED (no --headless)
    because Akamai Bot Manager detects headless Chrome.  Off-screen via
    --window-position=-2400,-2400, minimised via stealth_popen_kwargs().
    Navigates to homepage → Akamai challenge resolves → extract CSRF token
    → XHR to getLowestFare.  Cookies cached on success for Tier 1 reuse.

Cookie refresh: On first call CDP Chrome generates cookies; subsequent calls
reuse them via curl_cffi. If curl_cffi gets 403, falls through to CDP Chrome
which naturally refreshes the cache.

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
import os
import random
import re
import subprocess
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
from connectors.browser import stealth_popen_kwargs, find_chrome, _launched_procs

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 2
_IMPERSONATE = "chrome131"
_COOKIE_MAX_AGE = 5 * 60  # Reuse Akamai cookies for up to 5 minutes

# Domestic routes (within South Korea) use bookingType=DOM, currency=KRW
_DOMESTIC_AIRPORTS = {"GMP", "ICN", "CJU", "PUS", "TAE", "KWJ", "RSU", "USN", "MWX", "HIN", "WJU", "YNY", "KPO", "KUV"}

# Currency mapping by destination country
_COUNTRY_CURRENCY = {
    "JP": "JPY", "KR": "KRW", "TW": "TWD", "VN": "VND",
    "TH": "THB", "PH": "PHP", "SG": "SGD", "GU": "USD",
    "HK": "HKD", "MO": "MOP", "CN": "CNY",
}

# ── curl_cffi cookie cache (populated by CDP Chrome sessions) ─────────
_tw_cookies: dict | None = None
_tw_cookies_ts: float = 0
_tw_csrf_token: str = ""
_tw_csrf_header: str = "X-CSRF-TOKEN"

# CDP Chrome (primary browser tier — headed, no headless)
_DEBUG_PORT = 9451
_USER_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".twayair_chrome_data")
_pw_instance = None
_cdp_browser = None
_chrome_proc = None
_cdp_lock: Optional[asyncio.Lock] = None


def _get_cdp_lock() -> asyncio.Lock:
    global _cdp_lock
    if _cdp_lock is None:
        _cdp_lock = asyncio.Lock()
    return _cdp_lock


async def _get_browser():
    """Launch or connect to CDP Chrome.  Returns a Browser instance.

    Uses the default context (contexts[0]) so Akamai clearance cookies persist
    across calls. Same approach as the Scoot connector.
    """
    global _pw_instance, _cdp_browser, _chrome_proc
    lock = _get_cdp_lock()
    async with lock:
        if _cdp_browser:
            try:
                if _cdp_browser.is_connected():
                    return _cdp_browser
            except Exception:
                pass
            _cdp_browser = None

        from playwright.async_api import async_playwright

        # Try connecting to existing Chrome first
        pw = None
        try:
            pw = await async_playwright().start()
            _cdp_browser = await pw.chromium.connect_over_cdp(
                f"http://127.0.0.1:{_DEBUG_PORT}"
            )
            _pw_instance = pw
            logger.info("TwayAir: connected to existing Chrome on port %d", _DEBUG_PORT)
            return _cdp_browser
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

        # Launch Chrome HEADED (no --headless) — Akamai blocks headless Chrome.
        chrome_path = find_chrome()
        os.makedirs(_USER_DATA_DIR, exist_ok=True)
        _chrome_proc = subprocess.Popen(
            [
                chrome_path,
                f"--remote-debugging-port={_DEBUG_PORT}",
                f"--user-data-dir={_USER_DATA_DIR}",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-blink-features=AutomationControlled",
                "--disable-http2",
                "--window-position=-2400,-2400",
                "--window-size=1440,900",
                "about:blank",
            ],
            **stealth_popen_kwargs(),
        )
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.5)

        pw = await async_playwright().start()
        _pw_instance = pw
        _cdp_browser = await pw.chromium.connect_over_cdp(
            f"http://127.0.0.1:{_DEBUG_PORT}"
        )
        logger.info("TwayAir: Chrome launched headed on CDP port %d (pid %d)",
                     _DEBUG_PORT, _chrome_proc.pid)
        return _cdp_browser


class TwayAirConnectorClient:
    """T'way Air 2-tier hybrid: curl_cffi fast path → CDP headed Chrome."""

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
        # Strategy 1: curl_cffi fast path (reuse cached Akamai cookies)
        result = await self._search_via_api(req)
        if result is not None:
            logger.info("TwayAir: curl_cffi fast path succeeded")
            return result

        # Strategy 2: CDP headed Chrome (Akamai blocks headless — must be headed)
        logger.info("TwayAir: trying CDP headed Chrome (tier 2)")
        return await self._attempt_cdp(req)

    # ------------------------------------------------------------------
    # Tier 1: curl_cffi fast path (reuses Akamai cookies from browser)
    # ------------------------------------------------------------------

    async def _search_via_api(self, req: FlightSearchRequest) -> Optional[list[FlightOffer]]:
        """POST /ajax/booking/getLowestFare via curl_cffi with cached cookies.

        Returns parsed offers on success, None if cookies missing/stale or request fails.
        """
        if not HAS_CURL:
            return None

        global _tw_cookies, _tw_cookies_ts, _tw_csrf_token, _tw_csrf_header
        if not _tw_cookies or not _tw_csrf_token:
            return None
        if (time.monotonic() - _tw_cookies_ts) > _COOKIE_MAX_AGE:
            logger.info("TwayAir: cached cookies expired (>%ds)", _COOKIE_MAX_AGE)
            return None

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._search_via_api_sync, req,
            dict(_tw_cookies), _tw_csrf_token, _tw_csrf_header,
        )

    def _search_via_api_sync(
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
            return None

        text = r.text
        if not text:
            logger.warning("TwayAir [curl_cffi]: empty response body")
            return None

        logger.info("TwayAir [curl_cffi]: got %d bytes, parsing", len(text))
        return self._parse_fare_response(text, req, currency)

    # ------------------------------------------------------------------
    # Cookie caching helpers
    # ------------------------------------------------------------------

    @staticmethod
    async def _cache_cookies_from_cdp(context) -> None:
        """Extract cookies from a Playwright CDP context and cache them."""
        global _tw_cookies, _tw_cookies_ts
        try:
            all_cookies = await context.cookies()
            cookie_dict = {c["name"]: c["value"] for c in all_cookies if "twayair" in c.get("domain", "")}
            if cookie_dict:
                _tw_cookies = cookie_dict
                _tw_cookies_ts = time.monotonic()
                logger.info("TwayAir: cached %d cookies from CDP", len(cookie_dict))
        except Exception as e:
            logger.debug("TwayAir: cookie caching from CDP failed: %s", e)

    # ------------------------------------------------------------------
    # Tier 2: CDP headed Chrome (Akamai bypass)
    # ------------------------------------------------------------------

    async def _attempt_cdp(self, req: FlightSearchRequest) -> Optional[list[FlightOffer]]:
        browser = await _get_browser()

        # CDP browsers use default context — reuse to keep Akamai cookies warm
        is_cdp = hasattr(browser, 'contexts') and browser.contexts
        if is_cdp:
            context = browser.contexts[0]
            # Close extra tabs, reuse first page (avoids Akamai issues with new tabs)
            for p in context.pages[1:]:
                try:
                    await p.close()
                except Exception:
                    pass
            if context.pages:
                page = context.pages[0]
            else:
                page = await context.new_page()
        else:
            context = await browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="ko-KR",
                timezone_id="Asia/Seoul",
                service_workers="block",
            )
            page = await context.new_page()

        try:
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            await page.goto(
                "https://www.twayair.com/app/main",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            # Akamai sensor needs time to run — too short causes 403 on AJAX
            await asyncio.sleep(5.0)

            title = await page.title()
            if "denied" in title.lower():
                # Akamai interstitial — retry after waiting
                logger.info("TwayAir: Akamai challenge on first load, retrying...")
                await asyncio.sleep(5)
                await page.goto(
                    "https://www.twayair.com/app/main",
                    wait_until="domcontentloaded",
                    timeout=int(self.timeout * 1000),
                )
                await asyncio.sleep(8)
                title = await page.title()
                if "denied" in title.lower():
                    logger.warning("TwayAir [CDP]: Akamai blocked after retry (title=%r)", title)
                    return None
            # Dismiss popups
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

            # Cache CSRF for curl_cffi reuse
            if csrf_token:
                global _tw_csrf_token, _tw_csrf_header
                _tw_csrf_token = csrf_token
                _tw_csrf_header = csrf_header

            # Cache cookies for curl_cffi reuse
            await self._cache_cookies_from_cdp(context)

            is_domestic = req.origin in _DOMESTIC_AIRPORTS and req.destination in _DOMESTIC_AIRPORTS
            booking_type = "DOM" if is_domestic else "INT"
            currency = self._determine_currency(req, is_domestic)

            body = f"tripType=OW&bookingType={booking_type}&currency={currency}&depAirport={req.origin}&arrAirport={req.destination}&baseDeptAirportCode={req.origin}&_csrf={csrf_token}"

            logger.info("TwayAir [CDP]: calling getLowestFare (%s→%s, %s, %s)",
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
                logger.warning("TwayAir [CDP]: getLowestFare error: %s", err)
                return None

            status = result.get("status", 0)
            text = result.get("text", "")

            if status != 200 or not text:
                logger.warning("TwayAir [CDP]: HTTP %d, body=%d bytes", status, len(text))
                return None

            return self._parse_fare_response(text, req, currency)

        except Exception as e:
            logger.warning("TwayAir [CDP]: error: %s", e)
            return None

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
