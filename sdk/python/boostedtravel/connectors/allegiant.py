"""
Allegiant Air hybrid scraper — warm headed Chrome + GraphQL interception.

Allegiant (IATA: G4) is a US ultra-low-cost carrier operating leisure routes
from smaller US cities to vacation destinations (Las Vegas, Florida, etc.).

IMPORTANT: Allegiant's website (allegiantair.com) is behind aggressive
Cloudflare protection that BLOCKS all non-US IP addresses. This scraper
**requires a US IP address** to function. Configure via the ALLEGIANT_PROXY
environment variable (e.g. "http://user:pass@us-proxy.example.com:10001").

Website: allegiantair.com — Next.js + Apollo GraphQL SPA.
Endpoint: POST /graphql with "flights" operation returning FlightOptionFragment
data with price, flight number, times, and seat availability.

Strategy (converted from cold-launch-per-search Playwright):
1. Launch headed Chrome ONCE with US proxy (Cloudflare requires headed browser)
2. For each search: navigate to booking URL, intercept the "flights" GQL response
3. Cloudflare cookies persist across navigations — no re-challenge needed
4. Parse FlightOptionFragment JSON → FlightOffer objects

First search ~7s (page load + GQL), subsequent searches ~4s (warm browser).
Old approach launched a new browser per search (~15-30s each).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Any, Optional

from boostedtravel.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from boostedtravel.connectors.browser import stealth_args

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_TIMEZONES = [
    "America/New_York", "America/Chicago", "America/Denver",
    "America/Los_Angeles", "America/Phoenix",
]

_MAX_ATTEMPTS = 3
_GQL_WAIT = 20  # seconds to wait for flights GQL per attempt (comes in 4-8s when it works)

# ── GraphQL query (captured from allegiantair.com booking page) ──────

_FLIGHTS_GQL = """query flights($flightSearchCriteria: FlightSearchCriteriaInput!) {
  transactionId
  flights(flightSearchCriteria: $flightSearchCriteria) {
    departing {
      ...FlightOptionFragment
      __typename
    }
    returning {
      ...FlightOptionFragment
      __typename
    }
    __typename
  }
}

fragment FlightOptionFragment on FlightOption {
  id
  flight {
    id
    number
    operatedBy {
      carrier
      flightNo
      __typename
    }
    origin {
      displayName
      code
      __typename
    }
    destination {
      code
      displayName
      __typename
    }
    departingTime
    arrivalTime
    isOvernight
    providerId
    __typename
  }
  strikethruPrice
  price
  baseFare
  availableSeatsCount
  discountType
  totalDiscountValue
  __typename
}"""

# ── Warm browser state (shared across searches) ─────────────────────

_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None
_warm_ctx = None
_ctx_lock: Optional[asyncio.Lock] = None
_ctx_ready = False


def _get_proxy() -> Optional[dict]:
    """Read proxy config from ALLEGIANT_PROXY env var."""
    raw = os.environ.get("ALLEGIANT_PROXY", "").strip()
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


def _get_ctx_lock() -> asyncio.Lock:
    global _ctx_lock
    if _ctx_lock is None:
        _ctx_lock = asyncio.Lock()
    return _ctx_lock


async def _get_browser(proxy: dict):
    """Shared headless Chrome with US proxy (launched once, reused)."""
    global _pw_instance, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright

        _pw_instance = await async_playwright().start()
        proxy_args = [f"--proxy-server={proxy['server']}"] if proxy.get("server") else []
        try:
            _browser = await _pw_instance.chromium.launch(
                headless=True,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled", *proxy_args, *stealth_args()],
            )
        except Exception:
            _browser = await _pw_instance.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox", *proxy_args, *stealth_args()],
            )
        logger.info("Allegiant: headless Chrome launched with proxy")
        return _browser


async def _wait_cloudflare(page, timeout: int = 40) -> bool:
    """Wait for Cloudflare Turnstile challenge to resolve."""
    for i in range(timeout):
        try:
            title = (await page.title()).lower()
            url = page.url.lower()
        except Exception:
            # Page navigating (Cloudflare redirect) — wait and retry
            await asyncio.sleep(1)
            continue
        if (
            "cloudflare" not in title
            and "attention" not in title
            and "moment" not in title
            and "blocked" not in title
            and "challenge" not in url
        ):
            if i > 0:
                logger.info("Allegiant: Cloudflare passed after ~%ds", i)
            return True
        await asyncio.sleep(1)
    return False


async def _ensure_warm_ctx(proxy: dict):
    """Ensure a warm browser context with Cloudflare cookies.

    Returns the context. Each search opens a fresh page in this context
    (inherits CF cookies but gets a clean Apollo Client — no SPA routing issues).
    """
    global _warm_ctx, _ctx_ready
    lock = _get_ctx_lock()
    async with lock:
        if _ctx_ready and _warm_ctx:
            return _warm_ctx

        browser = await _get_browser(proxy)
        if _warm_ctx:
            try:
                await _warm_ctx.close()
            except Exception:
                pass

        vp = random.choice(_VIEWPORTS)
        tz = random.choice(_TIMEZONES)

        _warm_ctx = await browser.new_context(
            proxy=proxy,
            viewport=vp,
            locale="en-US",
            timezone_id=tz,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            extra_http_headers={
                "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
            },
        )

        # Warm up: load homepage to establish Cloudflare cookies in context
        logger.info("Allegiant: warming up Cloudflare cookies...")
        page = await _warm_ctx.new_page()
        try:
            from playwright_stealth import stealth_async
            await stealth_async(page)
        except ImportError:
            pass

        await page.goto(
            "https://www.allegiantair.com/",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        cf_ok = await _wait_cloudflare(page, timeout=40)
        if not cf_ok:
            logger.error("Allegiant: Cloudflare blocked during warm-up")
            _ctx_ready = False
            await page.close()
            raise RuntimeError("Cloudflare blocked")

        try:
            await page.evaluate(
                "document.querySelector('#onetrust-accept-btn-handler')?.click()"
            )
        except Exception:
            pass
        await page.close()

        _ctx_ready = True
        logger.info("Allegiant: warm context ready (CF cookies established)")
        return _warm_ctx


class AllegiantConnectorClient:
    """Allegiant Air scraper — warm headed Chrome + navigate + GQL intercept.

    Requires a US IP address. Set ALLEGIANT_PROXY env var to a US proxy URL.
    Browser is launched once and reused across searches (~4s per search).
    """

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        proxy = _get_proxy()
        if not proxy:
            logger.error(
                "Allegiant: ALLEGIANT_PROXY env var not set. "
                "This scraper requires a US IP address. "
                "Set ALLEGIANT_PROXY=http://user:pass@us-proxy:port"
            )
            return self._empty(req)

        t0 = time.monotonic()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                result = await self._attempt_search(req, proxy, t0)
                if result is not None:
                    return result
            except Exception as e:
                logger.warning(
                    "Allegiant: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e
                )
                # Reset warm context on browser/connection errors
                if "closed" in str(e).lower() or "disconnected" in str(e).lower():
                    global _ctx_ready
                    _ctx_ready = False

        return self._empty(req)

    async def _attempt_search(
        self, req: FlightSearchRequest, proxy: dict, t0: float
    ) -> Optional[FlightSearchResponse]:
        """Single attempt: fresh page in warm context → navigate → intercept GQL."""
        ctx = await _ensure_warm_ctx(proxy)

        dep = req.date_from.strftime("%Y-%m-%d")
        booking_url = self._build_booking_url(req)

        # Fresh page per search — avoids Next.js SPA routing / Apollo cache issues
        page = await ctx.new_page()
        try:
            from playwright_stealth import stealth_async
            await stealth_async(page)
        except ImportError:
            pass

        # Set up GQL response interception
        captured: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                if "/graphql" not in response.url.lower():
                    return
                if response.status != 200:
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                body = await response.json()
                data = body.get("data", {}) if isinstance(body, dict) else {}
                if "flights" in data:
                    captured["flights"] = data["flights"]
                    api_event.set()
            except Exception:
                pass

        page.on("response", on_response)

        logger.info(
            "Allegiant: searching %s→%s on %s (fresh page)",
            req.origin, req.destination, dep,
        )

        try:
            await page.goto(
                booking_url,
                wait_until="load",
                timeout=int(self.timeout * 1000),
            )

            # Wait for Cloudflare if needed (usually passes immediately — cookies from warm context)
            cf_ok = await _wait_cloudflare(page, timeout=20)
            if not cf_ok:
                logger.warning("Allegiant: Cloudflare blocked")
                return None

            # Wait for flights GQL response (per-attempt cap; retry is cheaper than long waits)
            await asyncio.wait_for(api_event.wait(), timeout=_GQL_WAIT)

        except asyncio.TimeoutError:
            logger.warning("Allegiant: GQL flight response timed out")
            return None
        except Exception as e:
            logger.warning("Allegiant: navigation error: %s", e)
            return None
        finally:
            try:
                page.remove_listener("response", on_response)
            except Exception:
                pass
            try:
                await page.close()
            except Exception:
                pass

        flights_data = captured.get("flights")
        if flights_data is None:
            return None

        elapsed = time.monotonic() - t0
        offers = self._parse_flights(flights_data, req)
        return self._build_response(offers, req, elapsed)

    # ── GQL response parsing ─────────────────────────────────────────────

    def _parse_flights(
        self, flights_data: dict, req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Parse the 'flights' GQL response (FlightOptionFragment list)."""
        departing = flights_data.get("departing") or []
        if not isinstance(departing, list):
            departing = []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for opt in departing:
            if not isinstance(opt, dict):
                continue
            offer = self._parse_flight_option(opt, req, booking_url)
            if offer:
                offers.append(offer)

        return offers

    def _parse_flight_option(
        self, opt: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        """Parse a single FlightOptionFragment → FlightOffer."""
        price = opt.get("price")
        if price is None:
            return None
        try:
            price = float(price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        flight = opt.get("flight") or {}
        flight_no = str(flight.get("number") or "").strip()

        origin_data = flight.get("origin") or {}
        dest_data = flight.get("destination") or {}
        origin_code = origin_data.get("code") or req.origin
        dest_code = dest_data.get("code") or req.destination

        dep_dt = self._parse_dt(flight.get("departingTime"))
        arr_dt = self._parse_dt(flight.get("arrivalTime"))

        operated = flight.get("operatedBy") or {}
        carrier = operated.get("carrier") or "G4"
        if operated.get("flightNo"):
            flight_no = str(operated["flightNo"])

        seg = FlightSegment(
            airline=carrier,
            airline_name="Allegiant",
            flight_no=f"G4{flight_no}" if flight_no and not flight_no.startswith("G4") else flight_no,
            origin=origin_code,
            destination=dest_code,
            departure=dep_dt,
            arrival=arr_dt,
            cabin_class="M",
        )

        dur = 0
        if dep_dt.year > 2000 and arr_dt.year > 2000:
            dur = int((arr_dt - dep_dt).total_seconds())
            if dur < 0:
                dur += 86400  # overnight flight

        route = FlightRoute(segments=[seg], total_duration_seconds=max(dur, 0), stopovers=0)

        opt_id = opt.get("id") or f"{flight_no}_{dep_dt.isoformat()}"

        return FlightOffer(
            id=f"g4_{hashlib.md5(str(opt_id).encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency="USD",
            price_formatted=f"${price:.2f}",
            outbound=route,
            inbound=None,
            airlines=["Allegiant"],
            owner_airline="G4",
            booking_url=booking_url,
            is_locked=False,
            source="allegiant_direct",
            source_tier="free",
        )

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in (
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M",
        ):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.allegiantair.com/booking/flights"
            f"?tt=ONEWAY&o={req.origin}&d={req.destination}"
            f"&ta={req.adults}&tc=0&tis=0&til=0"
            f"&ds={dep}&de=&c=1&h=1"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"allegiant{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "Allegiant %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        h = hashlib.md5(
            f"allegiant{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=offers,
            total_results=len(offers),
        )
