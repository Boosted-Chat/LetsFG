"""
Jetstar hybrid scraper — curl_cffi direct + Playwright CDP fallback.

Jetstar (IATA: JQ) is an Australian low-cost airline in the Qantas Group,
operating domestic/international flights across Asia-Pacific.

Hybrid strategy (Mar 2026):
1. FAST PATH (~1-3s): curl_cffi with Chrome TLS impersonation fetches the
   booking page HTML directly. Kasada's TLS/JA3 fingerprint check is
   bypassed by curl_cffi's Chrome TLS stack. Extract bundle-data-v2 JSON
   from the server-rendered HTML response.
2. FALLBACK (~15-25s): Playwright CDP Chrome with persistent user-data-dir.
   Navigate to booking page, handle deeplink redirect, dismiss overlays,
   extract bundle-data-v2 or DOM-scrape flight cards.

Booking engine observations (Mar 2026):
- Navitaire-powered SSR booking at booking.jetstar.com
- Direct URL: /search-flights?origin1=SYD&destination1=MEL&departuredate1=2026-04-15&ADT=1
- Redirects through deeplinksv2 interim page → /select-flights with results
- bundle-data-v2 JSON: Trips[0].Flights[] with JourneySellKey, Bundles[]
- JourneySellKey format: "JQ~ 501~ ~~SYD~04/15/2026 06:00~MEL~04/15/2026 07:40~~"
- Bundles[].RegularInclusiveAmount = regular price, CjInclusiveAmount = member price
- Kasada challenge blocks Playwright Chromium; curl_cffi bypasses TLS check
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

from curl_cffi import requests as cffi_requests

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import stealth_args, stealth_position_arg, stealth_popen_kwargs

logger = logging.getLogger(__name__)

# ── curl_cffi constants ──────────────────────────────────────────────────
_IMPERSONATE = "chrome124"
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-AU", "en-NZ", "en-GB", "en-US", "en-SG"]
_TIMEZONES = [
    "Australia/Sydney", "Australia/Melbourne", "Australia/Brisbane",
    "Pacific/Auckland", "Asia/Singapore",
]

_MAX_ATTEMPTS = 3
_DEBUG_PORT = 9444
_USER_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".jetstar_chrome_data")

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch real Chrome via CDP, or fall back to Playwright headed.

    Uses a persistent user-data-dir so Kasada clearance persists across runs.
    """
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
            logger.info("Jetstar: Chrome ready via CDP (port %d)", _DEBUG_PORT)
            return _browser
        except Exception as e:
            logger.warning("Jetstar: CDP failed: %s, falling back to Playwright", e)

        from connectors.browser import launch_headed_browser
        _browser = await launch_headed_browser()
        logger.info("Jetstar: Playwright browser launched (fallback)")
        return _browser


class JetstarConnectorClient:
    """Jetstar hybrid scraper — curl_cffi direct + Playwright CDP fallback."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        # ── Fast path: curl_cffi direct HTTP (bypasses Kasada TLS check) ──
        try:
            offers = await self._curl_search(req)
            if offers:
                elapsed = time.monotonic() - t0
                return self._build_response(offers, req, elapsed, source="curl_cffi")
        except Exception as e:
            logger.warning("Jetstar: curl_cffi fast path failed: %s", e)

        # ── Slow path: Playwright CDP (original approach) ─────────────────
        adults = getattr(req, "adults", 1) or 1
        dep = req.date_from.strftime("%Y-%m-%d")
        search_url = (
            f"https://booking.jetstar.com/au/en/booking/search-flights"
            f"?origin1={req.origin}&destination1={req.destination}"
            f"&departuredate1={dep}&ADT={adults}"
        )

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            try:
                offers = await self._attempt_search(search_url, req)
                if offers is not None:
                    elapsed = time.monotonic() - t0
                    return self._build_response(offers, req, elapsed)
                logger.warning(
                    "Jetstar: attempt %d/%d got no results or blocked",
                    attempt, _MAX_ATTEMPTS,
                )
            except Exception as e:
                logger.warning("Jetstar: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)

        return self._empty(req)

    # ------------------------------------------------------------------
    # curl_cffi fast path
    # ------------------------------------------------------------------

    async def _curl_search(
        self, req: FlightSearchRequest,
    ) -> Optional[list[FlightOffer]]:
        """Fetch booking page HTML via curl_cffi and extract bundle-data-v2."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._curl_search_sync, req)

    def _curl_search_sync(
        self, req: FlightSearchRequest,
    ) -> Optional[list[FlightOffer]]:
        """Synchronous curl_cffi search — fetch HTML, extract flight data."""
        sess = cffi_requests.Session(impersonate=_IMPERSONATE)

        url = self._build_booking_url(req)
        headers = {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": "en-AU,en;q=0.9,en-US;q=0.8",
            "User-Agent": _UA,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "no-cache",
        }

        try:
            r = sess.get(url, headers=headers, timeout=25, allow_redirects=True)
        except Exception as e:
            logger.warning("Jetstar curl_cffi: request error: %s", e)
            return None

        if r.status_code != 200:
            logger.warning("Jetstar curl_cffi: HTTP %d", r.status_code)
            return None

        html = r.text

        # Detect Kasada/bot challenge page (small HTML with challenge scripts)
        if len(html) < 5000 and any(
            k in html.lower() for k in ("kasada", "/ips-d/", "challenge", "blocked")
        ):
            logger.info("Jetstar curl_cffi: Kasada challenge detected, skipping")
            return None

        # Strategy 1: Extract bundle-data-v2 from the final page
        offers = self._extract_offers_from_html(html, req)
        if offers:
            return offers

        # Strategy 2: If on deeplinksv2 interim page, follow the redirect link
        final_url = str(r.url)
        if "deeplink" in final_url.lower() or "continue" in html.lower():
            redirect_url = self._extract_deeplink_url(html, final_url)
            if redirect_url:
                try:
                    r2 = sess.get(
                        redirect_url, headers=headers,
                        timeout=20, allow_redirects=True,
                    )
                    if r2.status_code == 200:
                        offers = self._extract_offers_from_html(r2.text, req)
                        if offers:
                            return offers
                except Exception as e:
                    logger.debug("Jetstar curl_cffi: deeplink follow failed: %s", e)

        return None

    def _extract_offers_from_html(
        self, html: str, req: FlightSearchRequest,
    ) -> Optional[list[FlightOffer]]:
        """Extract bundle-data-v2 JSON from server-rendered HTML."""
        # <script id="bundle-data-v2" type="application/json">{...}</script>
        m = re.search(
            r'<script\s+id=["\']bundle-data(?:-v2)?["\']\s+'
            r'type=["\']application/json["\']>\s*(.*?)\s*</script>',
            html, re.DOTALL,
        )
        if not m:
            # Try without type attribute
            m = re.search(
                r'<script\s+id=["\']bundle-data(?:-v2)?["\']>\s*(.*?)\s*</script>',
                html, re.DOTALL,
            )
        if not m:
            logger.debug(
                "Jetstar curl_cffi: no bundle-data in HTML (%d bytes)", len(html),
            )
            return None

        try:
            data = json.loads(m.group(1))
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug("Jetstar curl_cffi: bundle-data parse error: %s", e)
            return None

        offers = self._parse_bundle_data_v2(data, req)
        if offers:
            logger.info(
                "Jetstar curl_cffi: extracted %d offers from HTML bundle-data",
                len(offers),
            )
        return offers or None

    @staticmethod
    def _extract_deeplink_url(html: str, base_url: str) -> Optional[str]:
        """Extract redirect URL from deeplinksv2 interim page."""
        from urllib.parse import urljoin

        # Link to select-flights page
        m = re.search(
            r'href=["\']([^"\']*select-flights[^"\']*)["\']', html,
        )
        if m:
            return urljoin(base_url, m.group(1))

        # Form action URL
        m = re.search(r'action=["\']([^"\']+)["\']', html)
        if m:
            target = m.group(1)
            if "booking" in target.lower() or "select" in target.lower():
                return urljoin(base_url, target)

        # Meta refresh redirect
        m = re.search(
            r'content=["\'][^"\']*URL=([^"\';\\s]+)', html, re.IGNORECASE,
        )
        if m:
            return urljoin(base_url, m.group(1))

        return None

    # ------------------------------------------------------------------
    # Playwright fallback (original approach)
    # ------------------------------------------------------------------

    async def _attempt_search(
        self, url: str, req: FlightSearchRequest
    ) -> Optional[list[FlightOffer]]:
        browser = await _get_browser()

        # CDP browsers use default context — don't call new_context()
        is_cdp = hasattr(browser, 'contexts') and browser.contexts
        if is_cdp:
            context = browser.contexts[0]
            page = await context.new_page()
        else:
            context = await browser.new_context(
                viewport=random.choice(_VIEWPORTS),
                locale=random.choice(_LOCALES),
                timezone_id=random.choice(_TIMEZONES),
                service_workers="block",
            )
            page = await context.new_page()

        try:
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            logger.info("Jetstar: loading %s", url[:120])
            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(4)

            # Handle deeplink redirect page ("Continue to booking" button)
            await self._handle_deeplink_redirect(page)

            title = await page.title()
            if "not found" in title.lower() or "error" in title.lower():
                logger.warning("Jetstar: got error page: %s", title)
                return None

            # Dismiss privacy notices and "Multiple booking" overlay
            await self._dismiss_overlays(page)

            # Wait for flight results (bundle-data-v2 script or flight cards)
            try:
                await page.wait_for_selector(
                    "script#bundle-data-v2, [class*='flight-row'], "
                    "div[aria-label*='Departure'], div[aria-label*='price']",
                    timeout=20000,
                )
            except Exception:
                pass
            await asyncio.sleep(2)

            # Strategy 1: Extract bundle-data-v2 JSON (full structured flight data)
            flight_data = await self._extract_bundle_data_v2(page)
            if flight_data:
                offers = self._parse_bundle_data_v2(flight_data, req)
                if offers:
                    logger.info("Jetstar: extracted %d offers from bundle-data-v2", len(offers))
                    return offers

            # Strategy 2: Try Navitaire FlightData JSON from inline scripts
            flight_data = await self._extract_flight_data(page)
            if flight_data:
                offers = self._parse_navitaire_data(flight_data, req)
                if offers:
                    return offers

            # Strategy 3: DOM extraction from flight cards
            offers = await self._extract_from_dom(page, req)
            if offers:
                return offers

            return None
        finally:
            await page.close()
            if not is_cdp:
                await context.close()

    async def _handle_deeplink_redirect(self, page) -> None:
        """Handle the deeplinksv2 interim page that shows 'Continue to booking'."""
        try:
            current_url = page.url
            if "deeplink" in current_url.lower() or "continue" in (await page.title()).lower():
                # Click "Continue to booking" or similar button
                for selector in [
                    "button:has-text('Continue')",
                    "a:has-text('Continue to booking')",
                    "a:has-text('Continue')",
                    "button[type='submit']",
                ]:
                    try:
                        btn = page.locator(selector).first
                        if await btn.count() > 0 and await btn.is_visible():
                            await btn.click(timeout=5000)
                            await page.wait_for_load_state("domcontentloaded", timeout=15000)
                            await asyncio.sleep(3)
                            logger.info("Jetstar: clicked through deeplink redirect")
                            return
                    except Exception:
                        continue
        except Exception:
            pass

    async def _dismiss_overlays(self, page) -> None:
        """Dismiss privacy notice, cookie banners, and 'Multiple booking' overlay."""
        # Click close buttons on banners and overlays
        for selector in [
            "img[alt*='close']",
            "[class*='privacy'] img[role='button']",
            "[class*='notice'] button",
            "[class*='banner'] button",
            "button:has-text('OK')",
            "button:has-text('Got it')",
            "button:has-text('No thanks')",
            "button:has-text('Continue')",
            "[class*='modal'] button[class*='close']",
            "[class*='multiple-booking'] button",
        ]:
            try:
                el = page.locator(selector).first
                if await el.count() > 0 and await el.is_visible():
                    await el.click(timeout=2000)
                    await asyncio.sleep(0.3)
            except Exception:
                continue

        # JS-remove overlays that intercept pointer events
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="gdpr"], [class*="consent"], [class*="cookie"], [class*="onetrust"], ' +
                    '[class*="privacy-notice"], [class*="modal-overlay"], [class*="popup"], ' +
                    '[class*="multiple-booking"], [class*="overlay-backdrop"]'
                ).forEach(el => { if (el.offsetHeight > 0 && el.offsetHeight < 400) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _extract_bundle_data_v2(self, page) -> Optional[dict]:
        """Extract bundle-data-v2 JSON — full structured flight data from Navitaire.

        The Jetstar select-flights page embeds a ~327KB JSON blob in:
        <script id="bundle-data-v2" type="application/json">{...}</script>

        Structure: { Trips: [{ Flights: [...] }], ... }
        """
        raw_json = await page.evaluate(r"""() => {
            const el = document.querySelector('script#bundle-data-v2');
            if (el) return el.textContent;
            // Fallback: look for bundle-data (v1)
            const el2 = document.querySelector('script#bundle-data');
            if (el2) return el2.textContent;
            return null;
        }""")
        if not raw_json:
            return None
        try:
            data = json.loads(raw_json)
            logger.debug("Jetstar: bundle-data-v2 extracted (%d bytes)", len(raw_json))
            return data
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug("Jetstar: bundle-data-v2 parse error: %s", e)
            return None

    async def _extract_flight_data(self, page) -> Optional[dict]:
        """Fallback: try to extract Navitaire FlightData JSON from inline <script> tags."""
        import html as html_mod

        raw = await page.evaluate(r"""() => {
            const scripts = document.querySelectorAll('script');
            for (const s of scripts) {
                const t = s.textContent || '';
                const m = t.match(/FlightData\s*=\s*'([\s\S]*?)';/);
                if (m) return { type: 'flightdata', data: m[1] };
                const m2 = t.match(/(?:availability|AvailabilityV2|flightSearch)\s*=\s*({[\s\S]*?});/);
                if (m2) return { type: 'json', data: m2[1] };
            }
            if (window.FlightData) return { type: 'global', data: JSON.stringify(window.FlightData) };
            if (window.AvailabilityV2) return { type: 'global', data: JSON.stringify(window.AvailabilityV2) };
            return null;
        }""")
        if not raw:
            return None

        data_str = raw.get("data", "")
        if raw.get("type") == "flightdata":
            data_str = html_mod.unescape(data_str)

        try:
            return json.loads(data_str)
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug("Jetstar: FlightData parse error: %s", e)
            return None

    def _parse_bundle_data_v2(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse bundle-data-v2 JSON from Jetstar's Navitaire booking engine.

        Structure:
          { Trips: [{ Flights: [{ JourneySellKey, Bundles, Segments, ... }] }] }

        JourneySellKey format:
          "JQ~ 501~ ~~SYD~04/15/2026 06:00~MEL~04/15/2026 07:40~~"

        Bundles[].RegularInclusiveAmount = regular price
        Bundles[].CjInclusiveAmount = Club Jetstar (member) price
        """
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        trips = data.get("Trips") or data.get("trips") or []
        for trip in trips:
            flights = trip.get("Flights") or trip.get("flights") or []
            for flight in flights:
                offer = self._parse_bundle_flight(flight, req, booking_url)
                if offer:
                    offers.append(offer)

        return offers

    def _parse_bundle_flight(
        self, flight: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        """Parse a single flight from bundle-data-v2 Trips[].Flights[]."""
        # Extract price from Bundles — prefer RegularInclusiveAmount (non-member)
        price = self._extract_bundle_price(flight)
        if price is None or price <= 0:
            return None

        # Parse JourneySellKey for flight number, airports, times
        sell_key = flight.get("JourneySellKey") or flight.get("journeySellKey") or ""
        key_info = self._parse_journey_sell_key(sell_key)

        # Build segments from Segments array or from sell key
        segments_raw = flight.get("Segments") or flight.get("segments") or []
        segments: list[FlightSegment] = []

        if segments_raw:
            for seg_raw in segments_raw:
                legs = seg_raw.get("Legs") or seg_raw.get("legs") or [seg_raw]
                for leg in legs:
                    seg = self._build_bundle_segment(leg, req)
                    if seg:
                        segments.append(seg)

        # Fallback: build segment from sell key info
        if not segments and key_info:
            seg = FlightSegment(
                airline=key_info.get("carrier", "JQ"),
                airline_name="Jetstar",
                flight_no=key_info.get("flight_no", ""),
                origin=key_info.get("origin", req.origin),
                destination=key_info.get("destination", req.destination),
                departure=key_info.get("departure", datetime(2000, 1, 1)),
                arrival=key_info.get("arrival", datetime(2000, 1, 1)),
                cabin_class="M",
            )
            segments.append(seg)

        if not segments:
            return None

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        flight_key = sell_key or f"{req.origin}_{req.destination}_{time.monotonic()}"
        return FlightOffer(
            id=f"jq_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
            price=round(price, 2), currency="AUD",
            price_formatted=f"${price:.2f} AUD",
            outbound=route, inbound=None,
            airlines=["Jetstar"], owner_airline="JQ",
            booking_url=booking_url, is_locked=False,
            source="jetstar_direct", source_tier="free",
        )

    @staticmethod
    def _extract_bundle_price(flight: dict) -> Optional[float]:
        """Extract best price from Bundles[].RegularInclusiveAmount."""
        bundles = flight.get("Bundles") or flight.get("bundles") or []
        best = float("inf")
        for bundle in bundles:
            # Regular (non-member) price
            for key in ["RegularInclusiveAmount", "regularInclusiveAmount",
                        "InclusiveAmount", "inclusiveAmount",
                        "CjInclusiveAmount", "cjInclusiveAmount"]:
                val = bundle.get(key)
                if val is not None:
                    try:
                        v = float(val)
                        if 0 < v < best:
                            best = v
                    except (TypeError, ValueError):
                        pass
            # Nested price object
            price_obj = bundle.get("Price") or bundle.get("price") or {}
            if isinstance(price_obj, dict):
                for key in ["Amount", "amount", "Value", "value"]:
                    val = price_obj.get(key)
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        return best if best < float("inf") else None

    @staticmethod
    def _parse_journey_sell_key(sell_key: str) -> Optional[dict]:
        """Parse JourneySellKey: 'JQ~ 501~ ~~SYD~04/15/2026 06:00~MEL~04/15/2026 07:40~~'"""
        if not sell_key or "~" not in sell_key:
            return None
        parts = sell_key.split("~")
        # parts[0] = carrier (JQ), parts[1] = flight number (space + 501)
        # parts[4] = origin (SYD), parts[5] = dep datetime (04/15/2026 06:00)
        # parts[6] = destination (MEL), parts[7] = arr datetime (04/15/2026 07:40)
        try:
            carrier = parts[0].strip() if len(parts) > 0 else "JQ"
            flight_no = parts[1].strip() if len(parts) > 1 else ""
            origin = parts[4].strip() if len(parts) > 4 else ""
            dep_str = parts[5].strip() if len(parts) > 5 else ""
            destination = parts[6].strip() if len(parts) > 6 else ""
            arr_str = parts[7].strip() if len(parts) > 7 else ""

            dep_dt = datetime.strptime(dep_str, "%m/%d/%Y %H:%M") if dep_str else datetime(2000, 1, 1)
            arr_dt = datetime.strptime(arr_str, "%m/%d/%Y %H:%M") if arr_str else datetime(2000, 1, 1)

            full_flight_no = f"{carrier}{flight_no}" if flight_no else ""

            return {
                "carrier": carrier,
                "flight_no": full_flight_no,
                "origin": origin,
                "destination": destination,
                "departure": dep_dt,
                "arrival": arr_dt,
            }
        except (ValueError, IndexError) as e:
            logger.debug("Jetstar: JourneySellKey parse error: %s", e)
            return None

    def _build_bundle_segment(self, leg: dict, req: FlightSearchRequest) -> Optional[FlightSegment]:
        """Build a FlightSegment from a bundle-data-v2 Segments[].Legs[] entry."""
        carrier = leg.get("CarrierCode") or leg.get("carrierCode") or leg.get("Carrier") or "JQ"
        flight_no_raw = leg.get("FlightNumber") or leg.get("flightNumber") or leg.get("FlightDesignator") or ""
        if isinstance(flight_no_raw, dict):
            flight_no = str(flight_no_raw.get("FlightNumber", ""))
            carrier = flight_no_raw.get("CarrierCode", carrier)
        else:
            flight_no = str(flight_no_raw)

        full_flight_no = f"{carrier}{flight_no}" if flight_no else ""

        origin = (leg.get("DepartureStation") or leg.get("departureStation")
                  or leg.get("Origin") or leg.get("origin") or req.origin)
        destination = (leg.get("ArrivalStation") or leg.get("arrivalStation")
                       or leg.get("Destination") or leg.get("destination") or req.destination)

        dep_str = (leg.get("STD") or leg.get("std") or leg.get("DepartureDateTime")
                   or leg.get("departureDateTime") or leg.get("DepartureDate") or "")
        arr_str = (leg.get("STA") or leg.get("sta") or leg.get("ArrivalDateTime")
                   or leg.get("arrivalDateTime") or leg.get("ArrivalDate") or "")

        return FlightSegment(
            airline=carrier,
            airline_name="Jetstar",
            flight_no=full_flight_no,
            origin=origin,
            destination=destination,
            departure=self._parse_dt(dep_str),
            arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _parse_navitaire_data(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fallback: parse Navitaire-style FlightData JSON."""
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        journeys = data.get("journeys") or data.get("trips") or []
        for journey in journeys:
            flights = journey.get("flights") or journey.get("segments") or []
            for flight in flights:
                offer = self._parse_navitaire_flight(flight, req, booking_url)
                if offer:
                    offers.append(offer)

        if not offers:
            flights = data.get("flights") or data.get("outboundFlights") or []
            for flight in flights:
                offer = self._parse_navitaire_flight(flight, req, booking_url)
                if offer:
                    offers.append(offer)

        return offers

    def _parse_navitaire_flight(
        self, flight: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        price = self._extract_best_price(flight)
        if price is None or price <= 0:
            return None

        legs_raw = flight.get("legs") or flight.get("segments") or []
        segments: list[FlightSegment] = []
        for leg in legs_raw:
            segments.append(self._build_segment(leg, req.origin, req.destination))
        if not segments:
            segments.append(self._build_segment(flight, req.origin, req.destination))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("journeyKey") or flight.get("standardFareKey")
            or flight.get("id") or f"{req.origin}_{req.destination}_{time.monotonic()}"
        )
        return FlightOffer(
            id=f"jq_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(price, 2), currency="AUD",
            price_formatted=f"${price:.2f} AUD",
            outbound=route, inbound=None,
            airlines=["Jetstar"], owner_airline="JQ",
            booking_url=booking_url, is_locked=False,
            source="jetstar_direct", source_tier="free",
        )

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight data from DOM flight cards on the select-flights page.
        
        Navitaire renders flight cards with:
        - Departure/arrival times (e.g. "6:00am", "7:40am")
        - Airport codes (e.g. "SYD - Departure", "MEL - Arrival")
        - Duration (e.g. "Direct flight - 1hr 40mins travel")
        - Prices (e.g. "Regular price from 83 AUD")
        """
        flights_data = await page.evaluate(r"""() => {
            const flights = [];
            
            // Find all flight row containers - they are clickable divs containing
            // departure/arrival info and price buttons
            const allButtons = document.querySelectorAll('button[aria-label]');
            const priceButtons = [];
            for (const btn of allButtons) {
                const label = btn.getAttribute('aria-label') || '';
                if (label.includes('AUD') && label.includes('price')) {
                    priceButtons.push(btn);
                }
            }
            
            for (const priceBtn of priceButtons) {
                // Walk up to find the flight row container
                let row = priceBtn.closest('[class]');
                // Keep walking up until we find a container with time info
                for (let i = 0; i < 10 && row; i++) {
                    const text = row.textContent || '';
                    if (/\d{1,2}:\d{2}(am|pm)/i.test(text) && text.includes('Departure') && text.includes('Arrival')) {
                        break;
                    }
                    row = row.parentElement;
                }
                if (!row) continue;
                
                const text = row.textContent || '';
                
                // Extract times: "6:00am" pattern
                const timeMatches = text.match(/(\d{1,2}:\d{2}(?:am|pm))/gi);
                if (!timeMatches || timeMatches.length < 2) continue;
                
                const depTime = timeMatches[0];
                const arrTime = timeMatches[1];
                
                // Extract airport codes: "SYD - Departure" / "MEL - Arrival" 
                const depAirportMatch = text.match(/([A-Z]{3})\s*-\s*Departure/);
                const arrAirportMatch = text.match(/([A-Z]{3})\s*-\s*Arrival/);
                const depAirport = depAirportMatch ? depAirportMatch[1] : '';
                const arrAirport = arrAirportMatch ? arrAirportMatch[1] : '';
                
                // Extract duration: "1hr 40mins" or "Direct flight - 1hr 40mins travel"
                const durMatch = text.match(/(\d+)hr\s*(\d+)min/i);
                const durationMins = durMatch ? parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]) : 0;
                
                // Direct vs stops
                const isDirect = /direct\s*flight/i.test(text);
                const stopsMatch = text.match(/(\d+)\s*stop/i);
                const stops = isDirect ? 0 : (stopsMatch ? parseInt(stopsMatch[1]) : 0);
                
                // Extract prices from the button aria-label
                // Format: "Club Jetstar price from 76 AUD Regular price from 83 AUD"
                // or: "1 left at this price Club Jetstar price from 96 AUD Regular price from 103 AUD"
                const label = priceBtn.getAttribute('aria-label') || '';
                const regularPriceMatch = label.match(/Regular\s+price\s+from\s+(\d+(?:\.\d+)?)\s+AUD/i);
                const clubPriceMatch = label.match(/Club\s+Jetstar\s+price\s+from\s+(\d+(?:\.\d+)?)\s+AUD/i);
                
                // Use regular price (non-member price)
                const price = regularPriceMatch ? parseFloat(regularPriceMatch[1]) :
                              (clubPriceMatch ? parseFloat(clubPriceMatch[1]) : 0);
                
                if (price <= 0) continue;
                
                // Dedup check: don't add same dep+arr time twice
                const key = depTime + '_' + arrTime;
                if (flights.some(f => f.key === key)) continue;
                
                flights.push({
                    key: key,
                    depTime: depTime,
                    arrTime: arrTime,
                    depAirport: depAirport,
                    arrAirport: arrAirport,
                    durationMins: durationMins,
                    stops: stops,
                    price: price,
                    currency: 'AUD'
                });
            }
            
            return flights;
        }""")

        if not flights_data:
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        dep_date = req.date_from

        for fd in flights_data:
            try:
                dep_dt = self._parse_time_ampm(fd["depTime"], dep_date)
                arr_dt = self._parse_time_ampm(fd["arrTime"], dep_date)
                # Handle overnight: if arrival is before departure, add a day
                if arr_dt < dep_dt:
                    from datetime import timedelta
                    arr_dt = arr_dt + timedelta(days=1)

                origin = fd.get("depAirport") or req.origin
                destination = fd.get("arrAirport") or req.destination

                seg = FlightSegment(
                    airline="JQ", airline_name="Jetstar",
                    flight_no="",
                    origin=origin, destination=destination,
                    departure=dep_dt, arrival=arr_dt,
                    cabin_class="M",
                )
                dur = fd.get("durationMins", 0) * 60
                if dur == 0 and dep_dt and arr_dt:
                    dur = int((arr_dt - dep_dt).total_seconds())

                route = FlightRoute(
                    segments=[seg],
                    total_duration_seconds=max(dur, 0),
                    stopovers=fd.get("stops", 0),
                )
                price = fd["price"]
                flight_key = f"{origin}_{destination}_{fd['depTime']}_{fd['arrTime']}"
                offers.append(FlightOffer(
                    id=f"jq_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
                    price=round(price, 2), currency="AUD",
                    price_formatted=f"${price:.2f} AUD",
                    outbound=route, inbound=None,
                    airlines=["Jetstar"], owner_airline="JQ",
                    booking_url=booking_url, is_locked=False,
                    source="jetstar_direct", source_tier="free",
                ))
            except Exception as e:
                logger.debug("Jetstar: DOM flight parse error: %s", e)
                continue

        return offers

    @staticmethod
    def _parse_time_ampm(time_str: str, base_date: datetime) -> datetime:
        """Parse '6:00am' / '7:40pm' into datetime on the given date."""
        m = re.match(r"(\d{1,2}):(\d{2})(am|pm)", time_str, re.IGNORECASE)
        if not m:
            return datetime(2000, 1, 1)
        hour = int(m.group(1))
        minute = int(m.group(2))
        ampm = m.group(3).lower()
        if ampm == "pm" and hour != 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
        return datetime(base_date.year, base_date.month, base_date.day, hour, minute)

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareProducts") or flight.get("bundles") or []
        best = float("inf")
        for fare in fares:
            if isinstance(fare, dict):
                for key in ["price", "amount", "totalPrice", "basePrice", "fareAmount", "standardFare"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("value")
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        for key in ["price", "lowestFare", "totalPrice", "farePrice", "amount", "standardFare"]:
            p = flight.get(key)
            if p is not None:
                try:
                    v = float(p) if not isinstance(p, dict) else float(p.get("amount", 0))
                    if 0 < v < best:
                        best = v
                except (TypeError, ValueError):
                    pass
        return best if best < float("inf") else None

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = seg.get("departureDateTime") or seg.get("departure") or seg.get("departureDate") or seg.get("std") or ""
        arr_str = seg.get("arrivalDateTime") or seg.get("arrival") or seg.get("arrivalDate") or seg.get("sta") or ""
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("number") or "").replace(" ", "")
        origin = seg.get("origin") or seg.get("departureStation") or seg.get("departureAirport") or default_origin
        destination = seg.get("destination") or seg.get("arrivalStation") or seg.get("arrivalAirport") or default_dest
        carrier = seg.get("carrierCode") or seg.get("carrier") or seg.get("airline") or "JQ"
        return FlightSegment(
            airline=carrier, airline_name="Jetstar", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float, source: str = "Playwright") -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Jetstar %s->%s returned %d offers in %.1fs (%s)", req.origin, req.destination, len(offers), elapsed, source)
        h = hashlib.md5(f"jetstar{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="AUD", offers=offers, total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        adults = getattr(req, "adults", 1) or 1
        return (
            f"https://booking.jetstar.com/au/en/booking/search-flights"
            f"?origin1={req.origin}&destination1={req.destination}"
            f"&departuredate1={dep}&ADT={adults}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"jetstar{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="AUD", offers=[], total_results=0,
        )
