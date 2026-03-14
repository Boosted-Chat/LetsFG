"""
JetSMART Playwright scraper — homepage form + Navitaire API interception.

JetSMART (IATA: JA) is a South American ultra-low-cost carrier (Navitaire-based)
operating in Chile (SCL hub), Argentina, Peru, Colombia, and Brazil.
Default currencies: CLP (Chile), ARS (Argentina), PEN (Peru), COP (Colombia).

Strategy (rewritten Mar 2026):
1. Navigate to jetsmart.com/{market}/en homepage
2. Remove cookie consent overlay (lf_gdpr_modal + GDPR elements) via JS injection
3. Set one-way trip type via JS click on radio/label
4. Fill origin: JS-click dropdown trigger in figma-booking-form, keyboard type
   IATA, JS-click matching <li> in ac-dropdown2 airport list
5. Fill destination: same approach (dropdown may auto-open after origin)
6. Navigate calendar to correct month via next-arrow clicks, click target day
7. Click search button (.figma-search-btn)
8. Intercept Navitaire availability/timetable API responses
9. Parse trips/journeys → FlightOffers
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
from typing import Any, Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-US", "es-CL", "en-GB"]
_TIMEZONES = [
    "America/Santiago", "America/Argentina/Buenos_Aires", "America/Lima",
    "America/Bogota", "America/Sao_Paulo",
]

# Map origin airports to JetSMART markets
_MARKET_MAP = {
    "SCL": "cl", "IQQ": "cl", "CJC": "cl", "ANF": "cl", "CCP": "cl", "PMC": "cl",
    "ZCO": "cl", "ARI": "cl", "LSC": "cl", "ZOS": "cl",
    "EZE": "ar", "AEP": "ar", "COR": "ar", "MDZ": "ar", "IGR": "ar",
    "BRC": "ar", "NQN": "ar", "USH": "ar", "ROS": "ar", "SLA": "ar",
    "LIM": "pe", "CUZ": "pe", "AQP": "pe",
    "BOG": "co", "MDE": "co", "CTG": "co", "CLO": "co",
}

# ── Shared browser singleton via CDP ────────────────────────────────────
_CDP_PORT = 9461
_chrome_proc = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Connect to a real Chrome instance via CDP (launched once, reused)."""
    global _chrome_proc, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from connectors.browser import get_or_launch_cdp
        _user_data = os.path.join(os.environ.get("TEMP", "/tmp"), "chrome-cdp-jetsmart")
        _browser, _chrome_proc = await get_or_launch_cdp(_CDP_PORT, _user_data)
        logger.info("JetSMART: Chrome ready via CDP (port %d)", _CDP_PORT)
        return _browser


class JetSmartConnectorClient:
    """JetSMART Playwright scraper — homepage form + Navitaire API interception."""

    _TIMETABLE_URL = "https://jetsmart.com/farecache-lm/timetable"

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def _try_timetable_api(self, req: FlightSearchRequest) -> list[FlightOffer]:
        """Fast path: hit the timetable API directly via HTTP (no browser needed)."""
        import aiohttp
        start = req.date_from.replace(day=1).strftime("%Y-%m-%d")
        params = {
            "departure": req.origin,
            "destination": req.destination,
            "currency": req.currency or "CLP",
            "withPriceRange": "",
            "startDate": start,
            "return": "false",
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self._TIMETABLE_URL,
                    params=params,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        offers = self._parse_timetable(data, req)
                        if offers:
                            logger.info("JetSMART: got %d offers via direct API", len(offers))
                            return offers
        except Exception as e:
            logger.debug("JetSMART: direct API failed: %s", e)
        return []

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        # Fast path: direct HTTP timetable API (no browser needed)
        offers = await self._try_timetable_api(req)
        if offers:
            elapsed = time.monotonic() - t0
            return self._build_response(offers, req, elapsed)

        # Slow path: Playwright browser flow
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

            captured_availability: list[dict] = []
            captured_timetable: list[dict] = []
            avail_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    if response.status != 200:
                        return
                    ct = response.headers.get("content-type", "")
                    if "json" not in ct and "javascript" not in ct:
                        return

                    # Timetable = calendar pricing (pre-search), separate from real availability
                    if "timetable" in url and "farecache" in url:
                        data = await response.json()
                        if data and isinstance(data, (dict, list)):
                            captured_timetable.append(data)
                            logger.info("JetSMART: captured timetable from %s", response.url[:120])
                        return
                    # Skip other farecache endpoints (configurations, stations)
                    if "farecache" in url:
                        return

                    # Real availability / search results
                    if (
                        "availability" in url
                        or "/api/nsk/" in url
                        or "searchshop" in url
                        or "shopping" in url
                    ):
                        data = await response.json()
                        if data and isinstance(data, (dict, list)):
                            captured_availability.append(data)
                            avail_event.set()
                            logger.info("JetSMART: captured availability from %s", response.url[:120])
                except Exception:
                    pass

            page.on("response", on_response)

            market = _MARKET_MAP.get(req.origin, "cl")
            homepage = f"https://jetsmart.com/{market}/en"

            logger.info("JetSMART: loading %s for %s→%s on %s",
                        homepage, req.origin, req.destination, req.date_from)
            await page.goto(
                homepage,
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(3.0)

            # Remove all blocking overlays (cookie consent, GDPR modal)
            await self._remove_overlays(page)
            await asyncio.sleep(0.5)

            # Set one-way trip type
            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            # Remove overlays again (may reappear after interaction)
            await self._remove_overlays(page)

            # Fill origin airport
            ok = await self._fill_airport(page, req.origin, is_origin=True)
            if not ok:
                logger.warning("JetSMART: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(1.0)

            # Fill destination airport
            ok = await self._fill_airport(page, req.destination, is_origin=False)
            if not ok:
                logger.warning("JetSMART: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill departure date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("JetSMART: date fill failed")
                # Don't return empty — timetable may already be captured
            await asyncio.sleep(0.5)

            # Click search (navigates to booking.jetsmart.com which may 403)
            await self._click_search(page)

            # Wait briefly for availability API (booking engine often blocks bots)
            remaining = min(max(self.timeout - (time.monotonic() - t0), 5), 15)
            try:
                await asyncio.wait_for(avail_event.wait(), timeout=remaining)
                await asyncio.sleep(2.0)
            except asyncio.TimeoutError:
                logger.info("JetSMART: no availability API (expected — booking engine blocks bots)")

            # Parse real availability data first
            offers: list[FlightOffer] = []
            for data in captured_availability:
                parsed = self._parse_response(data, req)
                if parsed:
                    offers.extend(parsed)

            # Primary: timetable calendar pricing → filter to target date
            if not offers and captured_timetable:
                for data in captured_timetable:
                    parsed = self._parse_timetable(data, req)
                    if parsed:
                        offers.extend(parsed)

            # Fallback: DOM extraction
            if not offers:
                offers = await self._extract_from_dom(page, req)

            elapsed = time.monotonic() - t0
            if offers:
                return self._build_response(offers, req, elapsed)
            return self._empty(req)

        except Exception as e:
            logger.error("JetSMART Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ── Overlay removal ─────────────────────────────────────────────────

    async def _remove_overlays(self, page) -> None:
        """Remove cookie consent (lf_gdpr_modal), GDPR, and any blocking overlays."""
        await page.evaluate("""() => {
            const selectors = [
                '#lf_gdpr_modal', '.lf_gdpr_modal', '[id*="gdpr"]', '[class*="gdpr"]',
                '[class*="cookie"]', '[id*="cookie"]', '[class*="consent"]', '[id*="consent"]',
                '[class*="Cookie"]', '[id*="Cookie"]', '[class*="onetrust"]', '[id*="onetrust"]',
                '.modal-backdrop', '.cdk-overlay-backdrop',
            ];
            for (const s of selectors) {
                document.querySelectorAll(s).forEach(el => el.remove());
            }
            // Remove large fixed-position overlays that block clicks
            document.querySelectorAll('div').forEach(el => {
                const st = getComputedStyle(el);
                if (st.position === 'fixed' && el.offsetHeight > window.innerHeight * 0.5
                    && el.offsetWidth > window.innerWidth * 0.5) {
                    const tag = (el.className + ' ' + el.id).toLowerCase();
                    if (tag.includes('modal') || tag.includes('overlay') || tag.includes('gdpr')
                        || tag.includes('cookie') || tag.includes('consent') || tag.includes('popup')) {
                        el.remove();
                    }
                }
            });
            document.body.style.overflow = 'auto';
            document.documentElement.style.overflow = 'auto';
        }""")

    # ── One-way toggle ──────────────────────────────────────────────────

    async def _set_one_way(self, page) -> None:
        """Click the one-way trip type option via JS TreeWalker."""
        await page.evaluate("""() => {
            // JetSMART uses plain text nodes "Solo ida" / "Ida y vuelta" in divs
            // (no radio inputs). Use TreeWalker to find exact text node.
            const targets = ['Solo ida', 'One way', 'One-way', 'Ida'];
            const tw = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null);
            let node;
            while (node = tw.nextNode()) {
                const t = node.textContent.trim();
                for (const target of targets) {
                    if (t === target) {
                        node.parentElement.click();
                        return;
                    }
                }
            }
            // Fallback: radio inputs (other market sites)
            const radios = document.querySelectorAll('input[type="radio"]');
            for (const r of radios) {
                const container = r.closest('label') || r.parentElement;
                const text = (container ? container.textContent : '').toLowerCase();
                if (text.includes('one way') || text.includes('solo ida')) {
                    r.click();
                    return;
                }
            }
        }""")
        logger.info("JetSMART: set one-way")

    # ── Airport selection ───────────────────────────────────────────────

    async def _fill_airport(self, page, iata: str, is_origin: bool) -> bool:
        """Fill origin or destination airport.

        JetSMART form structure (verified Mar 2026):
        - input[placeholder="Origen"] / input[placeholder="Destino"]
        - ac-dropdown2 renders a fixed-position overlay with country groups
        - Each airport is: <li class="...cursor-pointer..."> City <span>IATA</span> </li>
        - After origin selection, destination dropdown auto-opens
        - Typing in the input filters airports across all countries
        """
        field_name = "origin" if is_origin else "destination"
        placeholder = "Origen" if is_origin else "Destino"

        # Step 1: Click the input to open/focus the dropdown
        opened = await page.evaluate("""(ph) => {
            const inp = document.querySelector('input[placeholder="' + ph + '"]');
            if (inp) { inp.focus(); inp.click(); return true; }
            return false;
        }""", placeholder)

        if not opened:
            # Fallback: find by index among visible text inputs
            opened = await page.evaluate("""(isOrigin) => {
                const inputs = document.querySelectorAll('input[type="text"]');
                const visible = [...inputs].filter(i => i.offsetHeight > 0);
                const idx = isOrigin ? 0 : 1;
                if (visible.length > idx) { visible[idx].focus(); visible[idx].click(); return true; }
                return false;
            }""", is_origin)

        if not opened:
            logger.warning("JetSMART: could not open %s input", field_name)
            return False

        await asyncio.sleep(0.8)
        await self._remove_overlays(page)

        # Step 2: Type IATA code to trigger dropdown filtering
        await page.keyboard.type(iata, delay=120)
        await asyncio.sleep(2.0)

        # Step 3: Click the matching airport <li> via the IATA <span>
        # JetSMART airports: <li class="...cursor-pointer...">City <span class="text-[#a1a1a1]...">IATA</span></li>
        clicked = await page.evaluate("""(iata) => {
            // Primary: find <span> elements with exact IATA text, click parent <li>
            const spans = document.querySelectorAll('span');
            for (const span of spans) {
                if (span.textContent.trim() === iata && span.offsetHeight > 0) {
                    const li = span.closest('li');
                    if (li && li.className.includes('cursor-pointer')) {
                        li.click();
                        return 'span-li';
                    }
                    // Fallback: click the span's parent
                    span.parentElement.click();
                    return 'span-parent';
                }
            }
            // Secondary: find li containing IATA text
            const items = document.querySelectorAll('li');
            for (const item of items) {
                const text = item.textContent || '';
                if ((text.includes(iata) || text.includes('(' + iata + ')'))
                    && item.offsetHeight > 0 && item.className.includes('cursor-pointer')) {
                    item.click();
                    return 'li-text';
                }
            }
            return false;
        }""", iata)

        if clicked:
            logger.info("JetSMART: selected %s airport %s via %s", field_name, iata, clicked)
            return True

        # Fallback: press Enter
        await page.keyboard.press("Enter")
        await asyncio.sleep(0.5)
        logger.info("JetSMART: pressed Enter for %s airport %s (fallback)", field_name, iata)
        return True

    # ── Date picker ─────────────────────────────────────────────────────

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Navigate calendar and click target day.

        JetSMART calendar structure (verified Mar 2026):
        - Calendar auto-opens as ac-dropdown2 overlay after destination selection
        - Shows 2 months side-by-side, scrollable
        - Day cells are <div class="...cursor-pointer..."> (NOT buttons!)
        - Inside a grid: <div class="grid grid-cols-7">
        - Month heading is a sibling div with text like "Abril 2026"
        - Navigate months by scrolling the calendar panel or clicking next arrow
        - The open calendar is inside a div with z-[9999] class
        """
        target = req.date_from
        target_month = target.month
        target_year = target.year
        target_day = target.day

        _MONTHS_ES = {
            1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo",
            6: "Junio", 7: "Julio", 8: "Agosto", 9: "Septiembre",
            10: "Octubre", 11: "Noviembre", 12: "Diciembre",
        }
        _MONTHS_EN = {
            1: "January", 2: "February", 3: "March", 4: "April", 5: "May",
            6: "June", 7: "July", 8: "August", 9: "September",
            10: "October", 11: "November", 12: "December",
        }
        month_es = f"{_MONTHS_ES[target_month]} {target_year}"
        month_en = f"{_MONTHS_EN[target_month]} {target_year}"

        # Click the date field to open calendar (if not already open)
        await page.evaluate("""() => {
            const inp = document.querySelector('input[placeholder="Fecha de ida"]')
                || document.querySelector('input[placeholder="Departure"]')
                || document.querySelector('input[placeholder="Date"]');
            if (inp) { inp.click(); }
        }""")
        await asyncio.sleep(1.0)

        # Navigate to the target month (scroll/click forward)
        for _ in range(12):
            found = await page.evaluate("""([monthEs, monthEn]) => {
                // Check if target month heading is visible inside the open calendar dropdown
                const openDD = document.querySelector('div[class*="z-[9999]"]');
                if (!openDD) return false;
                const text = openDD.innerText || '';
                return text.includes(monthEs) || text.includes(monthEn);
            }""", [month_es, month_en])

            if found:
                break

            # Try clicking next-month navigation (various selectors)
            scrolled = await page.evaluate("""() => {
                const openDD = document.querySelector('div[class*="z-[9999]"]');
                if (!openDD) return false;
                // Look for next-month arrow/button
                const nexts = openDD.querySelectorAll(
                    '[class*="next"], [aria-label*="next"], [aria-label*="siguiente"], ' +
                    'svg, [class*="chevron"], [class*="arrow"]'
                );
                for (const n of nexts) {
                    const btn = n.closest('div[class*="cursor-pointer"]') || n.closest('button') || n;
                    if (btn.offsetHeight > 0 && btn.offsetHeight < 60) {
                        btn.click();
                        return true;
                    }
                }
                // Fallback: scroll the calendar container
                const scrollable = openDD.querySelector('[class*="overflow"]') || openDD;
                scrollable.scrollTop += 300;
                return true;
            }""")
            if not scrolled:
                break
            await asyncio.sleep(0.5)

        # Click the target day in the correct month section
        clicked_day = await page.evaluate("""([day, monthEs, monthEn]) => {
            const dayStr = String(day);
            const openDD = document.querySelector('div[class*="z-[9999]"]');
            if (!openDD) return false;

            // Find all day divs with cursor-pointer that contain just the day number
            const allDivs = openDD.querySelectorAll('div');
            for (const d of allDivs) {
                if (d.textContent.trim() !== dayStr) continue;
                if (d.children.length > 0) continue;
                if (d.offsetHeight === 0) continue;
                if (!d.className.includes('cursor-pointer')) continue;

                // Verify this day is in the target month by walking up
                let el = d;
                let inTargetMonth = false;
                for (let i = 0; i < 8; i++) {
                    el = el.parentElement;
                    if (!el) break;
                    const text = el.innerText || '';
                    if (text.includes(monthEs) || text.includes(monthEn)) {
                        // Make sure it also does NOT contain a different month's full text
                        inTargetMonth = true;
                        break;
                    }
                }
                if (inTargetMonth) {
                    d.click();
                    return true;
                }
            }

            // Broader fallback: click any visible day matching the number
            for (const d of allDivs) {
                if (d.textContent.trim() === dayStr && d.children.length === 0
                    && d.offsetHeight > 0 && d.className.includes('cursor-pointer')) {
                    d.click();
                    return true;
                }
            }
            return false;
        }""", [target_day, month_es, month_en])

        if clicked_day:
            logger.info("JetSMART: selected date %s", target)
            await asyncio.sleep(0.5)
            return True

        logger.warning("JetSMART: could not click day %d", target_day)
        return False

    # ── Search button ───────────────────────────────────────────────────

    async def _click_search(self, page) -> None:
        """Click the search/submit button."""
        clicked = await page.evaluate("""() => {
            // JetSMART search: "buscar smart" text inside a cursor-pointer div
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
            while (walker.nextNode()) {
                const txt = walker.currentNode.textContent.trim().toLowerCase();
                if (txt === 'buscar smart' || txt === 'search smart' || txt === 'buscar' || txt === 'search flights') {
                    let el = walker.currentNode.parentElement;
                    for (let i = 0; i < 4; i++) {
                        if (el && el.className && el.className.includes('cursor-pointer')) {
                            el.click();
                            return true;
                        }
                        el = el.parentElement;
                    }
                    // Click immediate parent as fallback
                    walker.currentNode.parentElement.click();
                    return true;
                }
            }
            // Fallback: figma-search-btn
            const jsBtn = document.querySelector('#figma-search-btn');
            if (jsBtn) { jsBtn.click(); return true; }
            // Generic: any visible button with search text
            const buttons = document.querySelectorAll('button');
            for (const b of buttons) {
                const text = b.textContent.toLowerCase().trim();
                if (text.includes('search') || text.includes('buscar')) {
                    if (b.offsetHeight > 0) { b.click(); return true; }
                }
            }
            return false;
        }""")
        if clicked:
            logger.info("JetSMART: clicked search")
        else:
            await page.keyboard.press("Enter")
            logger.info("JetSMART: pressed Enter (search fallback)")

    # ── DOM extraction fallback ─────────────────────────────────────────

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Try to extract flight data from page JS state or DOM."""
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                // Try Angular/Navitaire state
                const appRoot = document.querySelector('app-root');
                if (appRoot && appRoot.__ngContext__) {
                    try { return JSON.parse(JSON.stringify(appRoot.__ngContext__)); } catch {}
                }
                // Try inline JSON scripts
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.journeys || d.fares || d.availability || d.trips)) return d;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
        return []

    def _parse_timetable(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse farecache-lm/timetable response — calendar pricing.

        Returns offers only for the requested date.
        Timetable format: list of {date, lowestFare, currency, ...} or similar.
        """
        target_str = req.date_from.strftime("%Y-%m-%d")
        currency = req.currency or "CLP"
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Response format: {"outbound": [{"departureDate": "...", "price": N, "currency": "..."}, ...], "metadata": ...}
        items = (
            data.get("outbound")
            or data.get("items")
            or data.get("dates")
            or (data if isinstance(data, list) else [])
        )
        if not isinstance(items, list):
            return []

        for item in items:
            if not isinstance(item, dict):
                continue
            date_val = item.get("date") or item.get("departureDate") or item.get("std") or ""
            if not str(date_val).startswith(target_str):
                continue

            price = None
            for key in ["lowestFare", "price", "amount", "totalPrice", "fareAmount", "basePrice"]:
                v = item.get(key)
                if v is not None:
                    try:
                        price = float(v)
                        if price > 0:
                            break
                    except (TypeError, ValueError):
                        pass

            if price is None or price <= 0:
                continue

            # Build a minimal offer — no segment details from timetable
            dep_dt = self._parse_dt(date_val)
            seg = FlightSegment(
                airline="JA", airline_name="JetSMART", flight_no="",
                origin=req.origin, destination=req.destination,
                departure=dep_dt, arrival=dep_dt,
                cabin_class="M",
            )
            route = FlightRoute(segments=[seg], total_duration_seconds=0, stopovers=0)
            h = hashlib.md5(f"tt_{date_val}_{price}".encode()).hexdigest()[:12]
            offers.append(FlightOffer(
                id=f"ja_{h}",
                price=round(price, 2),
                currency=item.get("currency", currency),
                price_formatted=f"{price:.0f} {currency}" if currency == "CLP" else f"{price:.2f} {currency}",
                outbound=route, inbound=None,
                airlines=["JetSMART"], owner_airline="JA",
                booking_url=booking_url, is_locked=False,
                source="jetsmart_direct", source_tier="free",
            ))

        if offers:
            logger.info("JetSMART: parsed %d offers from timetable for %s", len(offers), target_str)
        return offers

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if isinstance(data, list):
            data = {"flights": data}
        currency = req.currency or "CLP"
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Navitaire-style: trips[0].dates[0].journeys or direct journeys list
        flights_raw = None
        trips = data.get("trips") or data.get("data", {}).get("trips")
        if trips and isinstance(trips, list) and len(trips) > 0:
            dates = trips[0].get("dates") or trips[0].get("journeyDates") or []
            if dates and isinstance(dates, list):
                for d in dates:
                    jnys = d.get("journeys") or d.get("flights") or []
                    if jnys:
                        flights_raw = jnys
                        break
        if not flights_raw:
            flights_raw = (
                data.get("outboundFlights")
                or data.get("outbound")
                or data.get("journeys")
                or data.get("flights")
                or data.get("data", {}).get("flights", [])
                or []
            )
        if isinstance(flights_raw, dict):
            flights_raw = flights_raw.get("outbound", []) or flights_raw.get("journeys", [])
        if not isinstance(flights_raw, list):
            flights_raw = []

        for flight in flights_raw:
            offer = self._parse_single_flight(flight, currency, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    def _parse_single_flight(self, flight: dict, currency: str, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        best_price = self._extract_best_price(flight)
        if best_price is None or best_price <= 0:
            return None

        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = flight.get("journeyKey") or flight.get("id") or f"{flight.get('departureDate', '')}_{time.monotonic()}"
        return FlightOffer(
            id=f"ja_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=currency,
            price_formatted=f"{best_price:.0f} {currency}" if currency == "CLP" else f"{best_price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["JetSMART"],
            owner_airline="JA",
            booking_url=booking_url,
            is_locked=False,
            source="jetsmart_direct",
            source_tier="free",
        )

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareProducts") or flight.get("bundles") or flight.get("fareBundles") or []
        best = float("inf")
        for fare in fares:
            if isinstance(fare, dict):
                pax_fares = fare.get("passengerFares") or fare.get("paxFares") or []
                if pax_fares and isinstance(pax_fares, list):
                    for pf in pax_fares:
                        for key in ["fareAmount", "publishedFare", "fareTotal", "total"]:
                            v = pf.get(key)
                            if v is not None:
                                try:
                                    val = float(v)
                                    if 0 < val < best:
                                        best = val
                                except (TypeError, ValueError):
                                    pass
                for key in ["price", "amount", "totalPrice", "basePrice", "fareAmount"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("value") or val.get("total")
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        for key in ["price", "lowestFare", "totalPrice", "farePrice", "amount"]:
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
        dep_str = seg.get("departureDateTime") or seg.get("departure") or seg.get("std") or seg.get("designator", {}).get("departure", "")
        arr_str = seg.get("arrivalDateTime") or seg.get("arrival") or seg.get("sta") or seg.get("designator", {}).get("arrival", "")
        flight_no = str(
            seg.get("flightNumber") or seg.get("flight_no") or seg.get("number")
            or seg.get("identifier", {}).get("identifier", "")
        ).replace(" ", "")
        origin = seg.get("origin") or seg.get("departureStation") or seg.get("designator", {}).get("origin", default_origin)
        destination = seg.get("destination") or seg.get("arrivalStation") or seg.get("designator", {}).get("destination", default_dest)
        return FlightSegment(
            airline="JA", airline_name="JetSMART", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("JetSMART %s->%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"jetsmart{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "CLP"),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        market = _MARKET_MAP.get(req.origin, "cl")
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://jetsmart.com/{market}/en/select"
            f"?origin={req.origin}&destination={req.destination}"
            f"&departure={dep}&adults={req.adults}&children={req.children}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"jetsmart{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency or "CLP", offers=[], total_results=0,
        )
