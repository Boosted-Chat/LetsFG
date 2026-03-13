"""
IndiGo direct scraper — uses Playwright to scrape flight data.

IndiGo (IATA: 6E) is India's largest airline by market share.
Website: www.goindigo.in — custom React SPA with Module Federation micro-frontends.

Strategy:
1. Navigate to goindigo.in homepage
2. Dismiss cookie/session banners
3. Fill search form (From/To/Departure, One Way)
4. Intercept flight search API via page.route() + route.fetch()
5. Parse JSON → FlightOffer objects

Key technical details:
- API endpoint: api-prod-flight-skyplus6e.goindigo.in/v1/flight/search (POST)
- page.on("response") cannot see cross-origin API calls from micro-frontends
- page.route() + route.fetch() intercepts at the Playwright proxy level
- Akamai Bot Manager protects the API; may return 403 on some requests
- response is text/plain with JSON body (728KB+ for popular routes)
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
    {"width": 1600, "height": 900},
]
_LOCALES = ["en-IN", "en-US", "en-GB"]
_TIMEZONES = [
    "Asia/Kolkata", "Asia/Dubai", "Europe/London",
    "Asia/Singapore", "Asia/Bangkok",
]

# ── Shared browser singleton via CDP ────────────────────────────────────
_CDP_PORT = 9460
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
        _user_data = os.path.join(os.environ.get("TEMP", "/tmp"), "chrome-cdp-indigo")
        _browser, _chrome_proc = await get_or_launch_cdp(_CDP_PORT, _user_data)
        logger.info("IndiGo: Chrome ready via CDP (port %d)", _CDP_PORT)
        return _browser


class IndiGoConnectorClient:
    """IndiGo Playwright scraper — React SPA form search + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # Retry up to 2 times (Akamai may block the first attempt)
        for attempt in range(2):
            result = await self._try_search(req)
            if result.total_results > 0:
                return result
            if attempt == 0:
                logger.info("IndiGo: retrying search (attempt %d failed)", attempt + 1)
                await asyncio.sleep(2.0)
        return result

    async def _try_search(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
        )

        try:
            page = await context.new_page()

            captured_data: dict = {}
            api_event = asyncio.Event()
            import json as _json

            # Intercept the flight search API at the Playwright proxy level.
            # page.on("response") can't see this cross-origin API call, but
            # page.route() + route.fetch() intercepts at the proxy layer.
            async def _intercept_flight(route):
                if route.request.method == "OPTIONS":
                    await route.continue_()
                    return
                try:
                    resp = await route.fetch()
                    body = await resp.text()
                    if resp.status == 200 and body.strip():
                        data = _json.loads(body)
                        if isinstance(data, dict) and "data" in data:
                            captured_data["json"] = data["data"]
                        else:
                            captured_data["json"] = data
                        logger.info("IndiGo: captured flight/search API (%d bytes)", len(body))
                        api_event.set()
                    else:
                        logger.warning("IndiGo: API returned status %d (Akamai may be blocking)", resp.status)
                    await route.fulfill(
                        status=resp.status,
                        headers=resp.headers,
                        body=body,
                    )
                except Exception as exc:
                    logger.debug("IndiGo route intercept error: %s", exc)
                    try:
                        await route.continue_()
                    except Exception:
                        pass

            await page.route("**/v1/flight/search**", _intercept_flight)

            logger.info("IndiGo: loading homepage for %s->%s", req.origin, req.destination)
            await page.goto(
                "https://www.goindigo.in/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(6.0)

            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await self._dismiss_cookies(page)

            # Select One Way trip type
            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            # Fill origin
            ok = await self._fill_airport_field(page, "From", req.origin, 0)
            if not ok:
                logger.warning("IndiGo: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill destination - IndiGo uses "Going to?" placeholder
            ok = await self._fill_airport_field(page, "To", req.destination, 1)
            if not ok:
                logger.warning("IndiGo: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("IndiGo: date fill failed")
                return self._empty(req)
            await asyncio.sleep(0.3)

            # Click search
            await self._click_search(page)

            # Wait for the flight search API response via route interceptor
            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("IndiGo: API event timed out, trying DOM fallback")

            data = captured_data.get("json")
            if data:
                elapsed = time.monotonic() - t0
                offers = self._parse_response(data, req)
                return self._build_response(offers, req, elapsed)

            # DOM fallback
            logger.info("IndiGo: trying DOM extraction fallback")
            offers = await self._extract_from_dom(page, req)
            if offers:
                return self._build_response(offers, req, time.monotonic() - t0)
            return self._empty(req)

        except Exception as e:
            logger.error("IndiGo Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    async def _dismiss_cookies(self, page) -> None:
        # IndiGo has cookie notices and occasional modal dialogs — dismiss them
        for label in [
            "Accept", "Accept All", "Accept all", "I agree",
            "Got it", "OK", "Close", "Dismiss", "Agree",
            "Accept Cookies", "Accept cookies", "Continue",
        ]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        # Close dialog windows if any
        try:
            close_link = page.locator("a:has-text('Close this dialog window')")
            if await close_link.count() > 0:
                await close_link.first.click(timeout=2000)
                await asyncio.sleep(0.3)
        except Exception:
            pass
        # Close any close/X buttons on modals/overlays
        for close_sel in [
            "button[aria-label='Close'], button[aria-label='close']",
            "[class*='modal'] button[class*='close'], [class*='overlay'] button[class*='close']",
            "[class*='dialog'] button[class*='close']",
        ]:
            try:
                loc = page.locator(close_sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    return
            except Exception:
                continue
        # JS removal of cookie/consent/modal overlays
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="Cookie"], [id*="Cookie"], [class*="onetrust"], [id*="onetrust"], ' +
                    '[class*="modal-overlay"], [class*="popup"], [id*="popup"], ' +
                    '[class*="privacy"], [id*="privacy"], [class*="dialog"], ' +
                    '[class*="banner"], [class*="Banner"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        # IndiGo radio inputs are hidden; click the visible label/wrapper instead.
        # One Way is the default selection, but click it to be safe.

        # Strategy 1: Check if already selected via radio input
        for radio_sel in [
            "input#radio-input-triptype-oneWay",
            "input[value='oneWay'], input[value='one-way'], input[value='OW']",
            "input[name*='tripType'][value*='one' i]",
        ]:
            try:
                radio = page.locator(radio_sel)
                if await radio.count() > 0 and await radio.first.is_checked():
                    return
            except Exception:
                continue

        # Strategy 2: Click wrapper/label for radio input
        for wrapper_sel in [
            "label[for='radio-input-triptype-oneWay']",
            "input#radio-input-triptype-oneWay + *",
            "label[for*='oneWay'], label[for*='one-way'], label[for*='oneway']",
            "[data-testid*='oneWay'], [data-testid*='one-way']",
        ]:
            try:
                loc = page.locator(wrapper_sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click(timeout=3000)
                    return
            except Exception:
                continue

        # Strategy 3: Click by visible text
        for txt in ["One Way", "One way", "ONE WAY", "One-Way", "One-way"]:
            try:
                loc = page.get_by_text(txt, exact=True)
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click(timeout=3000)
                    return
            except Exception:
                continue

        # Strategy 4: Role-based radio button
        try:
            loc = page.get_by_role("radio", name=re.compile(r"one.?way", re.IGNORECASE))
            if await loc.count() > 0:
                await loc.first.click(timeout=3000, force=True)
                return
        except Exception:
            pass

        # Strategy 5: JS fallback — find and click one-way option
        try:
            await page.evaluate("""() => {
                const els = document.querySelectorAll(
                    'label, span, div, button, input, [role="radio"], [role="tab"]'
                );
                for (const el of els) {
                    const txt = (el.textContent || '').trim().toLowerCase();
                    if ((txt === 'one way' || txt === 'one-way' || txt === 'oneway') &&
                        el.offsetHeight > 0) {
                        el.click();
                        return;
                    }
                }
            }""")
        except Exception:
            pass

    async def _fill_airport_field(self, page, label: str, iata: str, index: int) -> bool:
        """
        Fill an airport field by clicking its container, typing the IATA code,
        and selecting from the suggestion dropdown.

        Uses multiple selector strategies for resilience against DOM changes.
        The IndiGo React SPA periodically changes class names, so we try
        text/placeholder/role-based selectors first, then CSS class patterns,
        then JS evaluation as a last resort.
        """
        try:
            # ── Step 1: Click the city selector container to reveal the input ──
            clicked = False

            # Build ordered selector lists per field
            if index == 0:
                css_selectors = [
                    "[data-testid*='origin'], [data-testid*='source'], [data-testid*='from']",
                    "[aria-label*='sourceCity'], [aria-label*='origin'], [aria-label*='Origin']",
                    "[class*='source'][class*='city'], [class*='origin'][class*='city']",
                    ".popover__wrapper.search-widget-form-body__from",
                    "[class*='search-widget'][class*='from'], [class*='searchWidget'][class*='from']",
                    "[class*='departure'][class*='city'], [class*='from'][class*='airport']",
                ]
                text_labels = ["From", "Flying from", "Origin", "Departure city", "Where from"]
                placeholder_patterns = [r"from", r"flying\s*from", r"origin", r"departure", r"where\s*from"]
            else:
                css_selectors = [
                    "[data-testid*='destination'], [data-testid*='dest'], [data-testid*='to']",
                    "[aria-label*='destinationCity'], [aria-label*='destination'], [aria-label*='Destination']",
                    "[class*='destination'][class*='city'], [class*='dest'][class*='city']",
                    ".popover__wrapper.search-widget-form-body__to",
                    "[class*='search-widget'][class*='to'], [class*='searchWidget'][class*='to']",
                    "[class*='arrival'][class*='city'], [class*='to'][class*='airport']",
                ]
                text_labels = ["To", "Going to", "Destination", "Arrival city", "Where to"]
                placeholder_patterns = [r"to", r"going\s*to", r"destination", r"arrival", r"where\s*to"]

            # Strategy A: CSS selectors
            for sel in css_selectors:
                try:
                    loc = page.locator(sel)
                    if await loc.count() > 0 and await loc.first.is_visible():
                        await loc.first.click(timeout=5000, no_wait_after=True)
                        clicked = True
                        break
                except Exception:
                    continue

            # Strategy B: Placeholder-based
            if not clicked:
                for pat in placeholder_patterns:
                    try:
                        loc = page.locator(f"input[placeholder]").filter(
                            has=page.locator(f":scope")
                        )
                        # Use get_by_placeholder for regex matching
                        loc = page.get_by_placeholder(re.compile(pat, re.IGNORECASE))
                        if await loc.count() > 0:
                            await loc.first.click(timeout=3000)
                            clicked = True
                            break
                    except Exception:
                        continue

            # Strategy C: Role-based textbox
            if not clicked:
                for txt in text_labels:
                    try:
                        loc = page.get_by_role("textbox", name=re.compile(rf".*{re.escape(txt)}.*", re.IGNORECASE))
                        if await loc.count() > 0 and await loc.first.is_visible():
                            await loc.first.click(timeout=3000)
                            clicked = True
                            break
                    except Exception:
                        continue

            # Strategy D: Text-based click on label/span
            if not clicked:
                for txt in text_labels:
                    try:
                        loc = page.get_by_text(txt, exact=True)
                        if await loc.count() > 0 and await loc.first.is_visible():
                            await loc.first.click(timeout=3000)
                            clicked = True
                            break
                    except Exception:
                        continue

            # Strategy E: JS evaluation — find visible clickable element by text
            if not clicked:
                js_labels = "|".join(t.lower() for t in text_labels)
                clicked = await page.evaluate(f"""() => {{
                    const labels = "{js_labels}".split("|");
                    const els = document.querySelectorAll(
                        'div, span, label, button, input, [role="combobox"], [role="textbox"]'
                    );
                    for (const el of els) {{
                        const txt = (el.textContent || el.placeholder || '').trim().toLowerCase();
                        if (labels.some(l => txt === l || txt.startsWith(l + '?') || txt.startsWith(l + ' ')) &&
                            el.offsetHeight > 0 && el.offsetWidth > 0) {{
                            el.click();
                            return true;
                        }}
                    }}
                    return false;
                }}""")

            if not clicked:
                logger.warning("IndiGo: could not find %s airport container", label)
                return False

            await asyncio.sleep(1.0)

            # ── Step 2: Find the visible input and type the IATA code ──
            inp = None

            # Try combobox role input first
            combo = page.locator("input[role='combobox']")
            for i in range(await combo.count()):
                try:
                    if await combo.nth(i).is_visible():
                        inp = combo.nth(i)
                        break
                except Exception:
                    continue

            # Try any visible text input in the search area
            if not inp:
                for sel in [
                    "input[type='text']:visible", "input[type='search']:visible",
                    "input:not([type]):visible",
                    "input[role='searchbox']",
                    "input[autocomplete]",
                ]:
                    try:
                        loc = page.locator(sel)
                        for i in range(await loc.count()):
                            if await loc.nth(i).is_visible() and await loc.nth(i).is_editable():
                                inp = loc.nth(i)
                                break
                        if inp:
                            break
                    except Exception:
                        continue

            # Last resort: any focused input
            if not inp:
                try:
                    inp = page.locator("input:focus")
                    if await inp.count() == 0:
                        inp = None
                except Exception:
                    pass

            if not inp:
                logger.warning("IndiGo: no visible input after clicking %s container", label)
                return False

            await inp.fill("")
            await asyncio.sleep(0.2)
            await inp.press_sequentially(iata, delay=80)
            await asyncio.sleep(1.5)

            # ── Step 3: Click the matching suggestion ──

            # Approach 1: Suggestion list items containing the IATA code
            for sugg_sel in [
                f".airport-search-list-item :text-is('{iata}')",
                f"[class*='airport'][class*='list'] :text-is('{iata}')",
                f"[class*='suggestion'] :text-is('{iata}')",
                f"[class*='dropdown'] :text-is('{iata}')",
                f"[class*='autocomplete'] :text-is('{iata}')",
                f"[role='listbox'] [role='option']:has-text('{iata}')",
                f"[role='listbox'] li:has-text('{iata}')",
                f"li:has-text('{iata}')",
            ]:
                try:
                    loc = page.locator(sugg_sel)
                    if await loc.count() > 0:
                        await loc.first.click(timeout=3000)
                        return True
                except Exception:
                    continue

            # Approach 2: Any visible div/span/li with exact IATA text
            exact_match = page.locator(
                f"div:text-is('{iata}'), span:text-is('{iata}'), "
                f"li:text-is('{iata}'), p:text-is('{iata}'), "
                f"strong:text-is('{iata}'), b:text-is('{iata}')"
            )
            for i in range(await exact_match.count()):
                el = exact_match.nth(i)
                try:
                    if await el.is_visible() and (await el.inner_text()).strip() == iata:
                        await el.click(timeout=3000)
                        return True
                except Exception:
                    continue

            # Approach 3: JS click on leaf text node matching IATA
            js_clicked = await page.evaluate(f"""() => {{
                const els = document.querySelectorAll('*');
                for (const el of els) {{
                    if (el.children.length === 0 &&
                        el.textContent.trim() === '{iata}' &&
                        el.offsetHeight > 0 &&
                        el.getBoundingClientRect().top > 0) {{
                        el.click();
                        return true;
                    }}
                }}
                return false;
            }}""")
            if js_clicked:
                return True

            # Approach 4: Keyboard selection (ArrowDown + Enter)
            await page.keyboard.press("ArrowDown")
            await asyncio.sleep(0.3)
            await page.keyboard.press("Enter")
            return True

        except Exception as e:
            logger.warning("IndiGo: %s airport field error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """
        IndiGo uses react-date-range or a custom calendar component.
        Click departure button → navigate months → click day.

        Uses multiple selector strategies for resilience.
        """
        target = req.date_from
        try:
            # ── Click departure date button to open calendar ──
            opened = False

            for sel in [
                "button[class*='departureDate']",
                ".popover__wrapper.search-widget-form-body__departure",
                "[aria-label*='departureDate'], [aria-label*='departure']",
                "[data-testid*='departure'], [data-testid*='date']",
                "[class*='departure'][class*='date'], [class*='depart'][class*='date']",
                "[class*='search-widget'][class*='departure'], [class*='searchWidget'][class*='departure']",
            ]:
                try:
                    loc = page.locator(sel)
                    if await loc.count() > 0 and await loc.first.is_visible():
                        await loc.first.click(timeout=5000)
                        opened = True
                        break
                except Exception:
                    continue

            if not opened:
                for txt in ["Departure", "Depart", "Travel date", "Date", "Select date"]:
                    try:
                        loc = page.get_by_text(txt, exact=False)
                        if await loc.count() > 0 and await loc.first.is_visible():
                            await loc.first.click(timeout=3000)
                            opened = True
                            break
                    except Exception:
                        continue

            if not opened:
                try:
                    loc = page.get_by_role("textbox", name=re.compile(r"depart|date|travel", re.IGNORECASE))
                    if await loc.count() > 0:
                        await loc.first.click(timeout=3000)
                        opened = True
                except Exception:
                    pass

            if not opened:
                logger.warning("IndiGo: could not open date picker")
                return False

            await asyncio.sleep(0.8)

            # ── Navigate to target month ──
            target_month_year = target.strftime("%B %Y")  # e.g. "April 2026"
            for _ in range(14):
                # Check month headers in various calendar implementations
                for header_sel in [
                    ".rdrMonthAndYearPickers", ".rdrMonthName",
                    "[class*='month'][class*='title']", "[class*='month'][class*='header']",
                    "[class*='calendar'][class*='header']", "[class*='Calendar'][class*='header']",
                    "[class*='monthYear'], [class*='month-year']",
                ]:
                    try:
                        header = page.locator(header_sel)
                        if await header.count() > 0:
                            text = await header.first.inner_text()
                            if target_month_year.lower() in text.lower():
                                break
                    except Exception:
                        continue
                else:
                    # Check all month name elements (multi-month view)
                    found_month = False
                    month_names = page.locator(
                        ".rdrMonthName, [class*='monthName'], [class*='month-name'], "
                        "[class*='calendarMonth'], [class*='calendar-month']"
                    )
                    for i in range(await month_names.count()):
                        try:
                            mn_text = await month_names.nth(i).inner_text()
                            if target_month_year.lower() in mn_text.lower():
                                found_month = True
                                break
                        except Exception:
                            continue

                    if not found_month:
                        # Click next month button
                        next_clicked = False
                        for nxt_sel in [
                            ".rdrNextButton",
                            "button[aria-label*='next' i]", "button[aria-label*='Next']",
                            "[class*='next'][class*='month']", "[class*='Next'][class*='Month']",
                            "[class*='calendar'] [class*='next']",
                            "[class*='arrow'][class*='right'], [class*='chevron'][class*='right']",
                        ]:
                            try:
                                nxt = page.locator(nxt_sel)
                                if await nxt.count() > 0 and await nxt.first.is_visible():
                                    await nxt.first.click(timeout=2000)
                                    next_clicked = True
                                    break
                            except Exception:
                                continue
                        if not next_clicked:
                            break
                        await asyncio.sleep(0.4)
                        continue
                    break
                break

            # ── Click the target day ──
            day_num = target.day
            day_name = target.strftime("%A")  # e.g. "Wednesday"
            month_name = target.strftime("%B")  # e.g. "March"

            # Approach 1: react-date-range aria-label format
            aria_label = f"{day_name}, {day_num} {month_name} {target.year}"
            day_el = page.locator(f"span[aria-label='{aria_label}'], button[aria-label='{aria_label}']")
            if await day_el.count() > 0:
                await asyncio.sleep(0.5)
                await day_el.first.click(timeout=5000, force=True)
                await asyncio.sleep(0.5)
                return True

            # Approach 2: Partial aria-label match
            day_el = page.locator(f"span[aria-label*='{day_num} {month_name} {target.year}'], button[aria-label*='{day_num} {month_name} {target.year}']")
            if await day_el.count() > 0:
                await asyncio.sleep(0.5)
                await day_el.first.click(timeout=5000, force=True)
                await asyncio.sleep(0.5)
                return True

            # Approach 3: US-style aria-label (Month Day, Year)
            aria_us = f"{month_name} {day_num}, {target.year}"
            day_el = page.locator(f"[aria-label='{aria_us}'], [aria-label*='{aria_us}']")
            if await day_el.count() > 0:
                await asyncio.sleep(0.5)
                await day_el.first.click(timeout=5000, force=True)
                await asyncio.sleep(0.5)
                return True

            # Approach 4: ISO date data attribute
            iso_date = target.strftime("%Y-%m-%d")
            for date_sel in [
                f"[data-date='{iso_date}']",
                f"[data-testid='{day_num}-{target.month}-{target.year}']",
                f"[data-value='{iso_date}']",
                f"[aria-label*='{iso_date}']",
            ]:
                try:
                    day_el = page.locator(date_sel)
                    if await day_el.count() > 0:
                        await day_el.first.click(timeout=3000, force=True)
                        await asyncio.sleep(0.5)
                        return True
                except Exception:
                    continue

            # Approach 5: react-date-range day button by number
            day_btns = page.locator(
                ".rdrDay:not(.rdrDayDisabled) .rdrDayNumber span, "
                "[class*='calendar'] [class*='day']:not([class*='disabled']) span, "
                "[class*='calendar'] td:not([class*='disabled']) button, "
                "[class*='calendar'] td:not([class*='disabled']) span"
            )
            for i in range(await day_btns.count()):
                btn = day_btns.nth(i)
                try:
                    txt = (await btn.inner_text()).strip()
                    if txt == str(day_num):
                        await btn.click(timeout=3000)
                        return True
                except Exception:
                    continue

            logger.warning("IndiGo: could not find day %s in calendar", day_num)
            return False
        except Exception as e:
            logger.warning("IndiGo: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        # IndiGo search button — try multiple strategies

        # Strategy 1: Enabled button with "Search" text
        try:
            search_btn = page.locator("button:has-text('Search'):not([disabled])")
            await search_btn.first.wait_for(state="visible", timeout=5000)
            await search_btn.first.click(timeout=5000)
            logger.info("IndiGo: clicked search")
            return
        except Exception:
            pass

        # Strategy 2: Role-based button matching search labels
        for label in ["Search", "SEARCH", "Search Flights", "Search flights", "Find flights", "Let's go"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000, force=True)
                    logger.info("IndiGo: clicked search (role)")
                    return
            except Exception:
                continue

        # Strategy 3: CSS class patterns
        for sel in [
            "button[class*='search'][class*='btn'], button[class*='Search'][class*='Btn']",
            "button[class*='search-button'], button[class*='searchButton']",
            "[data-testid*='search'][data-testid*='button']",
            "a[class*='search'][class*='btn'], a[class*='Search']",
        ]:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0 and await loc.first.is_visible():
                    await loc.first.click(timeout=5000)
                    logger.info("IndiGo: clicked search (css)")
                    return
            except Exception:
                continue

        # Strategy 4: Submit button
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
            logger.info("IndiGo: clicked search (submit)")
            return
        except Exception:
            pass

        # Strategy 5: Enter key
        await page.keyboard.press("Enter")

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.journeys || d.fares || d.availability)) return d;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
        return []

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        """
        Parse IndiGo v1/flight/search API response.
        Structure: data.trips[0].journeysAvailable[] → each journey has:
          - designator: {origin, destination, departure, arrival}
          - passengerFares[]: [{productClass, totalFareAmount, totalTax, ...}]
          - segments[]: [{designator: {...}, identifier: {identifier, carrierCode}}]
          - stops, flightType, journeyKey, segKey
        Currency at data.currencyCode (default INR).
        """
        if isinstance(data, list):
            data = {"trips": [{"journeysAvailable": data}]}
        booking_url = self._build_booking_url(req)
        currency = data.get("currencyCode") or req.currency
        offers: list[FlightOffer] = []

        trips = data.get("trips") or []
        if not trips:
            return offers
        trip = trips[0]
        journeys = trip.get("journeysAvailable") or []

        for journey in journeys:
            if journey.get("isSold"):
                continue
            offer = self._parse_journey(journey, req, booking_url, currency)
            if offer:
                offers.append(offer)
        return offers

    def _parse_journey(self, journey: dict, req: FlightSearchRequest, booking_url: str, currency: str) -> Optional[FlightOffer]:
        # Get cheapest fare from passengerFares (productClass R = economy regular)
        passenger_fares = journey.get("passengerFares") or []
        best_price = float("inf")
        for pf in passenger_fares:
            amt = pf.get("totalFareAmount")
            if amt is not None:
                try:
                    v = float(amt)
                    if 0 < v < best_price:
                        best_price = v
                except (TypeError, ValueError):
                    pass
        if best_price == float("inf"):
            return None

        # Build segments from journey.segments[]
        segments_raw = journey.get("segments") or []
        segments: list[FlightSegment] = []
        for seg in segments_raw:
            desig = seg.get("designator") or {}
            ident = seg.get("identifier") or {}
            segments.append(FlightSegment(
                airline=ident.get("carrierCode") or "6E",
                airline_name="IndiGo",
                flight_no=str(ident.get("identifier") or ""),
                origin=desig.get("origin") or req.origin,
                destination=desig.get("destination") or req.destination,
                departure=self._parse_dt(desig.get("departure") or ""),
                arrival=self._parse_dt(desig.get("arrival") or ""),
                cabin_class="M",
            ))
        if not segments:
            # Fallback: use journey-level designator
            desig = journey.get("designator") or {}
            segments.append(FlightSegment(
                airline="6E", airline_name="IndiGo", flight_no="",
                origin=desig.get("origin") or req.origin,
                destination=desig.get("destination") or req.destination,
                departure=self._parse_dt(desig.get("departure") or ""),
                arrival=self._parse_dt(desig.get("arrival") or ""),
                cabin_class="M",
            ))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = journey.get("journeyKey") or journey.get("segKey") or f"{time.monotonic()}"
        return FlightOffer(
            id=f"6e_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=currency,
            price_formatted=f"{best_price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["IndiGo"],
            owner_airline="6E",
            booking_url=booking_url,
            is_locked=False,
            source="indigo_direct",
            source_tier="free",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("IndiGo %s→%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"indigo{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
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
        dep = req.date_from.strftime("%d/%m/%Y")
        return (
            f"https://www.goindigo.in/flight-booking?origin={req.origin}"
            f"&destination={req.destination}&date={dep}&adults={req.adults}&tripType=O"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"indigo{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
