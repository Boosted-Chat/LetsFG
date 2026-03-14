# Connector Fix Coordination Registry

> **PURPOSE**: Multiple Claude Code instances work on fixing connectors in parallel.
> Before starting work on a connector, **claim it** by writing your agent ID in the "Claimed by" column.
> After fixing, update status to `done` and add your PR/commit reference.
>
> **RULES**:
> 1. **Read this file before starting work** — don't pick one that's already claimed
> 2. **Write your agent ID + timestamp when you claim** a connector
> 3. **Only work on ONE connector at a time** — finish or release before taking another
> 4. If a connector is claimed but the agent hasn't updated in >30 min, you may steal it
> 5. After fixing: set status=`done`, add commit hash or PR link, push this file
> 6. **Always `git pull` before editing this file** to avoid merge conflicts

---

## Status Legend

| Status | Meaning |
|--------|---------|
| `working` | Already working in the audit — no fix needed |
| `done` | Fixed and committed |
| `claimed` | An agent is actively working on this |
| `broken` | Needs fixing, unclaimed — available to pick up |
| `blocked` | Needs external input (proxy, geo, site down) |
| `skip` | Not a real connector (utility/engine file) |

---

## Connectors (58 total)

### Already Working (20) — DO NOT TOUCH

| Connector | IATA | Type | Status | Notes |
|-----------|------|------|--------|-------|
| airbaltic | BT | API | `working` | Calendar fare API |
| airindiaexpress | IX | API | `working` | Low fare calendar API |
| airpeace | P4 | API | `working` | Crane IBE HTML |
| akasa | QP | Browser→API | `working` | Navitaire token+search |
| condor | DE | Hybrid | `working` | Cookie farm + curl_cffi |
| flair | F8 | API | `working` | NEXT_DATA extraction |
| flybondi | FO | Hybrid | `working` | curl_cffi SSR + Playwright |
| flydubai | FZ | Hybrid | `working` | Calendar API + Playwright |
| flysafair | FA | API | `working` | Sabre EzyCommerce |
| frontier | F9 | Hybrid | `working` | curl_cffi SSR + Playwright |
| nokair | DD | API | `working` | Sabre EzyCommerce |
| ryanair | FR | API | `working` | Public REST API |
| spring | 9C | API | `working` | Direct httpx API |
| sunexpress | XQ | Browser | `done` | Fixed 2026-03-13: persistent headed Chrome, bypass Radware |

### Needs Fixing — Browser Connectors (25)

| # | Connector | IATA | Issue | Status | Claimed by | Timestamp | Commit/PR |
|---|-----------|------|-------|--------|------------|-----------|-----------|
| 1 | airasia | AK | [#19](https://github.com/Boosted-Chat/BoostedTravel/issues/19) | `done` | copilot-main | 2026-03-13T17:00Z | headed Chrome + API interception |
| 2 | azul | AD | [#33](https://github.com/Boosted-Chat/BoostedTravel/issues/33) | `done` | claude-connector-fix | 2026-03-13T21:00Z | Switched to headed Chrome + route interception. Akamai blocks headless; SPA sends empty criteria → rewrite via page.route(). Tested VCP→CNF (8), GRU→SSA (20), CNF→SSA (26). |
| 3 | batikair | ID | — | `done` | copilot-batikair-fix | 2026-03-13T18:11Z | nodriver CF bypass + PW DOM extraction, MYR |
| 4 | cebupacific | 5J | [#16](https://github.com/Boosted-Chat/BoostedTravel/issues/16) | `done` | copilot-main | 2026-03-13T18:15Z | MCP-style Chrome flags bypass Akamai; SOAR API interception |
| 5 | easyjet | U2 | [#20](https://github.com/Boosted-Chat/BoostedTravel/issues/20) | `done` | copilot-batikair-fix | 2026-03-13T21:11Z | Headed Chrome + form fill + response interception. Akamai blocks headless; fresh profile on 403. Tested LGW→BCN (4 offers). |
| 6 | eurowings | EW | — | `done` | copilot-eurowings-fix | 2026-03-13T20:51Z | cookie-farm hybrid: curl_cffi + CF cookies |
| 7 | flynas | XY | — | `done` | copilot-eurowings-fix | 2026-03-13T21:00Z | persistent headed Chrome, bypass Akamai headless detection |
| 8 | gol | G3 | [#34](https://github.com/Boosted-Chat/BoostedTravel/issues/34) | `done` | claude-connector-fix | 2026-03-13T21:07Z | Switched to headed Chrome. Akamai blocked headless; UUID now populates via waitForFunction. Tested GRU->GIG (5), CGH->SDU (24). |
| 9 | indigo | 6E | [#17](https://github.com/Boosted-Chat/BoostedTravel/issues/17) | `done` | claude-connector-fix | 2026-03-13T21:08Z | headed Chrome + city-selection selectors (73 offers DEL→BOM) |
| 10 | jet2 | LS | [#32](https://github.com/Boosted-Chat/BoostedTravel/issues/32) | `done` | copilot-eurowings-fix | 2026-03-13T21:30Z | persistent headed Chrome for Akamai bypass |
| 11 | jetsmart | JA | — | `done` | copilot-batikair-fix | 2026-03-13T21:17Z | Already fixed — timetable API. Tested SCL→LIM (1 offer, 60790 CLP, 2.1s). |
| 12 | jetstar | JQ | [#31](https://github.com/Boosted-Chat/BoostedTravel/issues/31) | `done` | copilot-eurowings-fix | 2026-03-14T00:00Z | CDP Chrome + Kasada warm-up bypass |
| 13 | lionair | JT | [#35](https://github.com/Boosted-Chat/BoostedTravel/issues/35) | `removed` | copilot-main | 2026-03-14T12:00Z | booking.lionair.co.id connection refused (IBE2 dead), connector removed |
| 14 | luckyair | 8L | — | `done` | copilot-eurowings-fix | 2026-03-14T01:15Z | timeout enforcement + cleanup |
| 15 | nineair | AQ | — | `done` | copilot-eurowings-fix | 2026-03-14T01:25Z | timeout + cleanup (works CAN→HRB 5 offers) |
| 16 | norwegian | DY | [#22](https://github.com/Boosted-Chat/BoostedTravel/issues/22) | `done` | copilot-batikair-fix | 2026-03-13T21:17Z | Headed CDP Chrome for Incapsula cookies; 20 offers OSL→LGW 1.1s cached |
| 17 | peach | MM | [#36](https://github.com/Boosted-Chat/BoostedTravel/issues/36) | `done` | claude-connector-fix | 2026-03-13T21:42Z | Headed CDP Chrome (no headless) + disable-http2 + modal dismissal; reCAPTCHA auto-passes with real CDP Chrome |
| 18 | pegasus | PC | [#37](https://github.com/Boosted-Chat/BoostedTravel/issues/37) | `done` | copilot-main | 2026-03-13T22:15Z | MCP-style Chrome flags bypass Akamai; direct booking URL + /pegasus/availability interception; 40 offers SAW→AYT |
| 19 | porter | — | [#24](https://github.com/Boosted-Chat/BoostedTravel/issues/24) | `done` | copilot-eurowings-fix | 2026-03-14T01:30Z | MCP Chrome flags bypass Cloudflare; direct URL + DOM scrape; 6 flights 12 offers YTZ→YOW |
| 20 | scoot | TR | [#30](https://github.com/Boosted-Chat/BoostedTravel/issues/30) | `done` | copilot-batikair-fix | 2026-03-14T00:30Z | Headed CDP Chrome for Akamai bypass; 6 offers SIN→BKK 167.63 SGD |
| 21 | volotea | V7 | [#18](https://github.com/Boosted-Chat/BoostedTravel/issues/18) | `done` | copilot-batikair-fix | 2026-03-14T01:00Z | Headed CDP Chrome (Incapsula default-ctx bypass); fixed schedule JSON direction bug — reverse file has current data; null field guards |
| 22 | volaris | Y4 | [#21](https://github.com/Boosted-Chat/BoostedTravel/issues/21) | `done` | copilot-eurowings-fix | 2026-03-14T02:00Z | headed Chrome + es-mx locale + v3 availability parser |

### Needs Fixing — API/Hybrid Connectors (5)

| # | Connector | IATA | Issue | Status | Claimed by | Timestamp | Commit/PR |
|---|-----------|------|-------|--------|------------|-----------|-----------|
| 23 | airarabia | G9 | — | `done` | copilot-eurowings-fix | 2026-03-14T02:30Z | relax date filter for monthly featured offers |
| 24 | jazeera | J9 | — | `done` | copilot-batikair-fix | 2026-03-14T03:00Z | Validated working — direct API, 5 routes tested, 3 fare classes KWI→DXB 25.70 KWD |
| 25 | jejuair | 7C | — | `done` | copilot-batikair-fix | 2026-03-14T03:00Z | Validated working — direct API, 4 routes tested, 7 offers GMP→CJU 21500 KRW |

### Blocked / Special (6)

| # | Connector | IATA | Issue | Status | Reason |
|---|-----------|------|-------|--------|--------|
| 26 | allegiant | G4 | [#38](https://github.com/Boosted-Chat/BoostedTravel/issues/38) | `done` | **US IP required.** Headed Chrome + GraphQL interception. Set ALLEGIANT_PROXY if outside US. |
| 27 | southwest | WN | [#26](https://github.com/Boosted-Chat/BoostedTravel/issues/26) | `done` | **US IP required.** Headed Chrome + Playwright form fill + API interception. Set SOUTHWEST_PROXY if outside US. |
| 28 | spirit | NK | [#28](https://github.com/Boosted-Chat/BoostedTravel/issues/28) | `blocked` | PX Enterprise detects all automation (Playwright, patchright, stealth patches) even with US proxy. Token endpoint 403 → Angular app can't search. Tried: proxy+stealth, patchright Chromium, form fill+interception, direct API (Akamai WAF blocks). Code has proxy+patchright+stealth ready for when detection is bypassed (e.g. nodriver/residential proxy). — `copilot-eurowings-fix` |
| 29 | smartwings | QS | [#23](https://github.com/Boosted-Chat/BoostedTravel/issues/23) | `done` | Headed CDP Chrome + CF Turnstile bypass (launch with URL before CDP attaches) |
| 30 | transavia | HV | [#25](https://github.com/Boosted-Chat/BoostedTravel/issues/25) | `done` | Headed CDP Chrome to bypass Cloudflare WAF |
| 31 | wizzair | W6 | [#27](https://github.com/Boosted-Chat/BoostedTravel/issues/27) | `done` | Fixed: launch Chrome headed (Kasada detects --headless=new) — `copilot-eurowings-fix` |

### Not Audited / Missing from Audit (7)

| # | Connector | IATA | Status | Claimed by | Timestamp | Commit/PR |
|---|-----------|------|--------|------------|-----------|-----------|
| 32 | kiwi | — | `done` | copilot-eurowings-fix | 2026-03-14T02:45Z | Validated working — GraphQL API, 50 offers STN→BCN 88 GBP, SDK in sync |
| 33 | play | OG | `blocked` | copilot-eurowings-fix | 2026-03-14T02:50Z | flyplay.com DNS offline — airline website shut down, connector returns empty |
| 34 | spicejet | SG | `done` | copilot-eurowings-fix | 2026-03-14T04:00Z | Carrier prefix fix + SDK sync, 4 offers DEL→BOM 5767 INR |
| 35 | twayair | TW | [#29](https://github.com/Boosted-Chat/BoostedTravel/issues/29) | `done` | copilot-batikair-fix | 2026-03-14T04:00Z | Headed CDP Chrome for Akamai bypass; removed nodriver tier; default context cookie persistence; 1 offer GMP→CJU 50600 KRW, 1 offer ICN→NRT 17357 JPY |
| 36 | vietjet | VJ | `done` | copilot-batikair-fix | 2026-03-14T04:00Z | Validated working — direct API, 25 offers SGN→HAN |
| 37 | vivaaerobus | VB | `done` | copilot-batikair-fix | 2026-03-14T04:00Z | Validated working — 7 offers MEX→CUN 1082.95 MXN |
| 38 | vueling | VY | `done` | copilot-batikair-fix | 2026-03-14T04:00Z | Validated working — 6 offers BCN→FCO 45.90 EUR |
| 39 | zipair | ZG | `done` | copilot-batikair-fix | 2026-03-14T04:00Z | Validated working — 2 offers NRT→ICN 24160 JPY |

### New US Market Connectors (10)

| # | Connector | IATA | Type | Status | Reason |
|---|-----------|------|------|--------|--------|
| 40 | jetblue | B6 | LCC | `done` | bestFares calendar API (no auth, ~2s), Playwright fallback for details. 114+ destinations. |
| 41 | breeze | MX | LCC | `done` | Playwright API interception (Navitaire NewSkies GraphQL). 75+ US routes. flybreeze.com. |
| 42 | avelo | XP | ULCC | `done` | Playwright deep-link scraper. 42 US domestic routes. Deep link URL bypasses form fill. Blazor WASM app. |
| 43 | suncountry | SY | ULCC | `broken` | **US IP required.** 81+ destinations from MSP hub, best low-cost carrier NA. suncountry.com. |
| 44 | alaska | AS | Major | `broken` | **US IP required.** West Coast dominant + Hawaii monopoly (post-Hawaiian merger). alaskaair.com. |
| 45 | hawaiian | HA | Major | `broken` | **US IP required.** Hawaii inter-island + mainland-Hawaii + Asia/Pacific. hawaiianairlines.com. |
| 46 | american | AA | Major | `broken` | **US IP required.** Largest airline worldwide, every US city pair. aa.com — complex SPA. |
| 47 | united | UA | Major | `broken` | **US IP required.** 2nd largest US carrier, massive hub network. united.com — complex SPA. |
| 48 | delta | DL | Major | `broken` | **US IP required.** 3rd largest US carrier, ATL world's busiest hub. delta.com — complex SPA. |
| 49 | jsx | XE | Premium | `broken` | **US IP required.** Semi-private $100-400/seat, FBO terminals (no TSA). jsx.com. |

### New India/Bangladesh → Middle East Connectors (4)

| # | Connector | IATA | Type | Status | Claimed by | Timestamp | Notes |
|---|-----------|------|------|--------|------------|-----------|-------|
| 50 | salamair | OV | API | `done` | claude-connector-builder | 2026-03-14T12:00Z | Pure httpx, api.salamair.com REST API. MCT hub → IN/BD/ME/AF. Tested MCT→SLL (8 offers 48.99 OMR), MCT→BOM (4 offers 124.94 OMR), MCT→DAC (4 offers 66.99 OMR), MCT→CGP (4 offers 132.40 OMR). |
| 51 | usbangla | BS | Browser | `done` | | | US-Bangla Airlines. Playwright form flow → Zenith FrontOffice DOM scraping. DAC hub → AE/OM/QA/SA/IN/MY/SG/TH/CN/MV/NP/DE/GB/US. |
| 52 | biman | BG | API | `done` | | | Biman Bangladesh Airlines. Pure httpx — Sabre DX GraphQL API at booking.biman-airlines.com/api/graphql with x-sabre-storefront: BGDX header. No browser/session/cookies needed. DAC hub → AE/SA/QA/KW/OM/IN/NP/TH/MY/SG/HK/CN/IT/GB/CA/PK. |
| 53 | gulfair | GF | — | `blocked` | | | Gulf Air. flights.gulfair.com protected by GeeTest CAPTCHA (visual puzzle). Angular SPA behind gt.js challenge. No usable direct API found. BAH hub → IN/BD/ME/EU. |

---

## How to Claim a Connector

```markdown
<!-- Replace the empty cells with your info -->
| 5 | easyjet | U2 | #20 | `claimed` | claude-easyjet-fix | 2026-03-13T15:30Z | |
```

After fixing:
```markdown
| 5 | easyjet | U2 | #20 | `done` | claude-easyjet-fix | 2026-03-13T15:30Z | e3921e1 |
```

## Common Patterns Learned

### Radware Bot Manager (SunExpress, possibly others)
- **Symptom**: Page redirects to `validate.perfdrive.com`
- **Fix**: Use `launch_persistent_context(headless=False, channel="chrome")` instead of CDP headless
- **Key**: Real headed Chrome with persistent user-data-dir at off-screen position (-2400,-2400)

### visibility:hidden gridcells (SunExpress calendar)
- **Symptom**: `get_by_role("gridcell", name=...)` returns 0 even though DOM has the element
- **Fix**: Use CSS attribute selector `[role="gridcell"][aria-label="..."]` + `force=True` or JS click

### Angular combobox typing
- **Symptom**: `.fill()` closes dropdown, Angular doesn't detect change
- **Fix**: Use `.press_sequentially(text, delay=80)` for character-by-character typing

### Form auto-submit
- **Symptom**: After date selection + Escape, form auto-navigates to results
- **Fix**: Check URL before trying to click Search button; wait for results URL first

### Test script template
```python
import sys, asyncio, logging
sys.path.insert(0, r"c:\Users\Adam\Desktop\folder\BoostedTravel-public\sdk\python\boostedtravel")
sys.path.insert(0, r"c:\Users\Adam\Desktop\folder\BoostedTravel-public")
logging.basicConfig(level=logging.INFO, format="%(name)s %(levelname)s: %(message)s")
from datetime import date
from models.flights import FlightSearchRequest
from connectors.XXXXX import XXXXXConnectorClient

async def main():
    req = FlightSearchRequest(origin="XXX", destination="YYY", date_from=date(2026, 4, 15), adults=1, currency="GBP")
    client = XXXXXConnectorClient(timeout=60.0)
    resp = await client.search_flights(req)
    print(f"Results: {resp.total_results} offers")
    for i, o in enumerate(resp.offers[:10], 1):
        seg = o.routes[0].segments[0]
        print(f"  {i}. {seg.departure.strftime('%H:%M')} -> {seg.arrival.strftime('%H:%M')} | {o.routes[0].total_duration_seconds//3600}h{(o.routes[0].total_duration_seconds%3600)//60}m | {o.routes[0].stopovers} stop(s) | {o.price} {o.currency}")

asyncio.run(main())
```
