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

## Connectors (101 total)

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
| 1 | airasia | AK | [#19](https://github.com/LetsFG/LetsFG/issues/19) | `done` | copilot-main | 2026-03-13T17:00Z | headed Chrome + API interception |
| 2 | azul | AD | [#33](https://github.com/LetsFG/LetsFG/issues/33) | `done` | claude-connector-fix | 2026-03-13T21:00Z | Switched to headed Chrome + route interception. Akamai blocks headless; SPA sends empty criteria → rewrite via page.route(). Tested VCP→CNF (8), GRU→SSA (20), CNF→SSA (26). |
| 3 | batikair | ID | — | `done` | copilot-batikair-fix | 2026-03-13T18:11Z | nodriver CF bypass + PW DOM extraction, MYR |
| 4 | cebupacific | 5J | [#16](https://github.com/LetsFG/LetsFG/issues/16) | `done` | copilot-main | 2026-03-13T18:15Z | MCP-style Chrome flags bypass Akamai; SOAR API interception |
| 5 | easyjet | U2 | [#20](https://github.com/LetsFG/LetsFG/issues/20) | `done` | copilot-batikair-fix | 2026-03-13T21:11Z | Headed Chrome + form fill + response interception. Akamai blocks headless; fresh profile on 403. Tested LGW→BCN (4 offers). |
| 6 | eurowings | EW | — | `done` | copilot-eurowings-fix | 2026-03-13T20:51Z | cookie-farm hybrid: curl_cffi + CF cookies |
| 7 | flynas | XY | — | `done` | copilot-eurowings-fix | 2026-03-13T21:00Z | persistent headed Chrome, bypass Akamai headless detection |
| 8 | gol | G3 | [#34](https://github.com/LetsFG/LetsFG/issues/34) | `done` | claude-connector-fix | 2026-03-13T21:07Z | Switched to headed Chrome. Akamai blocked headless; UUID now populates via waitForFunction. Tested GRU->GIG (5), CGH->SDU (24). |
| 9 | indigo | 6E | [#17](https://github.com/LetsFG/LetsFG/issues/17) | `done` | claude-connector-fix | 2026-03-13T21:08Z | headed Chrome + city-selection selectors (73 offers DEL→BOM) |
| 10 | jet2 | LS | [#32](https://github.com/LetsFG/LetsFG/issues/32) | `done` | copilot-eurowings-fix | 2026-03-13T21:30Z | persistent headed Chrome for Akamai bypass |
| 11 | jetsmart | JA | — | `done` | copilot-batikair-fix | 2026-03-13T21:17Z | Already fixed — timetable API. Tested SCL→LIM (1 offer, 60790 CLP, 2.1s). |
| 12 | jetstar | JQ | [#31](https://github.com/LetsFG/LetsFG/issues/31) | `done` | copilot-eurowings-fix | 2026-03-14T00:00Z | CDP Chrome + Kasada warm-up bypass |
| 13 | lionair | JT | [#35](https://github.com/LetsFG/LetsFG/issues/35) | `removed` | copilot-main | 2026-03-14T12:00Z | booking.lionair.co.id connection refused (IBE2 dead), connector removed |
| 14 | luckyair | 8L | — | `done` | copilot-eurowings-fix | 2026-03-14T01:15Z | timeout enforcement + cleanup |
| 15 | nineair | AQ | — | `done` | copilot-eurowings-fix | 2026-03-14T01:25Z | timeout + cleanup (works CAN→HRB 5 offers) |
| 16 | norwegian | DY | [#22](https://github.com/LetsFG/LetsFG/issues/22) | `done` | copilot-batikair-fix | 2026-03-13T21:17Z | Headed CDP Chrome for Incapsula cookies; 20 offers OSL→LGW 1.1s cached |
| 17 | peach | MM | [#36](https://github.com/LetsFG/LetsFG/issues/36) | `done` | claude-connector-fix | 2026-03-13T21:42Z | Headed CDP Chrome (no headless) + disable-http2 + modal dismissal; reCAPTCHA auto-passes with real CDP Chrome |
| 18 | pegasus | PC | [#37](https://github.com/LetsFG/LetsFG/issues/37) | `done` | copilot-main | 2026-03-13T22:15Z | MCP-style Chrome flags bypass Akamai; direct booking URL + /pegasus/availability interception; 40 offers SAW→AYT |
| 19 | porter | — | [#24](https://github.com/LetsFG/LetsFG/issues/24) | `done` | copilot-eurowings-fix | 2026-03-14T01:30Z | MCP Chrome flags bypass Cloudflare; direct URL + DOM scrape; 6 flights 12 offers YTZ→YOW |
| 20 | scoot | TR | [#30](https://github.com/LetsFG/LetsFG/issues/30) | `done` | copilot-batikair-fix | 2026-03-14T00:30Z | Headed CDP Chrome for Akamai bypass; 6 offers SIN→BKK 167.63 SGD |
| 21 | volotea | V7 | [#18](https://github.com/LetsFG/LetsFG/issues/18) | `done` | copilot-batikair-fix | 2026-03-14T01:00Z | Headed CDP Chrome (Incapsula default-ctx bypass); fixed schedule JSON direction bug — reverse file has current data; null field guards |
| 22 | volaris | Y4 | [#21](https://github.com/LetsFG/LetsFG/issues/21) | `done` | copilot-eurowings-fix | 2026-03-14T02:00Z | headed Chrome + es-mx locale + v3 availability parser |

### Needs Fixing — API/Hybrid Connectors (5)

| # | Connector | IATA | Issue | Status | Claimed by | Timestamp | Commit/PR |
|---|-----------|------|-------|--------|------------|-----------|-----------|
| 23 | airarabia | G9 | — | `done` | copilot-eurowings-fix | 2026-03-14T02:30Z | relax date filter for monthly featured offers |
| 24 | jazeera | J9 | — | `done` | copilot-batikair-fix | 2026-03-14T03:00Z | Validated working — direct API, 5 routes tested, 3 fare classes KWI→DXB 25.70 KWD |
| 25 | jejuair | 7C | — | `done` | copilot-batikair-fix | 2026-03-14T03:00Z | Validated working — direct API, 4 routes tested, 7 offers GMP→CJU 21500 KRW |

### Blocked / Special (6)

| # | Connector | IATA | Issue | Status | Reason |
|---|-----------|------|-------|--------|--------|
| 26 | allegiant | G4 | [#38](https://github.com/LetsFG/LetsFG/issues/38) | `done` | **US IP required.** Headed Chrome + GraphQL interception. Set ALLEGIANT_PROXY if outside US. |
| 27 | southwest | WN | [#26](https://github.com/LetsFG/LetsFG/issues/26) | `done` | **US IP required.** Headed Chrome + Playwright form fill + API interception. Set SOUTHWEST_PROXY if outside US. |
| 28 | spirit | NK | [#28](https://github.com/LetsFG/LetsFG/issues/28) | `blocked` | PX Enterprise detects all automation (Playwright, patchright, stealth patches) even with US proxy. Token endpoint 403 → Angular app can't search. Tried: proxy+stealth, patchright Chromium, form fill+interception, direct API (Akamai WAF blocks). Code has proxy+patchright+stealth ready for when detection is bypassed (e.g. nodriver/residential proxy). — `copilot-eurowings-fix` |
| 29 | smartwings | QS | [#23](https://github.com/LetsFG/LetsFG/issues/23) | `done` | Headed CDP Chrome + CF Turnstile bypass (launch with URL before CDP attaches) |
| 30 | transavia | HV | [#25](https://github.com/LetsFG/LetsFG/issues/25) | `done` | Headed CDP Chrome to bypass Cloudflare WAF |
| 31 | wizzair | W6 | [#27](https://github.com/LetsFG/LetsFG/issues/27) | `done` | Fixed: launch Chrome headed (Kasada detects --headless=new) — `copilot-eurowings-fix` |

### Not Audited / Missing from Audit (7)

| # | Connector | IATA | Status | Claimed by | Timestamp | Commit/PR |
|---|-----------|------|--------|------------|-----------|-----------|
| 32 | kiwi | — | `done` | copilot-eurowings-fix | 2026-03-14T02:45Z | Validated working — GraphQL API, 50 offers STN→BCN 88 GBP, SDK in sync |
| 33 | play | OG | `blocked` | copilot-eurowings-fix | 2026-03-14T02:50Z | flyplay.com DNS offline — airline website shut down, connector returns empty |
| 34 | spicejet | SG | `done` | copilot-eurowings-fix | 2026-03-14T04:00Z | Carrier prefix fix + SDK sync, 4 offers DEL→BOM 5767 INR |
| 35 | twayair | TW | [#29](https://github.com/LetsFG/LetsFG/issues/29) | `done` | copilot-batikair-fix | 2026-03-14T04:00Z | Headed CDP Chrome for Akamai bypass; removed nodriver tier; default context cookie persistence; 1 offer GMP→CJU 50600 KRW, 1 offer ICN→NRT 17357 JPY |
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
| 43 | suncountry | SY | ULCC | `ok` | Lowfare API via Playwright (availability/search endpoint is unreachable). 81+ destinations from MSP hub. suncountry.com. |
| 44 | alaska | AS | Major | `done` | In-browser fetch to /search/api/flightresults API via Playwright. SvelteKit SPA, no WAF/CAPTCHA. 300+ routes (US, HI, MX, CA, CR). alaskaair.com. |
| 45 | hawaiian | HA | Major | `done` | Same Alaska Airlines Group infrastructure. In-browser fetch to /search/api/flightresults via Playwright. Inter-island, mainland-Hawaii, Asia/Pacific. hawaiianairlines.com. |
| 46 | american | AA | Major | `done` | CDP Chrome + form fill + ng-state extraction. World's largest airline, 350+ destinations in 60+ countries. aa.com — Angular 20 SSR/SPA with Akamai. |
| 47 | united | UA | Major | `ok` | copilot-eurowings-fix | 2026-03-14T15:00Z | Playwright + SSE via CDP. No manual UA (Akamai). 60 offers ORD→LAX. |
| 48 | delta | DL | Major | `done` | CDP Chrome + form fill + GraphQL API interception. #2 US carrier, ATL world's busiest hub. 20 offers ATL→LAX in ~30s. |
| 49 | jsx | XE | Premium | `blocked` | copilot-eurowings-fix | 2026-03-14T16:00Z | **US IP required.** ERR_HTTP2_PROTOCOL_ERROR from EU. Semi-private $100-400/seat, FBO terminals. jsx.com. |

### New India/Bangladesh → Middle East Connectors (4)

| # | Connector | IATA | Type | Status | Claimed by | Timestamp | Notes |
|---|-----------|------|------|--------|------------|-----------|-------|
| 50 | salamair | OV | API | `done` | claude-connector-builder | 2026-03-14T12:00Z | Pure httpx, api.salamair.com REST API. MCT hub → IN/BD/ME/AF. Tested MCT→SLL (8 offers 48.99 OMR), MCT→BOM (4 offers 124.94 OMR), MCT→DAC (4 offers 66.99 OMR), MCT→CGP (4 offers 132.40 OMR). |
| 51 | usbangla | BS | Browser | `done` | | | US-Bangla Airlines. Playwright form flow → Zenith FrontOffice DOM scraping. DAC hub → AE/OM/QA/SA/IN/MY/SG/TH/CN/MV/NP/DE/GB/US. |
| 52 | biman | BG | API | `done` | | | Biman Bangladesh Airlines. Pure httpx — Sabre DX GraphQL API at booking.biman-airlines.com/api/graphql with x-sabre-storefront: BGDX header. No browser/session/cookies needed. DAC hub → AE/SA/QA/KW/OM/IN/NP/TH/MY/SG/HK/CN/IT/GB/CA/PK. |
| 53 | gulfair | GF | — | `blocked` | | | Gulf Air. flights.gulfair.com protected by GeeTest CAPTCHA (visual puzzle). Angular SPA behind gt.js challenge. No usable direct API found. BAH hub → IN/BD/ME/EU. |

### Middle East & Global Premium Carriers (14)

| # | Connector | IATA | Type | Status | Claimed by | Timestamp | Notes |
|---|-----------|------|------|--------|------------|-----------|-------|
| 54 | emirates | EK | Browser | `done` | copilot-main | 2026-03-16T12:00Z | CDP Chrome + form fill + DOM scraping. Akamai WAF bypass via headed Chrome. 10 offers DXB→LHR (AED 2,155 cheapest). emirates.com/english/book/ — Next.js SPA with auto-suggest airports, DayPicker calendar. |
| 55 | qatar | QR | API | `done` | copilot-main | 2026-03-15T12:00Z | Direct API via CDP Chrome. `page.evaluate(fetch('/dapi/public/bff/web/flight-search/flight-offers'))` with `Accept-Language: en`. Homepage visit for Akamai cookies. 5 offers DOH→DXB, 15 offers DOH→LHR. |
| 56 | etihad | EY | `etihad.py` | `done` | | CDP Chrome + form fill + calendar pricing API interception | Etihad Airways. AUH hub → 70+ destinations. Calendar pricing via ada-services/bff-calendar-pricing. |
| 57 | saudia | SV | — | `broken` | | | Saudia (Saudi Arabian Airlines). JED/RUH hubs → 100+ destinations. Hajj/Umrah traffic + regional. saudia.com. |
| 58 | omanair | WY | — | `broken` | | | Oman Air. MCT hub → 50+ destinations (complements SalamAir). book.omanair.com — Incapsula protected. |
| 59 | kuwaitairways | KU | — | `broken` | | | Kuwait Airways. KWI hub → 50+ destinations. kuwaitairways.com. |
| 60 | royaljordanian | RJ | — | `broken` | | | Royal Jordanian. AMM hub → Levant/EU/US connectivity. rj.com. |
| 61 | turkish | TK | — | `done` | | | Turkish Airlines. IST hub, largest network by destination count (340+). turkishairlines.com. |
| 62 | singapore | SQ | — | `done` | CDP Chrome | SIN→LHR | Singapore Airlines. SIN hub, premium Asia-Pacific carrier. singaporeair.com. |
| 63 | cathay | CX | cathay_direct | `done` | curl_cffi | open-search calendar API | Cathay Pacific. HKG hub → Asia/EU/NA/AU. cathaypacific.com. curl_cffi-only via open-search API (no auth). 80 destinations from HKG, also SIN/SYD/TPE/BKK origins. Calendar deal pricing. |
| 64 | thai | TG | thai_direct | `done` | httpx | EveryMundo airTRFX | Thai Airways. BKK hub → Asia/EU/AU. thaiairways.com. httpx-only via EveryMundo fare pages (__NEXT_DATA__ StandardFareModule). 65 TG-operated airports, ~60 mapped slugs. |
| 65 | ana | NH | nh_direct | `done` | nodriver+Playwright | Akamai bypass hybrid | ANA (All Nippon Airways). NRT/HND hubs → Asia/EU/NA. ana.co.jp. nodriver+Playwright hybrid (Akamai Bot Manager bypass). |
| 66 | korean | KE | CDP Chrome | `live` | EveryMundo | airTRFX | Korean Air. ICN hub → Asia/EU/NA/AU. koreanair.com. CDP headed Chrome (WAF blocks httpx/headless). |
| 67 | malaysia | MH | malaysia_direct | `done` | httpx | lowFares+flightSearch | Malaysia Airlines. KUL hub → Asia/EU/AU. malaysiaairlines.com. httpx-only via lowFares GET (daily prices) + flightSearch JSON POST (booking URL). |

### New Global Carriers (6)

| # | Connector | IATA | Type | Status | Claimed by | Timestamp | Notes |
|---|-----------|------|------|--------|------------|-----------|-------|
| 68 | westjet | WS | API | `done` | copilot-main | 2026-03-15T12:00Z | CDP Chrome + Vue.js SPA at `/shop/` (trailing slash critical). API interception of `flight-search-api/v1`. 13 offers YYC→YVR in 11.6s. |
| 69 | lot | LO | API | `done` | copilot-main | 2026-03-15T12:00Z | Direct API via `page.evaluate(fetch('/api/v1/ibe/search/air-bounds'))` with Angular custom headers (language, market, channel, action, step, x-xsrf-token). 11 offers KRK→LHR $186.60+, 1 offer WAW→JFK $739.83. |
| 70 | latam | LA | API | `done` | copilot-main | 2026-03-13T21:00Z | Direct API connector. 50 offers tested. LATAM Airlines — SCL/GRU hubs → Americas/EU/AU. |
| 71 | copa | CM | API | `done` | copilot-main | 2026-03-13T21:00Z | Direct API connector. 9 offers tested. Copa Airlines — PTY hub → Americas. |
| 72 | avianca | AV | API | `done` | copilot-main | 2026-03-13T21:00Z | Direct API connector. 24 offers tested. Avianca — BOG hub → Americas/EU. |

### New Coverage Expansion Connectors — Airlines (24)

| # | Connector | IATA | Type | Status | Notes |
|---|-----------|------|------|--------|-------|
| 73 | aegean | A3 | API | `done` | Aegean Airlines. ATH hub → EU/ME. EveryMundo + calendar API. |
| 74 | icelandair | FI | API | `done` | Icelandair. KEF hub → transatlantic EU↔NA. Calendar API + EveryMundo. |
| 75 | aircanada | AC | API | `done` | Air Canada. YYZ/YVR/YUL hubs → 200+ destinations. Lowfare calendar API. |
| 76 | finnair | AY | API | `done` | Finnair. HEL hub → Nordic/Asia. NDC lowfare calendar API. |
| 77 | tap | TP | API | `done` | TAP Air Portugal. LIS hub → EU/Brazil/Africa. Lowfare API. |
| 78 | sas | SK | API | `done` | SAS Scandinavian. CPH/OSL/ARN hubs → 180+ destinations. Lowfare calendar API. |
| 79 | wingo | P5 | API | `done` | Wingo (Copa subsidiary). Colombia LCC. Navitaire API. |
| 80 | skyairline | H2 | API | `done` | Sky Airline Chile. Chile's largest LCC. Navitaire API. |
| 81 | arajet | DM | API | `done` | Arajet. SDQ hub → Caribbean/Americas ULCC. Radixx booking API. |
| 82 | ethiopian | ET | API | `done` | Ethiopian Airlines. ADD hub → Africa's largest. Calendar API + EveryMundo. |
| 83 | kenyaairways | KQ | API | `done` | Kenya Airways. NBO hub → East Africa. Calendar API. |
| 84 | royalairmaroc | AT | API | `done` | Royal Air Maroc. CMN hub → Africa gateway. Calendar API. |
| 85 | philippineairlines | PR | API | `done` | Philippine Airlines. MNL hub → Asia/ME/US. Calendar API. |
| 86 | airindia | AI | API | `blocked` | Air India (Tata). HTTP/2 stream resets on all endpoints, shadow DOM, no visible form inputs. |
| 87 | qantas | QF | API | `blocked` | Qantas. No accessible pricing API — market-pricing 403 via httpx/curl_cffi, CORS blocks page.evaluate. Route search only. |
| 88 | egyptair | MS | API | `blocked` | EgyptAir. Booking URL returns "Page Not Found", SharePoint-based site. |
| 89 | virginaustralia | VA | API | `done` | Virgin Australia. SYD/MEL/BNE → 110+ domestic + NZ/FJ/ID. Calendar API. |
| 90 | airnewzealand | NZ | API | `done` | Air New Zealand. AKL hub → NZ/Pacific/AU/Asia/US. Calendar API. |
| 91 | jal | JL | API | `blocked` | Japan Airlines. Traditional form POST to book-i.jal.co.jp, no JSON API. |
| 92 | garuda | GA | API | `blocked` | Garuda Indonesia. React SPA, no accessible flight search API. CORS blocks all booking endpoints. |
| 93 | bangkokairways | PG | API | `blocked` | Bangkok Airways. WAF protection (403), no visible form inputs. |
| 94 | saa | SA | API | `done` | South African Airways. JNB hub → Africa/EU/US. Calendar API. |
| 95 | aerlingus | EI | API | `done` | Aer Lingus. DUB hub → EU + transatlantic. Calendar API. |
| 96 | itaairways | AZ | API | `blocked` | ITA Airways. Fare teaser requires DecisionId session token, Cloudflare on all fare endpoints. |

### New Coverage Expansion Connectors — OTAs / Aggregators (5)

| # | Connector | Type | Status | Notes |
|---|-----------|------|--------|-------|
| 97 | serpapi_google | OTA | `done` | Google Flights via SerpAPI. 900+ airlines globally. Requires SERPAPI_KEY env var. |
| 98 | traveloka | OTA | `done` | Traveloka. SE Asia's #1 OTA. 100+ airlines, exclusive promotional fares. |
| 99 | cleartrip | OTA | `done` | Cleartrip (Flipkart). India's leading OTA. All Indian + intl airlines. |
| 100 | despegar | OTA | `done` | Despegar/Decolar. Latin America's #1 OTA. All LATAM airlines. |
| 101 | wego | OTA | `done` | Wego. Middle East/Asia metasearch. 700+ airlines, GCC focus. |

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
sys.path.insert(0, r"c:\Users\Adam\Desktop\folder\LetsFG\sdk\python\letsfg")
sys.path.insert(0, r"c:\Users\Adam\Desktop\folder\LetsFG")
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
