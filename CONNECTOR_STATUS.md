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
| 13 | lionair | JT | [#35](https://github.com/Boosted-Chat/BoostedTravel/issues/35) | `blocked` | copilot-main | 2026-03-13T22:00Z | IBE2 booking engine dead (0-byte responses), booking.lionair.co.id connection refused |
| 14 | luckyair | 8L | — | `claimed` | copilot-eurowings-fix | 2026-03-14T01:00Z | |
| 15 | nineair | AQ | — | `broken` | | | |
| 16 | norwegian | DY | [#22](https://github.com/Boosted-Chat/BoostedTravel/issues/22) | `done` | copilot-batikair-fix | 2026-03-13T21:17Z | Headed CDP Chrome for Incapsula cookies; 20 offers OSL→LGW 1.1s cached |
| 17 | peach | MM | [#36](https://github.com/Boosted-Chat/BoostedTravel/issues/36) | `claimed` | claude-connector-fix | 2026-03-13T21:42Z | |
| 18 | pegasus | PC | [#37](https://github.com/Boosted-Chat/BoostedTravel/issues/37) | `claimed` | copilot-main | 2026-03-13T22:15Z | |
| 19 | porter | — | [#24](https://github.com/Boosted-Chat/BoostedTravel/issues/24) | `broken` | | | |
| 20 | scoot | TR | [#30](https://github.com/Boosted-Chat/BoostedTravel/issues/30) | `done` | copilot-batikair-fix | 2026-03-14T00:30Z | Headed CDP Chrome for Akamai bypass; 6 offers SIN→BKK 167.63 SGD |
| 21 | volotea | V7 | [#18](https://github.com/Boosted-Chat/BoostedTravel/issues/18) | `broken` | | | |
| 22 | volaris | Y4 | [#21](https://github.com/Boosted-Chat/BoostedTravel/issues/21) | `broken` | | | |

### Needs Fixing — API/Hybrid Connectors (5)

| # | Connector | IATA | Issue | Status | Claimed by | Timestamp | Commit/PR |
|---|-----------|------|-------|--------|------------|-----------|-----------|
| 23 | airarabia | G9 | — | `broken` | | | |
| 24 | jazeera | J9 | — | `broken` | | | |
| 25 | jejuair | 7C | — | `broken` | | | |

### Blocked / Special (6)

| # | Connector | IATA | Issue | Status | Reason |
|---|-----------|------|-------|--------|--------|
| 26 | allegiant | G4 | [#38](https://github.com/Boosted-Chat/BoostedTravel/issues/38) | `blocked` | Requires US proxy (ALLEGIANT_PROXY env var) |
| 27 | southwest | WN | [#26](https://github.com/Boosted-Chat/BoostedTravel/issues/26) | `blocked` | API returns 500/403, needs US proxy |
| 28 | spirit | NK | [#28](https://github.com/Boosted-Chat/BoostedTravel/issues/28) | `blocked` | PerimeterX blocks all automated access |
| 29 | smartwings | QS | [#23](https://github.com/Boosted-Chat/BoostedTravel/issues/23) | `blocked` | Cloudflare challenge, needs stealth work |
| 30 | transavia | HV | [#25](https://github.com/Boosted-Chat/BoostedTravel/issues/25) | `blocked` | 403 on booking page |
| 31 | wizzair | W6 | [#27](https://github.com/Boosted-Chat/BoostedTravel/issues/27) | `blocked` | KPSDK challenge + 429 rate limit |

### Not Audited / Missing from Audit (7)

| # | Connector | IATA | Status | Claimed by | Timestamp | Commit/PR |
|---|-----------|------|--------|------------|-----------|-----------|
| 32 | kiwi | — | `broken` | | | |
| 33 | play | OG | `broken` | | | |
| 34 | spicejet | SG | `broken` | | | |
| 35 | twayair | TW | [#29](https://github.com/Boosted-Chat/BoostedTravel/issues/29) | `broken` | | |
| 36 | vietjet | VJ | `broken` | | | |
| 37 | vivaaerobus | VB | `broken` | | | |
| 38 | vueling | VY | `broken` | | | |
| 39 | zipair | ZG | `broken` | | | |

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
