<table>
<tr>
<td width="140">
<img src="assets/logo.png" alt="LetsFG" width="120">
</td>
<td>

# LetsFG🔥🚀✈️ — agent-native travel search.

### Flights & hotels $50 cheaper in 5 seconds. Native to AI agents.

</td>
</tr>
</table>

LetsFG finds the cheapest flights across the entire internet — 102 airline connectors firing in parallel + enterprise GDS sources (Amadeus, Duffel, Sabre, Travelport) — and returns results in ~5 seconds. No web scraping wait times, no browser tabs, no inflated prices. Just raw airline prices, zero markup.

Native to **OpenClaw**, **Perplexity Computer**, **Manus**, **Claude Code**, **Codex**, **Cursor**, **Windsurf** — any AI agent that supports CLI, MCP, or packages.

> ⭐ **Search is free. Booking costs the ticket price only — zero markup.** Star → register → get unlimited search access forever. No trial, no catch. Booking charges the raw airline ticket price via Stripe (we add zero margin).

[![GitHub stars](https://img.shields.io/github/stars/LetsFG/LetsFG?style=social)](https://github.com/LetsFG/LetsFG)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/letsfg)](https://pypi.org/project/letsfg/)
[![npm](https://img.shields.io/npm/v/letsfg-mcp?label=npm%20%28MCP%29)](https://www.npmjs.com/package/letsfg-mcp)
[![npm](https://img.shields.io/npm/v/letsfg?label=npm%20%28JS%20SDK%29)](https://www.npmjs.com/package/letsfg)
[![Smithery](https://smithery.ai/badge/letsfg-mcp)](https://smithery.ai/server/letsfg-mcp)

## Demo: LetsFG vs Default Agent Search

<div align="center">
  <img src="assets/demo.gif" alt="Demo: LetsFG vs Default Agent Search" width="640">
</div>

> Side-by-side comparison: default agent search (OpenClaw, Perplexity Computer) vs LetsFG CLI. Same query — LetsFG finds cheaper flights across 102 airlines in seconds.

## Why LetsFG?

Flight websites inflate prices with demand tracking, cookie-based pricing, and surge markup. The same flight is often **$20–$50 cheaper** through LetsFG — raw airline price, zero markup.

LetsFG works by finding the best price across the entire internet. It fires 102 airline connectors in parallel, scanning carriers across Europe, Asia, Americas, Middle East, Africa, and Oceania — then merges results with enterprise GDS/NDC sources (Amadeus, Duffel, Sabre, Travelport) that provide competitive pricing from 400+ carriers including premium airlines like Lufthansa, British Airways, and Emirates. The best price wins.

| | Google Flights / Booking.com / Expedia | **LetsFG** |
|---|---|---|
| Search speed | 30s+ (loading, ads, redirects) | **~10 seconds** |
| Search | Free (with tracking/inflation) | **Free** (no tracking, no cookies) |
| Booking | Ticket + hidden markup | **Ticket price only** (raw airline price, zero markup) |
| Price goes up on repeat search? | Yes (demand tracking) | **Never** |
| LCC coverage | Missing many low-cost carriers | **102 direct airline connectors** |
| Works inside AI agents? | No | **Native** (CLI, MCP, SDK) |

---

## One-Click Install

```bash
pip install letsfg
```

That's it. You can search flights immediately — no account, no API key, no configuration:

```bash
letsfg search-local GDN BCN 2026-06-15
```

This runs 102 airline connectors locally on your machine and returns real-time prices. Completely free, unlimited, zero setup.

---

## Star History

<a href="https://www.star-history.com/?repos=LetsFG%2FLetsFG&type=date&legend=top-left">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/image?repos=LetsFG/LetsFG&type=date&theme=dark&legend=top-left" />
    <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/image?repos=LetsFG/LetsFG&type=date&legend=top-left" />
    <img alt="Star History Chart" src="https://api.star-history.com/image?repos=LetsFG/LetsFG&type=date&legend=top-left" />
  </picture>
</a>

---

## Two Ways to Use LetsFG

### Option A: Local Only (Free, No API Key)

Install and search. One command, zero configuration.

```bash
pip install letsfg
letsfg search-local LHR BCN 2026-04-15
```

**What you get:**
- 102 airline connectors running on your machine (Ryanair, Wizz Air, EasyJet, Southwest, AirAsia, Norwegian, Qantas, and 95 more)
- Real-time prices scraped directly from airline websites
- Virtual interlining — cross-airline round-trips that save 30–50%
- Completely free, unlimited searches

```python
from letsfg.local import search_local

result = await search_local("GDN", "BCN", "2026-06-15")
for offer in result.offers[:5]:
    print(f"{offer.airlines[0]}: {offer.currency} {offer.price}")
```

### Option B: With API Key (Recommended — Much Better Coverage)

One extra command unlocks the full power of LetsFG:

```bash
pip install letsfg
letsfg register --name my-agent --email you@example.com
# → Returns: trav_xxxxx... (your API key)
export LETSFG_API_KEY=trav_...

letsfg search LHR JFK 2026-04-15
```

**What you get (in addition to everything in Option A):**
- **Enterprise GDS/NDC providers** — Amadeus, Duffel, Sabre, Travelport, Kiwi. These are contract-only data sources that normally require enterprise agreements worth $50k+/year. LetsFG is contracted with these providers and makes their inventory available to every user.
- **400+ full-service airlines** — Lufthansa, British Airways, Emirates, Singapore Airlines, ANA, Cathay Pacific, and hundreds more that don't have public APIs
- **Competitive pricing** — the backend aggregates offers from multiple GDS sources and picks the cheapest for each route
- **Unlock & book** — confirm live prices and create real airline PNRs with e-tickets
- Both local connectors AND cloud sources run simultaneously — results merged and deduplicated automatically

**Registration is instant, free, and handled by CLI** — an AI agent can do it in one command. The API key connects you to our closed-source backend service which maintains enterprise contracts with GDS/NDC providers and premium carriers.

> ⭐ **Star this repo and register — unlimited search, zero-markup booking.** First 1,000 stars only.

```python
from letsfg import LetsFG

bt = LetsFG()  # reads LETSFG_API_KEY from env
flights = bt.search("LHR", "JFK", "2026-04-15")
print(f"{flights.total_results} offers, cheapest: {flights.cheapest.summary()}")
```

---

## Quick Start (Full Flow)

```bash
pip install letsfg

# Register and get API key (free, instant)
letsfg register --name my-agent --email you@example.com
export LETSFG_API_KEY=trav_...

# Search (free, unlimited)
letsfg search LHR JFK 2026-04-15
letsfg search LON BCN 2026-04-01 --return 2026-04-08 --cabin M --sort price

# Unlock (confirms live price, reserves for 30 min)
letsfg unlock off_xxx

# Book
letsfg book off_xxx \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --email john.doe@example.com
```

All commands support `--json` for machine-readable output:

```bash
letsfg search GDN BER 2026-03-03 --json | jq '.offers[0]'
```

## Install

### Python (recommended — includes 102 local airline connectors)

```bash
pip install letsfg
playwright install chromium  # needed for browser-based connectors
```

### JavaScript / TypeScript (API client only)

```bash
npm install -g letsfg
```

### MCP Server (Claude Desktop / Cursor / Windsurf / OpenClaw)

```bash
npx letsfg-mcp
```

Add to your MCP config:

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "npx",
      "args": ["-y", "letsfg-mcp"],
      "env": {
        "LETSFG_API_KEY": "trav_your_api_key"
      }
    }
  }
}
```

> **Note:** `LETSFG_API_KEY` is optional. Without it, the MCP server still runs all 102 local connectors. With it, you also get enterprise GDS/NDC sources (400+ more airlines).

### Python SDK

```python
from letsfg import LetsFG

bt = LetsFG(api_key="trav_...")
flights = bt.search("LHR", "JFK", "2026-04-15")
print(f"{flights.total_results} offers, cheapest: {flights.cheapest.summary()}")

unlocked = bt.unlock(flights.offers[0].id)
booking = bt.book(
    offer_id=unlocked.offer_id,
    passengers=[{"id": "pas_0", "given_name": "John", "family_name": "Doe", "born_on": "1990-01-15", "gender": "m", "title": "mr"}],
    contact_email="john.doe@example.com",
)
print(f"Booked! PNR: {booking.booking_reference}")
```

### JS SDK

```typescript
import { LetsFG } from 'letsfg';

const bt = new LetsFG({ apiKey: 'trav_...' });
const flights = await bt.search('LHR', 'JFK', '2026-04-15');
console.log(`${flights.totalResults} offers`);
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `letsfg register` | Get your API key |
| `letsfg search <origin> <dest> <date>` | Search flights (free) |
| `letsfg locations <query>` | Resolve city/airport to IATA codes |
| `letsfg unlock <offer_id>` | Confirm live price & reserve for 30 min |
| `letsfg book <offer_id>` | Book the flight |
| `letsfg system-info` | Show system resources & concurrency tier |
| `letsfg me` | View profile & usage stats |

All commands accept `--json` for structured output and `--api-key` to override the env variable.

## How It Works

1. **Search** (free) — returns offers with full details: price, airlines, duration, stopovers, conditions
2. **Unlock** — confirms live price with the airline, reserves for 30 minutes
3. **Book** — creates real airline PNR, e-ticket sent to passenger email

### Two Search Modes

| Mode | What it does | Speed | Auth |
|------|-------------|-------|------|
| **Cloud search** | Queries GDS/NDC providers (Duffel, Amadeus, Sabre, Travelport, Kiwi) via backend API | 2-15s | API key |
| **Local search** | Fires 102 airline connectors on your machine via Playwright + httpx | 5-25s | None |

Both modes run simultaneously by default. Results are merged, deduplicated, currency-normalized, and sorted.

### Virtual Interlining

The combo engine builds cross-airline round-trips by combining one-way fares from different carriers. A Ryanair outbound + Wizz Air return can save 30-50% vs booking a round-trip on either airline alone.

### City-Wide Airport Expansion

Search a city code and LetsFG automatically searches all airports in that city. `LON` expands to LHR, LGW, STN, LTN, SEN, LCY. `NYC` expands to JFK, EWR, LGA. Works for 25+ major cities worldwide — one search covers every airport.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  AI Agents / CLI / SDK / MCP Server                 │
├──────────────────┬──────────────────────────────────┤
│  Local connectors │  Enterprise Cloud API            │
│  (102 airlines via│  (Amadeus, Duffel, Sabre,        │
│   Playwright)     │   Travelport, Kiwi — contract-   │
│                   │   only GDS/NDC providers)        │
├──────────────────┴──────────────────────────────────┤
│            Merge + Dedup + Combo Engine              │
│            (virtual interlining, currency norm)      │
└─────────────────────────────────────────────────────┘
```

## Local Airline Connectors (102 airlines)

The Python SDK includes 102 production-grade airline connectors — not fragile scrapers, but maintained integrations that handle each airline's specific API pattern. No API key needed for local search. Each connector uses one of three proven strategies:

| Strategy | How it works | Example airlines |
|----------|-------------|-----------------|
| **Direct API** | Reverse-engineered REST/GraphQL endpoints via `httpx`/`curl_cffi` | Ryanair, Wizz Air, Norwegian, Akasa |
| **CDP Chrome** | Real Chrome + Playwright CDP for sites with bot detection | EasyJet, Southwest, Pegasus |
| **API Interception** | Playwright page navigation + response interception | VietJet, Cebu Pacific, Lion Air |

### Supported Airlines

<details>
<summary>Full list of 102 airline connectors</summary>

| Region | Airlines |
|--------|----------|
| **Europe** | Ryanair, Wizz Air, EasyJet, Norwegian, Vueling, Eurowings, Transavia, Pegasus, Turkish Airlines, Condor, SunExpress, Volotea, Smartwings, Jet2, LOT Polish Airlines, Finnair, SAS, Aegean, Aer Lingus, ITA Airways, TAP Portugal, Icelandair, PLAY |
| **Middle East & Africa** | Emirates, Etihad, Qatar Airways, flydubai, Air Arabia, flynas, Salam Air, Air Peace, FlySafair, EgyptAir, Ethiopian Airlines, Kenya Airways, Royal Air Maroc, South African Airways |
| **Asia-Pacific** | AirAsia, IndiGo, SpiceJet, Akasa Air, Air India, Air India Express, VietJet, Cebu Pacific, Scoot, Jetstar, Peach, Spring Airlines, Lucky Air, 9 Air, Nok Air, Batik Air, Jeju Air, T'way Air, ZIPAIR, Singapore Airlines, Cathay Pacific, Malaysian Airlines, Thai Airways, Korean Air, ANA, JAL, Qantas, Virgin Australia, Bangkok Airways, Air New Zealand, Garuda Indonesia, Philippine Airlines, US-Bangla, Biman Bangladesh |
| **Americas** | American Airlines, Delta, United, Southwest, JetBlue, Alaska Airlines, Hawaiian Airlines, Sun Country, Frontier, Volaris, VivaAerobus, Allegiant, Avelo, Breeze, Flair, GOL, Azul, JetSmart, Flybondi, Porter, WestJet, LATAM, Copa, Avianca, Air Canada, Arajet, Wingo, Sky Airline |
| **Aggregator** | Kiwi.com (virtual interlining + LCC fallback) |

</details>

### Local Search (No API Key)

```python
from letsfg.local import search_local

# Runs all relevant connectors on your machine — completely free
result = await search_local("GDN", "BCN", "2026-06-15")

# Limit browser concurrency for constrained environments
result = await search_local("GDN", "BCN", "2026-06-15", max_browsers=4)
```

```bash
# CLI local-only search
letsfg search-local GDN BCN 2026-06-15

# Limit browser concurrency
letsfg search-local GDN BCN 2026-06-15 --max-browsers 4
```

### Shared Browser Infrastructure

All browser-based connectors share a common launcher (`connectors/browser.py`) with:

- Automatic Chrome discovery (Windows, macOS, Linux)
- Stealth headless mode (`--headless=new`) — undetectable by airline bot protection
- Off-screen window positioning to avoid stealing focus
- CDP persistent sessions for airlines that require cookie state
- Adaptive concurrency — automatically scales browser instances based on system RAM
- `BOOSTED_BROWSER_VISIBLE=1` to show browser windows for debugging

### Performance Tuning

LetsFG auto-detects your system's available RAM and scales browser concurrency accordingly:

| System RAM | Tier | Max Browsers | Notes |
|-----------|------|-------------|-------|
| < 2 GB | Minimal | 2 | Low-end VMs, CI runners |
| 2–4 GB | Low | 3 | Budget laptops |
| 4–8 GB | Moderate | 5 | Standard laptops |
| 8–16 GB | Standard | 8 | Most desktops |
| 16–32 GB | High | 12 | Dev workstations |
| 32+ GB | Maximum | 16 | Servers |

Override auto-detection when needed:

```bash
# Environment variable (highest priority)
export LETSFG_MAX_BROWSERS=4

# CLI flag
letsfg search-local LHR BCN 2026-04-15 --max-browsers 4

# Check your system profile
letsfg system-info
```

```python
# Python SDK
from letsfg import configure_max_browsers, get_system_profile

profile = get_system_profile()
print(f"RAM: {profile['ram_available_gb']:.1f} GB, Tier: {profile['tier']}, Recommended: {profile['recommended_max_browsers']}")

configure_max_browsers(4)  # explicit override
```

## Error Handling

| Exception | HTTP | When |
|-----------|------|------|
| `AuthenticationError` | 401 | Missing or invalid API key |
| `OfferExpiredError` | 410 | Offer no longer available (search again) |
| `LetsFGError` | any | Base class for all API errors |

## Packages

| Package | Install | What it is |
|---------|---------|------------|
| **Python SDK + CLI** | `pip install letsfg` | SDK + `letsfg` CLI + 102 local airline connectors |
| **JS/TS SDK + CLI** | `npm install -g letsfg` | SDK + `letsfg` CLI command |
| **MCP Server** | `npx letsfg-mcp` | Model Context Protocol for Claude, Cursor, Windsurf |
| **Remote MCP** | `https://api.letsfg.co/mcp` | Streamable HTTP — no install needed |
| **Smithery** | [smithery.ai/server/letsfg-mcp](https://smithery.ai/server/letsfg-mcp) | One-click MCP install via Smithery |

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Authentication, payment setup, search flags, cabin classes |
| [API Guide](docs/api-guide.md) | Error handling, search results, workflows, unlock details, location resolution |
| [Agent Guide](docs/agent-guide.md) | AI agent architecture, preference scoring, price tracking, rate limits |
| [Architecture Guide](docs/architecture-guide.md) | Parallel execution, failure handling, caching, browser concurrency, performance tuning |
| [Tutorials](docs/tutorials.md) | Python & JS integration tutorials, concurrent search, travel assistant patterns |
| [Packages & SDKs](docs/packages.md) | Python SDK, JavaScript SDK, MCP Server, local connectors |
| [CLI Reference](docs/cli-reference.md) | Commands, flags, examples |
| [AGENTS.md](AGENTS.md) | Agent-specific instructions (for LLMs) |
| [CLAUDE.md](CLAUDE.md) | Codebase context for Claude |

## API Docs

- **OpenAPI spec:** [`openapi.yaml`](openapi.yaml) (included in this repo)
- **Interactive Swagger UI:** https://api.letsfg.co/docs
- **ReDoc:** https://api.letsfg.co/redoc
- **Agent discovery:** https://api.letsfg.co/.well-known/ai-plugin.json
- **Agent manifest:** https://api.letsfg.co/.well-known/agent.json
- **LLM instructions:** https://api.letsfg.co/llms.txt
- **Smithery:** https://smithery.ai/server/letsfg-mcp

**Base URL:** `https://api.letsfg.co`

## Links

- **PyPI:** https://pypi.org/project/letsfg/
- **npm (JS SDK):** https://www.npmjs.com/package/letsfg
- **npm (MCP):** https://www.npmjs.com/package/letsfg-mcp

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Security

See [SECURITY.md](SECURITY.md) for our security policy.

## License

[MIT](LICENSE)
