# BoostedTravel

Agent-native flight search & booking. 400+ airlines, 73 ready-to-run airline connectors, virtual interlining — straight from the terminal. Built for AI agents (OpenClaw, Perplexity Computer, Claude, Cursor, Windsurf) and developers.

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/boostedtravel)](https://pypi.org/project/boostedtravel/)
[![npm](https://img.shields.io/npm/v/boostedtravel)](https://www.npmjs.com/package/boostedtravel)
[![Smithery](https://smithery.ai/badge/boostedtravel-mcp)](https://smithery.ai/server/boostedtravel-mcp)

## Demo: BoostedTravel vs Default Agent Search

<div align="center">
  <img src="assets/demo.gif" alt="Demo: BoostedTravel vs Default Agent Search" width="640">
</div>

> Side-by-side comparison: default agent search (OpenClaw, Perplexity Computer) vs BoostedTravel CLI. Same query — BoostedTravel finds cheaper flights across 73 airlines in seconds.

## Why BoostedTravel?

Flight websites inflate prices with demand tracking, cookie-based pricing, and surge markup. The same flight is often **$20–$50 cheaper** through BoostedTravel — raw airline price, zero markup.

BoostedTravel works by finding the best price across the entire internet. It fires 73 airline connectors in parallel, scanning carriers across Europe, Asia, Americas, Middle East, and Africa — then merges results with enterprise GDS/NDC sources (Amadeus, Duffel, Sabre, Travelport) that provide competitive pricing from 400+ carriers including premium airlines like Lufthansa, British Airways, and Emirates. The best price wins.

| | Google Flights / Booking.com / Expedia | **BoostedTravel** |
|---|---|---|
| Search | Free | **Free** |
| View details & price | Free (with tracking/inflation) | **Free** (no tracking) |
| Book | Ticket + hidden markup | **$1 unlock + ticket price** |
| Price goes up on repeat search? | Yes | **Never** |
| LCC coverage | Missing many low-cost carriers | **73 direct airline connectors** |

---

## One-Click Install

```bash
pip install boostedtravel
```

That's it. You can search flights immediately — no account, no API key, no configuration:

```bash
boostedtravel search-local GDN BCN 2026-06-15
```

This runs 73 airline connectors locally on your machine and returns real-time prices. Completely free, unlimited, zero setup.

---

## Two Ways to Use BoostedTravel

### Option A: Local Only (Free, No API Key)

Install and search. One command, zero configuration.

```bash
pip install boostedtravel
boostedtravel search-local LHR BCN 2026-04-15
```

**What you get:**
- 73 airline connectors running on your machine (Ryanair, Wizz Air, EasyJet, Southwest, AirAsia, Norwegian, and 67 more)
- Real-time prices scraped directly from airline websites
- Virtual interlining — cross-airline round-trips that save 30–50%
- Completely free, unlimited searches

```python
from boostedtravel.local import search_local

result = await search_local("GDN", "BCN", "2026-06-15")
for offer in result.offers[:5]:
    print(f"{offer.airlines[0]}: {offer.currency} {offer.price}")
```

### Option B: With API Key (Recommended — Much Better Coverage)

One extra command unlocks the full power of BoostedTravel:

```bash
pip install boostedtravel
boostedtravel register --name my-agent --email you@example.com
# → Returns: trav_xxxxx... (your API key)
export BOOSTEDTRAVEL_API_KEY=trav_...

boostedtravel search LHR JFK 2026-04-15
```

**What you get (in addition to everything in Option A):**
- **Enterprise GDS/NDC providers** — Amadeus, Duffel, Sabre, Travelport, Kiwi. These are contract-only data sources that normally require enterprise agreements worth $50k+/year. BoostedTravel is contracted with these providers and makes their inventory available to every user.
- **400+ full-service airlines** — Lufthansa, British Airways, Emirates, Singapore Airlines, ANA, Cathay Pacific, and hundreds more that don't have public APIs
- **Competitive pricing** — the backend aggregates offers from multiple GDS sources and picks the cheapest for each route
- **Unlock & book** — confirm live prices ($1) and create real airline PNRs with e-tickets
- Both local connectors AND cloud sources run simultaneously — results merged and deduplicated automatically

**Registration is instant, free, and handled by CLI** — an AI agent can do it in one command. The API key connects you to our closed-source backend service which maintains enterprise contracts with GDS/NDC providers and premium carriers. We advise using the API key to connect to all sources for the best prices.

```python
from boostedtravel import BoostedTravel

bt = BoostedTravel()  # reads BOOSTEDTRAVEL_API_KEY from env
flights = bt.search("LHR", "JFK", "2026-04-15")
print(f"{flights.total_results} offers, cheapest: {flights.cheapest.summary()}")
```

---

## Quick Start (Full Flow)

```bash
pip install boostedtravel

# Register and get API key (free, instant)
boostedtravel register --name my-agent --email you@example.com
export BOOSTEDTRAVEL_API_KEY=trav_...

# Search (free, unlimited)
boostedtravel search LHR JFK 2026-04-15
boostedtravel search LON BCN 2026-04-01 --return 2026-04-08 --cabin M --sort price

# Unlock ($1 — confirms live price, reserves for 30 min)
boostedtravel unlock off_xxx

# Book (free after unlock)
boostedtravel book off_xxx \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --email john.doe@example.com
```

All commands support `--json` for machine-readable output:

```bash
boostedtravel search GDN BER 2026-03-03 --json | jq '.offers[0]'
```

## Install

### Python (recommended — includes 73 local airline connectors)

```bash
pip install boostedtravel
playwright install chromium  # needed for browser-based connectors
```

### JavaScript / TypeScript (API client only)

```bash
npm install -g boostedtravel
```

### MCP Server (Claude Desktop / Cursor / Windsurf / OpenClaw)

```bash
npx boostedtravel-mcp
```

Add to your MCP config:

```json
{
  "mcpServers": {
    "boostedtravel": {
      "command": "npx",
      "args": ["-y", "boostedtravel-mcp"],
      "env": {
        "BOOSTEDTRAVEL_API_KEY": "trav_your_api_key"
      }
    }
  }
}
```

> **Note:** `BOOSTEDTRAVEL_API_KEY` is optional. Without it, the MCP server still runs all 73 local connectors. With it, you also get enterprise GDS/NDC sources (400+ more airlines).

### Python SDK

```python
from boostedtravel import BoostedTravel

bt = BoostedTravel(api_key="trav_...")
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
import { BoostedTravel } from 'boostedtravel';

const bt = new BoostedTravel({ apiKey: 'trav_...' });
const flights = await bt.search('LHR', 'JFK', '2026-04-15');
console.log(`${flights.totalResults} offers`);
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `boostedtravel register` | Get your API key |
| `boostedtravel search <origin> <dest> <date>` | Search flights (free) |
| `boostedtravel locations <query>` | Resolve city/airport to IATA codes |
| `boostedtravel unlock <offer_id>` | Unlock offer details ($1) |
| `boostedtravel book <offer_id>` | Book the flight (free after unlock) |
| `boostedtravel setup-payment` | Set up payment method |
| `boostedtravel system-info` | Show system resources & concurrency tier |
| `boostedtravel me` | View profile & usage stats |

All commands accept `--json` for structured output and `--api-key` to override the env variable.

## How It Works

1. **Search** (free) — returns offers with full details: price, airlines, duration, stopovers, conditions
2. **Unlock** ($1) — confirms live price with the airline, reserves for 30 minutes
3. **Book** (free) — creates real airline PNR, e-ticket sent to passenger email

### Two Search Modes

| Mode | What it does | Speed | Auth |
|------|-------------|-------|------|
| **Cloud search** | Queries GDS/NDC providers (Duffel, Amadeus, Sabre, Travelport, Kiwi) via backend API | 2-15s | API key |
| **Local search** | Fires 73 airline connectors on your machine via Playwright + httpx | 5-25s | None |

Both modes run simultaneously by default. Results are merged, deduplicated, currency-normalized, and sorted.

### Virtual Interlining

The combo engine builds cross-airline round-trips by combining one-way fares from different carriers. A Ryanair outbound + Wizz Air return can save 30-50% vs booking a round-trip on either airline alone.

### City-Wide Airport Expansion

Search a city code and BoostedTravel automatically searches all airports in that city. `LON` expands to LHR, LGW, STN, LTN, SEN, LCY. `NYC` expands to JFK, EWR, LGA. Works for 25+ major cities worldwide — one search covers every airport.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  AI Agents / CLI / SDK / MCP Server                 │
├──────────────────┬──────────────────────────────────┤
│  Local connectors │  Enterprise Cloud API            │
│  (73 airlines via │  (Amadeus, Duffel, Sabre,        │
│   Playwright)     │   Travelport, Kiwi — contract-   │
│                   │   only GDS/NDC providers)        │
├──────────────────┴──────────────────────────────────┤
│            Merge + Dedup + Combo Engine              │
│            (virtual interlining, currency norm)      │
└─────────────────────────────────────────────────────┘
```

## Local Airline Connectors (73 airlines)

The Python SDK includes 73 production-grade airline connectors — not fragile scrapers, but maintained integrations that handle each airline's specific API pattern. No API key needed for local search. Each connector uses one of three proven strategies:

| Strategy | How it works | Example airlines |
|----------|-------------|-----------------|
| **Direct API** | Reverse-engineered REST/GraphQL endpoints via `httpx`/`curl_cffi` | Ryanair, Wizz Air, Norwegian, Akasa |
| **CDP Chrome** | Real Chrome + Playwright CDP for sites with bot detection | EasyJet, Southwest, Pegasus |
| **API Interception** | Playwright page navigation + response interception | VietJet, Cebu Pacific, Lion Air |

### Supported Airlines

<details>
<summary>Full list of 73 airline connectors</summary>

| Region | Airlines |
|--------|----------|
| **Europe** | Ryanair, Wizz Air, EasyJet, Norwegian, Vueling, Eurowings, Transavia, Pegasus, Turkish Airlines, Condor, SunExpress, Volotea, Smartwings, Jet2 |
| **Middle East & Africa** | Emirates, Etihad, flydubai, Air Arabia, flynas, Salam Air, Air Peace, FlySafair |
| **Asia-Pacific** | AirAsia, IndiGo, SpiceJet, Akasa Air, Air India Express, VietJet, Cebu Pacific, Scoot, Jetstar, Peach, Spring Airlines, Lucky Air, 9 Air, Nok Air, Batik Air, Jeju Air, T'way Air, ZIPAIR, Singapore Airlines, Cathay Pacific, Malaysian Airlines, Thai Airways, Korean Air, ANA, US-Bangla, Biman Bangladesh |
| **Americas** | American Airlines, Delta, United, Southwest, JetBlue, Alaska Airlines, Hawaiian Airlines, Sun Country, Frontier, Volaris, VivaAerobus, Allegiant, Avelo, Breeze, Flair, GOL, Azul, JetSmart, Flybondi, Porter |
| **Aggregator** | Kiwi.com (virtual interlining + LCC fallback) |

</details>

### Local Search (No API Key)

```python
from boostedtravel.local import search_local

# Runs all relevant connectors on your machine — completely free
result = await search_local("GDN", "BCN", "2026-06-15")

# Limit browser concurrency for constrained environments
result = await search_local("GDN", "BCN", "2026-06-15", max_browsers=4)
```

```bash
# CLI local-only search
boostedtravel search-local GDN BCN 2026-06-15

# Limit browser concurrency
boostedtravel search-local GDN BCN 2026-06-15 --max-browsers 4
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

BoostedTravel auto-detects your system's available RAM and scales browser concurrency accordingly:

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
export BOOSTEDTRAVEL_MAX_BROWSERS=4

# CLI flag
boostedtravel search-local LHR BCN 2026-04-15 --max-browsers 4

# Check your system profile
boostedtravel system-info
```

```python
# Python SDK
from boostedtravel import configure_max_browsers, get_system_profile

profile = get_system_profile()
print(f"RAM: {profile['ram_available_gb']:.1f} GB, Tier: {profile['tier']}, Recommended: {profile['recommended_max_browsers']}")

configure_max_browsers(4)  # explicit override
```

## Error Handling

| Exception | HTTP | When |
|-----------|------|------|
| `AuthenticationError` | 401 | Missing or invalid API key |
| `PaymentRequiredError` | 402 | No payment method (call `setup-payment`) |
| `OfferExpiredError` | 410 | Offer no longer available (search again) |
| `BoostedTravelError` | any | Base class for all API errors |

## Packages

| Package | Install | What it is |
|---------|---------|------------|
| **Python SDK + CLI** | `pip install boostedtravel` | SDK + `boostedtravel` CLI + 73 local airline connectors |
| **JS/TS SDK + CLI** | `npm install -g boostedtravel` | SDK + `boostedtravel` CLI command |
| **MCP Server** | `npx boostedtravel-mcp` | Model Context Protocol for Claude, Cursor, Windsurf |
| **Remote MCP** | `https://api.boostedchat.com/mcp` | Streamable HTTP — no install needed |
| **Smithery** | [smithery.ai/server/boostedtravel-mcp](https://smithery.ai/server/boostedtravel-mcp) | One-click MCP install via Smithery |

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Authentication, payment setup, search flags, cabin classes |
| [API Guide](docs/api-guide.md) | Error handling, search results, workflows, unlock details, location resolution |
| [Agent Guide](docs/agent-guide.md) | AI agent architecture, preference scoring, price tracking, rate limits |
| [Packages & SDKs](docs/packages.md) | Python SDK, JavaScript SDK, MCP Server, local connectors |
| [CLI Reference](docs/cli-reference.md) | Commands, flags, examples |
| [AGENTS.md](AGENTS.md) | Agent-specific instructions (for LLMs) |
| [CLAUDE.md](CLAUDE.md) | Codebase context for Claude |

## API Docs

- **OpenAPI spec:** [`openapi.yaml`](openapi.yaml) (included in this repo)
- **Interactive Swagger UI:** https://api.boostedchat.com/docs
- **ReDoc:** https://api.boostedchat.com/redoc
- **Agent discovery:** https://api.boostedchat.com/.well-known/ai-plugin.json
- **Agent manifest:** https://api.boostedchat.com/.well-known/agent.json
- **LLM instructions:** https://api.boostedchat.com/llms.txt
- **Smithery:** https://smithery.ai/server/boostedtravel-mcp

**Base URL:** `https://api.boostedchat.com`

## Links

- **PyPI:** https://pypi.org/project/boostedtravel/
- **npm (JS SDK):** https://www.npmjs.com/package/boostedtravel
- **npm (MCP):** https://www.npmjs.com/package/boostedtravel-mcp

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Security

See [SECURITY.md](SECURITY.md) for our security policy.

## License

[MIT](LICENSE)
