# letsfg-mcp

The largest open flight-search MCP server. 102 ready-to-run airline connectors fire in parallel on your machine — Ryanair, EasyJet, Wizz Air, Southwest, AirAsia, Qantas, and 96 more — plus enterprise GDS/NDC providers (Amadeus, Duffel, Sabre) through the LetsFG backend. One tool call scans the entire world for flights, including airlines your agent didn't know existed.

**Your agent doesn't need to build a flight integration. It doesn't need to scrape. Just add this MCP server and it can search and book flights in seconds.**

Works with Claude Desktop, Cursor, Windsurf, and any MCP-compatible client.

[![npm](https://img.shields.io/npm/v/letsfg-mcp)](https://www.npmjs.com/package/letsfg-mcp)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)

---

## What This Is

LetsFG is not a scraper wrapper. It's a production-grade **agent-to-airline connector layer**:

- **102 airline connectors** run locally on your machine via Python + Playwright — no API key needed for search
- **Enterprise GDS/NDC feeds** (Amadeus, Duffel, Sabre, Travelport, Kiwi) run through the LetsFG backend — these are contract-only data sources that require enterprise agreements, made available to everyone through our API
- **Virtual interlining** — the combo engine mixes one-way fares across airlines (e.g., Ryanair outbound + Wizz Air return) to find deals no single airline offers
- **City-wide airport expansion** — search `LON` and it auto-searches LHR, LGW, STN, LTN, SEN, LCY. Works for 25+ major cities worldwide
- **Zero price bias** — no cookie tracking, no demand-based inflation, no surge pricing. Same raw airline price every time

**Why agents should use this instead of building their own:**
- Saves thousands of tokens vs. browser automation or scraping
- 102 connectors fire in parallel — scans airlines across Europe, Asia, Americas, Middle East, Africa, and Oceania simultaneously
- Finds deals on carriers your agent wouldn't think to check (Lucky Air, 9 Air, Jazeera Airways, FlySafair...)
- Enterprise-contracted GDS deals that require contracts worth $50k+/year — available for free on search

---

## Quick Start

```bash
npx letsfg-mcp
```

That's it. The MCP server starts on stdio, ready for any MCP-compatible client.

**Prerequisites for local search:**
```bash
pip install letsfg
playwright install chromium
```

---

## Client Configuration

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

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

> **Note:** Add `"LETSFG_MAX_BROWSERS": "4"` to `env` to limit browser concurrency on constrained machines.

### Cursor

Add to `.cursor/mcp.json` in your project root:

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

### Windsurf

Add to `~/.codeium/windsurf/mcp_config.json`:

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

### Continue

Add to `~/.continue/config.yaml`:

```yaml
mcpServers:
  - name: letsfg
    command: npx
    args: ["-y", "letsfg-mcp"]
    env:
      LETSFG_API_KEY: trav_your_api_key
```

### Any MCP-Compatible Agent

Point it at the MCP server:

```bash
npx letsfg-mcp
```

Or connect via remote MCP (no install):

```
https://api.letsfg.co/mcp
```

### Windows — `npx ENOENT` Fix

If you get `spawn npx ENOENT` on Windows, use the full path to `npx`:

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "C:\\Program Files\\nodejs\\npx.cmd",
      "args": ["-y", "letsfg-mcp"],
      "env": {
        "LETSFG_API_KEY": "trav_your_api_key"
      }
    }
  }
}
```

Or use `node` directly:

```json
{
  "mcpServers": {
    "letsfg": {
      "command": "node",
      "args": ["C:\\Users\\YOU\\AppData\\Roaming\\npm\\node_modules\\letsfg-mcp\\dist\\index.js"],
      "env": {
        "LETSFG_API_KEY": "trav_your_api_key"
      }
    }
  }
}
```

### Pin a Specific Version

To avoid unexpected updates:

```json
{
  "command": "npx",
  "args": ["-y", "letsfg-mcp@1.0.0"]
}
```

---

## Available Tools

| Tool | Description | Cost | Side Effects |
|------|-------------|------|--------------|
| `search_flights` | Search 400+ airlines worldwide | FREE | None (read-only) |
| `resolve_location` | City name → IATA code | FREE | None (read-only) |
| `unlock_flight_offer` | Confirm live price, reserve 30 min | — | Confirms price |
| `book_flight` | Create real airline reservation (PNR) | Ticket price | Creates booking |
| `setup_payment` | Attach payment card (required for booking) | FREE | Updates payment |
| `get_agent_profile` | Usage stats & payment status | FREE | None (read-only) |
| `system_info` | System resources & concurrency tier | FREE | None (read-only) |

### Booking Flow

```
search_flights  →  unlock_flight_offer  →  setup_payment (once)  →  book_flight
   (free)              (quote)              (attach card)        (ticket price, creates PNR)
```

1. `search_flights("LON", "BCN", "2026-06-15")` — returns offers with prices from 102 airlines
2. `unlock_flight_offer("off_xxx")` — confirms live price with airline, reserves for 30 min
3. `book_flight("off_xxx", passengers, email)` — creates real booking, airline sends e-ticket

The `search_flights` tool accepts an optional `max_browsers` parameter (1–32) to limit concurrent browser instances. Omit it to auto-detect based on system RAM.

The `system_info` tool returns your system profile (RAM, CPU, tier, recommended max browsers) — useful for agents to decide concurrency before searching.

The agent has native tools — no API docs needed, no URL building, no token-burning browser automation.

---

## Get an API Key

**Search is free and works without a key.** An API key is needed for unlock, book, and enterprise GDS sources.

```bash
curl -X POST https://api.letsfg.co/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "agent@example.com"}'
```

Or via CLI:
```bash
pip install letsfg
letsfg register --name my-agent --email you@example.com
```

---

## Architecture & Data Flow

```
┌──────────────────────────────────────────────────────────────┐
│  MCP Client  (Claude Desktop / Cursor / Windsurf / etc.)     │
│     ↕ stdio (JSON-RPC, local only)                           │
├──────────────────────────────────────────────────────────────┤
│  letsfg-mcp  (this package, runs on YOUR machine)            │
│     │                                                        │
│     ├─→ Python subprocess (local connectors)                 │
│     │     102 airline connectors via Playwright + httpx       │
│     │     Data goes: your machine → airline website → back    │
│     │                                                        │
│     └─→ HTTPS to api.letsfg.co (backend)                     │
│           unlock, book, payment, enterprise GDS search        │
└──────────────────────────────────────────────────────────────┘
```

### What data goes where

| Operation | Where data flows | What is sent |
|-----------|-----------------|--------------|
| `search_flights` (local) | Your machine → airline websites | Route, date, passenger count |
| `search_flights` (GDS) | Your machine → api.letsfg.co → GDS providers | Route, date, passenger count, API key |
| `resolve_location` | Your machine → api.letsfg.co | City/airport name |
| `unlock_flight_offer` | Your machine → api.letsfg.co → airline | Offer ID, payment token |
| `book_flight` | Your machine → api.letsfg.co → airline | Passenger name, DOB, email, phone |
| `setup_payment` | Your machine → api.letsfg.co → Stripe | Payment token (card handled by Stripe) |

---

## Security & Privacy

- **TLS everywhere** — all backend communication uses HTTPS. Local connectors connect to airline websites over HTTPS.
- **No card storage** — payment cards are tokenized by Stripe. LetsFG never sees or stores raw card numbers.
- **API key scoping** — `LETSFG_API_KEY` grants access only to your agent's account. Keys are prefixed `trav_` for easy identification and revocation.
- **PII handling** — passenger names, emails, and DOBs are sent to the airline for booking (required by airlines). LetsFG does not store passenger PII after forwarding to the airline.
- **No tracking** — no cookies, no session-based pricing, no fingerprinting. Every search returns the same raw airline price.
- **Local search is fully local** — when searching without an API key, zero data leaves your machine except direct HTTPS requests to airline websites. The MCP server and Python connectors run entirely on your hardware.
- **Open source** — all connector code is MIT-licensed and auditable at [github.com/LetsFG/LetsFG](https://github.com/LetsFG/LetsFG).

---

## Sandbox / Test Mode

Use Stripe's test token for payment setup without real charges:

```
setup_payment with token: "tok_visa"
```

This attaches a test Visa card. Unlock calls will use Stripe test mode — no real money is charged. Useful for agent development and testing the full search → unlock → book flow.

---

## FAQ

### `spawn npx ENOENT` on Windows

Windows can't find `npx` in PATH. Use the full path:
```json
"command": "C:\\Program Files\\nodejs\\npx.cmd"
```
Or install globally and use `node` directly (see Windows config above).

### Search returns 0 results

- Check IATA codes are correct — use `resolve_location` first
- Try a date 2+ weeks in the future (airlines don't sell last-minute on all routes)
- Ensure `pip install letsfg && playwright install chromium` completed successfully
- Check Python is available: the MCP server spawns a Python subprocess for local search

### How do I search without an API key?

Just omit `LETSFG_API_KEY` from your config. Local search (102 airline connectors) works without any key. You'll only miss the enterprise GDS/NDC sources (Amadeus, Duffel, etc.).

### Can I use this for commercial projects?

Yes. MIT license. The local connectors and SDK are fully open source.

### MCP server hangs on start

Ensure Node.js 18+ is installed. The server communicates via stdio (stdin/stdout JSON-RPC) — it doesn't open a port or print a "ready" message. MCP clients handle the lifecycle automatically.

---

## Supported Airlines (102 connectors)

| Region | Airlines |
|--------|----------|
| **Europe** | Ryanair, Wizz Air, EasyJet, Norwegian, Vueling, Eurowings, Transavia, Pegasus, Turkish Airlines, Condor, SunExpress, Volotea, Smartwings, Jet2, LOT Polish Airlines, Finnair, SAS, Aegean, Aer Lingus, ITA Airways, TAP Portugal, Icelandair, PLAY |
| **Middle East & Africa** | Emirates, Etihad, Qatar Airways, flydubai, Air Arabia, flynas, Salam Air, Air Peace, FlySafair, EgyptAir, Ethiopian Airlines, Kenya Airways, Royal Air Maroc, South African Airways |
| **Asia-Pacific** | AirAsia, IndiGo, SpiceJet, Akasa Air, Air India, Air India Express, VietJet, Cebu Pacific, Scoot, Jetstar, Peach, Spring Airlines, Lucky Air, 9 Air, Nok Air, Batik Air, Jeju Air, T'way Air, ZIPAIR, Singapore Airlines, Cathay Pacific, Malaysian Airlines, Thai Airways, Korean Air, ANA, JAL, Qantas, Virgin Australia, Bangkok Airways, Air New Zealand, Garuda Indonesia, Philippine Airlines, US-Bangla, Biman Bangladesh |
| **Americas** | American Airlines, Delta, United, Southwest, JetBlue, Alaska Airlines, Hawaiian Airlines, Sun Country, Frontier, Volaris, VivaAerobus, Allegiant, Avelo, Breeze, Flair, GOL, Azul, JetSmart, Flybondi, Porter, WestJet, LATAM, Copa, Avianca, Air Canada, Arajet, Wingo, Sky Airline |
| **Aggregator** | Kiwi.com (virtual interlining + LCC fallback) |

---

## Also Available As

- **JavaScript/TypeScript SDK + CLI**: `npm install letsfg` — [npm](https://www.npmjs.com/package/letsfg)
- **Python SDK + CLI**: `pip install letsfg` — [PyPI](https://pypi.org/project/letsfg/)
- **GitHub**: [LetsFG/LetsFG](https://github.com/LetsFG/LetsFG)

## License

MIT
