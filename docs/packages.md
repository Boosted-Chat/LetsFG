# Packages

LetsFG is available as a Python SDK, JavaScript SDK, MCP server, and remote MCP endpoint. Works with OpenClaw, Perplexity Computer, Claude Desktop, Cursor, Windsurf, and any MCP-compatible agent.

## Overview

| Package | Install | What it is | API Key Required? |
|---------|---------|------------|-------------------|
| **Python SDK + CLI** | `pip install letsfg` | SDK + CLI + 102 local airline connectors | No (local search). Yes (cloud search, unlock, book) |
| **JS/TS SDK + CLI** | `npm install -g letsfg` | SDK + `letsfg` CLI command | Yes |
| **MCP Server** | `npx letsfg-mcp` | Model Context Protocol for AI agents | No (local search). Yes (cloud search, unlock, book) |
| **Remote MCP** | `https://api.letsfg.co/mcp` | Streamable HTTP — no install needed | Yes |
| **Smithery** | [smithery.ai/server/letsfg-mcp](https://smithery.ai/server/letsfg-mcp) | One-click MCP install | No (local search). Yes (cloud search) |

## Python SDK

[![PyPI](https://img.shields.io/pypi/v/letsfg)](https://pypi.org/project/letsfg/)

```bash
pip install letsfg
```

Provides:

- `LetsFG` client class with `search()`, `unlock()`, `book()`, `me()`, `resolve_location()`, `setup_payment()`
- **102 local airline connectors** — run directly on your machine (Ryanair, Wizz Air, EasyJet, Norwegian, AirAsia, IndiGo, Qatar Airways, LATAM, Finnair, and 93 more)
- `search_local()` — free local-only search, no API key needed
- `get_system_profile()` — detect system RAM/CPU and recommended concurrency
- `configure_max_browsers(n)` — set max concurrent browser instances (1–32)
- CLI command `letsfg` with all operations
- Virtual interlining engine — cross-airline round-trips from one-way fares
- Shared browser infrastructure — stealth Chrome launcher, CDP sessions, anti-bot handling
- Typed response models: `FlightSearchResponse`, `UnlockResponse`, `BookingResponse`, `AgentProfile`
- Exception classes: `AuthenticationError`, `PaymentRequiredError`, `OfferExpiredError`

```python
from letsfg import LetsFG

bt = LetsFG(api_key="trav_...")
flights = bt.search("LHR", "JFK", "2026-04-15")
```

### Local Search (No API Key)

```python
from letsfg.local import search_local

# Free, runs all relevant LCC connectors on your machine
result = await search_local("GDN", "BCN", "2026-06-15")
```

[Full Python SDK docs →](https://github.com/LetsFG/LetsFG/tree/main/sdk/python)

## JavaScript / TypeScript SDK

[![npm](https://img.shields.io/npm/v/letsfg)](https://www.npmjs.com/package/letsfg)

```bash
npm install -g letsfg
```

Provides:

- `LetsFG` client class with `search()`, `unlock()`, `book()`, `me()`
- CLI command `letsfg` (same interface as Python)
- TypeScript types for all responses

```typescript
import { LetsFG } from 'letsfg';

const bt = new LetsFG({ apiKey: 'trav_...' });
const flights = await bt.search('LHR', 'JFK', '2026-04-15');
```

[Full JS SDK docs →](https://github.com/LetsFG/LetsFG/tree/main/sdk/js)

## MCP Server

[![npm](https://img.shields.io/npm/v/letsfg-mcp)](https://www.npmjs.com/package/letsfg-mcp)

Model Context Protocol server for AI assistants like Claude Desktop, Cursor, and Windsurf.

### Quick Setup

```bash
npx letsfg-mcp
```

### Configuration

Add to your MCP config (Claude Desktop, Cursor, etc.):

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

### Remote MCP (Streamable HTTP)

If your client supports remote MCP servers, connect directly without installing anything:

```
https://api.letsfg.co/mcp
```

### Available Tools

| Tool | Description |
|------|-------------|
| `search_flights` | Search 400+ airlines for flights |
| `get_agent_profile` | View account info and usage stats |
| `resolve_location` | Convert city names to IATA codes |
| `system_info` | System resources & recommended concurrency |
| `setup_payment` | Attach a Stripe payment method |
| `unlock_flight_offer` | Confirm price and reserve (free) |
| `book_flight` | Create airline booking (PNR) |

[npm page →](https://www.npmjs.com/package/letsfg-mcp)

## API Endpoints

All packages connect to the same API:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/agents/register` | POST | Create account, get API key |
| `/api/v1/flights/search` | POST | Search flights |
| `/api/v1/flights/resolve-location` | GET | Resolve city/airport codes |
| `/api/v1/bookings/unlock` | POST | Unlock offer (free) |
| `/api/v1/bookings/book` | POST | Book flight |
| `/api/v1/agents/setup-payment` | POST | Setup Stripe payment |
| `/api/v1/agents/me` | GET | Agent profile |

**Base URL:** `https://api.letsfg.co`

**Interactive docs:** [api.letsfg.co/docs](https://api.letsfg.co/docs)
