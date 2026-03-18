# CLAUDE.md — LetsFG Codebase Context

> Instructions for Claude and other AI coding agents working on this repository.

## Project Overview

LetsFG is an agent-native flight search & booking platform. This public repository contains the SDKs, 102 local airline connectors, and documentation. The backend API runs on Cloud Run and is in a separate private repository.

**API Base URL:** `https://api.letsfg.co`

## Repository Structure

```
LetsFG/
├── sdk/
│   ├── python/                  # Python SDK → PyPI: letsfg
│   │   ├── letsfg/
│   │   │   ├── __init__.py          # Public exports, version
│   │   │   ├── client.py            # LetsFG main client class (urllib-based)
│   │   │   ├── cli.py               # CLI entry point (typer)
│   │   │   ├── local.py             # Local LCC search runner (no API key needed)
│   │   │   ├── system_info.py       # System resource detection (RAM, CPU, tier)
│   │   │   ├── models.py            # Re-exports from models/
│   │   │   ├── models/
│   │   │   │   ├── __init__.py
│   │   │   │   └── flights.py       # Pydantic models (FlightOffer, FlightSegment, etc.)
│   │   │   └── connectors/          # 102 airline scrapers + infrastructure
│   │   │       ├── __init__.py
│   │   │       ├── _connector_template.py  # Reference template (3 patterns)
│   │   │       ├── browser.py        # Shared Chrome launcher, stealth CDP, cleanup
│   │   │       ├── engine.py         # Multi-provider search orchestrator
│   │   │       ├── combo_engine.py   # Virtual interlining (cross-airline combos)
│   │   │       ├── currency.py       # Currency conversion
│   │   │       ├── airline_routes.py # Route coverage registry (country → connectors)
│   │   │       ├── ryanair.py        # Direct API connectors...
│   │   │       ├── wizzair.py
│   │   │       ├── easyjet.py        # CDP Chrome connectors...
│   │   │       ├── norwegian.py      # Cookie-farm hybrid connectors...
│   │   │       └── [50 more airline connectors]
│   │   ├── pyproject.toml
│   │   └── README.md
│   ├── js/                      # JS/TS SDK → npm: letsfg
│   │   ├── src/
│   │   │   ├── index.ts             # Main client class
│   │   │   └── cli.ts               # CLI entry point
│   │   ├── package.json
│   │   └── README.md
│   └── mcp/                     # MCP Server → npm: letsfg-mcp
│       ├── src/
│       │   └── index.ts             # MCP tool definitions
│       ├── package.json
│       └── README.md
├── docs/                        # MkDocs documentation site
│   ├── index.md
│   ├── getting-started.md
│   ├── api-guide.md
│   ├── agent-guide.md
│   ├── cli-reference.md
│   └── packages.md
├── mcp-config.json              # Example MCP configuration
├── server.json                  # OpenAI plugin manifest
├── mkdocs.yml                   # MkDocs config
├── AGENTS.md                    # Agent-facing instructions
├── CLAUDE.md                    # This file
├── CONTRIBUTING.md              # Contribution guidelines
├── SECURITY.md                  # Security policy
├── SKILL.md                     # Machine-readable skill manifest
├── LICENSE                      # MIT
└── README.md                    # Public README
```

## Key Concepts

### Three-Step Flow
1. **Search** (free) → Returns flight offers from 400+ airlines
2. **Unlock** (free with GitHub star) → Confirms live price, locks offer for booking
3. **Book** (free after unlock) → Creates the actual booking with the airline

### Two Search Modes
1. **Cloud search** — Queries GDS/NDC providers (Duffel, Amadeus, Sabre, Travelport, Kiwi) via backend API. Requires API key.
2. **Local search** — Fires 102 connectors on the user's machine via Playwright + httpx. No API key needed. Both modes run simultaneously and results are merged.

### 102 Local Airline Connectors
The `connectors/` directory contains scrapers for 102 airlines. Three connector patterns:
- **Direct API** — Reverse-engineered REST/GraphQL endpoints (fastest, ~0.3-2s)
- **CDP Chrome** — Real Chrome browser via Playwright CDP for bot-protected sites (~10-25s)
- **API Interception** — Playwright navigation + response capture (~5-15s)

Key infrastructure files in `connectors/`:
- `browser.py` — Shared Chrome discovery, stealth launch (headless/CDP), adaptive concurrency, cleanup
- `engine.py` — Orchestrates all connectors in parallel, merges/deduplicates results
- `combo_engine.py` — Virtual interlining (cross-airline round-trips from one-way fares)
- `currency.py` — Real-time currency conversion for price normalization
- `airline_routes.py` — Maps countries to relevant connectors (only fires scrapers for relevant routes)

### Browser Concurrency Management
`browser.py` throttles concurrent Chrome instances with an `asyncio.Semaphore`. The limit is resolved in priority order:
1. `LETSFG_MAX_BROWSERS` env var (highest priority)
2. Explicit call to `configure_max_browsers(n)` or `--max-browsers` CLI flag
3. Auto-detect from available RAM via `system_info.py` (default)

`system_info.py` provides `get_system_profile()` which returns RAM, CPU, tier, and recommended max browsers. Tiers: minimal (<2GB, 2), low (2-4GB, 3), moderate (4-8GB, 5), standard (8-16GB, 8), high (16-32GB, 12), maximum (32+GB, 16).

### Zero Price Bias
The API returns raw airline prices — no demand-based inflation, no cookie tracking, no surge pricing. This is a core selling point.

### 100% Free
Everything is free — just star the GitHub repo (https://github.com/LetsFG/LetsFG) and verify via link-github.

### Real Passenger Details Required
When booking, agents MUST use real passenger email and legal name. Airlines send e-tickets to the email provided. Placeholder/fake data will cause booking failures.

## SDK Development

### Python SDK
```bash
cd sdk/python
pip install -e ".[dev]"
python -m pytest
```

### JS/TS SDK
```bash
cd sdk/js
npm install
npm run build    # Compiles TypeScript → dist/
npm test
```

### MCP Server
```bash
cd sdk/mcp
npm install
npm run build    # Compiles TypeScript → dist/
```

After editing JS or MCP source files, always rebuild with `npm run build` to update the dist bundles.

## Publishing

### Python SDK → PyPI
```bash
cd sdk/python
python -m build
twine upload dist/*
```

### JS SDK → npm
```bash
cd sdk/js
npm run build
npm publish
```

### MCP Server → npm
```bash
cd sdk/mcp
npm run build
npm publish
```

## Conventions

- Keep SDK READMEs in sync with the root README for pricing, flow descriptions, and warnings.
- All agent-facing text should include the "zero price bias" messaging and passenger details warning.
- Python SDK client (`client.py`) uses stdlib `urllib` for HTTP — zero external dependencies.
- Python SDK connectors use `playwright`, `httpx`, `curl_cffi`, `beautifulsoup4` for scraping.
- JS/TS SDK uses native `fetch`, TypeScript strict mode.
- MCP server uses `@modelcontextprotocol/sdk`.
- New connectors should follow one of the 3 patterns in `_connector_template.py`.
- After adding a connector, register it in `engine.py` and `airline_routes.py`.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/agents/register` | Register for an API key |
| `POST` | `/api/v1/agents/setup-payment` | Attach Stripe payment method (required for booking) |
| `GET`  | `/api/v1/agents/me` | Agent profile + usage stats |
| `POST` | `/api/v1/agents/link-github` | Star repo for free access |
| `POST` | `/api/v1/flights/search` | Search flights (cloud providers) |
| `GET`  | `/api/v1/flights/locations/{q}` | Resolve city/airport to IATA codes |
| `POST` | `/api/v1/bookings/unlock` | Unlock an offer (free) |
| `POST` | `/api/v1/bookings/book` | Book a flight (ticket price charged via Stripe) |
| `GET`  | `/api/v1/bookings/booking/{id}` | Get booking details |
| `GET`  | `/.well-known/ai-plugin.json` | OpenAI Plugin manifest |
| `GET`  | `/.well-known/agent.json` | Agent Protocol manifest |
| `GET`  | `/llms.txt` | LLM instructions |
| `GET`  | `/openapi.json` | OpenAPI spec |
| `GET`  | `/mcp` | Remote MCP (Streamable HTTP) |

## Links

- **API Docs:** https://api.letsfg.co/docs
- **PyPI:** https://pypi.org/project/letsfg/
- **npm SDK:** https://www.npmjs.com/package/letsfg
- **npm MCP:** https://www.npmjs.com/package/letsfg-mcp
- **GitHub:** https://github.com/LetsFG/LetsFG
