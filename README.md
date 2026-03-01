# BoostedTravel

Agent-native flight search & booking. 400+ airlines, straight from the terminal — no browser, no scraping. Built for AI agents and developers.

**API Base URL:** `https://api.boostedchat.com`

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/boostedtravel)](https://pypi.org/project/boostedtravel/)
[![npm](https://img.shields.io/npm/v/boostedtravel)](https://www.npmjs.com/package/boostedtravel)

## Why BoostedTravel?

Flight websites inflate prices with demand tracking, cookie-based pricing, and surge markup. The same flight is often **$20–$50 cheaper** through BoostedTravel — raw airline price, zero markup.

| | Google Flights / Booking.com / Expedia | **BoostedTravel** |
|---|---|---|
| Search | Free | **Free** |
| View details & price | Free (with tracking/inflation) | **Free** (no tracking) |
| Book | Ticket + hidden markup | **$1 unlock + ticket price** |
| Price goes up on repeat search? | Yes | **Never** |

## Quick Start

```bash
pip install boostedtravel

boostedtravel register --name my-agent --email you@example.com
export BOOSTEDTRAVEL_API_KEY=trav_...

boostedtravel search LHR JFK 2026-04-15
boostedtravel search LON BCN 2026-04-01 --return 2026-04-08 --cabin M --sort price
boostedtravel unlock off_xxx
boostedtravel book off_xxx \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --email john.doe@example.com
```

All commands support `--json` for machine-readable output:

```bash
boostedtravel search GDN BER 2026-03-03 --json | jq '.offers[0]'
```

## Install

### Python (recommended)

```bash
pip install boostedtravel
```

### JavaScript / TypeScript

```bash
npm install -g boostedtravel
```

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

### MCP Server (Claude Desktop / Cursor / Windsurf)

```bash
npx boostedtravel-mcp
```

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

## CLI Commands

| Command | Description |
|---------|-------------|
| `boostedtravel register` | Get your API key |
| `boostedtravel search <origin> <dest> <date>` | Search flights (free) |
| `boostedtravel locations <query>` | Resolve city/airport to IATA codes |
| `boostedtravel unlock <offer_id>` | Unlock offer details ($1) |
| `boostedtravel book <offer_id>` | Book the flight (free after unlock) |
| `boostedtravel setup-payment` | Set up payment method |
| `boostedtravel me` | View profile & usage stats |

All commands accept `--json` for structured output and `--api-key` to override the env variable.

## How It Works

1. **Search** (free) — returns offers with full details: price, airlines, duration, stopovers, conditions
2. **Unlock** ($1) — confirms live price with the airline, reserves for 30 minutes
3. **Book** (free) — creates real airline PNR, e-ticket sent to passenger email

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
| **Python SDK + CLI** | `pip install boostedtravel` | SDK + `boostedtravel` CLI command |
| **JS/TS SDK + CLI** | `npm install -g boostedtravel` | SDK + `boostedtravel` CLI command |
| **MCP Server** | `npx boostedtravel-mcp` | Model Context Protocol for Claude, Cursor, etc. |

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Authentication, payment setup, search flags, cabin classes |
| [API Guide](docs/api-guide.md) | Error handling, search results, workflows, unlock details, location resolution |
| [Agent Guide](docs/agent-guide.md) | AI agent architecture, preference scoring, price tracking, rate limits |
| [AGENTS.md](AGENTS.md) | Agent-specific instructions |
| [CLAUDE.md](CLAUDE.md) | Codebase context for Claude |

## API Docs

- **OpenAPI/Swagger:** https://api.boostedchat.com/docs
- **Agent discovery:** https://api.boostedchat.com/.well-known/ai-plugin.json
- **Agent manifest:** https://api.boostedchat.com/.well-known/agent.json
- **LLM instructions:** https://api.boostedchat.com/llms.txt

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
