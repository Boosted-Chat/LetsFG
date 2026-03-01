# BoostedTravel

Open-source, agent-native, CLI-native flight search & booking. Search 400+ airlines and book tickets straight from the terminal — no browser, no scraping, no token-burning automation. Built for AI agents and developers who need travel built into their workflow.

**API Base URL:** `https://api.boostedchat.com`

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/boostedtravel)](https://pypi.org/project/boostedtravel/)
[![npm](https://img.shields.io/npm/v/boostedtravel)](https://www.npmjs.com/package/boostedtravel)

## Why BoostedTravel Exists

AI agents need a native, CLI-based way to search and book flights, hotels, and travel experiences. Without it, agents either burn massive amounts of tokens on browser automation and scraping — or simply can't do it at all. That's unacceptable in situations where time and cost matter.

BoostedTravel is an open-source interface to find flights and everything travel-related. One command, real results, real bookings.

**You don't pay extra for the brand of a website.** Flight websites like Booking.com, Expedia, Google Flights, and Kayak inflate prices based on demand patterns, cookie tracking, browser fingerprinting, and surge pricing. The same flight that shows as $350 on those sites is often **$20–$50 cheaper** through BoostedTravel — because we return the raw airline price with zero markup or bias. Same flight, same airline, same seat — just cheaper.

## Pricing: Us vs. Flight Websites

| | Google Flights / Booking.com / Expedia | **BoostedTravel** |
|---|---|---|
| Search flights | Free | **Free** |
| View full offer details & price | Free (with tracking/inflation) | **$1 unlock** (flat fee, no tracking) |
| Book flight | Ticket + website markup + surge pricing | **Just the ticket price** |
| Price changes on repeat search? | Yes — goes up | **Never** |
| Total extra cost | $20–$50+ hidden in inflated price | **$1 flat** |

The $1 unlock is the only fee. You search for free, find exactly what you need with all the details, and only pay $1 to confirm the price and open checkout. After that, booking is free — you pay only the actual airline ticket price.

## Install

### CLI (Python — recommended for agents)

```bash
pip install boostedtravel
```

This gives you the `boostedtravel` command in your terminal:

```bash
# Register and get your API key
boostedtravel register --name my-agent --email you@example.com

# Save your key
export BOOSTEDTRAVEL_API_KEY=trav_...

# Search flights
boostedtravel search LHR JFK 2026-04-15

# Round trip with options
boostedtravel search LON BCN 2026-04-01 --return 2026-04-08 --cabin M --sort price

# Resolve a city to IATA codes
boostedtravel locations "New York"

# Unlock an offer ($1)
boostedtravel unlock off_xxx

# Book the flight
boostedtravel book off_xxx \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --email john.doe@example.com

# Check your profile & usage
boostedtravel me
```

All commands support `--json` for machine-readable output (perfect for agent pipelines):

```bash
boostedtravel search GDN BER 2026-03-03 --json | jq '.offers[0]'
```

### CLI (JavaScript/TypeScript)

```bash
npm install -g boostedtravel
```

Same commands, same interface:

```bash
boostedtravel search LHR JFK 2026-04-15 --json
boostedtravel unlock off_xxx
boostedtravel book off_xxx --passenger '...' --email john@example.com
```

### SDK (Python)

```python
from boostedtravel import BoostedTravel

bt = BoostedTravel(api_key="trav_...")
flights = bt.search("LHR", "JFK", "2026-04-15")
print(f"{flights.total_results} offers, cheapest: {flights.cheapest.summary()}")

# Unlock
unlocked = bt.unlock(flights.offers[0].id)

# Book
booking = bt.book(
    offer_id=unlocked.offer_id,
    passengers=[{"id": "pas_0", "given_name": "John", "family_name": "Doe", "born_on": "1990-01-15", "gender": "m", "title": "mr"}],
    contact_email="john.doe@example.com",
)
print(f"Booked! PNR: {booking.booking_reference}")
```

### SDK (JavaScript / TypeScript)

```typescript
import { BoostedTravel } from 'boostedtravel';

const bt = new BoostedTravel({ apiKey: 'trav_...' });
const flights = await bt.searchFlights({ origin: 'LHR', destination: 'JFK', dateFrom: '2026-04-15' });
console.log(`${flights.totalResults} offers`);
```

### MCP Server (Claude Desktop / Cursor / Windsurf)

For AI agents using Model Context Protocol:

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

## Packages

| Package | Install | What it is |
|---------|---------|------------|
| **Python SDK + CLI** | `pip install boostedtravel` | SDK + `boostedtravel` CLI command |
| **JS/TS SDK + CLI** | `npm install -g boostedtravel` | SDK + `boostedtravel` CLI command |
| **MCP Server** | `npx boostedtravel-mcp` | Model Context Protocol for Claude, Cursor, etc. |

## ⚠️ Important: Real Passenger Details

When booking, you **must** use the real passenger's email and legal name. The airline sends e-tickets directly to the email provided. Placeholder or fake data will cause booking failures or the passenger won't receive their ticket.

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

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on submitting issues and pull requests.

## Security

See [SECURITY.md](SECURITY.md) for our security policy and how to report vulnerabilities.

## For AI Agents

See [AGENTS.md](AGENTS.md) for agent-specific instructions, or [CLAUDE.md](CLAUDE.md) for codebase context.

## License

[MIT](LICENSE)
