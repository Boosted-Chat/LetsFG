# @boostedtravel/mcp-server

MCP (Model Context Protocol) server for BoostedTravel — flight search & booking tools for Claude Desktop, Cursor, and any MCP-compatible AI agent.

## Install

```bash
npm install -g @boostedtravel/mcp-server
# or use npx (no install needed)
```

## Configure

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "boostedtravel": {
      "command": "npx",
      "args": ["@boostedtravel/mcp-server"],
      "env": {
        "BOOSTEDTRAVEL_API_KEY": "trav_your_api_key",
        "BOOSTEDTRAVEL_BASE_URL": "https://api.boostedchat.com"
      }
    }
  }
}
```

### Cursor

Add to `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "boostedtravel": {
      "command": "npx",
      "args": ["@boostedtravel/mcp-server"],
      "env": {
        "BOOSTEDTRAVEL_API_KEY": "trav_your_api_key"
      }
    }
  }
}
```

## Available Tools

| Tool | Description | Cost |
|------|-------------|------|
| `search_flights` | Search 400+ airlines worldwide | FREE |
| `resolve_location` | City name → IATA code | FREE |
| `unlock_flight_offer` | Confirm price, reserve 30min | $1 |
| `book_flight` | Create real airline reservation | FREE |
| `setup_payment` | Attach payment card (once) | FREE |
| `get_agent_profile` | Usage stats & payment status | FREE |

## How It Works

1. Agent says "find me a flight from London to Barcelona"
2. MCP server calls `resolve_location("London")` → LON
3. Calls `search_flights(LON, BCN, date)` → offers with prices
4. Agent picks cheapest, calls `unlock_flight_offer` → $1, confirms price
5. Calls `book_flight` with passenger details → real PNR code

The agent has native tools — no API documentation needed, no URL building, just tool calls.

## Get an API Key

```bash
npx @boostedtravel/mcp-server  # Doesn't work without a key
```

Register via the API:
```bash
curl -X POST https://api.boostedchat.com/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "agent@example.com"}'
```

## License

MIT
