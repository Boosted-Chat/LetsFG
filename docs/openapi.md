# OpenAPI Reference

LetsFG provides a full OpenAPI 3.1 specification for the REST API.

## Interactive Documentation

- **Swagger UI:** [api.letsfg.co/docs](https://api.letsfg.co/docs) — try every endpoint in your browser
- **ReDoc:** [api.letsfg.co/redoc](https://api.letsfg.co/redoc) — clean, readable API reference

## OpenAPI Spec

The full OpenAPI specification is included in the repository:

- **YAML:** [`openapi.yaml`](https://github.com/LetsFG/LetsFG/blob/main/openapi.yaml)

You can import this spec into any OpenAPI-compatible tool (Postman, Insomnia, Swagger Editor, etc.).

## Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/api/v1/agents/register` | POST | None | Create account, get API key |
| `/api/v1/agents/me` | GET | API key | Agent profile & usage stats |
| `/api/v1/agents/setup-payment` | POST | API key | Attach Stripe payment method |
| `/api/v1/flights/search` | POST | API key | Search 400+ airlines |
| `/api/v1/flights/resolve-location` | GET | API key | Resolve city/airport to IATA codes |
| `/api/v1/bookings/unlock` | POST | API key | Unlock offer (free) |
| `/api/v1/bookings/book` | POST | API key | Book flight (ticket price charged via Stripe) |

**Base URL:** `https://api.letsfg.co`

## Authentication

All endpoints except `/register` require an `X-API-Key` header:

```bash
curl -X POST https://api.letsfg.co/api/v1/flights/search \
  -H "X-API-Key: trav_your_api_key" \
  -H "Content-Type: application/json" \
  -d '{"origin": "LHR", "destination": "JFK", "date_from": "2026-04-15"}'
```

## Agent Discovery

LetsFG supports standard agent discovery protocols:

| URL | Description |
|-----|-------------|
| `https://api.letsfg.co/.well-known/ai-plugin.json` | OpenAI plugin manifest |
| `https://api.letsfg.co/.well-known/agent.json` | Agent discovery manifest |
| `https://api.letsfg.co/llms.txt` | LLM instructions |
| `https://api.letsfg.co/mcp` | MCP Streamable HTTP endpoint |

## Local Search (No API Key)

The 102 local airline connectors do not use the REST API — they run directly on your machine. No API key is needed:

```bash
pip install letsfg
letsfg search-local LHR BCN 2026-04-15
```

```python
from letsfg.local import search_local
result = await search_local("LHR", "BCN", "2026-04-15")
```
