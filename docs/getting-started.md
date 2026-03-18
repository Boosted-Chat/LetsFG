# Getting Started

> 🎬 **[Watch the demo](https://github.com/LetsFG/LetsFG#demo-lfg-vs-default-agent-search)** — see LetsFG in action vs default agent search (OpenClaw, Perplexity Computer).

## One-Click Install (No API Key Needed)

```bash
pip install letsfg
```

That's it. Search flights immediately:

```bash
letsfg search-local LHR BCN 2026-04-15
```

This runs 102 airline connectors locally on your machine — Ryanair, Wizz Air, EasyJet, Southwest, AirAsia, Norwegian, Qatar Airways, LATAM, Finnair, and 93 more. Completely free, unlimited, zero configuration.

```python
from letsfg.local import search_local

# Free, runs all relevant connectors on your machine
result = await search_local("GDN", "BCN", "2026-06-15")
for offer in result.offers[:5]:
    print(f"{offer.airlines[0]}: {offer.currency} {offer.price}")
```

---

## Unlock Full Power with an API Key (Recommended)

Adding an API key connects you to LetsFG's enterprise backend — GDS/NDC providers (Amadeus, Duffel, Sabre, Travelport, Kiwi) with 400+ premium carriers like Lufthansa, British Airways, Emirates, and Singapore Airlines. Both local connectors and cloud sources run simultaneously, giving you the best price from across the entire internet.

### 1. Register (one command, free, instant)

```bash
# CLI
letsfg register --name my-agent --email you@example.com

# cURL
curl -X POST https://api.letsfg.co/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "you@example.com"}'

# Response:
# { "agent_id": "ag_xxx", "api_key": "trav_xxxxx...", "message": "..." }
```

### 2. Use the API Key

Every authenticated request requires the `X-API-Key` header:

```bash
# Set as environment variable (recommended)
export LETSFG_API_KEY=trav_...

# CLI reads it automatically
letsfg search LHR JFK 2026-04-15

# Or pass explicitly
letsfg search LHR JFK 2026-04-15 --api-key trav_...

# cURL
curl -X POST https://api.letsfg.co/api/v1/flights/search \
  -H "X-API-Key: trav_..." \
  -H "Content-Type: application/json" \
  -d '{"origin": "LHR", "destination": "JFK", "date_from": "2026-04-15"}'
```

### 3. Python SDK

```python
from letsfg import LetsFG

# Option A: Pass directly
bt = LetsFG(api_key="trav_...")

# Option B: Read from environment (LETSFG_API_KEY)
bt = LetsFG()

# Option C: Register inline
creds = LetsFG.register("my-agent", "agent@example.com")
bt = LetsFG(api_key=creds["api_key"])
```

### 4. Link GitHub (required before unlock)

Star the LetsFG repo and link your GitHub account for free unlimited access. This is a one-time step.

```bash
# 1. Star the repo: https://github.com/LetsFG/LetsFG
# 2. Link your GitHub username:
letsfg star --github your-username
```

```python
# Python SDK
bt.link_github("your-username")
```

```bash
# cURL
curl -X POST https://api.letsfg.co/api/v1/agents/link-github \
  -H "X-API-Key: trav_..." \
  -H "Content-Type: application/json" \
  -d '{"github_username": "your-username"}'
```

After verification, all operations are free — no further setup needed.

### 5. Verify Authentication Works

```python
# Check your agent profile — confirms key and payment status
profile = bt.me()
print(f"Agent: {profile.agent_name}")
print(f"Payment: {profile.payment_status}")
print(f"Searches: {profile.search_count}")
print(f"Bookings: {profile.booking_count}")
```

```bash
letsfg me
# Agent: my-agent
# Payment: active
# Searches: 42
# Bookings: 3
```

### Authentication Failure Handling

```python
from letsfg import LetsFG, AuthenticationError

try:
    bt = LetsFG(api_key="trav_invalid_key")
    flights = bt.search("LHR", "JFK", "2026-04-15")
except AuthenticationError:
    # HTTP 401 — key is missing, invalid, or expired
    print("Invalid API key. Register a new one:")
    creds = LetsFG.register("my-agent", "agent@example.com")
    bt = LetsFG(api_key=creds["api_key"])
    # Don't forget to set up payment after re-registering
    bt.setup_payment(token="tok_visa")
```

---

## Search Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--return` | `-r` | _(one-way)_ | Return date for round-trip (YYYY-MM-DD) |
| `--adults` | `-a` | `1` | Number of adult passengers (1–9) |
| `--children` | | `0` | Number of children (2–11 years) |
| `--cabin` | `-c` | _(any)_ | Cabin class (see below) |
| `--max-stops` | `-s` | `2` | Maximum stopovers per direction (0–4) |
| `--currency` | | `EUR` | 3-letter currency code |
| `--limit` | `-l` | `20` | Maximum number of results (1–100) |
| `--sort` | | `price` | Sort by `price` or `duration` |
| `--json` | `-j` | | Output raw JSON (for agents/scripts) |
| `--max-browsers` | `-b` | _(auto)_ | Max concurrent browsers for local search (1–32) |

## Multi-Passenger Examples

```bash
# Family trip: 2 adults + 2 children, economy
letsfg search LHR BCN 2026-07-15 --return 2026-07-22 --adults 2 --children 2 --cabin M

# Business trip: 3 adults, business class, direct flights only
letsfg search JFK LHR 2026-05-01 --adults 3 --cabin C --max-stops 0

# Solo round-trip, first class, sorted by duration
letsfg search LAX NRT 2026-08-10 --return 2026-08-24 --cabin F --sort duration
```

When you search with multiple passengers, the response includes `passenger_ids` (e.g., `["pas_0", "pas_1", "pas_2"]`). You must provide passenger details for **each** ID when booking.

## Cabin Class Codes

| Code | Class | Typical Use Case |
|------|-------|-----------------|
| `M` | Economy | Standard seating, cheapest fares |
| `W` | Premium Economy | Extra legroom, better meals, priority boarding |
| `C` | Business | Lie-flat seats on long-haul, lounge access, flexible tickets |
| `F` | First | Top-tier service, suites on some airlines, maximum comfort |

If omitted, the search returns all cabin classes. Specify a cabin code to filter results to that class only.

---

## Performance Tuning

LetsFG auto-detects system RAM and scales browser concurrency. This prevents Chrome from overwhelming low-end machines while maximizing throughput on powerful ones.

| Available RAM | Tier | Max Browsers |
|-------------|------|-------------|
| < 2 GB | Minimal | 2 |
| 2–4 GB | Low | 3 |
| 4–8 GB | Moderate | 5 |
| 8–16 GB | Standard | 8 |
| 16–32 GB | High | 12 |
| 32+ GB | Maximum | 16 |

### Check Your System

```bash
letsfg system-info
```

### Override Auto-Detection

```bash
# Environment variable (highest priority)
export LETSFG_MAX_BROWSERS=4

# CLI flag (per-search)
letsfg search-local LHR BCN 2026-04-15 --max-browsers 4
```

```python
from letsfg import configure_max_browsers, get_system_profile

profile = get_system_profile()
print(f"Tier: {profile['tier']}, recommended: {profile['recommended_max_browsers']}")

# Set explicitly
configure_max_browsers(4)
```

Priority order: `LETSFG_MAX_BROWSERS` env var > explicit config > auto-detect from RAM.
