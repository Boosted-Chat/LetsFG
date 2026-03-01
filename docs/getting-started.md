# Getting Started

## Authentication

### 1. Register (one-time, no auth needed)

```bash
# CLI
boostedtravel register --name my-agent --email you@example.com

# cURL
curl -X POST https://api.boostedchat.com/api/v1/agents/register \
  -H "Content-Type: application/json" \
  -d '{"agent_name": "my-agent", "email": "you@example.com"}'

# Response:
# { "agent_id": "ag_xxx", "api_key": "trav_xxxxx...", "message": "..." }
```

### 2. Use the API Key

Every authenticated request requires the `X-API-Key` header:

```bash
# Set as environment variable (recommended)
export BOOSTEDTRAVEL_API_KEY=trav_...

# CLI reads it automatically
boostedtravel search LHR JFK 2026-04-15

# Or pass explicitly
boostedtravel search LHR JFK 2026-04-15 --api-key trav_...

# cURL
curl -X POST https://api.boostedchat.com/api/v1/flights/search \
  -H "X-API-Key: trav_..." \
  -H "Content-Type: application/json" \
  -d '{"origin": "LHR", "destination": "JFK", "date_from": "2026-04-15"}'
```

### 3. Python SDK

```python
from boostedtravel import BoostedTravel

# Option A: Pass directly
bt = BoostedTravel(api_key="trav_...")

# Option B: Read from environment (BOOSTEDTRAVEL_API_KEY)
bt = BoostedTravel()

# Option C: Register inline
creds = BoostedTravel.register("my-agent", "agent@example.com")
bt = BoostedTravel(api_key=creds["api_key"])
```

### 4. Setup Payment (required before unlock)

You must attach a payment method before you can unlock offers or book flights. This is a one-time step.

```bash
# CLI — opens Stripe to attach a card
boostedtravel setup-payment
```

```python
# Python SDK — multiple options:

# Option A: Stripe test token (for development)
bt.setup_payment(token="tok_visa")

# Option B: Stripe PaymentMethod ID (from Stripe.js or Elements)
bt.setup_payment(payment_method_id="pm_xxx")

# Option C: Raw card details (requires PCI-compliant Stripe account)
bt.setup_payment(card_number="4242424242424242", exp_month=12, exp_year=2027, cvc="123")
```

```bash
# cURL
curl -X POST https://api.boostedchat.com/api/v1/agents/setup-payment \
  -H "X-API-Key: trav_..." \
  -H "Content-Type: application/json" \
  -d '{"token": "tok_visa"}'
```

After setup, all charges ($1 unlock) are automatic — no further payment interaction needed.

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
boostedtravel me
# Agent: my-agent
# Payment: active
# Searches: 42
# Bookings: 3
```

### Authentication Failure Handling

```python
from boostedtravel import BoostedTravel, AuthenticationError

try:
    bt = BoostedTravel(api_key="trav_invalid_key")
    flights = bt.search("LHR", "JFK", "2026-04-15")
except AuthenticationError:
    # HTTP 401 — key is missing, invalid, or expired
    print("Invalid API key. Register a new one:")
    creds = BoostedTravel.register("my-agent", "agent@example.com")
    bt = BoostedTravel(api_key=creds["api_key"])
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

## Multi-Passenger Examples

```bash
# Family trip: 2 adults + 2 children, economy
boostedtravel search LHR BCN 2026-07-15 --return 2026-07-22 --adults 2 --children 2 --cabin M

# Business trip: 3 adults, business class, direct flights only
boostedtravel search JFK LHR 2026-05-01 --adults 3 --cabin C --max-stops 0

# Solo round-trip, first class, sorted by duration
boostedtravel search LAX NRT 2026-08-10 --return 2026-08-24 --cabin F --sort duration
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
