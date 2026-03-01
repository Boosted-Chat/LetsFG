# BoostedTravel — Agent-Native Flight Search & Booking

Search 400+ airlines at raw airline prices — **$20-50 cheaper** than Booking.com, Kayak, and other OTAs. Zero browser, zero markup, zero config. Built for autonomous AI agents.

## Install

```bash
pip install boostedtravel           # SDK only (zero dependencies)
pip install boostedtravel[cli]      # SDK + CLI (adds typer, rich)
```

## Quick Start (Python)

```python
from boostedtravel import BoostedTravel

# Register (one-time)
creds = BoostedTravel.register("my-agent", "agent@example.com")
print(creds["api_key"])  # Save this

# Use
bt = BoostedTravel(api_key="trav_...")

# Search flights — FREE
flights = bt.search("GDN", "BER", "2026-03-03")
print(f"{flights.total_results} offers, cheapest: {flights.cheapest.summary()}")

# Unlock — $1
unlock = bt.unlock(flights.cheapest.id)
print(f"Confirmed price: {unlock.confirmed_currency} {unlock.confirmed_price}")

# Book — FREE after unlock
booking = bt.book(
    offer_id=flights.cheapest.id,
    passengers=[{
        "id": flights.passenger_ids[0],
        "given_name": "John",
        "family_name": "Doe",
        "born_on": "1990-01-15",
        "gender": "m",
        "title": "mr",
        "email": "john@example.com",
    }],
    contact_email="john@example.com"
)
print(f"PNR: {booking.booking_reference}")
```

## Quick Start (CLI)

```bash
export BOOSTEDTRAVEL_API_KEY=trav_...

# Search
boostedtravel search GDN BER 2026-03-03 --sort price

# Machine-readable output (for agents)
boostedtravel search LON BCN 2026-04-01 --json

# Unlock
boostedtravel unlock off_xxx

# Book
boostedtravel book off_xxx \
  --passenger '{"id":"pas_xxx","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr","email":"john@example.com"}' \
  --email john@example.com

# Resolve location
boostedtravel locations "Berlin"
```

## All CLI Commands

| Command | Description | Cost |
|---------|-------------|------|
| `search` | Search flights between any two airports | FREE |
| `locations` | Resolve city name to IATA codes | FREE |
| `unlock` | Unlock offer (confirms price, reserves 30min) | $1 |
| `book` | Book flight (creates real airline PNR) | FREE |
| `register` | Register new agent, get API key | FREE |
| `setup-payment` | Attach payment card (payment token) | FREE |
| `me` | Show agent profile and usage stats | FREE |

Every command supports `--json` for machine-readable output.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `BOOSTEDTRAVEL_API_KEY` | Your agent API key |
| `BOOSTEDTRAVEL_BASE_URL` | API URL (default: `https://api.boostedchat.com`) |

## How It Works

1. **Search** — Free, unlimited. Returns real-time offers from 400+ airlines via NDC/GDS.
2. **Unlock** — $1 proof-of-intent. Confirms latest price with airline, reserves offer for 30 minutes.
3. **Book** — FREE after unlock. Creates real airline reservation with PNR code.

Prices are cheaper because we connect directly to airlines — no OTA markup.

## For Agents

The SDK uses **zero external dependencies** (only Python stdlib `urllib`). This means:
- Safe to install in sandboxed environments
- No dependency conflicts
- Minimal attack surface
- Works on Python 3.10+

The `--json` flag on every CLI command outputs structured JSON for easy parsing by agents.

## License

MIT
