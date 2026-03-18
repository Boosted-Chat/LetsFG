---
name: letsfg
description: "LetsFG — Agent-native flight search, hotel search, and booking API. 400+ airlines, zero markup, 20-50 USD cheaper than OTAs. letsfg.co"
---

# SKILL.md — LetsFG Capabilities

> Machine-readable skill manifest for AI agents and documentation indexers.
## Identity

- **Name:** LetsFG
- **Type:** API + SDK + MCP Server + CLI
- **Purpose:** Agent-native flight search, hotel search, and booking
- **Compatible agents:** OpenClaw, Perplexity Computer, Claude Desktop, Cursor, Windsurf, and any MCP-compatible client
- **API Base URL:** `https://api.letsfg.co`
- **MCP Endpoint:** `https://api.letsfg.co/mcp` (Streamable HTTP)
- **Packages:** PyPI `letsfg` · npm `letsfg` · npm `letsfg-mcp`
- **License:** MIT

## Skills

### search_flights
Search 400+ airlines worldwide via NDC and GDS. Returns real-time prices with zero markup or bias — $20–50 cheaper than OTAs.
- **Cost:** FREE (unlimited)
- **Input:** origin (IATA), destination (IATA), date_from, optional: date_to, return_from, return_to, adults, children, infants, cabin_class (M/W/C/F), max_stopovers, currency, sort, limit
- **Output:** List of flight offers with price, airlines, times, segments, conditions, passenger_ids
- **Note:** All offers are locked. Must unlock before booking.

### search_hotels
Search 300,000+ hotels worldwide via Amadeus GDS + Hotelbeds B2B wholesale net rates.
- **Cost:** FREE
- **Input:** location (city name or IATA), checkin, checkout, adults, children, rooms, min_stars, max_price, currency, sort, limit
- **Output:** List of hotel offers with name, address, stars, rating, photos, rooms, prices, cancellation policies

### search_transfers
Search ground transfers — private cars, taxis, shared shuttles, airport express.
- **Cost:** FREE
- **Input:** origin, destination, date, passengers
- **Output:** Transfer options with prices and vehicle types

### search_activities
Search 300,000+ activities — tours, museum tickets, day trips via Amadeus + Hotelbeds wholesale.
- **Cost:** FREE
- **Input:** location, date_from, date_to
- **Output:** Activity options with prices, descriptions, availability

### resolve_location
Resolve city names to IATA airport/city codes.
- **Cost:** FREE
- **Input:** query (city name, e.g. "London")
- **Output:** List of matching IATA codes (e.g. LON, LHR, LGW, STN, LTN, LCY)

### get_system_profile
Get system resource profile and recommended browser concurrency for local search.
- **Cost:** FREE
- **Input:** (none)
- **Output:** ram_total_gb, ram_available_gb, cpu_cores, recommended_max_browsers, tier (minimal/low/moderate/standard/high/maximum), platform
- **Tiers:** minimal (<2GB, 2 browsers), low (2-4GB, 3), moderate (4-8GB, 5), standard (8-16GB, 8), high (16-32GB, 12), maximum (32+GB, 16)
- **Python:** `from letsfg import get_system_profile; profile = get_system_profile()`
- **CLI:** `letsfg system-info` or `letsfg system-info --json`
- **MCP:** `system_info` tool
- **JS:** `import { systemInfo } from 'letsfg'; const info = await systemInfo();`

### configure_max_browsers
Override auto-detected browser concurrency limit for local search.
- **Cost:** FREE
- **Input:** max_browsers (integer, 1–32)
- **Priority:** env var `LETSFG_MAX_BROWSERS` > explicit config > auto-detect from RAM
- **Python:** `from letsfg import configure_max_browsers; configure_max_browsers(4)`
- **CLI:** `letsfg search-local LHR BCN 2026-04-15 --max-browsers 4`
- **MCP:** `search_flights` tool with `max_browsers` parameter
- **JS:** `await searchLocal('LHR', 'BCN', '2026-04-15', { maxBrowsers: 4 })`

### unlock_flight_offer
Confirm live price with airline and reserve offer for 30 minutes. FREE with GitHub star.
- **Cost:** FREE (requires GitHub star verification)
- **Endpoint:** `POST /api/v1/bookings/unlock`
- **Input:** offer_id from search results (only required parameter)
- **Output:** confirmed_price, confirmed_currency, offer_expires_at
- **Prerequisite:** GitHub star verified via `link_github` first
- **HTTP 403:** GitHub star not verified — call link_github first
- **HTTP 410:** Offer expired — airline sold the seats, search again (OfferExpiredError)
- **Note:** confirmed_price may differ from search price (airline prices change in real-time). After unlock, you have 30 minutes to call book. If the window expires, search again (free) and unlock again (free).
- **Python:** `unlocked = bt.unlock(offer_id)` → returns UnlockResult
- **CLI:** `letsfg unlock off_xxx`
- **JS/TS:** `const unlocked = await bt.unlock(offerId)`

### book_flight
Create a real airline reservation with PNR code. Charges ticket price via Stripe before booking.
- **Cost:** Ticket price + Stripe processing fee (2.9% + 30¢). Zero markup — LetsFG does not add any margin.
- **Prerequisite:** Payment method must be attached via `setup_payment` first.
- **Input:** offer_id, passengers (id, given_name, family_name, born_on, gender, title, email, phone_number), contact_email
- **Output:** booking_reference (airline PNR), status, flight_price, currency
- **CRITICAL:** Use real passenger names (must match passport/ID) and real email (airline sends e-ticket there)
- **Payment flow:** Your Stripe card is charged the ticket price → LetsFG books via the airline → you get the PNR. If the airline booking fails, you are automatically refunded.

### hotel_checkrate
Confirm hotel rate before booking (required if rate_type=RECHECK).
- **Cost:** FREE
- **Input:** rate_keys from hotel search
- **Output:** Confirmed price, board type, cancellation policy, rate comments

### hotel_book
Book a hotel room.
- **Cost:** Room price (charged via Hotelbeds)
- **Input:** holder_name, holder_surname, rooms with rate_key and paxes
- **Output:** reference, status, hotel details, total_net

### hotel_voucher
Get guest voucher for hotel check-in.
- **Cost:** FREE
- **Input:** booking reference
- **Output:** Hotel name, dates, room type, board, payment notice

### hotel_cancel
Cancel a hotel booking or simulate cancellation.
- **Cost:** Depends on cancellation policy
- **Input:** reference, simulate (true/false)
- **Output:** cancellation_amount, currency, status

### register
Register a new AI agent.
- **Cost:** FREE
- **Input:** agent_name, email
- **Output:** api_key (permanent credential)

### setup_payment
Attach a payment card for booking. **Required before booking Duffel/GDS flights.**
- **Cost:** FREE (attaching the card is free; you are charged the ticket price when you book)
- **Input:** token (e.g. "tok_visa" for testing) or payment_method_id or card details
- **Output:** Payment status confirmation
- **Note:** Must be called once before your first booking. The card stays on file for future bookings.

### get_agent_profile
Get current agent's profile, usage stats, and payment status.
- **Cost:** FREE
- **Output:** Agent details, search count, booking count, payment status

## Authentication

All endpoints except `register` require an `X-API-Key` header.

```
X-API-Key: trav_...
```

Get your key by calling `POST /api/v1/agents/register` with agent_name and email. The key is permanent — save it once.

After registration, star the GitHub repo and link your account via `POST /api/v1/agents/link-github` to unlock and book for free.

## Complete Workflow

### Flight Booking (6 API calls)

```
1. POST /api/v1/agents/register        → Get API key (once)
2. POST /api/v1/agents/link-github     → Star repo + verify (once)
3. POST /api/v1/agents/setup-payment   → Attach payment card (once)
4. POST /api/v1/flights/search         → Search flights (FREE)
5. POST /api/v1/bookings/unlock        → Unlock offer (FREE)
6. POST /api/v1/bookings/book          → Book flight (ticket price charged via Stripe)
```

### Hotel Booking (6 API calls)

```
1. POST /api/v1/agents/register        → Get API key (once)
2. POST /api/v1/agents/link-github     → Star repo + verify (once)
3. POST /api/v1/hotels/search          → Search hotels (FREE)
4. POST /api/v1/hotels/checkrate       → Confirm price (if rate_type=RECHECK)
5. POST /api/v1/hotels/book            → Book room
6. GET  /api/v1/hotels/voucher/{ref}   → Get guest voucher
```

## CLI Usage

```bash
pip install letsfg

letsfg register --name my-agent --email me@example.com
export LETSFG_API_KEY=trav_...

# Search flights
letsfg search LHR JFK 2026-04-15
letsfg search LON BCN 2026-04-01 --return 2026-04-08 --cabin C --sort price
letsfg search GDN BER 2026-05-10 --adults 2 --children 1

# Resolve locations
letsfg locations "New York"

# Unlock and book
letsfg unlock off_xxx
letsfg book off_xxx \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --email john.doe@example.com

# Machine-readable output
letsfg search GDN BER 2026-03-03 --json
```

## Python SDK Usage

```python
from letsfg import LetsFG

bt = LetsFG(api_key="trav_...")

# Search
results = bt.search("LHR", "JFK", "2026-04-15")
for offer in results.offers:
    print(f"{offer.price} {offer.currency} — {', '.join(offer.airlines)}")

# Unlock
unlocked = bt.unlock(results.offers[0].id)
print(f"Confirmed: {unlocked.confirmed_price} {unlocked.confirmed_currency}")

# Book
booking = bt.book(
    offer_id=results.offers[0].id,
    passengers=[{
        "id": results.passenger_ids[0],
        "given_name": "John",
        "family_name": "Doe",
        "born_on": "1990-01-15",
        "gender": "m",
        "title": "mr",
        "email": "john@example.com",
        "phone_number": "+447123456789",
    }],
    contact_email="john@example.com",
)
print(f"PNR: {booking.booking_reference}")
```

## MCP Server Setup

```json
{
  "mcpServers": {
    "letsfg": {
      "url": "https://api.letsfg.co/mcp",
      "headers": {
        "X-API-Key": "trav_..."
      }
    }
  }
}
```

Or run locally:

```bash
npm install -g letsfg-mcp
LETSFG_API_KEY=trav_... letsfg-mcp
```

## MCP Tools

| Tool | Description | Cost |
|------|-------------|------|
| `search_flights` | Search 400+ airlines worldwide | FREE |
| `resolve_location` | City name → IATA code | FREE |
| `link_github` | Star repo for free access (once) | FREE |
| `unlock_flight_offer` | Confirm price, reserve 30min | FREE |
| `book_flight` | Create real airline reservation | Ticket price |
| `setup_payment` | Attach payment card (required for booking) | FREE |
| `get_agent_profile` | View usage stats | FREE |

## Search Flags Reference

| Flag | API Field | Values | Default |
|------|-----------|--------|---------|
| `--adults` | `adults` | 1–9 | 1 |
| `--children` | `children` | 0–9 | 0 |
| `--infants` | `infants` | 0–9 | 0 |
| `--cabin` | `cabin_class` | M (economy), W (premium), C (business), F (first) | _(any)_ |
| `--return` | `return_from` | YYYY-MM-DD | — |
| `--max-stops` | `max_stopovers` | 0–4 | 2 |
| `--sort` | `sort` | price, duration | price |
| `--limit` | `limit` | 1–100 | 20 |
| `--currency` | `currency` | EUR, USD, GBP, etc. | EUR |

### Cabin Class Codes Explained

| Code | Class | Description | Typical Use Case |
|------|-------|-------------|------------------|
| `M` | Economy | Standard seating | Budget travel, most bookings |
| `W` | Premium Economy | Extra legroom, priority boarding | Long-haul comfort without business price |
| `C` | Business | Lie-flat on long-haul, lounge access | Corporate travel, 6+ hour flights |
| `F` | First | Private suites, premium dining | Ultra-premium routes (limited airlines) |
| `--json` | — | Output as JSON | — |

## Error Handling

| Exception | HTTP Code | When |
|-----------|-----------|------|
| `AuthenticationError` | 401 | Invalid or missing API key |
| `PaymentRequiredError` | 402 | No payment method (legacy flow) |
| `OfferExpiredError` | 410 | Offer no longer available |
| `LetsFGError` | 422 | Invalid request parameters |
| `LetsFGError` | 429 | Too many requests (retry with backoff) |
| `LetsFGError` | 502 | Upstream airline/hotel API error |

### Authentication Failure Recovery

```python
from letsfg import LetsFG, AuthenticationError

try:
    bt = LetsFG(api_key="trav_...")
    flights = bt.search("LHR", "JFK", "2026-04-15")
except AuthenticationError:
    # API key invalid or expired — re-register
    creds = LetsFG.register("my-agent", "agent@example.com")
    bt = LetsFG(api_key=creds["api_key"])
    # Don't forget to link GitHub
    bt.link_github("your-github-username")
```

### Rate Limit and Timeout Handling

```python
import time
from letsfg import LetsFG, LetsFGError

def search_with_retry(bt, origin, dest, date, max_retries=3):
    for attempt in range(max_retries):
        try:
            return bt.search(origin, dest, date)
        except LetsFGError as e:
            if "429" in str(e) or "rate limit" in str(e).lower():
                time.sleep(2 ** attempt)  # exponential backoff
            elif "timeout" in str(e).lower() or "504" in str(e):
                time.sleep(1)
            else:
                raise
    raise LetsFGError("Max retries exceeded")
```

## Rate Limits

| Endpoint | Rate Limit | Typical Latency |
|----------|-----------|------------------|
| Search flights | 60 req/min | 2-15s |
| Resolve location | 120 req/min | <1s |
| Unlock | 20 req/min | 2-5s |
| Book | 10 req/min | 3-10s |
| Search hotels | 30 req/min | 3-10s |
| Register | 5 req/min | <1s |

## Pricing Summary

| Action | Cost |
|--------|------|
| Search (flights, hotels, transfers, activities) | **Free** |
| Resolve locations | **Free** |
| Register agent | **Free** |
| Setup payment | **Free** |
| View profile | **Free** |
| Unlock offer | **Free** |
| Book flight (after unlock) | **Ticket price** (zero markup, Stripe processing fee only) |
| Hotel booking | Room price only |
| Hotel cancellation | Per cancellation policy |

## Key Facts

- 400+ airlines via NDC and GDS (Duffel)
- 300,000+ hotels via Amadeus GDS + Hotelbeds B2B
- 300,000+ activities via Amadeus + Hotelbeds wholesale
- Zero price bias — no demand inflation, no cookie tracking
- $20–50 cheaper than OTAs on average
- Real airline PNR codes and hotel confirmations
- E-tickets sent directly to passenger email
- Search is always free and unlimited
- Only requirement: star our GitHub repo for unlimited access
- API designed for machines, not browsers
