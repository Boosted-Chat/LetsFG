# API Guide

## Two Ways to Search Flights

LetsFG offers two search modes:

| Mode | Command / Method | API Key | Coverage | Cost |
|------|-----------------|---------|----------|------|
| **Local search** | `search-local` / `search_local()` | **Not needed** | 102 airline connectors | Free |
| **Full search** | `search` / `bt.search()` | Required | 400+ airlines (GDS/NDC) | Free (star repo) |

### Local Search (No API Key)

Search 102 airline websites directly — completely free, no registration required:

```python
from letsfg.local import search_local

# Async — queries airline sites directly
result = await search_local("LHR", "BCN", "2026-04-15")
for offer in result.offers:
    print(f"{offer.airlines[0]}: {offer.currency} {offer.price}")

# Limit browser concurrency on constrained machines
result = await search_local("LHR", "BCN", "2026-04-15", max_browsers=4)
```

```bash
# CLI — works immediately after pip install
letsfg search-local LHR BCN 2026-04-15

# Limit browsers
letsfg search-local LHR BCN 2026-04-15 --max-browsers 4
```

### Full Search (API Key)

Search 400+ airlines via GDS and NDC connections:

```python
from letsfg import LetsFG
bt = LetsFG(api_key="trav_...")
flights = bt.search("LHR", "BCN", "2026-04-15")
```

---

## Error Handling

The SDK raises specific exceptions for each failure mode:

| Exception | HTTP Code | When it happens |
|-----------|-----------|-----------------|
| `AuthenticationError` | 401 | Missing or invalid API key |
| `PaymentRequiredError` | 402 | No payment method set up (call `setup-payment` first) |
| `OfferExpiredError` | 410 | Offer no longer available (search again) |
| `LetsFGError` | any | Base class — catches all API errors |

### Python Error Handling

```python
from letsfg import (
    LetsFG, LetsFGError,
    AuthenticationError, PaymentRequiredError, OfferExpiredError,
)

bt = LetsFG(api_key="trav_...")

# Search — handle invalid locations
try:
    flights = bt.search("INVALID", "JFK", "2026-04-15")
except LetsFGError as e:
    if e.status_code == 422:
        print(f"Invalid location: {e.message}")
        # Resolve the location first
        locations = bt.resolve_location("New York")
        iata = locations[0]["iata_code"]  # "JFK"
        flights = bt.search("LHR", iata, "2026-04-15")
    else:
        raise

# Unlock — handle payment and expiry
try:
    unlocked = bt.unlock(flights.cheapest.id)
except PaymentRequiredError:
    print("Set up payment first: bt.setup_payment('tok_visa')")
except OfferExpiredError:
    print("Offer expired — search again for fresh results")

# Book — handle all errors
try:
    booking = bt.book(
        offer_id=unlocked.offer_id,
        passengers=[{
            "id": flights.passenger_ids[0],
            "given_name": "John",
            "family_name": "Doe",
            "born_on": "1990-01-15",
            "gender": "m",
            "title": "mr",
        }],
        contact_email="john@example.com",
    )
    print(f"Booked! PNR: {booking.booking_reference}")
except OfferExpiredError:
    print("Offer expired after unlock — search again (30min window may have passed)")
except LetsFGError as e:
    print(f"Booking failed ({e.status_code}): {e.message}")
```

### CLI Error Handling

The CLI exits with code 1 on errors and prints the message to stderr. Use `--json` for parseable error output:

```bash
# Check exit code in scripts
if ! letsfg search INVALID JFK 2026-04-15 --json 2>/dev/null; then
  echo "Search failed — check location codes"
fi
```

---

## Working with Search Results

Search returns offers from multiple airlines. Each offer includes price, airlines, route, duration, stopovers, and booking conditions.

### Python — Filter and Sort Results

```python
flights = bt.search("LON", "BCN", "2026-04-01", return_date="2026-04-08")

# Access all offers
for offer in flights.offers:
    print(f"{offer.owner_airline}: {offer.currency} {offer.price}")
    print(f"  Route: {offer.outbound.route_str}")
    print(f"  Duration: {offer.outbound.total_duration_seconds // 3600}h")
    print(f"  Stops: {offer.outbound.stopovers}")
    print(f"  Refundable: {offer.conditions.get('refund_before_departure', 'unknown')}")
    print(f"  Changeable: {offer.conditions.get('change_before_departure', 'unknown')}")

# Filter: only direct flights
direct = [o for o in flights.offers if o.outbound.stopovers == 0]

# Filter: only a specific airline
ba_flights = [o for o in flights.offers if "British Airways" in o.airlines]

# Filter: refundable only
refundable = [o for o in flights.offers if o.conditions.get("refund_before_departure") == "allowed"]

# Sort by duration (search already sorts by price by default)
by_duration = sorted(flights.offers, key=lambda o: o.outbound.total_duration_seconds)

# Get the cheapest
cheapest = flights.cheapest
print(f"Best price: {cheapest.price} {cheapest.currency} on {cheapest.owner_airline}")
```

### CLI — JSON Output for Agents

```bash
# Get structured JSON output
letsfg search LON BCN 2026-04-01 --return 2026-04-08 --json

# Pipe to jq for filtering
letsfg search LON BCN 2026-04-01 --json | jq '[.offers[] | select(.stopovers == 0)]'
letsfg search LON BCN 2026-04-01 --json | jq '.offers | sort_by(.duration_seconds) | .[0]'
```

### JSON Response Structure

```json
{
  "passenger_ids": ["pas_0", "pas_1"],
  "total_results": 47,
  "offers": [
    {
      "id": "off_xxx",
      "price": 89.50,
      "currency": "EUR",
      "airlines": ["Ryanair"],
      "owner_airline": "Ryanair",
      "route": "STN → BCN",
      "duration_seconds": 7800,
      "stopovers": 0,
      "conditions": {
        "refund_before_departure": "not_allowed",
        "change_before_departure": "allowed_with_fee"
      },
      "is_locked": false
    }
  ]
}
```

---

## Resolve Locations

Always resolve city names to IATA codes before searching. This avoids errors from invalid or ambiguous location names:

```python
# Resolve a city name
locations = bt.resolve_location("New York")
# Returns: [{"iata_code": "JFK", "name": "John F. Kennedy", "type": "airport", "city": "New York"}, ...]

# Use the IATA code in search
flights = bt.search(locations[0]["iata_code"], "LAX", "2026-04-15")
```

```bash
# CLI
letsfg locations "New York"
# Output:
#   JFK  John F. Kennedy International Airport
#   LGA  LaGuardia Airport
#   EWR  Newark Liberty International Airport
#   NYC  New York (all airports)
```

### Handling Ambiguous Locations

When a city has multiple airports, you have two strategies:

```python
locations = bt.resolve_location("London")
# Returns: LHR, LGW, STN, LTN, LCY, LON

# Strategy 1: Use the CITY code (searches ALL airports in that city)
flights = bt.search("LON", "BCN", "2026-04-01")  # all London airports

# Strategy 2: Use a specific AIRPORT code (only that airport)
flights = bt.search("LHR", "BCN", "2026-04-01")  # Heathrow only

# Strategy 3: Search multiple airports and compare (free!)
for loc in locations:
    if loc["type"] == "airport":
        result = bt.search(loc["iata_code"], "BCN", "2026-04-01")
        if result.offers:
            print(f"{loc['name']} ({loc['iata_code']}): cheapest {result.cheapest.price} {result.cheapest.currency}")
```

**Rule of thumb:** Use the city code (3-letter, e.g. `LON`, `NYC`, `PAR`) when you want the broadest search across all airports. Use a specific airport code when the user has a preference.

---

## Complete Search-to-Booking Workflow

### Python — Full Workflow

```python
from letsfg import (
    LetsFG, LetsFGError,
    AuthenticationError, PaymentRequiredError, OfferExpiredError,
)

def search_and_book(origin_city, dest_city, date, passenger_info, email):
    bt = LetsFG()  # reads LETSFG_API_KEY from env

    # Step 1: Resolve locations
    origins = bt.resolve_location(origin_city)
    dests = bt.resolve_location(dest_city)
    if not origins or not dests:
        raise ValueError(f"Could not resolve: {origin_city} or {dest_city}")
    origin_iata = origins[0]["iata_code"]
    dest_iata = dests[0]["iata_code"]

    # Step 2: Search (free)
    flights = bt.search(origin_iata, dest_iata, date, sort="price")
    if not flights.offers:
        print(f"No flights found {origin_iata} → {dest_iata} on {date}")
        return None

    print(f"Found {flights.total_results} offers")
    print(f"Cheapest: {flights.cheapest.price} {flights.cheapest.currency}")
    print(f"Passenger IDs: {flights.passenger_ids}")

    # Step 3: Unlock (free) — confirms price, reserves 30min
    try:
        unlocked = bt.unlock(flights.cheapest.id)
        print(f"Confirmed price: {unlocked.confirmed_currency} {unlocked.confirmed_price}")
    except PaymentRequiredError:
        print("Star repo first: letsfg star --github <username>")
        return None
    except OfferExpiredError:
        print("Offer expired — search again")
        return None

    # Step 4: Book (ticket price charged via Stripe)
    # Map passenger_info to each passenger_id from search
    passengers = []
    for i, pid in enumerate(flights.passenger_ids):
        pax = {**passenger_info[i], "id": pid}
        passengers.append(pax)

    try:
        booking = bt.book(
            offer_id=unlocked.offer_id,
            passengers=passengers,
            contact_email=email,
        )
        print(f"Booked! PNR: {booking.booking_reference}")
        return booking
    except OfferExpiredError:
        print("Offer expired — 30min window may have passed, search again")
        return None
    except LetsFGError as e:
        print(f"Booking failed: {e.message}")
        return None


# Usage — 2 passengers
search_and_book(
    origin_city="London",
    dest_city="Barcelona",
    date="2026-04-01",
    passenger_info=[
        {"given_name": "John", "family_name": "Doe", "born_on": "1990-01-15", "gender": "m", "title": "mr"},
        {"given_name": "Jane", "family_name": "Doe", "born_on": "1992-03-20", "gender": "f", "title": "ms"},
    ],
    email="john.doe@example.com",
)
```

### Bash — CLI Workflow

```bash
#!/bin/bash
set -euo pipefail
export LETSFG_API_KEY=trav_...

# Step 1: Resolve locations
ORIGIN=$(letsfg locations "London" --json | jq -r '.[0].iata_code')
DEST=$(letsfg locations "Barcelona" --json | jq -r '.[0].iata_code')

if [ -z "$ORIGIN" ] || [ -z "$DEST" ]; then
  echo "Error: Could not resolve locations" >&2
  exit 1
fi

# Step 2: Search
RESULTS=$(letsfg search "$ORIGIN" "$DEST" 2026-04-01 --adults 2 --json)
OFFER_ID=$(echo "$RESULTS" | jq -r '.offers[0].id')
TOTAL=$(echo "$RESULTS" | jq '.total_results')

if [ "$OFFER_ID" = "null" ] || [ -z "$OFFER_ID" ]; then
  echo "No flights found $ORIGIN → $DEST" >&2
  exit 1
fi

echo "Found $TOTAL offers, best: $OFFER_ID"

# Step 3: Unlock
if ! letsfg unlock "$OFFER_ID" --json > /dev/null 2>&1; then
  echo "Unlock failed — check payment setup" >&2
  exit 1
fi

# Step 4: Book (one --passenger per passenger_id)
letsfg book "$OFFER_ID" \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --passenger '{"id":"pas_1","given_name":"Jane","family_name":"Doe","born_on":"1992-03-20","gender":"f","title":"ms"}' \
  --email john.doe@example.com
```

---

## How Unlock Works

Unlocking confirms the live price with the airline. FREE with GitHub star verification.

### Endpoint

```
POST /api/v1/bookings/unlock
```

### What Happens When You Unlock

1. LetsFG sends the `offer_id` to the airline's NDC/GDS system
2. The airline confirms the **current live price** (may differ slightly from search)
3. The offer is **reserved for 30 minutes** — no one else can book it
4. You receive `confirmed_price`, `confirmed_currency`, and `offer_expires_at`

### Python Example

```python
from letsfg import LetsFG, PaymentRequiredError, OfferExpiredError

bt = LetsFG()  # reads LETSFG_API_KEY

# Search first (free)
flights = bt.search("LHR", "JFK", "2026-06-01")
print(f"Search price: {flights.cheapest.price} {flights.cheapest.currency}")

# Unlock (free) — confirms live price
try:
    unlocked = bt.unlock(flights.cheapest.id)
    print(f"Confirmed price: {unlocked.confirmed_price} {unlocked.confirmed_currency}")
    print(f"Expires at: {unlocked.offer_expires_at}")
    # Price may differ from search — airline prices change in real-time
except PaymentRequiredError:
    print("Star the repo first — run: letsfg star --github <username>")
except OfferExpiredError:
    print("Offer no longer available — search again for fresh results")
```

### CLI Example

```bash
# Unlock an offer
letsfg unlock off_xxx

# Output:
# Confirmed price: EUR 189.50
# Expires at: 2026-06-01T15:30:00Z (30 minutes)
# Offer ID: off_xxx — ready to book
```

### cURL Example

```bash
curl -X POST https://api.letsfg.co/api/v1/bookings/unlock \
  -H "X-API-Key: trav_..." \
  -H "Content-Type: application/json" \
  -d '{"offer_id": "off_xxx"}'

# Response:
# {
#   "offer_id": "off_xxx",
#   "confirmed_price": 189.50,
#   "confirmed_currency": "EUR",
#   "offer_expires_at": "2026-06-01T15:30:00Z",
#   "payment_status": "charged",
#   "charge_amount": 1.00,
#   "charge_currency": "USD"
# }
```

### Important Notes

- **GitHub star required.** You must star the repo and call `link-github` before your first unlock. If not verified, unlock returns HTTP 403.
- **30-minute window.** After unlock, you have 30 minutes to call `book`. If the window expires, search again (free) and unlock again (free).
- **Price confirmation.** The `confirmed_price` may differ from the search price because airline prices change in real-time. Always check `confirmed_price` before booking.
- **Offer expired (HTTP 410).** If the airline has already sold the seats, unlock returns `OfferExpiredError`. Search again for fresh offers.

---

## Unlock Best Practices

Searching is **completely free** — unlock is also free with GitHub star.

### Strategy 1: Search Wide, Unlock Narrow

```python
# Search multiple dates — FREE
dates = ["2026-04-01", "2026-04-02", "2026-04-03", "2026-04-04", "2026-04-05"]
all_offers = []
for date in dates:
    result = bt.search("LON", "BCN", date)
    all_offers.extend([(date, o) for o in result.offers])

# Compare across all dates — still FREE
all_offers.sort(key=lambda x: x[1].price)
best_date, best_offer = all_offers[0]
print(f"Cheapest is {best_offer.price} {best_offer.currency} on {best_date}")

# Only unlock the winner
unlocked = bt.unlock(best_offer.id)
```

### Strategy 2: Filter Before Unlocking

```python
# Search returns full details (airline, duration, conditions) for FREE
flights = bt.search("LHR", "JFK", "2026-06-01", limit=50)

# Apply all filters BEFORE unlocking
candidates = [
    o for o in flights.offers
    if o.outbound.stopovers == 0                          # Direct only
    and o.outbound.total_duration_seconds < 10 * 3600     # Under 10 hours
    and "British Airways" in o.airlines                    # Specific airline
    and o.conditions.get("change_before_departure") != "not_allowed"  # Changeable
]

if candidates:
    # Unlock only the best match
    best = min(candidates, key=lambda o: o.price)
    unlocked = bt.unlock(best.id)
```

### Strategy 3: Use the 30-Minute Window

After unlocking, the confirmed price is held for **30 minutes**. Use this window to:
- Present results to the user for decision
- Verify passenger details
- Complete the booking without re-searching

```python
# Unlock at minute 0
unlocked = bt.unlock(offer_id)
# ... user reviews details, confirms passenger info ...
# Book within 30 minutes — no additional search or unlock needed
booking = bt.book(offer_id=unlocked.offer_id, passengers=[...], contact_email="...")
```

### Cost Summary

| Action | Cost | Notes |
|--------|------|-------|
| Search | FREE | Unlimited. Search as many routes/dates as you want |
| Resolve location | FREE | Unlimited |
| View offer details | FREE | All details (price, airline, duration, conditions) returned in search |
| Unlock | FREE | Confirms price, holds for 30 minutes |
| Book | **Ticket price** | After unlock — charges ticket price via Stripe (zero markup) |
| Re-search same route | FREE | Prices may change (real-time airline data) |
