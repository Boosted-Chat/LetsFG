# API Guide

## Error Handling

The SDK raises specific exceptions for each failure mode:

| Exception | HTTP Code | When it happens |
|-----------|-----------|-----------------|
| `AuthenticationError` | 401 | Missing or invalid API key |
| `PaymentRequiredError` | 402 | No payment method set up (call `setup-payment` first) |
| `OfferExpiredError` | 410 | Offer no longer available (search again) |
| `BoostedTravelError` | any | Base class — catches all API errors |

### Python Error Handling

```python
from boostedtravel import (
    BoostedTravel, BoostedTravelError,
    AuthenticationError, PaymentRequiredError, OfferExpiredError,
)

bt = BoostedTravel(api_key="trav_...")

# Search — handle invalid locations
try:
    flights = bt.search("INVALID", "JFK", "2026-04-15")
except BoostedTravelError as e:
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
except BoostedTravelError as e:
    print(f"Booking failed ({e.status_code}): {e.message}")
```

### CLI Error Handling

The CLI exits with code 1 on errors and prints the message to stderr. Use `--json` for parseable error output:

```bash
# Check exit code in scripts
if ! boostedtravel search INVALID JFK 2026-04-15 --json 2>/dev/null; then
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
boostedtravel search LON BCN 2026-04-01 --return 2026-04-08 --json

# Pipe to jq for filtering
boostedtravel search LON BCN 2026-04-01 --json | jq '[.offers[] | select(.stopovers == 0)]'
boostedtravel search LON BCN 2026-04-01 --json | jq '.offers | sort_by(.duration_seconds) | .[0]'
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
boostedtravel locations "New York"
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
from boostedtravel import (
    BoostedTravel, BoostedTravelError,
    AuthenticationError, PaymentRequiredError, OfferExpiredError,
)

def search_and_book(origin_city, dest_city, date, passenger_info, email):
    bt = BoostedTravel()  # reads BOOSTEDTRAVEL_API_KEY from env

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

    # Step 3: Unlock ($1) — confirms price, reserves 30min
    try:
        unlocked = bt.unlock(flights.cheapest.id)
        print(f"Confirmed price: {unlocked.confirmed_currency} {unlocked.confirmed_price}")
    except PaymentRequiredError:
        print("Setup payment first: boostedtravel setup-payment")
        return None
    except OfferExpiredError:
        print("Offer expired — search again")
        return None

    # Step 4: Book (free after unlock)
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
    except BoostedTravelError as e:
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
export BOOSTEDTRAVEL_API_KEY=trav_...

# Step 1: Resolve locations
ORIGIN=$(boostedtravel locations "London" --json | jq -r '.[0].iata_code')
DEST=$(boostedtravel locations "Barcelona" --json | jq -r '.[0].iata_code')

if [ -z "$ORIGIN" ] || [ -z "$DEST" ]; then
  echo "Error: Could not resolve locations" >&2
  exit 1
fi

# Step 2: Search
RESULTS=$(boostedtravel search "$ORIGIN" "$DEST" 2026-04-01 --adults 2 --json)
OFFER_ID=$(echo "$RESULTS" | jq -r '.offers[0].id')
TOTAL=$(echo "$RESULTS" | jq '.total_results')

if [ "$OFFER_ID" = "null" ] || [ -z "$OFFER_ID" ]; then
  echo "No flights found $ORIGIN → $DEST" >&2
  exit 1
fi

echo "Found $TOTAL offers, best: $OFFER_ID"

# Step 3: Unlock
if ! boostedtravel unlock "$OFFER_ID" --json > /dev/null 2>&1; then
  echo "Unlock failed — check payment setup" >&2
  exit 1
fi

# Step 4: Book (one --passenger per passenger_id)
boostedtravel book "$OFFER_ID" \
  --passenger '{"id":"pas_0","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}' \
  --passenger '{"id":"pas_1","given_name":"Jane","family_name":"Doe","born_on":"1992-03-20","gender":"f","title":"ms"}' \
  --email john.doe@example.com
```

---

## How Unlock Works

Unlocking is the only paid step ($1). It serves as proof of booking intent and confirms the live price with the airline.

### Endpoint

```
POST /api/v1/bookings/unlock
```

### What Happens When You Unlock

1. BoostedTravel sends the `offer_id` to the airline's NDC/GDS system
2. The airline confirms the **current live price** (may differ slightly from search)
3. A $1.00 charge is made via Stripe to your saved payment method
4. The offer is **reserved for 30 minutes** — no one else can book it
5. You receive `confirmed_price`, `confirmed_currency`, and `offer_expires_at`

### Python Example

```python
from boostedtravel import BoostedTravel, PaymentRequiredError, OfferExpiredError

bt = BoostedTravel()  # reads BOOSTEDTRAVEL_API_KEY

# Search first (free)
flights = bt.search("LHR", "JFK", "2026-06-01")
print(f"Search price: {flights.cheapest.price} {flights.cheapest.currency}")

# Unlock ($1) — confirms live price
try:
    unlocked = bt.unlock(flights.cheapest.id)
    print(f"Confirmed price: {unlocked.confirmed_price} {unlocked.confirmed_currency}")
    print(f"Expires at: {unlocked.offer_expires_at}")
    # Price may differ from search — airline prices change in real-time
except PaymentRequiredError:
    print("No payment method — run: boostedtravel setup-payment")
except OfferExpiredError:
    print("Offer no longer available — search again for fresh results")
```

### CLI Example

```bash
# Unlock an offer
boostedtravel unlock off_xxx

# Output:
# Confirmed price: EUR 189.50
# Expires at: 2026-06-01T15:30:00Z (30 minutes)
# Offer ID: off_xxx — ready to book
```

### cURL Example

```bash
curl -X POST https://api.boostedchat.com/api/v1/bookings/unlock \
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

- **Payment method required.** You must call `setup-payment` before your first unlock. If payment is not set up, unlock returns HTTP 402 (`PaymentRequiredError`).
- **$1 per unlock.** Each unlock costs $1 regardless of flight price. This is the only fee.
- **30-minute window.** After unlock, you have 30 minutes to call `book`. If the window expires, you must search again (free) and unlock again ($1).
- **Price confirmation.** The `confirmed_price` may differ from the search price because airline prices change in real-time. Always check `confirmed_price` before booking.
- **Offer expired (HTTP 410).** If the airline has already sold the seats, unlock returns `OfferExpiredError`. Search again for fresh offers.
- **No refund on expired unlock.** If you unlock but don't book within 30 minutes, the $1 is not refunded. Plan your workflow to book promptly after unlock.

---

## Minimizing Unlock Costs

Searching is **completely free** — you can search as many routes, dates, and configurations as you want without cost. The $1 unlock fee is only charged when you confirm a specific offer.

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

# Only unlock the winner — $1
unlocked = bt.unlock(best_offer.id)
```

### Strategy 2: Filter Before Unlocking

```python
# Search returns full details (airline, duration, conditions) for FREE
flights = bt.search("LHR", "JFK", "2026-06-01", limit=50)

# Apply all filters BEFORE paying $1
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
| Unlock | $1 | Confirms price, holds for 30 minutes |
| Book | FREE | After unlock — creates real airline PNR |
| Re-search same route | FREE | Prices may change (real-time airline data) |
