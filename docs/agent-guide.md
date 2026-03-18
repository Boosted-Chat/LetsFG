# Building AI Agents with LetsFG

Guidelines for building autonomous AI agents that search, evaluate, and book flights. Works with OpenClaw, Perplexity Computer, Claude, Cursor, Windsurf, and any MCP-compatible agent framework.

> 🎬 **[Watch the demo](https://github.com/LetsFG/LetsFG#demo-lfg-vs-default-agent-search)** — side-by-side comparison of default agent search vs LetsFG.

## Two Search Modes

Agents can use **local search** (free, no API key) for quick lookups, or **full search** (API key) for comprehensive results:

```python
# Local search — no API key, 102 airline connectors
from letsfg.local import search_local
result = await search_local("LHR", "JFK", "2026-06-01")

# With concurrency limit for constrained environments
result = await search_local("LHR", "JFK", "2026-06-01", max_browsers=4)

# Full search — API key required, 400+ airlines via GDS/NDC
from letsfg import LetsFG
bt = LetsFG(api_key="trav_...")
result = bt.search("LHR", "JFK", "2026-06-01")
```

**When to use local search:** Quick price checks, free-tier agents, routes served by the 102 local connectors, no-registration workflows.

**When to use full search:** Comprehensive coverage across 400+ airlines, booking flow (unlock → book), GDS/NDC fares not available on airline websites.

## Architecture

```
User request → Agent parses intent → Resolve locations → Search (free)
  → Filter & rank offers → Present to user → Unlock best (free) → Book (free)
```

## Agent Best Practices

1. **Always resolve locations first.** City names are ambiguous — "London" could be LHR, LGW, STN, LCY, or LTN. Use `resolve_location()` to get IATA codes, then let the user confirm if multiple options exist.

2. **Search is free — use it liberally.** Search multiple dates, multiple origin/destination pairs, different cabin classes. Build a complete picture before unlocking.

3. **Understand the 30-minute expiration.** After unlocking, you have 30 minutes to book. If the window expires, you must search again (free) and unlock again (free). Plan your workflow to minimize the gap between unlock and book.

4. **Handle price changes gracefully.** Search prices are real-time snapshots. The unlock step confirms the actual current price with the airline. If the confirmed price differs significantly from the search price, inform the user before proceeding to book.

5. **Map passenger IDs correctly.** Search returns `passenger_ids` (e.g., `["pas_0", "pas_1"]`). When booking with multiple passengers, each passenger dict must include the correct `id` from this list. The first adult gets `pas_0`, second gets `pas_1`, etc.

6. **Use REAL passenger details.** Airlines send e-tickets to the contact email. Names must match the passenger's passport or government ID. Never use placeholder data.

7. **Be aware of system resources.** Local search fires up to 102 browser-based connectors in parallel. LetsFG auto-scales concurrency based on available RAM, but agents can check resources and override:

```python
from letsfg import get_system_profile, configure_max_browsers

profile = get_system_profile()
if profile['tier'] in ('minimal', 'low'):
    configure_max_browsers(2)  # go easy on constrained machines
```

Or use the MCP `system_info` tool before `search_flights` to decide concurrency.

## Handling Edge Cases

```python
from letsfg import (
    LetsFG, LetsFGError,
    PaymentRequiredError, OfferExpiredError,
    ErrorCode, ErrorCategory,
)
import uuid

# Retry on expired offers with idempotency protection
def resilient_book(bt, origin, dest, date, passengers, email, max_retries=2):
    idempotency_key = str(uuid.uuid4())  # prevents double-booking on retry

    for attempt in range(max_retries + 1):
        flights = bt.search(origin, dest, date)
        if not flights.offers:
            return None

        try:
            unlocked = bt.unlock(flights.cheapest.id)
            booking = bt.book(
                offer_id=unlocked.offer_id,
                passengers=[{**p, "id": pid} for p, pid in zip(passengers, flights.passenger_ids)],
                contact_email=email,
                idempotency_key=idempotency_key,
            )
            return booking
        except OfferExpiredError:
            if attempt < max_retries:
                print(f"Offer expired, retrying ({attempt + 1}/{max_retries})...")
                continue
            raise
        except LetsFGError as e:
            if e.is_retryable and attempt < max_retries:
                import time; time.sleep(2 ** attempt)
                continue
            raise
        except PaymentRequiredError:
            print("Payment method not set up — call bt.setup_payment()")
            raise

# Compare prices across dates intelligently
def find_cheapest_date(bt, origin, dest, dates):
    """Search multiple dates (free) and return the cheapest option."""
    best = None
    for date in dates:
        try:
            result = bt.search(origin, dest, date)
            if result.offers and (best is None or result.cheapest.price < best[1].price):
                best = (date, result.cheapest, result.passenger_ids)
        except LetsFGError:
            continue  # Skip dates with no routes
    return best  # (date, offer, passenger_ids) or None
```

## Rate Limits and Timeouts

The API has generous rate limits. Search is unlimited and free, so you can make many requests without cost.

| Endpoint | Rate Limit | Timeout |
|----------|-----------|---------|
| Search | 60 req/min per agent | 30s (airline APIs can be slow) |
| Resolve location | 120 req/min per agent | 5s |
| Unlock | 20 req/min per agent | 15s |
| Book | 10 req/min per agent | 30s |

Handle rate limits and timeouts in production:

```python
import time
from letsfg import LetsFG, LetsFGError

bt = LetsFG()

def search_with_retry(origin, dest, date, max_retries=3):
    """Retry with exponential backoff on rate limit or timeout."""
    for attempt in range(max_retries):
        try:
            return bt.search(origin, dest, date)
        except LetsFGError as e:
            if "rate limit" in str(e).lower() or "429" in str(e):
                wait = 2 ** attempt  # 1s, 2s, 4s
                print(f"Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif "timeout" in str(e).lower() or "504" in str(e):
                print(f"Timeout, retrying ({attempt + 1}/{max_retries})...")
                time.sleep(1)
            else:
                raise
    raise LetsFGError("Max retries exceeded")
```

## Advanced Preference Evaluation

Rather than always picking the cheapest flight, score offers by weighted criteria:

```python
def score_offer(offer, preferences=None):
    """Score a flight offer by multiple criteria (lower = better).
    
    preferences: dict with weights, e.g.:
        {"price": 0.4, "duration": 0.3, "stops": 0.2, "airline_pref": 0.1}
    """
    prefs = preferences or {"price": 0.4, "duration": 0.3, "stops": 0.2, "airline_pref": 0.1}
    preferred_airlines = {"British Airways", "Delta", "United", "Lufthansa"}
    
    # Normalize factors (0-1 scale, lower is better)
    price_score = offer.price / 2000        # Normalize against $2000 baseline
    duration_hours = offer.outbound.total_duration_seconds / 3600
    duration_score = duration_hours / 24    # Normalize against 24h baseline
    stops_score = offer.outbound.stopovers / 3  # Normalize against 3 stops
    airline_score = 0 if any(a in preferred_airlines for a in offer.airlines) else 1
    
    return (
        prefs["price"] * price_score +
        prefs["duration"] * duration_score +
        prefs["stops"] * stops_score +
        prefs["airline_pref"] * airline_score
    )

# Usage: find best offer considering multiple criteria
flights = bt.search("LHR", "JFK", "2026-06-01", limit=50)
best = min(flights.offers, key=lambda o: score_offer(o, {
    "price": 0.3,      # Price matters, but not everything
    "duration": 0.4,    # Shortest travel time is priority
    "stops": 0.2,       # Prefer direct flights
    "airline_pref": 0.1 # Slight preference for known airlines
}))
print(f"Best overall: {best.airlines[0]} ${best.price} — {best.outbound.stopovers} stops")
```

## Data Persistence for Price Tracking

For agents that track prices over time or compare across sessions:

```python
import json
from datetime import datetime
from pathlib import Path

CACHE_FILE = Path("flight_price_history.json")

def load_price_history():
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text())
    return {}

def save_search_result(origin, dest, date, result):
    """Save search results for later comparison."""
    history = load_price_history()
    key = f"{origin}-{dest}-{date}"
    if key not in history:
        history[key] = []
    history[key].append({
        "searched_at": datetime.utcnow().isoformat(),
        "cheapest_price": result.cheapest.price if result.offers else None,
        "total_offers": result.total_results,
        "airlines": list(set(a for o in result.offers[:5] for a in o.airlines)),
    })
    CACHE_FILE.write_text(json.dumps(history, indent=2))

def get_price_trend(origin, dest, date):
    """Check if prices are rising or falling for a route."""
    history = load_price_history()
    key = f"{origin}-{dest}-{date}"
    entries = history.get(key, [])
    if len(entries) < 2:
        return "insufficient_data"
    prices = [e["cheapest_price"] for e in entries if e["cheapest_price"]]
    if prices[-1] < prices[0]:
        return f"falling (${prices[0]} → ${prices[-1]})"
    return f"rising (${prices[0]} → ${prices[-1]})"
```
