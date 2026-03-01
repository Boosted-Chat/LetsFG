# BoostedTravel — Agent-Native Flight Search & Booking (Node.js)

Search 400+ airlines at raw airline prices — **$20-50 cheaper** than Booking.com, Kayak, and other OTAs. Zero dependencies. Built for autonomous AI agents.

## Install

```bash
npm install boostedtravel
```

## Quick Start (SDK)

```typescript
import { BoostedTravel, cheapestOffer, offerSummary } from 'boostedtravel';

// Register (one-time)
const creds = await BoostedTravel.register('my-agent', 'agent@example.com');
console.log(creds.api_key); // Save this

// Use
const bt = new BoostedTravel({ apiKey: 'trav_...' });

// Search — FREE
const flights = await bt.search('GDN', 'BER', '2026-03-03');
const best = cheapestOffer(flights);
console.log(offerSummary(best));

// Unlock — $1
const unlock = await bt.unlock(best.id);

// Book — FREE after unlock
const booking = await bt.book(
  best.id,
  [{
    id: flights.passenger_ids[0],
    given_name: 'John',
    family_name: 'Doe',
    born_on: '1990-01-15',
    gender: 'm',
    title: 'mr',
    email: 'john@example.com',
  }],
  'john@example.com'
);
console.log(`PNR: ${booking.booking_reference}`);
```

## Quick Start (CLI)

```bash
export BOOSTEDTRAVEL_API_KEY=trav_...

boostedtravel search GDN BER 2026-03-03 --sort price
boostedtravel search LON BCN 2026-04-01 --json  # Machine-readable
boostedtravel unlock off_xxx
boostedtravel book off_xxx -p '{"id":"pas_xxx","given_name":"John",...}' -e john@example.com
```

## API

### `new BoostedTravel({ apiKey, baseUrl?, timeout? })`

### `bt.search(origin, destination, dateFrom, options?)`
### `bt.resolveLocation(query)`
### `bt.unlock(offerId)`
### `bt.book(offerId, passengers, contactEmail, contactPhone?)`
### `bt.setupPayment(token?)`
### `bt.me()`
### `BoostedTravel.register(agentName, email, baseUrl?, ownerName?, description?)`

### Helpers
- `offerSummary(offer)` — One-line string summary
- `cheapestOffer(result)` — Get cheapest offer from search

## Zero Dependencies

Uses native `fetch` (Node 18+). No `axios`, no `node-fetch`, nothing. Safe for sandboxed environments.

## License

MIT
