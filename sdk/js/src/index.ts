/**
 * BoostedTravel — Agent-native flight search & booking SDK for Node.js/TypeScript.
 *
 * Zero external dependencies. Uses native fetch (Node 18+).
 *
 * @example
 * ```ts
 * import { BoostedTravel } from 'boostedtravel';
 *
 * const bt = new BoostedTravel({ apiKey: 'trav_...' });
 * const flights = await bt.search('GDN', 'BER', '2026-03-03');
 * console.log(flights.offers[0]);
 * ```
 */

// ── Types ────────────────────────────────────────────────────────────────

export interface FlightSegment {
  airline: string;
  airline_name: string;
  flight_no: string;
  origin: string;
  destination: string;
  origin_city: string;
  destination_city: string;
  departure: string;
  arrival: string;
  duration_seconds: number;
  cabin_class: string;
  aircraft: string;
}

export interface FlightRoute {
  segments: FlightSegment[];
  total_duration_seconds: number;
  stopovers: number;
}

export interface FlightOffer {
  id: string;
  price: number;
  currency: string;
  price_formatted: string;
  outbound: FlightRoute;
  inbound: FlightRoute | null;
  airlines: string[];
  owner_airline: string;
  bags_price: Record<string, number>;
  availability_seats: number | null;
  conditions: Record<string, string>;
  is_locked: boolean;
  fetched_at: string;
  booking_url: string;
}

export interface FlightSearchResult {
  search_id: string;
  offer_request_id: string;
  passenger_ids: string[];
  origin: string;
  destination: string;
  currency: string;
  offers: FlightOffer[];
  total_results: number;
  search_params: Record<string, unknown>;
  pricing_note: string;
}

export interface UnlockResult {
  offer_id: string;
  unlock_status: string;
  payment_charged: boolean;
  payment_amount_cents: number;
  payment_currency: string;
  payment_intent_id: string;
  confirmed_price: number | null;
  confirmed_currency: string;
  offer_expires_at: string;
  message: string;
}

export interface Passenger {
  id: string;
  given_name: string;
  family_name: string;
  born_on: string;
  gender?: string;
  title?: string;
  email?: string;
  phone_number?: string;
}

export interface BookingResult {
  booking_id: string;
  status: string;
  booking_type: string;
  offer_id: string;
  flight_price: number;
  service_fee: number;
  service_fee_percentage: number;
  total_charged: number;
  currency: string;
  order_id: string;
  booking_reference: string;
  unlock_payment_id: string;
  fee_payment_id: string;
  created_at: string;
  details: Record<string, unknown>;
}

export interface SearchOptions {
  returnDate?: string;
  adults?: number;
  children?: number;
  infants?: number;
  cabinClass?: 'M' | 'W' | 'C' | 'F';
  maxStopovers?: number;
  currency?: string;
  limit?: number;
  sort?: 'price' | 'duration';
}

export interface BoostedTravelConfig {
  apiKey?: string;
  baseUrl?: string;
  timeout?: number;
}

// ── Errors ────────────────────────────────────────────────────────────────

export class BoostedTravelError extends Error {
  statusCode: number;
  response: Record<string, unknown>;

  constructor(message: string, statusCode = 0, response: Record<string, unknown> = {}) {
    super(message);
    this.name = 'BoostedTravelError';
    this.statusCode = statusCode;
    this.response = response;
  }
}

export class AuthenticationError extends BoostedTravelError {
  constructor(message: string, response: Record<string, unknown> = {}) {
    super(message, 401, response);
    this.name = 'AuthenticationError';
  }
}

export class PaymentRequiredError extends BoostedTravelError {
  constructor(message: string, response: Record<string, unknown> = {}) {
    super(message, 402, response);
    this.name = 'PaymentRequiredError';
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────

function routeStr(route: FlightRoute): string {
  if (!route.segments.length) return '';
  const codes = [route.segments[0].origin, ...route.segments.map(s => s.destination)];
  return codes.join(' → ');
}

function durationHuman(seconds: number): string {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h${m.toString().padStart(2, '0')}m`;
}

/** One-line offer summary */
export function offerSummary(offer: FlightOffer): string {
  const route = routeStr(offer.outbound);
  const dur = durationHuman(offer.outbound.total_duration_seconds);
  const airline = offer.owner_airline || offer.airlines[0] || '?';
  return `${offer.currency} ${offer.price.toFixed(2)} | ${airline} | ${route} | ${dur} | ${offer.outbound.stopovers} stop(s)`;
}

/** Get cheapest offer from search results */
export function cheapestOffer(result: FlightSearchResult): FlightOffer | null {
  if (!result.offers.length) return null;
  return result.offers.reduce((min, o) => (o.price < min.price ? o : min), result.offers[0]);
}

// ── Client ────────────────────────────────────────────────────────────────

const DEFAULT_BASE_URL = 'https://api.boostedchat.com';

export class BoostedTravel {
  private apiKey: string;
  private baseUrl: string;
  private timeout: number;

  constructor(config: BoostedTravelConfig = {}) {
    this.apiKey = config.apiKey || process.env.BOOSTEDTRAVEL_API_KEY || '';
    this.baseUrl = (config.baseUrl || process.env.BOOSTEDTRAVEL_BASE_URL || DEFAULT_BASE_URL).replace(/\/$/, '');
    this.timeout = config.timeout || 30000;

    if (!this.apiKey) {
      throw new AuthenticationError(
        'API key required. Set apiKey in config or BOOSTEDTRAVEL_API_KEY env var. ' +
        'Get one: POST /api/v1/agents/register'
      );
    }
  }

  // ── Core methods ─────────────────────────────────────────────────────

  /**
   * Search for flights — FREE, unlimited.
   *
   * @param origin - IATA code (e.g., "GDN", "LON")
   * @param destination - IATA code (e.g., "BER", "BCN")
   * @param dateFrom - Departure date "YYYY-MM-DD"
   * @param options - Optional search parameters
   */
  async search(
    origin: string,
    destination: string,
    dateFrom: string,
    options: SearchOptions = {},
  ): Promise<FlightSearchResult> {
    const body: Record<string, unknown> = {
      origin: origin.toUpperCase(),
      destination: destination.toUpperCase(),
      date_from: dateFrom,
      adults: options.adults ?? 1,
      children: options.children ?? 0,
      infants: options.infants ?? 0,
      max_stopovers: options.maxStopovers ?? 2,
      currency: options.currency ?? 'EUR',
      limit: options.limit ?? 20,
      sort: options.sort ?? 'price',
    };
    if (options.returnDate) body.return_from = options.returnDate;
    if (options.cabinClass) body.cabin_class = options.cabinClass;

    return this.post<FlightSearchResult>('/api/v1/flights/search', body);
  }

  /**
   * Resolve a city/airport name to IATA codes.
   */
  async resolveLocation(query: string): Promise<Array<Record<string, unknown>>> {
    const data = await this.get<Record<string, unknown>>(`/api/v1/flights/locations/${encodeURIComponent(query)}`);
    if (Array.isArray(data)) return data;
    if (data && Array.isArray((data as Record<string, unknown>).locations)) return (data as Record<string, unknown>).locations as Array<Record<string, unknown>>;
    return data ? [data] : [];
  }

  /**
   * Unlock a flight offer — $1 fee.
   * Confirms price, reserves for 30 minutes.
   */
  async unlock(offerId: string): Promise<UnlockResult> {
    return this.post<UnlockResult>('/api/v1/bookings/unlock', { offer_id: offerId });
  }

  /**
   * Book a flight — FREE after unlock.
   * Creates a real airline reservation with PNR.
   */
  async book(
    offerId: string,
    passengers: Passenger[],
    contactEmail: string,
    contactPhone = '',
  ): Promise<BookingResult> {
    return this.post<BookingResult>('/api/v1/bookings/book', {
      offer_id: offerId,
      booking_type: 'flight',
      passengers,
      contact_email: contactEmail,
      contact_phone: contactPhone,
    });
  }

  /**
   * Set up payment method (payment token).
   */
  async setupPayment(token = 'tok_visa'): Promise<Record<string, unknown>> {
    return this.post<Record<string, unknown>>('/api/v1/agents/setup-payment', { token });
  }

  /**
   * Get current agent profile and usage stats.
   */
  async me(): Promise<Record<string, unknown>> {
    return this.get<Record<string, unknown>>('/api/v1/agents/me');
  }

  // ── Static methods ───────────────────────────────────────────────────

  /**
   * Register a new agent — no API key needed.
   */
  static async register(
    agentName: string,
    email: string,
    baseUrl?: string,
    ownerName = '',
    description = '',
  ): Promise<Record<string, unknown>> {
    const url = (baseUrl || DEFAULT_BASE_URL).replace(/\/$/, '');
    const resp = await fetch(`${url}/api/v1/agents/register`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        agent_name: agentName,
        email,
        owner_name: ownerName,
        description,
      }),
    });

    const data = await resp.json();
    if (!resp.ok) {
      throw new BoostedTravelError(
        data.detail || `Registration failed (${resp.status})`,
        resp.status,
        data,
      );
    }
    return data;
  }

  // ── Internal ────────────────────────────────────────────────────────

  private async post<T>(path: string, body: Record<string, unknown>): Promise<T> {
    return this.request<T>(path, {
      method: 'POST',
      body: JSON.stringify(body),
    });
  }

  private async get<T>(path: string): Promise<T> {
    return this.request<T>(path, { method: 'GET' });
  }

  private async request<T>(path: string, init: RequestInit): Promise<T> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeout);

    try {
      const resp = await fetch(`${this.baseUrl}${path}`, {
        ...init,
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': this.apiKey,
          'User-Agent': 'boostedtravel-js/0.1.0',
          ...(init.headers || {}),
        },
        signal: controller.signal,
      });

      const data = await resp.json();

      if (!resp.ok) {
        const detail = (data as Record<string, string>).detail || `API error (${resp.status})`;
        if (resp.status === 401) throw new AuthenticationError(detail, data);
        if (resp.status === 402) throw new PaymentRequiredError(detail, data);
        throw new BoostedTravelError(detail, resp.status, data);
      }

      return data as T;
    } finally {
      clearTimeout(timer);
    }
  }
}

export default BoostedTravel;
