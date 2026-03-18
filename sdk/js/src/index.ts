/**
 * LetsFG — Agent-native flight search & booking SDK for Node.js/TypeScript.
 *
 * 75 airline connectors run locally via Python + backend API for enterprise GDS/NDC sources.
 * Zero external JS dependencies. Uses native fetch (Node 18+).
 *
 * @example
 * ```ts
 * import { LetsFG, searchLocal } from 'letsfg';
 *
 * // Local search — FREE, no API key
 * const local = await searchLocal('SHA', 'CTU', '2026-03-20');
 *
 * // Full API — search + unlock + book
 * const bt = new LetsFG({ apiKey: 'trav_...' });
 * const flights = await bt.search('GDN', 'BER', '2026-03-03');
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
  /** Max concurrent browser instances (1-32). Omit for auto-detect based on system RAM. */
  maxBrowsers?: number;
}

export interface CheckoutProgress {
  status: string;               // "payment_page_reached", "url_only", "failed", "error"
  step: string;                 // Current checkout step
  step_index: number;           // Numeric step (0-8)
  airline: string;              // Airline name
  source: string;               // Source tag (e.g., "ryanair_direct")
  offer_id: string;
  total_price: number;          // Price shown on checkout page
  currency: string;
  booking_url: string;          // Direct URL for manual completion
  screenshot_b64: string;       // Base64 screenshot of current state
  message: string;
  can_complete_manually: boolean;
  elapsed_seconds: number;
  details: Record<string, unknown>;
}

export interface LetsFGConfig {
  apiKey?: string;
  baseUrl?: string;
  timeout?: number;
}

// ── Error codes ───────────────────────────────────────────────────────────
// Machine-readable error codes for agent decision-making.

export const ErrorCode = {
  // Transient (safe to retry after short delay)
  SUPPLIER_TIMEOUT: 'SUPPLIER_TIMEOUT',
  RATE_LIMITED: 'RATE_LIMITED',
  SERVICE_UNAVAILABLE: 'SERVICE_UNAVAILABLE',
  NETWORK_ERROR: 'NETWORK_ERROR',
  // Validation (fix input, then retry)
  INVALID_IATA: 'INVALID_IATA',
  INVALID_DATE: 'INVALID_DATE',
  INVALID_PASSENGERS: 'INVALID_PASSENGERS',
  UNSUPPORTED_ROUTE: 'UNSUPPORTED_ROUTE',
  MISSING_PARAMETER: 'MISSING_PARAMETER',
  INVALID_PARAMETER: 'INVALID_PARAMETER',
  // Business (requires human decision)
  AUTH_INVALID: 'AUTH_INVALID',
  PAYMENT_REQUIRED: 'PAYMENT_REQUIRED',
  PAYMENT_DECLINED: 'PAYMENT_DECLINED',
  OFFER_EXPIRED: 'OFFER_EXPIRED',
  OFFER_NOT_UNLOCKED: 'OFFER_NOT_UNLOCKED',
  FARE_CHANGED: 'FARE_CHANGED',
  ALREADY_BOOKED: 'ALREADY_BOOKED',
  BOOKING_FAILED: 'BOOKING_FAILED',
} as const;

export type ErrorCodeType = (typeof ErrorCode)[keyof typeof ErrorCode];

export const ErrorCategory = {
  TRANSIENT: 'transient',
  VALIDATION: 'validation',
  BUSINESS: 'business',
} as const;

export type ErrorCategoryType = (typeof ErrorCategory)[keyof typeof ErrorCategory];

const CODE_TO_CATEGORY: Record<string, ErrorCategoryType> = {
  [ErrorCode.SUPPLIER_TIMEOUT]: ErrorCategory.TRANSIENT,
  [ErrorCode.RATE_LIMITED]: ErrorCategory.TRANSIENT,
  [ErrorCode.SERVICE_UNAVAILABLE]: ErrorCategory.TRANSIENT,
  [ErrorCode.NETWORK_ERROR]: ErrorCategory.TRANSIENT,
  [ErrorCode.INVALID_IATA]: ErrorCategory.VALIDATION,
  [ErrorCode.INVALID_DATE]: ErrorCategory.VALIDATION,
  [ErrorCode.INVALID_PASSENGERS]: ErrorCategory.VALIDATION,
  [ErrorCode.UNSUPPORTED_ROUTE]: ErrorCategory.VALIDATION,
  [ErrorCode.MISSING_PARAMETER]: ErrorCategory.VALIDATION,
  [ErrorCode.INVALID_PARAMETER]: ErrorCategory.VALIDATION,
  [ErrorCode.AUTH_INVALID]: ErrorCategory.BUSINESS,
  [ErrorCode.PAYMENT_REQUIRED]: ErrorCategory.BUSINESS,
  [ErrorCode.PAYMENT_DECLINED]: ErrorCategory.BUSINESS,
  [ErrorCode.OFFER_EXPIRED]: ErrorCategory.BUSINESS,
  [ErrorCode.OFFER_NOT_UNLOCKED]: ErrorCategory.BUSINESS,
  [ErrorCode.FARE_CHANGED]: ErrorCategory.BUSINESS,
  [ErrorCode.ALREADY_BOOKED]: ErrorCategory.BUSINESS,
  [ErrorCode.BOOKING_FAILED]: ErrorCategory.BUSINESS,
};

function inferErrorCode(statusCode: number, detail: string): string {
  const d = detail.toLowerCase();
  if (statusCode === 401) return ErrorCode.AUTH_INVALID;
  if (statusCode === 402) return d.includes('declined') ? ErrorCode.PAYMENT_DECLINED : ErrorCode.PAYMENT_REQUIRED;
  if (statusCode === 410) return ErrorCode.OFFER_EXPIRED;
  if (statusCode === 422) {
    if (d.includes('iata') || d.includes('airport')) return ErrorCode.INVALID_IATA;
    if (d.includes('date')) return ErrorCode.INVALID_DATE;
    if (d.includes('passenger')) return ErrorCode.INVALID_PASSENGERS;
    if (d.includes('route')) return ErrorCode.UNSUPPORTED_ROUTE;
    return ErrorCode.INVALID_PARAMETER;
  }
  if (statusCode === 429) return ErrorCode.RATE_LIMITED;
  if (statusCode === 503) return ErrorCode.SERVICE_UNAVAILABLE;
  if (statusCode === 504) return ErrorCode.SUPPLIER_TIMEOUT;
  if (statusCode === 409) return ErrorCode.ALREADY_BOOKED;
  return statusCode >= 500 ? ErrorCode.BOOKING_FAILED : ErrorCode.INVALID_PARAMETER;
}

// ── Errors ────────────────────────────────────────────────────────────────

export class LetsFGError extends Error {
  statusCode: number;
  response: Record<string, unknown>;
  errorCode: string;
  errorCategory: ErrorCategoryType;
  isRetryable: boolean;

  constructor(message: string, statusCode = 0, response: Record<string, unknown> = {}, errorCode = '') {
    super(message);
    this.name = 'LetsFGError';
    this.statusCode = statusCode;
    this.response = response;
    this.errorCode = errorCode || (response.error_code as string) || '';
    this.errorCategory = CODE_TO_CATEGORY[this.errorCode] || ErrorCategory.BUSINESS;
    this.isRetryable = this.errorCategory === ErrorCategory.TRANSIENT;
  }
}

export class AuthenticationError extends LetsFGError {
  constructor(message: string, response: Record<string, unknown> = {}) {
    super(message, 401, response, ErrorCode.AUTH_INVALID);
    this.name = 'AuthenticationError';
  }
}

export class PaymentRequiredError extends LetsFGError {
  constructor(message: string, response: Record<string, unknown> = {}) {
    const code = message.toLowerCase().includes('declined') ? ErrorCode.PAYMENT_DECLINED : ErrorCode.PAYMENT_REQUIRED;
    super(message, 402, response, code);
    this.name = 'PaymentRequiredError';
  }
}

export class OfferExpiredError extends LetsFGError {
  constructor(message: string, response: Record<string, unknown> = {}) {
    super(message, 410, response, ErrorCode.OFFER_EXPIRED);
    this.name = 'OfferExpiredError';
  }
}

export class ValidationError extends LetsFGError {
  constructor(message: string, statusCode = 422, response: Record<string, unknown> = {}, errorCode = '') {
    super(message, statusCode, response, errorCode || ErrorCode.INVALID_PARAMETER);
    this.name = 'ValidationError';
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

// ── Local Search (Python subprocess) ──────────────────────────────────────

/**
 * Search flights using 73 local airline connectors — FREE, no API key needed.
 *
 * Requires: pip install letsfg && playwright install chromium
 *
 * @param origin - IATA code (e.g., "SHA")
 * @param destination - IATA code (e.g., "CTU")
 * @param dateFrom - Departure date "YYYY-MM-DD"
 * @param options - Optional: currency, adults, limit, etc.
 */
export async function searchLocal(
  origin: string,
  destination: string,
  dateFrom: string,
  options: Partial<SearchOptions> = {},
): Promise<FlightSearchResult> {
  const { spawn } = await import('child_process');

  const params = JSON.stringify({
    origin: origin.toUpperCase(),
    destination: destination.toUpperCase(),
    date_from: dateFrom,
    adults: options.adults ?? 1,
    children: options.children ?? 0,
    currency: options.currency ?? 'EUR',
    limit: options.limit ?? 50,
    return_date: options.returnDate,
    cabin_class: options.cabinClass,
    ...(options.maxBrowsers != null && { max_browsers: options.maxBrowsers }),
  });

  return new Promise((resolve, reject) => {
    const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';
    const child = spawn(pythonCmd, ['-m', 'letsfg.local'], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (d: Buffer) => { stdout += d.toString(); });
    child.stderr.on('data', (d: Buffer) => { stderr += d.toString(); });

    child.on('close', (code) => {
      try {
        const data = JSON.parse(stdout);
        if (data.error) reject(new LetsFGError(data.error));
        else resolve(data as FlightSearchResult);
      } catch {
        reject(new LetsFGError(
          `Python search failed (code ${code}): ${stdout || stderr}\n` +
          'Make sure LetsFG is installed: pip install letsfg && playwright install chromium'
        ));
      }
    });

    child.on('error', (err) => {
      reject(new LetsFGError(
        `Cannot start Python: ${err.message}\n` +
        'Install: pip install letsfg && playwright install chromium'
      ));
    });

    child.stdin.write(params);
    child.stdin.end();
  });
}

// ── Client ────────────────────────────────────────────────────────────────

const DEFAULT_BASE_URL = 'https://api.letsfg.co';

export class LetsFG {
  private apiKey: string;
  private baseUrl: string;
  private timeout: number;

  constructor(config: LetsFGConfig = {}) {
    this.apiKey = config.apiKey || process.env.LETSFG_API_KEY || '';
    this.baseUrl = (config.baseUrl || process.env.LETSFG_BASE_URL || DEFAULT_BASE_URL).replace(/\/$/, '');
    this.timeout = config.timeout || 30000;
  }

  private requireApiKey(): void {
    if (!this.apiKey) {
      throw new AuthenticationError(
        'API key required for this operation. Set apiKey in config or LETSFG_API_KEY env var.\n' +
        'Note: searchLocal() works without an API key.'
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
    this.requireApiKey();
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
    this.requireApiKey();
    const data = await this.get<Record<string, unknown>>(`/api/v1/flights/locations/${encodeURIComponent(query)}`);
    if (Array.isArray(data)) return data;
    if (data && Array.isArray((data as Record<string, unknown>).locations)) return (data as Record<string, unknown>).locations as Array<Record<string, unknown>>;
    return data ? [data] : [];
  }

  /**
   * Unlock a flight offer — FREE with GitHub star.
   * Confirms price, reserves for 30 minutes.
   */
  async unlock(offerId: string): Promise<UnlockResult> {
    this.requireApiKey();
    return this.post<UnlockResult>('/api/v1/bookings/unlock', { offer_id: offerId });
  }

  /**
   * Book a flight — charges ticket price via Stripe.
   * Creates a real airline reservation with PNR.
   *
   * Always provide idempotencyKey to prevent double-bookings on retry.
   */
  async book(
    offerId: string,
    passengers: Passenger[],
    contactEmail: string,
    contactPhone = '',
    idempotencyKey = '',
  ): Promise<BookingResult> {
    this.requireApiKey();
    const body: Record<string, unknown> = {
      offer_id: offerId,
      booking_type: 'flight',
      passengers,
      contact_email: contactEmail,
      contact_phone: contactPhone,
    };
    if (idempotencyKey) body.idempotency_key = idempotencyKey;
    return this.post<BookingResult>('/api/v1/bookings/book', body);
  }

  /**
   * Set up payment method (required before booking).
   */
  async setupPayment(token = 'tok_visa'): Promise<Record<string, unknown>> {
    this.requireApiKey();
    return this.post<Record<string, unknown>>('/api/v1/agents/setup-payment', { token });
  }

  /**
   * Start automated checkout — drives to payment page, NEVER submits payment.
   *
   * Requires unlock first. Returns progress with screenshot and
   * booking URL for manual completion.
   *
   * @param offerId - Offer ID from search results
   * @param passengers - Passenger details (use test data for safety)
   * @param checkoutToken - Token from unlock() response
   */
  async startCheckout(
    offerId: string,
    passengers: Passenger[],
    checkoutToken: string,
  ): Promise<CheckoutProgress> {
    this.requireApiKey();
    return this.post<CheckoutProgress>('/api/v1/bookings/start-checkout', {
      offer_id: offerId,
      passengers,
      checkout_token: checkoutToken,
    });
  }

  /**
   * Start checkout locally via Python (runs on your machine).
   * Requires: pip install letsfg && playwright install chromium
   *
   * @param offer - Full FlightOffer object from search results
   * @param passengers - Passenger details
   * @param checkoutToken - Token from unlock()
   */
  async startCheckoutLocal(
    offer: FlightOffer,
    passengers: Passenger[],
    checkoutToken: string,
  ): Promise<CheckoutProgress> {
    const { spawn } = await import('child_process');

    const input = JSON.stringify({
      __checkout: true,
      offer,
      passengers,
      checkout_token: checkoutToken,
      api_key: this.apiKey,
      base_url: this.baseUrl,
    });

    return new Promise((resolve, reject) => {
      const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';
      const child = spawn(pythonCmd, ['-m', 'letsfg.local'], {
        stdio: ['pipe', 'pipe', 'pipe'],
        timeout: 180_000,
      });

      let stdout = '';
      let stderr = '';

      child.stdout.on('data', (d: Buffer) => { stdout += d.toString(); });
      child.stderr.on('data', (d: Buffer) => { stderr += d.toString(); });

      child.on('close', (code) => {
        try {
          const data = JSON.parse(stdout);
          if (data.error) reject(new LetsFGError(data.error));
          else resolve(data as CheckoutProgress);
        } catch {
          reject(new LetsFGError(`Checkout failed (code ${code}): ${stdout || stderr}`));
        }
      });

      child.on('error', (err) => {
        reject(new LetsFGError(`Cannot start Python: ${err.message}`));
      });

      child.stdin.write(input);
      child.stdin.end();
    });
  }

  /**
   * Link GitHub account for FREE unlimited access.
   *
   * Star https://github.com/LetsFG/LetsFG, then call this with your username.
   * Once verified, access is permanent.
   */
  async linkGithub(githubUsername: string): Promise<Record<string, unknown>> {
    this.requireApiKey();
    return this.post<Record<string, unknown>>(`/api/v1/agents/link-github?github_username=${encodeURIComponent(githubUsername)}`, {});
  }

  /**
   * Get current agent profile and usage stats.
   */
  async me(): Promise<Record<string, unknown>> {
    this.requireApiKey();
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
      throw new LetsFGError(
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
          'User-Agent': 'LetsFG-js/0.1.0',
          ...(init.headers || {}),
        },
        signal: controller.signal,
      });

      const data = await resp.json();

      if (!resp.ok) {
        const detail = (data as Record<string, string>).detail || `API error (${resp.status})`;
        const code = (data as Record<string, string>).error_code || inferErrorCode(resp.status, detail);
        if (resp.status === 401) throw new AuthenticationError(detail, data);
        if (resp.status === 402) throw new PaymentRequiredError(detail, data);
        if (resp.status === 410) throw new OfferExpiredError(detail, data);
        if (resp.status === 422) throw new ValidationError(detail, resp.status, data, code);
        throw new LetsFGError(detail, resp.status, data, code);
      }

      return data as T;
    } finally {
      clearTimeout(timer);
    }
  }
}

/**
 * Get system resource profile and recommended concurrency settings.
 * Calls the Python backend's system-info detection.
 */
export async function systemInfo(): Promise<Record<string, unknown>> {
  const { spawn } = await import('child_process');

  return new Promise((resolve, reject) => {
    const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';
    const child = spawn(pythonCmd, ['-m', 'letsfg.local'], {
      stdio: ['pipe', 'pipe', 'pipe'],
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (d: Buffer) => { stdout += d.toString(); });
    child.stderr.on('data', (d: Buffer) => { stderr += d.toString(); });

    child.on('close', (code) => {
      try {
        const data = JSON.parse(stdout);
        if (data.error) reject(new LetsFGError(data.error));
        else resolve(data as Record<string, unknown>);
      } catch {
        reject(new LetsFGError(
          `Python system-info failed (code ${code}): ${stdout || stderr}`
        ));
      }
    });

    child.on('error', (err) => {
      reject(new LetsFGError(
        `Cannot start Python: ${err.message}\n` +
        'Install: pip install letsfg'
      ));
    });

    child.stdin.write(JSON.stringify({ __system_info: true }));
    child.stdin.end();
  });
}

export default LetsFG;
export { searchLocal as localSearch, systemInfo as getSystemInfo };

// Backward-compat aliases (deprecated — use LetsFG / LetsFGError directly)
export const BoostedTravel = LetsFG;
export const BoostedTravelError = LetsFGError;
export type BoostedTravelConfig = LetsFGConfig;
