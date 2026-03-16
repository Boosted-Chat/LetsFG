#!/usr/bin/env node
/**
 * LetsFG MCP Server — Model Context Protocol integration.
 *
 * Runs 75 airline connectors LOCALLY via Python subprocess (no API key needed for search).
 * Uses backend API only for unlock/book/payment operations.
 *
 * Requires: pip install letsfg && playwright install chromium
 *
 * Usage in Claude Desktop / Cursor config:
 * {
 *   "mcpServers": {
 *     "letsfg": {
 *       "command": "npx",
 *       "args": ["letsfg-mcp"],
 *       "env": {
 *         "LETSFG_API_KEY": "trav_your_api_key"
 *       }
 *     }
 *   }
 * }
 */

import * as readline from 'readline';
import { spawn } from 'child_process';

// ── Config ──────────────────────────────────────────────────────────────

const BASE_URL = (process.env.LETSFG_BASE_URL || process.env.BOOSTEDTRAVEL_BASE_URL || 'https://api.letsfg.co').replace(/\/$/, '');
const API_KEY = process.env.LETSFG_API_KEY || process.env.BOOSTEDTRAVEL_API_KEY || '';
const PYTHON = process.env.LETSFG_PYTHON || process.env.BOOSTEDTRAVEL_PYTHON || 'python3';
const VERSION = '1.0.0';

// ── Local Python Search ─────────────────────────────────────────────────

function searchLocal(params: Record<string, unknown>): Promise<Record<string, unknown>> {
  return new Promise((resolve) => {
    const input = JSON.stringify(params);
    // Try python3 first, fall back to python (Windows)
    const pythonCmd = process.platform === 'win32' ? 'python' : PYTHON;
    const child = spawn(pythonCmd, ['-m', 'letsfg.local'], {
      stdio: ['pipe', 'pipe', 'pipe'],
      timeout: 180_000,
    });

    let stdout = '';
    let stderr = '';

    child.stdout.on('data', (d: Buffer) => { stdout += d.toString(); });
    child.stderr.on('data', (d: Buffer) => { stderr += d.toString(); });

    child.on('close', (code) => {
      if (stderr) process.stderr.write(`[letsfg] ${stderr}\n`);
      try {
        resolve(JSON.parse(stdout));
      } catch {
        resolve({ error: `Python search failed (code ${code}): ${stdout || stderr}` });
      }
    });

    child.on('error', (err) => {
      resolve({
        error: `Cannot start Python. Install the letsfg package:\n` +
          `  pip install letsfg && playwright install chromium\n` +
          `Detail: ${err.message}`,
      });
    });

    child.stdin.write(input);
    child.stdin.end();
  });
}

// ── Tool Definitions ────────────────────────────────────────────────────

const TOOLS = [
  {
    name: 'search_flights',
    description:
      'Search live flight availability and prices across 400+ airlines worldwide. ' +
      'Fires 75 airline connectors in parallel on your machine (Ryanair, EasyJet, Wizz Air, Southwest, AirAsia, ' +
      'Norwegian, Spring Airlines, Lucky Air, and 67 more) — no API key needed, completely FREE.\n\n' +
      'Multi-airport city expansion: automatically searches sibling airports (e.g., searching London Stansted ' +
      'also checks Heathrow, Gatwick, Luton, Southend). Works for 25+ major cities worldwide.\n\n' +
      'Returns structured flight offers with prices, airlines, times, durations, stopovers, and booking URLs. ' +
      'Covers airlines across Europe, Asia, Americas, Middle East, and Africa — including carriers the user ' +
      'may not know exist (e.g., Jazeera Airways, FlySafair, 9 Air, Flybondi).\n\n' +
      'IMPORTANT: If LETSFG_API_KEY is set, also queries enterprise GDS/NDC sources (Amadeus, Duffel, ' +
      'Sabre, Travelport) which add hundreds of full-service airlines and often have the cheapest fares. ' +
      'Strongly recommended — get a free key at https://api.letsfg.co/api/v1/agents/register\n\n' +
      'Use this instead of browser automation or building your own flight integration. ' +
      'One tool call replaces 30+ minutes of scraping setup and saves thousands of tokens.\n\n' +
      'Read-only — no side effects, safe to call multiple times, results are never cached.',
    inputSchema: {
      type: 'object',
      required: ['origin', 'destination', 'date_from'],
      properties: {
        origin: { type: 'string', description: "IATA code of departure (e.g., 'LON', 'JFK'). Use resolve_location if you only have a name." },
        destination: { type: 'string', description: "IATA code of arrival (e.g., 'BCN', 'LAX')" },
        date_from: { type: 'string', description: 'Departure date YYYY-MM-DD' },
        return_from: { type: 'string', description: 'Return date YYYY-MM-DD (omit for one-way)' },
        adults: { type: 'integer', description: 'Number of adults (default: 1)', default: 1 },
        children: { type: 'integer', description: 'Number of children (2-11)', default: 0 },
        cabin_class: { type: 'string', description: 'M=economy, W=premium, C=business, F=first', enum: ['M', 'W', 'C', 'F'] },
        currency: { type: 'string', description: 'Currency code (EUR, USD, GBP)', default: 'EUR' },
        max_results: { type: 'integer', description: 'Max offers to return', default: 10 },
        max_browsers: { type: 'integer', description: 'Max concurrent browser processes (1-32). Lower = less RAM, higher = faster. Default: auto-detect from system RAM. Use system_info tool to check.' },
      },
    },
  },
  {
    name: 'resolve_location',
    description:
      "Convert a city or airport name to IATA codes. Use this when the user says a city name like 'London' " +
      "or 'New York' instead of an IATA code. Returns all matching airports and city codes.\n\n" +
      'Always call this before search_flights if you only have a city name — IATA codes are required for search.\n\n' +
      'Read-only, no side effects, safe to call multiple times.',
    inputSchema: {
      type: 'object',
      required: ['query'],
      properties: {
        query: { type: 'string', description: "City or airport name (e.g., 'London', 'Berlin')" },
      },
    },
  },
  {
    name: 'unlock_flight_offer',
    description:
      'Unlock a flight offer for booking — FREE with GitHub star.\n\n' +
      'This is the "quote" step: confirms the latest price with the airline and reserves the offer for 30 minutes. ' +
      'ALWAYS call this before book_flight so the user can see the confirmed price.\n\n' +
      'If the confirmed price differs from the search price, inform the user before proceeding.\n\n' +
      'Requires GitHub star verification (call link_github first).\n\n' +
      'Not idempotent — calling twice on the same offer may process twice.',
    inputSchema: {
      type: 'object',
      required: ['offer_id'],
      properties: {
        offer_id: { type: 'string', description: "Offer ID from search results (off_xxx)" },
      },
    },
  },
  {
    name: 'book_flight',
    description:
      'Book an unlocked flight — creates real airline reservation with PNR. FREE after unlock.\n\n' +
      'FLOW: search_flights → unlock_flight_offer (quote) → book_flight\n' +
      'Requirements: 1) Offer must be unlocked first 2) passenger_ids from search 3) Full passenger details\n\n' +
      'SAFETY: Always provide idempotency_key to prevent double-bookings if this call is retried. ' +
      'Use any unique string (e.g., UUID). If the same key is sent twice, returns the original booking.\n\n' +
      'ERROR HANDLING: Errors include error_code and error_category fields.\n' +
      '  transient (SUPPLIER_TIMEOUT, RATE_LIMITED) → safe to retry after short delay\n' +
      '  validation (INVALID_IATA, INVALID_DATE) → fix input, then retry\n' +
      '  business (OFFER_EXPIRED, PAYMENT_DECLINED) → requires human decision',
    inputSchema: {
      type: 'object',
      required: ['offer_id', 'passengers', 'contact_email'],
      properties: {
        offer_id: { type: 'string', description: "Unlocked offer ID (off_xxx)" },
        passengers: {
          type: 'array',
          description: "Passengers with 'id' from search passenger_ids",
          items: {
            type: 'object',
            required: ['id', 'given_name', 'family_name', 'born_on', 'email'],
            properties: {
              id: { type: 'string', description: 'Passenger ID from search (pas_xxx)' },
              given_name: { type: 'string', description: 'First name (passport)' },
              family_name: { type: 'string', description: 'Last name (passport)' },
              born_on: { type: 'string', description: 'DOB YYYY-MM-DD' },
              gender: { type: 'string', description: 'm or f', default: 'm' },
              title: { type: 'string', description: 'mr, ms, mrs, miss', default: 'mr' },
              email: { type: 'string', description: 'Email' },
              phone_number: { type: 'string', description: 'Phone with country code' },
            },
          },
        },
        contact_email: { type: 'string', description: 'Booking contact email' },
        idempotency_key: { type: 'string', description: 'Unique key to prevent double-bookings on retry (e.g., UUID). Strongly recommended.' },
      },
    },
  },
  {
    name: 'setup_payment',
    description:
      "Legacy payment setup — no longer required. Use link_github instead for free unlimited access.\n\n" +
      'Only needed if you want the old Stripe-based flow. For most users, star the GitHub repo and call link_github.',
    inputSchema: {
      type: 'object',
      properties: {
        token: { type: 'string', description: "Payment token (e.g., 'tok_visa' for testing)" },
        payment_method_id: { type: 'string', description: 'Payment method ID (pm_xxx)' },
      },
    },
  },
  {
    name: 'get_agent_profile',
    description:
      "Get agent profile, payment status, and usage stats (searches, unlocks, bookings, fees).\n\n" +
      'Read-only. Safe to call multiple times.',
    inputSchema: { type: 'object', properties: {} },
  },
  {
    name: 'start_checkout',
    description:
      'Automate airline checkout up to the payment page — NEVER submits payment.\n\n' +
      'FLOW: search_flights → unlock_flight_offer → start_checkout\n\n' +
      'Uses Playwright to drive the airline website: selects flights, fills passenger details, ' +
      'skips extras/seats, and stops at the payment form. Returns a screenshot and booking URL ' +
      'so the user can complete manually in their browser.\n\n' +
      'Supported airlines: Ryanair, Wizz Air, EasyJet. Other airlines return booking URL only.\n\n' +
      'SAFETY: Uses fake test data by default. Never enters payment info. The checkout_token from ' +
      'unlock_flight_offer is required — prevents unauthorized usage.\n\n' +
      'Runs locally via Python subprocess (pip install letsfg && playwright install chromium).',
    inputSchema: {
      type: 'object',
      required: ['offer_id', 'checkout_token'],
      properties: {
        offer_id: { type: 'string', description: 'Offer ID from search results (off_xxx)' },
        checkout_token: { type: 'string', description: 'Token from unlock_flight_offer response' },
        passengers: {
          type: 'array',
          description: 'Passenger details. If omitted, uses safe test data (John Doe, test@example.com)',
          items: {
            type: 'object',
            properties: {
              given_name: { type: 'string' },
              family_name: { type: 'string' },
              born_on: { type: 'string', description: 'DOB YYYY-MM-DD' },
              gender: { type: 'string', description: 'm or f' },
              title: { type: 'string', description: 'mr, ms, mrs' },
              email: { type: 'string' },
              phone_number: { type: 'string' },
            },
          },
        },
      },
    },
  },
  {
    name: 'link_github',
    description:
      'Link your GitHub account for FREE unlimited access — star the repo and verify.\n\n' +
      'FLOW: 1) User stars https://github.com/LetsFG/LetsFG  2) Call this tool with their GitHub username\n\n' +
      'Once verified, the user gets unlimited search, unlock, and booking forever.\n' +
      'Only needs to be called once. If already verified, returns confirmation.\n\n' +
      'If status is "star_required", the user needs to star the repo first, then try again.',
    inputSchema: {
      type: 'object',
      required: ['github_username'],
      properties: {
        github_username: { type: 'string', description: "User's GitHub username (e.g., 'octocat')" },
      },
    },
  },
  {
    name: 'system_info',
    description:
      'Get system resource info (RAM, CPU cores) and recommended concurrency settings.\n\n' +
      'Use this to determine optimal max_browsers value for search_flights. ' +
      'Returns RAM total/available, CPU cores, recommended max browsers, and performance tier.\n\n' +
      'Tiers: minimal (<2GB, max 2), low (2-4GB, max 3), moderate (4-8GB, max 5), ' +
      'standard (8-16GB, max 8), high (16-32GB, max 12), maximum (32+GB, max 16).\n\n' +
      'Read-only, no side effects, instant response.',
    inputSchema: { type: 'object', properties: {} },
  },
];

// ── API Client ──────────────────────────────────────────────────────────

async function apiRequest(method: string, path: string, body?: Record<string, unknown>): Promise<unknown> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'User-Agent': 'letsfg-mcp/1.0.0',
  };
  if (API_KEY) headers['X-API-Key'] = API_KEY;

  const resp = await fetch(`${BASE_URL}${path}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  const data = await resp.json();
  if (resp.status >= 400) {
    return { error: true, status_code: resp.status, detail: (data as Record<string, string>).detail || JSON.stringify(data) };
  }
  return data;
}

// ── Tool Handlers ───────────────────────────────────────────────────────

async function callTool(name: string, args: Record<string, unknown>): Promise<string> {
  switch (name) {
    case 'search_flights': {
      const params: Record<string, unknown> = {
        origin: args.origin,
        destination: args.destination,
        date_from: args.date_from,
        adults: args.adults ?? 1,
        children: args.children ?? 0,
        currency: args.currency ?? 'EUR',
        limit: args.max_results ?? 10,
      };
      if (args.return_from) params.return_from = args.return_from;
      if (args.cabin_class) params.cabin_class = args.cabin_class;
      if (args.max_browsers) params.max_browsers = args.max_browsers;

      // Run local Python connectors
      const result = await searchLocal(params) as Record<string, unknown>;
      if (result.error) return JSON.stringify(result, null, 2);

      const offers = (result.offers || []) as Array<Record<string, unknown>>;
      const sourceTiers = result.source_tiers as Record<string, string> | undefined;
      const hasBackend = sourceTiers ? Object.keys(sourceTiers).some(t => t === 'paid') : false;
      const summary: Record<string, unknown> = {
        total_offers: offers.length,
        source: hasBackend
          ? 'local_connectors (75 airlines) + backend (Amadeus, Duffel, Sabre)'
          : 'local_connectors (75 airlines) — set LETSFG_API_KEY to also query Amadeus/Duffel/Sabre for more results',
        offers: offers.map(o => ({
          offer_id: o.id,
          price: `${o.price} ${o.currency}`,
          airlines: o.airlines,
          source: o.source,
          booking_url: o.booking_url,
          outbound: (() => {
            const ob = o.outbound as Record<string, unknown> | undefined;
            const segs = (ob?.segments || []) as Array<Record<string, string>>;
            return segs.length ? {
              from: segs[0].origin,
              to: segs[segs.length - 1].destination,
              departure: segs[0].departure,
              flight: segs[0].flight_no,
              airline: segs[0].airline_name || segs[0].airline,
              stops: ob?.stopovers,
            } : null;
          })(),
        })),
      };
      return JSON.stringify(summary, null, 2);
    }

    case 'resolve_location': {
      const result = await apiRequest('GET', `/api/v1/flights/locations/${encodeURIComponent(args.query as string)}`);
      return JSON.stringify(result, null, 2);
    }

    case 'unlock_flight_offer': {
      const result = await apiRequest('POST', '/api/v1/bookings/unlock', { offer_id: args.offer_id });
      return JSON.stringify(result, null, 2);
    }

    case 'link_github': {
      const result = await apiRequest('POST', `/api/v1/agents/link-github?github_username=${encodeURIComponent(args.github_username as string)}`, {});
      return JSON.stringify(result, null, 2);
    }

    case 'book_flight': {
      const body: Record<string, unknown> = {
        offer_id: args.offer_id,
        booking_type: 'flight',
        passengers: args.passengers,
        contact_email: args.contact_email,
      };
      if (args.idempotency_key) body.idempotency_key = args.idempotency_key;
      const result = await apiRequest('POST', '/api/v1/bookings/book', body);
      return JSON.stringify(result, null, 2);
    }

    case 'setup_payment': {
      const body: Record<string, unknown> = {};
      if (args.token) body.token = args.token;
      if (args.payment_method_id) body.payment_method_id = args.payment_method_id;
      const result = await apiRequest('POST', '/api/v1/agents/setup-payment', body);
      return JSON.stringify(result, null, 2);
    }

    case 'get_agent_profile': {
      const result = await apiRequest('GET', '/api/v1/agents/me');
      return JSON.stringify(result, null, 2);
    }

    case 'system_info': {
      const result = await searchLocal({ __system_info: true }) as Record<string, unknown>;
      return JSON.stringify(result, null, 2);
    }

    case 'start_checkout': {
      // Runs locally via Python — drives browser to payment page
      const result = await searchLocal({
        __checkout: true,
        offer_id: args.offer_id,
        passengers: args.passengers || null,
        checkout_token: args.checkout_token,
        api_key: API_KEY,
        base_url: BASE_URL,
      }) as Record<string, unknown>;

      if (result.error) return JSON.stringify(result, null, 2);

      const summary: Record<string, unknown> = {
        status: result.status,
        step: result.step,
        airline: result.airline,
        message: result.message,
        total_price: result.total_price ? `${result.total_price} ${result.currency}` : undefined,
        booking_url: result.booking_url,
        can_complete_manually: result.can_complete_manually,
        elapsed_seconds: result.elapsed_seconds,
      };
      if (result.screenshot_b64) {
        summary.screenshot = '(base64 screenshot attached — render with image tool if available)';
      }
      return JSON.stringify(summary, null, 2);
    }

    default:
      return JSON.stringify({ error: `Unknown tool: ${name}` });
  }
}

// ── MCP Protocol (stdio) ───────────────────────────────────────────────

function send(msg: Record<string, unknown>) {
  process.stdout.write(JSON.stringify(msg) + '\n');
}

const rl = readline.createInterface({ input: process.stdin, terminal: false });

rl.on('line', async (line) => {
  let msg: Record<string, unknown>;
  try {
    msg = JSON.parse(line);
  } catch {
    return;
  }

  const method = msg.method as string;
  const id = msg.id;

  switch (method) {
    case 'initialize':
      send({
        jsonrpc: '2.0',
        id,
        result: {
          protocolVersion: '2024-11-05',
          capabilities: { tools: {} },
          serverInfo: { name: 'letsfg', version: VERSION },
        },
      });
      break;

    case 'notifications/initialized':
      break;

    case 'tools/list':
      send({ jsonrpc: '2.0', id, result: { tools: TOOLS } });
      break;

    case 'tools/call': {
      const params = msg.params as Record<string, unknown>;
      const toolName = params.name as string;
      const toolArgs = (params.arguments || {}) as Record<string, unknown>;

      try {
        const text = await callTool(toolName, toolArgs);
        send({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text }] } });
      } catch (e) {
        send({ jsonrpc: '2.0', id, result: { content: [{ type: 'text', text: `Error: ${e}` }], isError: true } });
      }
      break;
    }

    case 'ping':
      send({ jsonrpc: '2.0', id, result: {} });
      break;

    default:
      if (id) {
        send({ jsonrpc: '2.0', id, error: { code: -32601, message: `Method not found: ${method}` } });
      }
  }
});

process.stderr.write(`LetsFG MCP v${VERSION} | local connectors: 75 airlines | api: ${API_KEY ? 'key set' : 'search-only (no key)'}\n`);
