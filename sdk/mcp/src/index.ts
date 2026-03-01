#!/usr/bin/env node
/**
 * BoostedTravel MCP Server — Model Context Protocol integration.
 *
 * Makes BoostedTravel available as native tools in Claude Desktop, Cursor,
 * and any MCP-compatible AI agent.
 *
 * Usage in Claude Desktop / Cursor config:
 * {
 *   "mcpServers": {
 *     "boostedtravel": {
 *       "command": "npx",
 *       "args": ["@boostedtravel/mcp-server"],
 *       "env": {
 *         "BOOSTEDTRAVEL_API_KEY": "trav_your_api_key",
 *         "BOOSTEDTRAVEL_BASE_URL": "https://api.boostedchat.com"
 *       }
 *     }
 *   }
 * }
 */

import * as readline from 'readline';

// ── Config ──────────────────────────────────────────────────────────────

const BASE_URL = (process.env.BOOSTEDTRAVEL_BASE_URL || 'https://api.boostedchat.com').replace(/\/$/, '');
const API_KEY = process.env.BOOSTEDTRAVEL_API_KEY || '';

// ── Tool Definitions ────────────────────────────────────────────────────

const TOOLS = [
  {
    name: 'search_flights',
    description:
      'Search for flights between any two cities/airports worldwide. ' +
      'Connects to 400+ airlines via NDC and GDS with prices $20-50 cheaper than ' +
      'booking.com, Kayak, and other OTAs. Searching is completely FREE.\n\n' +
      'Returns flight offers with prices, airlines, times, and conditions. ' +
      'All offers are locked until you call `unlock_flight_offer` ($1 fee).\n\n' +
      'IMPORTANT: The response includes `passenger_ids` — save these for booking.',
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
      },
    },
  },
  {
    name: 'resolve_location',
    description: "Convert a city/airport name to IATA codes. Use when user says 'London' instead of 'LON'.",
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
      'Unlock a flight offer for booking — $1 fee. Confirms latest price and reserves for 30 minutes. ' +
      'Requires payment method (call setup_payment first).',
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
      'Requirements: 1) Offer must be unlocked first 2) Provide passenger_id from search 3) Full passenger details needed.',
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
      },
    },
  },
  {
    name: 'setup_payment',
    description:
      "Set up payment method. Required before unlock/book. For testing use token 'tok_visa'. Only needed once.",
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
    description: "Get agent profile, payment status, and usage stats (searches, unlocks, bookings, fees).",
    inputSchema: { type: 'object', properties: {} },
  },
];

// ── API Client ──────────────────────────────────────────────────────────

async function apiRequest(method: string, path: string, body?: Record<string, unknown>): Promise<unknown> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'User-Agent': 'boostedtravel-mcp/0.1.0',
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
      const body: Record<string, unknown> = {
        origin: args.origin,
        destination: args.destination,
        date_from: args.date_from,
        adults: args.adults ?? 1,
        children: args.children ?? 0,
        currency: args.currency ?? 'EUR',
        limit: args.max_results ?? 10,
      };
      if (args.return_from) body.return_from = args.return_from;
      if (args.cabin_class) body.cabin_class = args.cabin_class;

      const result = await apiRequest('POST', '/api/v1/flights/search', body) as Record<string, unknown>;
      if ((result as Record<string, unknown>).error) return JSON.stringify(result, null, 2);

      const offers = (result.offers || []) as Array<Record<string, unknown>>;
      const summary = {
        total_offers: offers.length,
        passenger_ids: result.passenger_ids,
        passenger_ids_note: 'SAVE THESE — required for booking',
        offers: offers.map(o => ({
          offer_id: o.id,
          price: o.price_formatted,
          airlines: o.airlines,
          outbound: (() => {
            const ob = o.outbound as Record<string, unknown> | undefined;
            const segs = (ob?.segments || []) as Array<Record<string, string>>;
            return segs.length ? {
              from: segs[0].origin,
              to: segs[segs.length - 1].destination,
              departure: segs[0].departure,
              stops: ob?.stopovers,
            } : null;
          })(),
          conditions: o.conditions,
          is_locked: o.is_locked,
        })),
        next_step: 'Call unlock_flight_offer, then book_flight',
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

    case 'book_flight': {
      const result = await apiRequest('POST', '/api/v1/bookings/book', {
        offer_id: args.offer_id,
        booking_type: 'flight',
        passengers: args.passengers,
        contact_email: args.contact_email,
      });
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
          serverInfo: { name: 'boostedtravel', version: '0.1.0' },
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

process.stderr.write(`BoostedTravel MCP server started | ${BASE_URL} | key: ${API_KEY ? 'set' : 'not set'}\n`);
