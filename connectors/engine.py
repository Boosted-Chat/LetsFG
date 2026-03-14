"""
Unified flight search engine — fires ALL sources in parallel:

1. Local agent-device connectors (46+ LCC scrapers) — zero auth, free
2. Cloud Run backend API (Duffel, Amadeus, Sabre, Travelport, etc.) — paid providers

Both fire simultaneously via asyncio.gather. The backend call is a single
HTTP POST to the Cloud Run API which internally parallelizes all paid providers.
Results from both are merged, deduplicated, and sorted.

Environment variables:
  BOOSTEDTRAVEL_API_KEY  — API key for the Cloud Run backend
  BOOSTEDTRAVEL_BASE_URL — Backend URL (default: https://api.boostedchat.com)
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sys
from typing import Optional

import httpx

from connectors.combo_engine import build_combos
from connectors.currency import fetch_rates, _fallback_convert
from connectors.airline_routes import get_country, get_relevant_connectors, AIRLINE_COUNTRIES
from connectors.ryanair import RyanairConnectorClient
from connectors.wizzair import WizzairConnectorClient
from connectors.kiwi import KiwiConnectorClient

# ── Direct airline website connectors (LCCs not in GDS) ────────────────────────
from connectors.easyjet import EasyjetConnectorClient
from connectors.southwest import SouthwestConnectorClient
from connectors.airasia import AirAsiaConnectorClient
from connectors.indigo import IndiGoConnectorClient
from connectors.norwegian import NorwegianConnectorClient
from connectors.vueling import VuelingConnectorClient
from connectors.eurowings import EurowingsConnectorClient
from connectors.transavia import TransaviaConnectorClient
from connectors.pegasus import PegasusConnectorClient
from connectors.flydubai import FlydubaiConnectorClient
from connectors.spirit import SpiritConnectorClient
from connectors.frontier import FrontierConnectorClient
from connectors.volaris import VolarisConnectorClient
from connectors.airarabia import AirArabiaConnectorClient
from connectors.vietjet import VietJetConnectorClient
from connectors.cebupacific import CebuPacificConnectorClient
from connectors.scoot import ScootConnectorClient
from connectors.jetsmart import JetSmartConnectorClient
from connectors.jetstar import JetstarConnectorClient
from connectors.jet2 import Jet2ConnectorClient
from connectors.flynas import FlynasConnectorClient
from connectors.gol import GolConnectorClient
from connectors.azul import AzulConnectorClient
from connectors.flysafair import FlySafairConnectorClient
from connectors.vivaaerobus import VivaAerobusConnectorClient
from connectors.allegiant import AllegiantConnectorClient
from connectors.jetblue import JetBlueConnectorClient
from connectors.flair import FlairConnectorClient
from connectors.spicejet import SpiceJetConnectorClient
from connectors.akasa import AkasaConnectorClient
from connectors.spring import SpringConnectorClient
from connectors.peach import PeachConnectorClient
from connectors.zipair import ZipairConnectorClient
from connectors.condor import CondorConnectorClient
from connectors.play import PlayConnectorClient
from connectors.sunexpress import SunExpressConnectorClient
from connectors.volotea import VoloteaConnectorClient
from connectors.smartwings import SmartwingsConnectorClient
from connectors.flybondi import FlybondiConnectorClient
from connectors.jejuair import JejuAirConnectorClient
from connectors.twayair import TwayAirConnectorClient
from connectors.porter import PorterConnectorClient
from connectors.nokair import NokAirConnectorClient
from connectors.airpeace import AirPeaceConnectorClient
from connectors.airindiaexpress import AirIndiaExpressConnectorClient
from connectors.batikair import BatikAirConnectorClient
from connectors.luckyair import LuckyAirConnectorClient
from connectors.nineair import NineAirConnectorClient
from connectors.avelo import AveloConnectorClient
from connectors.breeze import BreezeConnectorClient
from connectors.salamair import SalamAirConnectorClient
from connectors.usbangla import USBanglaConnectorClient
from connectors.biman import BimanConnectorClient
from connectors.etihad import EtihadConnectorClient
from connectors.suncountry import SunCountryConnectorClient

from models.flights import AirlineSummary, FlightOffer, FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)

# Connectors that launch Chrome/Playwright browsers.
# These are throttled by a semaphore to prevent 20+ Chrome processes at once.
_BROWSER_SOURCES: set[str] = {
    "airasia_direct", "allegiant_direct", "azul_direct", "batikair_direct",
    "cebupacific_direct", "condor_direct", "easyjet_direct", "eurowings_direct",
    "flybondi_direct", "flydubai_direct", "flynas_direct", "frontier_direct",
    "gol_direct", "indigo_direct", "jet2_direct", "jetsmart_direct",
    "jetstar_direct", "luckyair_direct", "9air_direct",
    "jetblue_direct", "avelo_direct", "breeze_direct",
    "norwegian_direct", "peach_direct", "pegasus_direct", "play_direct",
    "porter_direct", "scoot_direct", "smartwings_direct", "southwest_direct",
    "spirit_direct", "sunexpress_direct", "transavia_direct", "twayair_direct",
    "vietjet_direct", "volaris_direct", "volotea_direct", "vueling_direct",
    "usbangla_direct",
    "etihad_direct",
    "zipair_direct",
    "suncountry_direct",
}

# Registry of direct airline connectors: (source_name, connector_class, timeout)
# All are zero-auth, always available, "free" tier.
_DIRECT_AIRLINE_connectorS: list[tuple[str, type, float]] = [
    ("easyjet_direct", EasyjetConnectorClient, 25.0),
    ("southwest_direct", SouthwestConnectorClient, 25.0),
    ("airasia_direct", AirAsiaConnectorClient, 25.0),
    ("indigo_direct", IndiGoConnectorClient, 25.0),
    ("norwegian_direct", NorwegianConnectorClient, 25.0),
    ("vueling_direct", VuelingConnectorClient, 25.0),
    ("eurowings_direct", EurowingsConnectorClient, 25.0),
    ("transavia_direct", TransaviaConnectorClient, 25.0),
    ("pegasus_direct", PegasusConnectorClient, 25.0),
    ("flydubai_direct", FlydubaiConnectorClient, 25.0),
    ("spirit_direct", SpiritConnectorClient, 25.0),
    ("frontier_direct", FrontierConnectorClient, 25.0),
    ("volaris_direct", VolarisConnectorClient, 25.0),
    ("airarabia_direct", AirArabiaConnectorClient, 25.0),
    ("vietjet_direct", VietJetConnectorClient, 25.0),
    ("cebupacific_direct", CebuPacificConnectorClient, 25.0),
    ("scoot_direct", ScootConnectorClient, 25.0),
    ("jetsmart_direct", JetSmartConnectorClient, 25.0),
    ("jetstar_direct", JetstarConnectorClient, 25.0),
    ("jet2_direct", Jet2ConnectorClient, 25.0),
    ("flynas_direct", FlynasConnectorClient, 25.0),
    ("gol_direct", GolConnectorClient, 25.0),
    ("azul_direct", AzulConnectorClient, 25.0),
    ("flysafair_direct", FlySafairConnectorClient, 25.0),
    ("vivaaerobus_direct", VivaAerobusConnectorClient, 25.0),
    ("allegiant_direct", AllegiantConnectorClient, 25.0),    ("jetblue_direct", JetBlueConnectorClient, 25.0),    ("flair_direct", FlairConnectorClient, 25.0),
    ("spicejet_direct", SpiceJetConnectorClient, 25.0),
    ("akasa_direct", AkasaConnectorClient, 25.0),
    ("spring_direct", SpringConnectorClient, 25.0),
    ("peach_direct", PeachConnectorClient, 25.0),
    ("zipair_direct", ZipairConnectorClient, 25.0),
    ("condor_direct", CondorConnectorClient, 25.0),
    ("play_direct", PlayConnectorClient, 25.0),
    ("sunexpress_direct", SunExpressConnectorClient, 25.0),
    ("volotea_direct", VoloteaConnectorClient, 25.0),
    ("smartwings_direct", SmartwingsConnectorClient, 25.0),
    ("flybondi_direct", FlybondiConnectorClient, 25.0),
    ("jejuair_direct", JejuAirConnectorClient, 25.0),
    ("twayair_direct", TwayAirConnectorClient, 25.0),
    ("porter_direct", PorterConnectorClient, 25.0),
    ("nokair_direct", NokAirConnectorClient, 25.0),
    ("airpeace_direct", AirPeaceConnectorClient, 25.0),
    ("airindiaexpress_direct", AirIndiaExpressConnectorClient, 25.0),
    ("batikair_direct", BatikAirConnectorClient, 25.0),
    ("luckyair_direct", LuckyAirConnectorClient, 30.0),
    ("9air_direct", NineAirConnectorClient, 30.0),
    ("avelo_direct", AveloConnectorClient, 45.0),
    ("breeze_direct", BreezeConnectorClient, 45.0),
    ("salamair_direct", SalamAirConnectorClient, 20.0),
    ("usbangla_direct", USBanglaConnectorClient, 45.0),
    ("biman_direct", BimanConnectorClient, 25.0),
    ("etihad_direct", EtihadConnectorClient, 35.0),
    ("suncountry_direct", SunCountryConnectorClient, 45.0),
]


def _extract_legs_from_roundtrip(
    offers: list[FlightOffer],
    outbound_legs: list[FlightOffer],
    return_legs: list[FlightOffer],
) -> None:
    """Decompose round-trip offers into one-way legs for the combo engine.

    For airlines like Wizzair/Ryanair that price legs independently,
    we extract each direction so the combo engine can mix airlines.
    """
    seen_out: set[str] = set()
    seen_ret: set[str] = set()

    for offer in offers:
        if not offer.outbound or not offer.inbound:
            continue

        ob_key = "|".join(
            f"{s.flight_no}_{s.departure.isoformat()}"
            for s in offer.outbound.segments
        )
        rt_key = "|".join(
            f"{s.flight_no}_{s.departure.isoformat()}"
            for s in offer.inbound.segments
        )

        # Estimate per-leg price as half (the combo engine prices from
        # the cheapest one-way leg it finds across all sources)
        half_price = round(offer.price / 2, 2)

        if ob_key not in seen_out:
            seen_out.add(ob_key)
            outbound_legs.append(FlightOffer(
                id=f"{offer.id}_ob",
                price=half_price,
                currency=offer.currency,
                price_formatted=f"{half_price:.2f} {offer.currency}",
                price_normalized=offer.price_normalized / 2 if offer.price_normalized else None,
                outbound=offer.outbound,
                inbound=None,
                airlines=offer.airlines,
                owner_airline=offer.owner_airline,
                booking_url=offer.booking_url,
                is_locked=False,
                source=offer.source,
                source_tier=offer.source_tier,
            ))

        if rt_key not in seen_ret:
            seen_ret.add(rt_key)
            return_legs.append(FlightOffer(
                id=f"{offer.id}_rt",
                price=half_price,
                currency=offer.currency,
                price_formatted=f"{half_price:.2f} {offer.currency}",
                price_normalized=offer.price_normalized / 2 if offer.price_normalized else None,
                outbound=offer.inbound,  # inbound route becomes "outbound" of return leg
                inbound=None,
                airlines=offer.airlines,
                owner_airline=offer.owner_airline,
                booking_url=offer.booking_url,
                is_locked=False,
                source=offer.source,
                source_tier=offer.source_tier,
            ))


class MultiProvider:
    """Searches ALL flight sources in parallel — local connectors + Cloud Run backend."""

    _BACKEND_URL = os.environ.get("BOOSTEDTRAVEL_BASE_URL", "https://api.boostedchat.com").rstrip("/")
    _BACKEND_KEY = os.environ.get("BOOSTEDTRAVEL_API_KEY", "")
    _BACKEND_TIMEOUT = 30.0  # Backend queries paid APIs in parallel; 30s covers slowest GDS

    # ── Backend availability ─────────────────────────────────────────────

    @property
    def backend_available(self) -> bool:
        return bool(self._BACKEND_KEY)

    # Direct airline connectors — always available, no API keys needed
    @property
    def ryanair_connector_available(self) -> bool:
        return True

    @property
    def wizzair_connector_available(self) -> bool:
        return True

    @property
    def kiwi_connector_available(self) -> bool:
        return True

    def _get_ryanair_connector(self) -> Optional[RyanairConnectorClient]:
        return RyanairConnectorClient(timeout=20.0)

    def _get_wizzair_connector(self) -> Optional[WizzairConnectorClient]:
        return WizzairConnectorClient(timeout=25.0)

    def _get_kiwi_connector(self) -> Optional[KiwiConnectorClient]:
        return KiwiConnectorClient(timeout=25.0)

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search flights across ALL sources in parallel:
        - 1 async HTTP call to Cloud Run backend (paid APIs: Duffel, Amadeus, Sabre, etc.)
        - 46+ local connector tasks (LCC scrapers running on agent device)
        - Ryanair, Wizzair, Kiwi connectors (special handling)
        - Combo engine for cross-airline virtual interlining

        Everything fires at once via asyncio.gather. Total wall-clock time =
        max(backend_latency, slowest_local_connector) — typically 5-15s.

        All browser instances launched by connectors are automatically
        closed after results are collected.
        """
        try:
            return await self._search_flights_inner(req)
        finally:
            await self._cleanup_connectors()

    async def _search_flights_inner(self, req: FlightSearchRequest) -> FlightSearchResponse:
        tasks = []
        providers_used = []

        # ── Cloud Run backend (paid API providers: Duffel, Amadeus, Sabre, etc.) ──
        if self.backend_available:
            tasks.append(self._search_backend(req))
            providers_used.append("backend")
        else:
            logger.warning("No BOOSTEDTRAVEL_API_KEY — skipping Cloud Run backend (paid APIs)")

        # ── Local connectors: Ryanair, Wizzair, Kiwi (special handling) ──
        origin_country = get_country(req.origin)
        dest_country = get_country(req.destination)

        ryanair_connector = self._get_ryanair_connector()
        wizzair_connector = self._get_wizzair_connector()
        kiwi_connector = self._get_kiwi_connector()

        ryanair_countries = AIRLINE_COUNTRIES.get("ryanair")
        if ryanair_connector and (not origin_country or not dest_country or not ryanair_countries
                or origin_country in ryanair_countries or dest_country in ryanair_countries):
            tasks.append(self._search_ryanair_direct(ryanair_connector, req))
            providers_used.append("ryanair_direct")

        wizz_countries = AIRLINE_COUNTRIES.get("wizz")
        if wizzair_connector and (not origin_country or not dest_country or not wizz_countries
                or origin_country in wizz_countries or dest_country in wizz_countries):
            tasks.append(self._search_wizzair_direct(wizzair_connector, req))
            providers_used.append("wizzair_direct")

        # Kiwi is a global aggregator — always query it
        if kiwi_connector:
            tasks.append(self._search_kiwi_connector(kiwi_connector, req))
            providers_used.append("kiwi_connector")

        # ── Direct airline website connectors (46 LCCs) — route-filtered ──
        filtered_connectors = get_relevant_connectors(req.origin, req.destination, _DIRECT_AIRLINE_connectorS)
        skipped = len(_DIRECT_AIRLINE_connectorS) - len(filtered_connectors)
        if skipped:
            logger.info("Route filter: %s->%s -- skipped %d/%d irrelevant connectors",
                        req.origin, req.destination, skipped, len(_DIRECT_AIRLINE_connectorS))
        for source, connector_cls, timeout in filtered_connectors:
            connector = connector_cls(timeout=timeout)
            tasks.append(self._search_connector_generic(connector, req, source))
            providers_used.append(source)

        # ── Combo engine: one-way legs for cross-airline virtual interlining ──
        combo_tasks: list[asyncio.Task] = []
        combo_labels: list[str] = []
        is_round_trip = bool(req.return_from)

        if is_round_trip:
            # Build one-way requests for each direction
            outbound_req = req.model_copy(update={
                "return_from": None, "return_to": None,
            })
            return_req = req.model_copy(update={
                "origin": req.destination,
                "destination": req.origin,
                "date_from": req.return_from,
                "date_to": req.return_to,
                "return_from": None,
                "return_to": None,
            })

            # Fire one-way searches through direct connectors for combo engine.
            # SKIP Wizzair — its round-trip search already returns separate outbound/return
            # flights.  We extract those as one-way legs below to avoid extra API calls
            # that trigger rate limiting.
            for label, getter in [
                ("ryanair_direct", self._get_ryanair_connector),
                ("kiwi_connector", self._get_kiwi_connector),
            ]:
                client_out = getter()
                client_ret = getter()
                if client_out:
                    search_fn = self._combo_search_fn(label)
                    combo_tasks.append(search_fn(client_out, outbound_req))
                    combo_labels.append(f"{label}_out")
                    combo_tasks.append(search_fn(client_ret, return_req))
                    combo_labels.append(f"{label}_ret")

        if not tasks:
            logger.error("No flight providers configured!")
            return FlightSearchResponse(
                search_id="",
                origin=req.origin,
                destination=req.destination,
                currency=req.currency,
                offers=[],
                total_results=0,
            )

        # Run all providers in parallel (normal + combo one-way searches)
        all_tasks = tasks + combo_tasks
        results = await asyncio.gather(*all_tasks, return_exceptions=True)

        # Split results: normal providers vs combo legs
        normal_results = results[:len(tasks)]
        combo_results = results[len(tasks):]

        # Collect offers from successful providers
        all_offers: list[FlightOffer] = []
        merged_passenger_ids = []
        merged_offer_request_id = ""

        for i, result in enumerate(normal_results):
            provider = providers_used[i]
            if isinstance(result, Exception):
                logger.error("Provider %s failed: %s", provider, result)
                continue
            if isinstance(result, FlightSearchResponse):
                all_offers.extend(result.offers)
                # Keep backend's passenger_ids and offer_request_id (needed for bookings)
                if provider == "backend" and result.passenger_ids:
                    merged_passenger_ids = result.passenger_ids
                    merged_offer_request_id = result.offer_request_id

        # ── Build cross-airline combos from one-way legs ──
        if is_round_trip and (combo_results or True):
            outbound_legs: list[FlightOffer] = []
            return_legs: list[FlightOffer] = []

            for i, result in enumerate(combo_results):
                if isinstance(result, Exception):
                    logger.debug("Combo leg %s failed: %s", combo_labels[i], result)
                    continue
                if isinstance(result, FlightSearchResponse):
                    direction = combo_labels[i]
                    for offer in result.offers:
                        if direction.endswith("_out"):
                            outbound_legs.append(offer)
                        else:
                            return_legs.append(offer)

            # Extract Wizzair one-way legs from round-trip results (avoids extra API calls)
            w6_idx = None
            for i, p in enumerate(providers_used):
                if p == "wizzair_direct":
                    w6_idx = i
                    break
            if w6_idx is not None:
                w6_result = normal_results[w6_idx]
                if isinstance(w6_result, FlightSearchResponse):
                    _extract_legs_from_roundtrip(w6_result.offers, outbound_legs, return_legs)

            # Normalize one-way leg prices before combining
            await self._normalize_prices(outbound_legs, req.currency)
            await self._normalize_prices(return_legs, req.currency)

            # Build cross-airlines combos
            combos = build_combos(outbound_legs, return_legs, req.currency)
            logger.info(
                "Combo engine produced %d cross-airline offers from %d out + %d ret legs",
                len(combos), len(outbound_legs), len(return_legs),
            )
            all_offers.extend(combos)

        # Normalize prices to requested currency for fair comparison
        await self._normalize_prices(all_offers, req.currency)

        # Deduplicate similar offers (same route, similar time, similar price)
        deduped = self._deduplicate(all_offers)

        # --- Airline-diverse selection ---
        # Ensure at least the cheapest offer per airline is included,
        # then fill remaining slots with overall cheapest.
        deduped = self._diverse_select(deduped, req.sort, req.limit)

        # Build airlines summary (cheapest per airline across ALL deduped offers)
        airlines_summary = self._build_airlines_summary(all_offers)

        search_hash = hashlib.md5(
            f"{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        # Build source tiers map from providers that returned results
        source_tiers: dict[str, str] = {}
        tier_providers: dict[str, list[str]] = {}
        for offer in deduped:
            tier = offer.source_tier or "paid"
            src = offer.source or "unknown"
            if tier not in tier_providers:
                tier_providers[tier] = []
            if src not in tier_providers[tier]:
                tier_providers[tier].append(src)
        for tier, srcs in tier_providers.items():
            source_tiers[tier] = ", ".join(srcs)

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            offer_request_id=merged_offer_request_id,
            passenger_ids=merged_passenger_ids,
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=deduped,
            total_results=len(deduped),
            airlines_summary=airlines_summary,
            search_params={},
            source_tiers=source_tiers,
        )

    # ── Per-connector browser cleanup ──────────────────────────────────────────

    async def _cleanup_single_connector(self, client) -> None:
        """Close a single connector's module-level browser resources immediately.

        Called after a browser-based connector finishes so its Chrome process
        is freed without waiting for the full search to complete.
        """
        from connectors.browser import cleanup_module_browsers

        mod = sys.modules.get(type(client).__module__)
        if mod:
            closed = await cleanup_module_browsers(mod)
            if closed:
                logger.debug("Early cleanup: closed %d resource(s) for %s",
                             closed, type(client).__name__)

    # ── Full browser cleanup ─────────────────────────────────────────────────────

    async def _cleanup_connectors(self):
        """Close all browser instances launched by connectors during search.

        Introspects module-level globals (_browser, _chrome_proc, _pw_instance,
        _nd_browser etc.) in every imported connector module and terminates them.
        Only affects processes we created — never kills the user's own Chrome.
        """
        from connectors.browser import cleanup_module_browsers, cleanup_all_browsers

        modules_to_clean = []
        seen = set()

        # All direct airline connector modules
        for _source, cls, _timeout in _DIRECT_AIRLINE_connectorS:
            mod = sys.modules.get(cls.__module__)
            if mod and id(mod) not in seen:
                seen.add(id(mod))
                modules_to_clean.append(mod)

        # Ryanair, Wizzair, Kiwi connector modules
        for mod_name in ('connectors.ryanair', 'connectors.wizzair', 'connectors.kiwi_connector'):
            mod = sys.modules.get(mod_name)
            if mod and id(mod) not in seen:
                seen.add(id(mod))
                modules_to_clean.append(mod)

        per_mod = await cleanup_module_browsers(*modules_to_clean)
        helper = await cleanup_all_browsers()
        total = per_mod + helper
        if total:
            logger.info("Browser cleanup: closed %d resource(s) across %d modules",
                        total, len(modules_to_clean))

    # ── Cloud Run backend (single HTTP call, fires all paid APIs in parallel on server) ──

    async def _search_backend(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Call the Cloud Run backend API which queries all paid providers
        (Duffel, Amadeus, Sabre, Travelport, Kiwi, Travelpayouts, MCP, Trip.com)
        in parallel on the server side. Returns merged results.
        """
        body = {
            "origin": req.origin,
            "destination": req.destination,
            "date_from": str(req.date_from),
            "adults": req.adults,
            "children": req.children,
            "infants": req.infants,
            "max_stopovers": req.max_stopovers,
            "currency": req.currency,
            "limit": req.limit,
            "sort": req.sort,
        }
        if req.date_to:
            body["date_to"] = str(req.date_to)
        if req.return_from:
            body["return_from"] = str(req.return_from)
        if req.return_to:
            body["return_to"] = str(req.return_to)
        if req.cabin_class:
            body["cabin_class"] = req.cabin_class

        url = f"{self._BACKEND_URL}/api/v1/flights/search"
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self._BACKEND_KEY,
            "User-Agent": "boostedtravel-engine/1.0",
        }

        async with httpx.AsyncClient(timeout=self._BACKEND_TIMEOUT) as client:
            resp = await client.post(url, json=body, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        result = FlightSearchResponse(**data)
        logger.info("Backend returned %d offers from %s",
                     len(result.offers), result.source_tiers)
        return result

    # ── Local connector search methods ───────────────────────────────────

    async def _search_ryanair_direct(
        self, client: RyanairConnectorClient, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        """Search Ryanair's website API directly — definitive LCC pricing."""
        try:
            result = await client.search_flights(req)
            for offer in result.offers:
                offer.source = "ryanair_direct"
                offer.source_tier = "free"
            return result
        finally:
            await client.close()

    async def _search_wizzair_direct(
        self, client: WizzairConnectorClient, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        """Search Wizzair's website API directly — definitive LCC pricing."""
        try:
            result = await client.search_flights(req)
            for offer in result.offers:
                offer.source = "wizzair_direct"
                offer.source_tier = "free"
            return result
        finally:
            await client.close()

    async def _search_kiwi_connector(
        self, client: KiwiConnectorClient, req: FlightSearchRequest
    ) -> FlightSearchResponse:
        """Search Kiwi.com's public Skypicker API — LCCs + virtual interlining."""
        try:
            result = await client.search_flights(req)
            for offer in result.offers:
                offer.source = "kiwi_connector"
                offer.source_tier = "free"
            return result
        finally:
            await client.close()

    async def _search_connector_generic(
        self, client, req: FlightSearchRequest, source: str
    ) -> FlightSearchResponse:
        """Generic wrapper for direct airline connectors — tags source/tier, ensures cleanup.

        Browser-based connectors are throttled by a semaphore so at most 4
        Chrome processes run simultaneously (prevents resource exhaustion).
        """
        uses_browser = source in _BROWSER_SOURCES
        if uses_browser:
            from connectors.browser import acquire_browser_slot
            await acquire_browser_slot()
        try:
            result = await client.search_flights(req)
            for offer in result.offers:
                offer.source = source
                offer.source_tier = "free"
            return result
        finally:
            await client.close()
            if uses_browser:
                # Close module-level browser globals immediately so Chrome
                # doesn't linger until the full search completes.
                await self._cleanup_single_connector(client)
                from connectors.browser import release_browser_slot
                release_browser_slot()

    def _combo_search_fn(self, label: str):
        """Return the appropriate search method for combo one-way legs."""
        mapping = {
            "ryanair_direct": self._search_ryanair_direct,
            "wizzair_direct": self._search_wizzair_direct,
            "kiwi_connector": self._search_kiwi_connector,
        }
        return mapping[label]

    def _deduplicate(self, offers: list[FlightOffer]) -> list[FlightOffer]:
        """
        Remove near-duplicate offers from different providers.

        Two offers are considered duplicates if they have:
        - Same airlines
        - Same departure time (within 10 min)
        - Same arrival time (within 10 min)

        When duplicates found, keep the cheaper one (using normalized price
        for correct cross-currency comparison).
        """
        if not offers:
            return []

        def _norm_price(o: FlightOffer) -> float:
            return o.price_normalized if o.price_normalized is not None else o.price

        seen: dict[str, FlightOffer] = {}
        for offer in offers:
            key = self._dedup_key(offer)
            if key in seen:
                # Keep the cheaper one (normalized for cross-currency comparison)
                if _norm_price(offer) < _norm_price(seen[key]):
                    seen[key] = offer
            else:
                seen[key] = offer

        return list(seen.values())

    @staticmethod
    async def _normalize_prices(offers: list[FlightOffer], target_currency: str) -> None:
        """Set price_normalized on every offer, converting to target_currency."""
        target = target_currency.upper()

        # Collect unique source currencies that need conversion
        source_currencies = {o.currency.upper() for o in offers if o.currency.upper() != target}

        # Pre-fetch rates for each source currency (max 1 API call each, cached)
        rates_cache: dict[str, dict[str, float]] = {}
        for cur in source_currencies:
            try:
                rates_cache[cur] = await fetch_rates(cur)
            except Exception:
                rates_cache[cur] = {}

        for offer in offers:
            src = offer.currency.upper()
            if src == target:
                offer.price_normalized = offer.price
            else:
                rates = rates_cache.get(src, {})
                if target in rates:
                    offer.price_normalized = offer.price * rates[target]
                else:
                    offer.price_normalized = _fallback_convert(offer.price, src, target)

    def _diverse_select(
        self, offers: list[FlightOffer], sort: str, limit: int
    ) -> list[FlightOffer]:
        """
        Select offers ensuring airline diversity.

        1. Sort all offers by requested criteria
        2. Pick the cheapest offer per airline first (guarantees diversity)
        3. Fill remaining slots with cheapest overall
        """
        if not offers:
            return []

        # Sort
        def _sort_price(o: FlightOffer) -> float:
            return o.price_normalized if o.price_normalized is not None else o.price

        if sort == "duration":
            offers.sort(key=lambda o: o.outbound.total_duration_seconds if o.outbound else 0)
        else:
            # Default: price (normalized)
            offers.sort(key=_sort_price)

        # Step 1: Pick cheapest per airline (owner_airline)
        best_per_airline: dict[str, FlightOffer] = {}
        for offer in offers:
            key = offer.owner_airline or (offer.airlines[0] if offer.airlines else "?")
            if key not in best_per_airline:
                best_per_airline[key] = offer

        # Step 2: Build result — airline bests first, then fill by sort order
        selected_ids: set[str] = set()
        result: list[FlightOffer] = []

        # Add airline bests (sorted by sort criteria)
        airline_bests = sorted(
            best_per_airline.values(),
            key=lambda o: _sort_price(o) if sort != "duration" else (o.outbound.total_duration_seconds if o.outbound else 0),
        )
        for offer in airline_bests:
            if len(result) >= limit:
                break
            result.append(offer)
            selected_ids.add(offer.id)

        # Fill remaining with overall sorted offers
        for offer in offers:
            if len(result) >= limit:
                break
            if offer.id not in selected_ids:
                result.append(offer)
                selected_ids.add(offer.id)

        return result

    def _build_airlines_summary(self, offers: list[FlightOffer]) -> list[AirlineSummary]:
        """Build a summary of cheapest offer per airline."""
        by_airline: dict[str, list[FlightOffer]] = {}
        for offer in offers:
            key = offer.owner_airline or (offer.airlines[0] if offer.airlines else "?")
            if key not in by_airline:
                by_airline[key] = []
            by_airline[key].append(offer)

        summaries = []
        for airline_code, airline_offers in by_airline.items():
            airline_offers.sort(key=lambda o: o.price_normalized if o.price_normalized is not None else o.price)
            cheapest = airline_offers[0]

            # Build sample route string
            route_parts = []
            if cheapest.outbound and cheapest.outbound.segments:
                for seg in cheapest.outbound.segments:
                    route_parts.append(seg.origin)
                route_parts.append(cheapest.outbound.segments[-1].destination)
            sample_route = "→".join(route_parts)

            # Get airline name from first segment
            airline_name = ""
            if cheapest.outbound and cheapest.outbound.segments:
                airline_name = cheapest.outbound.segments[0].airline_name

            summaries.append(AirlineSummary(
                airline_code=airline_code,
                airline_name=airline_name,
                cheapest_price=cheapest.price,
                currency=cheapest.currency,
                offer_count=len(airline_offers),
                cheapest_offer_id=cheapest.id,
                sample_route=sample_route,
            ))

        summaries.sort(key=lambda s: s.cheapest_price)
        return summaries

    @staticmethod
    def _dedup_key(offer: FlightOffer) -> str:
        """Generate a deduplication key based on route and timing."""
        parts = []
        if offer.outbound and offer.outbound.segments:
            first = offer.outbound.segments[0]
            last = offer.outbound.segments[-1]
            # Round departure to nearest 10 minutes for fuzzy matching
            dep_rounded = first.departure.replace(
                minute=(first.departure.minute // 10) * 10, second=0, microsecond=0
            )
            arr_rounded = last.arrival.replace(
                minute=(last.arrival.minute // 10) * 10, second=0, microsecond=0
            )
            parts.append(f"{first.origin}-{last.destination}-{dep_rounded.isoformat()}-{arr_rounded.isoformat()}")
            # Add airlines
            airline_str = "-".join(sorted(s.airline for s in offer.outbound.segments))
            parts.append(airline_str)

        if offer.inbound and offer.inbound.segments:
            first = offer.inbound.segments[0]
            last = offer.inbound.segments[-1]
            dep_rounded = first.departure.replace(
                minute=(first.departure.minute // 10) * 10, second=0, microsecond=0
            )
            parts.append(f"ret-{dep_rounded.isoformat()}")

        return "|".join(parts) if parts else offer.id

    # ── Location resolution ──────────────────────────────────────────────

    async def resolve_location(self, query: str) -> dict:
        """Resolve location via the Cloud Run backend."""
        if not self.backend_available:
            return {"locations": [], "error": "No BOOSTEDTRAVEL_API_KEY configured"}

        url = f"{self._BACKEND_URL}/api/v1/flights/locations/{query}"
        headers = {
            "X-API-Key": self._BACKEND_KEY,
            "User-Agent": "boostedtravel-engine/1.0",
        }
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()
            return resp.json()

    # ── Active providers info ────────────────────────────────────────────

    @property
    def active_providers(self) -> list[str]:
        providers = ["ryanair_direct", "wizzair_direct", "kiwi_connector"]
        providers.extend(name for name, _, _ in _DIRECT_AIRLINE_connectorS)
        if self.backend_available:
            providers.append("backend (Duffel, Amadeus, Sabre, Travelport, Kiwi, MCP, Trip.com)")
        return providers


# Singleton
multi_provider = MultiProvider()
