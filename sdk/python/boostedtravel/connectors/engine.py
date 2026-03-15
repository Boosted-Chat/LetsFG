"""
Unified flight search engine — fires ALL sources in parallel:

1. Local agent-device connectors (73 LCC connectors) — zero auth, free
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
from connectors.airline_routes import get_country, get_relevant_connectors, get_city_airports, AIRLINE_COUNTRIES
from connectors.ryanair import RyanairConnectorClient
from connectors.wizzair import WizzairConnectorClient
from connectors.kiwi import KiwiConnectorClient

# ── Dynamic connector loading ──────────────────────────────────────────────────
# Each connector is loaded inside try/except so that missing optional
# dependencies (e.g. curl_cffi, nodriver, patchright) only disable that
# specific connector rather than crashing the entire engine on import.

from models.flights import AirlineSummary, FlightOffer, FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


def _safe_import(module: str, cls: str):
    """Import *cls* from *module*, returning None on failure."""
    try:
        mod = __import__(module, fromlist=[cls])
        return getattr(mod, cls)
    except Exception as exc:
        logger.debug("Connector %s.%s unavailable: %s", module, cls, exc)
        return None


# (source_name, module_path, class_name, timeout_seconds)
_CONNECTOR_DEFS: list[tuple[str, str, str, float]] = [
    ("easyjet_direct", "connectors.easyjet", "EasyjetConnectorClient", 25.0),
    ("southwest_direct", "connectors.southwest", "SouthwestConnectorClient", 25.0),
    ("airasia_direct", "connectors.airasia", "AirAsiaConnectorClient", 25.0),
    ("indigo_direct", "connectors.indigo", "IndiGoConnectorClient", 25.0),
    ("norwegian_direct", "connectors.norwegian", "NorwegianConnectorClient", 25.0),
    ("vueling_direct", "connectors.vueling", "VuelingConnectorClient", 25.0),
    ("eurowings_direct", "connectors.eurowings", "EurowingsConnectorClient", 25.0),
    ("transavia_direct", "connectors.transavia", "TransaviaConnectorClient", 25.0),
    ("pegasus_direct", "connectors.pegasus", "PegasusConnectorClient", 25.0),
    ("flydubai_direct", "connectors.flydubai", "FlydubaiConnectorClient", 25.0),
    # spirit — blocked: PX Enterprise detects all automation (#28)
    ("frontier_direct", "connectors.frontier", "FrontierConnectorClient", 25.0),
    ("volaris_direct", "connectors.volaris", "VolarisConnectorClient", 25.0),
    ("airarabia_direct", "connectors.airarabia", "AirArabiaConnectorClient", 25.0),
    ("vietjet_direct", "connectors.vietjet", "VietJetConnectorClient", 25.0),
    ("cebupacific_direct", "connectors.cebupacific", "CebuPacificConnectorClient", 25.0),
    ("scoot_direct", "connectors.scoot", "ScootConnectorClient", 25.0),
    ("jetsmart_direct", "connectors.jetsmart", "JetSmartConnectorClient", 25.0),
    ("jetstar_direct", "connectors.jetstar", "JetstarConnectorClient", 25.0),
    ("jet2_direct", "connectors.jet2", "Jet2ConnectorClient", 25.0),
    ("flynas_direct", "connectors.flynas", "FlynasConnectorClient", 25.0),
    ("gol_direct", "connectors.gol", "GolConnectorClient", 25.0),
    ("azul_direct", "connectors.azul", "AzulConnectorClient", 25.0),
    ("flysafair_direct", "connectors.flysafair", "FlySafairConnectorClient", 25.0),
    ("vivaaerobus_direct", "connectors.vivaaerobus", "VivaAerobusConnectorClient", 25.0),
    ("allegiant_direct", "connectors.allegiant", "AllegiantConnectorClient", 25.0),
    ("jetblue_direct", "connectors.jetblue", "JetBlueConnectorClient", 25.0),
    ("flair_direct", "connectors.flair", "FlairConnectorClient", 25.0),
    ("spicejet_direct", "connectors.spicejet", "SpiceJetConnectorClient", 25.0),
    ("akasa_direct", "connectors.akasa", "AkasaConnectorClient", 25.0),
    ("spring_direct", "connectors.spring", "SpringConnectorClient", 25.0),
    ("peach_direct", "connectors.peach", "PeachConnectorClient", 25.0),
    ("zipair_direct", "connectors.zipair", "ZipairConnectorClient", 25.0),
    ("condor_direct", "connectors.condor", "CondorConnectorClient", 25.0),
    # play — blocked: airline defunct, flyplay.com DNS offline
    ("sunexpress_direct", "connectors.sunexpress", "SunExpressConnectorClient", 25.0),
    ("volotea_direct", "connectors.volotea", "VoloteaConnectorClient", 25.0),
    ("smartwings_direct", "connectors.smartwings", "SmartwingsConnectorClient", 25.0),
    ("flybondi_direct", "connectors.flybondi", "FlybondiConnectorClient", 25.0),
    ("jejuair_direct", "connectors.jejuair", "JejuAirConnectorClient", 25.0),
    ("twayair_direct", "connectors.twayair", "TwayAirConnectorClient", 25.0),
    ("porter_direct", "connectors.porter", "PorterConnectorClient", 25.0),
    ("nokair_direct", "connectors.nokair", "NokAirConnectorClient", 25.0),
    ("airpeace_direct", "connectors.airpeace", "AirPeaceConnectorClient", 25.0),
    ("airindiaexpress_direct", "connectors.airindiaexpress", "AirIndiaExpressConnectorClient", 25.0),
    ("batikair_direct", "connectors.batikair", "BatikAirConnectorClient", 25.0),
    ("luckyair_direct", "connectors.luckyair", "LuckyAirConnectorClient", 30.0),
    ("9air_direct", "connectors.nineair", "NineAirConnectorClient", 30.0),
    ("avelo_direct", "connectors.avelo", "AveloConnectorClient", 45.0),
    ("breeze_direct", "connectors.breeze", "BreezeConnectorClient", 45.0),
    ("salamair_direct", "connectors.salamair", "SalamAirConnectorClient", 20.0),
    ("usbangla_direct", "connectors.usbangla", "USBanglaConnectorClient", 45.0),
    ("biman_direct", "connectors.biman", "BimanConnectorClient", 25.0),
    ("etihad_direct", "connectors.etihad", "EtihadConnectorClient", 35.0),
    ("turkish_direct", "connectors.turkish", "TurkishConnectorClient", 45.0),
    ("emirates_direct", "connectors.emirates", "EmiratesConnectorClient", 60.0),
    ("malaysia_direct", "connectors.malaysia", "MalaysiaConnectorClient", 25.0),
    ("suncountry_direct", "connectors.suncountry", "SunCountryConnectorClient", 45.0),
    ("alaska_direct", "connectors.alaska", "AlaskaConnectorClient", 45.0),
    ("hawaiian_direct", "connectors.hawaiian", "HawaiianConnectorClient", 45.0),
    ("american_direct", "connectors.american", "AmericanConnectorClient", 45.0),
    ("united_direct", "connectors.united", "UnitedConnectorClient", 55.0),
    ("delta_direct", "connectors.delta", "DeltaConnectorClient", 45.0),
    ("cathay_direct", "connectors.cathay", "CathayConnectorClient", 25.0),
    ("singapore_direct", "connectors.singapore", "SingaporeConnectorClient", 60.0),
    ("thai_direct", "connectors.thai", "ThaiConnectorClient", 25.0),
    ("korean_direct", "connectors.korean", "KoreanConnectorClient", 45.0),
    ("nh_direct", "connectors.nh", "ANAConnectorClient", 60.0),
    ("qatar_direct", "connectors.qatar", "QatarConnectorClient", 55.0),
    ("westjet_direct", "connectors.westjet", "WestjetConnectorClient", 45.0),
    ("latam_direct", "connectors.latam", "LatamConnectorClient", 50.0),
    ("copa_direct", "connectors.copa", "CopaConnectorClient", 45.0),
    ("avianca_direct", "connectors.avianca", "AviancaConnectorClient", 45.0),
    ("lot_direct", "connectors.lot", "LotConnectorClient", 45.0),
]

# Build the registry dynamically — skip connectors whose deps are missing.
_DIRECT_AIRLINE_connectorS: list[tuple[str, type, float]] = []
_skipped: list[str] = []
for _name, _mod, _cls, _timeout in _CONNECTOR_DEFS:
    _klass = _safe_import(_mod, _cls)
    if _klass is not None:
        _DIRECT_AIRLINE_connectorS.append((_name, _klass, _timeout))
    else:
        _skipped.append(_name)
if _skipped:
    logger.info("Skipped %d connectors (missing deps): %s", len(_skipped), ", ".join(_skipped))




# Connectors that launch Chrome/Playwright browsers.
# These are throttled by a semaphore to prevent 20+ Chrome processes at once.
_BROWSER_SOURCES: set[str] = {
    "airasia_direct", "allegiant_direct", "azul_direct", "batikair_direct",
    "cebupacific_direct", "condor_direct", "easyjet_direct", "eurowings_direct",
    "flybondi_direct", "flydubai_direct", "flynas_direct", "frontier_direct",
    "gol_direct", "indigo_direct", "jet2_direct", "jetsmart_direct",
    "jetstar_direct", "luckyair_direct", "9air_direct",
    "jetblue_direct", "avelo_direct", "breeze_direct",
    "norwegian_direct", "peach_direct", "pegasus_direct",
    "porter_direct", "scoot_direct", "smartwings_direct", "southwest_direct",
    "sunexpress_direct", "transavia_direct", "twayair_direct",
    "vietjet_direct", "volaris_direct", "volotea_direct", "vueling_direct",
    "usbangla_direct",
    "etihad_direct",
    "turkish_direct",
    "emirates_direct",
    "zipair_direct",
    "suncountry_direct",
    "alaska_direct",
    "hawaiian_direct",
    "american_direct",
    "united_direct",
    "delta_direct",
    "cathay_direct",
    "singapore_direct",
    "korean_direct",
    "nh_direct",
    "qatar_direct",
    "westjet_direct",
    "latam_direct",
    "copa_direct",
    "avianca_direct",
    "lot_direct",
}



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
        Search flights across ALL sources in parallel.

        Multi-airport city expansion: if origin or destination is in a
        multi-airport city (e.g. STN → London), we also search sibling
        airports (LHR, LGW, LTN, SEN) using the SAME connector instances
        (Kiwi, Ryanair, Wizzair) — no extra browsers launched.  Full
        browser-based connectors only fire for the originally-requested
        airport pair.
        """
        try:
            origin_airports = get_city_airports(req.origin)
            dest_airports = get_city_airports(req.destination)

            # Build sibling airport pairs (excluding the original)
            sibling_pairs: list[tuple[str, str]] = []
            seen = {(req.origin.upper(), req.destination.upper())}
            for o in origin_airports:
                for d in dest_airports:
                    if o == d:
                        continue
                    key = (o, d)
                    if key not in seen:
                        seen.add(key)
                        sibling_pairs.append(key)

            if sibling_pairs:
                logger.info("City expansion: %s->%s + %d sibling pairs: %s",
                            req.origin, req.destination, len(sibling_pairs),
                            ", ".join(f"{o}->{d}" for o, d in sibling_pairs))

            return await self._search_flights_inner(req, sibling_pairs=sibling_pairs)
        finally:
            await self._cleanup_connectors()

    async def _search_flights_inner(
        self, req: FlightSearchRequest, *, sibling_pairs: list[tuple[str, str]] | None = None,
    ) -> FlightSearchResponse:
        import time as _time
        _t_start = _time.monotonic()
        sibling_pairs = sibling_pairs or []
        tasks = []
        providers_used = []

        # Build the full list of pairs this search covers
        all_pairs = [(req.origin, req.destination)] + list(sibling_pairs)

        # ── Cloud Run backend (paid API providers: Duffel, Amadeus, Sabre, etc.) ──
        if self.backend_available:
            tasks.append(self._search_backend(req))
            providers_used.append("backend")
        else:
            logger.warning("No BOOSTEDTRAVEL_API_KEY — skipping Cloud Run backend (paid APIs)")

        # ── Fast connectors: Ryanair, Wizzair, Kiwi ──────────────────────
        # Created ONCE and reused for ALL airport pairs (original + siblings).
        # This avoids re-launching browsers / HTTP clients per sibling pair.
        origin_country = get_country(req.origin)
        dest_country = get_country(req.destination)

        ryanair_client = self._get_ryanair_connector()
        wizzair_client = self._get_wizzair_connector()
        kiwi_client = self._get_kiwi_connector()

        # Track fast clients so we can close them once at the end
        _fast_clients: list = []

        ryanair_countries = AIRLINE_COUNTRIES.get("ryanair")
        ryanair_ok = ryanair_client and (
            not origin_country or not dest_country or not ryanair_countries
            or origin_country in ryanair_countries or dest_country in ryanair_countries
        )
        if ryanair_ok:
            _fast_clients.append(ryanair_client)
            for o, d in all_pairs:
                sub = req.model_copy(update={"origin": o, "destination": d}) if (o, d) != (req.origin, req.destination) else req
                tasks.append(self._search_fast_one(ryanair_client, sub, "ryanair_direct"))
                providers_used.append("ryanair_direct")

        wizz_countries = AIRLINE_COUNTRIES.get("wizz")
        wizzair_ok = wizzair_client and (
            not origin_country or not dest_country or not wizz_countries
            or origin_country in wizz_countries or dest_country in wizz_countries
        )
        if wizzair_ok:
            _fast_clients.append(wizzair_client)
            for o, d in all_pairs:
                sub = req.model_copy(update={"origin": o, "destination": d}) if (o, d) != (req.origin, req.destination) else req
                tasks.append(self._search_fast_one(wizzair_client, sub, "wizzair_direct"))
                providers_used.append("wizzair_direct")

        # Kiwi is a global aggregator — always query all pairs
        if kiwi_client:
            _fast_clients.append(kiwi_client)
            for o, d in all_pairs:
                sub = req.model_copy(update={"origin": o, "destination": d}) if (o, d) != (req.origin, req.destination) else req
                tasks.append(self._search_fast_one(kiwi_client, sub, "kiwi_connector"))
                providers_used.append("kiwi_connector")

        # ── Direct airline website connectors (46 LCCs) — route-filtered ──
        filtered_connectors = get_relevant_connectors(req.origin, req.destination, _DIRECT_AIRLINE_connectorS)
        skipped = len(_DIRECT_AIRLINE_connectorS) - len(filtered_connectors)
        if skipped:
            logger.info("Route filter: %s->%s -- skipped %d/%d irrelevant connectors",
                        req.origin, req.destination, skipped, len(_DIRECT_AIRLINE_connectorS))
        for source, connector_cls, timeout in filtered_connectors:
            connector = connector_cls(timeout=timeout)
            tasks.append(self._search_connector_generic(
                connector, req, source, timeout, sibling_pairs=sibling_pairs,
            ))
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
            # Reuse already-created fast clients — no extra instances needed.
            # SKIP Wizzair — its round-trip search already returns separate outbound/return
            # flights.  We extract those as one-way legs below to avoid extra API calls
            # that trigger rate limiting.
            for label, client in [
                ("ryanair_direct", ryanair_client if ryanair_ok else None),
                ("kiwi_connector", kiwi_client),
            ]:
                if client:
                    combo_tasks.append(self._search_fast_one(client, outbound_req, label))
                    combo_labels.append(f"{label}_out")
                    combo_tasks.append(self._search_fast_one(client, return_req, label))
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
        logger.info("Launching %d provider tasks (%d normal + %d combo) for %s->%s",
                     len(all_tasks), len(tasks), len(combo_tasks), req.origin, req.destination)
        results = await asyncio.gather(*all_tasks, return_exceptions=True)
        _gather_elapsed = _time.monotonic() - _t_start
        _n_ok = sum(1 for r in results if isinstance(r, FlightSearchResponse))
        _n_err = sum(1 for r in results if isinstance(r, Exception))
        logger.info("All %d tasks done in %.1fs — %d succeeded, %d failed",
                     len(results), _gather_elapsed, _n_ok, _n_err)

        # Close fast clients that were reused across pairs
        for client in _fast_clients:
            try:
                await client.close()
            except Exception:
                pass

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
                logger.warning("Provider %s failed: %s", provider, result)
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

    async def _search_fast_one(
        self, client, req: FlightSearchRequest, source: str,
    ) -> FlightSearchResponse:
        """Search a fast connector for ONE airport pair.  Does NOT close the
        client — the caller reuses the same instance across multiple pairs
        and closes it once at the end."""
        result = await client.search_flights(req)
        for offer in result.offers:
            offer.source = source
            offer.source_tier = "free"
        return result

    async def _search_connector_generic(
        self, client, req: FlightSearchRequest, source: str, timeout: float = 30.0,
        *, sibling_pairs: list[tuple[str, str]] | None = None,
    ) -> FlightSearchResponse:
        """Generic wrapper for direct airline connectors — tags source/tier, ensures cleanup.

        Browser-based connectors are throttled by a semaphore so at most 8
        Chrome processes run simultaneously (prevents resource exhaustion).
        An outer asyncio.wait_for enforces a hard deadline (timeout + 5s
        grace), so a hung connector never stalls the entire search.

        When *sibling_pairs* is provided AND the primary search returns
        results, the same browser instance is reused to sequentially search
        each sibling pair — no extra Chrome launches.  If the primary
        search returns 0 offers, siblings are skipped (the airline likely
        doesn't serve this origin/destination).
        """
        import time as _time
        t0 = _time.monotonic()
        uses_browser = source in _BROWSER_SOURCES
        if uses_browser:
            from connectors.browser import acquire_browser_slot
            await acquire_browser_slot()
        try:
            result = await asyncio.wait_for(
                client.search_flights(req),
                timeout=timeout + 5.0,   # hard deadline = connector timeout + 5s grace
            )
            for offer in result.offers:
                offer.source = source
                offer.source_tier = "free"
            all_offers = list(result.offers)

            # Only search siblings when the primary pair returned results —
            # avoids burning 30s × N sequential browser navigations on
            # connectors that don't serve this route at all.
            if sibling_pairs and all_offers:
                for sib_o, sib_d in sibling_pairs:
                    sub_req = req.model_copy(update={"origin": sib_o, "destination": sib_d})
                    try:
                        sub_result = await asyncio.wait_for(
                            client.search_flights(sub_req),
                            timeout=timeout + 5.0,
                        )
                        for offer in sub_result.offers:
                            offer.source = source
                            offer.source_tier = "free"
                        all_offers.extend(sub_result.offers)
                        logger.info("%s sibling %s->%s: %d offers", source, sib_o, sib_d, len(sub_result.offers))
                    except Exception as exc:
                        logger.debug("%s sibling %s->%s failed: %s", source, sib_o, sib_d, exc)

            elapsed = _time.monotonic() - t0
            logger.info("%s: %d offers in %.1fs", source, len(all_offers), elapsed)
            result.offers = all_offers
            result.total_results = len(all_offers)
            return result
        except asyncio.TimeoutError:
            elapsed = _time.monotonic() - t0
            logger.warning("%s: hard timeout after %.1fs", source, elapsed)
            raise
        finally:
            await client.close()
            if uses_browser:
                # Close module-level browser globals immediately so Chrome
                # doesn't linger until the full search completes.
                await self._cleanup_single_connector(client)
                from connectors.browser import release_browser_slot
                release_browser_slot()

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
