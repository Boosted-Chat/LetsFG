"""
Virtual interlining combo engine — cross-airline one-way combinations.

For round-trip searches, splits into one-way outbound + one-way return
across ALL direct scrapers, then builds cross-airline combos.

This replicates what Kiwi.com does (Ryanair out + Wizzair back)
but uses our own direct API connections for the freshest prices.
"""

from __future__ import annotations

import hashlib
import logging

from models.flights import (
    FlightOffer,
    FlightRoute,
)

logger = logging.getLogger(__name__)

# Max one-way legs to keep per direction per source (prevents combinatorial explosion)
_MAX_LEGS_PER_SOURCE = 10
# Max total combos to generate
_MAX_COMBOS = 150


def _leg_key(route: FlightRoute) -> str:
    """Unique key for a leg based on flights and timing."""
    if not route or not route.segments:
        return ""
    parts = []
    for seg in route.segments:
        parts.append(f"{seg.flight_no}_{seg.departure.isoformat()}")
    return "|".join(parts)


def build_combos(
    outbound_offers: list[FlightOffer],
    return_offers: list[FlightOffer],
    target_currency: str,
) -> list[FlightOffer]:
    """
    Build cross-airline combo offers from one-way outbound + one-way return legs.

    Each input offer should be a one-way offer (has outbound, no inbound).
    Returns new FlightOffer objects combining outbound from one + return from another,
    skipping same-source pairs (those are already built by the provider's own round-trip logic).
    """
    if not outbound_offers or not return_offers:
        return []

    # Deduplicate legs per direction PER SOURCE — keeps each source's version
    # of the same flight so cross-source combos always have both sides.
    out_by_source: dict[str, dict[str, FlightOffer]] = {}
    for o in outbound_offers:
        if not o.outbound:
            continue
        key = _leg_key(o.outbound)
        if not key:
            continue
        src = o.source
        if src not in out_by_source:
            out_by_source[src] = {}
        if key not in out_by_source[src] or o.price < out_by_source[src][key].price:
            out_by_source[src][key] = o

    ret_by_source: dict[str, dict[str, FlightOffer]] = {}
    for r in return_offers:
        if not r.outbound:
            continue
        key = _leg_key(r.outbound)
        if not key:
            continue
        src = r.source
        if src not in ret_by_source:
            ret_by_source[src] = {}
        if key not in ret_by_source[src] or r.price < ret_by_source[src][key].price:
            ret_by_source[src][key] = r

    # Keep top N per source (sorted by price_normalized, fallback to price)
    def _sort_price(o: FlightOffer) -> float:
        return o.price_normalized if o.price_normalized is not None else o.price

    out_trimmed: dict[str, list[FlightOffer]] = {}
    for src, by_key in out_by_source.items():
        out_trimmed[src] = sorted(by_key.values(), key=_sort_price)[:_MAX_LEGS_PER_SOURCE]

    ret_trimmed: dict[str, list[FlightOffer]] = {}
    for src, by_key in ret_by_source.items():
        ret_trimmed[src] = sorted(by_key.values(), key=_sort_price)[:_MAX_LEGS_PER_SOURCE]

    combos: list[FlightOffer] = []
    seen_combo_keys: set[str] = set()

    # Build cross-source combos: source_a outbound × source_b return (a ≠ b)
    out_sources = list(out_trimmed.keys())
    ret_sources = list(ret_trimmed.keys())

    # Collect all cross-source pairs, sorted by cheapest potential combo
    cross_pairs: list[tuple[FlightOffer, FlightOffer]] = []
    for src_a in out_sources:
        for src_b in ret_sources:
            if src_a == src_b:
                continue
            for ob in out_trimmed[src_a]:
                for rt in ret_trimmed[src_b]:
                    cross_pairs.append((ob, rt))

    # Sort by estimated total price (normalized) for best-first generation
    cross_pairs.sort(key=lambda pair: _sort_price(pair[0]) + _sort_price(pair[1]))

    for ob, rt in cross_pairs:
        if len(combos) >= _MAX_COMBOS:
            break

        # Dedup by flight identity (regardless of source)
        ob_key = _leg_key(ob.outbound)
        rt_key = _leg_key(rt.outbound)
        combo_key = f"{ob_key}::{rt_key}"
        if combo_key in seen_combo_keys:
            continue
        seen_combo_keys.add(combo_key)

        # Need compatible currencies for price sum
        # Use price_normalized if available, otherwise raw price
        if ob.price_normalized is not None and rt.price_normalized is not None:
            total_normalized = ob.price_normalized + rt.price_normalized
        else:
            total_normalized = None

        total_price = ob.price + rt.price  # Raw sum (may be mixed currency)

        # Determine combo currency: if same, use it; if different, use target
        if ob.currency == rt.currency:
            combo_currency = ob.currency
            combo_price = total_price
        else:
            # Mixed currencies — use normalized prices in target currency
            combo_currency = target_currency
            combo_price = total_normalized if total_normalized else total_price

        # Collect airlines from both legs
        ob_airlines = set(ob.airlines) if ob.airlines else set()
        rt_airlines = set(rt.airlines) if rt.airlines else set()
        all_airlines = sorted(ob_airlines | rt_airlines)

        ob_id = ob.id[:8]
        rt_id = rt.id[:8]
        combo_hash = hashlib.md5(f"{ob_id}{rt_id}".encode()).hexdigest()[:12]

        # Return leg is an outbound-only offer — its "outbound" route becomes our "inbound"
        combo = FlightOffer(
            id=f"combo_{combo_hash}",
            price=round(combo_price, 2),
            currency=combo_currency,
            price_formatted=f"{combo_price:.2f} {combo_currency}",
            price_normalized=total_normalized,
            outbound=ob.outbound,
            inbound=rt.outbound,  # return leg's outbound becomes our inbound
            airlines=all_airlines,
            owner_airline="|".join(all_airlines),
            booking_url="",
            is_locked=False,
            source=f"combo:{ob.source}+{rt.source}",
            source_tier="free",
        )
        combos.append(combo)

    logger.info(
        "Combo engine: %d combos from %d sources out × %d sources ret",
        len(combos), len(out_sources), len(ret_sources),
    )
    return combos
