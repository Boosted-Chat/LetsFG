"""
LetsFG CLI — Agent-native flight search & booking from terminal.

Usage (local — free, no API key):
    letsfg search-local GDN BER 2026-03-03
    letsfg search-local LON BCN 2026-04-01 --return 2026-04-08 --sort price

Usage (API — requires key):
    letsfg search GDN BER 2026-03-03
    letsfg unlock off_xxx
    letsfg book off_xxx --passenger '{"id":"pas_xxx","given_name":"John",...}'
    letsfg register --name my-agent --email agent@example.com
    letsfg me
    letsfg locations London
"""

from __future__ import annotations

import json
import os
import sys
from typing import Optional

try:
    import typer
    from rich.console import Console
    from rich.table import Table
    from rich import print as rprint
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

from letsfg.client import LetsFG, LetsFGError

app = typer.Typer(
    name="letsfg",
    help=(
        "LetsFG — Agent-native flight search & booking.\n\n"
        "Search 400+ airlines at raw airline prices — $20-50 cheaper than OTAs.\n"
        "Search is free. Booking charges the ticket price only (zero markup).\n\n"
        "Quick start: letsfg register → letsfg star --github <username> → letsfg search\n"
        "Local search (no API key): letsfg search-local GDN BCN 2026-06-15\n"
        "Full search (API key):     letsfg search GDN BCN 2026-06-15"
    ),
    no_args_is_help=True,
)
console = Console() if HAS_RICH else None


def _get_client(api_key: str | None = None, base_url: str | None = None) -> LetsFG:
    key = api_key or os.environ.get("LETSFG_API_KEY", "")
    url = base_url or os.environ.get("LETSFG_BASE_URL")
    if not key:
        _err("API key required. Set LETSFG_API_KEY or use --api-key flag.\n"
             "Register: letsfg register --name my-agent --email you@example.com")
    return LetsFG(api_key=key, base_url=url)


def _err(msg: str):
    if HAS_RICH:
        console.print(f"[red]Error:[/red] {msg}")
    else:
        print(f"Error: {msg}", file=sys.stderr)
    raise typer.Exit(1)


def _json_out(data):
    """Print JSON output for machine consumption."""
    print(json.dumps(data, indent=2, default=str))


# ── Search ────────────────────────────────────────────────────────────────

@app.command()
def search(
    origin: str = typer.Argument(..., help="Departure IATA code (e.g., GDN, LON, JFK)"),
    destination: str = typer.Argument(..., help="Arrival IATA code (e.g., BER, BCN, LAX)"),
    date: str = typer.Argument(..., help="Departure date YYYY-MM-DD"),
    return_date: Optional[str] = typer.Option(None, "--return", "-r", help="Return date for round-trip"),
    adults: int = typer.Option(1, "--adults", "-a", help="Number of adults"),
    children: int = typer.Option(0, "--children", help="Number of children"),
    cabin: Optional[str] = typer.Option(None, "--cabin", "-c", help="M=economy W=premium C=business F=first"),
    stops: int = typer.Option(2, "--max-stops", "-s", help="Max stopovers"),
    currency: str = typer.Option("EUR", "--currency", help="Currency code"),
    limit: int = typer.Option(20, "--limit", "-l", help="Max results"),
    sort: str = typer.Option("price", "--sort", help="Sort: price or duration"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Search for flights — FREE, unlimited."""
    bt = _get_client(api_key, base_url)
    try:
        result = bt.search(
            origin=origin,
            destination=destination,
            date_from=date,
            return_date=return_date,
            adults=adults,
            children=children,
            cabin_class=cabin,
            max_stopovers=stops,
            currency=currency,
            limit=limit,
            sort=sort,
        )
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        # Machine-readable output for agents
        _json_out({
            "passenger_ids": result.passenger_ids,
            "total_results": result.total_results,
            "offers": [
                {
                    "id": o.id,
                    "price": o.price,
                    "currency": o.currency,
                    "airlines": o.airlines,
                    "owner_airline": o.owner_airline,
                    "route": o.outbound.route_str,
                    "duration_seconds": o.outbound.total_duration_seconds,
                    "stopovers": o.outbound.stopovers,
                    "conditions": o.conditions,
                    "is_locked": o.is_locked,
                }
                for o in result.offers
            ],
        })
        return

    # Human-readable output
    if not result.offers:
        print(f"No flights found for {origin} → {destination} on {date}")
        return

    print(f"\n  {result.total_results} offers  |  {origin} → {destination}  |  {date}")
    print(f"  Passenger IDs: {result.passenger_ids}\n")

    if HAS_RICH:
        table = Table(show_header=True, header_style="bold")
        table.add_column("#", style="dim", width=4)
        table.add_column("Price", justify="right", style="green")
        table.add_column("Airline")
        table.add_column("Route")
        table.add_column("Duration", justify="right")
        table.add_column("Stops", justify="center")
        table.add_column("Conditions")
        table.add_column("Offer ID", style="dim")

        for i, o in enumerate(result.offers, 1):
            refund = o.conditions.get("refund_before_departure", "?")
            change = o.conditions.get("change_before_departure", "?")
            conds = f"R:{refund[:3]} C:{change[:3]}"
            table.add_row(
                str(i),
                f"{o.currency} {o.price:.2f}",
                o.owner_airline or ",".join(o.airlines),
                o.outbound.route_str,
                o.outbound.duration_human,
                str(o.outbound.stopovers),
                conds,
                o.id[:20] + "...",
            )
        console.print(table)
    else:
        for i, o in enumerate(result.offers, 1):
            print(f"  {i:3d}. {o.summary()}")
            print(f"       ID: {o.id}")

    print(f"\n  To unlock: letsfg unlock <offer_id>")
    print(f"  Passenger IDs needed for booking: {result.passenger_ids}\n")


# ── Search Local ───────────────────────────────────────────────────────────

@app.command("search-local")
def search_local_cmd(
    origin: str = typer.Argument(..., help="Departure IATA code (e.g., GDN, LON, JFK)"),
    destination: str = typer.Argument(..., help="Arrival IATA code (e.g., BER, BCN, LAX)"),
    date: str = typer.Argument(..., help="Departure date YYYY-MM-DD"),
    return_date: Optional[str] = typer.Option(None, "--return", "-r", help="Return date for round-trip"),
    adults: int = typer.Option(1, "--adults", "-a", help="Number of adults"),
    children: int = typer.Option(0, "--children", help="Number of children"),
    cabin: Optional[str] = typer.Option(None, "--cabin", "-c", help="M=economy W=premium C=business F=first"),
    currency: str = typer.Option("EUR", "--currency", help="Currency code"),
    limit: int = typer.Option(20, "--limit", "-l", help="Max results"),
    sort: str = typer.Option("price", "--sort", help="Sort: price or duration"),
    max_browsers: Optional[int] = typer.Option(None, "--max-browsers", "-b", help="Max concurrent browsers (1-32, default: auto-detect from RAM)"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
):
    """Search flights locally — FREE, no API key required. Runs 75 airline connectors on your machine.

    Set LETSFG_API_KEY to also query Amadeus, Duffel, Sabre and Travelport for full-service airline fares.

    Use --max-browsers to tune performance: lower values (2-4) for low-RAM machines, higher (12-16) for powerful ones.
    Default: auto-detected from your system RAM. Run 'letsfg system-info' to see your profile.
    """
    import asyncio
    import logging
    import os
    import sys
    import warnings
    from letsfg.local import search_local

    # Only show errors in CLI mode — suppress connector warning noise
    logging.basicConfig(level=logging.ERROR, stream=sys.stderr, format="%(message)s")

    # Suppress asyncio transport warnings from Playwright subprocess cleanup
    warnings.filterwarnings("ignore", category=ResourceWarning, message=".*unclosed transport.*")

    # Suppress asyncio __del__ "Exception ignored" noise on Python 3.13+
    _orig_unraisable = sys.unraisablehook
    def _quiet_unraisable(hook_args):
        try:
            if hook_args.exc_type is ValueError and "pipe" in str(hook_args.exc_value).lower():
                return
            if "transport" in str(getattr(hook_args, "object", "")):
                return
        except Exception:
            return  # Suppress errors in the hook itself during shutdown
        _orig_unraisable(hook_args)
    sys.unraisablehook = _quiet_unraisable

    # Suppress Node.js DEP0169 deprecation warnings from Playwright subprocesses
    os.environ.setdefault("NODE_OPTIONS", "--no-deprecation")

    async def _run():
        # Suppress "Future exception was never retrieved" from Playwright cleanup
        asyncio.get_event_loop().set_exception_handler(lambda loop, ctx: None)
        return await search_local(
            origin=origin,
            destination=destination,
            date_from=date,
            return_date=return_date,
            adults=adults,
            children=children,
            cabin_class=cabin,
            currency=currency,
            limit=limit,
            max_browsers=max_browsers,
        )

    try:
        result = asyncio.run(_run())
    except Exception as e:
        _err(f"Local search failed: {e}")

    offers = result.get("offers", [])
    total = result.get("total_results", len(offers))

    # Sort
    if sort == "price":
        offers.sort(key=lambda o: o.get("price", float("inf")))
    elif sort == "duration":
        offers.sort(key=lambda o: (o.get("outbound", {}).get("total_duration_seconds") or float("inf")))

    offers = offers[:limit]

    if output_json:
        _json_out({"total_results": total, "offers": offers})
        return

    if not offers:
        print(f"No flights found for {origin} → {destination} on {date}")
        return

    source_tiers = result.get("source_tiers", {})
    has_backend = "paid" in source_tiers
    mode_label = "LOCAL + BACKEND" if has_backend else "LOCAL only (set LETSFG_API_KEY for Amadeus/Duffel)"

    print(f"\n  {total} offers  |  {origin} → {destination}  |  {date}  |  {mode_label}")

    if HAS_RICH:
        table = Table(show_header=True, header_style="bold")
        table.add_column("#", style="dim", width=4)
        table.add_column("Price", justify="right", style="green")
        table.add_column("Airline")
        table.add_column("Route")
        table.add_column("Duration", justify="right")
        table.add_column("Stops", justify="center")

        for i, o in enumerate(offers, 1):
            ob = o.get("outbound", {})
            route = ob.get("route_str", "")
            if not route:
                segs = ob.get("segments", [])
                if segs:
                    codes = [segs[0].get("origin", "")]
                    for s in segs:
                        codes.append(s.get("destination", ""))
                    route = "→".join(c for c in codes if c)
            airlines = o.get("owner_airline") or ",".join(o.get("airlines", []))
            dur_s = ob.get("total_duration_seconds")
            if dur_s:
                h, m = divmod(dur_s // 60, 60)
                dur = f"{h}h {m:02d}m"
            else:
                dur = "-"
            stops = str(ob.get("stopovers", 0))
            price = o.get("price", 0)
            cur = o.get("currency", currency)
            table.add_row(str(i), f"{cur} {price:.2f}", airlines, route, dur, stops)
        console.print(table)
    else:
        for i, o in enumerate(offers, 1):
            price = o.get("price", 0)
            cur = o.get("currency", currency)
            airlines = o.get("owner_airline") or ",".join(o.get("airlines", []))
            ob = o.get("outbound", {})
            route = ob.get("route_str", "")
            if not route:
                segs = ob.get("segments", [])
                if segs:
                    codes = [segs[0].get("origin", "")]
                    for s in segs:
                        codes.append(s.get("destination", ""))
                    route = "→".join(c for c in codes if c)
            print(f"  {i:3d}. {cur} {price:.2f}  {airlines}  {route}")

    print()


# ── Star (Link GitHub) ─────────────────────────────────────────────────────

@app.command()
def star(
    github: str = typer.Option(..., "--github", "-g", help="Your GitHub username"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Link your GitHub account — star the repo for FREE unlimited access.

    1. Star https://github.com/LetsFG/LetsFG
    2. Run: letsfg star --github <your-username>
    3. Done — unlimited search, unlock, and book forever.
    """
    bt = _get_client(api_key, base_url)
    try:
        result = bt.link_github(github)
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out(result)
        return

    status = result.get("status", "unknown")
    if status == "verified":
        print(f"\n  ✓ GitHub star verified! Unlimited access granted.")
        print(f"    Username: {result.get('github_username')}")
        print(f"\n    You're all set — search, unlock, and book for free.\n")
    elif status == "already_verified":
        print(f"\n  ✓ Already verified! You have unlimited access.")
        print(f"    Username: {result.get('github_username')}\n")
    elif status == "star_required":
        print(f"\n  ✗ Star not found for '{github}'.")
        print(f"    1. Star the repo: https://github.com/LetsFG/LetsFG")
        print(f"    2. Run this command again.\n")
    else:
        _err(f"Unexpected status: {status}")


# ── Unlock ────────────────────────────────────────────────────────────────

@app.command()
def unlock(
    offer_id: str = typer.Argument(..., help="Offer ID from search results"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Unlock a flight offer — FREE with GitHub star. Confirms price, reserves 30min."""
    bt = _get_client(api_key, base_url)
    try:
        result = bt.unlock(offer_id)
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out({
            "offer_id": result.offer_id,
            "unlock_status": result.unlock_status,
            "confirmed_price": result.confirmed_price,
            "confirmed_currency": result.confirmed_currency,
            "offer_expires_at": result.offer_expires_at,
            "payment_charged": result.payment_charged,
            "payment_amount_cents": result.payment_amount_cents,
        })
        return

    if result.is_unlocked:
        print(f"\n  ✓ Offer unlocked!")
        print(f"    Confirmed price: {result.confirmed_currency} {result.confirmed_price:.2f}")
        print(f"    Expires at: {result.offer_expires_at}")
        print(f"\n    Next: letsfg book {offer_id} --passenger '{{...}}' --email you@example.com\n")
    else:
        _err(f"Unlock failed: {result.message}")


# ── Book ──────────────────────────────────────────────────────────────────

@app.command()
def book(
    offer_id: str = typer.Argument(..., help="Offer ID (must be unlocked first)"),
    passenger: list[str] = typer.Option(..., "--passenger", "-p", help='JSON passenger object: \'{"id":"pas_xxx","given_name":"John","family_name":"Doe","born_on":"1990-01-15","gender":"m","title":"mr"}\''),
    email: str = typer.Option(..., "--email", "-e", help="Contact email"),
    phone: str = typer.Option("", "--phone", help="Contact phone"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Book a flight — charges ticket price via Stripe. Creates real airline reservation."""
    bt = _get_client(api_key, base_url)

    passengers = []
    for p_str in passenger:
        try:
            passengers.append(json.loads(p_str))
        except json.JSONDecodeError:
            _err(f"Invalid JSON for passenger: {p_str}")

    try:
        result = bt.book(
            offer_id=offer_id,
            passengers=passengers,
            contact_email=email,
            contact_phone=phone,
        )
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out({
            "booking_id": result.booking_id,
            "status": result.status,
            "booking_reference": result.booking_reference,
            "flight_price": result.flight_price,
            "service_fee": result.service_fee,
            "total_charged": result.total_charged,
            "currency": result.currency,
            "order_id": result.order_id,
        })
        return

    if result.is_confirmed:
        print(f"\n  ✓ Booking confirmed!")
        print(f"    PNR: {result.booking_reference}")
        print(f"    Flight price: {result.currency} {result.flight_price:.2f}")
        print(f"    Service fee: {result.currency} {result.service_fee:.2f} ({result.service_fee_percentage}%)")
        print(f"    Total: {result.currency} {result.total_charged:.2f}")
        print(f"    Order ID: {result.order_id}\n")
    else:
        _err(f"Booking failed: {result.details}")


# ── Locations ─────────────────────────────────────────────────────────────

@app.command()
def locations(
    query: str = typer.Argument(..., help="City or airport name to resolve"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Resolve city/airport name to IATA codes."""
    bt = _get_client(api_key, base_url)
    try:
        result = bt.resolve_location(query)
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out(result)
        return

    if not result:
        print(f"No locations found for '{query}'")
        return

    for loc in result:
        iata = loc.get("iata_code", loc.get("iata", "???"))
        name = loc.get("name", "")
        ltype = loc.get("type", "")
        city = loc.get("city_name", loc.get("city", ""))
        country = loc.get("country", "")
        print(f"  {iata:5s}  {name} ({ltype}) — {city}, {country}")


# ── Register ──────────────────────────────────────────────────────────────

@app.command()
def register(
    name: str = typer.Option(..., "--name", "-n", help="Agent name"),
    email: str = typer.Option(..., "--email", "-e", help="Contact email"),
    owner: str = typer.Option("", "--owner", help="Owner name"),
    description: str = typer.Option("", "--desc", help="Agent description"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Register a new agent — get your API key."""
    try:
        result = LetsFG.register(
            agent_name=name,
            email=email,
            base_url=base_url,
            owner_name=owner,
            description=description,
        )
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out(result)
        return

    print(f"\n  ✓ Agent registered!")
    print(f"    Agent ID: {result.get('agent_id')}")
    print(f"    API Key:  {result.get('api_key')}")
    print(f"\n    Save your API key:")
    print(f"    export LETSFG_API_KEY={result.get('api_key')}")
    print(f"\n    Next: Star the repo and link your GitHub:")
    print(f"    1. Star https://github.com/LetsFG/LetsFG")
    print(f"    2. letsfg star --github <your-github-username>\n")


# ── Setup Payment ──────────────────────────────────────────────────────────

@app.command("setup-payment")
def setup_payment(
    token: str = typer.Option("tok_visa", "--token", "-t", help="Payment token"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Set up payment method (required before booking)."""
    bt = _get_client(api_key, base_url)
    try:
        result = bt.setup_payment(token=token)
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out(result)
        return

    status = result.get("status", "unknown")
    if status == "ready":
        print(f"\n  ✓ Payment ready!")
        print(f"    You can now unlock offers and book flights.\n")
    else:
        _err(f"Payment setup failed: {result.get('message', status)}")


# ── Profile ───────────────────────────────────────────────────────────────

@app.command()
def me(
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="LETSFG_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="LETSFG_BASE_URL"),
):
    """Show your agent profile and usage stats."""
    bt = _get_client(api_key, base_url)
    try:
        profile = bt.me()
    except LetsFGError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out({
            "agent_id": profile.agent_id,
            "agent_name": profile.agent_name,
            "email": profile.email,
            "tier": profile.tier,
            "payment_ready": profile.payment_ready,
            "usage": profile.usage,
        })
        return

    print(f"\n  Agent: {profile.agent_name} ({profile.agent_id})")
    print(f"  Email: {profile.email}")
    print(f"  Tier:  {profile.tier}")
    gh = getattr(profile, 'github_username', '') or ''
    star_ok = getattr(profile, 'github_star_verified', False)
    if star_ok:
        print(f"  GitHub:  ✓ {gh} (star verified — unlimited access)")
    elif gh:
        print(f"  GitHub:  {gh} (star not verified)")
    else:
        print(f"  GitHub:  Not linked — run: letsfg star --github <username>")
    print(f"  Payment: {'✓ Ready' if profile.payment_ready else '—'}")
    u = profile.usage
    print(f"  Searches: {u.get('total_searches', 0)}")
    print(f"  Unlocks:  {u.get('total_unlocks', 0)}")
    print(f"  Bookings: {u.get('total_bookings', 0)}")
    print(f"  Total spent: ${u.get('total_spent_cents', 0) / 100:.2f}\n")


@app.command("system-info")
def system_info_cmd(
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
):
    """Show system resources and recommended concurrency settings.

    Agents can use this to pick optimal --max-browsers values.
    """
    from letsfg.system_info import get_system_profile
    from letsfg.connectors.browser import get_max_browsers

    profile = get_system_profile()
    current_max = get_max_browsers()

    if output_json:
        profile["current_max_browsers"] = current_max
        _json_out(profile)
        return

    ram_total = profile["ram_total_gb"]
    ram_avail = profile["ram_available_gb"]
    print(f"\n  Platform:     {profile['platform']}")
    print(f"  CPU cores:    {profile['cpu_cores']}")
    print(f"  RAM total:    {ram_total:.1f} GB" if ram_total else "  RAM total:    unknown")
    print(f"  RAM available: {ram_avail:.1f} GB" if ram_avail else "  RAM available: unknown")
    print(f"  Tier:         {profile['tier']}")
    print(f"  Recommended max browsers: {profile['recommended_max_browsers']}")
    print(f"  Current max browsers:     {current_max}")
    print(f"\n  Override with: letsfg search-local ... --max-browsers {current_max}")
    print(f"  Or set env:   LETSFG_MAX_BROWSERS={current_max}\n")


def main():
    app()


if __name__ == "__main__":
    main()
