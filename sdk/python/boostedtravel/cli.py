"""
BoostedTravel CLI — Agent-native flight search & booking from terminal.

Usage:
    boostedtravel search GDN BER 2026-03-03
    boostedtravel search LON BCN 2026-04-01 --return 2026-04-08 --sort price
    boostedtravel unlock off_xxx
    boostedtravel book off_xxx --passenger '{"id":"pas_xxx","given_name":"John",...}'
    boostedtravel register --name my-agent --email agent@example.com
    boostedtravel me
    boostedtravel locations London
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

from boostedtravel.client import BoostedTravel, BoostedTravelError

app = typer.Typer(
    name="boostedtravel",
    help=(
        "BoostedTravel — Agent-native flight search & booking.\n\n"
        "Search 400+ airlines at raw airline prices — $20-50 cheaper than OTAs.\n"
        "Search is FREE. Unlock: $1. Book: FREE after unlock.\n\n"
        "Set BOOSTEDTRAVEL_API_KEY or use --api-key."
    ),
    no_args_is_help=True,
)
console = Console() if HAS_RICH else None


def _get_client(api_key: str | None = None, base_url: str | None = None) -> BoostedTravel:
    key = api_key or os.environ.get("BOOSTEDTRAVEL_API_KEY", "")
    url = base_url or os.environ.get("BOOSTEDTRAVEL_BASE_URL")
    if not key:
        _err("API key required. Set BOOSTEDTRAVEL_API_KEY or use --api-key flag.\n"
             "Register: boostedtravel register --name my-agent --email you@example.com")
    return BoostedTravel(api_key=key, base_url=url)


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
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="BOOSTEDTRAVEL_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="BOOSTEDTRAVEL_BASE_URL"),
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
    except BoostedTravelError as e:
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

    print(f"\n  To unlock: boostedtravel unlock <offer_id>")
    print(f"  Passenger IDs needed for booking: {result.passenger_ids}\n")


# ── Unlock ────────────────────────────────────────────────────────────────

@app.command()
def unlock(
    offer_id: str = typer.Argument(..., help="Offer ID from search results"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="BOOSTEDTRAVEL_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="BOOSTEDTRAVEL_BASE_URL"),
):
    """Unlock a flight offer — $1 fee. Confirms price, reserves 30min."""
    bt = _get_client(api_key, base_url)
    try:
        result = bt.unlock(offer_id)
    except BoostedTravelError as e:
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
        print(f"    $1 unlock fee charged")
        print(f"\n    Next: boostedtravel book {offer_id} --passenger '{{...}}' --email you@example.com\n")
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
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="BOOSTEDTRAVEL_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="BOOSTEDTRAVEL_BASE_URL"),
):
    """Book a flight — FREE after unlock. Creates real airline reservation."""
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
    except BoostedTravelError as e:
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
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="BOOSTEDTRAVEL_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="BOOSTEDTRAVEL_BASE_URL"),
):
    """Resolve city/airport name to IATA codes."""
    bt = _get_client(api_key, base_url)
    try:
        result = bt.resolve_location(query)
    except BoostedTravelError as e:
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
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="BOOSTEDTRAVEL_BASE_URL"),
):
    """Register a new agent — get your API key."""
    try:
        result = BoostedTravel.register(
            agent_name=name,
            email=email,
            base_url=base_url,
            owner_name=owner,
            description=description,
        )
    except BoostedTravelError as e:
        _err(f"{e.message}")

    if output_json:
        _json_out(result)
        return

    print(f"\n  ✓ Agent registered!")
    print(f"    Agent ID: {result.get('agent_id')}")
    print(f"    API Key:  {result.get('api_key')}")
    print(f"\n    Save your API key:")
    print(f"    export BOOSTEDTRAVEL_API_KEY={result.get('api_key')}")
    print(f"\n    Next: boostedtravel setup-payment --token tok_visa\n")


# ── Setup Payment ──────────────────────────────────────────────────────────

@app.command("setup-payment")
def setup_payment(
    token: str = typer.Option("tok_visa", "--token", "-t", help="Payment token"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output raw JSON"),
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="BOOSTEDTRAVEL_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="BOOSTEDTRAVEL_BASE_URL"),
):
    """Set up payment method for booking."""
    bt = _get_client(api_key, base_url)
    try:
        result = bt.setup_payment(token=token)
    except BoostedTravelError as e:
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
    api_key: Optional[str] = typer.Option(None, "--api-key", "-k", envvar="BOOSTEDTRAVEL_API_KEY"),
    base_url: Optional[str] = typer.Option(None, "--base-url", envvar="BOOSTEDTRAVEL_BASE_URL"),
):
    """Show your agent profile and usage stats."""
    bt = _get_client(api_key, base_url)
    try:
        profile = bt.me()
    except BoostedTravelError as e:
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
    print(f"  Payment: {'✓ Ready' if profile.payment_ready else '✗ Not set up'}")
    u = profile.usage
    print(f"  Searches: {u.get('total_searches', 0)}")
    print(f"  Unlocks:  {u.get('total_unlocks', 0)}")
    print(f"  Bookings: {u.get('total_bookings', 0)}")
    print(f"  Total spent: ${u.get('total_spent_cents', 0) / 100:.2f}\n")


def main():
    app()


if __name__ == "__main__":
    main()
