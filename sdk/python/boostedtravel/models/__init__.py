"""Data models for BoostedTravel SDK — lightweight, no pydantic dependency."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class FlightSegment:
    """A single flight leg (e.g., GDN → MUC)."""
    airline: str
    airline_name: str
    flight_no: str
    origin: str
    destination: str
    origin_city: str
    destination_city: str
    departure: str
    arrival: str
    duration_seconds: int
    cabin_class: str
    aircraft: str

    @classmethod
    def from_dict(cls, d: dict) -> "FlightSegment":
        return cls(
            airline=d.get("airline", ""),
            airline_name=d.get("airline_name", ""),
            flight_no=d.get("flight_no", ""),
            origin=d.get("origin", ""),
            destination=d.get("destination", ""),
            origin_city=d.get("origin_city", ""),
            destination_city=d.get("destination_city", ""),
            departure=d.get("departure", ""),
            arrival=d.get("arrival", ""),
            duration_seconds=d.get("duration_seconds", 0),
            cabin_class=d.get("cabin_class", "economy"),
            aircraft=d.get("aircraft", ""),
        )


@dataclass
class FlightRoute:
    """One direction (outbound or return) composed of segments."""
    segments: list[FlightSegment] = field(default_factory=list)
    total_duration_seconds: int = 0
    stopovers: int = 0

    @classmethod
    def from_dict(cls, d: dict) -> "FlightRoute":
        return cls(
            segments=[FlightSegment.from_dict(s) for s in d.get("segments", [])],
            total_duration_seconds=d.get("total_duration_seconds", 0),
            stopovers=d.get("stopovers", 0),
        )

    @property
    def duration_human(self) -> str:
        h, m = divmod(self.total_duration_seconds // 60, 60)
        return f"{h}h{m:02d}m"

    @property
    def route_str(self) -> str:
        if not self.segments:
            return ""
        codes = [self.segments[0].origin]
        for seg in self.segments:
            codes.append(seg.destination)
        return " → ".join(codes)


@dataclass
class FlightOffer:
    """A single flight offer from BoostedTravel."""
    id: str
    price: float
    currency: str
    price_formatted: str
    outbound: FlightRoute
    inbound: Optional[FlightRoute]
    airlines: list[str]
    owner_airline: str
    bags_price: dict[str, float]
    availability_seats: Optional[int]
    conditions: dict[str, str]
    is_locked: bool
    fetched_at: str
    booking_url: str

    @classmethod
    def from_dict(cls, d: dict) -> "FlightOffer":
        inbound = FlightRoute.from_dict(d["inbound"]) if d.get("inbound") else None
        return cls(
            id=d.get("id", ""),
            price=d.get("price", 0.0),
            currency=d.get("currency", "EUR"),
            price_formatted=d.get("price_formatted", ""),
            outbound=FlightRoute.from_dict(d.get("outbound", {})),
            inbound=inbound,
            airlines=d.get("airlines", []),
            owner_airline=d.get("owner_airline", ""),
            bags_price=d.get("bags_price", {}),
            availability_seats=d.get("availability_seats"),
            conditions=d.get("conditions", {}),
            is_locked=d.get("is_locked", True),
            fetched_at=d.get("fetched_at", ""),
            booking_url=d.get("booking_url", ""),
        )

    def summary(self) -> str:
        route = self.outbound.route_str
        dur = self.outbound.duration_human
        stops = self.outbound.stopovers
        airline = self.owner_airline or (self.airlines[0] if self.airlines else "?")
        return f"{self.currency} {self.price:.2f} | {airline} | {route} | {dur} | {stops} stop(s)"


@dataclass
class FlightSearchResult:
    """Full search result from BoostedTravel."""
    search_id: str
    offer_request_id: str
    passenger_ids: list[str]
    origin: str
    destination: str
    currency: str
    offers: list[FlightOffer]
    total_results: int
    search_params: dict
    pricing_note: str

    @classmethod
    def from_dict(cls, d: dict) -> "FlightSearchResult":
        return cls(
            search_id=d.get("search_id", ""),
            offer_request_id=d.get("offer_request_id", ""),
            passenger_ids=d.get("passenger_ids", []),
            origin=d.get("origin", ""),
            destination=d.get("destination", ""),
            currency=d.get("currency", "EUR"),
            offers=[FlightOffer.from_dict(o) for o in d.get("offers", [])],
            total_results=d.get("total_results", 0),
            search_params=d.get("search_params", {}),
            pricing_note=d.get("pricing_note", ""),
        )

    @property
    def cheapest(self) -> Optional[FlightOffer]:
        if not self.offers:
            return None
        return min(self.offers, key=lambda o: o.price)


@dataclass
class UnlockResult:
    """Result of unlocking a flight offer ($1 fee)."""
    offer_id: str
    unlock_status: str
    payment_charged: bool
    payment_amount_cents: int
    payment_currency: str
    payment_intent_id: str
    confirmed_price: Optional[float]
    confirmed_currency: str
    offer_expires_at: str
    message: str

    @classmethod
    def from_dict(cls, d: dict) -> "UnlockResult":
        return cls(
            offer_id=d.get("offer_id", ""),
            unlock_status=d.get("unlock_status", ""),
            payment_charged=d.get("payment_charged", False),
            payment_amount_cents=d.get("payment_amount_cents", 0),
            payment_currency=d.get("payment_currency", "usd"),
            payment_intent_id=d.get("payment_intent_id", ""),
            confirmed_price=d.get("confirmed_price"),
            confirmed_currency=d.get("confirmed_currency", ""),
            offer_expires_at=d.get("offer_expires_at", ""),
            message=d.get("message", ""),
        )

    @property
    def is_unlocked(self) -> bool:
        return self.unlock_status == "unlocked"


@dataclass
class Passenger:
    """Passenger details for booking."""
    id: str
    given_name: str
    family_name: str
    born_on: str
    gender: str = "m"
    title: str = "mr"
    email: str = ""
    phone_number: str = ""

    def to_dict(self) -> dict:
        d = {
            "id": self.id,
            "given_name": self.given_name,
            "family_name": self.family_name,
            "born_on": self.born_on,
            "gender": self.gender,
            "title": self.title,
        }
        if self.email:
            d["email"] = self.email
        if self.phone_number:
            d["phone_number"] = self.phone_number
        return d


@dataclass
class BookingResult:
    """Result of a flight booking."""
    booking_id: str
    status: str
    booking_type: str
    offer_id: str
    flight_price: float
    service_fee: float
    service_fee_percentage: float
    total_charged: float
    currency: str
    order_id: str
    booking_reference: str
    unlock_payment_id: str
    fee_payment_id: str
    created_at: str
    details: dict

    @classmethod
    def from_dict(cls, d: dict) -> "BookingResult":
        return cls(
            booking_id=d.get("booking_id", ""),
            status=d.get("status", ""),
            booking_type=d.get("booking_type", ""),
            offer_id=d.get("offer_id", ""),
            flight_price=d.get("flight_price", 0.0),
            service_fee=d.get("service_fee", 0.0),
            service_fee_percentage=d.get("service_fee_percentage", 0.0),
            total_charged=d.get("total_charged", 0.0),
            currency=d.get("currency", "EUR"),
            order_id=d.get("order_id", ""),
            booking_reference=d.get("booking_reference", ""),
            unlock_payment_id=d.get("unlock_payment_id", ""),
            fee_payment_id=d.get("fee_payment_id", ""),
            created_at=d.get("created_at", ""),
            details=d.get("details", {}),
        )

    @property
    def is_confirmed(self) -> bool:
        return self.status == "confirmed"


@dataclass
class AgentProfile:
    """Agent's profile and usage stats."""
    agent_id: str
    agent_name: str
    email: str
    tier: str
    payment_ready: bool
    usage: dict
    payment: Optional[dict] = None

    @classmethod
    def from_dict(cls, d: dict) -> "AgentProfile":
        return cls(
            agent_id=d.get("agent_id", ""),
            agent_name=d.get("agent_name", ""),
            email=d.get("email", ""),
            tier=d.get("tier", "starter"),
            payment_ready=d.get("payment_ready", False),
            usage=d.get("usage", {}),
            payment=d.get("payment"),
        )
