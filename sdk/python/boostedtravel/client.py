"""
BoostedTravel Python SDK — agent-native flight search & booking.

Zero-config, zero-browser, zero-markup. Built for autonomous agents.

    from boostedtravel import BoostedTravel

    bt = BoostedTravel(api_key="trav_...")
    
    # Search (FREE)
    flights = bt.search("LON", "BCN", "2026-04-01")
    print(flights.cheapest.summary())
    
    # Unlock ($1)
    unlock = bt.unlock(flights.cheapest.id)
    
    # Book (FREE after unlock)
    booking = bt.book(
        offer_id=flights.cheapest.id,
        passengers=[{
            "id": flights.passenger_ids[0],
            "given_name": "John", "family_name": "Doe",
            "born_on": "1990-01-15", "gender": "m", "title": "mr",
            "email": "john@example.com"
        }],
        contact_email="john@example.com"
    )
    print(f"PNR: {booking.booking_reference}")
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

from boostedtravel.models import (
    AgentProfile,
    BookingResult,
    FlightSearchResult,
    Passenger,
    UnlockResult,
)

DEFAULT_BASE_URL = "https://api.boostedchat.com"


class BoostedTravelError(Exception):
    """Base exception for BoostedTravel SDK."""

    def __init__(self, message: str, status_code: int = 0, response: dict | None = None):
        self.message = message
        self.status_code = status_code
        self.response = response or {}
        super().__init__(message)


class AuthenticationError(BoostedTravelError):
    """API key is missing or invalid."""
    pass


class PaymentRequiredError(BoostedTravelError):
    """Payment method not set up."""
    pass


class OfferExpiredError(BoostedTravelError):
    """Offer is no longer available."""
    pass


class BoostedTravel:
    """
    BoostedTravel API client — for autonomous agents.

    Get an API key: POST /api/v1/agents/register
    Or set BOOSTEDTRAVEL_API_KEY environment variable.

    Pricing:
      - Search: FREE (unlimited)
      - Unlock: $1 per offer (confirms price, reserves 30min)
      - Book: FREE after unlock (creates real airline reservation)
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: int = 30,
    ):
        self.api_key = api_key or os.environ.get("BOOSTEDTRAVEL_API_KEY", "")
        self.base_url = (base_url or os.environ.get("BOOSTEDTRAVEL_BASE_URL", DEFAULT_BASE_URL)).rstrip("/")
        self.timeout = timeout

        if not self.api_key:
            raise AuthenticationError(
                "API key required. Set api_key parameter or BOOSTEDTRAVEL_API_KEY env var. "
                "Get one: POST /api/v1/agents/register"
            )

    # ── Core API methods ──────────────────────────────────────────────────

    def search(
        self,
        origin: str,
        destination: str,
        date_from: str,
        *,
        return_date: str | None = None,
        adults: int = 1,
        children: int = 0,
        infants: int = 0,
        cabin_class: str | None = None,
        max_stopovers: int = 2,
        currency: str = "EUR",
        limit: int = 20,
        sort: str = "price",
    ) -> FlightSearchResult:
        """
        Search for flights — completely FREE.

        Args:
            origin: IATA code (e.g., "LON", "GDN", "JFK")
            destination: IATA code (e.g., "BCN", "BER", "LAX")
            date_from: Departure date "YYYY-MM-DD"
            return_date: Return date for round-trip (omit for one-way)
            adults: Number of adult passengers (1-9)
            children: Number of children (0-9)
            infants: Number of infants (0-9)
            cabin_class: "M" (economy), "W" (premium), "C" (business), "F" (first)
            max_stopovers: Max connections per direction (0-4)
            currency: 3-letter currency code
            limit: Max results (1-100)
            sort: "price" or "duration"

        Returns:
            FlightSearchResult with offers, passenger_ids, and metadata.
        """
        body: dict[str, Any] = {
            "origin": origin.upper(),
            "destination": destination.upper(),
            "date_from": date_from,
            "adults": adults,
            "children": children,
            "infants": infants,
            "max_stopovers": max_stopovers,
            "currency": currency,
            "limit": limit,
            "sort": sort,
        }
        if return_date:
            body["return_from"] = return_date
        if cabin_class:
            body["cabin_class"] = cabin_class.upper()

        data = self._post("/api/v1/flights/search", body)
        return FlightSearchResult.from_dict(data)

    def resolve_location(self, query: str) -> list[dict]:
        """
        Resolve a city/airport name to IATA codes.

        Args:
            query: City or airport name (e.g., "London", "Berlin")

        Returns:
            List of matching locations with IATA codes.
        """
        data = self._get(f"/api/v1/flights/locations/{query}")
        if isinstance(data, dict) and "locations" in data:
            return data["locations"]
        if isinstance(data, list):
            return data
        return [data] if data else []

    def unlock(self, offer_id: str) -> UnlockResult:
        """
        Unlock a flight offer — $1 proof-of-intent fee.

        Confirms the latest price with the airline and reserves
        the offer for 30 minutes. Required before booking.

        Args:
            offer_id: The offer ID from search results.

        Returns:
            UnlockResult with confirmed price and status.
        """
        data = self._post("/api/v1/bookings/unlock", {"offer_id": offer_id})
        return UnlockResult.from_dict(data)

    def book(
        self,
        offer_id: str,
        passengers: list[dict | Passenger],
        contact_email: str,
        contact_phone: str = "",
    ) -> BookingResult:
        """
        Book a flight — creates a real airline reservation.

        Args:
            offer_id: The offer ID (must be unlocked first).
            passengers: List of passenger dicts or Passenger objects.
                Each must include: id (pas_xxx from search), given_name,
                family_name, born_on (YYYY-MM-DD), gender, title.
            contact_email: Contact email for the booking.
            contact_phone: Contact phone (optional).

        Returns:
            BookingResult with PNR, fees, and confirmation.
        """
        pax_list = []
        for p in passengers:
            if isinstance(p, Passenger):
                pax_list.append(p.to_dict())
            else:
                pax_list.append(p)

        body = {
            "offer_id": offer_id,
            "booking_type": "flight",
            "passengers": pax_list,
            "contact_email": contact_email,
            "contact_phone": contact_phone,
        }
        data = self._post("/api/v1/bookings/book", body)
        return BookingResult.from_dict(data)

    def setup_payment(self, token: str = "tok_visa") -> dict:
        """
        Set up a payment method using a payment token.

        Args:
            token: Payment token (default: "tok_visa" for testing).

        Returns:
            Dict with status and payment_method_id.
        """
        return self._post("/api/v1/agents/setup-payment", {"token": token})

    def me(self) -> AgentProfile:
        """Get the current agent's profile, usage, and payment status."""
        data = self._get("/api/v1/agents/me")
        return AgentProfile.from_dict(data)

    # ── Static methods (no auth needed) ───────────────────────────────────

    @staticmethod
    def register(
        agent_name: str,
        email: str,
        *,
        base_url: str | None = None,
        owner_name: str = "",
        description: str = "",
    ) -> dict:
        """
        Register a new agent — no API key needed.

        Args:
            agent_name: Your agent's name
            email: Contact email for billing
            base_url: API base URL (default: production)
            owner_name: Person/org name (optional)
            description: What your agent does (optional)

        Returns:
            Dict with agent_id, api_key, and instructions.
        """
        url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        body = {
            "agent_name": agent_name,
            "email": email,
            "owner_name": owner_name,
            "description": description,
        }
        data = json.dumps(body).encode()
        req = Request(
            f"{url}/api/v1/agents/register",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            body_text = e.read().decode() if e.fp else ""
            try:
                err = json.loads(body_text)
            except Exception:
                err = {"detail": body_text}
            raise BoostedTravelError(
                err.get("detail", f"Registration failed ({e.code})"),
                status_code=e.code,
                response=err,
            ) from e

    # ── Internals ─────────────────────────────────────────────────────────

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
            "User-Agent": "boostedtravel-python/0.1.0",
        }

    def _post(self, path: str, body: dict) -> Any:
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode()
        req = Request(url, data=data, headers=self._headers(), method="POST")
        return self._do_request(req)

    def _get(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        req = Request(url, headers=self._headers(), method="GET")
        return self._do_request(req)

    def _do_request(self, req: Request) -> Any:
        try:
            with urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            body_text = e.read().decode() if e.fp else ""
            try:
                err = json.loads(body_text)
            except Exception:
                err = {"detail": body_text}

            detail = err.get("detail", f"API error ({e.code})")

            if e.code == 401:
                raise AuthenticationError(detail, status_code=401, response=err) from e
            elif e.code == 402:
                raise PaymentRequiredError(detail, status_code=402, response=err) from e
            elif e.code == 410:
                raise OfferExpiredError(detail, status_code=410, response=err) from e
            else:
                raise BoostedTravelError(detail, status_code=e.code, response=err) from e
        except URLError as e:
            raise BoostedTravelError(f"Connection failed: {e.reason}") from e

    def __repr__(self) -> str:
        masked = self.api_key[:8] + "..." if len(self.api_key) > 8 else "***"
        return f"BoostedTravel(base_url={self.base_url!r}, api_key={masked!r})"
