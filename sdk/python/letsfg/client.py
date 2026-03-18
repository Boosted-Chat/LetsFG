"""
LetsFG Python SDK — agent-native flight search & booking.

Zero-config, zero-browser, zero-markup. Built for autonomous agents.
Search is free. Booking charges the ticket price via Stripe (zero markup).

    from letsfg import LetsFG

    bt = LetsFG(api_key="trav_...")
    
    # Link GitHub (one-time — star the repo first)
    bt.link_github("myusername")
    
    # Setup payment (one-time — required before booking)
    bt.setup_payment(token="tok_visa")
    
    # Search (FREE)
    flights = bt.search("LON", "BCN", "2026-04-01")
    print(flights.cheapest.summary())
    
    # Unlock (FREE)
    unlock = bt.unlock(flights.cheapest.id)
    
    # Book (ticket price charged via Stripe)
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

from letsfg.models import (
    AgentProfile,
    BookingResult,
    CheckoutProgress,
    FlightSearchResult,
    Passenger,
    UnlockResult,
)

DEFAULT_BASE_URL = "https://api.letsfg.co"

# ── Bookable connector registry ────────────────────────────────────────────
# Maps source tags to their BookableConnector subclass.
# Loaded lazily to avoid importing Playwright at module level.
# For the 3 airlines with hand-tuned connectors we keep explicit entries.
# All other airlines are handled by the GenericCheckoutEngine via config.

_BOOKABLE_CONNECTORS: dict[str, tuple[str, str]] = {
    "ryanair_direct": ("letsfg.connectors.ryanair", "RyanairBookableConnector"),
    "wizzair_api": ("letsfg.connectors.wizzair", "WizzairBookableConnector"),
    "easyjet_direct": ("letsfg.connectors.easyjet", "EasyjetBookableConnector"),
}


def _get_bookable_connector(source: str):
    """Dynamically load a bookable connector class by source tag.

    Falls back to the generic config-driven checkout engine if no
    hand-tuned connector exists but an airline config is registered.
    """
    # 1. Check for hand-tuned connector
    entry = _BOOKABLE_CONNECTORS.get(source)
    if entry:
        mod_name, cls_name = entry
        try:
            import importlib
            mod = importlib.import_module(mod_name)
            return getattr(mod, cls_name)
        except (ImportError, AttributeError):
            pass

    # 2. Fall back to generic checkout engine config
    try:
        from letsfg.connectors.checkout_engine import AIRLINE_CONFIGS
        if source in AIRLINE_CONFIGS:
            return _make_generic_connector(source)
    except ImportError:
        pass

    return None


def _make_generic_connector(source: str):
    """Return a BookableConnector subclass backed by the generic engine."""
    from letsfg.connectors.booking_base import BookableConnector, CheckoutProgress as _CP
    from letsfg.connectors.checkout_engine import AIRLINE_CONFIGS, GenericCheckoutEngine

    config = AIRLINE_CONFIGS[source]

    class _GenericBookable(BookableConnector):
        AIRLINE_NAME = config.airline_name
        SOURCE_TAG = config.source_tag

        async def _run_checkout(self, offer, passengers):
            # Token already verified by base class start_checkout()
            # but the engine also verifies — pass dummy to skip double-check
            engine = GenericCheckoutEngine()
            return await engine.run(
                config=config,
                offer=offer,
                passengers=passengers,
                checkout_token=self._last_token,
                api_key=self._last_api_key,
                base_url=self._last_base_url,
            )

    _GenericBookable.__name__ = f"{config.airline_name.replace(' ', '')}Bookable"
    return _GenericBookable


# ── Error codes ──────────────────────────────────────────────────────────
# Machine-readable error codes for agent decision-making.
# Each code has a category that tells the agent how to react:
#   transient  — retry after a short delay (network blip, rate limit, supplier timeout)
#   validation — fix the request and retry (bad input, unsupported route)
#   business   — requires human decision (payment declined, fare expired, policy violation)

class ErrorCode:
    """Machine-readable error codes returned in LetsFGError.error_code."""
    # ── Transient (safe to retry) ──
    SUPPLIER_TIMEOUT = "SUPPLIER_TIMEOUT"
    RATE_LIMITED = "RATE_LIMITED"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    NETWORK_ERROR = "NETWORK_ERROR"

    # ── Validation (fix input, then retry) ──
    INVALID_IATA = "INVALID_IATA"
    INVALID_DATE = "INVALID_DATE"
    INVALID_PASSENGERS = "INVALID_PASSENGERS"
    UNSUPPORTED_ROUTE = "UNSUPPORTED_ROUTE"
    MISSING_PARAMETER = "MISSING_PARAMETER"
    INVALID_PARAMETER = "INVALID_PARAMETER"

    # ── Business (human decision needed) ──
    AUTH_INVALID = "AUTH_INVALID"
    PAYMENT_REQUIRED = "PAYMENT_REQUIRED"
    PAYMENT_DECLINED = "PAYMENT_DECLINED"
    OFFER_EXPIRED = "OFFER_EXPIRED"
    OFFER_NOT_UNLOCKED = "OFFER_NOT_UNLOCKED"
    FARE_CHANGED = "FARE_CHANGED"
    ALREADY_BOOKED = "ALREADY_BOOKED"
    BOOKING_FAILED = "BOOKING_FAILED"


class ErrorCategory:
    """Error categories — tells agent whether to retry, fix input, or escalate."""
    TRANSIENT = "transient"
    VALIDATION = "validation"
    BUSINESS = "business"


_CODE_TO_CATEGORY = {
    ErrorCode.SUPPLIER_TIMEOUT: ErrorCategory.TRANSIENT,
    ErrorCode.RATE_LIMITED: ErrorCategory.TRANSIENT,
    ErrorCode.SERVICE_UNAVAILABLE: ErrorCategory.TRANSIENT,
    ErrorCode.NETWORK_ERROR: ErrorCategory.TRANSIENT,
    ErrorCode.INVALID_IATA: ErrorCategory.VALIDATION,
    ErrorCode.INVALID_DATE: ErrorCategory.VALIDATION,
    ErrorCode.INVALID_PASSENGERS: ErrorCategory.VALIDATION,
    ErrorCode.UNSUPPORTED_ROUTE: ErrorCategory.VALIDATION,
    ErrorCode.MISSING_PARAMETER: ErrorCategory.VALIDATION,
    ErrorCode.INVALID_PARAMETER: ErrorCategory.VALIDATION,
    ErrorCode.AUTH_INVALID: ErrorCategory.BUSINESS,
    ErrorCode.PAYMENT_REQUIRED: ErrorCategory.BUSINESS,
    ErrorCode.PAYMENT_DECLINED: ErrorCategory.BUSINESS,
    ErrorCode.OFFER_EXPIRED: ErrorCategory.BUSINESS,
    ErrorCode.OFFER_NOT_UNLOCKED: ErrorCategory.BUSINESS,
    ErrorCode.FARE_CHANGED: ErrorCategory.BUSINESS,
    ErrorCode.ALREADY_BOOKED: ErrorCategory.BUSINESS,
    ErrorCode.BOOKING_FAILED: ErrorCategory.BUSINESS,
}


def _infer_error_code(status_code: int, detail: str) -> str:
    """Infer a machine-readable error code from HTTP status and detail text."""
    detail_lower = detail.lower()
    if status_code == 401:
        return ErrorCode.AUTH_INVALID
    if status_code == 402:
        if "declined" in detail_lower:
            return ErrorCode.PAYMENT_DECLINED
        return ErrorCode.PAYMENT_REQUIRED
    if status_code == 410:
        return ErrorCode.OFFER_EXPIRED
    if status_code == 422:
        if "iata" in detail_lower or "airport" in detail_lower:
            return ErrorCode.INVALID_IATA
        if "date" in detail_lower:
            return ErrorCode.INVALID_DATE
        if "passenger" in detail_lower:
            return ErrorCode.INVALID_PASSENGERS
        if "route" in detail_lower:
            return ErrorCode.UNSUPPORTED_ROUTE
        return ErrorCode.INVALID_PARAMETER
    if status_code == 429:
        return ErrorCode.RATE_LIMITED
    if status_code == 503:
        return ErrorCode.SERVICE_UNAVAILABLE
    if status_code == 504:
        return ErrorCode.SUPPLIER_TIMEOUT
    if status_code == 409:
        return ErrorCode.ALREADY_BOOKED
    return ErrorCode.BOOKING_FAILED if status_code >= 500 else ErrorCode.INVALID_PARAMETER


class LetsFGError(Exception):
    """
    Base exception for LetsFG SDK.

    Attributes:
        message: Human-readable error description.
        status_code: HTTP status code (0 for client-side errors).
        error_code: Machine-readable code (e.g., 'OFFER_EXPIRED'). See ErrorCode.
        error_category: One of 'transient', 'validation', 'business'. See ErrorCategory.
        response: Raw error response dict from the API.
        is_retryable: True if the error is transient (safe to retry after delay).
    """

    def __init__(
        self,
        message: str,
        status_code: int = 0,
        response: dict | None = None,
        error_code: str = "",
    ):
        self.message = message
        self.status_code = status_code
        self.response = response or {}
        self.error_code = error_code or self.response.get("error_code", "")
        self.error_category = _CODE_TO_CATEGORY.get(self.error_code, ErrorCategory.BUSINESS)
        self.is_retryable = self.error_category == ErrorCategory.TRANSIENT
        super().__init__(message)


class AuthenticationError(LetsFGError):
    """API key is missing or invalid."""
    pass


class PaymentRequiredError(LetsFGError):
    """Payment method not set up or payment declined."""
    pass


class OfferExpiredError(LetsFGError):
    """Offer is no longer available — search again."""
    pass


class ValidationError(LetsFGError):
    """Request parameters are invalid — fix input and retry."""
    pass


class LetsFG:
    """
    LetsFG API client — for autonomous agents.

    Get an API key: POST /api/v1/agents/register
    Or set LETSFG_API_KEY environment variable.

    Pricing:
      - Search: FREE (unlimited)
      - Unlock: FREE (confirms price, reserves 30min)
      - Book: FREE after unlock (creates real airline reservation)
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: int = 30,
    ):
        self.api_key = api_key or os.environ.get("LETSFG_API_KEY") or os.environ.get("BOOSTEDTRAVEL_API_KEY", "")
        self.base_url = (base_url or os.environ.get("LETSFG_BASE_URL") or os.environ.get("BOOSTEDTRAVEL_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout

    def _require_api_key(self) -> None:
        if not self.api_key:
            raise AuthenticationError(
                "API key required for this operation. Set api_key parameter or "
                "LETSFG_API_KEY env var. Get one: POST /api/v1/agents/register\n"
                "Note: search_local() works without an API key."
            )

    # ── Local search (75 airline connectors, no API key needed) ────────────────

    def search_local(
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
        currency: str = "EUR",
        limit: int = 50,
        max_browsers: int | None = None,
    ) -> FlightSearchResult:
        """
        Search flights using 73 local airline connectors — FREE, no API key needed.

        Runs Ryanair, EasyJet, Spring Airlines, Lucky Air, and 54 more
        airline connectors directly on your machine. No backend call.

        Requires: playwright install chromium  (one-time setup)

        Args:
            origin: IATA code (e.g., "SHA", "GDN", "JFK")
            destination: IATA code (e.g., "CTU", "BER", "LAX")
            date_from: Departure date "YYYY-MM-DD"
            return_date: Return date for round-trip (omit for one-way)
            adults / children / infants: Passenger counts
            cabin_class: "M" (economy), "W" (premium), "C" (business), "F" (first)
            currency: 3-letter currency code
            limit: Max results (1-200)
            max_browsers: Max concurrent browser processes (1-32, default: auto-detect).

        Returns:
            FlightSearchResult with offers from local scrapers.
        """
        import asyncio
        from letsfg.local import search_local as _search

        result_dict = asyncio.run(_search(
            origin=origin,
            destination=destination,
            date_from=date_from,
            return_date=return_date,
            adults=adults,
            children=children,
            infants=infants,
            cabin_class=cabin_class,
            currency=currency,
            limit=limit,
            max_browsers=max_browsers,
        ))
        return FlightSearchResult.from_dict(result_dict)

    # ── Core API methods (requires API key) ───────────────────────────────

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
        self._require_api_key()
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
        self._require_api_key()
        data = self._get(f"/api/v1/flights/locations/{query}")
        if isinstance(data, dict) and "locations" in data:
            return data["locations"]
        if isinstance(data, list):
            return data
        return [data] if data else []

    def link_github(self, github_username: str) -> dict:
        """
        Link your GitHub account for free unlimited access.

        Star https://github.com/LetsFG/LetsFG first, then call this
        with your GitHub username. Once verified, you get free access
        to unlock, book, and checkout — forever.

        Args:
            github_username: Your GitHub username.

        Returns:
            Dict with verification status and message.
        """
        self._require_api_key()
        qs = urlencode({"github_username": github_username})
        return self._post(f"/api/v1/agents/link-github?{qs}", {})

    def unlock(self, offer_id: str) -> UnlockResult:
        """
        Unlock a flight offer — confirms live price and reserves for 30 minutes.

        FREE with GitHub star (link your account first via link_github()).
        Required before booking.

        Args:
            offer_id: The offer ID from search results.

        Returns:
            UnlockResult with confirmed price and status.
        """
        self._require_api_key()
        data = self._post("/api/v1/bookings/unlock", {"offer_id": offer_id})
        return UnlockResult.from_dict(data)

    def book(
        self,
        offer_id: str,
        passengers: list[dict | Passenger],
        contact_email: str,
        contact_phone: str = "",
        idempotency_key: str = "",
    ) -> BookingResult:
        """
        Book a flight — creates a real airline reservation.

        IMPORTANT: Always provide an idempotency_key to prevent double-bookings
        if your agent retries this call. Use any unique string (UUID, session ID,
        or deterministic hash of offer_id + passenger names).

        Args:
            offer_id: The offer ID (must be unlocked first).
            passengers: List of passenger dicts or Passenger objects.
                Each must include: id (pas_xxx from search), given_name,
                family_name, born_on (YYYY-MM-DD), gender, title.
            contact_email: Contact email for the booking.
            contact_phone: Contact phone (optional).
            idempotency_key: Unique key for this booking attempt. If the same key
                is sent twice, the second call returns the original booking instead
                of creating a duplicate. Strongly recommended for safety.

        Returns:
            BookingResult with PNR, fees, and confirmation.
        """
        self._require_api_key()
        pax_list = []
        for p in passengers:
            if isinstance(p, Passenger):
                pax_list.append(p.to_dict())
            else:
                pax_list.append(p)

        body: dict[str, Any] = {
            "offer_id": offer_id,
            "booking_type": "flight",
            "passengers": pax_list,
            "contact_email": contact_email,
            "contact_phone": contact_phone,
        }
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
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
        self._require_api_key()
        return self._post("/api/v1/agents/setup-payment", {"token": token})

    def start_checkout(
        self,
        offer_id: str,
        passengers: list[dict | Passenger] | None = None,
        *,
        checkout_token: str = "",
    ) -> CheckoutProgress:
        """
        Drive automated checkout up to (not including) payment — SAFE, no charge.

        This navigates the airline's website through flight selection, passenger
        details, and extras, stopping at the payment page. The user can then
        complete payment manually via the returned booking_url.

        Requires a checkout token from unlock() — the unlock step must be
        completed before checkout automation runs. This prevents abuse since the
        token is verified with the closed-source backend.

        For airlines without automated checkout, returns the booking_url
        for manual completion.

        Args:
            offer_id: The offer ID from search results.
            passengers: Passenger details. If None, uses safe test data
                (Test Traveler, test@example.com). Pass real data for
                actual bookings.
            checkout_token: Token from unlock() response. Required.

        Returns:
            CheckoutProgress with status, screenshot, and booking_url.
        """
        self._require_api_key()
        pax_list = []
        if passengers:
            for p in passengers:
                if isinstance(p, Passenger):
                    pax_list.append(p.to_dict())
                else:
                    pax_list.append(p)

        body: dict[str, Any] = {
            "offer_id": offer_id,
            "checkout_token": checkout_token,
        }
        if pax_list:
            body["passengers"] = pax_list

        data = self._post("/api/v1/bookings/start-checkout", body)
        return CheckoutProgress.from_dict(data)

    def start_checkout_local(
        self,
        offer: dict,
        passengers: list[dict] | None = None,
        *,
        checkout_token: str = "",
    ) -> CheckoutProgress:
        """
        Run checkout locally using Playwright — drives the airline website
        on your machine. Stops at the payment page (no charge).

        This is the local-first version: the connector runs on your device,
        but the checkout token is still verified with the backend to enforce
        the star-gating (you must have linked your GitHub first).

        Requires: playwright install chromium

        Args:
            offer: Full FlightOffer dict (from search results, must include
                booking_url, source, price, currency, outbound).
            passengers: Passenger dicts. If None, uses safe test data.
            checkout_token: Token from unlock(). Required.

        Returns:
            CheckoutProgress with status, screenshot, and booking_url.
        """
        import asyncio
        from letsfg.connectors.booking_base import (
            FAKE_PASSENGER,
            CheckoutProgress as _CP,
        )

        if not passengers:
            passengers = [FAKE_PASSENGER.copy()]
            passengers[0]["id"] = "pas_0"

        source = (offer.get("source") or "").lower()
        booking_url = offer.get("booking_url", "")

        # Try to load the connector's booking module
        connector_cls = _get_bookable_connector(source)
        if connector_cls is None:
            # No automated checkout — return URL-only progress
            return CheckoutProgress.from_dict({
                "status": "url_only",
                "step": "started",
                "step_index": 0,
                "airline": offer.get("owner_airline", ""),
                "source": source,
                "offer_id": offer.get("id", ""),
                "total_price": offer.get("price", 0.0),
                "currency": offer.get("currency", "EUR"),
                "booking_url": booking_url,
                "message": (
                    f"Automated checkout not available for {source}. "
                    f"Use the booking URL to complete manually."
                    + (f"\n\nBooking URL: {booking_url}" if booking_url else "")
                ),
                "can_complete_manually": bool(booking_url),
            })

        connector = connector_cls()
        result = asyncio.run(connector.start_checkout(
            offer=offer,
            passengers=passengers,
            checkout_token=checkout_token,
            api_key=self.api_key,
            base_url=self.base_url,
        ))
        return CheckoutProgress.from_dict(result.to_dict())

    def me(self) -> AgentProfile:
        """Get the current agent's profile, usage, and payment status."""
        self._require_api_key()
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
            headers={"Content-Type": "application/json", "User-Agent": "LetsFG-Python-SDK/1.0.3"},
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
            raise LetsFGError(
                err.get("detail", f"Registration failed ({e.code})"),
                status_code=e.code,
                response=err,
            ) from e

    # ── Internals ─────────────────────────────────────────────────────────

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
            "User-Agent": "LetsFG-Python-SDK/1.0.3",
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
            code = err.get("error_code") or _infer_error_code(e.code, detail)

            if e.code == 401:
                raise AuthenticationError(detail, status_code=401, response=err, error_code=code) from e
            elif e.code == 402:
                raise PaymentRequiredError(detail, status_code=402, response=err, error_code=code) from e
            elif e.code == 410:
                raise OfferExpiredError(detail, status_code=410, response=err, error_code=code) from e
            elif e.code == 422:
                raise ValidationError(detail, status_code=422, response=err, error_code=code) from e
            else:
                raise LetsFGError(detail, status_code=e.code, response=err, error_code=code) from e
        except URLError as e:
            raise LetsFGError(
                f"Connection failed: {e.reason}",
                error_code=ErrorCode.NETWORK_ERROR,
            ) from e

    def __repr__(self) -> str:
        masked = self.api_key[:8] + "..." if len(self.api_key) > 8 else "***"
        return f"LetsFG(base_url={self.base_url!r}, api_key={masked!r})"


# Backward-compat aliases (deprecated — use LetsFG / LetsFGError directly)
BoostedTravel = LetsFG
BoostedTravelError = LetsFGError
