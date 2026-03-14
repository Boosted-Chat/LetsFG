"""
BoostedTravel — Agent-native flight search & booking SDK.

58 airline connectors run locally + enterprise GDS/NDC APIs via backend.

Local search (FREE, no API key):
    from boostedtravel.local import search_local
    result = await search_local("SHA", "CTU", "2026-03-20")

Full API (search + unlock + book):
    from boostedtravel import BoostedTravel
    bt = BoostedTravel(api_key="trav_...")
    flights = bt.search("GDN", "BER", "2026-03-03")
    bt.unlock(flights.offers[0].id)
    bt.book(flights.offers[0].id, passenger={...})
"""

from boostedtravel.client import (
    BoostedTravel,
    BoostedTravelError,
    AuthenticationError,
    PaymentRequiredError,
    OfferExpiredError,
    ValidationError,
    ErrorCode,
    ErrorCategory,
)
from boostedtravel.models import (
    FlightOffer,
    FlightSearchResult,
    FlightSegment,
    FlightRoute,
    UnlockResult,
    BookingResult,
    Passenger,
    AgentProfile,
)

__version__ = "0.2.7"
__all__ = [
    "BoostedTravel",
    "BoostedTravelError",
    "AuthenticationError",
    "PaymentRequiredError",
    "OfferExpiredError",
    "ValidationError",
    "ErrorCode",
    "ErrorCategory",
    "FlightOffer",
    "FlightSearchResult",
    "FlightSegment",
    "FlightRoute",
    "UnlockResult",
    "BookingResult",
    "Passenger",
    "AgentProfile",
]
