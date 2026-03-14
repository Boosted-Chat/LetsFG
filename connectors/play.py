"""
PLAY Airlines stub connector — airline ceased operations.

PLAY (IATA: OG) was an Icelandic low-cost carrier operating transatlantic
and European routes from Keflavik (KEF). The airline shut down and
flyplay.com went offline. This stub ensures engine.py imports succeed
and returns empty results.
"""

from __future__ import annotations

import logging
from typing import Any

from models.flights import (
    FlightSearchRequest,
    FlightSearchResponse,
)

logger = logging.getLogger(__name__)


class PlayConnectorClient:
    """Stub connector for defunct PLAY Airlines (OG)."""

    def __init__(self, timeout: float = 25.0, **kwargs):
        pass

    async def search(self, req: FlightSearchRequest, **kw: Any) -> FlightSearchResponse:
        return FlightSearchResponse(search_id="play_defunct", offers=[], total_results=0)
