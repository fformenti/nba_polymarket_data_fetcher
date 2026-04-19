from __future__ import annotations

import pytest

# ── Canonical sample API response data ───────────────────────────────────────
# Module-level constants so integration and unit tests can share the same
# representative payloads without duplicating raw dicts everywhere.

SAMPLE_MARKET: dict = {
    "id": "market-1",
    "question": "Will Lakers beat Celtics?",
    "slug": "lakers-vs-celtics",
    "active": True,
    "closed": False,
    "liquidity": "10000.0",
    "volume": "50000.0",
    "conditionId": "cond-1",
    "endDateIso": "2026-04-10T00:00:00Z",
    "clobTokenIds": ["token-yes-1", "token-no-1"],
}

SAMPLE_PRICE_HISTORY: dict = {
    "history": [
        {"t": 1700000000, "p": 0.65},
        {"t": 1700003600, "p": 0.70},
    ]
}

SAMPLE_MIDPOINT: dict = {"mid": 0.68}


@pytest.fixture
def sample_market() -> dict:
    return SAMPLE_MARKET.copy()


@pytest.fixture
def sample_price_history() -> dict:
    return {"history": list(SAMPLE_PRICE_HISTORY["history"])}


@pytest.fixture
def sample_midpoint() -> dict:
    return SAMPLE_MIDPOINT.copy()
