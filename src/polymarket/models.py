from __future__ import annotations

import json
import re
from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

MARKETS_SCHEMA_VERSION: str = "1.0"
PRICE_HISTORY_SCHEMA_VERSION: str = "1.1"
MARKETS_BY_SLUG_SCHEMA_VERSION: str = "1.1"
PRICE_HISTORY_BY_SLUG_SCHEMA_VERSION: str = "1.1"

# Polymarket serializes gameStartTime with a 2-char UTC offset ("+00") and a
# space separator instead of "T". Pad the offset so pydantic/fromisoformat can
# parse it.
_SHORT_TZ_RE = re.compile(r"([+-]\d{2})$")

_CANCEL_THRESHOLD: float = 0.001


class GammaMarket(BaseModel):
    id: str
    question: str
    slug: str
    active: bool
    closed: bool
    liquidity: float = 0.0
    volume: float
    condition_id: str = Field(alias="conditionId")
    end_date_iso: str | None = Field(None, alias="endDateIso")
    game_start_time: datetime | None = Field(None, alias="gameStartTime")
    clob_token_ids: list[str] = Field(default_factory=list, alias="clobTokenIds")

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("liquidity", "volume", mode="before")
    @classmethod
    def coerce_numeric(cls, v: object) -> float:
        return float(v) if v is not None else 0.0

    @field_validator("clob_token_ids", mode="before")
    @classmethod
    def parse_clob_token_ids(cls, v: object) -> object:
        if isinstance(v, str):
            return json.loads(v)
        return v

    @field_validator("game_start_time", mode="before")
    @classmethod
    def _normalize_game_start_time(cls, v: object) -> object:
        if isinstance(v, str):
            return _SHORT_TZ_RE.sub(r"\1:00", v.strip())
        return v


class PricePoint(BaseModel):
    t: int    # Unix timestamp (seconds)
    p: float  # Implied probability [0.0, 1.0]


def detect_cancelled(price_points: list[PricePoint]) -> bool:
    """Return True if all price points are flat-lined at p=0.5 (cancelled market)."""
    if len(price_points) < 2:
        return False
    return all(abs(pt.p - 0.5) < _CANCEL_THRESHOLD for pt in price_points)


class TokenInfo(BaseModel):
    token_id: str
    outcome: str          # "Yes" or "No"
    team_name: str | None = None
    market_slug: str
    condition_id: str


class TokenBundle(BaseModel):
    token_id: str
    outcome: str                   # "Yes" or "No"
    midpoint: float | None = None  # snapshot midpoint price in [0, 1]
    history: list[PricePoint] = Field(default_factory=list)
    is_cancelled: bool = False


class SlugFetchResult(BaseModel):
    slug: str
    game_date: date
    fetched_at: datetime
    market: GammaMarket
    tokens: list[TokenBundle] = Field(default_factory=list)
