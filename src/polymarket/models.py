from typing import Optional

from pydantic import BaseModel, field_validator


class GammaMarket(BaseModel):
    id: int
    question: str
    slug: str
    active: bool
    closed: bool
    liquidity: float
    volume: float
    condition_id: Optional[str] = None
    end_date_iso: Optional[str] = None

    @field_validator("liquidity", "volume", mode="before")
    @classmethod
    def coerce_numeric(cls, v):
        return float(v) if v is not None else 0.0
