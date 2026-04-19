# Spec: Pydantic Models (`src/polymarket/models.py`)

## Purpose
All external API response shapes are validated here. No raw dicts escape into fetchers or storage.

## Models

### `GammaMarket`
Represents one market from the Gamma API `/markets` endpoint.

```python
class GammaMarket(BaseModel):
    id: str
    question: str
    slug: str
    active: bool
    closed: bool
    liquidity: float
    volume: float
    condition_id: str = Field(alias="conditionId")
    end_date_iso: str | None = Field(None, alias="endDateIso")
    clob_token_ids: list[str] = Field(default_factory=list, alias="clobTokenIds")

    model_config = ConfigDict(populate_by_name=True)

    @field_validator("liquidity", "volume", mode="before")
    @classmethod
    def coerce_numeric(cls, v):
        return float(v) if v is not None else 0.0
```

**Notes:**
- `clob_token_ids[0]` = YES token (team named in question wins)
- `clob_token_ids[1]` = NO token
- A market with an empty `clob_token_ids` list cannot be priced — skip it

---

### `PricePoint`
One data point from CLOB `/prices-history` response `history` array.

```python
class PricePoint(BaseModel):
    t: int   # Unix timestamp (seconds)
    p: float # Implied probability [0.0, 1.0]
```

**Notes:**
- `p` is directly usable as win probability — no conversion needed
- Flat series at `p=0.50` for ≥ 2 hours signals a postponed/cancelled game

---

### `TokenInfo`
Enriched token record used when writing price history to Parquet.

```python
class TokenInfo(BaseModel):
    token_id: str
    outcome: str        # "Yes" or "No"
    team_name: str | None = None
    market_slug: str
    condition_id: str
```

**Notes:**
- `team_name` is parsed from `GammaMarket.question` when possible (best-effort)
- Used to add context columns to price history Parquet files

---

## Validation Error Policy
Always catch `pydantic.ValidationError` at the call site (in fetchers), log it, and skip the record. Never let a validation error propagate to the pipeline orchestrator.
