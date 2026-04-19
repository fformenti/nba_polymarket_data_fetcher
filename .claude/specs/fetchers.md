# Spec: Fetchers (`src/polymarket/fetchers/`)

## Interface Contract

All fetchers are `async` functions. They:
1. Read their cursor/checkpoint from `storage.state.load_state(key)`
2. Call `client.get()` in a loop until exhausted
3. Validate each record against a Pydantic model
4. Save state after each successful page
5. Return the full accumulated list

## `fetchers/markets.py`

### `fetch_all_markets() → list[GammaMarket]`

```
GET https://gamma-api.polymarket.com/markets
  ?limit=100
  &offset={cursor}
  &tag=nba          ← filter to NBA only
```

- State key: `"markets"`, field: `{"offset": int}`
- Pages until response length < 100 (last page)
- Validates each item as `GammaMarket`; skips + logs on `ValidationError`
- Saves `{"offset": new_offset}` after each page
- Returns all valid `GammaMarket` instances across all pages

**BLOCKER dependency:** `storage/state.py` must be implemented first.

---

## `fetchers/price_history.py`

### `fetch_price_history(token_id: str, start_ts: int | None = None) → list[PricePoint]`

```
GET https://clob.polymarket.com/prices-history
  ?market={token_id}
  &fidelity=60
  &interval=max          ← if start_ts is None (first fetch)
  &startTs={start_ts}    ← if start_ts is provided (incremental)
```

- State key: `"price_history"`, field: `{token_id: last_timestamp_int}`
- On first call for a token: `interval=max`, no `startTs`
- On subsequent calls: `startTs = state[token_id]`
- Parses `response["history"]` list into `list[PricePoint]`
- After successful fetch, saves `max(point.t for point in points)` to state
- Returns `list[PricePoint]` (may be empty if no new data)

**BLOCKER dependency:** `PricePoint` model must be defined in `models.py`.

---

## `fetchers/prices.py`

### `fetch_snapshot_prices(token_ids: list[str]) → dict[str, float]`

```
GET https://clob.polymarket.com/midpoint?token_id={token_id}
```

- Calls the endpoint once per token_id (no pagination)
- Response shape: `{"mid": "0.62"}` — parse `mid` as float
- Returns `{token_id: midpoint_float}` for all tokens
- Skips tokens where the request fails (logs warning, continues)
- No state persistence (snapshot data, not incremental)

---

## Common Patterns

```python
# Standard fetcher skeleton
async def fetch_*(...)  -> list[Model]:
    log = structlog.get_logger().bind(fetcher="name")
    state = load_state("key")
    results = []
    # ... loop ...
    save_state("key", updated_state)
    log.info("fetch.complete", count=len(results))
    return results
```
