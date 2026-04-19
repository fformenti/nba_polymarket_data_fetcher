# M1: Core Infrastructure

**Goal:** A working foundation — async HTTP client, Pydantic models, Parquet storage, and state persistence — that all fetchers can build on.

**Status:** ✅ Complete

---

## E1.1 HTTP Client

> Story: As a developer, I need a resilient async HTTP client with rate limiting and retries so all fetchers share a single, consistent network layer.

- [x] `RateLimiter` token bucket class (100 req/min) — `src/polymarket/client.py`
- [x] `get(url, params)` with tenacity retry (3 attempts, exp backoff 1–10s) — `src/polymarket/client.py`
- [x] `structlog` integration (fetch.start, fetch.ok, fetch.error) — `src/polymarket/client.py`
- [x] Unit tests for `RateLimiter` (token acquisition, blocking behavior) — `tests/test_client.py`
- [x] Unit tests for `get()` (happy path, retry on 5xx, fail after 3 retries) — `tests/test_client.py`

---

## E1.2 Data Models

> Story: As a developer, I need Pydantic v2 models for all API response shapes so raw API dicts never escape into fetcher logic.

- [x] `GammaMarket` model with `coerce_numeric` validator — `src/polymarket/models.py`
- [x] 🚨 **BLOCKER** — `PricePoint` model (`t: int`, `p: float`) — `src/polymarket/models.py`
- [x] `TokenInfo` model (`token_id`, `outcome`, `team_name`, `market_slug`, `condition_id`) — `src/polymarket/models.py`
- [x] Unit tests for `GammaMarket` validator (string → float coercion, None handling) — `tests/test_models.py`
- [x] Unit tests for `PricePoint` (valid, out-of-range `p`, missing fields) — `tests/test_models.py`

---

## E1.3 Storage Layer

> Story: As a developer, I need date-partitioned Parquet storage and generic state persistence so all fetchers can save and resume.

**Parquet Writer**
- [x] `write_markets(records, partition_date)` — `src/polymarket/storage/parquet.py`
- [x] `write_price_history(records, partition_date)` — `src/polymarket/storage/parquet.py`
- [x] Add `schema_version` metadata field to all writes — `src/polymarket/storage/parquet.py`
- [x] Row count warning when writing 0 records — `src/polymarket/storage/parquet.py`

**State Persistence**
- [x] Inline state logic in `markets.py` (works but not reusable)
- [x] 🚨 **BLOCKER** — `load_state(key) → dict` and `save_state(key, data)` — `src/polymarket/storage/state.py`
- [x] Migrate `markets.py` to use generic `load_state` / `save_state` — `src/polymarket/fetchers/markets.py`
- [x] Unit tests for `load_state` (missing file → empty dict) and `save_state` (round-trip) — `tests/test_storage.py`

---

## Completion Criteria

M1 is complete when:
- All `- [ ]` tasks above are checked
- `uv run pytest tests/test_client.py tests/test_models.py tests/test_storage.py` passes
- `uv run ruff check src/` reports no errors
