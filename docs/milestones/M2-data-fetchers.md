# M2: Data Fetchers

**Goal:** Three working fetchers — market discovery, price history, and snapshot prices — each idempotent, paginated where needed, and persisting their state for incremental runs.

**Status:** ✅ Complete

**Blocked by:** M1/E1.2 (`PricePoint` model), M1/E1.3 (`storage/state.py`)

---

## E2.1 Market Discovery

> Story: As a data pipeline, I need to discover and persist all active NBA markets so I know which tokens to fetch prices for.

- [x] Paginated fetch from Gamma API `/markets` — `src/polymarket/fetchers/markets.py`
- [x] Cursor-based state persistence (offset in JSON) — `src/polymarket/fetchers/markets.py`
- [x] `GammaMarket` validation with error logging on `ValidationError` — `src/polymarket/fetchers/markets.py`
- [x] Filter markets to NBA only (`tag=nba` query param or response field check) — `src/polymarket/fetchers/markets.py`
- [x] Migrate state calls to use `storage.state.load_state` / `save_state` (after M1/E1.3) — `src/polymarket/fetchers/markets.py`
- [x] Unit tests: happy path, validation error skips, pagination across 3 pages — `tests/test_fetchers_markets.py`

---

## E2.2 Price History

> Story: As a data pipeline, I need to fetch the full time-series of win probabilities for each YES token, resuming from where I left off on each run.

- [x] `fetch_price_history(token_id, start_ts=None)` skeleton — `src/polymarket/fetchers/price_history.py`
- [x] Initial backfill path (`interval=max`, no `startTs`) — `src/polymarket/fetchers/price_history.py`
- [x] 🚨 **BLOCKER** — Fix `PricePoint` import (model not yet defined in `models.py`) — `src/polymarket/models.py`
- [x] Incremental fetch path (`startTs={last_ts}` from state) — `src/polymarket/fetchers/price_history.py`
- [x] Per-token checkpoint: save `max(point.t)` to `data/state/price_history.json` after each token — `src/polymarket/fetchers/price_history.py`
- [x] Unit tests: first-fetch (interval=max), incremental (startTs provided), empty history — `tests/test_fetchers_price_history.py`

---

## E2.3 Active Prices (Snapshot)

> Story: As a data pipeline, I need a snapshot of current midpoint prices for all active tokens to capture the "right now" win probability.

- [x] Implement `fetch_snapshot_prices(token_ids) → dict[str, float]` — `src/polymarket/fetchers/prices.py`
  - Calls `GET /midpoint?token_id={id}` for each token
  - Parses `response["mid"]` as float
  - Skips on request failure (log + continue)
- [x] Unit tests: multiple tokens, one failure mid-list — `tests/test_fetchers_prices.py`

---

## Completion Criteria

M2 is complete when:
- All `- [ ]` tasks above are checked
- `uv run pytest tests/test_fetchers_*.py` passes
- A manual dry-run (`uv run python scripts/run_pipeline.py --dry-run`) logs fetched market count and price point count with no exceptions
