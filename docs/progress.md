# Pipeline Progress

> For the full breakdown (Milestones → Epics → Stories → Tasks), see [docs/milestones/README.md](milestones/README.md).
> This file is a quick-scan summary. Mark items here when they are fully tested and merged.

---

## M1: Core Infrastructure

### E1.1 HTTP Client
- [x] `RateLimiter` token bucket (100 req/min)
- [x] `get()` with tenacity retry and structlog
- [x] Unit tests (`tests/test_client.py`)

### E1.2 Data Models
- [x] `GammaMarket` with `coerce_numeric` validator
- [x] `PricePoint` model
- [x] `TokenInfo` model
- [x] Unit tests (`tests/test_models.py`)

### E1.3 Storage Layer
- [x] `write_markets()` — Parquet, date-partitioned
- [x] `write_price_history()`
- [x] `storage/state.py` (`load_state` / `save_state`)
- [x] Unit tests (`tests/test_storage.py`)

---

## M2: Data Fetchers

### E2.1 Market Discovery
- [x] Paginated Gamma API fetch with cursor state
- [x] `GammaMarket` validation + error logging
- [x] NBA tag filter
- [x] Use generic `storage.state` module
- [x] Unit tests (`tests/test_fetchers_markets.py`)

### E2.2 Price History
- [x] `fetch_price_history()` skeleton + backfill path
- [x] Fix `PricePoint` import (model now exists)
- [x] Incremental fetch + per-token checkpoint
- [x] Unit tests (`tests/test_fetchers_price_history.py`)

### E2.3 Snapshot Prices
- [x] Implement `prices.py` (`fetch_snapshot_prices`)
- [x] Unit tests (`tests/test_fetchers_prices.py`)

---

## M3: Pipeline & Orchestration

### E3.1 Orchestrator
- [x] `pipeline.run()` fetches and writes markets
- [x] Wire price history + snapshot prices
- [x] Per-market error isolation

### E3.2 CLI
- [x] `scripts/run_pipeline.py`
- [x] `--dry-run` and `--date` flags
- [x] `main.py` delegates to `pipeline.run()`
- [x] Register `run_pipeline` as `[project.scripts]` entry point in `pyproject.toml`

### E3.3 Integration Tests
- [x] `tests/conftest.py` with shared fixtures
- [x] Full pipeline integration tests

---

## M4: Data Quality & Reliability

- [x] Row count warnings in storage
- [x] Schema version metadata
- [x] Per-page state checkpointing
- [x] Flat-line / cancelled game detection
- [x] Dead-letter file for permanent failures

---

## M5: NBA Historical Coverage

- [x] `scripts/backfill.py` — resolved markets
- [x] `scripts/verify_coverage.py` — season completeness report
