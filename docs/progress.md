# Pipeline Progress

## Phase 1: Core infrastructure
- [ ] HTTP client with rate limiting and retries
- [ ] Pydantic models for Gamma API market response
- [ ] Parquet writer with date partitioning
- [ ] State persistence for incremental fetching

## Phase 2: Fetchers
- [ ] markets.py — paginated Gamma API market fetcher
- [ ] prices.py — CLOB midpoint/orderbook snapshots

## Phase 3: Pipeline orchestration
- [ ] pipeline.py — fetch → validate → store
- [ ] CLI entrypoint in scripts/run_pipeline.py
- [ ] Integration test with real API (read-only)