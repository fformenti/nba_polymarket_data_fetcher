# Polymarket Data Pipeline — Project Memory

## What this project does
Fetches and stores data from the Polymarket prediction markets API for use
as ML training/test data. Data ingestion and storage ONLY. No ML, no modeling,
no trading, no order placement.

## Tech stack
- Python 3.12, managed with `uv` (never use pip directly)
- `httpx` for async HTTP — not `requests`
- `pydantic` v2 for all schema validation
- `structlog` for structured logging — not `logging` or print()
- Storage: Parquet (partitioned by date) via `pyarrow`
- State tracking: JSON files in `data/state/`

## Polymarket APIs used
- **Gamma API** (`https://gamma-api.polymarket.com`) — market discovery, no auth needed
- **CLOB API** (`https://clob.polymarket.com`) — order books, prices, no auth for reads

## Data types collected
1. Market metadata (Gamma API) — what markets exist, resolved/active status
2. Price history per YES token (CLOB /prices-history) — win probability over time
   - Initial backfill: interval=max for all existing markets (one-time script)
   - Incremental: startTs from last fetched timestamp per token

## Key conventions
- All fetchers must be idempotent — safe to re-run without duplicating data
- Pagination uses cursor-based state persisted to `data/state/{fetcher}.json`
- Rate limiting: max 100 req/min — enforced in `client.py` with token bucket
- Retry logic lives in `client.py` via `tenacity` — don't add retries elsewhere
- Log every fetch, every schema error, every skipped record via `structlog`

## Folder map
- `src/polymarket/client.py` — base HTTP client (rate limiting, retries, logging)
- `src/polymarket/models.py` — all Pydantic models
- `src/polymarket/fetchers/` — one file per data type (markets, prices)
- `src/polymarket/storage/` — Parquet writer and state persistence
- `src/polymarket/pipeline.py` — orchestrator, call this to run a full cycle
- `docs/progress.md` — task checklist; update it as work completes

## Run commands
- `uv run python scripts/run_pipeline.py` — run full pipeline
- `uv run pytest` — run tests
- `uv run ruff check src/` — lint

## Out of scope — do not build
- ML models, feature engineering, training loops
- Order placement or wallet integration
- A web UI or API server
- Any real-time streaming (WebSocket) until REST pipeline is stable