# Coding Conventions

Rules that apply to every file in `src/polymarket/`. Deviations require an explicit comment explaining why.

## HTTP
- Use `httpx` for all HTTP. Never use `requests` or `urllib`.
- All HTTP calls go through `client.get()` — never instantiate `httpx.Client` directly in fetchers.
- Async only: `async def`, `await`, `asyncio.run()` at the top level.

## Logging
- Use `structlog` exclusively. Never use `logging`, `print()`, or `warnings`.
- Bind context at call site: `log = structlog.get_logger().bind(token_id=token_id)`.
- Log every fetch start, every schema validation error, every skipped record, every page completion.

## Data Validation
- All external API responses must be validated through a Pydantic v2 model before use.
- On `ValidationError`, log the error and the raw payload, then skip the record — never raise.

## Fetchers
- All fetcher functions must be **idempotent**: re-running must not duplicate data.
- State (cursor/offset/last_ts) is always read at the start and written after each successful page.
- State lives in `data/state/{key}.json` via `storage.state.load_state` / `save_state`.
- No retry logic in fetchers — retries belong exclusively in `client.py` via `tenacity`.

## Storage
- Parquet only. No CSV, no JSON dumps to `data/raw/`.
- Partitioned by date: `data/raw/{dataset}/date={YYYY-MM-DD}/data.parquet`.
- Compression: snappy.

## Dependency Management
- Use `uv` only. Never invoke `pip`, `pip3`, or `poetry`.
- Add packages: `uv add <pkg>`. Dev packages: `uv add --dev <pkg>`.

## Type Annotations
- All function signatures must have full type annotations (mypy strict mode is enabled).
- Use `from __future__ import annotations` in every module.
