# M3: Pipeline & Orchestration

**Goal:** A fully wired pipeline that an operator can run from the command line, fetching all data types in the correct order, surviving per-market failures, and supporting dry-run mode.

**Status:** ✅ Complete

**Blocked by:** M2 complete (all fetchers working)

---

## E3.1 Pipeline Orchestrator

> Story: As a data pipeline, I need a single `run()` function that coordinates all fetchers and writes all data types so operators don't need to invoke fetchers individually.

- [x] `pipeline.run()` fetches markets and writes to Parquet — `src/polymarket/pipeline.py`
- [x] Wire `fetch_price_history()` for each token in each market — `src/polymarket/pipeline.py`
- [x] Wire `fetch_snapshot_prices()` for all active token IDs — `src/polymarket/pipeline.py`
- [x] Per-market error isolation: `try/except Exception` per token, log + `continue` — `src/polymarket/pipeline.py`
- [x] Log summary at completion: total markets, active tokens, price points fetched — `src/polymarket/pipeline.py`

---

## E3.2 CLI Entrypoint

> Story: As an operator, I need a runnable CLI script to trigger pipeline runs, with flags for dry-run and targeted backfill by date.

- [x] 🚨 **BLOCKER** — Create `scripts/run_pipeline.py` — `scripts/run_pipeline.py`
  - `argparse` with `--dry-run` (bool flag) and `--date YYYY-MM-DD` (optional)
  - Calls `asyncio.run(pipeline.run(dry_run=..., date=...))`
  - Exits with code 1 on unhandled exception
- [x] Update `main.py` to delegate to `pipeline.run()` — `main.py`
- [x] Register `run_pipeline` as a `[project.scripts]` entry point in `pyproject.toml` — `pyproject.toml`

---

## E3.3 Integration Tests

> Story: As a developer, I need end-to-end tests that exercise the full pipeline path without real network calls so I can verify correctness before each release.

- [x] Create `tests/conftest.py` with shared fixtures (mock client, `tmp_path` for Parquet) — `tests/conftest.py`
- [x] Integration test: markets fetch → GammaMarket validation → Parquet write → verify schema — `tests/test_integration.py`
- [x] Integration test: price history fetch → PricePoint validation → Parquet write — `tests/test_integration.py`
- [x] Integration test: full `pipeline.run(dry_run=True)` — no files written, no exceptions — `tests/test_integration.py`
- [x] Integration test: `pipeline.run()` with one market raising an exception — other markets still written — `tests/test_integration.py`

---

## Completion Criteria

M3 is complete when:
- All `- [ ]` tasks above are checked
- `uv run python scripts/run_pipeline.py --dry-run` runs without error and logs a completion summary
- `uv run pytest tests/test_integration.py` passes
- `uv run ruff check src/ scripts/` passes
