# M4: Data Quality & Reliability

**Goal:** The pipeline surfaces data quality issues via logs and metrics, recovers from partial failures without losing progress, and correctly handles edge cases like postponed games.

**Status:** ✅ Complete

**Blocked by:** M3 complete

---

## E4.1 Validation & Monitoring

> Story: As a data consumer, I need to know when data quality degrades (empty fetches, schema drift, cancelled games) without reading raw logs.

- [x] Row count warning: `log.warning("storage.empty_write", ...)` when writing 0 records — `src/polymarket/storage/parquet.py`
- [x] Schema version in Parquet metadata: `{"schema_version": "1"}` on every write — `src/polymarket/storage/parquet.py`
- [x] Pipeline run summary log: `{markets, active_tokens, total_price_points, skipped_markets}` at `INFO` level — `src/polymarket/pipeline.py`
- [x] Flat-line detection: if all `p` values in a price history are `0.50 ± 0.01` for ≥ 2 hours, tag the series as `is_cancelled=True` in the output records — `src/polymarket/fetchers/price_history.py`

---

## E4.2 Error Recovery

> Story: As an operator, I need the pipeline to pick up where it left off after a crash, not restart from scratch.

- [x] Checkpoint state after each page (not just at end of run): `save_state(key, state)` inside the pagination loop — `src/polymarket/fetchers/markets.py`
- [x] Checkpoint price history state after each token (not just at end of all tokens) — `src/polymarket/fetchers/price_history.py`
- [x] Dead-letter file for permanently failed tokens: write `{token_id, url, error, timestamp}` to `data/state/dead_letter.jsonl` — `src/polymarket/client.py`
- [x] Unit tests: verify state is saved after each page even if a later page fails — `tests/test_fetchers_markets.py`

---

## Completion Criteria

M4 is complete when:
- All `- [ ]` tasks above are checked
- A deliberate mid-run crash (keyboard interrupt after page 2 of 5) resumes from page 3 on next run
- A token with 100% flat-line history is written with `is_cancelled=True` and logged at `WARNING`
- `uv run pytest` still passes after all changes
