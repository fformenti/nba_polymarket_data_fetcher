# M5: Full NBA Historical Coverage

**Goal:** Complete price history for all resolved NBA games on Polymarket, with a coverage verification tool to confirm completeness before using the dataset for ML.

**Status:** ✅ Complete

**Blocked by:** M4 complete

---

## E5.1 Historical Backfill

> Story: As a ML engineer, I need price history for all past NBA games (not just today's active markets) so I have a full training dataset.

- [x] `scripts/backfill.py`: discover all **resolved** NBA markets from Gamma API — `scripts/backfill.py`
  - Query: `GET /markets?tag=nba&closed=true`
  - Paginate through all results
  - For each market, extract all `clobTokenIds`
- [x] Backfill runner: call `fetch_price_history(token_id)` for each resolved token — `scripts/backfill.py`
  - Use `interval=max` (full history, ignores incremental state)
  - Write to date-partitioned Parquet using game `endDateIso` as the partition date
  - Log progress: `{market_slug, token_count, price_point_count}` per market
- [x] Skip already-backfilled tokens: check if Parquet file for that token's date already exists before fetching — `scripts/backfill.py`
- [x] Handle postponed/cancelled games via `is_cancelled` flag (from M4 flat-line detection) — `src/polymarket/fetchers/price_history.py`

---

## E5.2 Season Coverage Verification

> Story: As a ML engineer, I need to verify that I have data for every game in a given NBA season so I can confidently use the dataset for training.

- [x] `scripts/verify_coverage.py`: list all NBA markets by season tag — `scripts/verify_coverage.py`
  - Accept `--season 2025-26` CLI argument
  - Query Gamma API for markets tagged with that season
- [x] Coverage report: for each game in the season, check if a Parquet file exists for that date and token — `scripts/verify_coverage.py`
  - Output: `{total_games, games_with_data, games_missing, cancelled_games}`
- [x] Export summary to `data/reports/coverage_{season}.json` — `scripts/verify_coverage.py`
- [x] Print games with missing data to stdout so gaps are immediately visible — `scripts/verify_coverage.py`

---

## Completion Criteria

M5 is complete when:
- `uv run python scripts/backfill.py` runs without error and writes Parquet files for all resolved NBA markets
- `uv run python scripts/verify_coverage.py --season 2025-26` prints a coverage report showing ≥ 95% of games have data
- No test regressions: `uv run pytest` still passes
- `data/raw/price_history/` contains partitions spanning the full season date range
