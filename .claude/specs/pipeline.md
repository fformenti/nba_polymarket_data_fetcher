# Spec: Pipeline Orchestrator (`src/polymarket/pipeline.py`)

## Purpose
Top-level coordinator. Calls fetchers in the correct order, passes data to storage, isolates per-market errors. No business logic lives here — only orchestration.

## `run(dry_run: bool = False, date: str | None = None) → None`

```python
async def run(dry_run: bool = False, date: str | None = None) -> None: ...
```

`date` defaults to today (`datetime.date.today().isoformat()`) if not provided.

### Execution Order

```
Step 1: fetch_all_markets()
         → list[GammaMarket]

Step 2: [if not dry_run] write_markets(records, partition_date=date)

Step 3: for each market in markets:
           for each token_id in market.clob_token_ids:
               try:
                   start_ts = load last checkpoint for token_id
                   points = await fetch_price_history(token_id, start_ts)
                   if not dry_run:
                       write_price_history(points_as_dicts, partition_date=date)
               except Exception as e:
                   log.warning("pipeline.market_error", market_slug=market.slug, error=str(e))
                   continue  ← NEVER abort the loop on a single failure

Step 4: active_token_ids = [t for m in markets if m.active for t in m.clob_token_ids]
        prices = await fetch_snapshot_prices(active_token_ids)
        if not dry_run:
            write_snapshot_prices(prices_as_records, partition_date=date)

Step 5: log.info("pipeline.complete",
            markets=len(markets),
            active_tokens=len(active_token_ids),
            dry_run=dry_run)
```

### Error Isolation Rules
- Failure in a single market's price fetch → log + `continue`. Never propagate.
- Failure in `fetch_all_markets()` → re-raise. Can't continue without the market list.
- Failure in `write_*()` → re-raise. Storage failure is a pipeline-level problem.
- `dry_run=True` → skip all `write_*()` calls. Fetches still run (validates API connectivity).

## Entry Point

`pipeline.run()` is called from:
- `scripts/run_pipeline.py` (CLI)
- `main.py` (convenience wrapper)

Both use `asyncio.run(run(...))`.
