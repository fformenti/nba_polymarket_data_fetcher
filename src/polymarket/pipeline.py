from __future__ import annotations

import datetime
from collections.abc import Callable
from datetime import date

import structlog

from polymarket.fetchers.markets import fetch_all_markets
from polymarket.fetchers.price_history import STATE_KEY as PH_STATE_KEY
from polymarket.fetchers.price_history import fetch_price_history
from polymarket.fetchers.prices import fetch_snapshot_prices
from polymarket.models import detect_cancelled
from polymarket.storage.parquet import write_markets, write_price_history
from polymarket.storage.state import append_dead_letter, load_state, save_state

log = structlog.get_logger()


def _make_checkpoint(token_id: str) -> Callable[[int], None]:
    def _commit(max_ts: int) -> None:
        state = load_state(PH_STATE_KEY)
        state[token_id] = max_ts
        save_state(PH_STATE_KEY, state)

    return _commit


async def run(*, dry_run: bool = False, date: date | None = None) -> None:
    partition_date = date if date is not None else datetime.date.today()
    log.info("pipeline.start", dry_run=dry_run, date=str(partition_date))
    if dry_run:
        log.info("pipeline.dry_run", note="writes disabled")

    # --- Markets ---
    markets = await fetch_all_markets()
    if not dry_run:
        write_markets([m.model_dump() for m in markets], partition_date=partition_date)
    log.info("pipeline.markets_done", count=len(markets))

    # --- Price history + snapshot prices (per market, isolated) ---
    # YES token is always clobTokenIds[0]
    all_token_ids: list[str] = []
    for market in markets:
        if market.clob_token_ids:
            all_token_ids.append(market.clob_token_ids[0])

    for market in markets:
        if not market.clob_token_ids:
            log.warning("pipeline.no_tokens", market_id=market.id)
            continue

        token_id = market.clob_token_ids[0]
        try:
            checkpoint = _make_checkpoint(token_id) if not dry_run else None
            points = await fetch_price_history(token_id, commit_checkpoint=checkpoint)
            if points:
                is_cancelled = detect_cancelled(points)
                if is_cancelled:
                    log.warning(
                        "pipeline.cancelled_market_detected",
                        token_id=token_id,
                        market_id=market.id,
                    )
                if not dry_run:
                    records = [p.model_dump() for p in points]
                    for r in records:
                        r["is_cancelled"] = is_cancelled
                    write_price_history(
                        records,
                        token_id=token_id,
                        partition_date=partition_date,
                    )
        except Exception as e:
            log.error(
                "pipeline.price_history_error",
                market_id=market.id,
                token_id=token_id,
                error=str(e),
            )
            append_dead_letter(
                token_id=token_id,
                error=str(e),
                context={"market_id": market.id, "market_slug": market.slug},
            )

    # Snapshot prices for all YES tokens in one pass
    try:
        snapshot = await fetch_snapshot_prices(all_token_ids)
        log.info("pipeline.snapshot_done", count=len(snapshot))
    except Exception as e:
        log.error("pipeline.snapshot_error", error=str(e))

    log.info("pipeline.complete", total_markets=len(markets))


async def main(*, dry_run: bool = False, date: date | None = None) -> None:
    from polymarket import client

    try:
        await run(dry_run=dry_run, date=date)
    finally:
        await client.aclose()
