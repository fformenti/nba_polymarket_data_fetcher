#!/usr/bin/env python3
"""Backfill full price history for all resolved NBA markets.

Usage:
    uv run python scripts/backfill.py [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import pathlib
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

import structlog

from polymarket.client import GAMMA_BASE, get
from polymarket.fetchers.price_history import fetch_price_history
from polymarket.models import GammaMarket, detect_cancelled
from polymarket.storage.parquet import write_price_history
from polymarket.storage.state import append_dead_letter, load_state, save_state

log = structlog.get_logger()

BACKFILL_MARKETS_KEY = "backfill_markets"
BACKFILL_DONE_KEY = "backfill_done"
PAGE_SIZE = 100


async def fetch_closed_markets() -> list[GammaMarket]:
    offset = load_state(BACKFILL_MARKETS_KEY).get("offset", 0)
    all_markets: list[GammaMarket] = []

    while True:
        data = await get(
            f"{GAMMA_BASE}/markets",
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "order": "id",
                "ascending": True,
                "tag": "nba",
                "closed": "true",
            },
        )
        if not data:
            break

        valid: list[GammaMarket] = []
        for item in data:
            try:
                valid.append(GammaMarket.model_validate(item))
            except Exception as e:
                log.warning("backfill.market_validation_error", error=str(e), item_id=item.get("id"))

        all_markets.extend(valid)
        offset += len(data)
        save_state(BACKFILL_MARKETS_KEY, {"offset": offset})
        log.info("backfill.markets_page_fetched", count=len(valid), offset=offset)

        if len(data) < PAGE_SIZE:
            break

    return all_markets


def _partition_date_for(market: GammaMarket) -> date:
    if market.end_date_iso:
        try:
            return date.fromisoformat(market.end_date_iso[:10])
        except ValueError:
            log.warning("backfill.missing_end_date", market_slug=market.slug, end_date_iso=market.end_date_iso)
    else:
        log.warning("backfill.missing_end_date", market_slug=market.slug, end_date_iso=None)
    return datetime.date.today()


def _parquet_path(token_id: str, partition_date: date) -> Path:
    return Path(f"data/raw/price_history/token_id={token_id}/date={partition_date}/prices.parquet")


def _is_done(token_id: str, done_set: set[str], partition_date: date) -> bool:
    return token_id in done_set or _parquet_path(token_id, partition_date).exists()


async def backfill(*, dry_run: bool = False) -> None:
    log.info("backfill.start", dry_run=dry_run)

    done_state = load_state(BACKFILL_DONE_KEY)
    done_set: set[str] = set(done_state.get("done", []))

    markets = await fetch_closed_markets()
    log.info("backfill.markets_fetched", count=len(markets))

    for market in markets:
        token_ids = market.clob_token_ids
        if not token_ids:
            log.warning("backfill.no_tokens", market_id=market.id, market_slug=market.slug)
            continue

        partition_date = _partition_date_for(market)
        log.info("backfill.market_start", market_slug=market.slug, token_count=len(token_ids))

        market_price_points = 0
        for token_id in token_ids:
            if _is_done(token_id, done_set, partition_date):
                log.info("backfill.token_skipped", token_id=token_id, market_slug=market.slug)
                continue

            try:
                points = await fetch_price_history(token_id, commit_checkpoint=None)
                is_cancelled = detect_cancelled(points)
                log.info(
                    "backfill.token_done",
                    token_id=token_id,
                    price_point_count=len(points),
                    is_cancelled=is_cancelled,
                )

                if not dry_run:
                    if points:
                        records = [p.model_dump() for p in points]
                        for r in records:
                            r["is_cancelled"] = is_cancelled
                        write_price_history(records, token_id=token_id, partition_date=partition_date)

                    done_set.add(token_id)
                    save_state(BACKFILL_DONE_KEY, {"done": list(done_set)})

                market_price_points += len(points)

            except Exception as e:
                log.error(
                    "backfill.token_error",
                    token_id=token_id,
                    market_slug=market.slug,
                    error=str(e),
                )
                append_dead_letter(
                    token_id=token_id,
                    error=str(e),
                    context={"market_id": market.id, "market_slug": market.slug},
                )

        log.info(
            "backfill.market_done",
            market_slug=market.slug,
            total_price_points=market_price_points,
        )

    log.info("backfill.complete", total_markets=len(markets))


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill resolved NBA market price history.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not write anything to disk.",
    )
    args = parser.parse_args()

    from polymarket import client

    async def _run() -> None:
        try:
            await backfill(dry_run=args.dry_run)
        finally:
            await client.aclose()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
