#!/usr/bin/env python3
"""Backfill price history for NBA markets ending on a specific date.

Usage:
    uv run python scripts/backfill_single_date.py 2022-08-18 [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
import pathlib
import sys
from datetime import date

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

import structlog

from polymarket.client import GAMMA_BASE, get
from polymarket.fetchers.price_history import fetch_price_history
from polymarket.models import GammaMarket, detect_cancelled
from polymarket.storage.parquet import write_price_history
from polymarket.storage.state import append_dead_letter

log = structlog.get_logger()

PAGE_SIZE = 500


async def fetch_markets_by_end_date(target_date: date) -> list[GammaMarket]:
    """Fetch closed NBA markets matching target_date, sorted by endDateIso for early exit."""
    all_markets: list[GammaMarket] = []
    offset = 0
    target_str = target_date.isoformat()  # "2022-10-18"

    while True:
        data = await get(
            f"{GAMMA_BASE}/markets",
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "order": "endDateIso",
                "ascending": True,
                "tag": "nba",
                "closed": "true",
                "endDateMin": target_str,
                "endDateMax": target_str,
            },
        )
        if not data:
            break

        # page_all_past: True when every *dated* record in this page is past target_date.
        # Null-dated records are skipped — they don't prevent early exit.
        page_all_past = True
        page_has_any_dated_record = False
        for item in data:
            end_date = item.get("endDateIso", "") or ""
            end_date_short = end_date[:10] if end_date else ""

            if not end_date_short:
                continue  # null date — skip for early-exit accounting

            page_has_any_dated_record = True
            if end_date_short <= target_str:
                page_all_past = False

            if end_date_short != target_str:
                continue  # wrong date — skip validation cost entirely

            try:
                market = GammaMarket.model_validate(item)
                all_markets.append(market)
            except Exception as e:
                log.warning("backfill.market_validation_error", error=str(e), item_id=item.get("id"))

        offset += len(data)
        log.info("backfill.page_fetched", offset=offset, matches_so_far=len(all_markets))

        if len(data) < PAGE_SIZE:
            break

        if page_all_past and page_has_any_dated_record:
            log.info("backfill.early_exit", offset=offset, target_date=target_str)
            break

    return all_markets


async def backfill_by_date(target_date: date, *, dry_run: bool = False) -> None:
    """Backfill price history for all markets ending on target_date."""
    log.info("backfill.start", target_date=str(target_date), dry_run=dry_run)

    markets = await fetch_markets_by_end_date(target_date)
    log.info("backfill.markets_found", count=len(markets), target_date=str(target_date))

    if not markets:
        log.warning("backfill.no_markets_found", target_date=str(target_date))
        return

    total_tokens = 0
    total_price_points = 0

    for market in markets:
        token_ids = market.clob_token_ids
        if not token_ids:
            log.warning("backfill.no_tokens", market_id=market.id, market_slug=market.slug)
            continue

        log.info("backfill.market_start", market_slug=market.slug, token_count=len(token_ids))

        for token_id in token_ids:
            try:
                points = await fetch_price_history(token_id, commit_checkpoint=None)
                is_cancelled = detect_cancelled(points)
                log.info(
                    "backfill.token_done",
                    token_id=token_id,
                    price_point_count=len(points),
                    is_cancelled=is_cancelled,
                )

                if not dry_run and points:
                    records = [p.model_dump() for p in points]
                    for r in records:
                        r["is_cancelled"] = is_cancelled
                    write_price_history(records, token_id=token_id, partition_date=target_date)

                total_tokens += 1
                total_price_points += len(points)

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

        log.info("backfill.market_done", market_slug=market.slug)

    log.info(
        "backfill.complete",
        target_date=str(target_date),
        total_markets=len(markets),
        total_tokens=total_tokens,
        total_price_points=total_price_points,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill NBA market price history for a specific date.")
    parser.add_argument("date", help="Target end date (YYYY-MM-DD format, e.g., 2022-08-18)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not write anything to disk.",
    )
    args = parser.parse_args()

    try:
        target_date = date.fromisoformat(args.date)
    except ValueError:
        print(f"Error: Invalid date format '{args.date}'. Use YYYY-MM-DD format.")
        sys.exit(1)

    from polymarket import client

    async def _run() -> None:
        try:
            await backfill_by_date(target_date, dry_run=args.dry_run)
        finally:
            await client.aclose()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
