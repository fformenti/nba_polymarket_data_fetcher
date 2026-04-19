#!/usr/bin/env python3
"""Fetch Polymarket market data for specific NBA games by slug.

Uses the pre-built slug lookup table (`data/raw/games_slug*.csv`) to drive
parallel per-slug fetches. Persists one parquet per slug under
`data/raw/markets_by_slug/` and `data/raw/price_history_by_slug/`, plus a
human-readable JSON mirror under `data/debug/markets_by_slug/`.

Usage:
    uv run python scripts/fetch_by_slug.py --date 2026-02-19
    uv run python scripts/fetch_by_slug.py --start 2026-02-19 --end 2026-02-21 --concurrency 8
"""
from __future__ import annotations

import argparse
import asyncio
import pathlib
import sys
from datetime import date

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

import structlog

from polymarket import client
from polymarket.fetchers.by_slug import fetch_games_batch
from polymarket.lookup import _date_from_slug, slugs_for_date, slugs_for_range
from polymarket.storage.debug_json import write_debug_json
from polymarket.storage.parquet import (
    is_closed_market_cached,
    write_market_by_slug,
    write_price_history_by_slug,
)

log = structlog.get_logger()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Polymarket data by game slug.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", type=date.fromisoformat, metavar="YYYY-MM-DD")
    group.add_argument("--start", type=date.fromisoformat, metavar="YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, metavar="YYYY-MM-DD")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not write parquet or JSON files to disk.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch slugs even if a closed-market parquet already exists.",
    )
    args = parser.parse_args()
    if args.start is not None and args.end is None:
        parser.error("--start requires --end")
    return args


def _resolve_slugs(args: argparse.Namespace) -> list[str]:
    if args.date is not None:
        return slugs_for_date(args.date)
    return slugs_for_range(args.start, args.end)


def _partition_cached(slugs: list[str]) -> tuple[list[str], list[str]]:
    """Split slugs into (to_fetch, cached_closed) based on existing parquet files."""
    to_fetch: list[str] = []
    cached: list[str] = []
    for slug in slugs:
        game_date = _date_from_slug(slug)
        if game_date is not None and is_closed_market_cached(slug, game_date):
            cached.append(slug)
        else:
            to_fetch.append(slug)
    return to_fetch, cached


async def _run(args: argparse.Namespace) -> int:
    slugs = _resolve_slugs(args)
    if not slugs:
        log.warning("fetch_by_slug.no_slugs_found")
        return 0

    if args.force:
        to_fetch, cached = slugs, []
    else:
        to_fetch, cached = _partition_cached(slugs)

    log.info(
        "fetch_by_slug.start",
        count=len(slugs),
        to_fetch=len(to_fetch),
        cached=len(cached),
        concurrency=args.concurrency,
    )
    if cached:
        log.info("fetch_by_slug.skip_cached", count=len(cached))
    if not to_fetch:
        log.info("fetch_by_slug.all_cached", count=len(slugs))
        return 0

    try:
        results = await fetch_games_batch(to_fetch, concurrency=args.concurrency)
    finally:
        await client.aclose()

    if args.dry_run:
        log.info(
            "fetch_by_slug.dry_run",
            fetched=len(results),
            failed=len(to_fetch) - len(results),
        )
        return len(results)

    for result in results:
        try:
            write_market_by_slug(result)
            write_price_history_by_slug(result)
            write_debug_json(result)
        except Exception as e:
            log.error("fetch_by_slug.write_error", slug=result.slug, error=str(e))

    log.info(
        "fetch_by_slug.complete",
        requested=len(slugs),
        fetched=len(results),
        failed=len(to_fetch) - len(results),
        cached=len(cached),
    )
    return len(results)


def main() -> None:
    args = _parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
