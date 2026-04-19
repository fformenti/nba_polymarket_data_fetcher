#!/usr/bin/env python3
"""Verify season coverage for resolved NBA markets.

Usage:
    uv run python scripts/verify_coverage.py --season 2025-26
"""
from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

import pyarrow.parquet as pq
import structlog

from polymarket.client import GAMMA_BASE, get
from polymarket.models import GammaMarket

log = structlog.get_logger()

PAGE_SIZE = 100
REPORTS_DIR = Path("data/reports")


def parse_season(season_str: str) -> tuple[date, date]:
    """Parse '2025-26' into (date(2025,10,1), date(2026,6,30))."""
    parts = season_str.split("-")
    if len(parts) != 2 or len(parts[0]) != 4 or len(parts[1]) != 2:
        raise ValueError(f"Season must be in YYYY-YY format, got: {season_str!r}")
    start_year = int(parts[0])
    century = (start_year // 100) * 100
    end_year = century + int(parts[1])
    return date(start_year, 10, 1), date(end_year, 6, 30)


async def fetch_markets_for_season(season_start: date, season_end: date) -> list[GammaMarket]:
    offset = 0
    season_markets: list[GammaMarket] = []

    while True:
        data = await get(
            f"{GAMMA_BASE}/markets",
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "order": "id",
                "ascending": True,
                "tag": "nba",
            },
        )
        if not data:
            break

        for item in data:
            try:
                market = GammaMarket.model_validate(item)
            except Exception as e:
                log.warning("coverage.market_validation_error", error=str(e), item_id=item.get("id"))
                continue

            if not market.end_date_iso:
                continue
            try:
                end_date = date.fromisoformat(market.end_date_iso[:10])
            except ValueError:
                continue
            if season_start <= end_date <= season_end:
                season_markets.append(market)

        offset += len(data)
        log.info("coverage.markets_page_fetched", offset=offset)

        if len(data) < PAGE_SIZE:
            break

    return season_markets


def _parquet_path(token_id: str, partition_date: date) -> Path:
    return Path(f"data/raw/price_history/token_id={token_id}/date={partition_date}/prices.parquet")


def check_is_cancelled(path: Path) -> bool:
    try:
        table = pq.read_table(str(path), columns=["is_cancelled"])
        col = table.column("is_cancelled")
        return bool(col[0].as_py())
    except Exception:
        return False


async def verify_coverage(season_str: str) -> dict:
    season_start, season_end = parse_season(season_str)
    log.info(
        "coverage.fetch_start",
        season=season_str,
        start=str(season_start),
        end=str(season_end),
    )

    markets = await fetch_markets_for_season(season_start, season_end)
    log.info("coverage.markets_fetched", count=len(markets))

    total = 0
    with_data = 0
    cancelled = 0
    missing_markets: list[dict] = []

    for market in markets:
        if not market.clob_token_ids:
            log.warning("coverage.no_tokens", market_id=market.id, market_slug=market.slug)
            continue

        total += 1
        token_id = market.clob_token_ids[0]
        partition_date = date.fromisoformat(market.end_date_iso[:10])  # type: ignore[index]
        path = _parquet_path(token_id, partition_date)

        if path.exists():
            with_data += 1
            if check_is_cancelled(path):
                cancelled += 1
        else:
            log.warning(
                "coverage.missing_game",
                slug=market.slug,
                end_date=str(partition_date),
                token_id=token_id,
            )
            missing_markets.append(
                {"slug": market.slug, "end_date": str(partition_date), "token_id": token_id}
            )

    games_missing = total - with_data
    summary: dict = {
        "season": season_str,
        "total_games": total,
        "games_with_data": with_data,
        "games_missing": games_missing,
        "cancelled_games": cancelled,
        "missing": missing_markets,
    }

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    report_path = REPORTS_DIR / f"coverage_{season_str}.json"
    report_path.write_text(json.dumps(summary, indent=2))
    log.info("coverage.report_saved", path=str(report_path))

    log.info(
        "coverage.complete",
        total_games=total,
        games_with_data=with_data,
        games_missing=games_missing,
        cancelled_games=cancelled,
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify NBA season data coverage.")
    parser.add_argument(
        "--season",
        required=True,
        metavar="YYYY-YY",
        help="NBA season string, e.g. 2025-26",
    )
    args = parser.parse_args()
    from polymarket import client

    async def _run() -> None:
        try:
            await verify_coverage(args.season)
        finally:
            await client.aclose()

    try:
        asyncio.run(_run())
    except ValueError as e:
        parser.error(str(e))


if __name__ == "__main__":
    main()
