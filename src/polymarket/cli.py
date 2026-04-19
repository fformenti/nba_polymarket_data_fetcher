from __future__ import annotations

import argparse
import asyncio
from datetime import date

from polymarket import pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NBA Polymarket data pipeline.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch data but do not write anything to disk.",
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        metavar="YYYY-MM-DD",
        help="Partition date for storage (defaults to today).",
    )
    args = parser.parse_args()
    asyncio.run(pipeline.main(dry_run=args.dry_run, date=args.date))
