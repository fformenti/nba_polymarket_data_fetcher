#!/usr/bin/env python3
"""Run the NBA Polymarket data pipeline.

Usage:
    uv run python scripts/run_pipeline.py [--dry-run] [--date YYYY-MM-DD]
"""
from __future__ import annotations

import argparse
import asyncio
import pathlib
import sys
from datetime import date

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))

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


if __name__ == "__main__":
    main()
