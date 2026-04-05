# src/polymarket/pipeline.py
from datetime import date

import structlog

from polymarket.fetchers.markets import fetch_all_markets
from polymarket.storage.parquet import write_markets

log = structlog.get_logger()


def run() -> None:
    log.info("pipeline.start")
    markets = fetch_all_markets()
    records = [m.model_dump() for m in markets]
    write_markets(records, partition_date=date.today())
    log.info("pipeline.complete", total_markets=len(records))
