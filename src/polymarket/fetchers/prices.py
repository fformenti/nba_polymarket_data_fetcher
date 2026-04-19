from __future__ import annotations

import structlog

from polymarket.client import CLOB_BASE, get

log = structlog.get_logger()


async def fetch_snapshot_prices(token_ids: list[str]) -> dict[str, float]:
    log.info("snapshot_prices.fetch_start", count=len(token_ids))
    prices: dict[str, float] = {}

    for token_id in token_ids:
        try:
            data = await get(f"{CLOB_BASE}/midpoint", params={"token_id": token_id})
            prices[token_id] = float(data["mid"])
        except Exception as e:
            log.warning("snapshot_prices.fetch_error", token_id=token_id, error=str(e))

    log.info("snapshot_prices.fetch_complete", count=len(prices))
    return prices
