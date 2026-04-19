from __future__ import annotations

import structlog

from polymarket.client import GAMMA_BASE, get
from polymarket.models import GammaMarket
from polymarket.storage.state import load_state, save_state

log = structlog.get_logger()
PAGE_SIZE = 100


async def fetch_all_markets() -> list[GammaMarket]:
    offset = load_state("markets").get("offset", 0)
    all_markets = []

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

        valid = []
        for item in data:
            try:
                valid.append(GammaMarket.model_validate(item))
            except Exception as e:
                log.warning("market.validation_error", error=str(e), item_id=item.get("id"))

        all_markets.extend(valid)
        offset += len(data)
        save_state("markets", {"offset": offset})
        log.info("markets.page_fetched", count=len(valid), offset=offset)

        if len(data) < PAGE_SIZE:
            break  # Last page

    return all_markets
