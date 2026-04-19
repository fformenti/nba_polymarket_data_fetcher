"""Reconstruct per-token price history from the Data API /trades endpoint.

The CLOB /prices-history endpoint returns an empty array for resolved
(closed) markets, so for backfill of finished games we fall back to the
trade-level history exposed by data-api.polymarket.com/trades. Each trade
carries a `timestamp` and a `price` and is keyed to the token it belongs
to via the `asset` field, which lets us rebuild a PricePoint series per
token from a single market-level query.
"""
from __future__ import annotations

from collections import defaultdict

import structlog

from polymarket.client import get
from polymarket.models import PricePoint

log = structlog.get_logger()

DATA_BASE = "https://data-api.polymarket.com"

# Empirically, the /trades endpoint rejects requests where offset > ~3000
# with a 400. limit also appears to cap at 500 regardless of the requested
# value, so we paginate 500 at a time up to the offset ceiling.
_PAGE_SIZE = 500
_MAX_OFFSET = 3000


async def fetch_price_history_from_trades(condition_id: str) -> dict[str, list[PricePoint]]:
    """Return a mapping of token_id -> sorted PricePoint list for one market.

    Paginates `/trades?market=<conditionId>` up to the API's hard offset cap
    and buckets each trade under its `asset` (token id). Missing or malformed
    trades are skipped with a warning — never raised.
    """
    buckets: dict[str, list[PricePoint]] = defaultdict(list)
    offset = 0
    total_pages = 0

    while offset <= _MAX_OFFSET:
        log.info("trade_history.page_start", condition_id=condition_id, offset=offset)
        try:
            data = await get(
                f"{DATA_BASE}/trades",
                params={"market": condition_id, "limit": _PAGE_SIZE, "offset": offset},
            )
        except Exception as e:
            log.warning(
                "trade_history.page_error",
                condition_id=condition_id,
                offset=offset,
                error=str(e),
            )
            break

        if not isinstance(data, list) or not data:
            break

        for trade in data:
            asset = trade.get("asset")
            if not asset:
                continue
            try:
                ts = int(trade["timestamp"])
                price = float(trade["price"])
            except (KeyError, TypeError, ValueError) as e:
                log.warning(
                    "trade_history.validation_error",
                    condition_id=condition_id,
                    error=str(e),
                )
                continue
            buckets[asset].append(PricePoint(t=ts, p=price))

        total_pages += 1
        if len(data) < _PAGE_SIZE:
            break
        offset += _PAGE_SIZE

    for token_id in buckets:
        buckets[token_id].sort(key=lambda pt: pt.t)

    log.info(
        "trade_history.fetch_complete",
        condition_id=condition_id,
        pages=total_pages,
        tokens=len(buckets),
        total_points=sum(len(v) for v in buckets.values()),
    )
    return dict(buckets)
