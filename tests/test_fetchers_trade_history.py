from __future__ import annotations

from unittest.mock import AsyncMock, patch

from polymarket.fetchers.trade_history import _PAGE_SIZE, fetch_price_history_from_trades


def _trade(asset: str, ts: int, price: float) -> dict:
    return {"asset": asset, "timestamp": ts, "price": price, "conditionId": "c"}


async def test_single_page_buckets_by_asset():
    page = [
        _trade("yes", 300, 0.7),
        _trade("no", 200, 0.3),
        _trade("yes", 100, 0.5),
        _trade("no", 100, 0.5),
    ]

    async def _router(url, params=None):
        if params.get("offset", 0) == 0:
            return page
        return []

    with patch("polymarket.fetchers.trade_history.get", AsyncMock(side_effect=_router)):
        result = await fetch_price_history_from_trades("cond-1")

    assert set(result.keys()) == {"yes", "no"}
    # Sorted ascending by timestamp.
    assert [pt.t for pt in result["yes"]] == [100, 300]
    assert [pt.p for pt in result["yes"]] == [0.5, 0.7]
    assert [pt.t for pt in result["no"]] == [100, 200]


async def test_paginates_until_short_page():
    full_page = [_trade("yes", i, 0.5) for i in range(_PAGE_SIZE)]
    short_page = [_trade("yes", _PAGE_SIZE + i, 0.6) for i in range(3)]
    calls: list[int] = []

    async def _router(url, params=None):
        offset = params.get("offset", 0)
        calls.append(offset)
        if offset == 0:
            return full_page
        if offset == _PAGE_SIZE:
            return short_page
        return []

    with patch("polymarket.fetchers.trade_history.get", AsyncMock(side_effect=_router)):
        result = await fetch_price_history_from_trades("cond-1")

    assert calls == [0, _PAGE_SIZE]
    assert len(result["yes"]) == _PAGE_SIZE + 3


async def test_skips_malformed_trades():
    page = [
        _trade("yes", 100, 0.5),
        {"asset": "yes", "timestamp": "not-a-number", "price": 0.6},  # bad ts
        {"asset": None, "timestamp": 200, "price": 0.7},  # missing asset
        {"asset": "yes", "price": 0.8},  # missing timestamp
    ]

    async def _router(url, params=None):
        if params.get("offset", 0) == 0:
            return page
        return []

    with patch("polymarket.fetchers.trade_history.get", AsyncMock(side_effect=_router)):
        result = await fetch_price_history_from_trades("cond-1")

    assert len(result["yes"]) == 1
    assert result["yes"][0].t == 100


async def test_empty_first_page_returns_empty_dict():
    async def _router(url, params=None):
        return []

    with patch("polymarket.fetchers.trade_history.get", AsyncMock(side_effect=_router)):
        result = await fetch_price_history_from_trades("cond-1")

    assert result == {}


async def test_http_error_stops_pagination_gracefully():
    page = [_trade("yes", 1, 0.5)]

    async def _router(url, params=None):
        if params.get("offset", 0) == 0:
            return page + [_trade("yes", i + 2, 0.5) for i in range(_PAGE_SIZE - 1)]
        raise RuntimeError("api down")

    with patch("polymarket.fetchers.trade_history.get", AsyncMock(side_effect=_router)):
        result = await fetch_price_history_from_trades("cond-1")

    # First page was salvaged; second page errored but did not raise.
    assert len(result["yes"]) == _PAGE_SIZE
