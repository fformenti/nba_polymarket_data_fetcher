from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

from polymarket.fetchers.by_slug import fetch_by_slug, fetch_games_batch

_MARKET = {
    "id": "m-1",
    "question": "Will Rockets beat Hornets?",
    "slug": "nba-hou-cha-2026-02-19",
    "active": True,
    "closed": False,
    "liquidity": "1000",
    "volume": "5000",
    "conditionId": "cond-1",
    "endDateIso": "2026-02-20T00:00:00Z",
    "clobTokenIds": ["token-yes", "token-no"],
}


def _event(slug: str = "nba-hou-cha-2026-02-19", markets: list | None = None) -> dict:
    return {
        "id": "evt-1",
        "slug": slug,
        "markets": markets if markets is not None else [{**_MARKET, "slug": slug}],
    }


_HISTORY = {"history": [{"t": 1, "p": 0.6}, {"t": 2, "p": 0.62}]}
_MIDPOINT = {"mid": 0.61}


def _make_get_mock(gamma_payload):
    """Return AsyncMock that routes by URL: gamma → markets, clob → history/midpoint."""

    async def _router(url, params=None):
        if "gamma-api" in url:
            return gamma_payload
        if "prices-history" in url:
            return _HISTORY
        if "midpoint" in url:
            return _MIDPOINT
        raise AssertionError(f"unexpected url: {url}")

    return AsyncMock(side_effect=_router)


async def test_fetch_by_slug_happy_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = _make_get_mock([_event()])

    with (
        patch("polymarket.fetchers.by_slug.get", mock_get),
        patch("polymarket.fetchers.price_history.get", mock_get),
    ):
        result = await fetch_by_slug("nba-hou-cha-2026-02-19")

    assert result is not None
    assert result.slug == "nba-hou-cha-2026-02-19"
    assert len(result.tokens) == 2
    assert result.tokens[0].outcome == "No"   # index 0 = away team (HOU)
    assert result.tokens[1].outcome == "Yes"  # index 1 = home team (CHA)
    assert all(tok.midpoint == 0.61 for tok in result.tokens)
    assert all(len(tok.history) == 2 for tok in result.tokens)


async def test_fetch_by_slug_not_found_returns_none(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = _make_get_mock([])

    with (
        patch("polymarket.fetchers.by_slug.get", mock_get),
        patch("polymarket.fetchers.price_history.get", mock_get),
    ):
        result = await fetch_by_slug("nba-bogus-slug-2026-02-19")

    assert result is None
    dl = tmp_path / "data/state/dead_letter.jsonl"
    assert dl.exists()


async def test_fetch_by_slug_closed_market_uses_trades(tmp_path, monkeypatch):
    """Closed markets should pull history from data-api /trades and skip midpoint."""
    monkeypatch.chdir(tmp_path)

    closed_market = {**_MARKET, "closed": True, "conditionId": "cond-closed"}
    event = _event(markets=[closed_market])

    trades_page = [
        {"asset": "token-yes", "timestamp": 100, "price": 0.55},
        {"asset": "token-no", "timestamp": 100, "price": 0.45},
        {"asset": "token-yes", "timestamp": 200, "price": 0.70},
        {"asset": "token-no", "timestamp": 200, "price": 0.30},
    ]
    midpoint_calls = 0

    async def _router(url, params=None):
        nonlocal midpoint_calls
        if "gamma-api" in url:
            return [event]
        if "data-api.polymarket.com/trades" in url:
            assert params["market"] == "cond-closed"
            return trades_page if params.get("offset", 0) == 0 else []
        if "midpoint" in url:
            midpoint_calls += 1
            return {"mid": 0.61}
        if "prices-history" in url:
            raise AssertionError("closed market should not hit CLOB prices-history")
        raise AssertionError(f"unexpected url: {url}")

    mock_get = AsyncMock(side_effect=_router)

    with (
        patch("polymarket.fetchers.by_slug.get", mock_get),
        patch("polymarket.fetchers.trade_history.get", mock_get),
        patch("polymarket.fetchers.price_history.get", mock_get),
    ):
        result = await fetch_by_slug("nba-hou-cha-2026-02-19")

    assert result is not None
    assert len(result.tokens) == 2
    yes, no = result.tokens
    assert yes.midpoint is None and no.midpoint is None
    assert midpoint_calls == 0
    assert [pt.t for pt in yes.history] == [100, 200]
    assert [pt.p for pt in yes.history] == [0.55, 0.70]
    assert [pt.t for pt in no.history] == [100, 200]
    assert [pt.p for pt in no.history] == [0.45, 0.30]


async def test_fetch_by_slug_no_moneyline_child(tmp_path, monkeypatch):
    """Event exists but has no child market whose slug matches the request."""
    monkeypatch.chdir(tmp_path)
    event = _event(markets=[{**_MARKET, "slug": "nba-hou-cha-2026-02-19-spread-home-2pt5"}])
    mock_get = _make_get_mock([event])

    with (
        patch("polymarket.fetchers.by_slug.get", mock_get),
        patch("polymarket.fetchers.price_history.get", mock_get),
    ):
        result = await fetch_by_slug("nba-hou-cha-2026-02-19")

    assert result is None
    dl = tmp_path / "data/state/dead_letter.jsonl"
    assert dl.exists()
    assert "no_moneyline_market" in dl.read_text()


async def test_fetch_by_slug_gamma_error_returns_none(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(side_effect=RuntimeError("gamma down"))

    with (
        patch("polymarket.fetchers.by_slug.get", mock_get),
        patch("polymarket.fetchers.price_history.get", mock_get),
    ):
        result = await fetch_by_slug("nba-hou-cha-2026-02-19")

    assert result is None


async def test_fetch_games_batch_parallelism_and_cap(tmp_path, monkeypatch):
    """Semaphore caps in-flight slugs at the configured concurrency."""
    monkeypatch.chdir(tmp_path)

    in_flight = 0
    peak = 0

    async def _router(url, params=None):
        nonlocal in_flight, peak
        if "gamma-api" in url:
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0.01)
            in_flight -= 1
            slug = params["slug"]
            return [_event(slug=slug, markets=[{**_MARKET, "slug": slug}])]
        if "prices-history" in url:
            return _HISTORY
        if "midpoint" in url:
            return _MIDPOINT
        raise AssertionError(url)

    mock_get = AsyncMock(side_effect=_router)
    slugs = [f"nba-game-{i}-2026-02-19" for i in range(10)]

    with (
        patch("polymarket.fetchers.by_slug.get", mock_get),
        patch("polymarket.fetchers.price_history.get", mock_get),
    ):
        results = await fetch_games_batch(slugs, concurrency=3)

    assert len(results) == 10
    assert peak <= 3
