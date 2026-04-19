from __future__ import annotations

import json
from unittest.mock import AsyncMock, patch

import pytest

from polymarket.fetchers.markets import fetch_all_markets
from polymarket.models import GammaMarket
from polymarket.storage.state import load_state
from tests.conftest import SAMPLE_MARKET

PAGE_SIZE = 100


def _make_market(**overrides) -> dict:
    return {**SAMPLE_MARKET, **overrides}


async def test_happy_path_returns_gamma_markets(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(return_value=[SAMPLE_MARKET])

    with patch("polymarket.fetchers.markets.get", mock_get):
        markets = await fetch_all_markets()

    assert len(markets) == 1
    assert isinstance(markets[0], GammaMarket)
    assert markets[0].id == SAMPLE_MARKET["id"]


async def test_malformed_item_skipped_no_exception(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    bad_item = {"question": "no id here"}
    good_item = _make_market(id="market-good")
    mock_get = AsyncMock(return_value=[bad_item, good_item])

    with patch("polymarket.fetchers.markets.get", mock_get):
        markets = await fetch_all_markets()

    assert len(markets) == 1
    assert markets[0].id == "market-good"


async def test_empty_response_returns_empty_list(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(return_value=[])

    with patch("polymarket.fetchers.markets.get", mock_get):
        markets = await fetch_all_markets()

    assert markets == []
    mock_get.assert_awaited_once()


async def test_pagination_fetches_all_pages(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    page1 = [_make_market(id=f"m-{i}") for i in range(PAGE_SIZE)]
    page2 = [_make_market(id=f"m-{i}") for i in range(PAGE_SIZE, PAGE_SIZE + 50)]
    mock_get = AsyncMock(side_effect=[page1, page2])

    with patch("polymarket.fetchers.markets.get", mock_get):
        markets = await fetch_all_markets()

    assert len(markets) == 150
    assert mock_get.await_count == 2
    assert load_state("markets")["offset"] == 150


async def test_state_saved_after_each_page_even_on_later_failure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    page1 = [_make_market(id=f"m-{i}") for i in range(PAGE_SIZE)]
    mock_get = AsyncMock(side_effect=[page1, Exception("network error")])

    with patch("polymarket.fetchers.markets.get", mock_get), pytest.raises(
        Exception, match="network error"
    ):
        await fetch_all_markets()

    assert load_state("markets")["offset"] == PAGE_SIZE


async def test_idempotency_resumes_from_saved_state(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    state_dir = tmp_path / "data" / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "markets.json").write_text(json.dumps({"offset": 50}))

    mock_get = AsyncMock(return_value=[_make_market(id="m-50")])

    with patch("polymarket.fetchers.markets.get", mock_get):
        await fetch_all_markets()

    call_params = mock_get.await_args.kwargs["params"]
    assert call_params["offset"] == 50
