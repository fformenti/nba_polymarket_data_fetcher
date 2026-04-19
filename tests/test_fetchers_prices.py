from __future__ import annotations

from unittest.mock import AsyncMock, patch

from polymarket.fetchers.prices import fetch_snapshot_prices


async def test_happy_path_two_tokens(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(side_effect=[{"mid": 0.7}, {"mid": 0.3}])

    with patch("polymarket.fetchers.prices.get", mock_get):
        prices = await fetch_snapshot_prices(["token-a", "token-b"])

    assert prices == {"token-a": 0.7, "token-b": 0.3}
    assert mock_get.await_count == 2


async def test_empty_list_returns_empty_dict(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock()

    with patch("polymarket.fetchers.prices.get", mock_get):
        prices = await fetch_snapshot_prices([])

    assert prices == {}
    mock_get.assert_not_awaited()


async def test_missing_mid_key_skips_token(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(return_value={"other_key": 0.5})

    with patch("polymarket.fetchers.prices.get", mock_get):
        prices = await fetch_snapshot_prices(["token-a"])

    assert prices == {}


async def test_partial_failure_returns_valid_token(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(side_effect=[RuntimeError("network error"), {"mid": 0.6}])

    with patch("polymarket.fetchers.prices.get", mock_get):
        prices = await fetch_snapshot_prices(["token-bad", "token-good"])

    assert prices == {"token-good": 0.6}
