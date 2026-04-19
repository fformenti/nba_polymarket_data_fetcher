from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

from polymarket.fetchers.price_history import STATE_KEY, fetch_price_history
from polymarket.storage.state import load_state


def _history(*timestamps: int) -> dict:
    return {"history": [{"t": t, "p": 0.5} for t in timestamps]}


async def test_first_fetch_uses_interval_max(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(return_value=_history(1000, 2000, 3000))

    with patch("polymarket.fetchers.price_history.get", mock_get):
        points = await fetch_price_history("token-abc")

    assert len(points) == 3
    params = mock_get.await_args.kwargs["params"]
    assert params["interval"] == "max"
    assert "startTs" not in params


async def test_incremental_uses_start_ts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    state_dir = tmp_path / "data" / "state"
    state_dir.mkdir(parents=True)
    (state_dir / "price_history.json").write_text(json.dumps({"token-abc": 5000}))

    mock_get = AsyncMock(return_value=_history(6000, 7000))
    with patch("polymarket.fetchers.price_history.get", mock_get):
        points = await fetch_price_history("token-abc")

    assert len(points) == 2
    params = mock_get.await_args.kwargs["params"]
    assert params["startTs"] == 5000
    assert "interval" not in params


async def test_state_saved_with_max_ts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(return_value=_history(100, 300, 200))

    with patch("polymarket.fetchers.price_history.get", mock_get):
        await fetch_price_history("token-xyz")

    assert load_state(STATE_KEY)["token-xyz"] == 300


async def test_empty_history_no_state_saved(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(return_value={"history": []})

    with patch("polymarket.fetchers.price_history.get", mock_get):
        points = await fetch_price_history("token-abc")

    assert points == []
    assert "token-abc" not in load_state(STATE_KEY)


async def test_commit_checkpoint_called_with_max_ts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(return_value=_history(100, 300, 200))
    checkpoint = MagicMock()

    with patch("polymarket.fetchers.price_history.get", mock_get):
        await fetch_price_history("token-abc", commit_checkpoint=checkpoint)

    checkpoint.assert_called_once_with(300)


async def test_commit_checkpoint_skips_inline_save(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(return_value=_history(1000, 2000))
    checkpoint = MagicMock()

    with patch("polymarket.fetchers.price_history.get", mock_get):
        await fetch_price_history("token-abc", commit_checkpoint=checkpoint)

    assert "token-abc" not in load_state(STATE_KEY)


async def test_commit_checkpoint_not_called_on_empty_history(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(return_value={"history": []})
    checkpoint = MagicMock()

    with patch("polymarket.fetchers.price_history.get", mock_get):
        await fetch_price_history("token-abc", commit_checkpoint=checkpoint)

    checkpoint.assert_not_called()


async def test_malformed_item_skipped_no_exception(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    mock_get = AsyncMock(return_value={
        "history": [
            {"t": 1000, "p": 0.7},
            {"t": "not-an-int", "p": "not-a-float"},
            {"t": 2000, "p": 0.6},
        ]
    })

    with patch("polymarket.fetchers.price_history.get", mock_get):
        points = await fetch_price_history("token-abc")

    assert all(isinstance(pt.t, int) for pt in points)
    assert all(isinstance(pt.p, float) for pt in points)
