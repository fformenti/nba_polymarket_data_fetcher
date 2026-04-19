"""End-to-end integration tests for the full pipeline.

Mocking strategy: each fetcher module imports `get` from `polymarket.client` as
a local binding (`from polymarket.client import get`).  Patching
`polymarket.client.get` after import has no effect on those local references, so
we patch each module's local name directly.  This exercises every layer below
the HTTP boundary: fetcher logic, Pydantic validation, Parquet writes, and state
persistence.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from unittest.mock import AsyncMock, patch

import pyarrow.parquet as pq

from polymarket.pipeline import run
from polymarket.storage.state import load_state

PARTITION_DATE = date(2026, 4, 6)

# ── Representative API payloads ───────────────────────────────────────────────

_MARKET_1 = {
    "id": "market-1",
    "question": "Will Lakers beat Celtics?",
    "slug": "lakers-vs-celtics",
    "active": True,
    "closed": False,
    "liquidity": "10000.0",
    "volume": "50000.0",
    "conditionId": "cond-1",
    "endDateIso": "2026-04-10T00:00:00Z",
    "clobTokenIds": ["token-yes-1", "token-no-1"],
}

_MARKET_NO_TOKENS = {
    "id": "market-2",
    "question": "Will Warriors beat Suns?",
    "slug": "warriors-vs-suns",
    "active": True,
    "closed": False,
    "liquidity": "0",
    "volume": "0",
    "conditionId": "cond-2",
    "endDateIso": None,
    "clobTokenIds": [],
}

_PRICE_HISTORY = {
    "history": [
        {"t": 1700000000, "p": 0.65},
        {"t": 1700003600, "p": 0.70},
    ]
}

_MIDPOINT = {"mid": 0.68}


# ── Helpers ───────────────────────────────────────────────────────────────────

@contextmanager
def _patch_fetchers(
    markets_return,
    history_return=None,
    prices_side_effect=None,
):
    """Patch async get() in all three fetcher modules simultaneously."""
    m_markets = AsyncMock(return_value=markets_return)
    m_history = AsyncMock(return_value=history_return or _PRICE_HISTORY)
    m_prices = AsyncMock(side_effect=prices_side_effect or [_MIDPOINT])

    with (
        patch("polymarket.fetchers.markets.get", m_markets),
        patch("polymarket.fetchers.price_history.get", m_history),
        patch("polymarket.fetchers.prices.get", m_prices),
    ):
        yield m_markets, m_history, m_prices


def _read_parquet(path):
    return pq.ParquetFile(str(path)).read()


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestHappyPath:
    async def test_markets_parquet_created(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with _patch_fetchers([_MARKET_1]):
            await run(date=PARTITION_DATE)

        out = tmp_path / f"data/raw/markets/date={PARTITION_DATE}/markets.parquet"
        assert out.exists()
        assert _read_parquet(out).num_rows == 1

    async def test_price_history_parquet_created(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with _patch_fetchers([_MARKET_1]):
            await run(date=PARTITION_DATE)

        out = (
            tmp_path
            / f"data/raw/price_history/token_id=token-yes-1/date={PARTITION_DATE}/prices.parquet"
        )
        assert out.exists()
        assert _read_parquet(out).num_rows == 2

    async def test_price_history_schema(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with _patch_fetchers([_MARKET_1]):
            await run(date=PARTITION_DATE)

        out = (
            tmp_path
            / f"data/raw/price_history/token_id=token-yes-1/date={PARTITION_DATE}/prices.parquet"
        )
        table = _read_parquet(out)
        assert set(table.column_names) == {"token_id", "t", "p", "is_cancelled"}

    async def test_markets_state_saved(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with _patch_fetchers([_MARKET_1]):
            await run(date=PARTITION_DATE)

        state = load_state("markets")
        assert "offset" in state
        assert state["offset"] == 1

    async def test_price_history_state_saved(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with _patch_fetchers([_MARKET_1]):
            await run(date=PARTITION_DATE)

        state = load_state("price_history")
        assert state.get("token-yes-1") == 1700003600


class TestDryRun:
    async def test_no_parquet_files_written(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with _patch_fetchers([_MARKET_1]):
            await run(date=PARTITION_DATE, dry_run=True)

        assert not (tmp_path / "data" / "raw").exists()

    async def test_fetchers_still_called(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with _patch_fetchers([_MARKET_1]) as (m_markets, m_history, _):
            await run(date=PARTITION_DATE, dry_run=True)

        m_markets.assert_awaited_once()
        m_history.assert_awaited_once()


class TestCustomDate:
    async def test_partition_uses_provided_date(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        custom_date = date(2025, 12, 25)
        with _patch_fetchers([_MARKET_1]):
            await run(date=custom_date)

        out = tmp_path / f"data/raw/markets/date={custom_date}/markets.parquet"
        assert out.exists()


class TestEdgeCases:
    async def test_market_without_tokens_skipped(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with _patch_fetchers([_MARKET_NO_TOKENS], prices_side_effect=[]):
            await run(date=PARTITION_DATE)

        out = tmp_path / f"data/raw/markets/date={PARTITION_DATE}/markets.parquet"
        assert out.exists()
        assert not (tmp_path / "data" / "raw" / "price_history").exists()

    async def test_price_history_error_doesnt_stop_pipeline(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        m_history = AsyncMock(side_effect=RuntimeError("clob timeout"))
        with (
            patch("polymarket.fetchers.markets.get", AsyncMock(return_value=[_MARKET_1])),
            patch("polymarket.fetchers.price_history.get", m_history),
            patch("polymarket.fetchers.prices.get", AsyncMock(return_value=_MIDPOINT)),
        ):
            await run(date=PARTITION_DATE)

        out = tmp_path / f"data/raw/markets/date={PARTITION_DATE}/markets.parquet"
        assert out.exists()

    async def test_empty_market_list(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with _patch_fetchers([], prices_side_effect=[]):
            await run(date=PARTITION_DATE)

        assert not (tmp_path / "data" / "raw" / "price_history").exists()

    async def test_price_history_error_writes_dead_letter(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        import json as _json

        m_history = AsyncMock(side_effect=RuntimeError("clob timeout"))
        with (
            patch("polymarket.fetchers.markets.get", AsyncMock(return_value=[_MARKET_1])),
            patch("polymarket.fetchers.price_history.get", m_history),
            patch("polymarket.fetchers.prices.get", AsyncMock(return_value=_MIDPOINT)),
        ):
            await run(date=PARTITION_DATE)

        dl = tmp_path / "data/state/dead_letter.jsonl"
        assert dl.exists()
        entry = _json.loads(dl.read_text().strip())
        assert entry["token_id"] == "token-yes-1"
        assert "clob timeout" in entry["error"]

    async def test_cancelled_market_logged(self, tmp_path, monkeypatch):
        from structlog.testing import capture_logs

        monkeypatch.chdir(tmp_path)
        flat_history = {"history": [{"t": 1000, "p": 0.5}, {"t": 2000, "p": 0.5}]}
        with _patch_fetchers([_MARKET_1], history_return=flat_history), capture_logs() as logs:
            await run(date=PARTITION_DATE)

        assert any(e["event"] == "pipeline.cancelled_market_detected" for e in logs)

    async def test_is_cancelled_false_for_normal_history(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with _patch_fetchers([_MARKET_1]):
            await run(date=PARTITION_DATE)

        out = (
            tmp_path
            / f"data/raw/price_history/token_id=token-yes-1/date={PARTITION_DATE}/prices.parquet"
        )
        table = _read_parquet(out)
        assert table.column("is_cancelled").to_pylist() == [False, False]

    async def test_is_cancelled_true_for_flat_history(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        flat_history = {"history": [{"t": 1000, "p": 0.5}, {"t": 2000, "p": 0.5}]}
        with _patch_fetchers([_MARKET_1], history_return=flat_history):
            await run(date=PARTITION_DATE)

        out = (
            tmp_path
            / f"data/raw/price_history/token_id=token-yes-1/date={PARTITION_DATE}/prices.parquet"
        )
        table = _read_parquet(out)
        assert all(v is True for v in table.column("is_cancelled").to_pylist())
