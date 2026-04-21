from __future__ import annotations

import json
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from polymarket.models import GammaMarket, PricePoint, SlugFetchResult, TokenBundle
from polymarket.storage.debug_json import write_debug_json
from polymarket.storage.parquet import (
    is_closed_market_cached,
    write_market_by_slug,
    write_price_history_by_slug,
)


def _sample_result() -> SlugFetchResult:
    market = GammaMarket.model_validate(
        {
            "id": "m-1",
            "question": "Will Rockets beat Hornets?",
            "slug": "nba-hou-cha-2026-02-19",
            "active": True,
            "closed": False,
            "liquidity": "1000",
            "volume": "5000",
            "conditionId": "cond-1",
            "endDateIso": "2026-02-20T00:00:00Z",
            "gameStartTime": "2026-02-20 00:30:00+00",
            "clobTokenIds": ["tok-yes", "tok-no"],
        }
    )
    return SlugFetchResult(
        slug="nba-hou-cha-2026-02-19",
        game_date=date(2026, 2, 19),
        fetched_at=datetime(2026, 2, 19, 23, 0, tzinfo=UTC),
        market=market,
        tokens=[
            TokenBundle(
                token_id="tok-yes",
                outcome="Yes",
                midpoint=0.61,
                history=[PricePoint(t=1, p=0.6), PricePoint(t=2, p=0.62)],
                is_cancelled=False,
            ),
            TokenBundle(
                token_id="tok-no",
                outcome="No",
                midpoint=0.39,
                history=[PricePoint(t=1, p=0.4)],
                is_cancelled=False,
            ),
        ],
    )


def _read(path):
    return pq.ParquetFile(str(path)).read()


def test_write_market_by_slug(tmp_path):
    result = _sample_result()
    root = tmp_path / "markets_by_slug"
    path = write_market_by_slug(result, root=root)

    assert path.exists()
    assert path.name == "nba-hou-cha-2026-02-19.parquet"
    assert path.parent.name == "date=2026-02-19"

    table = _read(path)
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["slug"] == "nba-hou-cha-2026-02-19"
    assert row["midpoint_yes"] == 0.61
    assert row["midpoint_no"] == 0.39
    assert row["is_cancelled_yes"] is False
    assert row["history_len_yes"] == 2
    assert row["game_start_time"] == "2026-02-20T00:30:00+00:00"


def test_write_price_history_by_slug(tmp_path):
    result = _sample_result()
    root = tmp_path / "prices_by_slug"
    path = write_price_history_by_slug(result, root=root)

    assert path.exists()
    table = _read(path)
    assert set(table.column_names) == {
        "slug",
        "token_id",
        "outcome",
        "t",
        "ts_utc",
        "p",
        "is_cancelled",
    }
    assert table.num_rows == 3  # 2 YES + 1 NO
    outcomes = sorted(table.column("outcome").to_pylist())
    assert outcomes == ["No", "Yes", "Yes"]

    # ts_utc must be the datetime-equivalent of t, anchored in UTC.
    rows = sorted(table.to_pylist(), key=lambda r: (r["outcome"], r["t"]))
    for row in rows:
        assert row["ts_utc"] == datetime.fromtimestamp(row["t"], tz=ZoneInfo("UTC"))


def test_is_closed_market_cached_missing_file(tmp_path):
    root = tmp_path / "markets_by_slug"
    assert is_closed_market_cached("nba-hou-cha-2026-02-19", date(2026, 2, 19), root=root) is False


def test_is_closed_market_cached_open_market(tmp_path):
    root = tmp_path / "markets_by_slug"
    result = _sample_result()  # _sample_result builds an open (closed=False) market
    write_market_by_slug(result, root=root)
    assert is_closed_market_cached(result.slug, result.game_date, root=root) is False


def test_is_closed_market_cached_closed_market(tmp_path):
    root = tmp_path / "markets_by_slug"
    result = _sample_result()
    # Closed markets are written with the slim schema (no "closed" column).
    # is_closed_market_cached must detect this via the column-absent fallback.
    result.market.closed = True
    write_market_by_slug(result, root=root)
    assert is_closed_market_cached(result.slug, result.game_date, root=root) is True


def test_write_market_by_slug_closed(tmp_path):
    """Closed (past) games should produce the slim 7-column schema only."""
    root = tmp_path / "markets_by_slug"
    result = _sample_result()
    result.market.closed = True
    path = write_market_by_slug(result, root=root)

    table = _read(path)
    assert table.num_rows == 1
    assert set(table.column_names) == {
        "slug",
        "game_date",
        "market_id",
        "game_start_time",
        "is_cancelled_yes",
        "fetched_at",
        "pre_game_price_yes",
    }
    row = table.to_pylist()[0]
    assert row["slug"] == "nba-hou-cha-2026-02-19"
    assert row["market_id"] == "m-1"
    assert row["is_cancelled_yes"] is False
    # Full-schema columns must be absent
    assert "active" not in row
    assert "closed" not in row
    assert "midpoint_yes" not in row
    assert "liquidity" not in row


def test_write_debug_json(tmp_path):
    result = _sample_result()
    root = tmp_path / "debug"
    path = write_debug_json(result, root=root)

    assert path.exists()
    assert path.name == "nba-hou-cha-2026-02-19.json"
    assert path.parent.name == "2026-02-19"

    data = json.loads(path.read_text())
    assert data["slug"] == "nba-hou-cha-2026-02-19"
    assert len(data["tokens"]) == 2
    assert data["tokens"][0]["midpoint"] == 0.61
    assert data["market"]["game_start_time"] == "2026-02-20T00:30:00Z"
