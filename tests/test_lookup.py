from __future__ import annotations

from datetime import date
from pathlib import Path

from polymarket.lookup import load_slug_table, slugs_for_date, slugs_for_range


def _write_sample(path: Path) -> None:
    path.write_text(
        "game_id,game_slug, gameDateOnlyStr\n"
        "1,nba-hou-cha-2026-02-19,2026-02-19\n"
        "2,nba-bkn-cle-2026-02-19,2026-02-19\n"
        "3,nba-ind-was-2026-02-20,2026-02-20\n"
    )


def _write_historical(path: Path) -> None:
    path.write_text(
        "game_id,game_slug\n"
        "10,nba-phi-bos-2022-10-18\n"
        "11,nba-lal-gsw-2022-10-18\n"
    )


def test_load_slug_table_strips_header_whitespace(tmp_path):
    sample = tmp_path / "sample.csv"
    _write_sample(sample)

    df = load_slug_table([sample])
    assert "gameDateOnlyStr" not in df.columns  # normalised into game_date
    assert set(df.columns) == {"game_id", "game_slug", "game_date"}
    assert len(df) == 3


def test_load_slug_table_parses_date_from_slug_when_column_missing(tmp_path):
    historical = tmp_path / "hist.csv"
    _write_historical(historical)

    df = load_slug_table([historical])
    assert len(df) == 2
    assert all(d == date(2022, 10, 18) for d in df["game_date"])


def test_load_slug_table_dedupes_by_slug(tmp_path):
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text(
        "game_id,game_slug, gameDateOnlyStr\n"
        "1,nba-hou-cha-2026-02-19,2026-02-19\n"
    )
    b.write_text(
        "game_id,game_slug, gameDateOnlyStr\n"
        "99,nba-hou-cha-2026-02-19,2026-02-19\n"
    )

    df = load_slug_table([a, b])
    assert len(df) == 1
    assert df.iloc[0]["game_id"] == 1  # first occurrence wins


def test_slugs_for_date(tmp_path):
    sample = tmp_path / "sample.csv"
    _write_sample(sample)

    slugs = slugs_for_date(date(2026, 2, 19), paths=[sample])
    assert sorted(slugs) == sorted(["nba-hou-cha-2026-02-19", "nba-bkn-cle-2026-02-19"])


def test_slugs_for_range(tmp_path):
    sample = tmp_path / "sample.csv"
    _write_sample(sample)

    slugs = slugs_for_range(date(2026, 2, 19), date(2026, 2, 20), paths=[sample])
    assert len(slugs) == 3
