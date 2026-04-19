"""Lookup table for (game_date, game_slug) pairs.

Loads the slug CSVs under `data/raw/` and exposes helpers that return the list
of slugs for a given date or date range.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pandas as pd
import structlog

log = structlog.get_logger()

_DEFAULT_PATHS: tuple[Path, ...] = (
    Path("data/raw/games_slug_sample.csv"),
    Path("data/raw/games_slug.csv"),
)

_SLUG_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})$")


def _date_from_slug(slug: str) -> date | None:
    m = _SLUG_DATE_RE.search(slug)
    if not m:
        return None
    try:
        return date.fromisoformat(m.group(1))
    except ValueError:
        return None


def load_slug_table(paths: list[Path] | None = None) -> pd.DataFrame:
    """Load every known slug CSV, normalise headers, and add a game_date column.

    Returns a DataFrame with columns: game_id, game_slug, game_date (date).
    De-duplicated by game_slug — first occurrence wins.
    """
    sources = paths if paths is not None else list(_DEFAULT_PATHS)
    frames: list[pd.DataFrame] = []

    for path in sources:
        if not path.exists():
            log.info("lookup.csv_missing", path=str(path))
            continue

        df = pd.read_csv(path)
        df.columns = [c.strip() for c in df.columns]

        if "gameDateOnlyStr" in df.columns:
            df["game_date"] = pd.to_datetime(df["gameDateOnlyStr"]).dt.date
        else:
            df["game_date"] = df["game_slug"].map(_date_from_slug)

        keep = ["game_id", "game_slug", "game_date"]
        frames.append(df[keep])

    if not frames:
        return pd.DataFrame(columns=["game_id", "game_slug", "game_date"])

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["game_date", "game_slug"])
    combined = combined.drop_duplicates(subset=["game_slug"], keep="first")
    return combined.reset_index(drop=True)


def slugs_for_date(d: date, paths: list[Path] | None = None) -> list[str]:
    table = load_slug_table(paths)
    mask = table["game_date"] == d
    return table.loc[mask, "game_slug"].tolist()


def slugs_for_range(start: date, end: date, paths: list[Path] | None = None) -> list[str]:
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    table = load_slug_table(paths)
    mask = (table["game_date"] >= start) & (table["game_date"] <= end)
    return table.loc[mask, "game_slug"].tolist()
