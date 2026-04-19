"""Human-readable JSON mirror of slug fetch results (for debugging).

Lives under `data/debug/`, not `data/raw/`, so it does not violate the
"Parquet only in data/raw/" project rule.
"""
from __future__ import annotations

from pathlib import Path

import structlog

from polymarket.models import SlugFetchResult

log = structlog.get_logger()


def write_debug_json(result: SlugFetchResult, *, root: Path | None = None) -> Path:
    base = root if root is not None else Path("data/debug/markets_by_slug")
    path = base / str(result.game_date) / f"{result.slug}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    log.info("debug_json.written", slug=result.slug, path=str(path))
    return path
