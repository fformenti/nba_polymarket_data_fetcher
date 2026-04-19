from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

from polymarket.models import (
    MARKETS_BY_SLUG_SCHEMA_VERSION,
    MARKETS_SCHEMA_VERSION,
    PRICE_HISTORY_BY_SLUG_SCHEMA_VERSION,
    PRICE_HISTORY_SCHEMA_VERSION,
    SlugFetchResult,
)

log = structlog.get_logger()


def _embed_and_verify(
    table: pa.Table,
    path: Path,
    schema_version: str,
    expected_rows: int,
) -> None:
    existing = table.schema.metadata or {}
    table = table.replace_schema_metadata({**existing, b"schema_version": schema_version.encode()})
    pq.write_table(table, path, compression="snappy")
    meta = pq.read_metadata(path)
    if meta.num_rows != expected_rows:
        log.warning(
            "parquet.row_count_mismatch",
            path=str(path),
            expected=expected_rows,
            actual=meta.num_rows,
        )
    stored_version = (meta.metadata or {}).get(b"schema_version", b"").decode()
    if stored_version != schema_version:
        log.warning(
            "parquet.schema_version_mismatch",
            path=str(path),
            expected=schema_version,
            actual=stored_version,
        )


def write_markets(markets: list[dict], partition_date: date) -> None:
    path = Path(f"data/raw/markets/date={partition_date}/markets.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(markets)
    _embed_and_verify(table, path, MARKETS_SCHEMA_VERSION, len(markets))


def write_price_history(price_points: list[dict], token_id: str, partition_date: date) -> None:
    path = Path(f"data/raw/price_history/token_id={token_id}/date={partition_date}/prices.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [{"token_id": token_id, **pp} for pp in price_points]
    table = pa.Table.from_pylist(records)
    _embed_and_verify(table, path, PRICE_HISTORY_SCHEMA_VERSION, len(records))


def is_closed_market_cached(
    slug: str, game_date: date, *, root: Path | None = None
) -> bool:
    """Return True iff we already have a parquet for this slug with closed=True.

    Closed markets never produce new data, so refetching them on every run
    burns the Data API budget for no benefit. The markets_by_slug parquet
    already records the `closed` flag per slug — reading it is cheaper than
    another round-trip to Gamma + paginating /trades.
    """
    base = root if root is not None else Path("data/raw/markets_by_slug")
    path = base / f"date={game_date}" / f"{slug}.parquet"
    if not path.exists():
        return False
    try:
        table = pq.read_table(path, columns=["closed"])
    except Exception as e:
        log.warning("cache_check.read_error", slug=slug, path=str(path), error=str(e))
        return False
    if table.num_rows == 0:
        return False
    return bool(table.column("closed")[0].as_py())


def write_market_by_slug(result: SlugFetchResult, *, root: Path | None = None) -> Path:
    """Write one-row market snapshot: metadata + midpoint/is_cancelled per outcome."""
    base = root if root is not None else Path("data/raw/markets_by_slug")
    path = base / f"date={result.game_date}" / f"{result.slug}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)

    row: dict = {
        "slug": result.slug,
        "game_date": str(result.game_date),
        "fetched_at": result.fetched_at.isoformat(),
        "market_id": result.market.id,
        "question": result.market.question,
        "active": result.market.active,
        "closed": result.market.closed,
        "liquidity": result.market.liquidity,
        "volume": result.market.volume,
        "condition_id": result.market.condition_id,
        "end_date_iso": result.market.end_date_iso,
        "game_start_time": (
            result.market.game_start_time.isoformat()
            if result.market.game_start_time is not None
            else None
        ),
    }
    for tok in result.tokens:
        suffix = tok.outcome.lower()
        row[f"token_id_{suffix}"] = tok.token_id
        row[f"midpoint_{suffix}"] = tok.midpoint
        row[f"is_cancelled_{suffix}"] = tok.is_cancelled
        row[f"history_len_{suffix}"] = len(tok.history)

    table = pa.Table.from_pylist([row])
    _embed_and_verify(table, path, MARKETS_BY_SLUG_SCHEMA_VERSION, 1)
    return path


def write_price_history_by_slug(result: SlugFetchResult, *, root: Path | None = None) -> Path:
    """Write long-form (token_id, t, p, ...) rows for every price point in this slug."""
    base = root if root is not None else Path("data/raw/price_history_by_slug")
    path = base / f"date={result.game_date}" / f"{result.slug}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)

    records: list[dict] = []
    for tok in result.tokens:
        for pt in tok.history:
            records.append(
                {
                    "slug": result.slug,
                    "token_id": tok.token_id,
                    "outcome": tok.outcome,
                    "t": pt.t,
                    "ts_utc": datetime.fromtimestamp(pt.t, tz=ZoneInfo("UTC")),
                    "p": pt.p,
                    "is_cancelled": tok.is_cancelled,
                }
            )

    schema = pa.schema(
        [
            ("slug", pa.string()),
            ("token_id", pa.string()),
            ("outcome", pa.string()),
            ("t", pa.int64()),
            ("ts_utc", pa.timestamp("s", tz="UTC")),
            ("p", pa.float64()),
            ("is_cancelled", pa.bool_()),
        ]
    )
    if not records:
        log.info("parquet.no_history_rows", slug=result.slug)
        table = pa.Table.from_pylist([], schema=schema)
    else:
        table = pa.Table.from_pylist(records, schema=schema)
    _embed_and_verify(table, path, PRICE_HISTORY_BY_SLUG_SCHEMA_VERSION, len(records))
    return path
