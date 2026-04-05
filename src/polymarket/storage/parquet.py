from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_markets(markets: list[dict], partition_date: date):
    path = Path(f"data/raw/markets/date={partition_date}/markets.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(markets)
    pq.write_table(table, path, compression="snappy")
