# Spec: Storage Layer (`src/polymarket/storage/`)

## `storage/parquet.py`

### `write_markets(records: list[dict], partition_date: str) → None`

Writes market metadata records.

- Output path: `data/raw/markets/date={partition_date}/markets.parquet`
- Compression: snappy
- Creates parent directories if missing
- Overwrites existing file at that path (idempotent re-run)
- Logs `{path, row_count}` after write
- Warns if `row_count == 0`

### `write_price_history(records: list[dict], partition_date: str) → None`

Writes price history records.

- Output path: `data/raw/price_history/date={partition_date}/data.parquet`
- Same conventions as `write_markets`
- Expected columns: `token_id (str), t (int64), p (float64), market_slug (str)`

### Schema Version

Include `schema_version = "1"` in Parquet file-level metadata:
```python
table = pa.Table.from_pylist(records)
table = table.replace_schema_metadata({"schema_version": "1"})
```

---

## `storage/state.py`

### `load_state(key: str) → dict`

```python
def load_state(key: str) -> dict:
    path = Path(f"data/state/{key}.json")
    if not path.exists():
        return {}
    return json.loads(path.read_text())
```

- Returns empty dict if file does not exist (first run)
- Does not create the file on read

### `save_state(key: str, data: dict) → None`

```python
def save_state(key: str, data: dict) -> None:
    path = Path(f"data/state/{key}.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))
```

- Creates `data/state/` directory if missing
- Writes atomically (Python's `write_text` is atomic on most OS for small files)
- `data` must be JSON-serializable

---

## Directory Layout

```
data/
├── raw/
│   ├── markets/
│   │   └── date=2026-04-05/
│   │       └── markets.parquet
│   └── price_history/
│       └── date=2026-04-05/
│           └── data.parquet
└── state/
    ├── markets.json
    └── price_history.json
```

The `data/` directory is git-ignored. State files are small JSON blobs; raw files are Parquet.
