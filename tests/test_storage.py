from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock

import pyarrow.parquet as pq
import pytest
from structlog.testing import capture_logs

from polymarket.models import MARKETS_SCHEMA_VERSION, PRICE_HISTORY_SCHEMA_VERSION
from polymarket.storage.parquet import write_markets, write_price_history
from polymarket.storage.state import append_dead_letter, load_state, save_state

PARTITION_DATE = date(2026, 4, 6)
TOKEN_ID = "abc123"


def _read(path):
    """Read a Parquet file without partition inference from directory names."""
    return pq.ParquetFile(str(path)).read()


class TestWriteMarkets:
    def test_happy_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        records = [{"id": "m1", "question": "Will team X win?", "active": True}]
        write_markets(records, partition_date=PARTITION_DATE)

        out = tmp_path / f"data/raw/markets/date={PARTITION_DATE}/markets.parquet"
        assert out.exists()
        table = _read(out)
        assert table.num_rows == 1
        assert set(table.column_names) == {"id", "question", "active"}

    def test_schema_version_embedded(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        write_markets([{"id": "m1"}], partition_date=PARTITION_DATE)
        out = tmp_path / f"data/raw/markets/date={PARTITION_DATE}/markets.parquet"
        meta = pq.read_metadata(out)
        assert (meta.metadata or {}).get(b"schema_version") == MARKETS_SCHEMA_VERSION.encode()

    def test_no_warning_on_happy_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        with capture_logs() as logs:
            write_markets([{"id": "m1"}, {"id": "m2"}], partition_date=PARTITION_DATE)
        assert not any(e["event"] in ("parquet.row_count_mismatch", "parquet.schema_version_mismatch") for e in logs)

    def test_row_count_warning_on_mismatch(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        fake_meta = MagicMock()
        fake_meta.num_rows = 999
        fake_meta.metadata = {b"schema_version": MARKETS_SCHEMA_VERSION.encode()}
        monkeypatch.setattr("polymarket.storage.parquet.pq.read_metadata", lambda _: fake_meta)
        with capture_logs() as logs:
            write_markets([{"id": "m1"}], partition_date=PARTITION_DATE)
        assert any(e["event"] == "parquet.row_count_mismatch" for e in logs)


class TestWritePriceHistory:
    def test_happy_path(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        price_points = [{"t": 1700000000, "p": 0.72}, {"t": 1700003600, "p": 0.68}]
        write_price_history(price_points, token_id=TOKEN_ID, partition_date=PARTITION_DATE)

        out = tmp_path / f"data/raw/price_history/token_id={TOKEN_ID}/date={PARTITION_DATE}/prices.parquet"
        assert out.exists()
        table = _read(out)
        assert table.num_rows == 2
        assert set(table.column_names) == {"token_id", "t", "p"}

    def test_token_id_injected_as_column(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        price_points = [{"t": 1700000000, "p": 0.5}]
        write_price_history(price_points, token_id=TOKEN_ID, partition_date=PARTITION_DATE)

        out = tmp_path / f"data/raw/price_history/token_id={TOKEN_ID}/date={PARTITION_DATE}/prices.parquet"
        table = _read(out)
        assert table.column("token_id").to_pylist() == [TOKEN_ID]

    def test_column_dtypes(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        price_points = [{"t": 1700000000, "p": 0.72}]
        write_price_history(price_points, token_id=TOKEN_ID, partition_date=PARTITION_DATE)

        out = tmp_path / f"data/raw/price_history/token_id={TOKEN_ID}/date={PARTITION_DATE}/prices.parquet"
        table = _read(out)
        schema = {field.name: str(field.type) for field in table.schema}
        assert schema["t"] == "int64"
        assert schema["p"] == "double"
        assert schema["token_id"] == "string"

    def test_empty_list_writes_zero_rows(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        write_price_history([], token_id=TOKEN_ID, partition_date=PARTITION_DATE)

        out = tmp_path / f"data/raw/price_history/token_id={TOKEN_ID}/date={PARTITION_DATE}/prices.parquet"
        assert out.exists()
        table = _read(out)
        assert table.num_rows == 0

    def test_schema_version_embedded(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        write_price_history([{"t": 1000, "p": 0.5}], token_id=TOKEN_ID, partition_date=PARTITION_DATE)
        out = tmp_path / f"data/raw/price_history/token_id={TOKEN_ID}/date={PARTITION_DATE}/prices.parquet"
        meta = pq.read_metadata(out)
        assert (meta.metadata or {}).get(b"schema_version") == PRICE_HISTORY_SCHEMA_VERSION.encode()

    def test_is_cancelled_column_written(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        records = [{"t": 1000, "p": 0.5, "is_cancelled": True}]
        write_price_history(records, token_id=TOKEN_ID, partition_date=PARTITION_DATE)
        out = tmp_path / f"data/raw/price_history/token_id={TOKEN_ID}/date={PARTITION_DATE}/prices.parquet"
        table = _read(out)
        assert "is_cancelled" in table.column_names
        assert table.column("is_cancelled").to_pylist() == [True]


class TestState:
    def test_round_trip(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        save_state("markets", {"offset": 42, "extra": "value"})
        assert load_state("markets") == {"offset": 42, "extra": "value"}

    def test_missing_key_returns_empty_dict(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert load_state("nonexistent") == {}

    def test_overwrites_existing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        save_state("markets", {"offset": 10})
        save_state("markets", {"offset": 99})
        assert load_state("markets") == {"offset": 99}

    def test_creates_directory(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        save_state("prices", {"last_ts": 1700000000})
        assert (tmp_path / "data/state/prices.json").exists()


class TestDeadLetter:
    def test_appends_entry(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        append_dead_letter("tok1", "HTTPError: 404", {"market_id": "m1"})
        dl = tmp_path / "data/state/dead_letter.jsonl"
        assert dl.exists()
        entry = json.loads(dl.read_text().strip())
        assert entry["token_id"] == "tok1"
        assert entry["error"] == "HTTPError: 404"
        assert entry["context"] == {"market_id": "m1"}
        assert "timestamp" in entry

    def test_timestamp_is_utc_iso8601(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        append_dead_letter("tok1", "err", {})
        dl = tmp_path / "data/state/dead_letter.jsonl"
        entry = json.loads(dl.read_text().strip())
        assert entry["timestamp"].endswith("+00:00")

    def test_multiple_entries_appended(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        append_dead_letter("tok1", "err1", {})
        append_dead_letter("tok2", "err2", {})
        dl = tmp_path / "data/state/dead_letter.jsonl"
        lines = [l for l in dl.read_text().splitlines() if l]
        assert len(lines) == 2
        assert json.loads(lines[0])["token_id"] == "tok1"
        assert json.loads(lines[1])["token_id"] == "tok2"

    def test_creates_state_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert not (tmp_path / "data/state").exists()
        append_dead_letter("tok1", "err", {})
        assert (tmp_path / "data/state").exists()

    def test_each_line_is_valid_json(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        for i in range(3):
            append_dead_letter(f"tok{i}", f"err{i}", {"i": i})
        dl = tmp_path / "data/state/dead_letter.jsonl"
        for line in dl.read_text().splitlines():
            parsed = json.loads(line)
            assert isinstance(parsed, dict)
