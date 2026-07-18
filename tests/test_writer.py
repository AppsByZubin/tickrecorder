from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

import pyarrow.parquet as pq
import pytest

from tickrecorder.normalizers import (
    normalize_control_event,
    normalize_depth,
    normalize_symbol_update,
)
from tickrecorder.writer import EventEnvelope, ParquetEventWriter


class FakeDepth:
    tbq = 500
    tsq = 600
    bidprice = [10.0] * 50
    askprice = [10.05] * 50
    bidqty = [100] * 50
    askqty = [200] * 50
    bidordn = [5] * 50
    askordn = [6] * 50
    snapshot = True
    timestamp = 1_000
    sendtime = 1_001
    seqNo = 1


def common(event_id: int, source: str, symbol: str | None) -> dict:
    return {
        "schema_version": 1,
        "run_id": "test-run",
        "event_id": event_id,
        "source": source,
        "connection_id": "connection",
        "connection_epoch": 1,
        "symbol": symbol,
        "channel": "1" if source == "tbt_depth" else None,
        "received_at_unix_ns": event_id,
        "received_at_monotonic_ns": event_id,
        "received_at_utc": datetime.now(timezone.utc),
        "received_at_local": "2026-07-17T09:15:00+05:30",
        "trading_date": "2026-07-17",
        "receipt_hour": "09",
    }


def submit(writer: ParquetEventWriter, source: str, row: dict) -> None:
    writer.submit(
        EventEnvelope(
            source=source,
            trading_date=row["trading_date"],
            receipt_hour=row["receipt_hour"],
            row=row,
        )
    )


def test_writer_round_trips_all_streams(tmp_path) -> None:
    writer = ParquetEventWriter(
        output_dir=tmp_path,
        run_id="test-run",
        flush_interval_seconds=60,
        max_rows_per_file=100,
        queue_max_events=100,
        queue_put_timeout_seconds=1,
        compression="zstd",
    )
    writer.start()

    symbol_row = normalize_symbol_update(
        {
            "symbol": "NSE:TEST-EQ",
            "type": "sf",
            "ltp": 10.05,
            "last_traded_qty": 2,
            "vol_traded_today": 100,
        },
        common(1, "symbol_update", "NSE:TEST-EQ"),
    )
    depth_row = normalize_depth(
        "NSE:TEST-EQ",
        FakeDepth(),
        common(2, "tbt_depth", "NSE:TEST-EQ"),
        previous_sequence_no=None,
        include_raw_json=False,
    )
    control_row = normalize_control_event(
        common(3, "control", None),
        component="process",
        event_type="process_start",
        severity="INFO",
        message=None,
        details={"test": True},
    )

    submit(writer, "symbol_update", symbol_row)
    submit(writer, "tbt_depth", depth_row)
    submit(writer, "control", control_row)
    writer.stop()

    files = sorted((tmp_path / "20260717").rglob("*.parquet"))
    assert len(files) == 3

    assert {path.parent.name for path in files} == {
        "control",
        "symbolupdate",
        "tbtdepth",
    }
    assert any(path.name.startswith("part-control-20260717-") for path in files)
    assert any(path.name.startswith("part-symbolupdate-20260717-") for path in files)
    assert any(path.name.startswith("part-tbtdepth-20260717-") for path in files)

    depth_file = next(path for path in files if path.parent.name == "tbtdepth")
    depth_table = pq.ParquetFile(depth_file).read()
    assert depth_table.num_rows == 1
    assert depth_table.column("bid_prices")[0].as_py() == [10.0] * 50

    stats = writer.stats()
    assert stats["rows_written"] == {
        "symbol_update": 1,
        "tbt_depth": 1,
        "control": 1,
    }
    assert all(part["sha256"] for part in stats["parts"])
    assert all("min_event_id" in part for part in stats["parts"])
    assert all("max_event_id" in part for part in stats["parts"])

    marker = writer.write_marker_atomic("_SUCCESS")
    assert marker.exists()


def test_market_streams_flush_while_writer_is_running(tmp_path) -> None:
    writer = ParquetEventWriter(
        output_dir=tmp_path,
        run_id="periodic-flush-run",
        flush_interval_seconds=0.05,
        max_rows_per_file=100,
        queue_max_events=100,
        queue_put_timeout_seconds=1,
        compression="zstd",
    )
    writer.start()

    symbol_row = normalize_symbol_update(
        {
            "symbol": "NSE:TEST-EQ",
            "type": "sf",
            "ltp": 10.05,
            "last_traded_qty": 2,
            "vol_traded_today": 100,
        },
        common(1, "symbol_update", "NSE:TEST-EQ"),
    )
    depth_row = normalize_depth(
        "NSE:TEST-EQ",
        FakeDepth(),
        common(2, "tbt_depth", "NSE:TEST-EQ"),
        previous_sequence_no=None,
        include_raw_json=False,
    )

    try:
        submit(writer, "symbol_update", symbol_row)
        submit(writer, "tbt_depth", depth_row)

        deadline = time.monotonic() + 3
        market_files = []
        while time.monotonic() < deadline:
            market_files = [
                path
                for path in (tmp_path / "20260717").rglob("*.parquet")
                if path.parent.name in {"symbolupdate", "tbtdepth"}
            ]
            if len(market_files) == 2:
                break
            time.sleep(0.02)

        assert writer.is_alive()
        assert len(market_files) == 2
        assert all(pq.ParquetFile(path).metadata.num_rows == 1 for path in market_files)
    finally:
        writer.stop()


def test_timestamp_name_collision_never_overwrites_a_part(tmp_path) -> None:
    writer = ParquetEventWriter(
        output_dir=tmp_path,
        run_id="collision-run",
        flush_interval_seconds=60,
        max_rows_per_file=1,
        queue_max_events=100,
        queue_put_timeout_seconds=1,
        compression="zstd",
    )
    writer.start()

    for event_id in (1, 2):
        row = normalize_symbol_update(
            {"symbol": "NSE:TEST-EQ", "type": "sf", "ltp": 10.05},
            common(event_id, "symbol_update", "NSE:TEST-EQ"),
        )
        submit(writer, "symbol_update", row)
    writer.stop()

    files = sorted((tmp_path / "20260717" / "symbolupdate").glob("*.parquet"))
    assert len(files) == 2
    assert sum(pq.ParquetFile(path).metadata.num_rows for path in files) == 2


def test_writer_stop_timeout_is_bounded(tmp_path) -> None:
    writer = ParquetEventWriter(
        output_dir=tmp_path,
        run_id="blocked-run",
        flush_interval_seconds=60,
        max_rows_per_file=100,
        queue_max_events=100,
        queue_put_timeout_seconds=0.1,
        compression="zstd",
    )
    release = threading.Event()
    original_flush = writer._flush_all

    def blocked_flush() -> None:
        release.wait(timeout=1)
        original_flush()

    writer._flush_all = blocked_flush
    writer.start()

    with pytest.raises(TimeoutError, match="Parquet writer"):
        writer.stop(timeout_seconds=0.01)

    release.set()
    writer._thread.join(timeout=1)
    assert not writer.is_alive()
