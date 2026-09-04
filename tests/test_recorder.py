from __future__ import annotations

import io
import json
import logging
import threading
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq
import pytest

from tickrecorder.config import Settings
from tickrecorder.recorder import FyersTickRecorder


class FakeSock:
    def __init__(self) -> None:
        self.connected = True


class FakeWebSocket:
    def __init__(self) -> None:
        self.sock = FakeSock()
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1
        self.sock.connected = False


class FakeReconnectDataSocket:
    """Model FYERS callbacks that can run before a handshake succeeds."""

    def __init__(self) -> None:
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 20
        self._FyersDataSocket__ws_object: FakeWebSocket | None = None
        self.subscriptions: list[tuple[tuple[str, ...], str]] = []

    def start_attempt(self) -> None:
        self._FyersDataSocket__ws_object = None

    def complete_attempt(self) -> None:
        self._FyersDataSocket__ws_object = FakeWebSocket()

    def disconnect(self) -> None:
        websocket = self._FyersDataSocket__ws_object
        if websocket is not None:
            websocket.sock.connected = False

    def subscribe(self, symbols: list[str], data_type: str) -> None:
        self.subscriptions.append((tuple(symbols), data_type))


class FakeMonotonicClock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now

    def monotonic(self) -> float:
        return self.now

    def wait(self, timeout: float) -> bool:
        self.now += timeout
        return False


class FakeDepth:
    tbq = 1_000
    tsq = 900
    bidprice = [100.0 - index * 0.05 for index in range(50)]
    askprice = [100.05 + index * 0.05 for index in range(50)]
    bidqty = [index + 1 for index in range(50)]
    askqty = [index + 2 for index in range(50)]
    bidordn = [3] * 50
    askordn = [4] * 50
    snapshot = True
    timestamp = 100
    sendtime = 101
    seqNo = 42


class FakeSubscriptionInfo:
    def __init__(self) -> None:
        self.symbols: set[str] = set()
        self.channels: set[str] = set()
        self.symbols_by_channel: dict[str, set[str]] = {}

    def subscribe(self, symbols: set[str], channel: str, _mode: Any) -> None:
        self.symbols.update(symbols)
        self.channels.add(channel)
        self.symbols_by_channel.setdefault(channel, set()).update(symbols)

    def updateChannels(
        self,
        _pause_channels: set[str],
        resume_channels: set[str],
    ) -> None:
        self.channels.update(resume_channels)


class FakeSpacesClient:
    def __init__(
        self,
        remote_size_delta: int = 0,
        fail_dates: set[str] | None = None,
    ) -> None:
        self.remote_size_delta = remote_size_delta
        self.fail_dates = fail_dates or set()
        self.uploads: list[tuple[Path, str, str]] = []
        self._objects: dict[tuple[str, str], bytes] = {}
        self._metadata: dict[tuple[str, str], dict[str, str]] = {}

    def upload_file(
        self,
        local_path: str,
        bucket: str,
        key: str,
        ExtraArgs: dict[str, Any],
    ) -> None:
        path = Path(local_path)
        self.uploads.append((path, bucket, key))
        if any(f"/{trading_date}/" in key for trading_date in self.fail_dates):
            raise RuntimeError("simulated upload failure")
        self._objects[(bucket, key)] = path.read_bytes()
        self._metadata[(bucket, key)] = ExtraArgs["Metadata"]

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return {
            "ContentLength": len(self._objects[(Bucket, Key)]) + self.remote_size_delta,
            "ETag": '"test-etag"',
            "Metadata": self._metadata[(Bucket, Key)],
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        return {"Body": io.BytesIO(self._objects[(Bucket, Key)])}


class FakeDataSocket:
    def __init__(self, **callbacks: Any) -> None:
        self.on_connect = callbacks["on_connect"]
        self.on_close = callbacks["on_close"]
        self.on_message = callbacks["on_message"]
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = callbacks["reconnect_retry"]
        self.restart_flag = callbacks["reconnect"]
        self.background_flag = False
        self._FyersDataSocket__ws_object: FakeWebSocket | None = None
        self.symbols: list[str] = []

    def connect(self) -> None:
        self._FyersDataSocket__ws_object = FakeWebSocket()
        self.on_connect()
        for symbol in self.symbols:
            self.on_message(
                {
                    "symbol": symbol,
                    "type": "sf",
                    "ltp": 100.05,
                    "last_traded_qty": 5,
                    "last_traded_time": 1_000,
                    "vol_traded_today": 500,
                }
            )

    def subscribe(self, symbols: list[str], data_type: str) -> None:
        assert data_type == "SymbolUpdate"
        self.symbols = symbols

    def close_connection(self) -> None:
        if self._FyersDataSocket__ws_object is not None:
            self._FyersDataSocket__ws_object.sock.connected = False
        self.on_close({"s": "ok", "message": "closed"})


class FakeTbtSocket:
    _instance: FakeTbtSocket | None = None

    def __new__(cls, **_callbacks: Any) -> FakeTbtSocket:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, **callbacks: Any) -> None:
        self.on_open = callbacks["on_open"]
        self.on_close = callbacks["on_close"]
        self.on_depth_update = callbacks["on_depth_update"]
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = callbacks["reconnect_retry"]
        self.restart_flag = callbacks["reconnect"]
        self.background_flag = False
        self._subsinfo = FakeSubscriptionInfo()
        self._datastore = SimpleNamespace(depth={"stale": FakeDepth()})
        self._FyersTbtSocket__ws_object: FakeWebSocket | None = None
        self.running_thread: threading.Thread | None = None
        self._running = threading.Event()

    def keep_running(self) -> None:
        self._running.set()

        def loop() -> None:
            while self._running.is_set():
                time.sleep(0.01)

        self.running_thread = threading.Thread(target=loop, daemon=True)
        self.running_thread.start()

    def stop_running(self) -> None:
        self._running.clear()

    def connect(self) -> None:
        self._FyersTbtSocket__ws_object = FakeWebSocket()
        self.on_open()
        for symbol in sorted(self._subsinfo.symbols):
            self.on_depth_update(symbol, FakeDepth())

    def close_connection(self) -> None:
        self.stop_running()
        if self._FyersTbtSocket__ws_object is not None:
            self._FyersTbtSocket__ws_object.sock.connected = False
        if self.running_thread is not None:
            self.running_thread.join(timeout=1)
        self.on_close({"s": "ok", "message": "closed"})


def settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Settings:
    monkeypatch.setenv("FYERS_APP_ID", "APP-100")
    monkeypatch.setenv("FYERS_ACCESS_TOKEN", "test-secret")
    monkeypatch.setenv("FYERS_SYMBOLS", "NSE:TEST-EQ")
    monkeypatch.setenv(
        "DO_S3_ENDPOINT_URL", "https://sgp1.digitaloceanspaces.com"
    )
    monkeypatch.setenv("DO_S3_REGION", "sgp1")
    monkeypatch.setenv("DO_S3_ACCESS_KEY_ID", "spaces-access-key")
    monkeypatch.setenv("DO_S3_SECRET_ACCESS_KEY", "spaces-secret-key")
    monkeypatch.setenv("TICKRECORDER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("TICKRECORDER_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("TICKRECORDER_SDK_LOG_DIR", str(tmp_path / "sdk-logs"))
    monkeypatch.setenv("TICKRECORDER_DURATION_SECONDS", "0.01")
    monkeypatch.setenv("TICKRECORDER_CONNECT_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("TICKRECORDER_SHUTDOWN_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("TICKRECORDER_QUEUE_PUT_TIMEOUT_SECONDS", "0.1")
    monkeypatch.setenv("TICKRECORDER_FLUSH_INTERVAL_SECONDS", "0.1")
    return Settings.from_env(env_file=tmp_path / "missing.env")


def test_recorder_writes_complete_three_stream_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(settings(monkeypatch, tmp_path))
    spaces_client = FakeSpacesClient()
    recorder._spaces_client = spaces_client
    recorder.data_ws_module = SimpleNamespace(FyersDataSocket=FakeDataSocket)
    recorder.FyersTbtSocket = FakeTbtSocket
    recorder.SubscriptionModes = SimpleNamespace(DEPTH="depth")
    monkeypatch.setattr(recorder, "_load_sdk", lambda: None)
    archive_after_shutdown = recorder._archive_and_upload_trade_ticks

    def verify_shutdown_then_archive() -> None:
        assert not recorder.writer.is_alive()
        assert not recorder._socket_connected(recorder.data_socket)
        assert not recorder._socket_connected(recorder.tbt_socket)
        archive_after_shutdown()

    monkeypatch.setattr(
        recorder,
        "_archive_and_upload_trade_ticks",
        verify_shutdown_then_archive,
    )

    assert recorder.run() == 0
    assert recorder._final_status == "complete"

    run_root = recorder.writer.run_root
    assert (run_root / "_SUCCESS").exists()
    date_roots = [
        path
        for path in (tmp_path / "data").iterdir()
        if path.is_dir() and path.name.isdigit()
    ]
    assert len(date_roots) == 1
    date_root = date_roots[0]
    assert len(list((date_root / "symbolupdate").glob("*.parquet"))) == 1
    assert len(list((date_root / "tbtdepth").glob("*.parquet"))) == 1
    assert len(list((date_root / "control").glob("*.parquet"))) >= 1

    archive_path = tmp_path / "data" / f"{date_root.name}_trade_ticks.tar.gz"
    assert archive_path.is_file()
    assert spaces_client.uploads == [
        (
            archive_path,
            "index-bucket",
            (
                "index-bucket-holder/contracts/"
                f"{date_root.name}/{date_root.name}_trade_ticks.tar.gz"
            ),
        )
    ]

    manifest = json.loads((run_root / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert manifest["streams_ready"] is True
    assert manifest["fatal_error"] is None
    assert manifest["trade_tick_archives"][0]["upload_status"] == "verified"
    assert manifest["trade_tick_archives"][0]["etag"] == "test-etag"
    assert "test-secret" not in json.dumps(manifest)
    assert "spaces-secret-key" not in json.dumps(manifest)


def test_tbt_symbols_are_partitioned_across_distinct_connections(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    configured_symbols = tuple(
        f"NSE:TEST{index}-EQ" for index in range(10)
    )
    recorder = FyersTickRecorder(
        replace(
            settings(monkeypatch, tmp_path),
            symbols=configured_symbols,
            s3_upload_enabled=False,
        )
    )
    recorder.data_ws_module = SimpleNamespace(FyersDataSocket=FakeDataSocket)
    recorder.FyersTbtSocket = FakeTbtSocket
    recorder.SubscriptionModes = SimpleNamespace(DEPTH="depth")
    monkeypatch.setattr(recorder, "_load_sdk", lambda: None)

    assert recorder.run() == 0
    assert recorder._final_status == "complete"
    assert len(recorder.tbt_sockets) == 2
    assert recorder.tbt_sockets[0] is not recorder.tbt_sockets[1]
    assert all(
        len(socket_object._subsinfo.symbols) == 5
        for socket_object in recorder.tbt_sockets
    )
    assert {
        symbol
        for socket_object in recorder.tbt_sockets
        for symbol in socket_object._subsinfo.symbols
    } == set(configured_symbols)
    assert all(
        socket_object._datastore.depth == {}
        for socket_object in recorder.tbt_sockets
    )
    depth_rows = [
        row
        for depth_file in (
            next(
                path
                for path in (tmp_path / "data").iterdir()
                if path.is_dir() and path.name.isdigit()
            )
            / "tbtdepth"
        ).glob("*.parquet")
        for row in pq.ParquetFile(depth_file).read().to_pylist()
    ]
    symbols_by_connection: dict[str, set[str]] = {}
    for row in depth_rows:
        assert row["channel"] == "1"
        symbols_by_connection.setdefault(
            row["connection_id"],
            set(),
        ).add(row["symbol"])
    assert len(symbols_by_connection) == 2
    assert all(
        len(symbols) == 5 for symbols in symbols_by_connection.values()
    )

    manifest = json.loads(
        (recorder.writer.run_root / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["received_events"]["received_tbt_depth"] == 10
    assert manifest["connection_epochs"]["tbt:1"] == 1
    assert manifest["connection_epochs"]["tbt:2"] == 1


def test_upload_verification_failure_marks_run_failed_and_retains_archive(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(settings(monkeypatch, tmp_path))
    recorder._spaces_client = FakeSpacesClient(remote_size_delta=1)
    recorder.data_ws_module = SimpleNamespace(FyersDataSocket=FakeDataSocket)
    recorder.FyersTbtSocket = FakeTbtSocket
    recorder.SubscriptionModes = SimpleNamespace(DEPTH="depth")
    monkeypatch.setattr(recorder, "_load_sdk", lambda: None)

    assert recorder.run() == 1
    assert recorder._final_status == "failed"
    assert recorder.stop_reason == "archive_upload_failure"
    assert (recorder.writer.run_root / "_FAILED").is_file()
    assert not (recorder.writer.run_root / "_SUCCESS").exists()

    manifest = json.loads(
        (recorder.writer.run_root / "manifest.json").read_text(encoding="utf-8")
    )
    archive_record = manifest["trade_tick_archives"][0]
    assert archive_record["upload_status"] == "failed"
    assert Path(archive_record["archive_path"]).is_file()


def test_archive_upload_attempts_later_and_retained_dates_after_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(settings(monkeypatch, tmp_path))
    recorder._spaces_client = FakeSpacesClient(fail_dates={"20260717"})
    for trading_date in ("20260717", "20260718"):
        control_dir = tmp_path / "data" / trading_date / "control"
        control_dir.mkdir(parents=True)
        (control_dir / "part-control.parquet").write_bytes(b"data")
    recorder.writer._parts = [
        {"path": "20260718/control/part-control.parquet"},
    ]

    with pytest.raises(RuntimeError, match="20260717"):
        recorder._archive_and_upload_trade_ticks()

    assert len(recorder._spaces_client.uploads) == 2
    records = {
        record["trading_date"]: record
        for record in recorder._trade_tick_archives
    }
    assert records["20260717"]["upload_status"] == "failed"
    assert records["20260718"]["upload_status"] == "verified"
    assert not (tmp_path / "data" / "_uploads" / "20260717.json").exists()
    assert (tmp_path / "data" / "_uploads" / "20260718.json").is_file()


def test_disabled_s3_creates_local_archive_without_upload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(
        replace(settings(monkeypatch, tmp_path), s3_upload_enabled=False)
    )
    recorder._spaces_client = FakeSpacesClient()
    trading_date = "20260718"
    control_dir = tmp_path / "data" / trading_date / "control"
    control_dir.mkdir(parents=True)
    (control_dir / "part-control.parquet").write_bytes(b"data")
    recorder.writer._parts = [
        {"path": f"{trading_date}/control/part-control.parquet"},
    ]

    recorder._archive_and_upload_trade_ticks()

    archive_path = tmp_path / "data" / f"{trading_date}_trade_ticks.tar.gz"
    assert archive_path.is_file()
    assert recorder._spaces_client.uploads == []
    assert len(recorder._trade_tick_archives) == 1
    archive_record = recorder._trade_tick_archives[0]
    assert archive_record["trading_date"] == trading_date
    assert archive_record["archive_path"] == str(archive_path)
    assert archive_record["file_count"] == 1
    assert archive_record["size_bytes"] == archive_path.stat().st_size
    assert archive_record["upload_status"] == "disabled"
    assert archive_record["bucket_name"] is None
    assert archive_record["object_key"] is None
    assert not (tmp_path / "data" / "_uploads" / f"{trading_date}.json").exists()


def test_data_directory_lock_prevents_overlapping_recorders(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder_one = FyersTickRecorder(settings(monkeypatch, tmp_path))
    recorder_two = FyersTickRecorder(settings(monkeypatch, tmp_path))
    recorder_one._prepare_directories()
    monkeypatch.setattr(recorder_one.writer, "is_alive", lambda: True)
    recorder_one._release_data_directory_lock_if_safe()
    with pytest.raises(RuntimeError, match="already using data directory"):
        recorder_two._prepare_directories()

    monkeypatch.setattr(recorder_one.writer, "is_alive", lambda: False)
    recorder_one._release_data_directory_lock_if_safe()
    recorder_two._prepare_directories()
    recorder_two._release_data_directory_lock()


def test_market_open_waits_in_code_until_0915(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(settings(monkeypatch, tmp_path))
    ist = ZoneInfo("Asia/Kolkata")
    current = [datetime(2026, 7, 17, 8, 0, tzinfo=ist)]
    waits: list[float] = []

    monkeypatch.setattr(recorder, "_now_market", lambda: current[0])

    def advance_to_open(timeout: float) -> bool:
        waits.append(timeout)
        current[0] = datetime(2026, 7, 17, 9, 15, tzinfo=ist)
        return False

    monkeypatch.setattr(recorder.stop_event, "wait", advance_to_open)

    market_close = recorder._wait_for_market_open()

    assert waits == [30.0]
    assert market_close == datetime(2026, 7, 17, 15, 31, tzinfo=ist)


def test_market_window_closed_job_does_not_connect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(
        replace(settings(monkeypatch, tmp_path), duration_seconds=0)
    )
    ist = ZoneInfo("Asia/Kolkata")
    monkeypatch.setattr(
        recorder,
        "_now_market",
        lambda: datetime(2026, 7, 17, 15, 31, tzinfo=ist),
    )
    monkeypatch.setattr(
        recorder,
        "start",
        lambda: pytest.fail("recorder must not connect after market close"),
    )

    assert recorder.run() == 0
    assert recorder.stop_reason == "market_window_closed"


def test_market_close_watcher_requests_disconnect_at_1531(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(settings(monkeypatch, tmp_path))
    ist = ZoneInfo("Asia/Kolkata")
    market_close = datetime(2026, 7, 17, 15, 31, tzinfo=ist)
    monkeypatch.setattr(recorder, "_now_market", lambda: market_close)

    recorder._start_market_close_watcher(market_close)
    recorder._join_market_close_watcher()

    assert recorder.stop_event.is_set()
    assert recorder.stop_reason == "market_close"


def test_first_fatal_reason_is_preserved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(settings(monkeypatch, tmp_path))

    first = RuntimeError("first")
    recorder._set_fatal(first, "first_reason")
    recorder._set_fatal(RuntimeError("second"), "second_reason")

    assert recorder._fatal_error is first
    assert recorder.stop_reason == "first_reason"


def test_established_data_false_open_callback_does_not_reuse_initial_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(
        replace(
            settings(monkeypatch, tmp_path),
            connect_timeout_seconds=15,
            disconnect_grace_seconds=30,
        )
    )
    socket = FakeReconnectDataSocket()
    recorder.data_socket = socket
    recorder._ready_events["data"].set()
    recorder._streams_ready = True
    recorder._connection_epochs["data"] = 1
    original_connection_id = recorder._connection_ids["data"]
    clock = FakeMonotonicClock()
    controls: list[tuple[str, str, Any, dict[str, Any]]] = []

    monkeypatch.setattr("tickrecorder.recorder.time.monotonic", clock.monotonic)
    monkeypatch.setattr(recorder.stop_event, "wait", clock.wait)
    monkeypatch.setattr(
        recorder,
        "_safe_control",
        lambda component, event_type, details, **kwargs: controls.append(
            (component, event_type, details, kwargs)
        ),
    )

    # The pinned FYERS data SDK invokes its application callback after a fixed
    # delay even when no WebSocket handshake completed.
    socket.start_attempt()
    recorder.on_data_open()

    assert clock.now == 100.0
    assert recorder._fatal_error is None
    assert not recorder.stop_event.is_set()
    assert recorder.stop_reason == "not_stopped"
    assert recorder._connection_epochs["data"] == 1
    assert recorder._connection_ids["data"] == original_connection_id
    assert socket.subscriptions == []
    assert all(event_type != "connect_timeout" for _, event_type, _, _ in controls)


def test_health_poll_reconciles_data_reconnect_once_after_false_callback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(
        replace(
            settings(monkeypatch, tmp_path),
            connect_timeout_seconds=15,
            disconnect_grace_seconds=30,
            stale_feed_timeout_seconds=0,
        )
    )
    socket = FakeReconnectDataSocket()
    recorder.data_socket = socket
    clock = FakeMonotonicClock()
    controls: list[tuple[str, str, Any, dict[str, Any]]] = []

    monkeypatch.setattr("tickrecorder.recorder.time.monotonic", clock.monotonic)
    monkeypatch.setattr(recorder.stop_event, "wait", clock.wait)
    monkeypatch.setattr(
        recorder,
        "_safe_control",
        lambda component, event_type, details, **kwargs: controls.append(
            (component, event_type, details, kwargs)
        ),
    )

    socket.complete_attempt()
    recorder.on_data_open()
    recorder._streams_ready = True
    initial_connection_id = recorder._connection_ids["data"]

    socket.disconnect()
    recorder._check_feed_health()
    outage_started_at = clock.now
    recorder._check_feed_health()
    assert recorder._disconnected_since["data"] == outage_started_at

    clock.now = 110.0
    socket.start_attempt()
    recorder.on_data_open()
    recorder.on_data_open()
    assert recorder._fatal_error is None

    # FYERS does not issue another application callback when the underlying
    # handshake later succeeds, so health polling must discover and reconcile it.
    clock.now = 120.0
    socket.complete_attempt()
    recorder._check_feed_health()
    recorder._check_feed_health()
    recorder.on_data_open()

    event_types = [event_type for _, event_type, _, _ in controls]
    assert recorder._fatal_error is None
    assert not recorder.stop_event.is_set()
    assert recorder._disconnected_since["data"] is None
    assert recorder._connection_epochs["data"] == 2
    assert recorder._connection_ids["data"] != initial_connection_id
    assert socket.subscriptions == [
        (("NSE:TEST-EQ",), "SymbolUpdate"),
        (("NSE:TEST-EQ",), "SymbolUpdate"),
    ]
    assert event_types.count("disconnect_detected") == 1
    assert event_types.count("socket_open") == 2
    assert event_types.count("subscribe_sent") == 2
    assert "connect_timeout" not in event_types
    assert "data_connection_interrupted" in recorder._degraded_reasons
    assert "data_reconnected" in recorder._degraded_reasons


def test_initial_data_false_open_callback_still_times_out(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(
        replace(settings(monkeypatch, tmp_path), connect_timeout_seconds=1)
    )
    socket = FakeReconnectDataSocket()
    recorder.data_socket = socket
    clock = FakeMonotonicClock()
    controls: list[tuple[str, str, Any, dict[str, Any]]] = []

    monkeypatch.setattr("tickrecorder.recorder.time.monotonic", clock.monotonic)
    monkeypatch.setattr(recorder.stop_event, "wait", clock.wait)
    monkeypatch.setattr(
        recorder,
        "_safe_control",
        lambda component, event_type, details, **kwargs: controls.append(
            (component, event_type, details, kwargs)
        ),
    )

    socket.start_attempt()
    recorder.on_data_open()

    assert clock.now == pytest.approx(101.0)
    assert isinstance(recorder._fatal_error, RuntimeError)
    assert recorder.stop_event.is_set()
    assert recorder.stop_reason == "data_connect_timeout"
    assert not recorder._ready_events["data"].is_set()
    assert recorder._connection_epochs["data"] == 0
    assert socket.subscriptions == []
    assert [event_type for _, event_type, _, _ in controls] == ["connect_timeout"]


def test_established_data_disconnect_fails_at_grace_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(
        replace(
            settings(monkeypatch, tmp_path),
            connect_timeout_seconds=15,
            disconnect_grace_seconds=30,
            stale_feed_timeout_seconds=0,
        )
    )
    socket = FakeReconnectDataSocket()
    recorder.data_socket = socket
    clock = FakeMonotonicClock()
    controls: list[tuple[str, str, Any, dict[str, Any]]] = []

    monkeypatch.setattr("tickrecorder.recorder.time.monotonic", clock.monotonic)
    monkeypatch.setattr(recorder.stop_event, "wait", clock.wait)
    monkeypatch.setattr(
        recorder,
        "_safe_control",
        lambda component, event_type, details, **kwargs: controls.append(
            (component, event_type, details, kwargs)
        ),
    )

    socket.complete_attempt()
    recorder.on_data_open()
    recorder._streams_ready = True
    socket.disconnect()
    recorder._check_feed_health()
    outage_started_at = clock.now

    clock.now = outage_started_at + 29.999
    recorder._check_feed_health()
    assert recorder._fatal_error is None
    assert not recorder.stop_event.is_set()

    clock.now = outage_started_at + 30.0
    recorder._check_feed_health()

    event_types = [event_type for _, event_type, _, _ in controls]
    timeout_details = next(
        details for _, event_type, details, _ in controls if event_type == "data_disconnect_timeout"
    )
    assert isinstance(recorder._fatal_error, RuntimeError)
    assert recorder.stop_event.is_set()
    assert recorder.stop_reason == "data_disconnect_timeout"
    assert event_types.count("disconnect_detected") == 1
    assert event_types.count("data_disconnect_timeout") == 1
    assert "connect_timeout" not in event_types
    assert timeout_details["outage_seconds"] == pytest.approx(30.0)
    assert timeout_details["grace_seconds"] == 30
    assert timeout_details["reconnect_attempts"] == 0
    assert timeout_details["max_reconnect_attempts"] == 20


def test_health_dispatches_blocking_data_subscribe_and_stop_releases_callback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    subscribe_started = threading.Event()
    release_subscribe = threading.Event()
    callback_returned = threading.Event()
    main_thread = threading.current_thread()

    class BlockingSubscribeDataSocket(FakeReconnectDataSocket):
        def __init__(self) -> None:
            super().__init__()
            self.subscribe_thread: threading.Thread | None = None

        def subscribe(self, symbols: list[str], data_type: str) -> None:
            self.subscribe_thread = threading.current_thread()
            subscribe_started.set()
            if self.subscribe_thread is main_thread:
                raise AssertionError("health polling subscribed on MainThread")
            release_subscribe.wait()
            super().subscribe(symbols, data_type)

        def close_connection(self) -> None:
            self.disconnect()

    recorder = FyersTickRecorder(
        replace(
            settings(monkeypatch, tmp_path),
            stale_feed_timeout_seconds=0,
            shutdown_timeout_seconds=0.05,
        )
    )
    socket = BlockingSubscribeDataSocket()
    socket.complete_attempt()
    recorder.data_socket = socket
    recorder._ready_events["data"].set()
    recorder._streams_ready = True
    recorder._connection_epochs["data"] = 1
    controls: list[tuple[str, str, Any, dict[str, Any]]] = []
    monkeypatch.setattr(
        recorder,
        "_safe_control",
        lambda component, event_type, details, **kwargs: controls.append(
            (component, event_type, details, kwargs)
        ),
    )

    def sdk_callback() -> None:
        recorder.on_data_open()
        callback_returned.set()

    callback_thread = threading.Thread(
        target=sdk_callback,
        name="test-sdk-data-open",
        daemon=True,
    )

    try:
        recorder._check_feed_health()
        assert recorder._fatal_error is None
        callback_thread.start()

        assert subscribe_started.wait(timeout=0.5)
        assert socket.subscribe_thread is not main_thread
        assert not callback_returned.is_set()
        assert len(recorder._data_connection_workers) == 1
        worker = recorder._data_connection_workers[0]
        assert worker.is_alive()
        assert worker.daemon is True

        recorder.request_stop("test_stop")

        assert callback_returned.wait(timeout=0.5)
        callback_thread.join(timeout=0.1)
        assert not callback_thread.is_alive()
        assert worker.is_alive()

        recorder._shutdown_sockets()

        shutdown_details = next(
            details
            for _, event_type, details, _ in controls
            if event_type == "socket_shutdown"
        )
        assert any(
            "data-reconciler: worker thread still alive" in error
            for error in shutdown_details["errors"]
        )
        assert {
            "component": "data-reconciler",
            "name": worker.name,
            "alive": True,
            "daemon": True,
        } in shutdown_details["worker_threads"]
        assert recorder.stop_reason == "socket_shutdown_failure"
    finally:
        release_subscribe.set()
        if callback_thread.ident is not None:
            callback_thread.join(timeout=0.5)
        for worker in recorder._data_connection_workers:
            worker.join(timeout=0.5)

    assert all(not worker.is_alive() for worker in recorder._data_connection_workers)


def test_final_in_flight_data_retry_is_not_exhausted_until_thread_stops(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(
        replace(
            settings(monkeypatch, tmp_path),
            disconnect_grace_seconds=30,
            stale_feed_timeout_seconds=0,
        )
    )
    socket = FakeReconnectDataSocket()
    socket.start_attempt()
    socket.reconnect_attempts = socket.max_reconnect_attempts
    recorder.data_socket = socket
    recorder._ready_events["data"].set()
    recorder._streams_ready = True
    controls: list[tuple[str, str, Any, dict[str, Any]]] = []
    monkeypatch.setattr(
        recorder,
        "_safe_control",
        lambda component, event_type, details, **kwargs: controls.append(
            (component, event_type, details, kwargs)
        ),
    )

    attempt_started = threading.Event()
    finish_attempt = threading.Event()

    def final_attempt() -> None:
        attempt_started.set()
        finish_attempt.wait()

    socket.ws_thread = threading.Thread(
        target=final_attempt,
        name="test-final-data-reconnect",
        daemon=True,
    )
    recorder._check_feed_health()
    assert recorder._fatal_error is None
    assert not recorder.stop_event.is_set()

    socket.ws_thread.start()

    try:
        assert attempt_started.wait(timeout=0.5)
        recorder._check_feed_health()

        event_types = [event_type for _, event_type, _, _ in controls]
        assert recorder._fatal_error is None
        assert not recorder.stop_event.is_set()
        assert event_types.count("disconnect_detected") == 1
        assert "data_reconnect_exhausted" not in event_types

        finish_attempt.set()
        socket.ws_thread.join(timeout=0.5)
        assert not socket.ws_thread.is_alive()

        recorder._check_feed_health()
    finally:
        finish_attempt.set()
        socket.ws_thread.join(timeout=0.5)

    event_types = [event_type for _, event_type, _, _ in controls]
    exhausted_details = next(
        details
        for _, event_type, details, _ in controls
        if event_type == "data_reconnect_exhausted"
    )
    assert isinstance(recorder._fatal_error, RuntimeError)
    assert recorder.stop_event.is_set()
    assert recorder.stop_reason == "data_reconnect_exhausted"
    assert event_types.count("data_reconnect_exhausted") == 1
    assert exhausted_details["reconnect_attempts"] == 20
    assert exhausted_details["max_reconnect_attempts"] == 20
    assert exhausted_details["outage_seconds"] < 30


def test_control_action_is_logged_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    recorder = FyersTickRecorder(settings(monkeypatch, tmp_path))
    submitted: list[tuple[str, dict[str, Any]]] = []
    monkeypatch.setattr(
        recorder,
        "_submit",
        lambda source, row: submitted.append((source, row)),
    )
    caplog.set_level(logging.INFO, logger="tickrecorder.recorder")

    recorder._control(
        "data",
        "socket_open",
        {
            "token": "APP-100:test-secret",
            "spaces_secret": "spaces-secret-key",
        },
        message=(
            "connected with APP-100:test-secret and spaces-secret-key"
        ),
    )

    assert "Action component=data event=socket_open" in caplog.text
    assert "connected with <redacted> and <redacted>" in caplog.text
    assert "test-secret" not in caplog.text
    assert "spaces-secret-key" not in caplog.text
    assert "test-secret" not in json.dumps(submitted, default=str)
    assert "spaces-secret-key" not in json.dumps(submitted, default=str)


def test_stalled_valid_callback_stream_is_fatal(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder_settings = replace(
        settings(monkeypatch, tmp_path),
        stale_feed_timeout_seconds=1,
        stale_feed_retries=0,
    )
    recorder = FyersTickRecorder(recorder_settings)
    recorder._streams_ready = True
    recorder._last_valid_event_monotonic["data"]["NSE:TEST-EQ"] = (
        time.monotonic() - 2
    )
    monkeypatch.setattr(recorder, "_safe_control", lambda *args, **kwargs: None)

    assert recorder._check_feed_staleness("data", time.monotonic()) is True
    assert recorder.stop_reason == "data_feed_stale"


def test_stale_tbt_feed_restarts_transport_before_failing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(
        replace(
            settings(monkeypatch, tmp_path),
            stale_feed_timeout_seconds=1,
            stale_feed_retries=2,
        )
    )
    socket = FakeTbtSocket(
        on_open=lambda: None,
        on_close=lambda message: None,
        on_depth_update=lambda ticker, message: None,
        reconnect_retry=20,
        reconnect=True,
    )
    socket._FyersTbtSocket__ws_object = FakeWebSocket()
    recorder.tbt_sockets[0] = socket
    recorder.tbt_socket = socket
    recorder._streams_ready = True
    recorder._ready_events["tbt"].set()
    recorder._tbt_ready_connections.add("tbt:1")
    controls: list[tuple[str, str, Any, dict[str, Any]]] = []
    monkeypatch.setattr(
        recorder,
        "_safe_control",
        lambda component, event_type, details, **kwargs: controls.append(
            (component, event_type, details, kwargs)
        ),
    )
    now = time.monotonic()
    recorder._last_valid_event_monotonic["tbt"]["NSE:TEST-EQ"] = now - 2

    assert recorder._check_feed_staleness("tbt", now) is False
    assert recorder._fatal_error is None
    assert socket._FyersTbtSocket__ws_object.close_calls == 1
    assert recorder._stale_feed_retry_attempts["tbt:1"] == 1
    retry = next(details for _, event, details, _ in controls if event == "feed_stale_retry")
    assert retry["attempt"] == 1
    assert retry["max_retries"] == 2

    later = now + 1
    assert recorder._check_feed_staleness("tbt", later) is False
    assert recorder._stale_feed_retry_attempts["tbt:1"] == 2

    assert recorder._check_feed_staleness("tbt", later + 1) is True
    assert recorder.stop_reason == "tbt_feed_stale"


def test_fresh_callbacks_reset_stale_retry_budget(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    recorder = FyersTickRecorder(settings(monkeypatch, tmp_path))
    controls: list[tuple[str, str, Any, dict[str, Any]]] = []
    monkeypatch.setattr(
        recorder,
        "_safe_control",
        lambda component, event_type, details, **kwargs: controls.append(
            (component, event_type, details, kwargs)
        ),
    )
    recorder._stale_feed_retry_attempts["tbt:1"] = 1
    recorder._stale_feed_retry_started_at["tbt:1"] = time.monotonic() - 1
    recorder._last_valid_event_monotonic["tbt"]["NSE:TEST-EQ"] = time.monotonic()

    recorder._record_stale_feed_recovery("tbt", "NSE:TEST-EQ")

    assert recorder._stale_feed_retry_attempts["tbt:1"] == 0
    assert recorder._stale_feed_retry_started_at["tbt:1"] is None
    assert [event for _, event, _, _ in controls] == ["feed_recovered"]
