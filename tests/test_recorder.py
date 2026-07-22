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

import pytest

from tickrecorder.config import Settings
from tickrecorder.recorder import FyersTickRecorder


class FakeSock:
    def __init__(self) -> None:
        self.connected = True


class FakeWebSocket:
    def __init__(self) -> None:
        self.sock = FakeSock()


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

    def subscribe(self, symbols: set[str], channel: str, _mode: Any) -> None:
        self.symbols.update(symbols)
        self.channels.add(channel)

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

    def connect(self) -> None:
        self._FyersDataSocket__ws_object = FakeWebSocket()
        self.on_connect()
        self.on_message(
            {
                "symbol": "NSE:TEST-EQ",
                "type": "sf",
                "ltp": 100.05,
                "last_traded_qty": 5,
                "last_traded_time": 1_000,
                "vol_traded_today": 500,
            }
        )

    def subscribe(self, symbols: list[str], data_type: str) -> None:
        assert symbols == ["NSE:TEST-EQ"]
        assert data_type == "SymbolUpdate"

    def close_connection(self) -> None:
        if self._FyersDataSocket__ws_object is not None:
            self._FyersDataSocket__ws_object.sock.connected = False
        self.on_close({"s": "ok", "message": "closed"})


class FakeTbtSocket:
    def __init__(self, **callbacks: Any) -> None:
        self.on_open = callbacks["on_open"]
        self.on_close = callbacks["on_close"]
        self.on_depth_update = callbacks["on_depth_update"]
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = callbacks["reconnect_retry"]
        self.restart_flag = callbacks["reconnect"]
        self.background_flag = False
        self._subsinfo = FakeSubscriptionInfo()
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
        self.on_depth_update("NSE:TEST-EQ", FakeDepth())

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
    )
    recorder = FyersTickRecorder(recorder_settings)
    recorder._streams_ready = True
    recorder._last_valid_event_monotonic["data"]["NSE:TEST-EQ"] = (
        time.monotonic() - 2
    )
    monkeypatch.setattr(recorder, "_safe_control", lambda *args, **kwargs: None)

    assert recorder._check_feed_staleness("data", time.monotonic()) is True
    assert recorder.stop_reason == "data_feed_stale"
