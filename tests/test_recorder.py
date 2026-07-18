from __future__ import annotations

import json
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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
    monkeypatch.setenv("FYERS_WS_TOKEN", "APP-100:test-secret")
    monkeypatch.setenv("FYERS_SYMBOLS", "NSE:TEST-EQ")
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
    recorder.data_ws_module = SimpleNamespace(FyersDataSocket=FakeDataSocket)
    recorder.FyersTbtSocket = FakeTbtSocket
    recorder.SubscriptionModes = SimpleNamespace(DEPTH="depth")
    monkeypatch.setattr(recorder, "_load_sdk", lambda: None)

    assert recorder.run() == 0
    assert recorder._final_status == "complete"

    run_root = recorder.writer.run_root
    assert (run_root / "_SUCCESS").exists()
    date_root = tmp_path / "data" / "20260717"
    assert len(list((date_root / "symbolupdate").glob("*.parquet"))) == 1
    assert len(list((date_root / "tbtdepth").glob("*.parquet"))) == 1
    assert len(list((date_root / "control").glob("*.parquet"))) >= 1

    manifest = json.loads((run_root / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert manifest["streams_ready"] is True
    assert manifest["fatal_error"] is None
    assert "test-secret" not in json.dumps(manifest)


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
