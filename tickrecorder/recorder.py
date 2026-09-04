from __future__ import annotations

import fcntl
import importlib.metadata
import os
import platform
import socket
import sys
import threading
import time
import uuid
from collections import Counter
from collections.abc import Mapping
from datetime import datetime, time as wall_time, timezone
from typing import Any
from zoneinfo import ZoneInfo

from tickrecorder import __version__
from tickrecorder.config import Settings, TBT_SYMBOLS_PER_CONNECTION
from tickrecorder.logger import create_logger
from tickrecorder.normalizers import (
    normalize_control_event,
    normalize_depth,
    normalize_symbol_update,
)
from tickrecorder.schemas import SCHEMA_VERSION
from tickrecorder.spaces import (
    DigitalOceanSpacesConfig,
    create_trade_ticks_archive,
    finalized_trading_dates,
    pending_trading_dates,
    upload_trade_ticks_archive,
    write_upload_receipt_atomic,
)
from tickrecorder.writer import EventEnvelope, ParquetEventWriter


LOG = create_logger(__name__)
MARKET_OPEN_TIME = wall_time(9, 15)
MARKET_CLOSE_TIME = wall_time(15, 31)
DATA_CALLBACK_SETTLE_SECONDS = 0.05
DATA_HEALTH_SETTLE_SECONDS = 2.0


class FyersTickRecorder:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.run_id = str(uuid.uuid4())
        self.local_timezone = ZoneInfo(settings.timezone)
        self.market_timezone = ZoneInfo("Asia/Kolkata")
        self.stop_event = threading.Event()
        self.stop_reason = "not_stopped"

        self.writer = ParquetEventWriter(
            output_dir=settings.data_dir,
            run_id=self.run_id,
            flush_interval_seconds=settings.flush_interval_seconds,
            max_rows_per_file=settings.max_rows_per_file,
            queue_max_events=settings.queue_max_events,
            queue_put_timeout_seconds=settings.queue_put_timeout_seconds,
            compression=settings.parquet_compression,
        )

        self._event_lock = threading.Lock()
        self._next_event_id = 1
        self._stats_lock = threading.Lock()
        self._stats: Counter[str] = Counter()
        self._state_lock = threading.RLock()
        self._data_connection_lock = threading.RLock()
        self._fatal_lock = threading.Lock()
        self._quality_lock = threading.Lock()
        self._submission_condition = threading.Condition()
        self._accepting_submissions = True
        self._active_submissions = 0
        self._connection_ids = {
            "process": self.run_id,
            "data": str(uuid.uuid4()),
            "tbt": str(uuid.uuid4()),
            "writer": str(uuid.uuid4()),
        }
        self._connection_epochs = {"process": 0, "data": 0, "tbt": 0, "writer": 0}
        self._last_sequence_by_symbol: dict[str, int] = {}
        self._tbt_symbol_groups = tuple(
            tuple(
                settings.symbols[
                    offset : offset + TBT_SYMBOLS_PER_CONNECTION
                ]
            )
            for offset in range(
                0,
                len(settings.symbols),
                TBT_SYMBOLS_PER_CONNECTION,
            )
        )
        self._tbt_connection_keys = tuple(
            f"tbt:{index + 1}"
            for index in range(len(self._tbt_symbol_groups))
        )
        for connection_key in self._tbt_connection_keys:
            self._connection_ids[connection_key] = str(uuid.uuid4())
            self._connection_epochs[connection_key] = 0
        self._tbt_connection_by_symbol = {
            symbol.upper(): connection_key
            for connection_key, symbols in zip(
                self._tbt_connection_keys,
                self._tbt_symbol_groups,
                strict=True,
            )
            for symbol in symbols
        }
        self._tbt_symbols_seen_in_epoch = {
            connection_key: set()
            for connection_key in self._tbt_connection_keys
        }
        self._tbt_ready_connections: set[str] = set()
        self._ready_events = {
            "data": threading.Event(),
            "tbt": threading.Event(),
        }
        self._first_event_events = {
            "data": threading.Event(),
            "tbt": threading.Event(),
        }
        self._first_symbols_seen: dict[str, set[str]] = {
            "data": set(),
            "tbt": set(),
        }
        self._last_valid_event_monotonic: dict[str, dict[str, float]] = {
            "data": {},
            "tbt": {},
        }
        self._disconnected_since: dict[str, float | None] = {
            "data": None,
            "tbt": None,
            **{
                connection_key: None
                for connection_key in self._tbt_connection_keys
            },
        }
        self._stale_feed_retry_attempts = {
            "data": 0,
            **{connection_key: 0 for connection_key in self._tbt_connection_keys},
        }
        self._stale_feed_retry_started_at: dict[str, float | None] = {
            connection_key: None
            for connection_key in self._stale_feed_retry_attempts
        }
        self._degraded_reasons: set[str] = set()
        self._streams_ready = False
        self._threads: list[threading.Thread] = []
        self._started_at_ns: int | None = None
        self._ended_at_ns: int | None = None
        self._clean_shutdown = False
        self._final_status: str | None = None
        self._fatal_error: BaseException | None = None
        self._trade_tick_archives: list[dict[str, Any]] = []
        self._spaces_client: Any = None
        self._data_lock_handle: Any = None
        self._market_close_thread: threading.Thread | None = None
        self._last_status_at = 0.0
        self._started = False
        self._configured_symbols_upper = {
            configured.upper() for configured in self.settings.symbols
        }
        self._data_connection_reconciliations: list[dict[str, Any]] = []
        self._pending_data_connection_reconciliations: list[dict[str, Any]] = []
        self._data_connection_worker: threading.Thread | None = None
        self._data_connection_workers: list[threading.Thread] = []

        self.data_socket: Any = None
        self.tbt_socket: Any = None
        self.tbt_sockets: list[Any] = [
            None for _symbols in self._tbt_symbol_groups
        ]
        self.data_ws_module: Any = None
        self.FyersTbtSocket: Any = None
        self.SubscriptionModes: Any = None

    def _set_fatal(self, exc: BaseException, reason: str) -> None:
        first_failure = False
        with self._fatal_lock:
            if self._fatal_error is None:
                self._fatal_error = exc
                self.stop_reason = reason
                first_failure = True
        if first_failure:
            LOG.error(
                "Fatal recorder condition reason=%s error=%s",
                reason,
                type(exc).__name__,
            )
        self.stop_event.set()

    def _mark_degraded(self, reason: str) -> None:
        with self._quality_lock:
            first_occurrence = reason not in self._degraded_reasons
            self._degraded_reasons.add(reason)
        if first_occurrence:
            LOG.warning("Run marked degraded: %s", reason)

    def _redact_text(self, value: Any) -> str:
        rendered = str(value)
        secrets = {
            self.settings.ws_token,
            self.settings.ws_token.split(":", 1)[-1],
            self.settings.app_id,
            self.settings.do_s3_access_key_id,
            self.settings.do_s3_secret_access_key,
        }
        for secret in sorted((item for item in secrets if item), key=len, reverse=True):
            rendered = rendered.replace(secret, "<redacted>")
        return rendered

    def _redact_value(self, value: Any) -> Any:
        if isinstance(value, Mapping):
            return {
                str(key): self._redact_value(item)
                for key, item in value.items()
            }
        if isinstance(value, (list, tuple, set)):
            return [self._redact_value(item) for item in value]
        if isinstance(value, str):
            return self._redact_text(value)
        return value

    def _event_id(self) -> int:
        with self._event_lock:
            value = self._next_event_id
            self._next_event_id += 1
            return value

    def _common_fields(
        self,
        source: str,
        component: str,
        symbol: str | None,
        channel: str | None = None,
        connection_key: str | None = None,
    ) -> dict[str, Any]:
        wall_ns = time.time_ns()
        monotonic_ns = time.monotonic_ns()
        utc_dt = datetime.fromtimestamp(wall_ns / 1_000_000_000, tz=timezone.utc)
        local_dt = utc_dt.astimezone(self.local_timezone)
        with self._state_lock:
            state_key = connection_key or component
            connection_id = self._connection_ids[state_key]
            connection_epoch = self._connection_epochs[state_key]
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "event_id": self._event_id(),
            "source": source,
            "connection_id": connection_id,
            "connection_epoch": connection_epoch,
            "symbol": symbol,
            "channel": channel,
            "received_at_unix_ns": wall_ns,
            "received_at_monotonic_ns": monotonic_ns,
            "received_at_utc": utc_dt,
            "received_at_local": local_dt.isoformat(timespec="microseconds"),
            "trading_date": local_dt.strftime("%Y-%m-%d"),
            "receipt_hour": local_dt.strftime("%H"),
        }

    def _submit(self, source: str, row: dict[str, Any]) -> None:
        envelope = EventEnvelope(
            source=source,
            trading_date=row["trading_date"],
            receipt_hour=row["receipt_hour"],
            row=row,
        )
        with self._submission_condition:
            if not self._accepting_submissions:
                raise RuntimeError("Recorder is no longer accepting callback events")
            self._active_submissions += 1
        try:
            self.writer.submit(envelope)
            with self._stats_lock:
                self._stats[f"received_{source}"] += 1
        except BaseException as exc:
            self._set_fatal(exc, "writer_backpressure_or_failure")
            raise
        finally:
            with self._submission_condition:
                self._active_submissions -= 1
                self._submission_condition.notify_all()

    def _control(
        self,
        component: str,
        event_type: str,
        details: Any,
        *,
        severity: str = "INFO",
        message: str | None = None,
        symbol: str | None = None,
        connection_key: str | None = None,
    ) -> None:
        common = self._common_fields(
            "control",
            component,
            symbol,
            self.settings.tbt_channel if component == "tbt" else None,
            connection_key,
        )
        redacted_message = self._redact_text(message) if message is not None else None
        redacted_details = self._redact_value(details)
        log_method = getattr(LOG, severity.lower(), LOG.info)
        connection_label = connection_key or component
        if redacted_message is None:
            log_method(
                "Action component=%s event=%s symbol=%s connection=%s",
                component,
                event_type,
                symbol or "-",
                connection_label,
            )
        else:
            log_method(
                "Action component=%s event=%s symbol=%s connection=%s message=%s",
                component,
                event_type,
                symbol or "-",
                connection_label,
                redacted_message,
            )
        row = normalize_control_event(
            common=common,
            component=component,
            event_type=event_type,
            severity=severity,
            message=redacted_message,
            details=redacted_details,
        )
        self._submit("control", row)

    def _new_connection(
        self,
        component: str,
        connection_key: str | None = None,
    ) -> None:
        state_key = connection_key or component
        with self._state_lock:
            previous_epoch = self._connection_epochs[state_key]
            self._connection_epochs[state_key] += 1
            connection_epoch = self._connection_epochs[state_key]
            self._connection_ids[state_key] = str(uuid.uuid4())
            if component == "tbt":
                self._tbt_symbols_seen_in_epoch[state_key].clear()
            if state_key in self._disconnected_since:
                self._disconnected_since[state_key] = None
        LOG.info(
            "Connection opened component=%s connection=%s epoch=%d reconnect=%s",
            component,
            state_key,
            connection_epoch,
            previous_epoch > 0,
        )
        if component in {"data", "tbt"} and previous_epoch > 0:
            self._mark_degraded(f"{component}_reconnected")

    def _load_sdk(self) -> None:
        try:
            from fyers_apiv3.FyersWebsocket import data_ws
            from fyers_apiv3.FyersWebsocket.tbt_ws import (
                FyersTbtSocket,
                SubscriptionModes,
            )
        except ImportError as exc:
            raise RuntimeError(
                'Install the project dependencies with: python -m pip install -e ".[dev]"'
            ) from exc
        self.data_ws_module = data_ws
        self.FyersTbtSocket = FyersTbtSocket
        self.SubscriptionModes = SubscriptionModes
        LOG.info(
            "FYERS SDK loaded version=%s",
            self._package_version("fyers-apiv3") or "unknown",
        )

    @staticmethod
    def _sdk_websocket(socket_object: Any) -> Any:
        for attribute in (
            "_FyersDataSocket__ws_object",
            "_FyersTbtSocket__ws_object",
        ):
            if hasattr(socket_object, attribute):
                return getattr(socket_object, attribute)
        return None

    @classmethod
    def _socket_connected(cls, socket_object: Any) -> bool:
        if socket_object is None:
            return False
        websocket_object = cls._sdk_websocket(socket_object)
        if websocket_object is not None:
            websocket_socket = getattr(websocket_object, "sock", None)
            return bool(
                websocket_socket is not None
                and getattr(websocket_socket, "connected", False)
            )
        checker = getattr(socket_object, "is_connected", None)
        if callable(checker):
            try:
                return bool(checker())
            except BaseException:
                return False
        return False

    def _wait_until_connected(self, component: str, socket_object: Any) -> bool:
        deadline = time.monotonic() + self.settings.connect_timeout_seconds
        while not self.stop_event.is_set():
            if self._socket_connected(socket_object):
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                exc = RuntimeError(
                    f"FYERS {component} socket did not connect within "
                    f"{self.settings.connect_timeout_seconds:g} seconds"
                )
                self._safe_control(
                    component,
                    "connect_timeout",
                    {"timeout_seconds": self.settings.connect_timeout_seconds},
                    severity="ERROR",
                    message=str(exc),
                )
                self._set_fatal(exc, f"{component}_connect_timeout")
                return False
            self.stop_event.wait(min(0.1, remaining))
        return False

    def on_data_message(self, message: Any) -> None:
        try:
            if not isinstance(message, Mapping):
                self._control(
                    "data",
                    "unrecognized_message",
                    {"payload": str(message)},
                    severity="WARNING",
                    message="Data socket callback was not a mapping",
                )
                return

            symbol_value = message.get("symbol")
            symbol = str(symbol_value) if symbol_value else None
            if symbol and symbol.upper() not in self._configured_symbols_upper:
                self._control(
                    "data",
                    "unexpected_symbol",
                    dict(message),
                    severity="WARNING",
                    symbol=symbol,
                )
                return

            if symbol:
                common = self._common_fields("symbol_update", "data", symbol)
                self._submit(
                    "symbol_update",
                    normalize_symbol_update(message, common),
                )
                with self._state_lock:
                    self._first_symbols_seen["data"].add(symbol.upper())
                    self._last_valid_event_monotonic["data"][
                        symbol.upper()
                    ] = time.monotonic()
                    if self._configured_symbols_upper.issubset(
                        self._first_symbols_seen["data"]
                    ):
                        self._first_event_events["data"].set()
                self._record_stale_feed_recovery("data", symbol.upper())
            else:
                self._control(
                    "data",
                    "service_message",
                    dict(message),
                    message="FYERS data-socket service message",
                )
        except BaseException as exc:
            LOG.exception("Failed to record FYERS SymbolUpdate callback")
            self._set_fatal(exc, "data_callback_failure")

    def on_data_error(self, message: Any) -> None:
        redacted_message = self._redact_text(message)
        if not self.stop_event.is_set():
            self._mark_degraded("data_socket_error")
        self._safe_control(
            "data",
            "socket_error",
            {"payload": redacted_message},
            severity="ERROR",
            message=redacted_message,
        )
        if not self.stop_event.is_set() and isinstance(message, Mapping) and (
            str(message.get("s", "")).lower() == "error"
            or str(message.get("type", "")).lower() == "error"
        ):
            self._set_fatal(
                RuntimeError(
                    "FYERS data server rejected the request; see the control stream"
                ),
                "data_server_error",
            )

    def on_data_close(self, message: Any) -> None:
        redacted_message = self._redact_text(message)
        expected_shutdown = self.stop_event.is_set()
        if not expected_shutdown:
            self._mark_degraded("data_socket_closed")
        self._safe_control(
            "data",
            "socket_close",
            {"payload": redacted_message},
            severity="INFO" if expected_shutdown else "WARNING",
            message=redacted_message,
        )

    def on_data_open(self) -> None:
        completion_event: threading.Event | None = None
        with self._data_connection_lock:
            if self.stop_event.is_set():
                return
            if (
                not self._ready_events["data"].is_set()
                and not self._wait_until_connected("data", self.data_socket)
            ):
                return
            _scheduled, completion_event = (
                self._dispatch_data_connection_reconciliation_locked(
                    from_sdk_callback=True
                )
            )

        # Preserve the SDK callback's historical ordering: connect() must not
        # return before this genuine connection has been subscribed. The work
        # itself runs on a tracked daemon so health polling never performs the
        # SDK's blocking symbol-token request on MainThread.
        while (
            completion_event is not None
            and not self.stop_event.is_set()
            and not completion_event.wait(0.1)
        ):
            pass

    def _reconcile_data_connection(self) -> bool:
        with self._data_connection_lock:
            scheduled, _completion_event = (
                self._dispatch_data_connection_reconciliation_locked(
                    from_sdk_callback=False
                )
            )
            return scheduled

    def _dispatch_data_connection_reconciliation_locked(
        self,
        *,
        from_sdk_callback: bool,
    ) -> tuple[bool, threading.Event | None]:
        if self.stop_event.is_set():
            return False, None

        socket_object = self.data_socket
        if socket_object is None:
            return False, None
        websocket_object = self._sdk_websocket(socket_object)
        if (
            websocket_object is None
            or not self._socket_connected(socket_object)
            or self._sdk_websocket(socket_object) is not websocket_object
        ):
            return False, None

        now = time.monotonic()
        not_before = now + (
            DATA_CALLBACK_SETTLE_SECONDS
            if from_sdk_callback
            else DATA_HEALTH_SETTLE_SECONDS
        )
        self._data_connection_reconciliations = [
            reconciliation
            for reconciliation in self._data_connection_reconciliations
            if (
                not reconciliation["completion_event"].is_set()
                or reconciliation["websocket"] is websocket_object
            )
        ]
        for reconciliation in self._data_connection_reconciliations:
            if reconciliation["websocket"] is websocket_object:
                if from_sdk_callback:
                    reconciliation["not_before"] = min(
                        reconciliation["not_before"],
                        not_before,
                    )
                return True, reconciliation["completion_event"]

        reconciliation = {
            "socket": socket_object,
            "websocket": websocket_object,
            "not_before": not_before,
            "completion_event": threading.Event(),
        }
        self._data_connection_reconciliations.append(reconciliation)
        self._pending_data_connection_reconciliations.append(reconciliation)
        if self._data_connection_worker is None:
            worker = threading.Thread(
                target=self._run_data_connection_reconciler,
                name="data-connection-reconciler",
                daemon=True,
            )
            self._data_connection_worker = worker
            self._data_connection_workers = [
                existing
                for existing in self._data_connection_workers
                if existing.is_alive()
            ]
            self._data_connection_workers.append(worker)
            worker.start()
        return True, reconciliation["completion_event"]

    def _run_data_connection_reconciler(self) -> None:
        current_thread = threading.current_thread()
        while True:
            with self._data_connection_lock:
                if self.stop_event.is_set():
                    abandoned = list(
                        self._pending_data_connection_reconciliations
                    )
                    self._pending_data_connection_reconciliations.clear()
                    if self._data_connection_worker is current_thread:
                        self._data_connection_worker = None
                    for reconciliation in abandoned:
                        reconciliation["completion_event"].set()
                    return
                if not self._pending_data_connection_reconciliations:
                    if self._data_connection_worker is current_thread:
                        self._data_connection_worker = None
                    return
                reconciliation = (
                    self._pending_data_connection_reconciliations.pop(0)
                )

            try:
                self._activate_data_connection(reconciliation)
            except BaseException as exc:
                if not self.stop_event.is_set():
                    LOG.exception("Data-connection reconciliation failed")
                    self._set_fatal(
                        exc,
                        "data_connection_reconciliation_failure",
                    )
            finally:
                reconciliation["completion_event"].set()

    def _data_websocket_is_current(
        self,
        socket_object: Any,
        websocket_object: Any,
    ) -> bool:
        return bool(
            not self.stop_event.is_set()
            and socket_object is self.data_socket
            and self._sdk_websocket(socket_object) is websocket_object
            and self._socket_connected(socket_object)
            and self._sdk_websocket(socket_object) is websocket_object
        )

    @staticmethod
    def _data_socket_callback_state_ready(socket_object: Any) -> bool:
        if not hasattr(socket_object, "message_thread_stop_event"):
            return True
        stop_event = getattr(socket_object, "message_thread_stop_event", None)
        message_thread = getattr(socket_object, "message_thread", None)
        return bool(
            stop_event is not None
            and not stop_event.is_set()
            and isinstance(message_thread, threading.Thread)
            and message_thread.is_alive()
        )

    def _wait_for_data_connection_settle(
        self,
        reconciliation: dict[str, Any],
    ) -> bool:
        socket_object = reconciliation["socket"]
        websocket_object = reconciliation["websocket"]
        while self._data_websocket_is_current(
            socket_object,
            websocket_object,
        ):
            with self._data_connection_lock:
                remaining = reconciliation["not_before"] - time.monotonic()
            if remaining > 0:
                self.stop_event.wait(min(0.05, remaining))
                continue
            if self._data_socket_callback_state_ready(socket_object):
                return True
            self.stop_event.wait(0.01)
        return False

    def _activate_data_connection(
        self,
        reconciliation: dict[str, Any],
    ) -> None:
        socket_object = reconciliation["socket"]
        websocket_object = reconciliation["websocket"]
        if not self._wait_for_data_connection_settle(reconciliation):
            return
        if not self._data_websocket_is_current(
            socket_object,
            websocket_object,
        ):
            return
        self._new_connection("data")
        if not self._data_websocket_is_current(
            socket_object,
            websocket_object,
        ):
            return
        self._safe_control(
            "data",
            "socket_open",
            {"symbols": list(self.settings.symbols)},
        )
        if not self._data_websocket_is_current(
            socket_object,
            websocket_object,
        ):
            return
        try:
            socket_object.subscribe(
                symbols=list(self.settings.symbols),
                data_type="SymbolUpdate",
            )
            if not self._data_websocket_is_current(
                socket_object,
                websocket_object,
            ):
                return
            self._safe_control(
                "data",
                "subscribe_sent",
                {"symbols": list(self.settings.symbols), "data_type": "SymbolUpdate"},
            )
            if not self._data_websocket_is_current(
                socket_object,
                websocket_object,
            ):
                return
            self._ready_events["data"].set()
        except BaseException as exc:
            if self._data_websocket_is_current(
                socket_object,
                websocket_object,
            ):
                LOG.exception("Data-socket subscription failed")
                self._set_fatal(exc, "data_subscription_failure")

    def on_tbt_depth(self, ticker: str, message: Any) -> None:
        try:
            ticker = str(ticker)
            normalized_ticker = ticker.upper()
            connection_key = self._tbt_connection_by_symbol.get(
                normalized_ticker
            )
            if connection_key is None:
                self._control(
                    "tbt",
                    "unexpected_symbol",
                    {"ticker": ticker},
                    severity="WARNING",
                    symbol=ticker,
                )
                return
            with self._state_lock:
                previous_sequence = self._last_sequence_by_symbol.get(ticker)
                connection_epoch = self._connection_epochs[connection_key]
                first_after_reconnect = (
                    connection_epoch > 1
                    and ticker
                    not in self._tbt_symbols_seen_in_epoch[connection_key]
                    and previous_sequence is not None
                )
            common = self._common_fields(
                "tbt_depth",
                "tbt",
                ticker,
                self.settings.tbt_channel,
                connection_key,
            )
            row = normalize_depth(
                ticker=ticker,
                message=message,
                common=common,
                previous_sequence_no=previous_sequence,
                include_raw_json=self.settings.include_depth_raw_json,
                first_after_reconnect=first_after_reconnect,
            )
            sequence_no = row["sequence_no"]
            with self._state_lock:
                self._tbt_symbols_seen_in_epoch[connection_key].add(ticker)
                if sequence_no is not None:
                    self._last_sequence_by_symbol[ticker] = sequence_no
            self._submit("tbt_depth", row)
            with self._state_lock:
                self._first_symbols_seen["tbt"].add(ticker.upper())
                self._last_valid_event_monotonic["tbt"][
                    ticker.upper()
                ] = time.monotonic()
                if self._configured_symbols_upper.issubset(
                    self._first_symbols_seen["tbt"]
                ):
                    self._first_event_events["tbt"].set()
            self._record_stale_feed_recovery("tbt", normalized_ticker)

            status = row["sequence_status"]
            base_status = status.removeprefix("first_after_reconnect_")
            if first_after_reconnect:
                self._safe_control(
                    "tbt",
                    "sequence_after_reconnect",
                    {
                        "sequence_no": sequence_no,
                        "previous_sequence_no": previous_sequence,
                        "sequence_delta": row["sequence_delta"],
                        "sequence_status": status,
                        "diagnostic_scope": "per_symbol",
                    },
                    severity="WARNING",
                    symbol=ticker,
                    connection_key=connection_key,
                )
            elif base_status in {"gap", "duplicate", "regression", "reset"}:
                severity = "WARNING" if base_status != "reset" else "INFO"
                self._safe_control(
                    "tbt",
                    f"sequence_{base_status}",
                    {
                        "sequence_no": sequence_no,
                        "previous_sequence_no": previous_sequence,
                        "sequence_delta": row["sequence_delta"],
                        "diagnostic_scope": "per_symbol",
                    },
                    severity=severity,
                    symbol=ticker,
                    connection_key=connection_key,
                )
            if base_status in {"gap", "regression"}:
                self._mark_degraded(f"tbt_sequence_{base_status}")
        except BaseException as exc:
            LOG.exception("Failed to record FYERS TBT callback")
            self._set_fatal(exc, "tbt_callback_failure")

    def on_tbt_server_error(
        self,
        message: Any,
        connection_key: str | None = None,
    ) -> None:
        redacted_message = self._redact_text(message)
        if not self.stop_event.is_set():
            self._mark_degraded("tbt_server_error")
        self._safe_control(
            "tbt",
            "server_error",
            {"payload": redacted_message},
            severity="ERROR",
            message=redacted_message,
            connection_key=connection_key,
        )
        if not self.stop_event.is_set():
            self._set_fatal(
                RuntimeError(
                    "FYERS TBT server rejected the request; see the control stream"
                ),
                "tbt_server_error",
            )

    def on_tbt_error(
        self,
        message: Any,
        connection_key: str | None = None,
    ) -> None:
        redacted_message = self._redact_text(message)
        if not self.stop_event.is_set():
            self._mark_degraded("tbt_socket_error")
        self._safe_control(
            "tbt",
            "socket_error",
            {"payload": redacted_message},
            severity="ERROR",
            message=redacted_message,
            connection_key=connection_key,
        )

    def on_tbt_close(
        self,
        message: Any,
        connection_key: str | None = None,
    ) -> None:
        redacted_message = self._redact_text(message)
        expected_shutdown = self.stop_event.is_set()
        if not expected_shutdown:
            self._mark_degraded("tbt_socket_closed")
        self._safe_control(
            "tbt",
            "socket_close",
            {"payload": redacted_message},
            severity="INFO" if expected_shutdown else "WARNING",
            message=redacted_message,
            connection_key=connection_key,
        )

    def on_tbt_open(self, connection_key: str | None = None) -> None:
        if self.stop_event.is_set():
            return
        connection_key = connection_key or self._tbt_connection_keys[0]
        connection_index = self._tbt_connection_keys.index(connection_key)
        symbols = self._tbt_symbol_groups[connection_index]
        self._new_connection("tbt", connection_key)
        self._safe_control(
            "tbt",
            "socket_open",
            {
                "symbols": list(symbols),
                "channel": self.settings.tbt_channel,
                "connection": connection_key,
                "subscription_delivery": "sdk_automatic_after_callback",
            },
            connection_key=connection_key,
        )
        with self._state_lock:
            if not self.stop_event.is_set():
                self._tbt_ready_connections.add(connection_key)
                if len(self._tbt_ready_connections) == len(
                    self._tbt_connection_keys
                ):
                    self._ready_events["tbt"].set()

    def _safe_control(self, *args: Any, **kwargs: Any) -> None:
        try:
            self._control(*args, **kwargs)
        except BaseException as exc:
            LOG.exception("Could not persist control event")
            self._set_fatal(exc, "control_event_failure")

    def _connect_data_socket(self) -> None:
        try:
            self._safe_control(
                "data",
                "socket_connecting",
                {"symbols": list(self.settings.symbols)},
            )
            self.data_socket = self.data_ws_module.FyersDataSocket(
                access_token=self.settings.ws_token,
                log_path=str(self.settings.sdk_log_dir),
                litemode=False,
                write_to_file=False,
                reconnect=self.settings.data_reconnect,
                reconnect_retry=self.settings.data_reconnect_retries,
                on_connect=self.on_data_open,
                on_close=self.on_data_close,
                on_error=self.on_data_error,
                on_message=self.on_data_message,
            )
            # FYERS starts its worker threads from this flag. Daemon workers
            # let the process exit after a bounded failed shutdown.
            self.data_socket.background_flag = True
            if self.stop_event.is_set():
                return
            self.data_socket.connect()
        except BaseException as exc:
            LOG.exception("FYERS data-socket thread failed")
            self._set_fatal(exc, "data_socket_failure")

    def _new_tbt_socket(self, **kwargs: Any) -> Any:
        socket_class = self.FyersTbtSocket
        if "_instance" in vars(socket_class):
            # fyers-apiv3 3.1.14 implements FyersTbtSocket as a singleton even
            # though FYERS permits up to three TBT connections. Construct each
            # pinned-SDK client directly so every five-symbol group has its own
            # WebSocket and reconnect state.
            socket_object = object.__new__(socket_class)
            socket_class.__init__(socket_object, **kwargs)
        else:
            socket_object = socket_class(**kwargs)

        # The pinned SDK also declares its reconstructed depth cache at class
        # scope. Shadow it per socket to prevent cross-connection state sharing.
        datastore = getattr(socket_object, "_datastore", None)
        if datastore is not None and hasattr(datastore, "depth"):
            datastore.depth = {}
        return socket_object

    def _connect_tbt_socket(
        self,
        connection_index: int = 0,
        symbols: tuple[str, ...] | None = None,
    ) -> None:
        connection_key = self._tbt_connection_keys[connection_index]
        connection_symbols = (
            symbols
            if symbols is not None
            else self._tbt_symbol_groups[connection_index]
        )
        try:
            self._safe_control(
                "tbt",
                "socket_connecting",
                {
                    "symbols": list(connection_symbols),
                    "channel": self.settings.tbt_channel,
                    "connection": connection_key,
                },
                connection_key=connection_key,
            )
            tbt_socket = self._new_tbt_socket(
                access_token=self.settings.ws_token,
                write_to_file=False,
                log_path=str(self.settings.sdk_log_dir),
                on_open=lambda: self.on_tbt_open(connection_key),
                on_close=lambda message: self.on_tbt_close(
                    message,
                    connection_key,
                ),
                on_error=lambda message: self.on_tbt_error(
                    message,
                    connection_key,
                ),
                on_depth_update=self.on_tbt_depth,
                on_error_message=lambda message: self.on_tbt_server_error(
                    message,
                    connection_key,
                ),
                reconnect=self.settings.tbt_reconnect,
                diff_only=False,
                reconnect_retry=self.settings.tbt_reconnect_retries,
            )
            self.tbt_sockets[connection_index] = tbt_socket
            if connection_index == 0:
                self.tbt_socket = tbt_socket
            tbt_socket.background_flag = True
            subscription_info = getattr(tbt_socket, "_subsinfo", None)
            if subscription_info is None:
                raise RuntimeError(
                    "Pinned FYERS TBT SDK no longer exposes subscription state"
                )
            subscription_info.subscribe(
                set(connection_symbols),
                self.settings.tbt_channel,
                self.SubscriptionModes.DEPTH,
            )
            subscription_info.updateChannels(
                set(),
                {self.settings.tbt_channel},
            )
            self._safe_control(
                "tbt",
                "subscription_registered",
                {
                    "symbols": list(connection_symbols),
                    "channel": self.settings.tbt_channel,
                    "connection": connection_key,
                    "mode": "depth",
                    "diff_only": False,
                    "delivery": "sdk_automatic_on_open",
                },
                connection_key=connection_key,
            )
            if self.stop_event.is_set():
                return
            # The FYERS TBT shutdown path expects this helper thread to exist.
            # Start it once per recorder, not from every reconnect callback.
            tbt_socket.keep_running()
            tbt_socket.connect()
        except BaseException as exc:
            LOG.exception("FYERS TBT thread failed")
            self._set_fatal(exc, "tbt_socket_failure")

    def _wait_for_initial_readiness(self) -> bool:
        deadline = time.monotonic() + self.settings.connect_timeout_seconds + 3.0
        while not self.stop_event.is_set():
            if all(event.is_set() for event in self._ready_events.values()):
                self._safe_control(
                    "process",
                    "sockets_ready",
                    {
                        "streams": sorted(self._ready_events),
                        "readiness": "socket_connected_and_subscription_registered",
                    },
                )
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                missing = sorted(
                    component
                    for component, event in self._ready_events.items()
                    if not event.is_set()
                )
                exc = RuntimeError(
                    "FYERS streams did not become ready: " + ", ".join(missing)
                )
                self._safe_control(
                    "process",
                    "stream_readiness_timeout",
                    {
                        "missing_streams": missing,
                        "timeout_seconds": self.settings.connect_timeout_seconds,
                    },
                    severity="ERROR",
                    message=str(exc),
                )
                self._set_fatal(exc, "stream_readiness_timeout")
                return False
            self.stop_event.wait(min(0.1, remaining))
        if self.stop_event.is_set():
            return False

        if self.settings.require_first_event:
            deadline = (
                time.monotonic() + self.settings.first_event_timeout_seconds
            )
            while not self.stop_event.is_set():
                if all(
                    event.is_set()
                    for event in self._first_event_events.values()
                ):
                    break
                self._check_feed_health()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    missing = sorted(
                        component
                        for component, event in self._first_event_events.items()
                        if not event.is_set()
                    )
                    with self._state_lock:
                        missing_symbols = {
                            component: sorted(
                                self._configured_symbols_upper
                                - self._first_symbols_seen[component]
                            )
                            for component in missing
                        }
                    exc = RuntimeError(
                        "FYERS streams produced no initial event: "
                        + ", ".join(missing)
                    )
                    self._safe_control(
                        "process",
                        "first_event_timeout",
                        {
                            "missing_streams": missing,
                            "missing_symbols": missing_symbols,
                            "timeout_seconds": (
                                self.settings.first_event_timeout_seconds
                            ),
                        },
                        severity="ERROR",
                        message=str(exc),
                    )
                    self._set_fatal(exc, "first_event_timeout")
                    return False
                self.stop_event.wait(min(0.1, remaining))
            if self.stop_event.is_set():
                return False

        self._streams_ready = True
        self._safe_control(
            "process",
            "streams_ready",
            {
                "streams": sorted(self._ready_events),
                "first_event_required": self.settings.require_first_event,
                "first_events_seen": {
                    component: event.is_set()
                    for component, event in self._first_event_events.items()
                },
            },
        )
        return not self.stop_event.is_set()

    def _check_feed_health(self) -> None:
        self._reconcile_data_connection()
        if self.stop_event.is_set():
            return
        now = time.monotonic()
        for component in ("data", "tbt"):
            if (
                self._ready_events[component].is_set()
                and self._check_feed_staleness(component, now)
            ):
                return

        connection_items = [
            ("data", "data", self.data_socket),
            *[
                (connection_key, "tbt", socket_object)
                for connection_key, socket_object in zip(
                    self._tbt_connection_keys,
                    self.tbt_sockets,
                    strict=True,
                )
            ],
        ]
        for connection_key, component, socket_object in connection_items:
            connection_ready = (
                self._ready_events["data"].is_set()
                if component == "data"
                else connection_key in self._tbt_ready_connections
            )
            if not connection_ready or socket_object is None:
                continue
            connected = self._socket_connected(socket_object)
            new_disconnect = False
            with self._state_lock:
                disconnected_since = self._disconnected_since[connection_key]
                if connected:
                    self._disconnected_since[connection_key] = None
                elif disconnected_since is None:
                    disconnected_since = now
                    self._disconnected_since[connection_key] = now
                    new_disconnect = True

            if connected:
                continue

            if new_disconnect:
                self._mark_degraded(f"{component}_connection_interrupted")
                self._safe_control(
                    component,
                    "disconnect_detected",
                    {
                        "connection": connection_key,
                        "grace_seconds": self.settings.disconnect_grace_seconds,
                    },
                    severity="WARNING",
                    connection_key=connection_key,
                )

            reconnect_attempts = int(getattr(socket_object, "reconnect_attempts", 0))
            max_attempts = int(
                getattr(socket_object, "max_reconnect_attempts", 0)
            )
            retries_at_limit = (
                max_attempts > 0 and reconnect_attempts >= max_attempts
            )
            reconnect_worker_attribute = next(
                (
                    attribute
                    for attribute in ("ws_thread", "t", "websocket_task")
                    if hasattr(socket_object, attribute)
                ),
                None,
            )
            has_reconnect_worker = reconnect_worker_attribute is not None
            reconnect_worker = (
                getattr(socket_object, reconnect_worker_attribute, None)
                if reconnect_worker_attribute is not None
                else None
            )
            worker_is_alive = getattr(reconnect_worker, "is_alive", None)
            try:
                reconnect_worker_alive = bool(
                    callable(worker_is_alive) and worker_is_alive()
                )
            except BaseException:
                reconnect_worker_alive = False
            reconnect_worker_pending = bool(
                isinstance(reconnect_worker, threading.Thread)
                and reconnect_worker.ident is None
            )
            retries_exhausted = retries_at_limit and (
                not has_reconnect_worker
                or not (reconnect_worker_alive or reconnect_worker_pending)
            )
            outage_seconds = now - disconnected_since
            grace_expired = outage_seconds >= self.settings.disconnect_grace_seconds
            if retries_exhausted or grace_expired:
                reason = (
                    f"{component}_reconnect_exhausted"
                    if retries_exhausted
                    else f"{component}_disconnect_timeout"
                )
                exc = RuntimeError(
                    f"FYERS {component} feed unavailable for "
                    f"{outage_seconds:.1f} seconds"
                )
                self._safe_control(
                    component,
                    reason,
                    {
                        "connection": connection_key,
                        "outage_seconds": outage_seconds,
                        "reconnect_attempts": reconnect_attempts,
                        "max_reconnect_attempts": max_attempts,
                        "grace_seconds": self.settings.disconnect_grace_seconds,
                    },
                    severity="ERROR",
                    message=str(exc),
                    connection_key=connection_key,
                )
                self._set_fatal(exc, reason)
                return

    def _check_feed_staleness(self, component: str, now: float) -> bool:
        timeout = self.settings.stale_feed_timeout_seconds
        if not self._streams_ready or timeout <= 0:
            return False
        with self._state_lock:
            last_events = dict(self._last_valid_event_monotonic[component])
        stale_symbols = {
            symbol: now - last_events[symbol]
            for symbol in self._configured_symbols_upper
            if symbol in last_events and now - last_events[symbol] >= timeout
        }
        missing_symbols = sorted(
            self._configured_symbols_upper - last_events.keys()
        )
        if not stale_symbols and not missing_symbols:
            return False

        reason = f"{component}_feed_stale"
        self._mark_degraded(reason)
        details = {
            "timeout_seconds": timeout,
            "stale_symbols": {
                symbol: round(age, 3)
                for symbol, age in sorted(stale_symbols.items())
            },
            "missing_symbols": missing_symbols,
        }
        affected_symbols = set(stale_symbols) | set(missing_symbols)
        connection_symbols = self._stale_connection_symbols(
            component,
            affected_symbols,
        )
        with self._state_lock:
            exhausted = [
                connection_key
                for connection_key in connection_symbols
                if self._stale_feed_retry_attempts[connection_key]
                >= self.settings.stale_feed_retries
            ]
        if exhausted:
            exc = RuntimeError(
                f"FYERS {component} feed stopped delivering valid callbacks"
            )
            self._safe_control(
                component,
                "feed_stale",
                {
                    **details,
                    "exhausted_connections": exhausted,
                    "max_retries": self.settings.stale_feed_retries,
                },
                severity="ERROR",
                message=str(exc),
            )
            self._set_fatal(exc, reason)
            return True

        for connection_key, symbols in connection_symbols.items():
            self._retry_stale_connection(
                component,
                connection_key,
                symbols,
                now,
                details,
            )
            if self.stop_event.is_set():
                return True
        return False

    def _stale_connection_symbols(
        self,
        component: str,
        affected_symbols: set[str],
    ) -> dict[str, set[str]]:
        if component == "data":
            return {"data": set(self._configured_symbols_upper)}
        result: dict[str, set[str]] = {}
        for symbol in affected_symbols:
            connection_key = self._tbt_connection_by_symbol[symbol]
            result.setdefault(connection_key, set()).update(
                configured.upper()
                for configured in self._tbt_symbol_groups[
                    self._tbt_connection_keys.index(connection_key)
                ]
            )
        return result

    def _retry_stale_connection(
        self,
        component: str,
        connection_key: str,
        symbols: set[str],
        now: float,
        stale_details: dict[str, Any],
    ) -> None:
        socket_object = (
            self.data_socket
            if component == "data"
            else self.tbt_sockets[
                self._tbt_connection_keys.index(connection_key)
            ]
        )
        websocket_object = self._sdk_websocket(socket_object)
        close_method = getattr(websocket_object, "close", None)
        if not callable(close_method) or not getattr(
            socket_object,
            "restart_flag",
            False,
        ):
            exc = RuntimeError(
                f"FYERS {component} stale socket cannot be restarted safely; "
                "automatic reconnect is disabled or unavailable"
            )
            self._safe_control(
                component,
                "feed_stale_retry_failure",
                {**stale_details, "connection": connection_key},
                severity="ERROR",
                message=str(exc),
                connection_key=connection_key,
            )
            self._set_fatal(exc, f"{component}_feed_stale_retry_failure")
            return

        with self._state_lock:
            attempt = self._stale_feed_retry_attempts[connection_key] + 1
            self._stale_feed_retry_attempts[connection_key] = attempt
            self._stale_feed_retry_started_at[connection_key] = now
            # A retry gets a full stale timeout to produce fresh callbacks.
            # Without this baseline the health loop would repeatedly close the
            # new socket.
            for symbol in symbols:
                self._last_valid_event_monotonic[component][symbol] = now
            self._disconnected_since[connection_key] = now
        self._safe_control(
            component,
            "feed_stale_retry",
            {
                **stale_details,
                "connection": connection_key,
                "attempt": attempt,
                "max_retries": self.settings.stale_feed_retries,
                "retry_observation_seconds": (
                    self.settings.stale_feed_timeout_seconds
                ),
            },
            severity="WARNING",
            message=(
                f"Restarting stale FYERS {component} transport "
                f"(attempt {attempt}/{self.settings.stale_feed_retries})"
            ),
            connection_key=connection_key,
        )
        try:
            # Close only the underlying transport. The SDK sees an unexpected
            # disconnect and applies its configured bounded reconnect policy;
            # its high-level close method would disable reconnect altogether.
            close_method()
        except BaseException as exc:
            self._safe_control(
                component,
                "feed_stale_retry_failure",
                {
                    "connection": connection_key,
                    "attempt": attempt,
                    "max_retries": self.settings.stale_feed_retries,
                    "error": self._redact_text(exc),
                },
                severity="ERROR",
                message=self._redact_text(exc),
                connection_key=connection_key,
            )
            self._set_fatal(exc, f"{component}_feed_stale_retry_failure")

    def _record_stale_feed_recovery(
        self,
        component: str,
        symbol: str,
    ) -> None:
        connection_key = (
            "data"
            if component == "data"
            else self._tbt_connection_by_symbol[symbol]
        )
        with self._state_lock:
            retry_started_at = self._stale_feed_retry_started_at[connection_key]
            if retry_started_at is None:
                return
            expected_symbols = (
                self._configured_symbols_upper
                if component == "data"
                else {
                    configured.upper()
                    for configured in self._tbt_symbol_groups[
                        self._tbt_connection_keys.index(connection_key)
                    ]
                }
            )
            last_events = self._last_valid_event_monotonic[component]
            if not all(
                last_events.get(expected, 0.0) > retry_started_at
                for expected in expected_symbols
            ):
                return
            attempt = self._stale_feed_retry_attempts[connection_key]
            self._stale_feed_retry_attempts[connection_key] = 0
            self._stale_feed_retry_started_at[connection_key] = None
            self._disconnected_since[connection_key] = None
        self._safe_control(
            component,
            "feed_recovered",
            {"connection": connection_key, "attempt": attempt},
            connection_key=connection_key,
        )

    def start(self) -> None:
        LOG.info(
            "Starting recorder run=%s symbols=%d tbt_connections=%d data_dir=%s",
            self.run_id,
            len(self.settings.symbols),
            len(self._tbt_symbol_groups),
            self.settings.data_dir,
        )
        if self._data_lock_handle is None:
            self._prepare_directories()
        self._load_sdk()
        self.writer.start()
        self._started = True
        self._started_at_ns = time.time_ns()
        self._write_manifest(status="running")
        self._control(
            "process",
            "process_start",
            {
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "symbols": list(self.settings.symbols),
                "tbt_connections": [
                    {
                        "connection": connection_key,
                        "symbols": list(symbols),
                        "channel": self.settings.tbt_channel,
                    }
                    for connection_key, symbols in zip(
                        self._tbt_connection_keys,
                        self._tbt_symbol_groups,
                        strict=True,
                    )
                ],
            },
        )

        data_thread = threading.Thread(
            target=self._connect_data_socket,
            name="fyers-data-connector",
            daemon=True,
        )
        tbt_threads = [
            threading.Thread(
                target=self._connect_tbt_socket,
                args=(connection_index, symbols),
                name=f"fyers-tbt-connector-{connection_index + 1}",
                daemon=True,
            )
            for connection_index, symbols in enumerate(
                self._tbt_symbol_groups
            )
        ]
        self._threads = [data_thread, *tbt_threads]
        for thread in self._threads:
            thread.start()
        LOG.info(
            "Connector threads started names=%s",
            [thread.name for thread in self._threads],
        )

    def run(self) -> int:
        exit_code = 0
        try:
            market_close_at: datetime | None = None
            if self.settings.duration_seconds <= 0:
                # Match taperecorder: a Job may be launched before market open,
                # while the application owns the trading-session clock.
                self._prepare_directories()
                market_close_at = self._wait_for_market_open()
                if market_close_at is None:
                    return 0
            self.start()
            if market_close_at is not None:
                self._start_market_close_watcher(market_close_at)
            if self._wait_for_initial_readiness():
                deadline = (
                    time.monotonic() + self.settings.duration_seconds
                    if self.settings.duration_seconds > 0
                    else None
                )
                while not self.stop_event.wait(0.25):
                    self.writer.raise_if_failed()
                    self._check_feed_health()
                    if deadline is not None and time.monotonic() >= deadline:
                        self.request_stop("duration_elapsed")
                        break
                    self._log_status_if_due()
            if self._fatal_error is not None:
                exit_code = 1
        except KeyboardInterrupt:
            self.request_stop("keyboard_interrupt")
        except BaseException as exc:
            self._set_fatal(exc, "fatal_runtime_error")
            LOG.exception("Recorder failed")
            exit_code = 1
        finally:
            if self._started:
                try:
                    self.stop()
                except BaseException as exc:
                    self._set_fatal(exc, "shutdown_failure")
                    LOG.exception("Recorder shutdown failed")
                    exit_code = 1
                if self._final_status != "complete":
                    exit_code = 1
            self._release_data_directory_lock_if_safe()
            self._join_market_close_watcher()
        return exit_code

    def _now_market(self) -> datetime:
        return datetime.now(self.market_timezone)

    def _market_window(self, now: datetime) -> tuple[datetime, datetime]:
        market_open = datetime.combine(
            now.date(),
            MARKET_OPEN_TIME,
            tzinfo=self.market_timezone,
        )
        market_close = datetime.combine(
            now.date(),
            MARKET_CLOSE_TIME,
            tzinfo=self.market_timezone,
        )
        return market_open, market_close

    def _wait_for_market_open(self) -> datetime | None:
        """Wait interruptibly until 09:15 IST and return today's 15:31 close."""

        now = self._now_market()
        if now.weekday() >= 5:
            self.stop_reason = "weekend"
            LOG.warning(
                "Recorder Job will not connect because today is a weekend date=%s",
                now.date(),
            )
            return None

        market_open, market_close = self._market_window(now)
        if now >= market_close:
            self.stop_reason = "market_window_closed"
            LOG.warning(
                "Recorder Job started after market close; exiting without connecting "
                "now=%s market_close=%s",
                now.isoformat(),
                market_close.isoformat(),
            )
            return None

        if now < market_open:
            LOG.info(
                "Waiting for market open now=%s market_open=%s seconds=%d",
                now.isoformat(),
                market_open.isoformat(),
                int((market_open - now).total_seconds()),
            )
        while now < market_open and not self.stop_event.is_set():
            remaining = max(0.0, (market_open - now).total_seconds())
            self.stop_event.wait(min(remaining, 30.0))
            now = self._now_market()
        if self.stop_event.is_set():
            LOG.info("Market-open wait interrupted reason=%s", self.stop_reason)
            return None

        LOG.info(
            "Market open reached; starting FYERS connections market_open=%s "
            "market_close=%s",
            market_open.isoformat(),
            market_close.isoformat(),
        )
        return market_close

    def _start_market_close_watcher(self, market_close_at: datetime) -> None:
        if self._market_close_thread is not None:
            return

        def watch_market_close() -> None:
            LOG.info(
                "Market-close watcher started disconnect_at=%s",
                market_close_at.isoformat(),
            )
            while not self.stop_event.is_set():
                remaining = (market_close_at - self._now_market()).total_seconds()
                if remaining <= 0:
                    LOG.info(
                        "Market close reached; requesting websocket disconnect "
                        "market_close=%s",
                        market_close_at.isoformat(),
                    )
                    self.request_stop("market_close")
                    return
                self.stop_event.wait(min(remaining, 30.0))

        self._market_close_thread = threading.Thread(
            target=watch_market_close,
            name="market-close-watcher",
            daemon=True,
        )
        self._market_close_thread.start()

    def _join_market_close_watcher(self) -> None:
        thread = self._market_close_thread
        if thread is None or thread is threading.current_thread():
            return
        thread.join(timeout=1.0)
        if thread.is_alive():
            LOG.warning("Market-close watcher did not stop within one second")

    def request_stop(self, reason: str) -> None:
        if not self.stop_event.is_set():
            self.stop_reason = reason
            LOG.info("Stop requested: %s", reason)
        self.stop_event.set()

    @staticmethod
    def _sdk_worker_threads(socket_object: Any) -> list[threading.Thread]:
        result: list[threading.Thread] = []
        seen: set[int] = set()
        for attribute in (
            "ws_thread",
            "message_thread",
            "ping_thread",
            "infy_loop",
            "t",
            "running_thread",
            "websocket_task",
        ):
            thread = getattr(socket_object, attribute, None)
            if isinstance(thread, threading.Thread) and id(thread) not in seen:
                result.append(thread)
                seen.add(id(thread))
        return result

    @staticmethod
    def _signal_sdk_stop(socket_object: Any) -> None:
        socket_object.restart_flag = False
        stop_running = getattr(socket_object, "stop_running", None)
        if callable(stop_running):
            stop_running()
        for attribute in (
            "_FyersDataSocket__ws_run",
            "_FyersTbtSocket__ws_run",
        ):
            if hasattr(socket_object, attribute):
                setattr(socket_object, attribute, False)
        stop_message_thread = getattr(
            socket_object,
            "message_thread_stop_event",
            None,
        )
        if stop_message_thread is not None:
            stop_message_thread.set()
        condition = getattr(socket_object, "message_condition", None)
        if condition is not None:
            with condition:
                condition.notify_all()

    def _shutdown_sockets(self) -> None:
        LOG.info(
            "Stopping FYERS sockets timeout_seconds=%g",
            self.settings.shutdown_timeout_seconds,
        )
        deadline = time.monotonic() + self.settings.shutdown_timeout_seconds
        socket_items = [
            *[
                (f"tbt-{index + 1}", socket_object)
                for index, socket_object in enumerate(self.tbt_sockets)
            ],
            ("data", self.data_socket),
        ]
        close_threads: list[tuple[str, threading.Thread]] = []
        close_errors: list[str] = []
        close_errors_lock = threading.Lock()

        for component, socket_object in socket_items:
            if socket_object is None:
                continue
            try:
                self._signal_sdk_stop(socket_object)
            except BaseException as exc:
                close_errors.append(f"{component}: stop signal failed: {exc!r}")

            close_method = next(
                (
                    method
                    for method_name in ("close_connection", "close", "disconnect")
                    if callable(method := getattr(socket_object, method_name, None))
                ),
                None,
            )
            if close_method is None:
                close_errors.append(f"{component}: no supported close method")
                continue

            def close_target(
                method: Any = close_method,
                name: str = component,
            ) -> None:
                try:
                    method()
                except BaseException as exc:
                    with close_errors_lock:
                        close_errors.append(f"{name}: close failed: {exc!r}")

            thread = threading.Thread(
                target=close_target,
                name=f"fyers-{component}-closer",
                daemon=True,
            )
            close_threads.append((component, thread))
            thread.start()

        for component, thread in close_threads:
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(timeout=remaining)
            if thread.is_alive():
                close_errors.append(f"{component}: close timed out")

        worker_threads: list[tuple[str, threading.Thread]] = []
        for component, socket_object in socket_items:
            if socket_object is None:
                continue
            worker_threads.extend(
                (component, thread)
                for thread in self._sdk_worker_threads(socket_object)
            )
        worker_threads.extend(("connector", thread) for thread in self._threads)
        worker_threads.extend(
            ("data-reconciler", thread)
            for thread in self._data_connection_workers
        )

        current_thread = threading.current_thread()
        for component, thread in worker_threads:
            if thread is current_thread:
                continue
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(timeout=remaining)
            if thread.is_alive():
                close_errors.append(
                    f"{component}: worker thread still alive: {thread.name}"
                )

        self._safe_control(
            "process",
            "socket_shutdown",
            {
                "timeout_seconds": self.settings.shutdown_timeout_seconds,
                "errors": close_errors,
                "worker_threads": [
                    {
                        "component": component,
                        "name": thread.name,
                        "alive": thread.is_alive(),
                        "daemon": thread.daemon,
                    }
                    for component, thread in worker_threads
                ],
            },
            severity="ERROR" if close_errors else "INFO",
        )
        if close_errors:
            self._set_fatal(
                RuntimeError("; ".join(close_errors)),
                "socket_shutdown_failure",
            )

    def _stop_accepting_submissions(self) -> None:
        deadline = time.monotonic() + self.settings.shutdown_timeout_seconds
        with self._submission_condition:
            self._accepting_submissions = False
            while self._active_submissions:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    exc = TimeoutError(
                        "Timed out waiting for in-flight callback submissions"
                    )
                    self._set_fatal(exc, "callback_shutdown_timeout")
                    raise exc
                self._submission_condition.wait(timeout=remaining)
        LOG.debug("Callback submissions drained")

    def _archive_and_upload_trade_ticks(self) -> None:
        writer_stats = self.writer.stats()
        current_dates = (
            finalized_trading_dates(writer_stats["parts"])
            if writer_stats["parts"]
            else ()
        )
        trading_date_set = set(current_dates)
        if self.settings.s3_upload_enabled:
            trading_date_set.update(
                pending_trading_dates(
                    self.settings.data_dir,
                    self.settings.do_s3_bucket_name,
                    self.settings.do_s3_spaces_prefix,
                )
            )
        trading_dates = tuple(sorted(trading_date_set))
        spaces = None
        if self.settings.s3_upload_enabled:
            spaces = DigitalOceanSpacesConfig(
                endpoint_url=self.settings.do_s3_endpoint_url,
                region=self.settings.do_s3_region,
                bucket_name=self.settings.do_s3_bucket_name,
                prefix=self.settings.do_s3_spaces_prefix,
                access_key_id=self.settings.do_s3_access_key_id,
                secret_access_key=self.settings.do_s3_secret_access_key,
            )
        LOG.info("Archiving finalized trade-tick dates dates=%s", trading_dates)
        failures: list[tuple[str, BaseException]] = []
        for trading_date in trading_dates:
            archive_record: dict[str, Any] = {
                "trading_date": trading_date,
                "archive_path": None,
                "source_dir": str(self.settings.data_dir / trading_date),
                "file_count": None,
                "size_bytes": None,
                "sha256": None,
                "source_fingerprint": None,
                "upload_status": "archiving",
                "bucket_name": spaces.bucket_name if spaces is not None else None,
                "object_key": None,
                "uri": None,
                "etag": None,
            }
            self._trade_tick_archives.append(archive_record)
            try:
                artifact = create_trade_ticks_archive(
                    self.settings.data_dir,
                    trading_date,
                )
            except BaseException as exc:
                archive_record["upload_status"] = "archive_failed"
                archive_record["error"] = self._redact_text(repr(exc))
                failures.append((trading_date, exc))
                LOG.exception(
                    "Trade-ticks archive creation failed date=%s",
                    trading_date,
                )
                continue

            archive_record.update(
                {
                    "archive_path": str(artifact.archive_path),
                    "file_count": artifact.file_count,
                    "size_bytes": artifact.size_bytes,
                    "sha256": artifact.sha256,
                    "source_fingerprint": artifact.source_fingerprint,
                    "upload_status": (
                        "uploading" if spaces is not None else "disabled"
                    ),
                }
            )
            if spaces is None:
                LOG.info(
                    "S3 upload disabled; retained local trade-ticks archive "
                    "date=%s path=%s",
                    trading_date,
                    artifact.archive_path,
                )
                continue
            try:
                receipt = upload_trade_ticks_archive(
                    artifact,
                    spaces,
                    client=self._spaces_client,
                )
                archive_record.update(
                    {
                        "upload_status": "verified",
                        "object_key": receipt.object_key,
                        "uri": receipt.uri,
                        "remote_sha256": receipt.sha256,
                        "etag": receipt.etag,
                    }
                )
                receipt_path = write_upload_receipt_atomic(
                    self.settings.data_dir,
                    trading_date,
                    archive_record,
                )
                archive_record["receipt_path"] = str(receipt_path)
            except BaseException as exc:
                archive_record["upload_status"] = "failed"
                archive_record["error"] = self._redact_text(repr(exc))
                failures.append((trading_date, exc))
                LOG.exception(
                    "Trade-ticks upload or receipt failed date=%s",
                    trading_date,
                )

        if failures:
            failed_dates = ", ".join(date for date, _exc in failures)
            raise RuntimeError(
                f"Archive/upload failed for trading dates: {failed_dates}"
            ) from failures[0][1]

    def stop(self) -> None:
        if self._ended_at_ns is not None:
            LOG.debug("Recorder stop ignored because shutdown already completed")
            self._release_data_directory_lock_if_safe()
            return
        LOG.info(
            "Stopping recorder run=%s reason=%s",
            self.run_id,
            self.stop_reason,
        )
        self.stop_event.set()
        self._safe_control(
            "process",
            "process_stop_requested",
            {"reason": self.stop_reason},
        )

        self._shutdown_sockets()
        if not self._streams_ready and self._fatal_error is None:
            self._set_fatal(
                RuntimeError("Recorder stopped before both streams became ready"),
                "streams_not_ready",
            )
        self._stop_accepting_submissions()

        writer_error: BaseException | None = None
        try:
            self.writer.stop(
                timeout_seconds=self.settings.shutdown_timeout_seconds
            )
        except BaseException as exc:
            writer_error = exc
            self._set_fatal(exc, "writer_shutdown_failure")
            LOG.exception("Parquet writer did not stop cleanly")
        self._ended_at_ns = time.time_ns()
        if writer_error is not None and self.writer.is_alive():
            self._clean_shutdown = False
            self._final_status = "failed"
            self._release_data_directory_lock_if_safe()
            raise writer_error
        archive_error: BaseException | None = None
        finalized_parts = self.writer.stats()["parts"]
        has_pending_uploads = self.settings.s3_upload_enabled and bool(
            pending_trading_dates(
                self.settings.data_dir,
                self.settings.do_s3_bucket_name,
                self.settings.do_s3_spaces_prefix,
            )
        )
        if finalized_parts or has_pending_uploads:
            try:
                self._archive_and_upload_trade_ticks()
            except BaseException as exc:
                archive_error = exc
                self._set_fatal(exc, "archive_upload_failure")
                LOG.exception("Trade-ticks archive or upload failed")
        self._clean_shutdown = self._fatal_error is None
        with self._quality_lock:
            degraded = bool(self._degraded_reasons)
        if not self._clean_shutdown:
            status = "failed"
            marker_name = "_FAILED"
        elif degraded:
            status = "degraded"
            marker_name = "_DEGRADED"
        else:
            status = "complete"
            marker_name = "_SUCCESS"
        self._final_status = status
        self._write_manifest(status=status)
        self.writer.write_marker_atomic(marker_name)
        LOG.info("Recorder stopped status=%s run=%s", status, self.run_id)
        self._release_data_directory_lock_if_safe()
        if writer_error is not None:
            raise writer_error
        if archive_error is not None:
            raise archive_error

    def _prepare_directories(self) -> None:
        for path in (
            self.settings.data_dir,
            self.settings.log_dir,
            self.settings.sdk_log_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
            probe = path / f".tickrecorder-write-test-{uuid.uuid4().hex}"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
            LOG.debug("Verified writable directory path=%s", path)
        self._acquire_data_directory_lock()

    def _acquire_data_directory_lock(self) -> None:
        if self._data_lock_handle is not None:
            return
        lock_path = self.settings.data_dir / ".tickrecorder.lock"
        handle = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.close()
            raise RuntimeError(
                "Another tickrecorder process is already using data directory "
                f"{self.settings.data_dir}"
            ) from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()} run_id={self.run_id}\n")
        handle.flush()
        os.fsync(handle.fileno())
        self._data_lock_handle = handle
        LOG.info("Acquired recorder data lock path=%s", lock_path)

    def _release_data_directory_lock(self) -> None:
        handle = self._data_lock_handle
        if handle is None:
            return
        self._data_lock_handle = None
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
        LOG.info("Released recorder data lock path=%s", handle.name)

    def _release_data_directory_lock_if_safe(self) -> None:
        if self.writer.is_alive():
            LOG.error(
                "Retaining recorder data lock because the Parquet writer is still alive"
            )
            return
        self._release_data_directory_lock()

    def _log_status_if_due(self) -> None:
        now = time.monotonic()
        if now - self._last_status_at < self.settings.status_interval_seconds:
            return
        self._last_status_at = now
        with self._stats_lock:
            stats = dict(self._stats)
        LOG.info(
            "STATUS run=%s depth=%d symbol=%d control=%d queue=%d",
            self.run_id,
            stats.get("received_tbt_depth", 0),
            stats.get("received_symbol_update", 0),
            stats.get("received_control", 0),
            self.writer.queue_size(),
        )

    def _manifest_payload(self, status: str) -> dict[str, Any]:
        with self._stats_lock:
            received_stats = dict(self._stats)
        with self._quality_lock:
            degraded_reasons = sorted(self._degraded_reasons)
        with self._state_lock:
            connection_epochs = dict(self._connection_epochs)
        first_events_seen = {
            component: event.is_set()
            for component, event in self._first_event_events.items()
        }
        with self._state_lock:
            first_symbols_seen = {
                component: sorted(symbols)
                for component, symbols in self._first_symbols_seen.items()
            }
        return {
            "schema_version": SCHEMA_VERSION,
            "tickrecorder_version": __version__,
            "status": status,
            "clean_shutdown": self._clean_shutdown,
            "streams_ready": self._streams_ready,
            "first_events_seen": first_events_seen,
            "first_symbols_seen": first_symbols_seen,
            "degraded_reasons": degraded_reasons,
            "stop_reason": self.stop_reason,
            "run_id": self.run_id,
            "started_at_unix_ns": self._started_at_ns,
            "ended_at_unix_ns": self._ended_at_ns,
            "host": {
                "hostname": socket.gethostname(),
                "platform": platform.platform(),
                "python": sys.version,
                "pid": os.getpid(),
            },
            "dependencies": {
                "fyers_apiv3": self._package_version("fyers-apiv3"),
                "protobuf": self._package_version("protobuf"),
                "pyarrow": self._package_version("pyarrow"),
            },
            "connection_epochs": connection_epochs,
            "configuration": self.settings.redacted_dict(),
            "received_events": received_stats,
            "writer": self.writer.stats(),
            "trade_tick_archives": list(self._trade_tick_archives),
            "fatal_error": (
                self._redact_text(repr(self._fatal_error))
                if self._fatal_error
                else None
            ),
        }

    def _write_manifest(self, status: str) -> None:
        path = self.writer.write_json_atomic(
            "manifest.json",
            self._manifest_payload(status),
        )
        LOG.info("Wrote run manifest status=%s path=%s", status, path)

    @staticmethod
    def _package_version(package: str) -> str | None:
        try:
            return importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            return None
