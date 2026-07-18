from __future__ import annotations

import importlib.metadata
import logging
import os
import platform
import socket
import sys
import threading
import time
import uuid
from collections import Counter
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from tickrecorder import __version__
from tickrecorder.config import Settings
from tickrecorder.normalizers import (
    normalize_control_event,
    normalize_depth,
    normalize_symbol_update,
)
from tickrecorder.schemas import SCHEMA_VERSION
from tickrecorder.writer import EventEnvelope, ParquetEventWriter


LOG = logging.getLogger("tickrecorder.recorder")


class FyersTickRecorder:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.run_id = str(uuid.uuid4())
        self.local_timezone = ZoneInfo(settings.timezone)
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
        self._tbt_symbols_seen_in_epoch: set[str] = set()
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
        }
        self._degraded_reasons: set[str] = set()
        self._streams_ready = False
        self._threads: list[threading.Thread] = []
        self._started_at_ns: int | None = None
        self._ended_at_ns: int | None = None
        self._clean_shutdown = False
        self._final_status: str | None = None
        self._fatal_error: BaseException | None = None
        self._last_status_at = 0.0
        self._started = False
        self._configured_symbols_upper = {
            configured.upper() for configured in self.settings.symbols
        }

        self.data_socket: Any = None
        self.tbt_socket: Any = None
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
            LOG.error("Fatal recorder condition reason=%s error=%s", reason, type(exc).__name__)
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
    ) -> dict[str, Any]:
        wall_ns = time.time_ns()
        monotonic_ns = time.monotonic_ns()
        utc_dt = datetime.fromtimestamp(wall_ns / 1_000_000_000, tz=timezone.utc)
        local_dt = utc_dt.astimezone(self.local_timezone)
        with self._state_lock:
            connection_id = self._connection_ids[component]
            connection_epoch = self._connection_epochs[component]
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
    ) -> None:
        common = self._common_fields(
            "control",
            component,
            symbol,
            self.settings.tbt_channel if component == "tbt" else None,
        )
        row = normalize_control_event(
            common=common,
            component=component,
            event_type=event_type,
            severity=severity,
            message=self._redact_text(message) if message is not None else None,
            details=self._redact_value(details),
        )
        self._submit("control", row)

    def _new_connection(self, component: str) -> None:
        with self._state_lock:
            previous_epoch = self._connection_epochs[component]
            self._connection_epochs[component] += 1
            self._connection_ids[component] = str(uuid.uuid4())
            if component == "tbt":
                self._tbt_symbols_seen_in_epoch.clear()
            if component in self._disconnected_since:
                self._disconnected_since[component] = None
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
        LOG.error("FYERS data socket error: %s", redacted_message)
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
        LOG.warning("FYERS data socket closed: %s", redacted_message)
        if not self.stop_event.is_set():
            self._mark_degraded("data_socket_closed")
        self._safe_control(
            "data",
            "socket_close",
            {"payload": redacted_message},
            severity="WARNING",
            message=redacted_message,
        )

    def on_data_open(self) -> None:
        if not self._wait_until_connected("data", self.data_socket):
            return
        self._new_connection("data")
        LOG.info("FYERS data socket opened; subscribing SymbolUpdate for %s", self.settings.symbols)
        self._safe_control(
            "data",
            "socket_open",
            {"symbols": list(self.settings.symbols)},
        )
        try:
            self.data_socket.subscribe(
                symbols=list(self.settings.symbols),
                data_type="SymbolUpdate",
            )
            self._safe_control(
                "data",
                "subscribe_sent",
                {"symbols": list(self.settings.symbols), "data_type": "SymbolUpdate"},
            )
            if not self.stop_event.is_set():
                self._ready_events["data"].set()
        except BaseException as exc:
            LOG.exception("Data-socket subscription failed")
            self._set_fatal(exc, "data_subscription_failure")

    def on_tbt_depth(self, ticker: str, message: Any) -> None:
        try:
            ticker = str(ticker)
            if ticker.upper() not in self._configured_symbols_upper:
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
                connection_epoch = self._connection_epochs["tbt"]
                first_after_reconnect = (
                    connection_epoch > 1
                    and ticker not in self._tbt_symbols_seen_in_epoch
                    and previous_sequence is not None
                )
            common = self._common_fields(
                "tbt_depth",
                "tbt",
                ticker,
                self.settings.tbt_channel,
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
                self._tbt_symbols_seen_in_epoch.add(ticker)
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
                )
            if base_status in {"gap", "regression"}:
                self._mark_degraded(f"tbt_sequence_{base_status}")
        except BaseException as exc:
            LOG.exception("Failed to record FYERS TBT callback")
            self._set_fatal(exc, "tbt_callback_failure")

    def on_tbt_server_error(self, message: Any) -> None:
        redacted_message = self._redact_text(message)
        LOG.error("FYERS TBT server error: %s", redacted_message)
        if not self.stop_event.is_set():
            self._mark_degraded("tbt_server_error")
        self._safe_control(
            "tbt",
            "server_error",
            {"payload": redacted_message},
            severity="ERROR",
            message=redacted_message,
        )
        if not self.stop_event.is_set():
            self._set_fatal(
                RuntimeError(
                    "FYERS TBT server rejected the request; see the control stream"
                ),
                "tbt_server_error",
            )

    def on_tbt_error(self, message: Any) -> None:
        redacted_message = self._redact_text(message)
        LOG.error("FYERS TBT socket error: %s", redacted_message)
        if not self.stop_event.is_set():
            self._mark_degraded("tbt_socket_error")
        self._safe_control(
            "tbt",
            "socket_error",
            {"payload": redacted_message},
            severity="ERROR",
            message=redacted_message,
        )

    def on_tbt_close(self, message: Any) -> None:
        redacted_message = self._redact_text(message)
        LOG.warning("FYERS TBT socket closed: %s", redacted_message)
        if not self.stop_event.is_set():
            self._mark_degraded("tbt_socket_closed")
        self._safe_control(
            "tbt",
            "socket_close",
            {"payload": redacted_message},
            severity="WARNING",
            message=redacted_message,
        )

    def on_tbt_open(self) -> None:
        if self.stop_event.is_set():
            return
        self._new_connection("tbt")
        LOG.info(
            "FYERS TBT opened; SDK will activate channel=%s symbols=%s",
            self.settings.tbt_channel,
            self.settings.symbols,
        )
        self._safe_control(
            "tbt",
            "socket_open",
            {
                "symbols": list(self.settings.symbols),
                "channel": self.settings.tbt_channel,
                "subscription_delivery": "sdk_automatic_after_callback",
            },
        )
        if not self.stop_event.is_set():
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

    def _connect_tbt_socket(self) -> None:
        try:
            self._safe_control(
                "tbt",
                "socket_connecting",
                {
                    "symbols": list(self.settings.symbols),
                    "channel": self.settings.tbt_channel,
                },
            )
            self.tbt_socket = self.FyersTbtSocket(
                access_token=self.settings.ws_token,
                write_to_file=False,
                log_path=str(self.settings.sdk_log_dir),
                on_open=self.on_tbt_open,
                on_close=self.on_tbt_close,
                on_error=self.on_tbt_error,
                on_depth_update=self.on_tbt_depth,
                on_error_message=self.on_tbt_server_error,
                reconnect=self.settings.tbt_reconnect,
                diff_only=False,
                reconnect_retry=self.settings.tbt_reconnect_retries,
            )
            self.tbt_socket.background_flag = True
            subscription_info = getattr(self.tbt_socket, "_subsinfo", None)
            if subscription_info is None:
                raise RuntimeError(
                    "Pinned FYERS TBT SDK no longer exposes subscription state"
                )
            subscription_info.subscribe(
                set(self.settings.symbols),
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
                    "symbols": list(self.settings.symbols),
                    "channel": self.settings.tbt_channel,
                    "mode": "depth",
                    "diff_only": False,
                    "delivery": "sdk_automatic_on_open",
                },
            )
            if self.stop_event.is_set():
                return
            # The FYERS TBT shutdown path expects this helper thread to exist.
            # Start it once per recorder, not from every reconnect callback.
            self.tbt_socket.keep_running()
            self.tbt_socket.connect()
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
        now = time.monotonic()
        for component, socket_object in (
            ("data", self.data_socket),
            ("tbt", self.tbt_socket),
        ):
            if not self._ready_events[component].is_set() or socket_object is None:
                continue
            if self._check_feed_staleness(component, now):
                return
            connected = self._socket_connected(socket_object)
            new_disconnect = False
            with self._state_lock:
                disconnected_since = self._disconnected_since[component]
                if connected:
                    self._disconnected_since[component] = None
                elif disconnected_since is None:
                    disconnected_since = now
                    self._disconnected_since[component] = now
                    new_disconnect = True

            if connected:
                continue

            if new_disconnect:
                self._mark_degraded(f"{component}_connection_interrupted")
                self._safe_control(
                    component,
                    "disconnect_detected",
                    {
                        "grace_seconds": self.settings.disconnect_grace_seconds,
                    },
                    severity="WARNING",
                )

            reconnect_attempts = int(getattr(socket_object, "reconnect_attempts", 0))
            max_attempts = int(
                getattr(socket_object, "max_reconnect_attempts", 0)
            )
            retries_exhausted = (
                max_attempts > 0 and reconnect_attempts >= max_attempts
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
                        "outage_seconds": outage_seconds,
                        "reconnect_attempts": reconnect_attempts,
                        "max_reconnect_attempts": max_attempts,
                        "grace_seconds": self.settings.disconnect_grace_seconds,
                    },
                    severity="ERROR",
                    message=str(exc),
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
        exc = RuntimeError(
            f"FYERS {component} feed stopped delivering valid callbacks"
        )
        self._safe_control(
            component,
            "feed_stale",
            details,
            severity="ERROR",
            message=str(exc),
        )
        self._set_fatal(exc, reason)
        return True

    def start(self) -> None:
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
            },
        )

        data_thread = threading.Thread(
            target=self._connect_data_socket,
            name="fyers-data-connector",
            daemon=True,
        )
        tbt_thread = threading.Thread(
            target=self._connect_tbt_socket,
            name="fyers-tbt-connector",
            daemon=True,
        )
        self._threads = [data_thread, tbt_thread]
        for thread in self._threads:
            thread.start()

    def run(self) -> int:
        exit_code = 0
        try:
            self.start()
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
        return exit_code

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
        deadline = time.monotonic() + self.settings.shutdown_timeout_seconds
        socket_items = [
            ("tbt", self.tbt_socket),
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

    def stop(self) -> None:
        if self._ended_at_ns is not None:
            return
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
            raise writer_error
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
        if writer_error is not None:
            raise writer_error

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
            "fatal_error": (
                self._redact_text(repr(self._fatal_error))
                if self._fatal_error
                else None
            ),
        }

    def _write_manifest(self, status: str) -> None:
        self.writer.write_json_atomic(
            "manifest.json",
            self._manifest_payload(status),
        )

    @staticmethod
    def _package_version(package: str) -> str | None:
        try:
            return importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            return None
