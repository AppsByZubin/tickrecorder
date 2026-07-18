from __future__ import annotations

import hashlib
import json
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from tickrecorder.logger import create_logger
from tickrecorder.schemas import SCHEMAS


LOG = create_logger(__name__)
_STOP = object()
STREAM_DIRECTORIES = {
    "control": "control",
    "symbol_update": "symbolupdate",
    "tbt_depth": "tbtdepth",
}


class RecorderBackpressureError(RuntimeError):
    """Raised instead of silently dropping market-data callbacks."""


@dataclass(frozen=True)
class EventEnvelope:
    source: str
    trading_date: str
    receipt_hour: str
    row: dict[str, Any]


class ParquetEventWriter:
    def __init__(
        self,
        output_dir: Path,
        run_id: str,
        flush_interval_seconds: float,
        max_rows_per_file: int,
        queue_max_events: int,
        queue_put_timeout_seconds: float,
        compression: str,
    ) -> None:
        self.output_dir = output_dir
        self.run_id = run_id
        self.metadata_root = output_dir / "_runs" / run_id
        # Kept as an alias for callers that locate per-run manifests and markers.
        self.run_root = self.metadata_root
        self.flush_interval_seconds = flush_interval_seconds
        self.max_rows_per_file = max_rows_per_file
        self.queue_max_events = queue_max_events
        self.queue_put_timeout_seconds = queue_put_timeout_seconds
        self.compression = None if compression == "none" else compression

        self._queue: queue.Queue[EventEnvelope | object] = queue.Queue(
            maxsize=queue_max_events
        )
        self._buffers: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        self._last_flush = time.monotonic()
        self._thread = threading.Thread(
            target=self._run,
            name="parquet-writer",
            daemon=True,
        )
        self._failure: BaseException | None = None
        self._failure_lock = threading.Lock()
        self._started = False
        self._stopped = False
        self._stats_lock = threading.Lock()
        self._rows_written: dict[str, int] = {key: 0 for key in SCHEMAS}
        self._files_written: dict[str, int] = {key: 0 for key in SCHEMAS}
        self._parts: list[dict[str, Any]] = []

    def start(self) -> None:
        if self._started:
            LOG.debug("Parquet writer start ignored run=%s", self.run_id)
            return
        self.metadata_root.mkdir(parents=True, exist_ok=False)
        self._started = True
        self._thread.start()
        LOG.info(
            "Parquet writer started run=%s metadata_dir=%s",
            self.run_id,
            self.metadata_root,
        )

    def submit(self, event: EventEnvelope) -> None:
        self.raise_if_failed()
        if not self._started or self._stopped:
            raise RuntimeError("Parquet writer is not accepting events")
        try:
            self._queue.put(event, timeout=self.queue_put_timeout_seconds)
        except queue.Full as exc:
            LOG.error(
                "Parquet writer queue full size=%d capacity=%d timeout_seconds=%g",
                self._queue.qsize(),
                self.queue_max_events,
                self.queue_put_timeout_seconds,
            )
            raise RecorderBackpressureError(
                "Recorder queue is full; terminating rather than silently dropping data"
            ) from exc

    def queue_size(self) -> int:
        return self._queue.qsize()

    def raise_if_failed(self) -> None:
        with self._failure_lock:
            failure = self._failure
        if failure is not None:
            raise RuntimeError("Parquet writer failed") from failure

    def stop(self, timeout_seconds: float | None = None) -> None:
        if not self._started or self._stopped:
            LOG.debug(
                "Parquet writer stop ignored run=%s started=%s stopped=%s",
                self.run_id,
                self._started,
                self._stopped,
            )
            self.raise_if_failed()
            return
        LOG.info(
            "Stopping Parquet writer run=%s queued=%d",
            self.run_id,
            self._queue.qsize(),
        )
        self._stopped = True
        deadline = (
            time.monotonic() + timeout_seconds
            if timeout_seconds is not None
            else None
        )
        while True:
            timeout = self.queue_put_timeout_seconds
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        "Timed out before the Parquet writer accepted its stop signal"
                    )
                timeout = min(timeout, remaining)
            try:
                self._queue.put(_STOP, timeout=timeout)
                break
            except queue.Full:
                self.raise_if_failed()
        join_timeout = None
        if deadline is not None:
            join_timeout = max(0.0, deadline - time.monotonic())
        self._thread.join(timeout=join_timeout)
        if self._thread.is_alive():
            raise TimeoutError("Timed out waiting for the Parquet writer to stop")
        self.raise_if_failed()
        stats = self.stats()
        LOG.info(
            "Parquet writer stopped run=%s rows=%d files=%d",
            self.run_id,
            sum(stats["rows_written"].values()),
            sum(stats["files_written"].values()),
        )

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def stats(self) -> dict[str, Any]:
        with self._stats_lock:
            return {
                "rows_written": dict(self._rows_written),
                "files_written": dict(self._files_written),
                "parts": list(self._parts),
                "queue_size": self._queue.qsize(),
            }

    def _set_failure(self, exc: BaseException) -> None:
        with self._failure_lock:
            self._failure = exc

    def _run(self) -> None:
        try:
            while True:
                timeout = max(
                    0.05,
                    self.flush_interval_seconds - (time.monotonic() - self._last_flush),
                )
                try:
                    item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    item = None

                if item is _STOP:
                    LOG.debug("Parquet writer received stop signal run=%s", self.run_id)
                    self._flush_all()
                    return
                if isinstance(item, EventEnvelope):
                    key = (item.source, item.trading_date, item.receipt_hour)
                    buffer = self._buffers.setdefault(key, [])
                    buffer.append(item.row)
                    if len(buffer) >= self.max_rows_per_file:
                        self._flush_key(key)

                if time.monotonic() - self._last_flush >= self.flush_interval_seconds:
                    self._flush_all()
        except BaseException as exc:
            LOG.exception("Parquet writer stopped after a fatal error")
            self._set_failure(exc)

    def _flush_all(self) -> None:
        buffered_rows = sum(len(rows) for rows in self._buffers.values())
        if buffered_rows:
            LOG.debug(
                "Flushing Parquet buffers run=%s rows=%d streams=%d",
                self.run_id,
                buffered_rows,
                sum(bool(rows) for rows in self._buffers.values()),
            )
        for key in list(self._buffers):
            self._flush_key(key)
        self._last_flush = time.monotonic()

    def _flush_key(self, key: tuple[str, str, str]) -> None:
        rows = self._buffers.get(key)
        if not rows:
            return
        source, trading_date, _receipt_hour = key
        schema = SCHEMAS[source]

        event_ids = [int(row["event_id"]) for row in rows]
        min_event = min(event_ids)
        max_event = max(event_ids)
        compact_date = trading_date.replace("-", "")
        stream_directory = STREAM_DIRECTORIES[source]
        directory = (
            self.output_dir
            / compact_date
            / stream_directory
        )
        directory.mkdir(parents=True, exist_ok=True)
        latest_row = max(rows, key=lambda row: int(row["received_at_unix_ns"]))
        local_timestamp = datetime.fromisoformat(
            str(latest_row["received_at_local"])
        ).strftime("%H%M%S%f")
        filename = (
            f"part-{stream_directory}-{compact_date}-{local_timestamp}.parquet"
        )
        final_path = directory / filename
        temporary_path = directory / f".{filename}.{uuid.uuid4().hex}.inprogress"

        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(
            table,
            temporary_path,
            compression=self.compression,
            use_dictionary=True,
            write_statistics=True,
            row_group_size=min(len(rows), self.max_rows_per_file),
        )
        self._fsync_file(temporary_path)
        final_path = self._publish_part(temporary_path, final_path)
        self._fsync_directory(directory)

        digest = self._sha256(final_path)
        relative_path = str(final_path.relative_to(self.output_dir))
        part = {
            "path": relative_path,
            "source": source,
            "rows": len(rows),
            "min_event_id": min_event,
            "max_event_id": max_event,
            "min_received_at_unix_ns": min(
                int(row["received_at_unix_ns"]) for row in rows
            ),
            "max_received_at_unix_ns": max(
                int(row["received_at_unix_ns"]) for row in rows
            ),
            "bytes": final_path.stat().st_size,
            "sha256": digest,
        }
        with self._stats_lock:
            self._rows_written[source] += len(rows)
            self._files_written[source] += 1
            self._parts.append(part)

        LOG.info(
            "Wrote %s rows=%d events=%d..%d",
            relative_path,
            len(rows),
            min_event,
            max_event,
        )
        self._buffers[key] = []

    @staticmethod
    def _publish_part(temporary_path: Path, requested_path: Path) -> Path:
        """Publish a complete part without ever replacing an existing part."""
        final_path = requested_path
        while True:
            try:
                os.link(temporary_path, final_path)
                temporary_path.unlink()
                return final_path
            except FileExistsError:
                LOG.warning("Parquet part name collision path=%s", final_path)
                final_path = requested_path.with_name(
                    f"{requested_path.stem}-{uuid.uuid4().hex[:8]}"
                    f"{requested_path.suffix}"
                )

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()

    @staticmethod
    def _fsync_file(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def write_json_atomic(self, filename: str, payload: dict[str, Any]) -> Path:
        self.metadata_root.mkdir(parents=True, exist_ok=True)
        final_path = self.metadata_root / filename
        temporary_path = self.metadata_root / f".{filename}.{uuid.uuid4().hex}.inprogress"
        with temporary_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, final_path)
        self._fsync_directory(self.metadata_root)
        LOG.info("Wrote run metadata path=%s", final_path)
        return final_path

    def write_marker_atomic(self, filename: str) -> Path:
        self.metadata_root.mkdir(parents=True, exist_ok=True)
        final_path = self.metadata_root / filename
        temporary_path = self.metadata_root / f".{filename}.{uuid.uuid4().hex}.inprogress"
        with temporary_path.open("xb") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, final_path)
        self._fsync_directory(self.metadata_root)
        LOG.info("Wrote run marker path=%s", final_path)
        return final_path
