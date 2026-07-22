from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv

from tickrecorder.spaces import (
    DEFAULT_BUCKET_NAME,
    DEFAULT_SPACES_PREFIX,
    normalize_do_spaces_endpoint_url,
    normalize_s3_key,
)


SUPPORTED_COMPRESSIONS = {"brotli", "gzip", "lz4", "none", "snappy", "zstd"}


class ConfigurationError(ValueError):
    """Raised when recorder configuration is incomplete or invalid."""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ConfigurationError(f"{name} must be true or false, got {raw!r}")


def _env_int(
    name: str,
    default: int,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ConfigurationError(f"{name} must be an integer, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ConfigurationError(f"{name} must be >= {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ConfigurationError(f"{name} must be <= {maximum}, got {value}")
    return value


def _env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        value = default
    else:
        try:
            value = float(raw)
        except ValueError as exc:
            raise ConfigurationError(f"{name} must be numeric, got {raw!r}") from exc
    if minimum is not None and value < minimum:
        raise ConfigurationError(f"{name} must be >= {minimum}, got {value}")
    return value


def parse_symbols(raw: str) -> tuple[str, ...]:
    raw = raw.strip()
    if not raw:
        raise ConfigurationError(
            "Set FYERS_SYMBOLS to one or more exact FYERS symbols, separated by commas"
        )

    values: list[Any]
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConfigurationError("FYERS_SYMBOLS contains invalid JSON") from exc
        if not isinstance(parsed, list):
            raise ConfigurationError("JSON-form FYERS_SYMBOLS must be a list")
        values = parsed
    else:
        values = raw.split(",")

    symbols: list[str] = []
    seen: set[str] = set()
    for value in values:
        symbol = str(value).strip()
        if not symbol:
            continue
        normalized = symbol.upper()
        if normalized not in seen:
            symbols.append(symbol)
            seen.add(normalized)

    if not symbols:
        raise ConfigurationError("FYERS_SYMBOLS did not contain any usable symbols")
    return tuple(symbols)


def build_ws_token() -> tuple[str, str]:
    app_id = os.getenv("FYERS_APP_ID", "").strip()
    access_token = os.getenv("FYERS_ACCESS_TOKEN", "").strip()
    if not app_id or not access_token:
        raise ConfigurationError("Set both FYERS_APP_ID and FYERS_ACCESS_TOKEN")
    if access_token and ":" in access_token:
        token_app_id = access_token.split(":", 1)[0]
        if token_app_id != app_id:
            raise ConfigurationError(
                "FYERS_ACCESS_TOKEN contains an app ID that does not match FYERS_APP_ID"
            )
        return access_token, app_id
    return f"{app_id}:{access_token}", app_id


@dataclass(frozen=True)
class Settings:
    ws_token: str = field(repr=False)
    app_id: str = field(repr=False)
    symbols: tuple[str, ...]
    data_dir: Path
    log_dir: Path
    sdk_log_dir: Path
    s3_upload_enabled: bool
    do_s3_endpoint_url: str
    do_s3_region: str
    do_s3_bucket_name: str
    do_s3_spaces_prefix: str
    do_s3_access_key_id: str = field(repr=False)
    do_s3_secret_access_key: str = field(repr=False)
    tbt_channel: str
    data_reconnect: bool
    data_reconnect_retries: int
    tbt_reconnect: bool
    tbt_reconnect_retries: int
    connect_timeout_seconds: float
    require_first_event: bool
    first_event_timeout_seconds: float
    disconnect_grace_seconds: float
    stale_feed_timeout_seconds: float
    shutdown_timeout_seconds: float
    flush_interval_seconds: float
    max_rows_per_file: int
    queue_max_events: int
    queue_put_timeout_seconds: float
    parquet_compression: str
    status_interval_seconds: float
    duration_seconds: float
    include_depth_raw_json: bool
    log_level: str
    timezone: str

    @classmethod
    def from_env(
        cls,
        env_file: Path | None = None,
        symbols_override: str | None = None,
    ) -> "Settings":
        if env_file is None:
            runtime_root = Path.cwd().resolve()
            dotenv_path = runtime_root / ".env"
        else:
            dotenv_path = env_file.expanduser()
            if not dotenv_path.is_absolute():
                dotenv_path = Path.cwd() / dotenv_path
            dotenv_path = dotenv_path.resolve()
            runtime_root = dotenv_path.parent
        load_dotenv(dotenv_path=dotenv_path, override=False)

        ws_token, app_id = build_ws_token()
        symbol_text = (
            symbols_override
            if symbols_override is not None
            else os.getenv("FYERS_SYMBOLS", os.getenv("FYERS_SYMBOL", ""))
        )
        symbols = parse_symbols(symbol_text)

        raw_data_dir = os.getenv("TICKRECORDER_DATA_DIR", "data")
        raw_log_dir = os.getenv("TICKRECORDER_LOG_DIR", "logs")
        raw_sdk_log_dir = os.getenv("TICKRECORDER_SDK_LOG_DIR", "logs/fyers-sdk")

        def resolved_path(raw: str) -> Path:
            path = Path(raw).expanduser()
            if not path.is_absolute():
                path = runtime_root / path
            return path.resolve()

        s3_upload_enabled = _env_bool(
            "TICKRECORDER_S3_UPLOAD_ENABLED",
            True,
        )
        do_s3_region = os.getenv("DO_S3_REGION", "").strip()
        do_s3_bucket_name = (
            os.getenv("DO_S3_BUCKET_NAME", "").strip() or DEFAULT_BUCKET_NAME
        )
        do_s3_endpoint_url = normalize_do_spaces_endpoint_url(
            os.getenv("DO_S3_ENDPOINT_URL", "").strip(),
            do_s3_region,
            do_s3_bucket_name,
        )
        do_s3_spaces_prefix = normalize_s3_key(
            do_s3_bucket_name,
            os.getenv("DO_S3_SPACES_PREFIX", "").strip()
            or DEFAULT_SPACES_PREFIX,
        ).strip("/")
        do_s3_access_key_id = os.getenv("DO_S3_ACCESS_KEY_ID", "").strip()
        do_s3_secret_access_key = os.getenv(
            "DO_S3_SECRET_ACCESS_KEY", ""
        ).strip()
        required_spaces_values = {
            "DO_S3_ENDPOINT_URL": do_s3_endpoint_url,
            "DO_S3_REGION": do_s3_region,
            "DO_S3_ACCESS_KEY_ID": do_s3_access_key_id,
            "DO_S3_SECRET_ACCESS_KEY": do_s3_secret_access_key,
        }
        missing_spaces_values = [
            name for name, value in required_spaces_values.items() if not value
        ]
        if s3_upload_enabled and missing_spaces_values:
            raise ConfigurationError(
                "Set required DigitalOcean Spaces variables: "
                + ", ".join(missing_spaces_values)
            )

        channel = os.getenv("FYERS_TBT_CHANNEL", "1").strip()
        try:
            channel_number = int(channel)
        except ValueError as exc:
            raise ConfigurationError("FYERS_TBT_CHANNEL must be an integer from 1 to 50") from exc
        if not 1 <= channel_number <= 50:
            raise ConfigurationError("FYERS_TBT_CHANNEL must be between 1 and 50")

        compression = os.getenv("TICKRECORDER_PARQUET_COMPRESSION", "zstd").strip().lower()
        if compression not in SUPPORTED_COMPRESSIONS:
            choices = ", ".join(sorted(SUPPORTED_COMPRESSIONS))
            raise ConfigurationError(
                f"TICKRECORDER_PARQUET_COMPRESSION must be one of: {choices}"
            )

        log_level = os.getenv("TICKRECORDER_LOG_LEVEL", "INFO").strip().upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ConfigurationError(f"Unsupported TICKRECORDER_LOG_LEVEL={log_level!r}")

        timezone_name = (
            os.getenv("TICKRECORDER_TIMEZONE", "Asia/Kolkata").strip()
            or "Asia/Kolkata"
        )
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise ConfigurationError(
                f"TICKRECORDER_TIMEZONE is not a known timezone: {timezone_name!r}"
            ) from exc

        settings = cls(
            ws_token=ws_token,
            app_id=app_id,
            symbols=symbols,
            data_dir=resolved_path(raw_data_dir),
            log_dir=resolved_path(raw_log_dir),
            sdk_log_dir=resolved_path(raw_sdk_log_dir),
            s3_upload_enabled=s3_upload_enabled,
            do_s3_endpoint_url=do_s3_endpoint_url,
            do_s3_region=do_s3_region,
            do_s3_bucket_name=do_s3_bucket_name,
            do_s3_spaces_prefix=do_s3_spaces_prefix,
            do_s3_access_key_id=do_s3_access_key_id,
            do_s3_secret_access_key=do_s3_secret_access_key,
            tbt_channel=str(channel_number),
            data_reconnect=_env_bool("TICKRECORDER_DATA_RECONNECT", True),
            data_reconnect_retries=_env_int(
                "TICKRECORDER_DATA_RECONNECT_RETRIES",
                20,
                minimum=1,
                maximum=50,
            ),
            tbt_reconnect=_env_bool("TICKRECORDER_TBT_RECONNECT", True),
            tbt_reconnect_retries=_env_int(
                "TICKRECORDER_TBT_RECONNECT_RETRIES",
                20,
                minimum=1,
                maximum=50,
            ),
            connect_timeout_seconds=_env_float(
                "TICKRECORDER_CONNECT_TIMEOUT_SECONDS",
                15.0,
                minimum=1.0,
            ),
            require_first_event=_env_bool(
                "TICKRECORDER_REQUIRE_FIRST_EVENT",
                True,
            ),
            first_event_timeout_seconds=_env_float(
                "TICKRECORDER_FIRST_EVENT_TIMEOUT_SECONDS",
                30.0,
                minimum=1.0,
            ),
            disconnect_grace_seconds=_env_float(
                "TICKRECORDER_DISCONNECT_GRACE_SECONDS",
                30.0,
                minimum=1.0,
            ),
            stale_feed_timeout_seconds=_env_float(
                "TICKRECORDER_STALE_FEED_TIMEOUT_SECONDS",
                60.0,
                minimum=0.0,
            ),
            shutdown_timeout_seconds=_env_float(
                "TICKRECORDER_SHUTDOWN_TIMEOUT_SECONDS",
                20.0,
                minimum=1.0,
            ),
            flush_interval_seconds=_env_float(
                "TICKRECORDER_FLUSH_INTERVAL_SECONDS", 60.0, minimum=0.1
            ),
            max_rows_per_file=_env_int(
                "TICKRECORDER_MAX_ROWS_PER_FILE", 5_000, minimum=1
            ),
            queue_max_events=_env_int(
                "TICKRECORDER_QUEUE_MAX_EVENTS", 50_000, minimum=100
            ),
            queue_put_timeout_seconds=_env_float(
                "TICKRECORDER_QUEUE_PUT_TIMEOUT_SECONDS", 2.0, minimum=0.01
            ),
            parquet_compression=compression,
            status_interval_seconds=_env_float(
                "TICKRECORDER_STATUS_INTERVAL_SECONDS", 10.0, minimum=1.0
            ),
            duration_seconds=_env_float(
                "TICKRECORDER_DURATION_SECONDS", 0.0, minimum=0.0
            ),
            include_depth_raw_json=_env_bool(
                "TICKRECORDER_INCLUDE_DEPTH_RAW_JSON", False
            ),
            log_level=log_level,
            timezone=timezone_name,
        )
        if (
            settings.queue_put_timeout_seconds
            >= settings.shutdown_timeout_seconds
        ):
            raise ConfigurationError(
                "TICKRECORDER_QUEUE_PUT_TIMEOUT_SECONDS must be less than "
                "TICKRECORDER_SHUTDOWN_TIMEOUT_SECONDS"
            )
        return settings

    def redacted_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["ws_token"] = "<redacted>"
        result["app_id"] = "<redacted>"
        result["do_s3_access_key_id"] = "<redacted>"
        result["do_s3_secret_access_key"] = "<redacted>"
        result["symbols"] = list(self.symbols)
        for key in ("data_dir", "log_dir", "sdk_log_dir"):
            result[key] = str(result[key])
        return result
