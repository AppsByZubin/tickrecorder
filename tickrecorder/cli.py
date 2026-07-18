from __future__ import annotations

import argparse
import json
import signal
import sys
from dataclasses import replace
from pathlib import Path

from tickrecorder.config import ConfigurationError, Settings
from tickrecorder.logger import (
    configure_logging as configure_application_logging,
    create_logger,
)
from tickrecorder.recorder import FyersTickRecorder


LOG = create_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Record FYERS SymbolUpdate and reconstructed 50-level TBT depth "
            "callbacks into immutable Parquet parts."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbols",
        help="Comma-separated FYERS symbols; overrides FYERS_SYMBOLS",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="Parquet output directory; overrides TICKRECORDER_DATA_DIR",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        help="Stop automatically after this many seconds; zero runs until signalled",
    )
    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Override TICKRECORDER_LOG_LEVEL",
    )
    parser.add_argument(
        "--validate-config",
        action="store_true",
        help="Validate and print redacted configuration without connecting",
    )
    return parser


def configure_logging(settings: Settings) -> Path:
    return configure_application_logging(
        settings.log_dir,
        settings.log_level,
        {
            settings.ws_token,
            settings.ws_token.split(":", 1)[-1],
        },
    )


def apply_cli_overrides(settings: Settings, args: argparse.Namespace) -> Settings:
    changes = {}
    if args.data_dir:
        changes["data_dir"] = args.data_dir.expanduser().resolve()
    if args.duration_seconds is not None:
        if args.duration_seconds < 0:
            raise ConfigurationError("--duration-seconds must be >= 0")
        changes["duration_seconds"] = args.duration_seconds
    if args.log_level:
        changes["log_level"] = args.log_level
    return replace(settings, **changes)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = apply_cli_overrides(
            Settings.from_env(symbols_override=args.symbols),
            args,
        )
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    log_path = configure_logging(settings)
    LOG.info(
        "Logging configured level=%s file=%s",
        settings.log_level,
        log_path,
    )
    LOG.info(
        "Configuration loaded symbols=%d data_dir=%s sdk_log_dir=%s duration_seconds=%g",
        len(settings.symbols),
        settings.data_dir,
        settings.sdk_log_dir,
        settings.duration_seconds,
    )
    LOG.debug("Configured FYERS symbols=%s", settings.symbols)
    if args.validate_config:
        LOG.info("Configuration validation completed successfully")
        print(json.dumps(settings.redacted_dict(), indent=2, sort_keys=True))
        return 0

    recorder = FyersTickRecorder(settings)

    def stop_handler(signum: int, _frame: object) -> None:
        signal_name = signal.Signals(signum).name.lower()
        LOG.info("Received process signal signal=%s", signal_name)
        recorder.request_stop(signal_name)

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    LOG.debug("Installed SIGINT and SIGTERM stop handlers")
    exit_code = recorder.run()
    LOG.info("Tick recorder exited code=%d", exit_code)
    return exit_code
