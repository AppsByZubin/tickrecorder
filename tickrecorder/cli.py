from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
from dataclasses import replace
from pathlib import Path

from tickrecorder.config import ConfigurationError, Settings
from tickrecorder.recorder import FyersTickRecorder


class RedactingFormatter(logging.Formatter):
    def __init__(self, fmt: str, secrets: set[str]) -> None:
        super().__init__(fmt)
        self.secrets = sorted((secret for secret in secrets if secret), key=len, reverse=True)

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        for secret in self.secrets:
            rendered = rendered.replace(secret, "<redacted>")
        return rendered


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


def configure_logging(settings: Settings) -> None:
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    level = getattr(logging, settings.log_level)
    formatter = RedactingFormatter(
        "%(asctime)s %(levelname)s %(name)s %(threadName)s %(message)s",
        {
            settings.ws_token,
            settings.ws_token.split(":", 1)[-1],
        },
    )
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    file_handler = logging.FileHandler(
        settings.log_dir / "tickrecorder.log",
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


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

    configure_logging(settings)
    if args.validate_config:
        print(json.dumps(settings.redacted_dict(), indent=2, sort_keys=True))
        return 0

    recorder = FyersTickRecorder(settings)

    def stop_handler(signum: int, _frame: object) -> None:
        recorder.request_stop(signal.Signals(signum).name.lower())

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)
    return recorder.run()
