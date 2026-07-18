from __future__ import annotations

import logging
import sys
import threading
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path


APPLICATION_LOGGER_NAME = "tickrecorder"
LOG_FORMAT = (
    "%(color_on)s%(asctime)s - %(levelname)s - %(name)s - %(lineno)d - "
    "[%(threadName)s] %(message)s%(color_off)s"
)


class LogFormatter(logging.Formatter):
    """Format application logs with optional colors and secret redaction."""

    COLOR_CODES = {
        logging.CRITICAL: "\033[1;35m",
        logging.ERROR: "\033[1;31m",
        logging.WARNING: "\033[1;33m",
        logging.INFO: "\033[0;32m",
        logging.DEBUG: "\033[1;30m",
    }
    RESET_CODE = "\033[0m"

    def __init__(
        self,
        fmt: str,
        secrets: Iterable[str] = (),
        *,
        color: bool = False,
    ) -> None:
        super().__init__(fmt)
        self.color = color
        self.secrets = sorted(
            {str(secret) for secret in secrets if secret},
            key=len,
            reverse=True,
        )

    def format(self, record: logging.LogRecord) -> str:
        missing = object()
        original_color_on = getattr(record, "color_on", missing)
        original_color_off = getattr(record, "color_off", missing)
        if self.color and record.levelno in self.COLOR_CODES:
            record.color_on = self.COLOR_CODES[record.levelno]
            record.color_off = self.RESET_CODE
        else:
            record.color_on = ""
            record.color_off = ""

        try:
            rendered = super().format(record)
        finally:
            if original_color_on is missing:
                del record.color_on
            else:
                record.color_on = original_color_on
            if original_color_off is missing:
                del record.color_off
            else:
                record.color_off = original_color_off

        for secret in self.secrets:
            rendered = rendered.replace(secret, "<redacted>")
        return rendered


_CONFIGURATION_LOCK = threading.Lock()
_CONFIGURED_HANDLERS: list[logging.Handler] = []


def create_logger(name: str) -> logging.Logger:
    """Return a named logger routed through the shared application handlers."""

    if name != APPLICATION_LOGGER_NAME and not name.startswith(
        f"{APPLICATION_LOGGER_NAME}."
    ):
        name = f"{APPLICATION_LOGGER_NAME}.{name}"
    return logging.getLogger(name)


def configure_logging(
    log_dir: Path,
    log_level: str,
    secrets: Iterable[str] = (),
) -> Path:
    """Configure colorized console output and a plain daily application log."""

    level = getattr(logging, log_level.upper(), None)
    if not isinstance(level, int):
        raise ValueError(f"Unsupported log level: {log_level!r}")

    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / datetime.now().strftime("%Y-%m-%d_tickrecorder.log")
    application_logger = logging.getLogger(APPLICATION_LOGGER_NAME)

    with _CONFIGURATION_LOCK:
        for handler in _CONFIGURED_HANDLERS:
            application_logger.removeHandler(handler)
            handler.close()
        _CONFIGURED_HANDLERS.clear()

        application_logger.setLevel(level)
        application_logger.propagate = False

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(
            LogFormatter(
                LOG_FORMAT,
                secrets,
                color=bool(getattr(sys.stdout, "isatty", lambda: False)()),
            )
        )

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(LogFormatter(LOG_FORMAT, secrets, color=False))

        application_logger.addHandler(console_handler)
        application_logger.addHandler(file_handler)
        _CONFIGURED_HANDLERS.extend((console_handler, file_handler))

    return log_path


def shutdown_logging() -> None:
    """Close application handlers, primarily for clean reconfiguration and tests."""

    application_logger = logging.getLogger(APPLICATION_LOGGER_NAME)
    with _CONFIGURATION_LOCK:
        for handler in _CONFIGURED_HANDLERS:
            application_logger.removeHandler(handler)
            handler.close()
        _CONFIGURED_HANDLERS.clear()
        application_logger.setLevel(logging.NOTSET)
        application_logger.propagate = True
