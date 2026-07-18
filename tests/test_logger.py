from __future__ import annotations

import logging
import re
from pathlib import Path

import pytest

from tickrecorder.logger import (
    APPLICATION_LOGGER_NAME,
    LOG_FORMAT,
    LogFormatter,
    configure_logging,
    create_logger,
    shutdown_logging,
)


@pytest.fixture(autouse=True)
def clean_application_logging() -> None:
    shutdown_logging()
    yield
    shutdown_logging()


def test_formatter_colors_console_and_redacts_secrets() -> None:
    formatter = LogFormatter(LOG_FORMAT, {"full:access-token", "access-token"}, color=True)
    record = logging.LogRecord(
        name="tickrecorder.test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=42,
        msg="connection failed token=%s",
        args=("full:access-token",),
        exc_info=None,
    )

    rendered = formatter.format(record)

    assert rendered.startswith("\033[1;31m")
    assert rendered.endswith("\033[0m")
    assert "full:access-token" not in rendered
    assert "access-token" not in rendered
    assert "token=<redacted>" in rendered


def test_configured_logger_writes_plain_daily_file_and_redacts_console(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    log_path = configure_logging(
        tmp_path,
        "INFO",
        {"APP-100:access-token", "access-token"},
    )
    logger = create_logger("component")

    logger.debug("not emitted")
    logger.info("connected token=%s", "APP-100:access-token")

    console = capsys.readouterr().out
    file_text = log_path.read_text(encoding="utf-8")

    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}_tickrecorder\.log", log_path.name)
    assert logger.name == "tickrecorder.component"
    assert "not emitted" not in console
    assert "not emitted" not in file_text
    assert "APP-100:access-token" not in console
    assert "access-token" not in console
    assert "APP-100:access-token" not in file_text
    assert "access-token" not in file_text
    assert "connected token=<redacted>" in console
    assert "connected token=<redacted>" in file_text
    assert "\033[" not in file_text
    assert "tickrecorder.component" in file_text
    assert "[MainThread]" in file_text


def test_reconfiguration_replaces_owned_handlers_without_duplicate_messages(
    tmp_path: Path,
) -> None:
    log_path = configure_logging(tmp_path, "INFO")
    logger = create_logger("reconfigure")
    logger.info("before reconfiguration")

    configure_logging(tmp_path, "INFO")
    logger.info("after reconfiguration")

    file_text = log_path.read_text(encoding="utf-8")
    application_logger = logging.getLogger(APPLICATION_LOGGER_NAME)
    assert file_text.count("before reconfiguration") == 1
    assert file_text.count("after reconfiguration") == 1
    assert len(application_logger.handlers) == 2
