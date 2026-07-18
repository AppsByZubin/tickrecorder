from __future__ import annotations

import pytest

from tickrecorder.config import ConfigurationError, Settings, parse_symbols


FYERS_ENV_NAMES = (
    "FYERS_WS_TOKEN",
    "FYERS_APP_ID",
    "FYERS_ACCESS_TOKEN",
    "FYERS_SYMBOL",
    "FYERS_SYMBOLS",
)


def clear_fyers_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in FYERS_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_parse_symbols_deduplicates_without_changing_first_spelling() -> None:
    assert parse_symbols("NSE:ONE-EQ, NSE:TWO-EQ,nse:one-eq") == (
        "NSE:ONE-EQ",
        "NSE:TWO-EQ",
    )


def test_settings_accept_combined_ws_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    clear_fyers_env(monkeypatch)
    monkeypatch.setenv("FYERS_WS_TOKEN", "APP-100:secret")
    monkeypatch.setenv("FYERS_SYMBOLS", '["NSE:NIFTY26JULFUT"]')
    monkeypatch.setenv("TICKRECORDER_DATA_DIR", str(tmp_path / "data"))
    settings = Settings.from_env(env_file=tmp_path / "missing.env")

    assert settings.app_id == "APP-100"
    assert settings.ws_token == "APP-100:secret"
    assert settings.symbols == ("NSE:NIFTY26JULFUT",)
    assert settings.data_dir == (tmp_path / "data").resolve()


def test_symbols_override_can_replace_missing_symbol_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    clear_fyers_env(monkeypatch)
    monkeypatch.setenv("FYERS_WS_TOKEN", "APP-100:secret")

    settings = Settings.from_env(
        env_file=tmp_path / "missing.env",
        symbols_override="NSE:ONE-EQ,NSE:TWO-EQ",
    )

    assert settings.symbols == ("NSE:ONE-EQ", "NSE:TWO-EQ")


def test_relative_paths_resolve_from_env_file_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    clear_fyers_env(monkeypatch)
    monkeypatch.setenv("FYERS_WS_TOKEN", "APP-100:secret")
    monkeypatch.setenv("FYERS_SYMBOLS", "NSE:ONE-EQ")
    monkeypatch.setenv("TICKRECORDER_DATA_DIR", "recordings")

    settings = Settings.from_env(env_file=tmp_path / ".env")

    assert settings.data_dir == (tmp_path / "recordings").resolve()


def test_settings_require_credentials(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    clear_fyers_env(monkeypatch)
    monkeypatch.setenv("FYERS_SYMBOLS", "NSE:NIFTY26JULFUT")
    with pytest.raises(ConfigurationError, match="FYERS_WS_TOKEN"):
        Settings.from_env(env_file=tmp_path / "missing.env")


def test_redacted_settings_do_not_expose_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    clear_fyers_env(monkeypatch)
    monkeypatch.setenv("FYERS_APP_ID", "APP-100")
    monkeypatch.setenv("FYERS_ACCESS_TOKEN", "very-secret")
    monkeypatch.setenv("FYERS_SYMBOLS", "NSE:NIFTY26JULFUT")
    settings = Settings.from_env(env_file=tmp_path / "missing.env")

    rendered = str(settings.redacted_dict())
    assert "very-secret" not in rendered
    assert "<redacted>" in rendered


def test_queue_timeout_must_fit_inside_shutdown_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    clear_fyers_env(monkeypatch)
    monkeypatch.setenv("FYERS_WS_TOKEN", "APP-100:secret")
    monkeypatch.setenv("FYERS_SYMBOLS", "NSE:ONE-EQ")
    monkeypatch.setenv("TICKRECORDER_QUEUE_PUT_TIMEOUT_SECONDS", "2")
    monkeypatch.setenv("TICKRECORDER_SHUTDOWN_TIMEOUT_SECONDS", "2")

    with pytest.raises(ConfigurationError, match="must be less than"):
        Settings.from_env(env_file=tmp_path / "missing.env")
