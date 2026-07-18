from __future__ import annotations

from datetime import datetime, timezone

from tickrecorder.normalizers import (
    normalize_depth,
    normalize_symbol_update,
    sequence_diagnostics,
)


def common(source: str, symbol: str) -> dict:
    return {
        "schema_version": 1,
        "run_id": "run",
        "event_id": 1,
        "source": source,
        "connection_id": "connection",
        "connection_epoch": 1,
        "symbol": symbol,
        "channel": "1" if source == "tbt_depth" else None,
        "received_at_unix_ns": 1,
        "received_at_monotonic_ns": 2,
        "received_at_utc": datetime.now(timezone.utc),
        "received_at_local": "2026-07-17T09:15:00+05:30",
        "trading_date": "2026-07-17",
        "receipt_hour": "09",
    }


class FakeDepth:
    tbq = 1_000
    tsq = 900
    bidprice = [100.0 - index * 0.05 for index in range(50)]
    askprice = [100.05 + index * 0.05 for index in range(50)]
    bidqty = [index + 1 for index in range(50)]
    askqty = [index + 2 for index in range(50)]
    bidordn = [3] * 50
    askordn = [4] * 50
    snapshot = False
    timestamp = 100
    sendtime = 101
    seqNo = 42


def test_depth_normalizer_copies_all_fifty_levels() -> None:
    row = normalize_depth(
        ticker="NSE:TEST-EQ",
        message=FakeDepth(),
        common=common("tbt_depth", "NSE:TEST-EQ"),
        previous_sequence_no=40,
        include_raw_json=True,
    )

    assert len(row["bid_prices"]) == 50
    assert len(row["ask_order_counts"]) == 50
    assert row["bid_prices"][0] == 100.0
    assert row["sequence_delta"] == 2
    assert row["sequence_status"] == "gap"
    assert '"seqNo":42' in row["raw_json"]


def test_symbol_update_preserves_raw_payload_and_nulls() -> None:
    row = normalize_symbol_update(
        {
            "symbol": "NSE:TEST-EQ",
            "type": "sf",
            "ltp": 100.05,
            "last_traded_qty": 75,
            "vol_traded_today": 1_000,
            "future_field": {"value": 1},
        },
        common("symbol_update", "NSE:TEST-EQ"),
    )

    assert row["ltp"] == 100.05
    assert row["last_traded_qty"] == 75
    assert row["bid_price"] is None
    assert "future_field" in row["present_fields"]
    assert '"future_field":{"value":1}' in row["raw_json"]


def test_symbol_update_nulls_sdk_synthetic_circuit_zeroes() -> None:
    row = normalize_symbol_update(
        {
            "symbol": "NSE:TEST-EQ",
            "lower_ckt": 0,
            "upper_ckt": 0,
        },
        common("symbol_update", "NSE:TEST-EQ"),
    )

    assert row["lower_circuit"] is None
    assert row["upper_circuit"] is None
    assert row["sanitized_fields"] == ["lower_circuit", "upper_circuit"]
    assert '"lower_ckt":0' in row["raw_json"]


def test_sequence_diagnostics() -> None:
    assert sequence_diagnostics(11, 10, False) == (1, "ok")
    assert sequence_diagnostics(10, 10, False) == (0, "duplicate")
    assert sequence_diagnostics(15, 10, False) == (5, "gap")
    assert sequence_diagnostics(1, 10, True) == (-9, "reset")
    assert sequence_diagnostics(9, 10, False) == (-1, "regression")
    assert sequence_diagnostics(15, 10, False, True) == (
        5,
        "first_after_reconnect_gap",
    )


def test_depth_normalizer_preserves_uint64_sequence_precision() -> None:
    class LargeSequenceDepth(FakeDepth):
        seqNo = 9_007_199_254_740_993

    row = normalize_depth(
        ticker="NSE:TEST-EQ",
        message=LargeSequenceDepth(),
        common=common("tbt_depth", "NSE:TEST-EQ"),
        previous_sequence_no=9_007_199_254_740_992,
        include_raw_json=False,
        first_after_reconnect=True,
    )

    assert row["sequence_no"] == 9_007_199_254_740_993
    assert row["sequence_delta"] == 1
    assert row["sequence_status"] == "first_after_reconnect_ok"
