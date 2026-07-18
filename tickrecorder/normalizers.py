from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        try:
            return int(Decimal(str(value)))
        except (InvalidOperation, TypeError, ValueError, OverflowError):
            return None


def _first(message: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in message and message[key] is not None:
            return message[key]
    return None


def _json(value: Any) -> str:
    return json.dumps(
        value,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def normalize_symbol_update(
    message: Mapping[str, Any],
    common: dict[str, Any],
) -> dict[str, Any]:
    lower_circuit = _float_or_none(_first(message, "lower_ckt", "lower_circuit"))
    upper_circuit = _float_or_none(_first(message, "upper_ckt", "upper_circuit"))
    sanitized_fields: list[str] = []
    if lower_circuit == 0.0 and upper_circuit == 0.0:
        # fyers-apiv3 3.1.14 injects zero for both fields before invoking the
        # SymbolUpdate callback. Preserve those exact values in raw_json, but
        # expose null typed values so consumers cannot mistake them for limits.
        lower_circuit = None
        upper_circuit = None
        sanitized_fields.extend(("lower_circuit", "upper_circuit"))

    return {
        **common,
        "update_type": str(message.get("type", "")) or None,
        "ltp": _float_or_none(_first(message, "ltp", "last_price", "last_traded_price")),
        "last_traded_qty": _int_or_none(
            _first(message, "last_traded_qty", "ltq", "last_quantity")
        ),
        "last_traded_time": _int_or_none(
            _first(message, "last_traded_time", "ltt", "exchange_time")
        ),
        "exchange_feed_time": _int_or_none(
            _first(message, "exch_feed_time", "exchange_feed_time")
        ),
        "volume_traded_today": _int_or_none(
            _first(message, "vol_traded_today", "vtt", "volume")
        ),
        "bid_price": _float_or_none(_first(message, "bid_price", "best_bid_price")),
        "ask_price": _float_or_none(_first(message, "ask_price", "best_ask_price")),
        "bid_size": _int_or_none(_first(message, "bid_size", "best_bid_size")),
        "ask_size": _int_or_none(_first(message, "ask_size", "best_ask_size")),
        "total_buy_qty": _int_or_none(_first(message, "tot_buy_qty", "total_buy_qty")),
        "total_sell_qty": _int_or_none(_first(message, "tot_sell_qty", "total_sell_qty")),
        "average_trade_price": _float_or_none(
            _first(message, "avg_trade_price", "atp")
        ),
        "open_interest": _int_or_none(_first(message, "OI", "oi", "open_interest")),
        "open_price": _float_or_none(message.get("open_price")),
        "high_price": _float_or_none(message.get("high_price")),
        "low_price": _float_or_none(message.get("low_price")),
        "previous_close_price": _float_or_none(
            _first(message, "prev_close_price", "previous_close_price")
        ),
        "year_high": _float_or_none(_first(message, "Yhigh", "year_high")),
        "year_low": _float_or_none(_first(message, "Ylow", "year_low")),
        "lower_circuit": lower_circuit,
        "upper_circuit": upper_circuit,
        "change": _float_or_none(_first(message, "ch", "change")),
        "change_percent": _float_or_none(_first(message, "chp", "change_percent")),
        "present_fields": sorted(str(key) for key in message.keys()),
        "sanitized_fields": sanitized_fields,
        "raw_json": _json(dict(message)),
    }


def _fixed_float_list(value: Any, field_name: str) -> list[float]:
    values = list(value) if value is not None else []
    if len(values) != 50:
        raise ValueError(f"{field_name} must contain exactly 50 values, got {len(values)}")
    result: list[float] = []
    for item in values:
        converted = _float_or_none(item)
        result.append(0.0 if converted is None else converted)
    return result


def _fixed_int_list(value: Any, field_name: str) -> list[int]:
    values = list(value) if value is not None else []
    if len(values) != 50:
        raise ValueError(f"{field_name} must contain exactly 50 values, got {len(values)}")
    result: list[int] = []
    for item in values:
        converted = _int_or_none(item)
        result.append(0 if converted is None else converted)
    return result


def sequence_diagnostics(
    current: int | None,
    previous: int | None,
    is_snapshot: bool,
    first_after_reconnect: bool = False,
) -> tuple[int | None, str]:
    if current is None or previous is None:
        return None, "first"
    delta = current - previous
    if delta == 1:
        status = "ok"
    elif delta == 0:
        status = "duplicate"
    elif delta > 1:
        status = "gap"
    elif is_snapshot:
        status = "reset"
    else:
        status = "regression"
    if first_after_reconnect:
        status = f"first_after_reconnect_{status}"
    return delta, status


def normalize_depth(
    ticker: str,
    message: Any,
    common: dict[str, Any],
    previous_sequence_no: int | None,
    include_raw_json: bool,
    first_after_reconnect: bool = False,
) -> dict[str, Any]:
    sequence_no = _int_or_none(getattr(message, "seqNo", None))
    is_snapshot = bool(getattr(message, "snapshot", False))
    sequence_delta, sequence_status = sequence_diagnostics(
        sequence_no,
        previous_sequence_no,
        is_snapshot,
        first_after_reconnect,
    )

    payload = {
        "tbq": _int_or_none(getattr(message, "tbq", None)),
        "tsq": _int_or_none(getattr(message, "tsq", None)),
        "bidprice": _fixed_float_list(getattr(message, "bidprice", None), "bidprice"),
        "askprice": _fixed_float_list(getattr(message, "askprice", None), "askprice"),
        "bidqty": _fixed_int_list(getattr(message, "bidqty", None), "bidqty"),
        "askqty": _fixed_int_list(getattr(message, "askqty", None), "askqty"),
        "bidordn": _fixed_int_list(getattr(message, "bidordn", None), "bidordn"),
        "askordn": _fixed_int_list(getattr(message, "askordn", None), "askordn"),
        "snapshot": is_snapshot,
        "timestamp": _int_or_none(getattr(message, "timestamp", None)),
        "sendtime": _int_or_none(getattr(message, "sendtime", None)),
        "seqNo": sequence_no,
    }

    return {
        **common,
        "symbol": ticker,
        "total_buy_qty": payload["tbq"],
        "total_sell_qty": payload["tsq"],
        "bid_prices": payload["bidprice"],
        "ask_prices": payload["askprice"],
        "bid_quantities": payload["bidqty"],
        "ask_quantities": payload["askqty"],
        "bid_order_counts": payload["bidordn"],
        "ask_order_counts": payload["askordn"],
        "is_snapshot": is_snapshot,
        "feed_time": payload["timestamp"],
        "send_time": payload["sendtime"],
        "sequence_no": sequence_no,
        "previous_sequence_no": previous_sequence_no,
        "sequence_delta": sequence_delta,
        "sequence_status": sequence_status,
        "is_first_after_reconnect": first_after_reconnect,
        "payload_semantics": "sdk_reconstructed_state",
        "raw_json": _json(payload) if include_raw_json else None,
    }


def normalize_control_event(
    common: dict[str, Any],
    component: str,
    event_type: str,
    severity: str,
    message: str | None,
    details: Any,
) -> dict[str, Any]:
    return {
        **common,
        "component": component,
        "event_type": event_type,
        "severity": severity,
        "message": message,
        "details_json": _json(details),
    }
