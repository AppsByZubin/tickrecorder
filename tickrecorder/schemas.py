from __future__ import annotations

import pyarrow as pa


SCHEMA_VERSION = 1


COMMON_FIELDS = [
    pa.field("schema_version", pa.int16(), nullable=False),
    pa.field("run_id", pa.string(), nullable=False),
    pa.field("event_id", pa.int64(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("connection_id", pa.string(), nullable=False),
    pa.field("connection_epoch", pa.int32(), nullable=False),
    pa.field("symbol", pa.string()),
    pa.field("channel", pa.string()),
    pa.field("received_at_unix_ns", pa.int64(), nullable=False),
    pa.field("received_at_monotonic_ns", pa.int64(), nullable=False),
    pa.field("received_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("received_at_local", pa.string(), nullable=False),
    pa.field("trading_date", pa.string(), nullable=False),
    pa.field("receipt_hour", pa.string(), nullable=False),
]


SYMBOL_UPDATE_SCHEMA = pa.schema(
    COMMON_FIELDS
    + [
        pa.field("update_type", pa.string()),
        pa.field("ltp", pa.float64()),
        pa.field("last_traded_qty", pa.int64()),
        pa.field("last_traded_time", pa.int64()),
        pa.field("exchange_feed_time", pa.int64()),
        pa.field("volume_traded_today", pa.int64()),
        pa.field("bid_price", pa.float64()),
        pa.field("ask_price", pa.float64()),
        pa.field("bid_size", pa.int64()),
        pa.field("ask_size", pa.int64()),
        pa.field("total_buy_qty", pa.int64()),
        pa.field("total_sell_qty", pa.int64()),
        pa.field("average_trade_price", pa.float64()),
        pa.field("open_interest", pa.int64()),
        pa.field("open_price", pa.float64()),
        pa.field("high_price", pa.float64()),
        pa.field("low_price", pa.float64()),
        pa.field("previous_close_price", pa.float64()),
        pa.field("year_high", pa.float64()),
        pa.field("year_low", pa.float64()),
        pa.field("lower_circuit", pa.float64()),
        pa.field("upper_circuit", pa.float64()),
        pa.field("change", pa.float64()),
        pa.field("change_percent", pa.float64()),
        pa.field("present_fields", pa.list_(pa.string())),
        pa.field("sanitized_fields", pa.list_(pa.string()), nullable=False),
        pa.field("raw_json", pa.large_string(), nullable=False),
    ]
)


DEPTH_50_FLOAT = pa.list_(pa.float64(), 50)
DEPTH_50_INT = pa.list_(pa.int64(), 50)

TBT_DEPTH_SCHEMA = pa.schema(
    COMMON_FIELDS
    + [
        pa.field("total_buy_qty", pa.int64()),
        pa.field("total_sell_qty", pa.int64()),
        pa.field("bid_prices", DEPTH_50_FLOAT, nullable=False),
        pa.field("ask_prices", DEPTH_50_FLOAT, nullable=False),
        pa.field("bid_quantities", DEPTH_50_INT, nullable=False),
        pa.field("ask_quantities", DEPTH_50_INT, nullable=False),
        pa.field("bid_order_counts", DEPTH_50_INT, nullable=False),
        pa.field("ask_order_counts", DEPTH_50_INT, nullable=False),
        pa.field("is_snapshot", pa.bool_(), nullable=False),
        pa.field("feed_time", pa.int64()),
        pa.field("send_time", pa.int64()),
        pa.field("sequence_no", pa.uint64()),
        pa.field("previous_sequence_no", pa.uint64()),
        pa.field("sequence_delta", pa.int64()),
        pa.field("sequence_status", pa.string(), nullable=False),
        pa.field("is_first_after_reconnect", pa.bool_(), nullable=False),
        pa.field("payload_semantics", pa.string(), nullable=False),
        pa.field("raw_json", pa.large_string()),
    ]
)


CONTROL_SCHEMA = pa.schema(
    COMMON_FIELDS
    + [
        pa.field("component", pa.string(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("severity", pa.string(), nullable=False),
        pa.field("message", pa.large_string()),
        pa.field("details_json", pa.large_string(), nullable=False),
    ]
)


SCHEMAS = {
    "symbol_update": SYMBOL_UPDATE_SCHEMA,
    "tbt_depth": TBT_DEPTH_SCHEMA,
    "control": CONTROL_SCHEMA,
}
