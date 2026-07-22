# tickrecorder

`tickrecorder` records the two FYERS feeds selected for DOM and proxy-footprint research:

- FYERS TBT `DEPTH`: reconstructed 50-level bid/ask state.
- FYERS `SymbolUpdate`: LTP, LTQ, trade time, cumulative volume and related quote fields.

It writes immutable Parquet part files suitable for deterministic replay. It does not calculate signals, place orders, or pretend that `SymbolUpdate` is an exchange trade tape.

## Important data semantics

The TBT callback supplied by `fyers-apiv3` is a reconstructed `Depth` object. With `diff_only=False`, each recorded depth row contains the SDK's complete post-update 50-level state:

- bid and ask prices;
- bid and ask quantities;
- bid and ask order counts;
- total buy and sell quantities;
- snapshot flag;
- feed time, send time and sequence number.

`SymbolUpdate` is recorded without deduplication. Typed columns make common fields easy to query, and `raw_json` preserves all fields supplied by the SDK for forward compatibility.

The resulting footprint will still be a proxy because FYERS does not expose a supported market-wide per-trade TBT stream. A later analysis process should attribute at most `min(LTQ, positive VTT delta)` and retain the remainder as unknown volume.

## Project layout

```text
tickrecorder/
├── .github/workflows/docker-publish.yml
├── tickrecorder/
│   ├── cli.py
│   ├── config.py
│   ├── normalizers.py
│   ├── recorder.py
│   ├── schemas.py
│   ├── spaces.py
│   └── writer.py
├── tests/
├── .env.example
├── Dockerfile
├── Makefile
├── main.py
├── pyproject.toml
└── requirements.txt
```

## Environment setup

The project is intended to use the existing Python 3.12 conda environment:

```bash
cd /home/amit/python/src/github.com/AppsByZubin/tickrecorder
conda activate tickrecorder
python -m pip install -e ".[dev]"
```

For non-interactive shells where `conda` is not initialized:

```bash
/home/amit/anaconda3/bin/conda run -n tickrecorder \
  python -m pip install -e ".[dev]"
```

Copy the example configuration:

```bash
cp .env.example .env
```

The application loads `.env` from the current working directory automatically. Existing
shell or deployment environment variables take precedence. Relative output paths are also
resolved from the current working directory.

## Required environment variables

| Variable | Required | Description |
|---|---:|---|
| `FYERS_APP_ID` | Yes | FYERS application ID, normally ending in `-100`. |
| `FYERS_ACCESS_TOKEN` | Yes | FYERS access token. It normally needs to be refreshed each trading day. |
| `FYERS_SYMBOLS` | Yes | Exact FYERS symbols, comma-separated or a JSON list. Example: `NSE:NIFTY26JULFUT`. |

`FYERS_SYMBOL` is accepted as a backwards-compatible single-symbol fallback, but `FYERS_SYMBOLS` is preferred.

### DigitalOcean Spaces

Every safely finalized date partition is archived during recorder shutdown. S3 upload is
enabled by default and can be disabled while retaining the local archive.

| Variable | Required | Default |
|---|---:|---|
| `TICKRECORDER_S3_UPLOAD_ENABLED` | No | `true` |
| `DO_S3_ENDPOINT_URL` | When upload enabled | — |
| `DO_S3_REGION` | When upload enabled | — |
| `DO_S3_ACCESS_KEY_ID` | When upload enabled | — |
| `DO_S3_SECRET_ACCESS_KEY` | When upload enabled | — |
| `DO_S3_BUCKET_NAME` | No | `index-bucket` |
| `DO_S3_SPACES_PREFIX` | No | `index-bucket-holder/contracts` |

Set `TICKRECORDER_S3_UPLOAD_ENABLED=false` to keep finalized `.tar.gz` archives locally
without requiring S3 credentials or attempting an upload. A later run with uploads enabled
will discover and upload retained date directories that do not have verified receipts.

For a `20260717` partition, the local archive and verified destination are:

```text
data/20260717_trade_ticks.tar.gz
s3://index-bucket/index-bucket-holder/contracts/20260717/20260717_trade_ticks.tar.gz
```

The archive retains `20260717/` as its top-level directory. Upload completion is verified
against the remote object size and by reading the stored object back and hashing its bytes
with SHA-256 before the run can receive a success or degraded marker. Each receipt is also
bound to the source-directory inventory and exact bucket/key, so later same-date parts or a
destination change invalidate the old receipt. An archive or upload failure produces
`_FAILED` and a nonzero exit, while retaining the source directory and local archive for
automatic retry on the next run.

## Optional environment variables

### FYERS sockets

| Variable | Default | Description |
|---|---:|---|
| `FYERS_TBT_CHANNEL` | `1` | TBT channel number from 1 through 50. |
| `TICKRECORDER_DATA_RECONNECT` | `true` | Enable regular FYERS data-socket reconnects. |
| `TICKRECORDER_DATA_RECONNECT_RETRIES` | `20` | Maximum SDK data-socket reconnect attempts (1 through 50). |
| `TICKRECORDER_TBT_RECONNECT` | `true` | Enable TBT-socket reconnects. |
| `TICKRECORDER_TBT_RECONNECT_RETRIES` | `20` | Maximum SDK TBT reconnect attempts (1 through 50). |
| `TICKRECORDER_CONNECT_TIMEOUT_SECONDS` | `15` | Maximum initial wait for both sockets to become genuinely connected. |
| `TICKRECORDER_REQUIRE_FIRST_EVENT` | `true` | Require one persisted SymbolUpdate and one persisted depth callback for every configured symbol before declaring the run ready. |
| `TICKRECORDER_FIRST_EVENT_TIMEOUT_SECONDS` | `30` | Maximum post-connect wait for those first callbacks. Set the requirement to `false` only for deliberate idle-feed capture. |
| `TICKRECORDER_DISCONNECT_GRACE_SECONDS` | `30` | Maximum continuous outage before the run fails. Any reconnect still marks the run degraded. |
| `TICKRECORDER_STALE_FEED_TIMEOUT_SECONDS` | `60` | Fail when any configured symbol stops producing valid callbacks on either feed for this long while connected. Zero disables this check for illiquid/idle capture. |
| `TICKRECORDER_SHUTDOWN_TIMEOUT_SECONDS` | `20` | Per-phase bound for FYERS SDK, in-flight callback, and Parquet-writer shutdown. |

### Parquet output

| Variable | Default | Description |
|---|---:|---|
| `TICKRECORDER_DATA_DIR` | `data` | Root directory for recorded runs. Relative paths are resolved from the current working directory. |
| `TICKRECORDER_FLUSH_INTERVAL_SECONDS` | `60` | Flush buffered rows into immutable part files at this interval. |
| `TICKRECORDER_MAX_ROWS_PER_FILE` | `5000` | Rotate a stream/partition early after this many rows. |
| `TICKRECORDER_PARQUET_COMPRESSION` | `zstd` | `zstd`, `snappy`, `gzip`, `brotli`, `lz4`, or `none`. |
| `TICKRECORDER_INCLUDE_DEPTH_RAW_JSON` | `false` | Also duplicate all depth arrays into JSON. Structured arrays are always recorded. |

### Queue and loss handling

| Variable | Default | Description |
|---|---:|---|
| `TICKRECORDER_QUEUE_MAX_EVENTS` | `50000` | Maximum callback events awaiting the Parquet writer. |
| `TICKRECORDER_QUEUE_PUT_TIMEOUT_SECONDS` | `2` | How long callbacks wait for queue space before the recorder fails closed. |

The recorder never silently drops an event. Queue exhaustion terminates the run so the dataset cannot appear complete when it is not.
The queue put timeout must be lower than the shutdown timeout.

Both `stream=symbol_update` and `stream=tbt_depth` are flushed to readable,
immutable Parquet part files while capture is running. A flush occurs at
`TICKRECORDER_FLUSH_INTERVAL_SECONDS` or when a stream partition reaches
`TICKRECORDER_MAX_ROWS_PER_FILE`, whichever happens first. Shutdown drains the
queue and force-flushes any remaining rows.

### Logging and operation

| Variable | Default | Description |
|---|---:|---|
| `TICKRECORDER_LOG_DIR` | `logs` | Application log directory. |
| `TICKRECORDER_SDK_LOG_DIR` | `logs/fyers-sdk` | Directory used by FYERS SDK log files. |
| `TICKRECORDER_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. |
| `TICKRECORDER_STATUS_INTERVAL_SECONDS` | `10` | Console status interval. |
| `TICKRECORDER_TIMEZONE` | `Asia/Kolkata` | Local timestamp and receipt-date partition timezone. |
| `TICKRECORDER_DURATION_SECONDS` | `0` | Zero uses the code-owned 09:15–15:31 IST market window. A positive value bypasses the wall-clock window and stops after that many seconds, primarily for testing or manual runs. |

Application modules share one logger and emit action-oriented messages for startup,
connections, subscriptions, readiness, Parquet publication, shutdown, and failures. Console
output is colorized when attached to a terminal; the plain daily file is written as
`logs/YYYY-MM-DD_tickrecorder.log` (or under `TICKRECORDER_LOG_DIR`). FYERS SDK logs remain
separate under `TICKRECORDER_SDK_LOG_DIR`.

Successful market-data callbacks are not logged one by one. The periodic `STATUS` message
reports aggregate event counts and queue depth without adding per-tick I/O.

### Trading-session lifecycle

Market timing is owned by tickrecorder, not Helm. A Job launched before 09:15 IST waits
without opening either FYERS WebSocket. At 09:15 it connects and records into immutable
Parquet parts. At 15:31 it requests shutdown, disconnects both sockets, drains and flushes
the writer, creates the date archive, and uploads it to DigitalOcean Spaces when S3 upload
is enabled. A Job launched after 15:31 or on a weekend exits without connecting.

## Validate configuration

This prints only redacted configuration and does not connect to FYERS:

```bash
conda activate tickrecorder
tickrecorder --validate-config
```

Or:

```bash
/home/amit/anaconda3/bin/conda run -n tickrecorder \
  tickrecorder --validate-config
```

## Run

```bash
conda activate tickrecorder
tickrecorder
```

Equivalent commands:

```bash
python -m tickrecorder
python main.py
```

Useful command-line overrides:

```bash
tickrecorder \
  --symbols NSE:NIFTY26JULFUT \
  --data-dir /data/tickrecorder \
  --duration-seconds 60 \
  --log-level DEBUG
```

The futures symbol shown above is illustrative. Always use the exact currently tradable FYERS symbol.

## Output layout

Parquet data is grouped by local receipt date and stream. Per-run metadata is
kept separately so repeated or concurrent runs cannot overwrite manifests or
completion markers:

```text
data/
├── YYYYMMDD/
│   ├── control/
│   │   └── part-control-YYYYMMDD-HHMMSSffffff.parquet
│   ├── symbolupdate/
│   │   └── part-symbolupdate-YYYYMMDD-HHMMSSffffff.parquet
│   └── tbtdepth/
│       └── part-tbtdepth-YYYYMMDD-HHMMSSffffff.parquet
├── YYYYMMDD_trade_ticks.tar.gz
├── _uploads/
│   └── YYYYMMDD.json
└── _runs/
    └── <run-id>/
        ├── manifest.json
        └── _SUCCESS, _DEGRADED, or _FAILED
```

Exactly one durable completion marker is created after socket shutdown, queue drain,
archive creation, and, when enabled, verified upload:

- `_SUCCESS`: both feeds became ready, files were archived, any enabled upload was verified, and no feed interruption or data-quality warning was detected.
- `_DEGRADED`: files were archived and any enabled upload was verified, but a reconnect/socket error or sequence discontinuity occurred.
- `_FAILED`: a feed never became ready, an outage exceeded its grace period, the writer failed, shutdown could not be verified, or archive/upload verification failed.

A hard process kill or bounded writer/callback shutdown timeout may leave a `.inprogress`
file and no completion marker. Replay code should reject or quarantine such a run.
Automated capture jobs should also treat the recorder's nonzero exit status as requiring
attention.

`manifest.json` contains:

- resolved non-secret configuration;
- Python, FYERS SDK and PyArrow versions;
- start/end times and stop reason;
- received and written row counts;
- every part filename, event range, byte size and SHA-256 checksum;
- local archive metadata and, when upload is enabled, the verified DigitalOcean object key, size, SHA-256 and ETag;
- complete/degraded/failed status and degradation reasons.

No FYERS token or DigitalOcean credential is written to the manifest or application logs.

## Recorded datasets

### `tbtdepth`

One row per FYERS TBT callback:

- a process-wide `event_id`;
- wall-clock and monotonic receipt timestamps;
- socket connection ID and reconnect epoch;
- symbol and channel;
- 50 bid prices, quantities and order counts;
- 50 ask prices, quantities and order counts;
- total buy/sell quantities;
- snapshot, feed time, send time and sequence number;
- per-symbol sequence diagnostics.

The sequence diagnostic is observational. Always preserve and analyze the raw FYERS sequence number; do not assume a detected jump can be reconstructed.

### `symbolupdate`

One row per FYERS callback:

- LTP, LTQ, LTT and cumulative volume;
- exchange feed time;
- best bid/ask prices and sizes;
- total buy/sell quantity;
- ATP, OHLC and other common fields when supplied by the SDK;
- list of fields present in the callback;
- list of typed fields sanitized because the pinned SDK value is known to be synthetic;
- complete callback JSON.

Repeated callback states are intentionally retained.

Pinned-SDK caveat: `fyers-apiv3==3.1.14` removes `OI`, `Yhigh`, and `Ylow` and injects
zero for both circuit values before invoking `SymbolUpdate`. Those removed values cannot
be recovered by this recorder. The exact injected zeroes remain in `raw_json`, while the
typed circuit columns are set to null and named in `sanitized_fields`.

### `control`

Lifecycle and data-quality evidence:

- process start/stop;
- socket connection and subscription events;
- close/error messages;
- TBT sequence gaps, duplicates, resets or regressions;
- service and unexpected messages.

Replay should union/interleave rows from all three datasets and order them by `event_id`.
The IDs are globally ordered, not relational join keys. Do not order live causality by
exchange timestamp alone.

## Query examples

DuckDB can query the dataset directly:

```sql
SELECT
    event_id,
    symbol,
    ltp,
    last_traded_qty,
    volume_traded_today,
    received_at_local
FROM read_parquet(
    'data/*/symbolupdate/*.parquet'
)
ORDER BY event_id;
```

Inspect the best level from TBT:

```sql
SELECT
    event_id,
    symbol,
    bid_prices[1] AS best_bid,
    bid_quantities[1] AS best_bid_qty,
    ask_prices[1] AS best_ask,
    ask_quantities[1] AS best_ask_qty,
    sequence_no,
    sequence_status
FROM read_parquet(
    'data/*/tbtdepth/*.parquet'
)
ORDER BY event_id;
```

## Tests

```bash
conda activate tickrecorder
pytest
ruff check .
```

The offline tests verify:

- credential and symbol configuration;
- 50-level depth normalization;
- missing-versus-zero handling for SymbolUpdate;
- sequence diagnostics;
- typed Parquet round trips;
- immutable part metadata and checksums;
- atomic `.tar.gz` creation, optional S3 upload, exact DigitalOcean object keys and upload verification.

## Container publishing

Pushes to `main` or `master`, and manual workflow dispatches, run
`.github/workflows/docker-publish.yml`. The workflow publishes
`docker.io/bizzkpm/tickrecorder:sha-<commit>` and `:latest`, then updates
`helm/tickrecorder/values.yaml` in `AppsByZubin/infrastructure` to the immutable
SHA tag.

The workflow requires these tickrecorder repository secrets:

- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`
- `INFRASTRUCTURE_REPO_TOKEN`

## Operational notes

- Stop with `Ctrl-C`, SIGINT or SIGTERM to drain the queue and create the appropriate completion marker.
- Do not use `kill -9` during normal operation.
- Mount `TICKRECORDER_DATA_DIR` on durable storage in containers or Kubernetes.
- Allow enough process-termination grace time for Parquet drain, gzip creation, and any enabled upload and verification.
- Monitor queue depth and disk space; the source date directory and compressed archive are both retained.
- Run only one recorder against a given data directory while its final archive is being created.
- The recorder does not place trades and requests no order-socket data.
