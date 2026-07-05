# Hyperliquid Node Indexer

Indexes data streams written by a [Hyperliquid node](https://github.com/hyperliquid-dex/node) into ClickHouse, with an optional WebSocket server for real-time subscriptions.

## Supported streams

- [x] `replica_cmds` -- blocks, signed action bundles, and every action with its execution status and response
- [x] `node_fills` -- fills with full detail (liquidations, builder fees, TWAP ids, etc.)
- [ ] `hip3_oracle_updates`
- [ ] `system_and_core_writer_actions`
- [ ] `misc_events`
- [ ] `evm_blocks_and_receipts`
- [ ] `node_twap_statuses`
- [ ] `node_trades`
- [ ] `node_raw_book_diffs`
- [ ] `node_order_statuses`

## Quick start

Requirements: Rust, [Task](https://taskfile.dev), Docker.

```sh
# start ClickHouse locally
docker compose up -d

# create tables
task migrate

# run against a directory of node data
cargo run --release -- --config config/default.toml
```

`data_dir` in the config must point at the node's data directory (the one containing `replica_cmds/` and `node_fills_streaming/`). To replay a specific file instead of following live data:

```sh
cargo run --release -- parse-replica-cmds --path /path/to/replica_cmds/.../file
cargo run --release -- parse-node-fills --path /path/to/node_fills_streaming/.../file
```

## Configuration

```toml
checkpoints_dir = "/opt/node-indexer/checkpoints"
data_dir = "/home/hyperliquid/hl/data"

[storage]
url = "http://localhost:8123"
user = "hl"
password = "hl"
database = "hl"

[metrics]
enabled = true
addr = "0.0.0.0:9090"

[websocket]
enabled = true
addr = "0.0.0.0:8000"
```

Checkpoints (file + byte offset per stream) are saved every 5 seconds. On restart the indexer resumes from the last checkpoint; the ClickHouse tables use `ReplacingMergeTree` to avoid duplicates.

## WebSocket API

```
wscat -c ws://localhost:8000
> {"method":"subscribe","subscription":{"type":"blocks"}}
< {"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"blocks"}}}
< {"channel":"blocks","data":{"number":123456,"hash":"0x...","proposer":"0x...","time":"2026-07-05T12:00:00Z","round":7,"parentRound":6}}
```

Subscriptions can also be requested in the connection URL:

```
ws://localhost:8000/ws?subscription=nodeFills
ws://localhost:8000/ws?subscription=blocks&subscription=nodeFills
```

Available subscription types:

| type        | channel     | payload                                  |
| ----------- | ----------- | ---------------------------------------- |
| `blocks`    | `blocks`    | one block header per message (no actions) |
| `nodeFills` | `nodeFills` | array of fills, full data                 |

`{"method":"unsubscribe",...}` stops a stream, `{"method":"ping"}` returns `{"channel":"pong"}`. Slow consumers that fall behind the 4096-message buffer receive an error message and skip ahead rather than stalling ingestion.

## Storage

One table per stream, defined in `migrations/`:

| table                  | source         | contents                                    |
| ---------------------- | -------------- | ------------------------------------------- |
| `blocks`               | `replica_cmds` | block headers                               |
| `signed_action_bundle` | `replica_cmds` | bundle hash, broadcaster, nonce             |
| `actions`              | `replica_cmds` | every action: signature, status, JSON payload |
| `node_fills`           | `node_fills`   | fills, one row per (user, fill)             |

Prices and sizes are stored as `Decimal(38, 18)`; addresses and hashes as raw `FixedString` bytes. Tables have a short TTL by default (1 day) -- adjust the migrations to your retention needs before deploying.

## Metrics

Prometheus metrics are served on `[metrics].addr`:

- `ingested_total{stream,type}` -- records ingested
- `lag_seconds{stream}` -- source timestamp to ingestion delay
- `parse_duration_seconds{stream}` -- per-line parse time
- `commit_duration_seconds{table}` -- ClickHouse batch flush time
- `tailer_lines_total` -- lines read by the tailer

## License

[MIT](LICENSE)
