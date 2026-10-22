# uge

Public version of the repository with algorithm, trade history, live state, signals, and deployment details anonymized.

A lightweight Rust trading engine for [Kalshi](https://kalshi.com) prediction markets:
an RSA-PSS signed REST client, a 5-second tick loop, a pluggable `Strategy` trait, per-tick
latency and uptime telemetry, outbound-only dashboard publishing, and git-backed state
persistence. Ships as a small Docker image with CI on every push.

## Architecture

```
            Kalshi REST API (RSA-PSS signed, pooled connections)
                               │
┌──────────────────────────────▼───────────────────────────────┐
│ engine loop (5 s tick)                                       │
│   scan watchlist slice → poll fills/settles → Strategy       │
│   → validate / cap / dedupe → place or cancel → ledger       │
└───────┬────────────────────┬────────────────────┬────────────┘
        ▼                    ▼                    ▼
   state/ ledger        scan telemetry       uge publish / persist
   (sole writer)        (p95 latency,        (outbound-only, own
                         uptime, errors)      processes; never block)
```

| module | role |
|---|---|
| `main.rs` | engine loop, env config, startup adoption of resting orders |
| `strategy.rs` | the `Strategy` trait and the toy `ThresholdBid` strategy |
| `orders.rs` | order lifecycle: validation, notional caps, dedupe, cooldowns, backoff |
| `kalshi.rs` | request signing, V2 API surface, per-call latency and error counters |
| `market.rs` / `ledger.rs` | market snapshots; bankroll, positions and settlements |
| `scanlog.rs` | per-tick health telemetry with daily rollups and rotation |
| `stats.rs` | performance stats re-derived from the settled ledger on demand |
| `subtick.rs` | re-samples watched books in the idle tail of each tick |
| `pricelog.rs` | delta-only price log, gzip-rotated |
| `publish.rs` / `persist.rs` | dashboard snapshot over bearer auth; narrow git commits of `state/` |
| `cli.rs` / `paths.rs` | subcommand dispatch; env-configurable paths |

## Running

```bash
cargo test
cargo run -- --dry-run                          # scan and evaluate, place nothing
UGE_LIVE=1 UGE_SERIES="SERIES-A" cargo run      # live; needs keys in ./keys

docker build -t uge .
docker run --rm -v "$PWD/state:/app/state" -v "$PWD/keys:/app/keys" uge --dry-run
```

Subcommands: `balance`, `orders`, `cancel <id>`, `stats`, `publish`, `persist`, plus
`--once` to run a single tick.

| variable | default | purpose |
|---|---|---|
| `UGE_ROOT` / `UGE_KEY_DIR` / `UGE_LOG_DIR` | `./state` / `./keys` / `./logs` | paths (`/app/*` in Docker) |
| `UGE_LIVE` | unset | `1` enables live order placement |
| `UGE_SERIES` | unset | comma-separated series to watch |
| `UGE_SCAN_PER_TICK` | `5` | series scanned per tick |
| `UGE_MAX_ORDER_USD` | `5` | per-order notional cap |
| `UGE_OPENING_USD` | unset | seeds a new ledger when none exists |
| `UGE_SITE_URL` / `UGE_SITE_TOKEN_FILE` | unset / `.uge-site-token` | dashboard endpoint and bearer token |
| `UGE_TOY_*` | see `strategy.rs` | toy strategy parameters |

## License

MIT
