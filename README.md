# ETL Engine

A lightweight, async ETL (Extract, Transform, Load) pipeline engine written in Rust. Reads data from a PostgreSQL source, applies a configurable chain of transformations, and loads the results into a PostgreSQL destination.

## Features

- **Async pipeline** — built on Tokio and SQLx for non-blocking I/O
- **Incremental extraction** — polls the source on a configurable interval, tracking the last run timestamp
- **Composable transforms** — filter rows, rename columns, and aggregate (sum) by group
- **JSON-driven config** — the entire pipeline is described in a single `pipeline.json` file; no code changes required to reconfigure
- **Typed error handling** — a dedicated `EtlError` enum covers connection, query, transform, config, and load failures

## Project Layout

```
etl-engine/
├── config/
│   └── pipeline.json        # Pipeline configuration
└── src/
    ├── main.rs
    ├── config.rs            # Config parsing (PipelineConfig)
    ├── error.rs             # EtlError enum
    ├── types.rs             # Row / Value types
    ├── extractor/
    │   ├── mod.rs           # Extractor trait
    │   └── postgres.rs      # PostgreSQL extractor
    ├── transformer/
    │   ├── mod.rs           # Transformer trait
    │   ├── filter.rs        # FilterTransformer
    │   ├── mapper.rs        # MapTransformer (column rename)
    │   └── aggregator.rs    # AggregateTransformer (group + sum)
    └── loader/
        ├── mod.rs           # Loader trait
        └── postgres.rs      # PostgreSQL loader
```

## Configuration

Edit `config/pipeline.json` to describe your pipeline:

```json
{
  "source": {
    "type": "postgres",
    "connection_string": "postgresql://user:password@localhost:5432/source_db",
    "query": "SELECT id, user_id, amount, status, updated_at FROM transactions WHERE updated_at > $1",
    "poll_interval_secs": 5
  },
  "transforms": [
    { "type": "filter",    "column": "status", "value": "active" },
    { "type": "map",       "rename": { "user_id": "client_id", "amount": "total_amount" } },
    { "type": "aggregate", "group_by": ["client_id"], "sum": "total_amount" }
  ],
  "destination": {
    "type": "postgres",
    "connection_string": "postgresql://user:password@localhost:5432/destination_db",
    "table": "orders_summary"
  }
}
```

### Transform types

| Type | Fields | Description |
|------|--------|-------------|
| `filter` | `column`, `value` | Keep only rows where the column equals the value (text match) |
| `map` | `rename` (object) | Rename columns according to the key→value mapping |
| `aggregate` | `group_by`, `sum` | Group rows by a column and sum a numeric column per group |

## Prerequisites

- Rust 1.85+ (edition 2024)
- PostgreSQL (source and/or destination databases)

## Build & Run

```bash
# build
cargo build

# run
cargo run

# run tests
cargo test
```

## Dependencies

| Crate | Purpose |
|-------|---------|
| `tokio` | Async runtime |
| `sqlx` | Async PostgreSQL driver |
| `serde` / `serde_json` | Config deserialization |
| `async-trait` | Async methods in traits |
| `chrono` | Timestamp handling for incremental extraction |
| `log` / `env_logger` | Structured logging |

## Environment Variables

Set `RUST_LOG` to control log verbosity:

```bash
RUST_LOG=info cargo run
```
