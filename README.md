# dataflow-rs

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org/)

**JSON pipelines. One Rust binary.**

Async **ETL** (Extract → Transform → Load): poll, transform, and load data with a config file — Postgres, ClickHouse, CSV — plus incremental state, retries, and a live Web dashboard. No code changes needed to reconfigure the pipeline.

---

## Features

| Area | What you get |
|------|----------------|
| **Sources** | PostgreSQL, ClickHouse, CSV file watching |
| **Transforms** | Filter, column rename (map), group + sum aggregate |
| **Destination** | PostgreSQL (batched inserts, optional upsert via `unique_key`) |
| **Incremental** | Tracks `last_run` / processed files so restarts don't re-load everything |
| **Reliability** | Exponential backoff retries on pipeline failures |
| **Observability** | `RUST_LOG` logging + Web UI with status, metrics, and live logs (WebSocket) |
| **Ops** | Docker image + `docker-compose` for local Postgres + engine |

---

## Quick start

### Prerequisites

- [Rust](https://rustup.rs/) **1.85+** (edition 2024)
- PostgreSQL for the destination (and for Postgres sources)

### Build & run

```bash
# Clone
git clone https://github.com/Pixelzzz99/dataflow-rs.git
cd dataflow-rs

# Build
cargo build --release

# Run (config path, state file, web UI port)
RUST_LOG=info cargo run -- config/pipeline_csv.json etl_state.json 3456
```

Open the dashboard: **http://localhost:3456**

### CLI arguments

```text
etl-engine [CONFIG] [STATE] [PORT]

CONFIG   Pipeline JSON          (default: config/pipeline.json)
STATE    Persistent state file  (default: etl_state.json)
PORT     Web UI port            (default: 3000)
```

---

## Configuration

Pipelines are described in a single JSON file. Example configs live under `config/`:

| File | Source |
|------|--------|
| `config/pipeline.json` | PostgreSQL |
| `config/pipeline_csv.json` | CSV watch directory |
| `config/pipeline_clickhouse.json` | ClickHouse |

### PostgreSQL source

```json
{
  "source": {
    "type": "postgres",
    "connection_string": "postgresql://user:password@localhost:5432/source_db",
    "query": "SELECT id, user_id, amount, status, updated_at FROM transactions WHERE updated_at > $1",
    "poll_interval_secs": 5
  },
  "transforms": [
    { "type": "filter", "column": "status", "value": "active" },
    { "type": "map", "rename": { "user_id": "client_id", "amount": "total_amount" } },
    { "type": "aggregate", "group_by": "client_id", "sum": "total_amount" }
  ],
  "destination": {
    "type": "postgres",
    "connection_string": "postgresql://user:password@localhost:5432/destination_db",
    "table": "orders_summary",
    "unique_key": "client_id"
  }
}
```

The query parameter `$1` is bound to the last successful run timestamp (incremental extract).

### CSV source

```json
{
  "source": {
    "type": "csv",
    "watch_dir": "data/watched",
    "processed_dir": "data/processed",
    "delimiter": ",",
    "chunk_size": 10000,
    "poll_interval_secs": 10
  },
  "transforms": [],
  "destination": {
    "type": "postgres",
    "connection_string": "postgres://etl:etlpassword@localhost:5434/etldb",
    "table": "imported_data",
    "unique_key": null
  }
}
```

Drop `.csv` files into `watch_dir`. After a successful load, files are tracked in state (and can be moved under `processed_dir`).

### ClickHouse source

```json
{
  "source": {
    "type": "clickhouse",
    "host": "http://localhost:8123",
    "database": "default",
    "query": "SELECT id, user_id, amount, status, updated_at FROM orders WHERE updated_at > '{last_run}' FORMAT JSONEachRow",
    "username": "default",
    "password": "",
    "chunk_size": 10000,
    "poll_interval_secs": 30
  },
  "transforms": [],
  "destination": {
    "type": "postgres",
    "connection_string": "postgresql://postgres:password@localhost:5432/dest_db",
    "table": "orders",
    "unique_key": "id"
  }
}
```

Use `{last_run}` in the query template; it is replaced with the last run timestamp.

### Transform types

| Type | Fields | Description |
|------|--------|-------------|
| `filter` | `column`, `value` | Keep rows where the column equals `value` (text match) |
| `map` | `rename` | Rename columns (`old_name` → `new_name`) |
| `aggregate` | `group_by`, `sum` | Group by one column and sum a numeric column |

---

## Docker

Start destination Postgres + the engine:

```bash
docker compose up --build
```

- Postgres: `localhost:5434` (`etl` / `etlpassword` / `etldb`)
- Engine config mounted from `./docker`
- Data & state under `./data`

Or build the image alone:

```bash
docker build -t etl-engine .
docker run --rm -e RUST_LOG=info \
  -v "$(pwd)/config:/app/config:ro" \
  -v "$(pwd)/data:/app/data" \
  -p 3000:3000 \
  etl-engine /app/config/pipeline_csv.json /app/data/etl_state.json 3000
```

---

## Project layout

```text
├── config/                 # Example pipeline configs
├── docker/                 # Compose-mounted config
├── src/
│   ├── main.rs             # CLI + wiring
│   ├── config.rs           # JSON config types
│   ├── pipeline.rs         # Extract → transform → load
│   ├── extractor/          # Postgres, CSV, ClickHouse
│   ├── transformer/        # Filter, map, aggregate
│   ├── loader/             # Postgres loader
│   ├── state.rs            # Persistent state + log buffer
│   ├── retry.rs            # Backoff retries
│   └── web/                # Axum dashboard + WebSocket logs
├── docker-compose.yml
└── Dockerfile
```

---

## Development

```bash
cargo test
RUST_LOG=debug cargo run -- config/pipeline.json etl_state.json 3456
```

---

## License

This project is licensed under the [MIT License](LICENSE).
