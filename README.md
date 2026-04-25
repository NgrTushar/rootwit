# RootWit

**Reliable, lightweight data pipeline. Postgres → BigQuery. Single binary. No Docker.**

RootWit is a self-hosted sync engine that pulls data from your PostgreSQL database and loads it into Google BigQuery on a configurable schedule. It ships as a single compiled Go binary — no Docker, no Kubernetes, no JVM. Just download, configure, and run.

Built for startups that are tired of Airbyte breaking silently and Fivetran's unpredictable bills.

---

## Why RootWit?

| Problem | RootWit's Answer |
|---|---|
| Airbyte requires Docker Compose / Kubernetes, 5+ containers, 8GB RAM minimum | Single ~8MB binary. Runs on a $5/month VPS. |
| Airbyte's connectors silently stop syncing when schemas change | Explicit 4-case schema change detection. Incompatible changes halt that table and alert you immediately. |
| Fivetran charges per Monthly Active Row — unpredictable bills at scale | Self-hosted. Free forever. Flat-fee managed hosting coming soon. |
| No native alerting in Airbyte — you find out when dashboards are wrong | Slack webhook + email alerts built-in from day one. |
| Airbyte state lives in its own internal Postgres — OOM kills lose sync state | Local `state.json` with atomic writes and automatic crash recovery. No external dependency. |

---

## Quick Start

**3 steps. That's it.**

### 1. Build

```bash
git clone https://github.com/yourusername/rootwit.git
cd rootwit
go build -o rootwit .
```

### 2. Configure

```bash
cp config.example.yaml config.yaml

# Set your credentials as environment variables (never hardcode them)
export POSTGRES_HOST=your-db-host
export POSTGRES_PORT=5432
export POSTGRES_DB=your-database
export POSTGRES_USER=your-user
export POSTGRES_PASS=your-password
export GCP_PROJECT_ID=your-gcp-project
export GOOGLE_CREDENTIALS_FILE=/path/to/service-account.json
```

Edit `config.yaml` to add your tables and set sync modes. See [`config.example.yaml`](config.example.yaml) for a fully commented reference.

### 3. Run

```bash
# Test connections first
./rootwit --config config.yaml --validate

# Preview what would happen (no data moved)
./rootwit --config config.yaml --dry-run

# Run a single sync to verify everything works
./rootwit --config config.yaml --once

# Start the scheduler (syncs on your cron schedule)
./rootwit --config config.yaml
```

---

## How It Works

Every sync run follows this exact sequence for each configured table:

```
Cron fires (or --once)
  → Load state.json (crash recovery check)
  → For each table (parallel goroutines):
      → Read source schema from Postgres
      → Read destination schema from BigQuery
      → Diff schemas → classify changes (add, remove, widen, incompatible)
      → If incompatible change → halt this table, alert, continue others
      → Apply safe schema migrations automatically
      → Mark sync started → save state to disk (atomic)
      → Read rows from Postgres (cursor-based or full table)
      → Write rows to BigQuery via load jobs (free, not streaming inserts)
      → Mark sync completed → advance cursor → save state (atomic)
      → On failure: mark failed, increment failure counter, DO NOT advance cursor
  → Collect results → send alerts for any failures
```

**Key design principle:** Each table runs in its own goroutine. One table failing never prevents any other table from syncing.

---

## Sync Modes

| Mode | Behavior | Best For |
|---|---|---|
| `incremental` | Reads only rows where `cursor_field > last_cursor`. Advances the cursor only after BigQuery confirms the write. | Tables that get updated: `users`, `orders`, `accounts` |
| `append_only` | Same cursor logic as incremental, but rows are never expected to be updated at the source. | Event/log tables: `events`, `page_views`, `audit_log` |
| `full_refresh` | Writes all rows to a staging table, then atomically swaps it with the destination. The destination is never empty during a reload. | Small lookup tables: `plans`, `countries`, `config` |

### Staging-Swap for Full Refresh

Unlike naive truncate-and-reload, RootWit writes all rows to a `{table}_rootwit_staging` table first, then performs an atomic swap. Anyone querying the destination table always sees either the old data or the new data — never an empty table.

---

## Schema Change Handling

RootWit detects and classifies schema changes before every sync:

| Change Type | What Happens | Example |
|---|---|---|
| **New column in source** | Auto-added to BigQuery as `NULLABLE`. Sync continues. | `ALTER TABLE users ADD COLUMN phone TEXT` |
| **Column removed from source** | Kept in BigQuery, fills with `NULL`s going forward. | `ALTER TABLE users DROP COLUMN legacy_field` |
| **Type widened** | Auto-altered in BigQuery to the wider type. | `INT32` → `INT64` |
| **Incompatible type change** | **Sync halts for that table only.** Alert fires immediately. Other tables continue normally. | `INTEGER` → `VARCHAR` |

This is where RootWit fundamentally differs from Airbyte: schema changes are never silent. You are always informed, and incompatible changes are never auto-resolved because the risk of data corruption is too high.

---

## Crash Recovery

RootWit uses a two-phase cursor system to guarantee **at-least-once delivery** with zero data loss:

1. **Before reading any rows:** Write `cursor_value_inprogress` and `status: running` to `state.json` (atomically via temp file + rename).
2. **After BigQuery confirms the write:** Promote `cursor_value_inprogress → cursor_value`, set `status: success`, save state (atomically).

**If the process crashes mid-sync:**
- On restart, RootWit detects the incomplete sync (`started` is set but `completed` is null).
- It resumes from the **last completed cursor** — never the in-progress one.
- Rows between the last completed cursor and the crash point may be re-synced (at-least-once).
- Deduplication is handled in your BigQuery query layer (e.g., `ROW_NUMBER()` or dbt).

**State file atomicity:** `SaveState` always writes to `state.json.tmp` first, then performs `os.Rename`. A crash during write can never corrupt the existing state file.

---

## CLI Reference

| Flag | Description |
|---|---|
| `--config <path>` | Path to `config.yaml` **(required)** |
| `--validate` | Test Postgres and BigQuery connections, then exit. Host/database names are redacted in output for CI safety. |
| `--dry-run` | Detect schema changes and print diff. No data is moved. |
| `--once` | Run sync for all tables once and exit. No scheduler. |
| `--dest local` | Override destination to write JSONL files locally instead of BigQuery. Useful for testing the full pipeline without GCP credentials. |
| `--repair-state` | Attempt to recover a corrupted `state.json`. Tries to recover from `.tmp` file first. |
| `--confirm-fresh` | Used with `--repair-state`. Confirms writing a fresh empty state when no `.tmp` recovery is possible. **Warning:** this resets all cursors and causes a full re-sync. |

---

## Alerting

Configure Slack and/or email in `config.yaml`. Alerts fire on:

- **Sync failure** — after 3 retries with exponential backoff (1s → 2s → 4s)
- **Schema change detected** — even non-breaking changes (configurable: `on_schema_change: false` by default to prevent alert fatigue)
- **Sync gap** — if the last successful sync exceeds 2× the schedule interval

```yaml
alerts:
  on_failure:
    slack_webhook: ${SLACK_WEBHOOK_URL}
    email:
      smtp_host: ${SMTP_HOST}
      smtp_port: 587
      from: rootwit@company.com
      to:
        - data-team@company.com
  on_schema_change: false
  on_sync_gap: true
```

---

## Type Mapping

### Postgres → Internal Types

| Postgres Type | Internal Type | Notes |
|---|---|---|
| `int2`, `int4`, `int8`, `bigint`, `smallint` | `INT64` | |
| `float4`, `float8`, `real`, `double precision` | `FLOAT64` | |
| `numeric`, `decimal` | `NUMERIC` | Scale and precision preserved |
| `text`, `varchar`, `char` | `STRING` | |
| `boolean` | `BOOL` | |
| `timestamp`, `timestamptz` | `TIMESTAMP` | Converted to UTC |
| `date` | `DATE` | |
| `uuid` | `STRING` | BigQuery has no native UUID type |
| `json`, `jsonb` | `JSON` | |
| `bytea` | `BYTES` | |
| `ARRAY` of any type | `REPEATED` | Element type mapped recursively |
| Unknown / custom types | `STRING` | Cast to text, logged as WARNING — **never crashes** |

### Internal Types → BigQuery

All types map 1:1 to their BigQuery equivalents. All columns are created as `NULLABLE` by default. `REPEATED` fields use the inner type with BigQuery's `REPEATED` mode.

---

## Configuration Reference

```yaml
version: "1"
name: "prod-pipeline"              # Used as the connection key in state.json

source:
  type: postgres                   # Currently supported: postgres
  host: ${POSTGRES_HOST}
  port: 5432
  database: ${POSTGRES_DB}
  username: ${POSTGRES_USER}
  password: ${POSTGRES_PASS}       # MUST use ${ENV_VAR} — literal creds are rejected
  ssl_mode: require                # disable | require | verify-full
  max_connections: 5               # Concurrent Postgres connections (default: 5)
  connection_timeout_seconds: 30   # Per-connection timeout (default: 30)

destination:
  type: bigquery
  project_id: ${GCP_PROJECT_ID}
  dataset_id: rootwit_sync
  credentials_file: ${GOOGLE_CREDENTIALS_FILE}
  auto_create_dataset: true        # Creates the dataset if it doesn't exist
  location: US                     # BigQuery dataset location

sync:
  schedule: "*/30 * * * *"         # Standard cron syntax (robfig/cron)
  batch_size: 10000                # Rows per batch from source (default: 10000)
  state_file: ./rootwit-state.json

  tables:
    - name: users
      sync_mode: incremental
      cursor_field: updated_at
      # destination_table: dim_users  # Optional: override destination table name
      # timeout_minutes: 30           # Per-table timeout (default: 30)

    - name: events
      sync_mode: append_only
      cursor_field: created_at

    - name: product_catalog
      sync_mode: full_refresh
```

**Security:** RootWit refuses to start if it detects literal credentials in `config.yaml`. All secrets must use `${ENV_VAR}` substitution.

---

## Project Architecture

```
rootwit/
├── main.go                    # CLI flags, mode dispatch, connector wiring
├── config/
│   ├── config.go              # YAML loader, ${ENV_VAR} substitution, validation
│   └── types.go               # Config type definitions
├── types/
│   └── types.go               # Shared types (Schema, Row, Field, SyncResult)
│                               # Zero internal imports — leaf of the dependency graph
├── sources/
│   ├── source.go              # SourceConnector interface
│   └── postgres/
│       ├── postgres.go        # Postgres implementation (pgx connection pool)
│       └── typemap.go         # Postgres OID → internal FieldType (17 mappings)
├── destinations/
│   ├── destination.go         # DestinationConnector interface
│   ├── bigquery/
│   │   ├── bigquery.go        # BigQuery implementation (load jobs)
│   │   └── typemap.go         # Internal FieldType → BigQuery FieldType
│   └── local/
│       └── local.go           # JSONL file destination (for testing)
├── sync/
│   ├── engine.go              # Orchestration — imports only interfaces, never concrete connectors
│   ├── state.go               # Atomic state persistence, crash recovery, cursor management
│   ├── schema.go              # Schema diffing (4-case classification)
│   ├── strategies.go          # Sync mode implementations (incremental, full_refresh, append_only)
│   └── retry.go               # Exponential backoff, fatal error classification
├── scheduler/
│   └── scheduler.go           # Cron scheduler, overlap prevention (mutex), SIGTERM handling
├── alerts/
│   └── alerts.go              # Slack webhook + SMTP email alerting
├── logger/
│   └── logger.go              # Structured logging (JSON in prod, console in dev)
├── config.example.yaml        # Fully commented configuration template
└── README.md
```

### Design Principles

- **Interface-driven engine:** `sync/engine.go` imports only `sources.SourceConnector` and `destinations.DestinationConnector` interfaces. It never references `postgres` or `bigquery` directly. Connector selection happens in `main.go`.
- **Per-table isolation:** Each table syncs in its own goroutine with independent error handling. One table's failure cannot affect another.
- **Atomic state writes:** State is always written via temp file + rename. No partial writes are possible.
- **Overlap prevention:** The scheduler uses a mutex with `TryLock()`. If a sync outlasts its cron interval, the next tick is skipped rather than running concurrently.
- **Exclusive file lock:** Only one instance of RootWit can run against a given `state.json` at a time. A second instance will refuse to start.

---

## Retry & Error Handling

| Error Type | Behavior | Retries | Alert? |
|---|---|---|---|
| Network timeout / connection refused | Exponential backoff | 3 attempts (1s, 2s, 4s) | Only after all retries fail |
| BigQuery quota exceeded (429) | Wait 60s, retry once | 1 retry | Yes, if retry fails |
| Incompatible schema change | Halt table immediately | None — fatal for this table | Yes, immediate |
| Unknown Postgres column type | Map to STRING, log WARNING | N/A — continues | No |
| Auth / credentials error | Fatal — exit immediately | None | No (process exits before alerts are wired) |
| Config validation failure | Fatal — exit immediately | None | No |

---

## Local Testing (No GCP Required)

You can test the entire pipeline without BigQuery credentials using the `--dest local` flag:

```bash
./rootwit --config config.yaml --once --dest local
```

This writes JSONL files (one per table) to the directory specified by `destination.dataset_id` in your config. You can inspect the raw rows, verify schema detection, and confirm crash recovery — all locally.

---

## State File: `state.json`

The state file is human-readable JSON that tracks cursor positions, sync status, and failure counts for every table:

```json
{
  "version": "1",
  "connections": {
    "prod-pipeline": {
      "tables": {
        "users": {
          "sync_mode": "incremental",
          "cursor_field": "updated_at",
          "cursor_value": "2026-04-21T10:30:00Z",
          "cursor_value_inprogress": null,
          "last_sync_started": "2026-04-22T06:00:00Z",
          "last_sync_completed": "2026-04-22T06:00:45Z",
          "rows_synced_last_run": 1523,
          "consecutive_failures": 0,
          "status": "success"
        }
      }
    }
  }
}
```

- `cursor_value` — the last safely completed cursor. This is your guaranteed resume point.
- `cursor_value_inprogress` — set during a sync, cleared on completion. If this is non-null after a restart, the engine knows a crash occurred.
- `consecutive_failures` — increments on each failure, resets to 0 on success. Useful for backoff and escalation.

---

## Requirements

- **Source:** PostgreSQL 10+
- **Destination:** Google BigQuery (or `--dest local` for testing)
- **Runtime:** Linux, macOS, or any platform Go compiles to. No Docker. No Kubernetes. No JVM.
- **Go 1.22+** (for building from source)

## Dependencies

```
github.com/jackc/pgx/v5           # Postgres driver (native protocol)
cloud.google.com/go/bigquery       # Official BigQuery client
gopkg.in/yaml.v3                   # YAML config parsing
github.com/robfig/cron/v3          # Cron scheduler
go.uber.org/zap                    # Structured logging
```

---

## Delivery Guarantee

RootWit guarantees **at-least-once delivery**. In the normal (no-crash) path, every row is delivered exactly once. In the crash-recovery path, rows in the cursor overlap window may be delivered twice.

If you need exactly-once semantics in your analytics layer, handle deduplication in BigQuery using `ROW_NUMBER()` window functions or dbt's incremental models with a unique key.

---

## Roadmap

### Available Now

| Feature | Description |
|---|---|
| ✅ Postgres → BigQuery sync | Full table + incremental sync with cursor tracking |
| ✅ Schema change detection | 4-case classification: add, remove, widen, incompatible |
| ✅ Crash recovery | Atomic state persistence, automatic resume from last safe cursor |
| ✅ Slack + email alerting | Immediate notifications on sync failure, schema change, or sync gap |
| ✅ Per-table isolation | One table failing never blocks another — each runs in its own goroutine |
| ✅ Local testing mode | `--dest local` writes JSONL files for full pipeline testing without GCP |

### Coming Next (Q2 2026)

| Feature | Description |
|---|---|
| 🔨 Razorpay connector | Sync payment events, settlements, and refunds directly into your warehouse |
| 🔨 Stripe connector | Charges, subscriptions, invoices — all in BigQuery |
| 🔨 Managed hosted version | We run the infra. You just connect your credentials. Flat-fee pricing. |
| 🔨 MySQL source | Same engine, same reliability — for MySQL-first teams |

### Planned (Q3–Q4 2026)

| Feature | Description |
|---|---|
| 📋 CDC / real-time streaming | WAL-based Change Data Capture for sub-second sync latency |
| 📋 Web dashboard | Monitor sync health, view logs, manage connections from a browser |
| 📋 Snowflake + ClickHouse destinations | Bring your own warehouse |
| 📋 MongoDB source | Document database support |

### The Vision

RootWit is building toward a world where any startup can have **enterprise-grade data infrastructure** without enterprise-grade budgets or ops teams. The core engine will always be open source and free to self-host. Future revenue comes from managed hosting and premium support — not from locking features behind paywalls.

---

## Contributing

RootWit is open source and we welcome contributions. Whether it's a bug report, a new connector, or documentation improvements — open an issue or submit a PR.

---

## License

Licensed under the **Business Source License (BSL) 1.1**. 

You are free to self-host and modify RootWit for your own company's internal use. You may **not** use this code to build a competing managed hosting service or Data-Pipeline-as-a-Service. See the `LICENSE` file for exact terms.

---

**Website:** [rootwit.com](https://rootwit.com)

# rootwit
