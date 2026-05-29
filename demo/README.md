# RootWit Demo

Test a full data pipeline — SQLite → DuckDB — in 2 minutes. No cloud accounts, no Docker, no config.

---

## Setup (once)

```bash
# 1. Build the binary
go build -o rootwit .

# 2. Create the sample SQLite database (users, orders, events)
go run demo/seed/main.go
```

## Run

```bash
# Validate connections
./rootwit --config demo/demo-config.yaml --validate

# Preview schemas without syncing
./rootwit --config demo/demo-config.yaml --dry-run

# Run a full sync and exit
./rootwit --config demo/demo-config.yaml --once
```

## Query the output

After `--once` completes, open `demo/output.duckdb` with DuckDB:

```bash
duckdb demo/output.duckdb

# Inside DuckDB shell:
SELECT * FROM users;
SELECT status, COUNT(*), SUM(amount) FROM orders GROUP BY status;
SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY 2 DESC;
```

## What the demo proves

| Feature | How it's exercised |
|---|---|
| Incremental sync | `users` and `orders` use `cursor_field`; re-run `--once` and only new rows move |
| Append-only sync | `events` never re-syncs old rows |
| Schema detection | `--dry-run` prints columns and types without touching data |
| Crash recovery | Kill mid-sync (`Ctrl-C`), re-run — pipeline resumes from last safe cursor |
| Single binary | No Docker, no daemon, no dependencies beyond the binary itself |

## Re-seeding

To reset to a clean state:

```bash
rm demo/sample.db demo/output.duckdb demo/demo-state.json
go run demo/seed/main.go
```
