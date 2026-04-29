# Contributing to RootWit

Thanks for wanting to help. RootWit is early-stage and actively built — contributions have real impact.

**Before anything else: say hi.** Open an issue or email [tushr.nagr@gmail.com](mailto:tushr.nagr@gmail.com) so we can coordinate. It takes 2 minutes and avoids you building something that's already in progress or out of scope.

---

## What's needed right now

- **New connectors** — MySQL source, Snowflake destination, and others. The connector interface is stable and documented in `sources/` and `destinations/`.
- **Bug reports from real environments** — if you're running this against a production Postgres, your environment will break things a local dev machine never could. Open an issue.
- **Schema edge cases** — unusual Postgres column types, partitioned tables, generated columns. Open an issue with a reproducible case.

## What's out of scope (don't build these)

To keep RootWit reliable and simple, some things are intentionally excluded:

- Web dashboard or UI (deferred to Phase 3)
- Data transformation (never — use dbt)
- Streaming inserts to BigQuery (load jobs only)
- Multi-tenancy
- Deduplication sync mode
- Any dependency that requires running a second process alongside the binary

If you're unsure whether something fits, ask before building.

## How to contribute

1. Fork the repo and create a branch.
2. Make your change. Run `go test ./...` — all tests must pass.
3. Open a PR with a clear description of what and why.

## Connector interface

New sources implement `sources.Source`. New destinations implement `destinations.Destination`. Both interfaces are in `types/`. Don't modify the interface itself — it's locked for Phase 0/1.

## Contact

Tushar — [tushr.nagr@gmail.com](mailto:tushr.nagr@gmail.com)
