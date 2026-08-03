# 5. Outbox/inbox use *sql.Tx, with per-database dialect submodules

## Status

Accepted

## Context

`outbox.Enqueue`/`inbox.MarkProcessed` need to commit atomically with
the caller's own business writes — the outbox/inbox row and the
business change must land in the same database transaction.

Mid-discussion it surfaced that SQL Server needs to be supported
alongside Postgres, not just Postgres (the repo currently only
scaffolds `outbox/postgres` and `inbox/postgres`). A Postgres-only
driver type (e.g. `pgx.Tx`) would not satisfy that.

## Decision

Core `outbox`/`inbox` packages accept a `*sql.Tx` (`database/sql`),
not a driver-specific transaction type. `Enqueue`/`MarkProcessed`
execute an `INSERT` against the caller's already-open transaction —
business writes and the outbox/inbox row commit together when the
caller commits. This works against any `database/sql` driver:
Postgres via pgx's `database/sql` stdlib adapter, SQL Server via a
`database/sql` driver.

Dialect-specific pieces — the relay's batch-claim locking SQL
(Postgres `FOR UPDATE SKIP LOCKED` vs SQL Server `UPDLOCK`/
`READPAST`) — live in `outbox/postgres` (existing) and a new
`outbox/sqlserver` submodule (and the `inbox` equivalents where
dialect matters).

## Consequences

- No pgx-specific features (native `LISTEN`/`NOTIFY`, `COPY`, richer
  type mapping) are available in the core packages — only what
  `database/sql` exposes.
- A new `outbox/sqlserver` (and `inbox/sqlserver` if needed) module
  must be added; it doesn't exist yet.
- Relay claim-batch SQL must be written and tested per dialect from
  the start, not deferred as Postgres-only.
