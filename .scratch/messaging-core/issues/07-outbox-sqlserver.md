# 07 — Outbox SQL Server dialect

**What to build:** the same outbox relay loop from ticket 06, proven
against SQL Server: a new `outbox/sqlserver` submodule supplying the
`UPDLOCK`/`READPAST` batch-claim SQL in place of Postgres's `FOR
UPDATE SKIP LOCKED`. See ADR 0005.

**Blocked by:** 06 (outbox core + Postgres dialect).

**Status:** done

- [x] `outbox/sqlserver` submodule added to the workspace, supplying
      SQL Server batch-claim SQL to the same core `Relay`/`Enqueue`
      from ticket 06 — no core-package changes required to add this
      dialect.
- [x] Two concurrent relay instances against the same SQL Server table
      claim disjoint rows (same race test as ticket 06, run against
      SQL Server).
- [x] Integration test (testcontainers SQL Server, fake `Producer`):
      the full enqueue → relay → publish → delete loop from ticket 06,
      repeated against SQL Server, same assertions.
- [x] Schema/table-creation script or migration for the SQL Server
      dialect exists alongside the Postgres one.

## Comments

`outbox/sqlserver.Dialect` restates ticket 06's three SQL strings in
T-SQL, and that was the whole dialect: `store.go` is the only
non-test file this ticket adds, and no file in the core `outbox`
package changed. The seam ticket 06 chose held.

Delivered as a *package* in the `outbox` module rather than its own Go
module, matching `outbox/postgres`. The ticket says "submodule", but
`outbox/postgres` is a package too, and a separate module would mean a
`go.work` entry, a `replace`, and a `go.mod` for one file containing
four SQL strings and no imports.

The claim is `SELECT TOP (@p1) ... WITH (UPDLOCK, READPAST, ROWLOCK)
ORDER BY id`. `UPDLOCK` takes locks that outlast the `SELECT` (a plain
read lock would be released immediately and let a second relay claim
the same rows), `READPAST` steps over rows another relay holds instead
of blocking on them, and `ROWLOCK` stops a lock escalation from hiding
unclaimed rows behind a claimed one. Placeholders are ordinal
(`@p1`), which is why the tests' own business-table SQL had to move
into the backend definition.

`headers` is `VARBINARY(MAX)`, not a text type. The core encodes
headers to JSON `[]byte` and scans them back as `[]byte`; against an
`NVARCHAR` column SQL Server's implicit conversion reinterprets those
bytes as UCS-2 and silently corrupts the JSON. `created_at` is
`DATETIME2 DEFAULT SYSUTCDATETIME()` and `id` is `BIGINT
IDENTITY(1,1)`. `sqlserver.CreateTableSQL` is exported for callers'
migrations like the Postgres one, wrapped in an `IF OBJECT_ID(...) IS
NULL` guard because SQL Server has no `CREATE TABLE IF NOT EXISTS`.

Rather than copy ticket 06's integration tests, `internal/pgtest`
became `internal/dbtest`, which exposes a `Backend` per dialect
(container start, dialect, table name, and the placeholder-specific
business-table SQL the tests own). Every test in `integration_test.go`
now runs against every backend via `eachBackend`, so all five of
ticket 06's assertions — transactional enqueue, in-order publish with
the row id as `event-id`, an unconfirmed row blocking everything
behind it, two relays claiming disjoint rows, clean shutdown on
cancel — are asserted against SQL Server rather than restated for it.
Adding a dialect from here is a `Backend` entry, not a test file.

The race test was verified by mutation, as in ticket 06: dropping
`READPAST` from the claim makes
`TestConcurrentRelaysClaimDisjointRows/sqlserver` fail with "a claim
blocked on the other relay's locked rows instead of skipping them",
so the test is measuring the lock hints and not passing for free.

Note for whoever picks up ticket 09 (`inbox/sqlserver`): the SQL
Server container is much slower to start than Postgres, and these
tests start one per test. The outbox suite now takes ~3 minutes. If
the inbox suite pushes CI past its 20m timeout, sharing one container
per backend across a package is the lever.
