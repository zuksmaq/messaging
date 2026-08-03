# 09 — Inbox SQL Server dialect

**What to build:** the same inbox dedup loop from ticket 08, proven
against SQL Server, via a dialect submodule for whatever SQL Server
needs that differs from Postgres. See ADR 0005.

**Blocked by:** 08 (inbox core + Postgres dialect).

**Status:** done

- [x] SQL Server dialect submodule/schema added, reusing the core
      `HasProcessed`/`MarkProcessed` from ticket 08 — no core-package
      changes required to add this dialect.
- [x] Integration test (testcontainers SQL Server): the full
      mark-then-check dedup loop from ticket 08, repeated against SQL
      Server, same assertions including the concurrent-check race.
- [x] Schema/table-creation script or migration for the SQL Server
      dialect exists alongside the Postgres one.

## Comments

Ticket 08 called this correctly: the whole dialect is one file,
`inbox/sqlserver/store.go`, plus a `sqlServerBackend()` in
`inbox/internal/dbtest`. The core is untouched, and the five existing
assertions were inherited rather than restated — `eachBackend` picked
them up as soon as `Backends()` returned the second entry.

Both hints ticket 08 predicted were needed, and its reasoning for
`READPAST` on the select was exactly right: with the hint removed,
`TestHasProcessedReportsOnlyCommittedWork` reaches its
`t.Fatal("HasProcessed blocked …")` — a plain `SELECT` waits on the
exclusive key lock of an uncommitted insert, which is the stall that
would queue every consumer behind one in-flight handler. `ROWLOCK`
rides along so the engine cannot take a page lock that `READPAST`
would then skip wholesale, hiding committed ids that merely share a
page with a locked one.

**The `UPDLOCK, HOLDLOCK` half needed a test that did not exist yet,
and that is this ticket's one real finding.** Removing those hints left
the whole inherited suite green, including
`TestConcurrentDeliveriesSettleOnOneWinner` — so as written, that test
does not measure them. The reason is that it forces an ordering: the
first delivery finishes its `MarkProcessed` before the second begins,
and under read committed a plain `SELECT` in the second one blocks on
the first's uncommitted row anyway. Same observable outcome, hints or
not.

What the hints actually protect is the interleaving that test cannot
reach — both deliveries passing the `NOT EXISTS` check before either
inserts, which a single statement does not make atomic on its own.
`TestManyConcurrentDeliveriesNeverCollide` reaches it by opening ten
transactions up front and releasing them on one channel: without the
hints that fails with `Violation of PRIMARY KEY constraint`, three
deliveries at a time, and with them it is clean across repeated runs.
The error is the point — `MarkProcessed` promises a repeat reports
`false` rather than failing, and a duplicate-key error is something the
caller cannot distinguish from a real fault, so it would surface as an
unhandled message rather than a skipped one. `UPDLOCK` makes the
existence check take a lock no other checker can share and `HOLDLOCK`
holds it over the empty key range until the transaction ends, so the
second delivery waits at the check instead of racing past it.

The new test runs against both backends, not just SQL Server: Postgres
passes it unchanged (3.7s vs 22s — `ON CONFLICT DO NOTHING` already had
this covered), so it is a real gap in ticket 08's suite that only SQL
Server's need for explicit hints exposed.

Schema differences from the Postgres table are the two SQL Server
already forced on the outbox in ticket 07: no `CREATE TABLE IF NOT
EXISTS`, so the existence check is an explicit `IF OBJECT_ID(...) IS
NULL`, and the primary key is `NVARCHAR(255)` because SQL Server cannot
index an unbounded text column. `processed_at` is `DATETIME2 DEFAULT
SYSUTCDATETIME()`, matching the outbox's `created_at`.

**Note for whoever runs CI:** the ticket-07 worry now bites. The inbox
suite was 15s on Postgres alone; with SQL Server it is ~155s, six
containers at roughly 25s each, and it is container startup rather than
anything the tests wait on. Still inside the 20m timeout with room, but
the next dialect or the next test multiplies it again.

One trap worth recording: `go work sync` is the wrong tool for adding a
dependency to one module here. Run from the workspace it pushed
workspace-wide maximum versions into `inbox/go.mod` and cross-module
churn into `outbox/go.mod` and `outbox/go.sum`, which then broke the
standalone `GOWORK=off` build CI runs per module. `GOWORK=off go get`
plus `GOWORK=off go mod tidy` inside `inbox/` keeps the change to the
two direct deps and their indirects.
