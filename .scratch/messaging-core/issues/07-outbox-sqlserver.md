# 07 — Outbox SQL Server dialect

**What to build:** the same outbox relay loop from ticket 06, proven
against SQL Server: a new `outbox/sqlserver` submodule supplying the
`UPDLOCK`/`READPAST` batch-claim SQL in place of Postgres's `FOR
UPDATE SKIP LOCKED`. See ADR 0005.

**Blocked by:** 06 (outbox core + Postgres dialect).

**Status:** ready-for-agent

- [ ] `outbox/sqlserver` submodule added to the workspace, supplying
      SQL Server batch-claim SQL to the same core `Relay`/`Enqueue`
      from ticket 06 — no core-package changes required to add this
      dialect.
- [ ] Two concurrent relay instances against the same SQL Server table
      claim disjoint rows (same race test as ticket 06, run against
      SQL Server).
- [ ] Integration test (testcontainers SQL Server, fake `Producer`):
      the full enqueue → relay → publish → delete loop from ticket 06,
      repeated against SQL Server, same assertions.
- [ ] Schema/table-creation script or migration for the SQL Server
      dialect exists alongside the Postgres one.
