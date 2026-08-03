# 09 — Inbox SQL Server dialect

**What to build:** the same inbox dedup loop from ticket 08, proven
against SQL Server, via a dialect submodule for whatever SQL Server
needs that differs from Postgres. See ADR 0005.

**Blocked by:** 08 (inbox core + Postgres dialect).

**Status:** ready-for-agent

- [ ] SQL Server dialect submodule/schema added, reusing the core
      `HasProcessed`/`MarkProcessed` from ticket 08 — no core-package
      changes required to add this dialect.
- [ ] Integration test (testcontainers SQL Server): the full
      mark-then-check dedup loop from ticket 08, repeated against SQL
      Server, same assertions including the concurrent-check race.
- [ ] Schema/table-creation script or migration for the SQL Server
      dialect exists alongside the Postgres one.
