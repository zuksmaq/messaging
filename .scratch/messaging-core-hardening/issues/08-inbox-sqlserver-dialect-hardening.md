# 08 — Inbox SQL Server dialect hardening

**What to build:** two independent SQL Server-only correctness fixes
to the inbox dialect. First, `MarkProcessed` calls for *different*
event ids must not serialize on each other just because they land in
the same still-empty key-range gap — replace the existence-check
locking pattern with an approach that only serializes genuinely
colliding event ids (e.g. an insert-and-catch-duplicate-key
pattern). Second, concurrent `CreateTableSQL` runs against a
brand-new database (e.g. two service replicas provisioning schema at
startup) must not fail each other outright.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] Concurrent `MarkProcessed` calls for different event ids that
      fall in the same empty key-range gap no longer block on each
      other; only calls for the *same* event id serialize.
- [ ] Concurrent `MarkProcessed` calls for the *same* event id still
      behave correctly (exactly one succeeds in recording it; no
      duplicate rows).
- [ ] Two concurrent `CreateTableSQL` runs against a database where
      the table does not yet exist both complete without either
      failing outright.
- [ ] A test runs `MarkProcessed` concurrently for a spread of
      distinct event ids on a fresh/sparse table and asserts none of
      the calls block on each other beyond what's needed for their
      own row.
- [ ] A test runs `CreateTableSQL` concurrently from two connections
      against a fresh database and asserts both succeed.
