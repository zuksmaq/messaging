# 09 — Shared topic/event-id length-limit validation

**What to build:** pick one maximum length for topic name and event
id (matching the SQL Server dialect's column limit) and enforce it
identically, in both the `outbox` and `inbox` modules, at the
dialect-agnostic core (`Outbox.Enqueue` and
`Inbox.HasProcessed`/`MarkProcessed`) — validated before any
database write is attempted. An over-limit value must be rejected
the same documented way regardless of which dialect (Postgres or SQL
Server) is configured, and on the outbox side must fail *before* the
caller's own business transaction is affected.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] `Outbox.Enqueue` rejects a topic or event id over the shared
      length limit with a clear, documented error, on both the
      Postgres and SQL Server dialects, before attempting any
      database write.
- [ ] `Inbox.HasProcessed`/`MarkProcessed` reject an event id over
      the shared length limit with the same clear, documented error,
      on both dialects.
- [ ] The limit is the same numeric value in both modules.
- [ ] A test in each module's existing cross-dialect integration
      suite asserts an over-limit value is rejected identically on
      both Postgres and SQL Server.
