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

**Status:** done

- [x] `Outbox.Enqueue` rejects a topic or event id over the shared
      length limit with a clear, documented error, on both the
      Postgres and SQL Server dialects, before attempting any
      database write.
- [x] `Inbox.HasProcessed`/`MarkProcessed` reject an event id over
      the shared length limit with the same clear, documented error,
      on both dialects.
- [x] The limit is the same numeric value in both modules.
- [x] A test in each module's existing cross-dialect integration
      suite asserts an over-limit value is rejected identically on
      both Postgres and SQL Server.

## Comments

The limit is 255 characters (runes, not UTF-8 bytes — `NVARCHAR(255)`
is a 255-character column), matching both dialects' event_id/topic
columns. `outbox.MaxTopicLength` and `inbox.MaxEventIDLength` are
declared independently in each module (they are separate Go modules
that don't share code) but carry the same numeric value.

`Outbox.Enqueue` only validates the topic: the outbox's event id is
the auto-assigned int64 row id the Relay stamps as
`messaging.EventIDHeader`, never a caller-supplied string, so it can
never approach the limit. `Inbox.HasProcessed`/`MarkProcessed`
validate the caller-supplied event id, which is the composable
idempotency key this ticket's user story is about.

Per spec.md's Testing Decisions (more specific than this ticket's
generic wording), the outbox test extends the existing cross-dialect
`integration_test.go` (real Postgres/SQL Server via testcontainers,
since this defect is dialect-specific by nature and the fix's
practical effect — accept vs. reject a real write — is only fully
observable against a real column); the inbox test extends the
existing unit-level `dedup_test.go`, since the guard runs before any
SQL and needs no real database. Both length checks use
`utf8.RuneCountInString`, not `len()`, since NVARCHAR(255) counts
characters, not bytes — pinned by a multi-byte (`"é"`) case in each
suite: an outbox integration case proving a 255-character/510-byte
topic is accepted, and an inbox unit case proving a 256-character
multi-byte event id is rejected.
