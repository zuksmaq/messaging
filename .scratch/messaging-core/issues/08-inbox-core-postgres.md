# 08 — Inbox core + Postgres dialect

**What to build:** the idempotent-consumer inbox, end to end against a
real Postgres database: `HasProcessed(ctx, tx, eventID)` and
`MarkProcessed(ctx, tx, eventID)` work against the caller's own
`*sql.Tx`, deduping on the transport-agnostic `EventId`. See ADR 0005.

**Blocked by:** 01 (root contracts).

**Status:** ready-for-agent

- [ ] `MarkProcessed(ctx, tx *sql.Tx, eventID)` inserts an inbox row
      against the caller's transaction — no variant that opens its own
      transaction/connection.
- [ ] Inbox row commits atomically with an unrelated write the caller
      makes on the same `*sql.Tx` (test proves this by rolling back
      the caller's transaction and asserting the inbox row is gone
      too).
- [ ] `HasProcessed(ctx, tx, eventID)` returns `true` only after a
      committed `MarkProcessed` for that `eventID`; returns `false`
      for both an unseen id and one only staged-but-not-yet-committed
      in another transaction.
- [ ] Dedup key is the same `EventId` string used by the outbox
      (ticket 06/07) and the kafka `ReceivedMessage.EventId` — no
      Kafka-specific type in the inbox's public API.
- [ ] Integration test (testcontainers Postgres): mark an event
      processed, assert `HasProcessed` is true; assert a second
      `MarkProcessed` for the same id doesn't error or duplicate.
- [ ] Integration test: two concurrent transactions both check
      `HasProcessed` before either commits `MarkProcessed` — assert
      the expected concurrency behavior (documented, not left
      ambiguous) for the duplicate-delivery race.
