# 10 — End-to-end integration: outbox → Kafka → Runner → inbox

**What to build:** the full pattern this library exists for, proven
together against real infrastructure: an outbox relay (ticket 06)
publishing through the real `kafka.Producer` (ticket 02, not the fake
used in tickets 06/08), consumed by a hosted `Runner` (ticket 05) whose
handler checks/marks the inbox (ticket 08) before acting — demonstrating
at-least-once delivery with end-to-end de-duplication. Postgres only;
the SQL Server dialect tickets (07, 09) are not required for this
proof.

**Blocked by:** 02 (kafka producer), 05 (hosted Runner + poison
policy), 06 (outbox core + Postgres), 08 (inbox core + Postgres).

**Status:** ready-for-agent

- [ ] A business write + `outbox.Enqueue` commit together in one
      `*sql.Tx`; the outbox relay picks it up and publishes it via the
      real `kafka.Producer` to a real broker (testcontainers).
- [ ] A `Runner` consumes that message, its `Handler` checks
      `inbox.HasProcessed` first (skips if already seen), does its
      work, calls `inbox.MarkProcessed`, and only then the offset
      commits.
- [ ] Simulating a duplicate delivery (re-publish the same `EventId`,
      or replay the same offset) proves the handler's business effect
      does not happen twice, via the inbox check.
- [ ] Simulating a relay crash between publish and delete (e.g. kill
      the relay before the delete, restart it) proves the event is
      republished — and the inbox on the consumer side absorbs the
      resulting duplicate.
- [ ] The whole scenario runs as one integration test suite
      (testcontainers: Postgres + Kafka) that exercises real
      components at every layer except the initiating business
      write/read, which can be a minimal test fixture table.
