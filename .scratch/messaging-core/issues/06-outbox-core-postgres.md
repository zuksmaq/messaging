# 06 — Outbox core + Postgres dialect

**What to build:** the transactional outbox, end to end against a
fake `Producer` and a real Postgres database: `Enqueue` stages an
event on the caller's own `*sql.Tx`; `Relay.Run(ctx)` polls, claims a
batch under Postgres `FOR UPDATE SKIP LOCKED`, publishes each claimed
row, stamps the `EventId` header, and deletes only the rows the
producer confirmed `Persisted` — preserving per-key order by stopping
at the first failure in a batch. See ADR 0005, ADR 0007.

**Blocked by:** 01 (root contracts).

**Status:** ready-for-agent

- [ ] `Enqueue(ctx, tx *sql.Tx, topic, key, value, headers)` inserts an
      outbox row against the caller's transaction — no `Enqueue`
      variant that opens its own transaction or connection.
- [ ] Outbox row commits atomically with an unrelated write the caller
      makes on the same `*sql.Tx` (test proves this by rolling back
      the caller's transaction and asserting the outbox row is gone
      too).
- [ ] `Relay.Run(ctx)` is a plain blocking `Run(ctx) error`; caller
      starts it with `go relay.Run(ctx)`.
- [ ] `outbox/postgres` supplies the `FOR UPDATE SKIP LOCKED`
      batch-claim SQL; two concurrent relay instances against the
      same table claim disjoint rows (test with two `Relay`s racing).
- [ ] Every claimed row is published via the injected `Producer[K,V]`;
      the `event-id` header is set from the outbox row id.
- [ ] A row is only deleted after its publish returns `DeliveryStatus
      == Persisted`; `PossiblyPersisted`/`NotPersisted` leaves the row
      for the next poll.
- [ ] On a publish failure/non-`Persisted` result mid-batch, the relay
      stops publishing the rest of that batch (preserves per-key
      order) rather than skipping ahead.
- [ ] Integration test (testcontainers Postgres, fake `Producer`
      implementing the ticket-01 interface): enqueue several events in
      one transaction, run the relay, assert the fake producer
      recorded the right topic/key/value/headers in order and the
      rows are gone.
- [ ] Integration test: fake producer returns `PossiblyPersisted` for
      one row — assert that row (and nothing after it) survives to the
      next poll.
