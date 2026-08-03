# 06 — Outbox core + Postgres dialect

**What to build:** the transactional outbox, end to end against a
fake `Producer` and a real Postgres database: `Enqueue` stages an
event on the caller's own `*sql.Tx`; `Relay.Run(ctx)` polls, claims a
batch under Postgres `FOR UPDATE SKIP LOCKED`, publishes each claimed
row, stamps the `EventId` header, and deletes only the rows the
producer confirmed `Persisted` — preserving per-key order by stopping
at the first failure in a batch. See ADR 0005, ADR 0007.

**Blocked by:** 01 (root contracts).

**Status:** done

- [x] `Enqueue(ctx, tx *sql.Tx, topic, key, value, headers)` inserts an
      outbox row against the caller's transaction — no `Enqueue`
      variant that opens its own transaction or connection.
- [x] Outbox row commits atomically with an unrelated write the caller
      makes on the same `*sql.Tx` (test proves this by rolling back
      the caller's transaction and asserting the outbox row is gone
      too).
- [x] `Relay.Run(ctx)` is a plain blocking `Run(ctx) error`; caller
      starts it with `go relay.Run(ctx)`.
- [x] `outbox/postgres` supplies the `FOR UPDATE SKIP LOCKED`
      batch-claim SQL; two concurrent relay instances against the
      same table claim disjoint rows (test with two `Relay`s racing).
- [x] Every claimed row is published via the injected `Producer[K,V]`;
      the `event-id` header is set from the outbox row id.
- [x] A row is only deleted after its publish returns `DeliveryStatus
      == Persisted`; `PossiblyPersisted`/`NotPersisted` leaves the row
      for the next poll.
- [x] On a publish failure/non-`Persisted` result mid-batch, the relay
      stops publishing the rest of that batch (preserves per-key
      order) rather than skipping ahead.
- [x] Integration test (testcontainers Postgres, fake `Producer`
      implementing the ticket-01 interface): enqueue several events in
      one transaction, run the relay, assert the fake producer
      recorded the right topic/key/value/headers in order and the
      rows are gone.
- [x] Integration test: fake producer returns `PossiblyPersisted` for
      one row — assert that row (and nothing after it) survives to the
      next poll.

## Comments

Built as `outbox.Outbox` (`New(dialect)` + `Enqueue`) and `outbox.Relay`
(`NewRelay(RelayConfig{...}, opts...)` + `Run(ctx)`), with the Postgres
SQL in `outbox/postgres.Dialect`.

Keys and values are staged as `[]byte` and the relay publishes through a
`messaging.Producer[[]byte, []byte]`, so callers serialize before
staging. The alternative — a generic `Outbox[K, V]` that serializes on
the way in — would mean importing the `kafka` module's serde into
`outbox`, dragging cgo and librdkafka into every service that only
stages rows. Same instantiation-at-bytes choice ticket 05 made for the
dead-letter producer.

The dialect seam is three SQL strings (`InsertSQL`/`ClaimSQL`/
`DeleteSQL`) rather than methods that run queries: the core owns all the
scanning, transaction handling and ordering logic, and ticket 07 only
has to restate the statements in T-SQL. `postgres.CreateTableSQL` is
exported for callers' own migrations — this package never runs DDL.

One poll is one transaction: claim under `FOR UPDATE SKIP LOCKED`,
publish each row in id order, delete each row as its publish is
confirmed `Persisted`, then commit. Holding the claim's locks until the
deletes commit is what keeps a second relay off the same rows, and
rolling back on the way out leaves anything unconfirmed staged. The
first row the producer will not confirm ends the batch, so a key's later
events can never overtake it.

`Run` treats publish and database failures as retryable — logged,
counted, retried next poll — and returns nil only when `ctx` is
cancelled. A relay that exited on a transient broker blip would be worse
than one that keeps its rows staged and waits. A full batch skips the
poll interval so a backlog drains at broker speed. Counters are
`messaging.outbox.published` and `messaging.outbox.publish_failures`.

Covered by `relay_test.go` (config validation, `Enqueue` guards, the
claim SQL's locking clause) and `integration_test.go` (real Postgres via
testcontainers, fake `Producer`): rollback discards the outbox row along
with the caller's business write, a staged batch publishes in id order
with the row id as the `event-id` header, a `PossiblyPersisted` row and
everything behind it survive until the producer confirms it, two relays
publish concurrently and claim disjoint rows, and cancelling `ctx` ends
`Run` cleanly.

The two behavioral guarantees were checked against deliberately broken
code before being trusted: dropping `SKIP LOCKED` from the claim and
letting the relay skip past an unconfirmed row each make their test
fail.
