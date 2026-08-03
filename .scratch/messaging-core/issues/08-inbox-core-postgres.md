# 08 — Inbox core + Postgres dialect

**What to build:** the idempotent-consumer inbox, end to end against a
real Postgres database: `HasProcessed(ctx, tx, eventID)` and
`MarkProcessed(ctx, tx, eventID)` work against the caller's own
`*sql.Tx`, deduping on the transport-agnostic `EventId`. See ADR 0005.

**Blocked by:** 01 (root contracts).

**Status:** done

- [x] `MarkProcessed(ctx, tx *sql.Tx, eventID)` inserts an inbox row
      against the caller's transaction — no variant that opens its own
      transaction/connection.
- [x] Inbox row commits atomically with an unrelated write the caller
      makes on the same `*sql.Tx` (test proves this by rolling back
      the caller's transaction and asserting the inbox row is gone
      too).
- [x] `HasProcessed(ctx, tx, eventID)` returns `true` only after a
      committed `MarkProcessed` for that `eventID`; returns `false`
      for both an unseen id and one only staged-but-not-yet-committed
      in another transaction.
- [x] Dedup key is the same `EventId` string used by the outbox
      (ticket 06/07) and the kafka `ReceivedMessage.EventId` — no
      Kafka-specific type in the inbox's public API.
- [x] Integration test (testcontainers Postgres): mark an event
      processed, assert `HasProcessed` is true; assert a second
      `MarkProcessed` for the same id doesn't error or duplicate.
- [x] Integration test: two concurrent transactions both check
      `HasProcessed` before either commits `MarkProcessed` — assert
      the expected concurrency behavior (documented, not left
      ambiguous) for the duplicate-delivery race.

## Comments

The inbox mirrors the outbox's shape: a `Dialect` interface of SQL
strings, a core that holds no SQL, and `inbox/postgres` supplying it.
Two methods, both taking the caller's `*sql.Tx`, no `*sql.DB` variant
anywhere.

**`MarkProcessed` returns `(bool, error)`, and that is the ticket's one
real design decision.** The last two checkboxes pull against each
other: a second `MarkProcessed` must not error (bullet 5), but the
duplicate-delivery race must have unambiguous behavior (bullet 6) — and
at SQL level those are the same "row is already there" condition, so
one signal cannot be both an error and not an error. Returning
`recorded bool` splits them: a repeat gets `(false, nil)` — no error,
no duplicate row — and the loser of the race gets the same `false` as
its instruction to roll back. Reporting it as an error instead would
have meant sniffing driver-specific unique-violation codes in the core,
which ADR 0005 exists to avoid; making it silently idempotent would
have let both racers commit their business writes, which is the bug the
pattern exists to prevent.

The documented race behavior, now pinned by
`TestConcurrentDeliveriesSettleOnOneWinner`: both transactions read
`HasProcessed` as false (neither can see the other's staged row), both
do the work, and the second `MarkProcessed` **blocks** until the first
transaction ends and then reports `false`. The test asserts the block
too — it fails if the second call returns before the first commits — so
the waiting is contract, not incidental. Exactly one delivery commits;
the loser rolls back and the event is re-delivered, by which point
`HasProcessed` is true.

`HasProcessed` deliberately reports on committed work only, and the
`Dialect` doc comment makes non-blocking reads a requirement rather
than an accident of Postgres. Postgres gets this free under read
committed. **Note for ticket 09:** SQL Server does not — a plain
`SELECT` blocks on the exclusive lock of an uncommitted insert, which
would both fail
`TestHasProcessedReportsOnlyCommittedWork` (it asserts the read does
not block) and stall every consumer queued behind an in-flight
handler. `WITH (READPAST)` on the select is the fix, the same lock hint
ticket 07 used on the outbox claim. The insert-if-absent side needs
`INSERT ... WHERE NOT EXISTS (SELECT 1 ... WITH (UPDLOCK, HOLDLOCK))`
so the rows-affected count keeps meaning "this transaction won".

Test infrastructure is `inbox/internal/dbtest` with the `Backend` shape
ticket 07 converged on, already parameterized via `eachBackend` even
though only Postgres is wired. Ticket 09 adds one file — a
`sqlServerBackend()` — and inherits all five assertions rather than
restating them. Container cost is one per test as in the outbox, but
the whole suite runs in 15s: there is no relay loop to wait on, so the
CI-timeout worry ticket 07 flagged does not bite here. It will once
SQL Server joins.

Both dedup tests were verified by mutation: changing the dialect's `ON
CONFLICT (event_id) DO NOTHING` to `DO UPDATE SET processed_at =
now()` — so a duplicate insert reports a row written — fails
`TestMarkProcessedTwiceNeitherErrorsNorDuplicates` and
`TestConcurrentDeliveriesSettleOnOneWinner`. They are measuring the
conflict clause, not passing for free.

No logger or metrics options: unlike the relay there is no background
loop here, nothing is swallowed, and every outcome is already a return
value the caller branches on. Nothing to observe that the caller does
not already know.
