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

**Status:** done

- [x] A business write + `outbox.Enqueue` commit together in one
      `*sql.Tx`; the outbox relay picks it up and publishes it via the
      real `kafka.Producer` to a real broker (testcontainers).
- [x] A `Runner` consumes that message, its `Handler` checks
      `inbox.HasProcessed` first (skips if already seen), does its
      work, calls `inbox.MarkProcessed`, and only then the offset
      commits.
- [x] Simulating a duplicate delivery (re-publish the same `EventId`,
      or replay the same offset) proves the handler's business effect
      does not happen twice, via the inbox check.
- [x] Simulating a relay crash between publish and delete (e.g. kill
      the relay before the delete, restart it) proves the event is
      republished — and the inbox on the consumer side absorbs the
      resulting duplicate.
- [x] The whole scenario runs as one integration test suite
      (testcontainers: Postgres + Kafka) that exercises real
      components at every layer except the initiating business
      write/read, which can be a minimal test fixture table.

## Comments

**The ticket needed a fifth module, and that is its one structural
decision.** No existing module can host this test: `outbox` would have
to take on Kafka, `kafka` would have to take on two database drivers,
and either way a library module grows a dependency it has no other use
for — the decoupling ADR 0005 and 0008 bought would be spent on a test.
So `integration/` is a new module that depends on all four and exports
nothing; its `doc.go` is a package comment and no code. It is in
`go.work` and in both CI loops.

The test-infrastructure packages could not be reused, which is the cost
of that choice: `outbox/internal/dbtest` and `kafka/internal/kafkatest`
are `internal` to their own modules, so the container helpers are
written a third time in `env_test.go`. Making them importable would
mean promoting them out of `internal` and publishing test scaffolding as
API — worse than the ~80 duplicated lines.

Containers are started once for the package in `TestMain`, not per
test, which is the first departure from tickets 06–09. Every test here
needs both a database and a broker, and ticket 09's warning about
container startup dominating the run applies with force: per test it
would be eight containers instead of two. The whole suite is ~40s.
Tests share the one database and `reset(t)` truncates between them;
topics and consumer groups are per test.

**Four tests, and the fourth is the one the checkboxes do not obviously
ask for.** Checkbox 2's "and only then the offset commits" cannot be
observed from a passing happy path — the offset ends up committed either
way. `TestFailedHandlerCommitsNeitherInboxRowNorOffset` fails the
handler with its transaction still open, after both the receipt and the
inbox row have been written to it, and asserts all three are absent:
receipt, inbox row, committed offset. That ordering is only visible
stated as a negative.

The `beforeCommit` hook exists for that test alone. It is the one seam
in the handler; everything else about it is the handler a real service
would write.

Three mutations, one per mechanism, each confirmed to kill its test:

- Handler ignores the inbox (both `HasProcessed` and `MarkProcessed`
  results discarded) → both dedup tests fail with `receipts = 2`. The
  `receipts` table deliberately has no unique constraint, so a
  duplicated business effect shows up as a second row rather than a
  database error: the inbox is the only thing preventing it.
- `beforeCommit` moved after `tx.Commit` → the failure test fails with
  `receipts = 1, inbox rows = 1`, so it is measuring the rollback and
  not merely the error.
- `Runner.commit` made a no-op → the happy path fails on
  `assertCommittedPast`. Worth mutating: that assertion waits for
  silence, and silence is also what a broken rebalance looks like. Its
  converse `assertRedelivers` passing in the failure test, on the same
  infrastructure, is the other half of that proof.

**Two traps for anyone extending this suite.** First, the crash
simulation: `crashingProducer` publishes through the real producer and
then returns an error, so the relay leaves the row staged — the broker
has the message, the outbox does not know it. Cancelling the relay's
context was the first attempt and is worse: the delete then fails on a
cancelled `*sql.Tx`, reaching the same state less directly, and the
cancel has to reach the producer before the relay's first poll or the
test races itself.

Second, and it cost a segfault before it cost a fix: closing a consumer
while its `Runner` is still in `Consume` crashes inside librdkafka
rather than failing the test. Ticket 05's runner test documents this for
cleanup ordering; it bites here too because `assertCommittedPast` needs
the group free, so the runner must be stopped explicitly before the
consumer is closed. `runInBackground` returns an idempotent stop for
exactly that, and `newConsumer` a `sync.Once`-guarded close.

Runtime is ~40s with a 20s floor: `silenceWindow` is 20s because a
consumer joining a group waits out a rebalance first, and a shorter
window would make an idle consumer indistinguishable from a committed
offset. The happy-path test spends that 20s proving nothing arrives.
