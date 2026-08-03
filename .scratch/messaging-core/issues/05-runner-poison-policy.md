# 05 — Hosted Runner + poison-message policy

**What to build:** `Runner[K, V]`, the hosted consumer loop: wraps a
`Consumer[K, V]` (ticket 03), drives a caller-supplied `Handler[K, V]`
per message, and applies the configured `PoisonMessageAction` (`Skip`
/ `DeadLetter` / `Halt`) when deserialization or the handler fails.
Exposed as a blocking `Run(ctx) error`. See ADR 0007.

**Blocked by:** 03 (kafka consumer).

**Status:** done

- [x] `Handler[K, V]` type: processes one `ReceivedMessage[K, V]`,
      returns an error to trigger the poison policy or nil to commit.
- [x] `Runner.Run(ctx)` blocks, consuming/handling/committing in a
      loop until `ctx` is cancelled or an unrecoverable error occurs.
- [x] Successful `Handler` call commits the offset; the offset never
      advances before the handler returns without error.
- [x] `Skip`: failure (deserialize or handler error) is logged and
      counted, offset commits past it, loop continues.
- [x] `DeadLetter`: the original message is forwarded to the
      configured dead-letter topic; the offset only commits once that
      publish is confirmed `Persisted`. A failed dead-letter publish
      leaves the offset uncommitted.
- [x] `Halt`: the loop stops without committing; a restart re-delivers
      the same message.
- [x] Every poison outcome is logged and counted regardless of policy
      — none is silent.
- [x] Integration test (testcontainers, real broker): a handler that
      errors on a specific message exercises all three policies, each
      asserting the offset/dead-letter-topic/logged-count outcome
      above.
- [x] Integration test: cancelling `ctx` stops `Run` cleanly (returns
      without leaking goroutines/connections).

## Comments

Built as `kafka/consumer.Runner[K, V]` (ADR 0008 put it beside the
`Consumer` it wraps), constructed with
`NewRunner(consumer, handler, RunnerConfig{...}, opts...)`. `Handler` is a
plain func type; the zero `RunnerConfig` selects `Halt`, so no message is
dropped until the caller asks for it.

`DeadLetter` needed a decision the ticket left open: a message that fails
to deserialize has no decoded key or value left to forward, so a
`Producer[K, V]` would publish an empty payload for exactly the case that
most needs the original preserved. `messaging.ReceivedMessage` therefore
gained `RawKey`/`RawValue` (the bytes as the broker delivered them, set by
the ticket-03 consumer at no cost), and the dead-letter producer is typed
`messaging.Producer[[]byte, []byte]`. Both failure kinds forward the
original bytes, plus the original headers and four
`dead-letter-*` annotation headers (error, source topic, partition,
offset). The offset commits only after the publish returns `Persisted` —
`PossiblyPersisted` is treated as a failure and stops the loop with the
offset uncommitted.

`Run` returns nil when `ctx` is cancelled; a cancelled context checked
before the poison policy means shutdown never looks like a poison message.
Counters are `messaging.runner.handled` and `messaging.runner.poisoned`
(attributed by topic and action).

Covered by `runner_test.go` (fake consumer/producer: policy outcomes,
commit ordering, dead-letter payload and failure modes, cancellation,
config validation) and `runner_integration_test.go` (real broker via
testcontainers: all three policies against the same seeded topic, plus
clean shutdown on cancellation). Both suites pass, as does the pre-existing
kafka integration suite after the `ReceivedMessage` change.
