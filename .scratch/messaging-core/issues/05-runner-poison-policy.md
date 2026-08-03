# 05 — Hosted Runner + poison-message policy

**What to build:** `Runner[K, V]`, the hosted consumer loop: wraps a
`Consumer[K, V]` (ticket 03), drives a caller-supplied `Handler[K, V]`
per message, and applies the configured `PoisonMessageAction` (`Skip`
/ `DeadLetter` / `Halt`) when deserialization or the handler fails.
Exposed as a blocking `Run(ctx) error`. See ADR 0007.

**Blocked by:** 03 (kafka consumer).

**Status:** ready-for-agent

- [ ] `Handler[K, V]` type: processes one `ReceivedMessage[K, V]`,
      returns an error to trigger the poison policy or nil to commit.
- [ ] `Runner.Run(ctx)` blocks, consuming/handling/committing in a
      loop until `ctx` is cancelled or an unrecoverable error occurs.
- [ ] Successful `Handler` call commits the offset; the offset never
      advances before the handler returns without error.
- [ ] `Skip`: failure (deserialize or handler error) is logged and
      counted, offset commits past it, loop continues.
- [ ] `DeadLetter`: the original message is forwarded to the
      configured dead-letter topic; the offset only commits once that
      publish is confirmed `Persisted`. A failed dead-letter publish
      leaves the offset uncommitted.
- [ ] `Halt`: the loop stops without committing; a restart re-delivers
      the same message.
- [ ] Every poison outcome is logged and counted regardless of policy
      — none is silent.
- [ ] Integration test (testcontainers, real broker): a handler that
      errors on a specific message exercises all three policies, each
      asserting the offset/dead-letter-topic/logged-count outcome
      above.
- [ ] Integration test: cancelling `ctx` stops `Run` cleanly (returns
      without leaking goroutines/connections).
