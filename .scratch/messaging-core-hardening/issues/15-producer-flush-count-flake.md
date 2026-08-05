# 15 — Flaky unflushed-count assertion in producer shutdown tests

**What to build:** `kafka/producer/shutdown_test.go`'s
`TestCloseReportsUnflushedMessages`, `TestCloseTwiceIsSafeNoOp`, and
`TestCloseConcurrentIsSafeNoOp` intermittently fail with the
`messaging.producer.unflushed` counter one higher than the number of
messages queued (e.g. "want 5, got 6"). Reproduces locally with no
broker or network involved — every test points at `127.0.0.1:1`
(guaranteed connection-refused) — so it isn't CI-environment flakiness.
Root cause needs a real fix so these tests stop being flaky in CI.

**Suspected mechanism:** idempotence is always on
(`producer.go` `clientConfig`), and neither
`message.send.max.retries` nor `retry.backoff.ms` is set, so
librdkafka retries indefinitely with a ~100ms backoff — longer than
these tests' 0–50ms `FlushTimeout`. `Producer.Close` trusts
`client.Flush()`'s returned count directly; a message mid-retry
appears to occasionally get double-counted in librdkafka's internal
outstanding-queue bookkeeping when `Flush()` samples it during that
transition.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] `TestCloseReportsUnflushedMessages`, `TestCloseTwiceIsSafeNoOp`,
      and `TestCloseConcurrentIsSafeNoOp` pass reliably across many
      repeated local runs (e.g. `go test -count=50`), not just most
      of the time.
- [ ] The fix addresses the actual race (e.g. a bounded/deterministic
      retry policy for these tests, or counting delivery reports
      instead of trusting `Flush()`'s raw return value) rather than
      papering over it with a longer timeout.
- [ ] No change in `Producer.Close`'s observable behavior against a
      real broker.
