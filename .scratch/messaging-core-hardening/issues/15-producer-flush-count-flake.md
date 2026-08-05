# 15 — Flaky unflushed-count assertion in producer shutdown tests

**What to build:** `kafka/producer/shutdown_test.go`'s
`TestCloseReportsUnflushedMessages`, `TestCloseTwiceIsSafeNoOp`, and
`TestCloseConcurrentIsSafeNoOp` intermittently fail with the
`messaging.producer.unflushed` counter one higher than the number of
messages queued (e.g. "want 5, got 6"). Reproduces locally with no
broker or network involved — every test points at `127.0.0.1:1`
(guaranteed connection-refused) — so it isn't CI-environment flakiness.
Root cause needs a real fix so these tests stop being flaky in CI.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] `TestCloseReportsUnflushedMessages`, `TestCloseTwiceIsSafeNoOp`,
      and `TestCloseConcurrentIsSafeNoOp` pass reliably across many
      repeated local runs (e.g. `go test -count=50`), not just most
      of the time.
- [x] The fix addresses the actual race (e.g. a bounded/deterministic
      retry policy for these tests, or counting delivery reports
      instead of trusting `Flush()`'s raw return value) rather than
      papering over it with a longer timeout.
- [x] No change in `Producer.Close`'s observable behavior against a
      real broker.

## Comments

The original "suspected mechanism" above (librdkafka retry/backoff
double-counting a message mid-retry) was wrong. The real cause is that
`Flush()`'s return value does not count messages at all.

`confluent-kafka-go`'s `Producer.Flush` returns `Producer.Len()`,
which is `len(produceChannel) + len(events) + rd_kafka_outq_len(rk)`,
and its own doc calls this "the number of outstanding *events* still
un-flushed". Broker error events share those queues: pointing a
producer at `127.0.0.1:1` generates a steady stream of
`Connect ... Connection refused` events, and `rd_kafka_outq_len` also
includes librdkafka's `rk_rep` reply queue. `Producer.Close` was
treating that return value as a message count, so whenever an error
event happened to be pending at the instant `Flush` sampled — racing
the `drainEvents` goroutine that consumes them — the residual came
back one too high.

Confirmed by direct instrumentation rather than inference: over 300
iterations replicating the flush step by hand, the inflated sample was
caught red-handed as `Flush()=6 (want 5) len(Events())=1` — the extra
unit is a queued error event, not a message.

This was a production bug, not just a test bug: the
`messaging.producer.unflushed` metric over-reported during a broker
outage, which is exactly when an operator would lean on it.

Fixed by tracking outstanding messages explicitly: `Producer` now
carries an `inFlight atomic.Int64`, incremented when `Produce` hands a
message to the client and decremented when it observes the delivery
report. A `Produce` that returns on a cancelled/expired context leaves
its message counted, which is correct — nothing will read the
acknowledgement off the abandoned per-call delivery channel.
`closeClient` still calls `Flush` to do the waiting, but reports
`inFlight` as the residual.

The three shutdown tests now drive the public `Produce` API (via new
`unreachableProducer`/`produceUnacknowledged` helpers) instead of
reaching past it to `p.client.Produce(msg, nil)`, so they exercise the
accounting they assert on. Verified with 60 consecutive
`go test -run TestClose -race` runs, all green; the same loop against
the previous code reproduced the failure within ~10 iterations.
