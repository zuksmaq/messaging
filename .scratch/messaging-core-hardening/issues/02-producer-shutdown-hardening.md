# 02 — Producer shutdown hardening: idempotent Close + cancellation

**What to build:** calling `Producer.Close(ctx)` a second time must
be a safe no-op instead of re-entering the underlying Kafka client's
flush/close against an already-destroyed handle. Separately,
`Producer.Close` and `Producer.ReadyCheck` must notice an already-
cancelled context immediately — using the shared clamp helper from
ticket 01 — rather than waiting out the full configured timeout
regardless of cancellation.

**Blocked by:** 01 — Shared context-deadline timeout clamp helper.

**Status:** done

- [x] A second call to `Producer.Close` (concurrent or sequential)
      returns without panicking, hanging, or crashing the process,
      and does not re-invoke the underlying client's flush/close.
- [x] `Producer.Close` and `Producer.ReadyCheck` called with an
      already-cancelled context return promptly (bounded by the
      shared clamp helper) instead of blocking for the full
      configured timeout.
- [x] A test calls `Close` twice on the same `Producer` (pointed at
      an unreachable broker, matching this package's existing
      shutdown-test technique) and asserts no hang, panic, or crash.
- [x] A test calls `Close`/`ReadyCheck` with a pre-cancelled context
      and asserts it returns near-immediately.

## Comments

`Producer.Close` now guards the actual flush/close body (renamed
`closeClient`) behind a `sync.Once`, caching the first call's error in
`closeErr` so every subsequent call returns the same result without
touching the underlying client again. Confirmed this was a real bug,
not a theoretical one: before the fix, a second `Close` call on a
producer with queued-but-unacknowledged messages reliably segfaulted
inside `confluent-kafka-go`'s `Flush` (cgo call into a destroyed
librdkafka handle) — `TestCloseTwiceIsSafeNoOp` reproduces this by
queuing messages, closing twice, and asserting the
`messaging.producer.unflushed` counter isn't double-counted (which
would indicate a re-flush).

`Producer.ReadyCheck`'s already-cancelled-context handling turned out
to already be correct after ticket 01 — `ClampTimeout` returns 0 for a
done context and `ReadyCheck` already treated a non-positive timeout
as an immediate error. `TestReadyCheckWithCancelledContextReturnsPromptly`
locks this in. `TestCloseWithCancelledContextReturnsPromptly` covers
the same for `Close`.

Verified with `GOWORK=off go build ./... && GOWORK=off go vet ./...`,
`GOWORK=off go test ./...` (unit, all green), and
`-tags=integration go test ./producer/... -run TestReadyCheckAgainstRealBroker`
against a real broker container.
