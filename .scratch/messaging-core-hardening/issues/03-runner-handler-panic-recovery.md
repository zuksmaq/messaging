# 03 — Runner: recover Handler panics through the poison policy

**What to build:** a panic raised by the caller's `Handler` during
`Runner.Run` must be recovered, converted into an error (preserving
the recovered value and a stack trace for diagnostics), and routed
through the same `PoisonMessageAction` handling
(`Skip`/`DeadLetter`/`Halt`) that a returned `Handler` error already
goes through — so a panicking handler behaves identically to one
that returns an error, under whichever policy the caller configured.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] A `Handler` that panics on a given message no longer crashes
      the hosting process; `Runner.Run` continues running.
- [x] Under `PoisonAction: Skip`, a handler panic is logged, counted,
      and the offset is committed past it — matching the existing
      behavior for a returned handler error.
- [x] Under `PoisonAction: DeadLetter`, a handler panic results in
      the message being forwarded to the dead-letter topic —
      matching the existing behavior for a returned handler error.
- [x] Under `PoisonAction: Halt`, a handler panic stops `Run` without
      committing the offset, so the message is redelivered on
      restart — matching the existing behavior for a returned
      handler error.
- [x] A test triggers a real panic (not a returned error) inside a
      `Handler` for each of the three poison policies and asserts
      the corresponding existing behavior.

## Comments

`Runner.Run` now calls the handler through a new `callHandler` method
that defers a `recover()`, converting a panic into an error carrying
the recovered value (`kafka/consumer/runner.go`). That error flows
into the exact same `if err := ...; err != nil { poison(...) }` branch
a returned handler error already used — no separate panic-handling
path, so Skip/DeadLetter/Halt behave identically to the existing error
case by construction rather than by parallel implementation.

Code review flagged two refinements, both applied:
- The full `runtime/debug.Stack()` trace is logged immediately at
  panic-recovery time (`slog` `ErrorContext`) rather than folded into
  the error string — a multi-KB stack trace has no business riding
  inside a dead-letter header or a poison-message warning log.
- A panic value that is itself an `error` is wrapped with `%w` instead
  of `%v`, so `errors.Is`/`errors.As` still work through a panicked
  sentinel error (per ADR 0004). New subtest
  `TestRunHandlerPanic/panic_value_is_an_error,_preserved_through_errors.Is`
  locks this in.

Confirmed the crash was real before the fix: the
`TestRunHandlerPanic/skip` subtest reliably crashed the test process
with an unrecovered panic prior to the `callHandler` change.
`TestRunHandlerPanic` covers all three `PoisonMessageAction` values,
reusing the existing `fakeConsumer`/`fakeProducer`/`assertPoisonReported`
test infrastructure.

Verified with `GOWORK=off go build ./... && GOWORK=off go vet ./...`
and `GOWORK=off go test -race ./...` (all packages green, no races).
