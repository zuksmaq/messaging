# 01 — Shared context-deadline timeout clamp helper

**What to build:** a single shared helper (in the `kafka` package)
that clamps a caller-supplied timeout to whatever remains on a
`context.Context`'s deadline — including treating an already-
cancelled or already-expired context as an immediate (zero) timeout.
Replace the three existing, independently-duplicated copies of this
logic (`Producer.Close`, `Producer.ReadyCheck`, `Consumer.
ReadyCheck`) with calls to the shared helper. This is a behavior-
preserving refactor: no caller-visible behavior changes as a result
of this ticket alone.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] One helper computes a clamped timeout from a `context.Context`
      and a base timeout, treating an already-done context as a zero
      timeout.
- [x] `Producer.Close`, `Producer.ReadyCheck`, and
      `Consumer.ReadyCheck` all call the shared helper instead of
      their own inline clamping logic.
- [x] All existing tests for these three methods continue to pass
      unmodified — this ticket changes no observable behavior.

## Comments

Implemented as `kafka.ClampTimeout(ctx, base)` in `kafka/timeout.go`,
unit-tested in `kafka/timeout_test.go` (no deadline, deadline shorter/
longer than base, already-cancelled context, already-expired
deadline). `Producer.Close`, `Producer.ReadyCheck`, and
`Consumer.ReadyCheck` now call it in place of their inline clamping.
Verified with `GOWORK=off go build ./... && GOWORK=off go vet ./...`
and `GOWORK=off go test ./...` (unit) plus `-tags=integration` runs of
`TestCloseReportsUnflushedMessages`, `TestReadyCheckAgainstRealBroker`,
and `TestConsumerReadyCheck` against real broker containers — all
green, unmodified.
