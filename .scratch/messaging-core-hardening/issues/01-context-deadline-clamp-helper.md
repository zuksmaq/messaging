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

**Status:** ready-for-agent

- [ ] One helper computes a clamped timeout from a `context.Context`
      and a base timeout, treating an already-done context as a zero
      timeout.
- [ ] `Producer.Close`, `Producer.ReadyCheck`, and
      `Consumer.ReadyCheck` all call the shared helper instead of
      their own inline clamping logic.
- [ ] All existing tests for these three methods continue to pass
      unmodified — this ticket changes no observable behavior.
