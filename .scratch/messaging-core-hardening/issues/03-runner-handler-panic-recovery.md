# 03 — Runner: recover Handler panics through the poison policy

**What to build:** a panic raised by the caller's `Handler` during
`Runner.Run` must be recovered, converted into an error (preserving
the recovered value and a stack trace for diagnostics), and routed
through the same `PoisonMessageAction` handling
(`Skip`/`DeadLetter`/`Halt`) that a returned `Handler` error already
goes through — so a panicking handler behaves identically to one
that returns an error, under whichever policy the caller configured.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] A `Handler` that panics on a given message no longer crashes
      the hosting process; `Runner.Run` continues running.
- [ ] Under `PoisonAction: Skip`, a handler panic is logged, counted,
      and the offset is committed past it — matching the existing
      behavior for a returned handler error.
- [ ] Under `PoisonAction: DeadLetter`, a handler panic results in
      the message being forwarded to the dead-letter topic —
      matching the existing behavior for a returned handler error.
- [ ] Under `PoisonAction: Halt`, a handler panic stops `Run` without
      committing the offset, so the message is redelivered on
      restart — matching the existing behavior for a returned
      handler error.
- [ ] A test triggers a real panic (not a returned error) inside a
      `Handler` for each of the three poison policies and asserts
      the corresponding existing behavior.
