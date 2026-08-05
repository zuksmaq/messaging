# 13 — Runner: DeadLetter/commit trade-off doc + header preservation

**What to build:** two related `Runner`/`DeadLetter` fixes. First,
document explicitly — next to `PoisonMessageAction`'s existing
documentation — that a commit failure occurring after a confirmed
dead-letter publish can duplicate the dead-lettered message on
redelivery, and that DLQ consumers are expected to dedupe on
`EventId`/the dead-letter source headers, the same expectation
already placed on regular `Handler`s. Second, when a message that
already carries dead-letter headers (from a prior dead-letter pass)
is dead-lettered again, the prior pass's header values must be
preserved rather than silently overwritten.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] `PoisonMessageAction`'s documentation (doc comment and/or
      `CONTEXT.md`) states the DeadLetter/commit duplication
      trade-off and the dedupe expectation on DLQ consumers
      explicitly.
- [x] Dead-lettering a message that already carries dead-letter
      headers from a prior pass preserves the prior pass's values
      (e.g. under a distinct/nested key) instead of overwriting
      them.
- [x] A test dead-letters a message twice (simulating a DLQ replay)
      and asserts both passes' dead-letter information is present
      afterward.

## Comments

Landed in #26. The status field and checklist were not
updated when the work merged; this catches the ticket up to the code.
