# 07 — Outbox: document concurrent-relay ordering trade-off

**What to build:** state plainly, in `Relay`'s doc comment and in
`CONTEXT.md`, that per-key publish ordering is guaranteed only when
a single `Relay` instance runs against a given table — running
multiple relay instances concurrently against the same table is an
explicit throughput-for-ordering trade-off, not a bug. No behavior
change; this ticket is documentation plus a test that pins the
current (documented) behavior so a future change to it is
deliberate.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] `Relay`'s doc comment states the single-instance-for-ordering
      requirement explicitly.
- [x] `CONTEXT.md`'s `Outbox`/`Relay` glossary entry states the same
      trade-off.
- [x] A test pins that two concurrent relay instances against the
      same table can interleave same-key rows out of order today, so
      a future change to this behavior is a deliberate, visible test
      change rather than a silent regression either way.

## Comments

Landed in #25. The status field and checklist were not
updated when the work merged; this catches the ticket up to the code.
