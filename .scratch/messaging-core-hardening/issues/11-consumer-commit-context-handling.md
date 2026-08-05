# 11 — Consumer.Commit: honor context deadline/cancellation

**What to build:** `Consumer.Commit` must derive a bounded timeout
from its `ctx` argument — using the shared clamp helper from ticket
01, the same way `ReadyCheck` already does — and pass it to the
underlying broker commit call, returning `ctx.Err()` once the bound
is reached, instead of ignoring `ctx` and blocking on the broker
indefinitely.

**Blocked by:** 01 — Shared context-deadline timeout clamp helper.

**Status:** ready-for-agent

- [ ] `Commit` bounds the underlying commit call to whatever remains
      on `ctx`'s deadline, using the shared clamp helper.
- [ ] `Commit` called with an already-cancelled or expired context
      returns `ctx.Err()` promptly instead of attempting the broker
      call.
- [ ] A test exercises `Commit` with a pre-cancelled/expired context
      and asserts a prompt return.
