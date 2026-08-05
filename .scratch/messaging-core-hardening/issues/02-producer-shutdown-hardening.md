# 02 — Producer shutdown hardening: idempotent Close + cancellation

**What to build:** calling `Producer.Close(ctx)` a second time must
be a safe no-op instead of re-entering the underlying Kafka client's
flush/close against an already-destroyed handle. Separately,
`Producer.Close` and `Producer.ReadyCheck` must notice an already-
cancelled context immediately — using the shared clamp helper from
ticket 01 — rather than waiting out the full configured timeout
regardless of cancellation.

**Blocked by:** 01 — Shared context-deadline timeout clamp helper.

**Status:** ready-for-agent

- [ ] A second call to `Producer.Close` (concurrent or sequential)
      returns without panicking, hanging, or crashing the process,
      and does not re-invoke the underlying client's flush/close.
- [ ] `Producer.Close` and `Producer.ReadyCheck` called with an
      already-cancelled context return promptly (bounded by the
      shared clamp helper) instead of blocking for the full
      configured timeout.
- [ ] A test calls `Close` twice on the same `Producer` (pointed at
      an unreachable broker, matching this package's existing
      shutdown-test technique) and asserts no hang, panic, or crash.
- [ ] A test calls `Close`/`ReadyCheck` with a pre-cancelled context
      and asserts it returns near-immediately.
