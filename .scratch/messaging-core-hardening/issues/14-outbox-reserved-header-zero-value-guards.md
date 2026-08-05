# 14 — Outbox: reserved header validation + zero-value guards

**What to build:** two related outbox/kafka safety fixes. First,
`Outbox.Enqueue` must reject a caller-supplied header keyed with the
reserved `EventId` header name, consistent with `Enqueue`'s existing
input validation, so a caller can't accidentally overwrite the
outbox's own idempotency key. Second, a zero-value (unconstructed)
`Outbox`, `Relay`, or `SchemaRegistry` — e.g. built by a DI
container bypassing the documented constructor — must return a
documented configuration error from its exported entry points
instead of panicking with a nil-pointer/interface dereference.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] `Enqueue` called with a caller-supplied header keyed `EventId`
      returns a clear, documented error instead of silently allowing
      the value to be overwritten later.
- [ ] A zero-value `Outbox`/`Relay`'s exported entry points (`Run`,
      `Enqueue`) return a documented configuration error instead of
      panicking.
- [ ] A zero-value `SchemaRegistry`'s `Close` returns a documented
      configuration error (or a safe no-op) instead of panicking.
- [ ] Tests cover: `Enqueue` rejecting a reserved header; a
      zero-value `Outbox`/`Relay` failing safely; a zero-value
      `SchemaRegistry` failing safely.
