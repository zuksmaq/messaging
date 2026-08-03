# 7. Keep both consumer layers; poison-message policy; loops are Run(ctx)

## Status

Accepted

## Context

Consuming services need both a low-level manual consume-then-commit
loop (for tests/fine control) and a hosted layer that drives
per-message handling automatically. They also need a well-defined
policy for what happens when a message can't be deserialized or a
handler errors — a poison message must never be silently skipped. Go
has no generic-host/DI framework to hang a background-service type off
of.

## Decision

- Keep both consumer layers: `Consumer[K, V]` (manual
  `Consume(ctx)`/`Commit(msg)`) and `Runner[K, V]` (wraps a `Consumer`,
  takes a `Handler[K, V]`, runs the poison-policy-aware
  consume-handle-commit loop). Collapsing to manual-only was
  considered and rejected — it would push every consuming service to
  reimplement the same loop and poison-policy logic.
- `PoisonMessageAction` has three values: `Skip` (log, count, commit
  past it), `DeadLetter` (forward to a dead-letter topic, only commit
  once that publish is confirmed), `Halt` (stop without committing;
  message is re-delivered on restart). Every case is logged and
  counted — never silent.
- Both the `Runner` and the outbox `Relay` are exposed as a blocking
  `Run(ctx) error` function, not a framework-managed service type. The
  caller starts it with `go r.Run(ctx)` or awaits it directly; no
  supervisor/errgroup helper is bundled in this module.

## Consequences

- `Handler[K, V]` is a plain function/interface type the caller
  registers with a `Runner`; must be idempotent (at-least-once
  delivery means a handler can see the same message twice).
- No hosting-framework dependency is introduced.
