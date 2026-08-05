# Messaging core hardening: lifecycle, poison handling, outbox/inbox fixes

Status: ready-for-agent

## Problem Statement

Services built on `github.com/zuksmaq/messaging` (the Kafka
`Producer[K, V]`/`Consumer[K, V]` contracts, the hosted `Runner`, and
the transactional `Outbox`/idempotent `Inbox`) hit several defects
under ordinary production conditions — not misuse, not exotic edge
cases, but the everyday shapes of Go service code: `defer
producer.Close(ctx)` next to an explicit `Close` on an error path, a
`Handler` that panics on a malformed payload, a rolling deploy that
triggers a consumer-group rebalance during shutdown, two replicas of a
service racing to provision their own SQL Server schema at startup, or
an outbox row that can never be published because the topic was
deleted.

Each of these currently either crashes the whole process, hangs
forever, silently loses or duplicates a message, or produces a wrong
result — despite the library's stated goals (ADR 0004's no-silent
failures stance, ADR 0007's "every poison outcome is logged and
counted, never silent" guarantee for the `Runner`, and the general
at-least-once-with-dedup contract in `CONTEXT.md`). A service team
adopting this library today has no way to know these gaps exist short
of reading `librdkafka`/`confluent-kafka-go` internals or hitting them
in production.

## Solution

A hardening pass across the `kafka` (producer + consumer + `Runner`),
`outbox`, `inbox`, and root modules that closes each of the defects
below, without changing the library's existing architecture or public
API shapes beyond the minimum needed (a small number of additive
fields/methods, called out explicitly in Implementation Decisions).
Every fix keeps the library's existing conventions: sentinel errors
wrapped with `%w` (ADR 0004), `*sql.Tx`-scoped outbox/inbox operations
(ADR 0005), `log/slog` + OpenTelemetry observability for anything that
was previously silent (ADR 0006), and the `Run(ctx) error` /
poison-policy shape already established for the `Runner` (ADR 0007) —
extended, where it fits, to the `Outbox.Relay`.

These findings came out of an adversarially-verified production-bug
hunt (multiple independent finders per risk surface, each candidate
finding re-checked by a separate agent instructed to try to refute it;
several were additionally proven by executing real code against the
`confluent-kafka-go` client). Three raw candidates were refuted as
exact duplicates of another confirmed finding and are folded into the
single corresponding user story below; none of the 21 stories below
represents a refuted or unconfirmed claim.

## User Stories

1. As a service author who calls `producer.Close(ctx)` from both a
   `defer` and an explicit error-path cleanup, I want a second `Close`
   call to be a safe no-op, so that an ordinary shutdown idiom cannot
   crash or hang the process via a use-after-free on the underlying
   Kafka client handle.
2. As a service author shutting down under a deadline, I want
   `Producer.Close`/`Producer.ReadyCheck` to notice an
   already-cancelled context immediately (not just a context with a
   passed `Deadline`), so that shutdown doesn't block for the full
   configured timeout regardless of cancellation.
3. As a service author consuming Kafka via `Consumer.Consume`, I want
   a partition/fetch-level broker error on a polled record to be
   surfaced as an error (and never committed as if it were a real
   message), so that a broker-side hiccup can't be silently treated as
   valid message data.
4. As a service operator calling `consumer.Close()` concurrently with
   a running `Run`/`Consume` loop during shutdown, I want `Close` to
   wait for any in-flight poll to finish before tearing down the
   client, so that the documented segfault risk of closing a consumer
   mid-poll is eliminated.
5. As a service operator running a hosted `Runner`, I want a panic
   inside my `Handler` to be recovered and routed through the
   configured `PoisonMessageAction` (`Skip`/`DeadLetter`/`Halt`) like
   any other handler error, so that one bad message can never take the
   whole process down regardless of the poison policy I configured.
6. As a platform engineer running two `Outbox.Relay` instances
   against the same table for throughput, I want it clearly documented
   that per-key publish ordering is only guaranteed with a single
   relay instance, so that I don't assume an ordering guarantee the
   concurrent-relay deployment mode doesn't actually provide.
7. As a service author using the SQL Server inbox dialect, I want
   `MarkProcessed` calls for *different* event ids to never block each
   other just because they land in the same empty key-range gap, so
   that idempotency-check throughput doesn't collapse under
   monotonically-increasing event ids (ULIDs, Snowflake ids) on a
   fresh or sparse table.
8. As an SRE operating an `Outbox.Relay` in production, I want a row
   that can never be published (oversized payload, missing topic,
   revoked ACL) to be quarantined after a bounded number of attempts
   instead of blocking every other row behind it forever, so that one
   permanently-poisoned row can't halt outbox delivery for every topic
   and key in the table.
9. As a service author calling `Consumer.Commit(ctx, ...)` with a
   deadline on `ctx`, I want that deadline to actually bound the
   underlying broker commit call, so that a slow or unreachable group
   coordinator can't make `Commit` block indefinitely regardless of
   the context I passed in.
10. As a service operator relying on a `Runner`'s `DeadLetter` policy,
    I want the accepted at-least-once gap between "DLQ publish
    confirmed" and "source offset committed" to be documented plainly,
    so that I know to make DLQ consumers dedupe on `EventId`/the
    dead-letter source headers rather than assuming exactly-once
    forwarding.
11. As an SRE operating an `Outbox.Relay`, I want claimed-but-abandoned
    rows (relay killed mid-batch, dropped connection invisible to a
    connection pooler) to be reclaimed within a bounded, documented
    time, so that a relay crash doesn't leave rows silently stuck
    until something else happens to notice.
12. As a platform engineer provisioning the SQL Server inbox schema
    from multiple service replicas at startup, I want concurrent
    `CreateTableSQL` runs against a brand-new database to not fail
    outright, so that ordinary multi-replica startup doesn't require
    external coordination just to provision one table.
13. As a service author composing my own idempotency key for the
    inbox (not just the outbox-stamped row id), I want an event id or
    topic name that exceeds the SQL Server dialect's column limit to
    be rejected with a clear, documented error identical across both
    dialects, so that I don't get a confusing SQL truncation/collision
    failure that repeats identically on every retry.
14. As a service author using the outbox with the SQL Server dialect,
    I want an over-limit topic or event id to be rejected by
    `Enqueue` itself (before the caller's business transaction
    commits), so that a value the Postgres dialect would silently
    accept doesn't roll back my own unrelated business write on SQL
    Server.
15. As a service author consuming Kafka records with duplicate header
    keys (e.g. two `trace-id` headers stamped by different hops), I
    want a way to see every value, not just the last one, so that a
    duplicate header key doesn't silently and irrecoverably discard
    data a downstream system needs.
16. As a service author consuming a log-compacted topic with a
    non-`[]byte` value type (e.g. `Consumer[string, any]`), I want
    `ReceivedMessage.Tombstone()` to correctly report a compaction
    tombstone regardless of the decoded value's type, so that
    tombstone-handling logic doesn't silently fail to fire for any
    value type other than raw bytes.
17. As a service author staging outbox events, I want `Enqueue` to
    reject a caller-supplied header that collides with the reserved
    `EventId` header name, so that I can't accidentally overwrite the
    outbox's own idempotency key with a same-named header I chose
    independently.
18. As a service author who constructs an `Outbox`/`Relay` or
    `SchemaRegistry` as a zero-value struct (e.g. via a DI container
    or config map, bypassing the documented constructor), I want a
    clear configuration error instead of a nil-pointer panic, so that
    a wiring mistake fails safely and diagnosably.
19. As a service operator replaying a previously dead-lettered message
    back through the same `Runner` for a retry, I want the dead-letter
    headers from the first pass to be preserved (not silently
    overwritten) if the replay is itself dead-lettered again, so that
    I can see the full dead-letter history instead of only the most
    recent hop.
20. As a service operator debugging a consumer shutdown, I want
    `Consumer.Close()` to report both the client-close error and the
    schema-registry-close error when both occur, so that a
    registry-side shutdown problem isn't silently swallowed just
    because the client also failed to close.

## Implementation Decisions

- **Producer.Close idempotency**: guard the exported `Close` method
  with a `sync.Once` (or equivalent atomic-flag pattern) so that any
  call after the first is a safe no-op returning the first call's
  result, instead of re-invoking flush/close against an
  already-destroyed client handle.
- **Producer.Close / ReadyCheck cancellation**: check `ctx.Err()`
  up front, not only `ctx.Deadline()`, when computing the remaining
  timeout for a flush/metadata call; an already-cancelled context
  should behave as an immediate timeout.
- **Consumer decode error handling**: the per-record decode path must
  check the broker-reported per-record error before touching the
  record's key/value, and surface it through the same sentinel-error
  path (ADR 0004) as other consume-time errors, so it flows through
  `Runner`'s poison-message handling like any other consume failure.
- **Consumer/Runner shutdown synchronization**: introduce internal
  synchronization (e.g. a `sync.RWMutex`) so that `Close` cannot run
  concurrently with an in-flight `Consume`/poll call — `Close` should
  wait for the in-flight call to return before tearing down the
  client.
- **Handler panic recovery**: `Runner.Run` must recover a panic raised
  by the caller's `Handler` (and, if feasible, by a caller-supplied
  `Consumer`/deserializer), convert it into an error (preserving the
  recovered value and a stack trace for diagnostics), and feed it
  through the existing `PoisonMessageAction` path exactly as a
  returned `Handler` error would.
- **Concurrent-relay ordering**: document, on `Relay` and in
  `CONTEXT.md`, that per-key publish ordering is guaranteed only when
  a single `Relay` instance runs against a table; running multiple
  instances concurrently is an explicit throughput/ordering trade-off.
  Making the row-claim key-aware (so concurrent relays partition by
  key rather than racing on row id) is out of scope for this pass —
  see Out of Scope.
- **Outbox poison-row policy**: give `Relay` a bounded-attempts policy
  for a row that repeatedly fails to publish — after a configurable
  attempt threshold, the row is marked quarantined (logged and
  counted, mirroring the "never silent" posture ADR 0007 established
  for `Runner`) and the relay continues past it rather than blocking
  every subsequent row. The exact quarantine storage shape (a status
  column vs. a separate table) is an implementation detail for the
  ticket stage, not fixed here.
- **Commit context handling**: `Consumer.Commit` must derive a bounded
  timeout from its `ctx` argument (clamped the same way `ReadyCheck`
  already clamps its timeout against a context deadline) and pass it
  to the underlying commit call, returning `ctx.Err()` once the bound
  is reached.
- **DeadLetter/commit ordering**: document explicitly, next to
  `PoisonMessageAction`'s existing documentation, that a commit
  failure occurring after a confirmed dead-letter publish can result
  in the dead-lettered message being duplicated on redelivery — this
  is an accepted at-least-once trade-off, not a defect to eliminate,
  and DLQ consumers are expected to dedupe (the same expectation
  already placed on regular `Handler`s per `CONTEXT.md`).
- **Outbox claim lease/timeout**: document and, where the driver
  supports it, configure an aggressive statement/session timeout (and
  TCP keepalive) on the database connections `Relay` uses for
  claiming, so a relay that dies mid-batch has its claimed-but-
  abandoned rows released by the database within a bounded, documented
  window instead of relying on incidental connection-drop detection.
- **Idempotent schema provisioning (SQL Server)**: make the inbox's
  `CreateTableSQL` safe under concurrent execution (e.g. an
  application lock around the check-and-create, or an explicit
  "already exists" tolerance on the relevant SQL Server error code),
  so multiple replicas provisioning schema at startup don't fail each
  other.
- **Shared topic/event-id length limit**: pick one maximum length
  (matching the SQL Server dialect's column limit) for topic name and
  event id, enforced identically by both dialects at the
  dialect-agnostic core (`Outbox.Enqueue`, `Inbox.HasProcessed`/
  `MarkProcessed`) — validated before any database write is attempted,
  so an over-limit value fails the same documented way on both
  dialects instead of behaving differently per database and, for the
  outbox, rolling back the caller's own business transaction.
- **Header multiplicity**: add a new field on `ReceivedMessage`
  carrying the original, order-preserving, duplicate-preserving header
  list (alongside the existing convenience map, which remains
  last-wins for backward compatibility) so a caller that needs every
  value for a repeated header key has a documented way to get it.
- **Tombstone detection**: `ReceivedMessage.Tombstone()` must key off
  the raw broker-delivered value (already carried for exactly this
  kind of case) rather than reflecting on the decoded value, so
  tombstone detection is correct independent of the value's generic
  type.
- **Reserved header validation**: `Outbox.Enqueue` must reject a
  caller-supplied header keyed with the reserved `EventId` header
  name, consistent with `Enqueue`'s existing input validation.
- **Zero-value safety**: `Outbox`/`Relay` and `SchemaRegistry` must
  defensively check for an unconstructed (zero-value) receiver at the
  top of their exported entry points and return a documented
  configuration-error sentinel instead of panicking — matching the
  guard pattern the constructors already apply.
- **Dead-letter header preservation**: reserve the `Runner`'s four
  dead-letter header names the same way `EventId`'s header name is
  reserved, and preserve a colliding pre-existing value (e.g. under a
  distinct/nested key) instead of silently overwriting it when a
  message is dead-lettered more than once.
- **Consumer.Close error reporting**: combine the underlying client's
  close error and the schema-registry's close error (e.g. via
  `errors.Join`) instead of returning only whichever the current
  branch happens to pick.

## Testing Decisions

- Tests exercise only externally observable behavior through each
  module's existing public API (`Producer`, `Consumer`, `Runner`,
  `Outbox`, `Relay`, `Inbox`, `ReceivedMessage`) — never unexported
  internals — matching this repo's existing test style.
- **`kafka/producer`**: extend the existing pattern of pointing a real
  `Producer` at an unreachable broker address to exercise
  close/shutdown/timeout behavior without needing a live Kafka broker
  (prior art: the existing shutdown-focused producer test in that
  package already uses this technique for the "N message(s)
  un-acknowledged" timeout path).
- **`kafka/consumer`**: extend the existing consumer- and
  runner-level test files, including the runner integration test used
  for poison-policy behavior, to cover decode-error handling,
  close/poll synchronization, handler-panic recovery, commit
  context-bounding, header multiplicity, and dead-letter header
  preservation.
- **`outbox`**: extend the existing fake-producer-backed relay test
  (no real database needed) for the poison-row policy, claim
  lease/timeout behavior, and the reserved-header validation; extend
  the existing cross-dialect integration test (which already runs
  every test against both Postgres and SQL Server through a shared
  backend harness) for the topic/event-id length-limit behavior, since
  that defect is dialect-specific by nature.
- **`inbox`**: extend the existing cross-dialect integration test
  (same shared-backend harness pattern as outbox) for the SQL Server
  lock-contention and schema-provisioning races, since both require a
  real SQL Server instance to observe; extend the existing dedup unit
  test for the shared length-limit validation, which doesn't need a
  real database.
- **root module**: this module currently has no test file of its own.
  Add one new, minimal test file at the module root exercising
  `ReceivedMessage.Tombstone()` directly across multiple value types
  (raw bytes, a decoded struct, a decoded `any`) — pure Go, no broker
  or database required. This is the only new test seam introduced by
  this hardening pass; every other fix is tested through an existing
  seam.
- A good test here pins the *documented* behavior (e.g. "a second
  `Close` call returns nil and does not panic/hang," "a handler panic
  results in the configured `PoisonMessageAction` being applied," "an
  event id over the shared length limit is rejected by `Enqueue` on
  both dialects") — not the specific mechanism used internally to get
  there.

## Out of Scope

- Key-aware row claiming so concurrent `Relay` instances preserve
  per-key ordering — the accepted fix for this pass is documentation
  of the existing single-writer-for-ordering trade-off, not a claim
  algorithm change.
- A general dead-letter/quarantine subsystem for the outbox beyond a
  minimal bounded-attempts marker — no new admin tooling, no
  retry-backoff scheduling beyond what's needed to stop a poisoned row
  from blocking others.
- Eliminating the DeadLetter/commit at-least-once duplication window
  entirely (e.g. via a two-phase or transactional publish-and-commit)
  — documenting it as an accepted trade-off is the decision for this
  pass.
- Any change to the wire formats in scope (Bytes/String/JSON/Avro per
  ADR 0002) or to consumer-group rebalance strategy tuning.
- Any breaking change to an existing exported method signature or
  removal of an existing exported field — all fixes here are additive
  (new fields/methods) or internal-behavior-only changes.
- A process supervisor/errgroup helper for wiring multiple `Run(ctx)`
  loops together (already excluded by ADR 0007).
- Any new SQL Server-specific feature beyond correctness fixes to the
  existing outbox/inbox batch-claim/dedup SQL (per the original
  spec's scope boundary, unchanged here).

## Further Notes

- This is a hardening pass on top of the already-implemented
  `messaging-core` spec (tickets 01–10, all done) — it does not
  revisit any decision from that spec except where a listed defect
  requires it.
- As with the original spec, this is expected to be split into one
  implementation ticket per fix (or small cluster of related fixes)
  under `.scratch/messaging-core-hardening/issues/` at the next stage,
  following this repo's numbered-issue-file convention.
- Every defect listed here was independently re-verified by an agent
  specifically instructed to try to refute it (a second, independent
  refuter for anything rated high-severity); several were additionally
  proven by executing real code against the actual
  `confluent-kafka-go` client rather than reasoned about from source
  alone. Candidates that didn't survive that verification are not
  included above.
- The shared topic/event-id length limit (Implementation Decisions)
  and the outbox poison-row quarantine shape are the two decisions
  most likely to need a follow-up conversation with whoever picks up
  the corresponding ticket, since both involve a concrete numeric/
  schema choice this spec deliberately leaves to the ticket stage.
