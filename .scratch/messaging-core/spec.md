# Messaging core: Kafka producer/consumer + transactional outbox + idempotent inbox

Status: ready-for-agent

## Problem Statement

Go services in this organization need to publish and consume Kafka
events reliably, without every service reimplementing the same
plumbing: broker-agnostic produce/consume contracts, safe wire
formats (including Schema-Registry-backed Avro), a poison-message
policy so bad messages are never silently dropped, and the
transactional-outbox / idempotent-consumer (inbox) patterns needed to
avoid dual-write bugs between a service's own database and Kafka.

Today there is no shared Go library for this — each service would
either hand-roll it or (for teams coming from the .NET side of the
organization) have no equivalent of the existing `Tfg.Crm.Messaging.*`
package family to reach for.

## Solution

A multi-module Go library (`github.com/zuksmaq/messaging` and its
`kafka`, `outbox`, `inbox` submodules) providing:

- Generic, broker-agnostic `Producer[K, V]` / `Consumer[K, V]`
  contracts in the root module, built on `confluent-kafka-go`.
- A hosted `Runner[K, V]` that drives a caller-supplied `Handler[K, V]`
  per message with a configurable poison-message policy (`Skip` /
  `DeadLetter` / `Halt`).
- Bytes / String / JSON / Avro (Schema Registry) wire formats.
- A transactional `Outbox` (stage-then-relay) and idempotent `Inbox`
  (dedup-on-EventId), both built on `database/sql` so they work
  against either Postgres or SQL Server, with dialect-specific
  submodules for the parts that must differ (batch-claim locking SQL).

See `CONTEXT.md` for the full glossary and `docs/adr/0001`–`0007` for
the individual design decisions this spec builds on.

## User Stories

1. As a service developer, I want to construct a Kafka producer from a
   config struct (`New(cfg, opts...)`), so that I don't need a fluent
   builder to get a working producer.
2. As a service developer, I want `Produce` to await the broker
   acknowledgement and return a `DeliveryStatus`, so that I know
   whether a message was durably persisted before I act on that fact
   (e.g. before deleting an outbox row).
3. As a service developer, I want a producer that is idempotent by
   default (no duplicate/reordered messages on retry within a
   partition), so that I get that safety without extra configuration.
4. As a service developer, I want to choose the key and value wire
   format independently (Bytes / String / JSON / Avro), so that I can
   match what a given topic actually expects.
5. As a service developer, I want Avro serialization backed by Schema
   Registry, so that I can publish schema-checked events without
   hand-rolling a Confluent-compatible wire format.
6. As a service developer, I want a clear error when I configure Avro
   without a Schema Registry, so that misconfiguration fails fast
   instead of at first publish.
7. As a service developer, I want `Flush`/graceful shutdown to log and
   count any messages still un-acknowledged after a timeout, so that
   potential message loss on shutdown is never silent.
8. As a service developer, I want a manual `Consume(ctx)`/`Commit(msg)`
   loop available, so that I can write tests or custom commit cadences
   without the hosted `Runner`.
9. As a service developer, I want consumers to have auto-commit off by
   construction, so that at-least-once delivery is the only delivery
   mode and I can't accidentally lose messages by mis-configuring
   auto-commit.
10. As a service developer, I want a hosted `Runner[K, V]` that drives
    my `Handler[K, V]` for every message and commits only after it
    returns without error, so that I don't reimplement the
    consume-handle-commit loop myself.
11. As a service developer, I want to configure what happens when a
    message can't be deserialized or my handler errors (`Skip` /
    `DeadLetter` / `Halt`), so that I control the poison-message
    trade-off per consumer.
12. As a service developer, I want `DeadLetter` to only advance the
    offset after the dead-letter publish is confirmed, so that a
    failed dead-letter publish never silently loses the original
    message.
13. As a service developer, I want `Halt` to stop the consumer without
    committing, so that a human can intervene and the message is
    re-delivered on restart.
14. As a service developer, I want every poison-message outcome logged
    and counted, so that I have visibility even when the policy is
    `Skip`.
15. As a service developer, I want the `Runner`/`Relay` to run as a
    plain blocking `Run(ctx) error`, so that I can start it with
    `go r.Run(ctx)` or wire it into whatever process-supervision my
    service already has, without adopting a hosting framework.
16. As a service developer, I want to stage an outbox event
    (`Enqueue`) against my own already-open `*sql.Tx`, so that the
    outbox row commits atomically with my business writes.
17. As a service developer, I want the outbox relay to run as a
    separate process/goroutine that polls and publishes staged rows to
    Kafka at-least-once, so that publishing is decoupled from my
    request path.
18. As a service developer, I want the relay to claim a batch under a
    dialect-appropriate row lock (Postgres `FOR UPDATE SKIP LOCKED`,
    SQL Server `UPDLOCK`/`READPAST`), so that multiple relay instances
    can run concurrently without duplicating work.
19. As a service developer, I want the relay to only delete an outbox
    row after the publish reports `Persisted`, so that a
    possibly-lost message is retried rather than dropped.
20. As a service developer, I want the relay to preserve per-key
    publish order within a batch (stopping at the first failure rather
    than skipping ahead), so that I don't get out-of-order events for
    the same key.
21. As a service developer, I want the relay to stamp an `EventId`
    header (from the outbox row id) on every published message, so
    that consumers have a stable de-duplication key.
22. As a service developer, I want to check `HasProcessed(eventID)` and
    call `MarkProcessed(eventID)` against my own already-open
    `*sql.Tx`, so that inbox bookkeeping commits atomically with my
    handler's business writes.
23. As a service developer, I want the inbox to dedup on the
    transport-agnostic `EventId`, so that the same inbox code works
    regardless of which broker delivered the message.
24. As a service developer, I want both outbox and inbox to work
    against either Postgres or SQL Server through the same core API,
    so that switching databases doesn't require switching to a
    different package shape.
25. As a service developer, I want errors from this library to be
    sentinel values I can check with `errors.Is`/`errors.As`, wrapped
    with context via `%w`, so that I can branch on error category
    without depending on broker-specific error types.
26. As a service developer, I want to pass my own `*slog.Logger` and an
    OpenTelemetry `Meter`, so that this library's logs/metrics flow
    into whatever observability stack my service already uses.
27. As a service developer, I want a `ReadyCheck(ctx) error` on
    producer/consumer, so that I can wire it into my service's own
    health-check endpoint without a framework-specific health-check
    type.
28. As a library maintainer, I want the mandatory config fields to be
    real struct fields validated by `Validate()`, not functional
    options, so that a caller can't construct a producer/consumer
    missing a required field.

## Implementation Decisions

- **Kafka client**: `confluentinc/confluent-kafka-go/v2` (cgo,
  librdkafka). See ADR 0001.
- **Wire formats**: `Bytes`, `String`, `Json`, `Avro` all supported
  from the first release; Avro uses `confluent-kafka-go`'s own
  `schemaregistry`/`avro` sub-packages. See ADR 0002.
- **API shape**: generic `Producer[K, V]` / `Consumer[K, V]` in the
  root module. Construction is `New(cfg, opts...)` — a config struct
  with a `Validate() error` method, plus functional options
  (`WithLogger`, `WithMetrics`) for optional cross-cutting concerns.
  No builder type. See ADR 0003.
- **Errors**: package-level sentinel errors wrapped with `%w`; no
  exception-style type hierarchy. See ADR 0004.
- **Outbox/Inbox persistence**: core packages accept `*sql.Tx`
  (`database/sql`), driver-agnostic. Dialect-specific batch-claim
  locking SQL lives in `outbox/postgres` (existing) and a new
  `outbox/sqlserver` submodule (and `inbox` equivalents where dialect
  matters). See ADR 0005.
- **Observability**: `log/slog` for logging, OpenTelemetry metrics API
  for instruments, plain `ReadyCheck(ctx) error` for readiness — no
  framework dependency forced on callers. See ADR 0006.
- **Consumer layers & poison policy**: both a manual
  `Consumer[K, V]` (`Consume`/`Commit`) and a hosted `Runner[K, V]`
  (wraps a `Consumer`, drives a `Handler[K, V]`) ship in the `kafka`
  module. `PoisonMessageAction` has three values (`Skip` /
  `DeadLetter` / `Halt`) with the semantics in the User Stories above.
  `Runner.Run` and the outbox `Relay.Run` are both plain blocking
  `Run(ctx) error` functions — no hosting-framework type. See ADR 0007.
- **Module layout**: `github.com/zuksmaq/messaging` (root: contracts,
  errors, `Message`/`ProducedMessage` types), `.../kafka` (client,
  `Runner`, serde), `.../outbox` (core + `outbox/postgres`,
  `outbox/sqlserver`), `.../inbox` (core + dialect submodules as
  needed) — already scaffolded via `go.work`.
- **EventId**: the outbox relay derives it from the outbox row id and
  stamps it as an `event-id` header; the inbox dedups on that same
  header value regardless of transport.

## Testing Decisions

- Good tests here exercise the module's **public API only** — no
  reaching into unexported types/functions to check internal state.
  Assert on observable behavior: what got published (topic, key,
  value, headers, delivery status), what got committed/not committed,
  what rows remain in the outbox/inbox table, what the `Runner` did
  with a message that failed to deserialize.
- **kafka module**: tested through `Producer[K,V]` / `Consumer[K,V]` /
  `Runner[K,V]` against a real broker via testcontainers (Kafka +,
  where Avro is under test, a real Schema Registry container). Avoid
  mocking librdkafka directly — the value of these tests is exercising
  the real client against a real broker.
- **outbox / inbox modules**: tested through `Enqueue` / `HasProcessed`
  / `MarkProcessed` / `Relay.Run` against a real database via
  testcontainers, run against **both** Postgres and SQL Server
  (parameterized/table-driven across dialect) since ADR 0005 makes
  dialect parity a first-class requirement. Use a fake `Producer`
  (implementing the `Producer[K,V]` interface from ADR 0003) as the
  seam so these tests don't need a real Kafka broker — assert on what
  the fake recorded (topic, key, value, headers) rather than on Kafka
  itself.
- No prior art for these test seams exists yet in this repo (the
  module stubs are currently empty) — this spec establishes the
  pattern going forward: one seam per module's public API, real
  infra via testcontainers for the module's own dependency (broker or
  database), a fake for the *other* module's dependency where a module
  depends on another module's public interface (outbox/inbox depend on
  `Producer`).

## Out of Scope

- SQL Server support for anything beyond the outbox/inbox
  batch-claim/dedup SQL (no broader SQL Server-specific feature work).
- Exactly-once (transactional) Kafka semantics — the idempotent
  producer plus outbox/inbox gives at-least-once with
  application-level dedup, which is the explicitly accepted trade-off.
- A process supervisor/errgroup helper for wiring multiple `Run(ctx)`
  loops together (ADR 0007 explicitly excludes this from the module).
- Any hosting-framework integration (no `IHostedService`-style
  adapters, no framework-specific health-check types).
- Protobuf or JSON-Schema-Registry formats (only Bytes/String/JSON/Avro
  are in scope per ADR 0002).
- Consumer-group rebalance strategy tuning beyond librdkafka defaults.
- Any UI, CLI, or admin tooling for inspecting outbox/inbox state.

## Further Notes

- This spec covers the initial ("v1") build of all three modules
  together, since they share the config/error/observability
  conventions in ADRs 0003–0006 and are meant to ship as one coherent
  release. Splitting into per-module implementation tickets is
  expected at the next stage (issues under
  `.scratch/messaging-core/issues/`).
- `outbox/sqlserver` does not exist yet in the repo and must be
  created as part of this work (ADR 0005).
- Nothing here mandates a specific ORM or query builder — the decision
  was explicitly to stay on `database/sql` directly.
