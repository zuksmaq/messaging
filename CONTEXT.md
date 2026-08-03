# Messaging — Context

Kafka messaging plus transactional outbox and inbox (idempotent-consumer)
patterns for Go services. See `docs/adr/` for the individual decisions
and their rationale.

## Modules

| Module | Path | Depends on |
|---|---|---|
| root | `github.com/zuksmaq/messaging` | — |
| kafka | `github.com/zuksmaq/messaging/kafka` | root |
| outbox | `github.com/zuksmaq/messaging/outbox` | root |
| inbox | `github.com/zuksmaq/messaging/inbox` | root |
| integration | `github.com/zuksmaq/messaging/integration` | all four |

`outbox/postgres` and `outbox/sqlserver` (and the `inbox` equivalents)
hold dialect-specific SQL only — the core `outbox`/`inbox` packages are
database-agnostic.

## Glossary

- **Producer[K, V] / Consumer[K, V]** — generic, broker-agnostic
  publish/consume contracts (root module). `Producer.Produce` awaits
  the broker acknowledgement; never fire-and-forget. `Consumer` is the
  manual consume-then-commit loop: auto-commit is always off, offsets
  advance only after a message is handled.
- **Runner[K, V]** — hosted consumer loop (kafka module) that wraps a
  `Consumer` and drives a `Handler[K, V]` per message, applying the
  poison-message policy. Exposed as a blocking `Run(ctx) error`.
- **Handler[K, V]** — caller-supplied per-message processing function
  registered with a `Runner`. Must be idempotent: at-least-once
  delivery means a handler can see the same message twice.
- **PoisonMessageAction** — what a `Runner` does when a message can't
  be deserialized or a `Handler` returns an error: `Skip` (log, count,
  commit past it), `DeadLetter` (forward to a dead-letter topic, only
  commit once that publish is confirmed), `Halt` (stop without
  committing; message is re-delivered on restart). Never silent. The
  zero `RunnerConfig` selects `Halt`: nothing is dropped until the
  caller asks for it.
- **RawKey / RawValue** — the key and value as the broker delivered
  them, carried on every `ReceivedMessage` alongside the decoded ones.
  They survive a deserialization failure, which is what lets
  `DeadLetter` forward the original bytes through a
  `Producer[[]byte, []byte]` even when the decoded key and value are
  still zero.
- **DeliveryStatus** — how durably a produced message was persisted:
  `NotPersisted` / `PossiblyPersisted` / `Persisted`. The outbox relay
  requires `Persisted` before deleting a row.
- **SchemaRegistry** — the Confluent Schema Registry client the `Avro`
  wire format needs (`SchemaRegistryConfig` on the producer's and
  consumer's `Config`). Schemas register under the topic-name strategy
  subjects `<topic>-key` and `<topic>-value`, so a topic's key and value
  carry independent schemas. Configuring `Avro` without a registry is a
  construction-time error, never a first-publish surprise.
- **EventId** — the producer-assigned idempotency id (an `event-id`
  header). The outbox relay stamps it from the outbox row id;
  consumers use it as the inbox de-duplication key.
- **Outbox** — stages an event in the caller's own database
  transaction (`*sql.Tx`) alongside business writes. A separate relay
  process (`outbox.Relay.Run(ctx)`) polls and publishes staged rows to
  Kafka at-least-once (publish-then-delete), claiming batches under a
  dialect-specific row lock.
- **Inbox** — consumer-side idempotency store. Records processed
  `EventId`s in the caller's own transaction so a `Handler` can check
  `HasProcessed`/`MarkProcessed` and skip duplicates at-least-once
  delivery can re-deliver.

## Conventions

- No builder pattern (not idiomatic Go). Construction is
  `New(cfg, opts...)`: a config struct with a `Validate()` method for
  mandatory fields, functional options for optional cross-cutting
  concerns (logger, metrics).
- Errors are sentinel values (`var ErrXxx = errors.New(...)`) wrapped
  with `%w`, checked via `errors.Is`/`errors.As` — no exception-style
  type hierarchy.
- Logging via `log/slog`; metrics via the OpenTelemetry metrics API.
  Neither is forced on the caller beyond the interface.
