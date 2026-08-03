# 8. Split the kafka module into producer and consumer packages

## Status

Accepted

## Context

ADR 0003 fixes construction as `New(cfg, opts...)`. With the producer
and consumer both living in a single `kafka` package that name can only
be spent once — the producer took `New`, forcing the consumer into
`NewConsumer` and breaking the symmetry the ADR describes.
`confluent-kafka-go` itself takes the other route
(`kafka.NewProducer`/`kafka.NewConsumer` in one package), so both shapes
had precedent.

## Decision

Split the `kafka` module into sub-packages: `kafka/producer` and
`kafka/consumer`, each owning its own `Config` and a constructor named
`New`. This remains one Go module — the split is packages, not modules.

The root `kafka` package keeps only what both sides share and callers
name at the call site: `Format` and the `Serializer`/`Deserializer`
contracts, `Security`, and the `Option` set (`WithLogger`,
`WithMetrics`).

Shared plumbing the sub-packages need is exported rather than hidden
behind an internal package: `SerializerFor`/`DeserializerFor`,
`Security.Settings`, `Security.WithDefaults`, and `ResolveOptions`.
Exporting a librdkafka `ConfigMap` was rejected — `Security.Settings`
returns a plain `map[string]string` so the broker client stays out of
the public API.

Ticket 05's `Runner` will live in `kafka/consumer` alongside the
`Consumer` it wraps.

## Consequences

- Construction reads `producer.New(producer.Config{...})` /
  `consumer.New(consumer.Config{...})`, matching ADR 0003 literally.
- Call sites name two packages: `producer.Config{KeyFormat:
  kafka.FormatString}`. Accepted as the cost of the symmetry.
- The root `kafka` package holds no client type — it describes how to
  reach a cluster and how to encode what travels over it.
- Integration-test scaffolding shared by both sub-packages lives in
  `kafka/internal/kafkatest`, built only under the `integration` tag so
  testcontainers stays out of ordinary builds.
