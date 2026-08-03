# 1. Use confluent-kafka-go as the Kafka client

## Status

Accepted

## Context

The `kafka` module needs a Kafka client library. Candidates
considered:

- `confluentinc/confluent-kafka-go` — official Confluent client, cgo
  wrapper over librdkafka.
- `twmb/franz-go` — pure Go, no cgo, actively maintained, own Schema
  Registry module.
- `segmentio/kafka-go` — pure Go, no cgo, but effectively unmaintained
  and with weaker consumer-group/transaction support.

## Decision

Use `confluentinc/confluent-kafka-go/v2`.

Rationale: librdkafka is a mature, battle-tested engine with proven
idempotent-producer semantics, delivery-status guarantees, and
consumer-group behavior. It also ships first-party Schema Registry +
Avro/Protobuf/JSON-Schema serde support from the same vendor, which
the wire-format decision (ADR 0002) depends on.

The cgo/librdkafka build+deploy dependency is accepted as the
trade-off for behavioral maturity and first-party schema registry
support.

## Consequences

- Build and container images need librdkafka available (cgo).
- `segmentio/kafka-go` is explicitly rejected — not recommended for
  new production code given its maintenance state.
