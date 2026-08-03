# 2. Support Bytes/String/JSON/Avro wire formats, including Schema Registry, from day one

## Status

Accepted

## Context

Producers/consumers need pluggable wire formats. JSON is what the
outbox and current consumers actually use today; Avro adds real
complexity (schema registry client, Avro codec, schema compatibility
checks) with no current caller.

## Decision

Support the full format surface now — `Bytes`, `String`, `Json`, and
`Avro` — rather than deferring Avro. Use
`confluentinc/confluent-kafka-go`'s own `schemaregistry`/`avro`
sub-packages (ADR 0001) for the Schema Registry and Avro codec.

This was an explicit call to front-load the flexibility rather than
add Avro later as a bolt-on serde package, even though no current
consumer needs it.

## Consequences

- `Serializer[T]`/`Deserializer[T]` need an Avro implementation and a
  Schema Registry client wire-up from the start, not a stub.
- Schema Registry config (URL, auth, TLS, schema cache size) needs a
  config struct from the start.
