# 04 — Avro / Schema Registry serde

**What to build:** the `Avro` wire format for both the producer
(ticket 02) and consumer (ticket 03), backed by Confluent Schema
Registry via `confluent-kafka-go`'s own `schemaregistry`/`avro`
sub-packages. See ADR 0002.

**Blocked by:** 02 (kafka producer), 03 (kafka consumer).

**Status:** done

- [x] `SchemaRegistryConfig` (URL, auth, TLS, schema cache size) is a
      distinct config block, required only when key or value format
      is `Avro`.
- [x] Producer/consumer construction fails fast with a clear
      configuration-error (sentinel error from ticket 01) when `Avro`
      is configured without a Schema Registry — never at first
      publish/consume.
- [x] Avro serializer/deserializer round-trip a value through a real
      Schema Registry container (testcontainers), key and value format
      configurable independently of each other and of Bytes/String/
      JSON.
- [x] Integration test: producing an Avro message that violates the
      registered schema fails with a serialization sentinel error, not
      a raw Schema-Registry-client error type.

## Notes

- Subjects use the topic-name strategy (`<topic>-key` /
  `<topic>-value`) rather than the client's newer default, which
  resolves the subject through a topic-to-subject association API that
  only some registries serve.
- `Avro` requires a struct for the half it encodes: the codec derives
  the schema from the type's exported fields, so a pointer, slice or
  scalar is refused at construction.
- The Avro integration tests keep the Confluent broker but take their
  registry from a Redpanda container, whose Schema Registry speaks the
  same HTTP API in a much smaller image; `cp-schema-registry` needs a
  broker of its own plus a docker network to reach it over. Where
  schemas are stored is invisible to the code under test.
