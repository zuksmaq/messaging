# 04 — Avro / Schema Registry serde

**What to build:** the `Avro` wire format for both the producer
(ticket 02) and consumer (ticket 03), backed by Confluent Schema
Registry via `confluent-kafka-go`'s own `schemaregistry`/`avro`
sub-packages. See ADR 0002.

**Blocked by:** 02 (kafka producer), 03 (kafka consumer).

**Status:** ready-for-agent

- [ ] `SchemaRegistryConfig` (URL, auth, TLS, schema cache size) is a
      distinct config block, required only when key or value format
      is `Avro`.
- [ ] Producer/consumer construction fails fast with a clear
      configuration-error (sentinel error from ticket 01) when `Avro`
      is configured without a Schema Registry — never at first
      publish/consume.
- [ ] Avro serializer/deserializer round-trip a value through a real
      Schema Registry container (testcontainers), key and value format
      configurable independently of each other and of Bytes/String/
      JSON.
- [ ] Integration test: producing an Avro message that violates the
      registered schema fails with a serialization sentinel error, not
      a raw Schema-Registry-client error type.
