# 02 — Kafka producer (Bytes/String/JSON)

**What to build:** a working `kafka` producer a service can construct
and publish through end to end: `New(cfg, opts...)` builds an
idempotent-by-default producer against `confluent-kafka-go`; `Produce`
serializes key/value in the chosen format (Bytes/String/JSON — Avro
is ticket 04), awaits the broker acknowledgement, and returns the
`DeliveryStatus`. See ADR 0001, ADR 0002 (formats other than Avro),
ADR 0003, ADR 0006.

**Blocked by:** 01 (root contracts).

**Status:** ready-for-agent

- [ ] `ProducerConfig` struct with mandatory fields (bootstrap
      servers, key/value format, security, etc.) and a `Validate()
      error` method; `New` calls it and refuses to construct an
      invalid producer.
- [ ] No builder type — construction is `New(cfg, opts...)` only.
- [ ] Producer implements the root `Producer[K, V]` interface.
- [ ] Idempotent producer semantics on by default (acks=all,
      max-in-flight consistent with idempotence).
- [ ] Bytes / String / JSON key and value formats work independently
      (key format need not match value format).
- [ ] `Produce` returns a `ProducedMessage` with the real
      topic/partition/offset/`DeliveryStatus` from the broker.
- [ ] Shutdown/`Flush` blocks up to the configured timeout; a non-zero
      un-acknowledged remainder is logged (via the caller's
      `*slog.Logger`, `WithLogger`) and counted (via the caller's OTel
      `Meter`, `WithMetrics`) — never silently dropped.
- [ ] `ReadyCheck(ctx) error` reports producer health without a
      framework-specific health-check type.
- [ ] Integration test (testcontainers, real Kafka broker): construct
      a producer, publish a message per supported format, assert the
      returned `DeliveryStatus` is `Persisted` and the message is
      actually readable back off the topic.
- [ ] Integration test: shutdown with buffered messages exceeding the
      flush timeout logs+counts the residual instead of hanging or
      silently dropping.
