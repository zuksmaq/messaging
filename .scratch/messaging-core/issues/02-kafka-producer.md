# 02 — Kafka producer (Bytes/String/JSON)

**What to build:** a working `kafka` producer a service can construct
and publish through end to end: `New(cfg, opts...)` builds an
idempotent-by-default producer against `confluent-kafka-go`; `Produce`
serializes key/value in the chosen format (Bytes/String/JSON — Avro
is ticket 04), awaits the broker acknowledgement, and returns the
`DeliveryStatus`. See ADR 0001, ADR 0002 (formats other than Avro),
ADR 0003, ADR 0006.

**Blocked by:** 01 (root contracts).

**Status:** done

- [x] `ProducerConfig` struct with mandatory fields (bootstrap
      servers, key/value format, security, etc.) and a `Validate()
      error` method; `New` calls it and refuses to construct an
      invalid producer.
- [x] No builder type — construction is `New(cfg, opts...)` only.
- [x] Producer implements the root `Producer[K, V]` interface.
- [x] Idempotent producer semantics on by default (acks=all,
      max-in-flight consistent with idempotence).
- [x] Bytes / String / JSON key and value formats work independently
      (key format need not match value format).
- [x] `Produce` returns a `ProducedMessage` with the real
      topic/partition/offset/`DeliveryStatus` from the broker.
- [x] Shutdown/`Flush` blocks up to the configured timeout; a non-zero
      un-acknowledged remainder is logged (via the caller's
      `*slog.Logger`, `WithLogger`) and counted (via the caller's OTel
      `Meter`, `WithMetrics`) — never silently dropped.
- [x] `ReadyCheck(ctx) error` reports producer health without a
      framework-specific health-check type.
- [x] Integration test (testcontainers, real Kafka broker): construct
      a producer, publish a message per supported format, assert the
      returned `DeliveryStatus` is `Persisted` and the message is
      actually readable back off the topic.
- [x] Integration test: shutdown with buffered messages exceeding the
      flush timeout logs+counts the residual instead of hanging or
      silently dropping.

## Comments

Implemented in `kafka/config.go` (`ProducerConfig`, `Security`,
`Validate`), `kafka/serde.go` (`Format` + Bytes/String/JSON
serializers), `kafka/options.go` (`WithLogger`/`WithMetrics`), and
`kafka/producer.go`.

Design note: `Format` is a `ProducerConfig` field per this ticket, so
`New[K, V]` resolves it to a `Serializer[T]` at construction time and
rejects a format/type mismatch (e.g. `FormatString` with `K=int`) with
`ErrInvalidConfig`. That pairing is therefore a runtime check, not a
compile-time one — the trade-off of keeping format in config, which
ticket 04 (Avro + schema registry config) also needs.

Two defects found while testing:

- `New` called `Validate()` before applying defaults, so a config that
  left `Security.Protocol` unset was rejected. An empty protocol now
  validates and means `plaintext`.
- `Flush` returns librdkafka's whole queue length, which includes
  broker error events, so the reported residual was inflated (9 for 5
  queued messages). The producer now drains the client event channel
  in a background goroutine, which both fixes the count and stops the
  events queue growing unbounded. Transient client errors log at debug
  (librdkafka re-reports them every retry, and `Produce` already
  returns the error); fatal ones log at error.

Integration tests live behind the `integration` build tag with a new
`integration` CI job, so the default job stays fast. Verified locally
against a real broker (`confluentinc/confluent-local:7.6.1`) via
testcontainers. The shutdown/residual test needs no broker (it points
at a dead port), so it runs untagged on every CI run.
