# 03 — Kafka manual consumer (Bytes/String/JSON)

**What to build:** the manual consume-then-commit loop: `New(cfg,
opts...)` builds a consumer subscribed to its configured topics with
auto-commit off by construction; `Consume(ctx)` blocks for the next
message and deserializes it, `Commit(msg)` advances the offset only
when the caller calls it. See ADR 0002 (formats other than Avro), ADR
0003, ADR 0006, ADR 0007 (this ticket is the manual layer only — the
hosted `Runner` is ticket 05).

**Blocked by:** 01 (root contracts), 02 (kafka producer — reused to
seed test messages on the broker).

**Status:** ready-for-agent

- [ ] `ConsumerConfig` struct (bootstrap servers, group id, topics,
      key/value format, security, etc.) with `Validate()`; `New` calls
      it.
- [ ] No builder type — construction is `New(cfg, opts...)` only.
- [ ] Consumer implements the root `Consumer[K, V]` interface.
- [ ] Auto-commit is off unconditionally — there is no config knob to
      turn it on.
- [ ] `Consume(ctx)` returns a deserialized `ReceivedMessage[K, V]`,
      correctly surfacing the tombstone case (nil value) without
      attempting to decode it.
- [ ] `Commit(msg)` commits offset+1 for the message's
      topic/partition (matches Kafka's "next offset to read"
      semantics).
- [ ] `ReadyCheck(ctx) error` reports consumer health.
- [ ] Integration test (testcontainers, real broker): publish via the
      ticket-02 producer, consume+commit via this consumer for each
      supported format, assert the round-tripped value matches and
      the committed offset is correct.
- [ ] Integration test: a tombstone (nil value) round-trips with
      `IsTombstone` true and no deserialization error.
