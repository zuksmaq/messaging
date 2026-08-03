# 01 — Root contracts & sentinel errors

**What to build:** the broker-agnostic seam every other module builds
against: generic `Producer[K, V]` / `Consumer[K, V]` interfaces,
`Message`/`ProducedMessage`/`ReceivedMessage` value types,
`DeliveryStatus`, the `EventId` header constant, and the package-level
sentinel errors (wrapped with `%w`, checked via `errors.Is`/
`errors.As`). See ADR 0003 and ADR 0004.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] `Producer[K, V]` interface defined in the root module: a
      `Produce`-style method that awaits broker acknowledgement and
      returns a `ProducedMessage` (topic, partition, offset,
      `DeliveryStatus`), plus a flush/close method.
- [x] `Consumer[K, V]` interface defined: a `Consume`/`Commit`-style
      pair, auto-commit intentionally absent from the contract.
- [x] `DeliveryStatus` enum: `NotPersisted` / `PossiblyPersisted` /
      `Persisted`.
- [x] `ReceivedMessage[K, V]` carries topic/partition/offset/key/value/
      headers/timestamp and exposes the tombstone case (nil value) and
      the `EventId` derived from the `event-id` header.
- [x] `EventId` header name is an exported constant, usable by both
      the `kafka` and `outbox`/`inbox` modules without a Kafka-specific
      dependency.
- [x] Sentinel errors exist for the categories callers need to branch
      on (serialization failure, deserialization failure, schema-
      registry-required, configuration invalid, producer/consumer
      broker error) — no exception-style type hierarchy.
- [x] Package compiles standalone (`go build ./...` at the root module)
      with no dependency on the `kafka`/`outbox`/`inbox` submodules.

## Comments

Implemented in `message.go` (types + DeliveryStatus + EventIDHeader),
`producer.go`, `consumer.go`, `errors.go`. Verified with
`GOWORK=off go build ./... && GOWORK=off go vet ./...` from the root
module.
