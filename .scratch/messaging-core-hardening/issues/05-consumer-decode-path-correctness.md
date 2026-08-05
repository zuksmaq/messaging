# 05 — Consumer decode-path correctness

**What to build:** the per-record decode path must check the
broker-reported per-record error before touching the record's
key/value, surfacing it as an error through the existing sentinel-
error conventions rather than treating the record's bytes as valid
message data. Separately, `ReceivedMessage` must expose the
original, order-preserving, duplicate-preserving header list (a new
field alongside the existing convenience map) so a consumer with
duplicate header keys (e.g. two headers of the same name from
different hops) can recover every value, not just the last one.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] A polled record carrying a partition/fetch-level broker error
      is surfaced as an error from `Consume` (flowing through
      `Runner`'s poison-message handling like any other consume
      failure) instead of being decoded and treated as real message
      data.
- [x] `ReceivedMessage` carries a new field exposing every header
      value for a given key, in delivery order, for records with
      duplicate header keys.
- [x] The existing convenience header map remains unchanged
      (last-wins) for backward compatibility.
- [x] A test delivers a record with a broker-reported per-record
      error and asserts it surfaces as an error, not decoded data.
- [x] A test delivers a record with two headers sharing the same key
      and asserts both values are recoverable from the new field.

## Comments

`Consumer[K, V].decode` now checks `m.TopicPartition.Error` right after
building `RawKey`/`RawValue`/headers but before either deserializer runs,
returning an error wrapping both `messaging.ErrDeserialization` and
`messaging.ErrBroker` (Go 1.20+ multiple `%w`). Wrapping
`ErrDeserialization` was the deliberate choice: `Runner.Run` only applies
the poison-message policy on `errors.Is(err, messaging.ErrDeserialization)`
— everything else wrapping `ErrBroker` (a fatal client error, a failed
commit) is treated as unrecoverable and stops `Run`. Wrapping both
sentinels lets this case flow through the poison policy like a
deserialization failure while still reporting truthfully as a broker
error to any caller checking for that.

`ReceivedMessage` gained `HeaderList []Header` (new exported
`messaging.Header{Key, Value}` type), populated alongside the existing
`Headers map[string][]byte` by the consumer's `fromHeaders`, which now
returns both from a single pass over the broker's header slice. The map
stays last-wins and unchanged for existing callers.

Covered by `TestConsumerDecodeSurfacesPartitionError` (asserts the error
wraps both sentinels and that `Key`/`Value` stay zero-valued) and
`TestConsumerDecodePreservesDuplicateHeaders` (asserts last-wins on the
map and full delivery-order recovery from `HeaderList`) in
`kafka/consumer/consumer_test.go`.
