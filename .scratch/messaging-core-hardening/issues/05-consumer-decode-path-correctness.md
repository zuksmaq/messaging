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

**Status:** ready-for-agent

- [ ] A polled record carrying a partition/fetch-level broker error
      is surfaced as an error from `Consume` (flowing through
      `Runner`'s poison-message handling like any other consume
      failure) instead of being decoded and treated as real message
      data.
- [ ] `ReceivedMessage` carries a new field exposing every header
      value for a given key, in delivery order, for records with
      duplicate header keys.
- [ ] The existing convenience header map remains unchanged
      (last-wins) for backward compatibility.
- [ ] A test delivers a record with a broker-reported per-record
      error and asserts it surfaces as an error, not decoded data.
- [ ] A test delivers a record with two headers sharing the same key
      and asserts both values are recoverable from the new field.
