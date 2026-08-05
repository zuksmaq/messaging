# 10 — Root module: fix Tombstone() for non-[]byte value types

**What to build:** `ReceivedMessage.Tombstone()` must determine
tombstone status from the raw broker-delivered value (`RawValue`),
not by reflecting on the decoded, generically-typed `Value` — so a
compaction tombstone (nil value) is correctly reported regardless of
whether `V` is `[]byte`, a decoded struct, or `any`.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] `Tombstone()` reports true for a nil-value record regardless
      of the generic value type `V` (raw bytes, a decoded struct,
      and `any` are all covered).
- [x] A new test file at the root module (the module's first)
      directly exercises `ReceivedMessage.Tombstone()` across at
      least these three value-type shapes, with no broker or
      database dependency.
