# 06 — Outbox: poison-row quarantine policy

**What to build:** give `Outbox.Relay` a bounded-attempts policy for
a staged row that repeatedly fails to publish (e.g. an oversized
payload, a missing topic, a revoked ACL). After a configurable
attempt threshold, the row is marked quarantined — logged and
counted, never silent — and the relay continues processing
subsequent rows instead of blocking indefinitely behind the one
poisoned row.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] A row whose publish fails on every attempt is quarantined after
      a configurable number of attempts, rather than blocking every
      later-staged row forever.
- [x] A quarantined row is logged and its count is observable (metric
      or equivalent), matching the "never silent" posture already
      established for the `Runner`'s poison-message handling.
- [x] Rows staged after a quarantined row continue to be claimed and
      published normally.
- [x] A test stages a permanently-unpublishable row followed by
      normally-publishable rows and asserts the later rows are still
      delivered, and the poisoned row is quarantined and counted.

## Comments

Each dialect's outbox table gains an `attempts INT` column and a
nullable `quarantined_at` timestamp; `ClaimSQL` now also selects
`attempts` and excludes rows where `quarantined_at IS NOT NULL`.
`Dialect` gained `IncrementAttemptsSQL`/`QuarantineSQL`, one param (the
row id) each.

`RelayConfig.MaxAttempts` (default 5) is the configurable threshold.
On a publish failure, `Relay.recordFailure` either increments the row's
attempt count or, once `Attempts+1 >= MaxAttempts`, quarantines it —
logged via `slog.WarnContext` and counted via a new
`messaging.outbox.quarantined` metric, mirroring the Runner's poison
handling. `publishBatch`'s per-row loop `continue`s past a quarantined
row (it's now excluded from future claims, so publishing past it can't
reorder anything) but still `break`s on a row that hasn't yet exhausted
its attempts, preserving the existing per-key ordering guarantee up to
that threshold. The commit-vs-rollback decision now tracks a
`progressed` flag rather than `published == 0`, since a batch can do
real work (an attempt increment) with zero successful publishes.

Covered by `TestRelayQuarantinesAPermanentlyUnpublishableRow`
(integration, both dialects): a permanently-unconfirmable row followed
by two normal rows ends with the poison row quarantined and the other
two published and deleted. Fixing this also required widening
`startRelay`'s test helper with a `maxAttempts` parameter:
`TestRelayStopsAtTheFirstUnconfirmedRow`'s fast poll interval was
tripping the new default threshold mid-test, so it now passes a high
value to keep testing the ordering guarantee in isolation from
quarantining. Full outbox integration suite (all backends) passes.
