# 06 — Outbox: poison-row quarantine policy

**What to build:** give `Outbox.Relay` a bounded-attempts policy for
a staged row that repeatedly fails to publish (e.g. an oversized
payload, a missing topic, a revoked ACL). After a configurable
attempt threshold, the row is marked quarantined — logged and
counted, never silent — and the relay continues processing
subsequent rows instead of blocking indefinitely behind the one
poisoned row.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] A row whose publish fails on every attempt is quarantined after
      a configurable number of attempts, rather than blocking every
      later-staged row forever.
- [ ] A quarantined row is logged and its count is observable (metric
      or equivalent), matching the "never silent" posture already
      established for the `Runner`'s poison-message handling.
- [ ] Rows staged after a quarantined row continue to be claimed and
      published normally.
- [ ] A test stages a permanently-unpublishable row followed by
      normally-publishable rows and asserts the later rows are still
      delivered, and the poisoned row is quarantined and counted.
