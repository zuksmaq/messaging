# 12 — Outbox: claim lease/timeout for abandoned rows

**What to build:** claimed-but-abandoned outbox rows (e.g. a `Relay`
process killed mid-batch, or a dropped connection invisible to a
connection pooler) must be released within a bounded, documented
window — configure an aggressive statement/session timeout (and
driver-level TCP keepalive, where supported) on the database
connections `Relay` uses for claiming, rather than relying on
incidental, unbounded connection-drop detection.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] `Relay`'s documented configuration includes an explicit,
      bounded timeout (and keepalive, where the driver supports it)
      for claim-holding database connections.
- [ ] A row claimed by a relay instance that then disappears (e.g.
      its connection is killed) becomes claimable again by another
      relay instance within the documented bound, rather than being
      stuck indefinitely.
- [ ] A test simulates an abandoned claim (e.g. killing the claiming
      connection) and asserts the row becomes claimable again within
      the documented bound.
