# 04 — Consumer close/shutdown hardening

**What to build:** `Consumer.Close()` must not run concurrently with
an in-flight `Consume`/poll call — it should wait for any in-flight
call to return before tearing down the underlying client, closing
the documented segfault window between a cancelled `Run` loop and a
concurrent `Close`. Separately, `Close()` must report both the
underlying client's close error and the schema-registry's close
error when both occur, instead of returning only one and dropping
the other.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] `Close()` called while a `Consume`/poll call is in flight waits
      for that call to return before proceeding, instead of tearing
      down the client concurrently with it.
- [ ] `Close()` calling both a failing client close and a failing
      schema-registry close returns an error that surfaces both
      failures (e.g. via `errors.Join`), not just one.
- [ ] A test exercises `Close()` racing a concurrent `Consume`/`Run`
      call (e.g. under the race detector) and asserts no data race
      and no crash.
- [ ] A test asserts that a `Close()` call with both underlying
      failures reports both errors.
