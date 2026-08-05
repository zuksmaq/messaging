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

**Status:** done

- [x] `Close()` called while a `Consume`/poll call is in flight waits
      for that call to return before proceeding, instead of tearing
      down the client concurrently with it.
- [x] `Close()` calling both a failing client close and a failing
      schema-registry close returns an error that surfaces both
      failures (e.g. via `errors.Join`), not just one.
- [x] A test exercises `Close()` racing a concurrent `Consume`/`Run`
      call (e.g. under the race detector) and asserts no data race
      and no crash.
- [x] A test asserts that a `Close()` call with both underlying
      failures reports both errors.

## Comments

Added `pollMu sync.RWMutex` and `closed bool` to `Consumer`. Each
`Consume` iteration now calls a new `poll()` method that holds
`pollMu` for read only for the duration of one `client.Poll()` call
and checks `closed` first; `Close()` sets `closed` and takes `pollMu`
for write before touching the client, which both waits out any
in-flight poll and stops the next one from starting once the client
handle is torn down (`kafka/consumer/consumer.go`).

`Close()`'s two failure sources are combined via a small extracted
`joinCloseErrors(clientErr, registryErr) error` helper using
`errors.Join`, tested directly with synthetic errors
(`TestJoinCloseErrors`) since forcing the real `confluent-kafka-go`
client and a real schema registry to fail simultaneously isn't
practical to do deterministically. `TestCloseWaitsForInFlightConsume`
races a `Consume` loop against `Close()` under `-race` (stress-run
`-count=10` locally with no failures); `TestConsumeAfterCloseReturnsPromptly`
asserts a `Consume` call after `Close()` returns promptly with an
error instead of touching the destroyed client.

Verified with `GOWORK=off go build ./... && GOWORK=off go vet ./...`,
`GOWORK=off go test -race ./...` (all packages green, no races), and
`-tags=integration go test ./consumer/... -run TestConsumerReadyCheck`
against a real broker container.
