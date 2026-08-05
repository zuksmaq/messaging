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

Added `pollMu sync.Mutex` and `closed bool` to `Consumer`. Each
`Consume` iteration now calls a new `poll()` method that holds
`pollMu` only for the duration of one `client.Poll()` call and checks
`closed` first; `Close()` marks the consumer closed via a small
extracted `markClosed()` (which takes `pollMu` before setting the
flag) before touching the client, which both waits out any in-flight
poll and stops the next one from starting once the client handle is
torn down (`kafka/consumer/consumer.go`).

`Close()`'s two failure sources are combined via a small extracted
`joinCloseErrors(clientErr, registryErr) error` helper using
`errors.Join`, tested directly with synthetic errors
(`TestJoinCloseErrors`) since forcing the real `confluent-kafka-go`
client and a real schema registry to fail simultaneously isn't
practical to do deterministically.

Code review caught two issues in the first pass, both fixed:
- `pollMu` was originally a `sync.RWMutex`, which only pays off with
  concurrent readers — `Consume` is a single-caller blocking loop, so
  a plain `Mutex` says the same thing with less surface area.
- The original concurrency test (`TestCloseWaitsForInFlightConsume`)
  raced a real `Consume` loop against `Close()` behind a
  `time.Sleep(10ms)`, which only reliably lands `Close` mid-poll a
  fraction of the time (an unreachable-broker `Poll` call returns
  almost immediately, so there's little window to hit). Added
  `TestMarkClosedWaitsForInFlightPoll`, which holds `pollMu` directly
  to simulate an in-flight poll and asserts `markClosed()` blocks
  until it's released — a deterministic proof of the actual lock
  invariant, independent of real client timing. The original test is
  kept as a supplementary end-to-end sanity check under `-race`.
`TestConsumeAfterCloseReturnsPromptly` asserts a `Consume` call after
`Close()` returns promptly with an error instead of touching the
destroyed client.

Verified with `GOWORK=off go build ./... && GOWORK=off go vet ./...`,
`GOWORK=off go test -race ./...` (all packages green, no races), and
`-tags=integration go test ./consumer/... -run TestConsumerReadyCheck`
against a real broker container.
