# 9. Collapse the repository into a single Go module

## Status

Accepted. Supersedes the multi-module layout assumed by ADR 0005 and
the "one Go module" aside in ADR 0008 (which described the `kafka`
module specifically).

## Context

The repository shipped five Go modules — root, `kafka`, `outbox`,
`inbox`, `integration` — so each carried its own `go.mod` and, once
released, its own path-prefixed tag: `kafka/v0.1.1`, `outbox/v0.1.1`,
`inbox/v0.1.1` alongside the root `v0.1.1`.

Two costs showed up in practice.

Releasing anything that crossed a module boundary took two rounds. The
`kafka` module pinned `github.com/zuksmaq/messaging v0.1.1`, so adding
`Header`/`HeaderList` to the root package and consuming it from
`kafka/consumer` could not land as one change: the root had to be
tagged first, then `kafka/go.mod` bumped to that tag, then `kafka`
tagged. A `replace` would have papered over it locally while breaking
consumers.

Four tags per release also made the version story ambiguous — nothing
in the tag list said which combination had actually been tested
together, and MVS is free to pick a mismatched set.

Against that, the multi-module split bought dependency isolation: the
root module had no third-party requirements at all, so a service that
only wanted the `Producer[K, V]` contract pulled in nothing.

## Decision

Collapse to a single module, `github.com/zuksmaq/messaging`, versioned
by one tag per release. Delete the four sub-`go.mod` files, their
`go.sum` files, and `go.work`/`go.work.sum`.

No source file changes. The directory layout already matched the
package paths, so `github.com/zuksmaq/messaging/kafka/producer` names
the same package before and after — every import statement in the repo
and in any consumer is unchanged.

The `integration` package stays where it is. Its stated reason for
being a separate module — depending on all four without any of them
depending on each other — is now simply how packages work, and its
four `replace` directives are gone.

Sub-package boundaries are unaffected: ADR 0005's dialect split
(`outbox/postgres`, `outbox/sqlserver`, and the `inbox` equivalents)
and ADR 0008's `kafka/producer` / `kafka/consumer` split are package
structure, which this does not touch.

## Consequences

- One tag per release. Cross-cutting changes land in one commit, and a
  version identifies a combination that was tested together.
- The root package no longer implies a dependency-free import. Its own
  imports are still stdlib-only, but the module now requires
  `confluent-kafka-go` (cgo/librdkafka), `pgx`, `go-mssqldb`, `otel`,
  and `testcontainers`. Go's module-graph pruning means a consumer
  importing only `outbox` still neither downloads nor compiles the
  Kafka client — but dependency scanners read the module graph, so such
  a service will see librdkafka and Docker advisories reported against
  it. This was the accepted cost.
- Test-only dependencies (`testcontainers`) sit in the published
  `go.mod`. They already did in three of the five modules.
- `go.work` is gone; ordinary `go build ./...` covers the repo, and CI
  no longer loops modules under `GOWORK=off`.
- The `kafka/v0.1.1`, `outbox/v0.1.1`, and `inbox/v0.1.1` tags are
  deleted from the repository, but `proxy.golang.org` has already
  cached them and cannot be made to forget. They stay resolvable for
  anyone who pinned them; nothing at those paths will be published
  again. A consumer requiring both the new root module and one of the
  old sub-modules would hit an ambiguous-import error and must drop the
  latter.
