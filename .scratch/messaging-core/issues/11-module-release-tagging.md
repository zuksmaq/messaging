# 11 — Publish the modules: drop replace directives, tag a release

**What to build:** whatever it takes for another service to run
`go get github.com/zuksmaq/messaging/kafka` and have it work. Today
nothing outside this repo can consume any of these modules: there are no
git tags at all, and `kafka`, `outbox` and `inbox` each carry
`replace github.com/zuksmaq/messaging => ../`, which a consuming build
ignores — so the `github.com/zuksmaq/messaging v0.0.0` they require
resolves to a version that does not exist.

The spec's premise is that services import this library instead of
hand-rolling the plumbing. Until this ticket lands, they cannot.

**Blocked by:** nothing. Tickets 01–10 are all done and the library is
feature-complete against the spec.

**Status:** ready-for-agent

- [ ] The `replace github.com/zuksmaq/messaging => ../` directive is
      gone from `kafka/go.mod`, `outbox/go.mod` and `inbox/go.mod`, and
      each requires a real published version of the root module instead
      of `v0.0.0`.
- [ ] Local development still works with no replace directives —
      `go.work` is what resolves the sibling modules, and both the
      workspace build and the whole test suite pass unchanged.
- [ ] CI's `GOWORK=off` per-module loop passes, which it can only do
      once the root module's tag is fetchable: the requires it resolves
      are no longer redirected to the local directory.
- [ ] Tags are pushed for the root module and for each published
      submodule, using the path-prefixed form a multi-module repository
      needs (`v0.1.0`, `kafka/v0.1.0`, `outbox/v0.1.0`,
      `inbox/v0.1.0`).
- [ ] Proven from outside the repo: in a temporary directory that is
      not inside this checkout and not covered by `go.work`,
      `go mod init` plus `go get github.com/zuksmaq/messaging/kafka@<tag>`
      plus a build of a file importing `kafka/producer`, `outbox` and
      `inbox` all succeed. This is the only acceptance test that
      actually measures the ticket.
- [ ] `README.md`'s reference to `messaging-handoff.md` is fixed or
      dropped — the file does not exist in the repo, and a release
      should not ship a README pointing at a missing design record.

## Notes

**The tagging order is forced, and getting it wrong wastes a tag.**
Tags are immutable in the module proxy, so this cannot be done by
trial and error:

1. Tag and push the root module (`v0.1.0`). It has no dependencies of
   its own — `go.mod` is three lines — so nothing gates it.
2. Point `kafka`, `outbox` and `inbox` at that version, remove their
   replace directives, and run `GOWORK=off go mod tidy` in each.
3. Only now can the per-module `GOWORK=off` build be trusted, because
   only now does the required root version resolve to something real.
4. Tag and push the three submodules.

The dependency graph makes this two layers rather than four: none of
`kafka`, `outbox` or `inbox` requires either of the others — they all
require only the root module. One root tag unlocks all three.

**The `integration` module is not published and should keep its replace
directives.** It exists only to host ticket 10's end-to-end tests, it
imports all four other modules, and its `GOWORK=off` build in CI would
otherwise need every sibling tagged before it could run. Leaving its
four replaces in place keeps it working off the local tree, which is
the only place it is ever built. Worth stating explicitly in the ticket
that lands this, or someone will "consistently" strip them later.

**`v0.1.0` or `v1.0.0` is the one open decision, and it should be made
deliberately rather than defaulted into.** The spec calls this the "v1"
build, but `v1.0.0` in Go is a compatibility promise: every exported
name in five modules becomes something that cannot change without a
`/v2` import path. Against that, `v0.x` lets the API move while the
first real consumers shake it out, at the cost of consumers pinning a
version Go treats as unstable.

Recommendation: `v0.1.0`. There are no external consumers yet, so
nothing is gained by the promise, and ADR-level decisions like the
`Dialect` interface shape and `MarkProcessed`'s `(bool, error)` signature
have never been exercised by a service that did not come with its own
test. Revisit `v1.0.0` after the first one or two integrations.

**Nothing here needs `go work sync`.** Ticket 09 recorded why: run from
the workspace it pushes workspace-wide maximum versions into individual
`go.mod` files and churns unrelated modules. Per-module
`GOWORK=off go mod tidy` is the tool for every step above.
