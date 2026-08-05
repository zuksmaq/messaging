# messaging

Kafka messaging plus transactional outbox and inbox (idempotent-consumer)
patterns, as a single Go module.

```bash
go get github.com/zuksmaq/messaging
```

## Packages

| Package | Import path | Holds |
|---|---|---|
| root | `github.com/zuksmaq/messaging` | broker-agnostic contracts and sentinel errors; no third-party imports |
| kafka | `github.com/zuksmaq/messaging/kafka` | wire formats and connection settings both sides share |
| kafka/producer | `…/kafka/producer` | `Config` + `New` for publishing |
| kafka/consumer | `…/kafka/consumer` | `Config` + `New`, plus the hosted `Runner` loop |
| outbox | `github.com/zuksmaq/messaging/outbox` | transactional outbox and its relay |
| inbox | `github.com/zuksmaq/messaging/inbox` | idempotent-consumer inbox |
| integration | `github.com/zuksmaq/messaging/integration` | end-to-end tests only, no library code |

`outbox`/`inbox` stay database-agnostic; the dialect-specific SQL lives
in their `postgres` and `sqlserver` sub-packages (ADR 0005). The kafka
split into `producer`/`consumer` is ADR 0008.

The whole repo versions as one module, so there is a single tag per
release and no cross-package version skew (ADR 0009).

See `messaging-handoff.md` for the full design record: decisions,
invariants, open items, and rejected alternatives.

## Development

```bash
go build ./...
go test ./...

# Tests that stand up real brokers/databases via testcontainers:
go test -tags integration -timeout 20m ./...
```
