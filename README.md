# messaging

Kafka messaging plus transactional outbox and inbox (idempotent-consumer)
patterns, structured as multiple Go modules in one repository.

## Modules

| Module | Path | Depends on |
|---|---|---|
| root | `github.com/zuksmaq/messaging` | — |
| kafka | `github.com/zuksmaq/messaging/kafka` | root |
| outbox | `github.com/zuksmaq/messaging/outbox` | root |
| inbox | `github.com/zuksmaq/messaging/inbox` | root |

The kafka module is split into packages: `kafka` holds the wire formats
and connection settings both sides share, `kafka/producer` and
`kafka/consumer` each own a `Config` and a `New` constructor (ADR 0008).

See `messaging-handoff.md` for the full design record: decisions,
invariants, open items, and rejected alternatives.

## Development

```bash
go work use .   # workspace already committed as go.work
```

CI builds each module with `GOWORK=off` to catch un-tagged local-only
changes before release.
