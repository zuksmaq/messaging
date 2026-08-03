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
| integration | `github.com/zuksmaq/messaging/integration` | all four |

The kafka module is split into packages: `kafka` holds the wire formats
and connection settings both sides share, `kafka/producer` and
`kafka/consumer` each own a `Config` and a `New` constructor (ADR 0008).

The integration module ships no library code: it exists so the
end-to-end test of outbox → Kafka → Runner → inbox can depend on every
module at once without any of them depending on the others.

See `messaging-handoff.md` for the full design record: decisions,
invariants, open items, and rejected alternatives.

## Development

```bash
go work use .   # workspace already committed as go.work
```

CI builds each module with `GOWORK=off` to catch un-tagged local-only
changes before release.
