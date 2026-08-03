# 6. Observability via log/slog + OpenTelemetry metrics

## Status

Accepted

## Context

Producers, consumers, and the outbox relay need structured logging,
metrics (produce/consume counters, latency histograms, shutdown-drop
counts), and a readiness check. Several logging/metrics ecosystems
(slog, zap/zerolog + Prometheus client, OpenTelemetry) were
candidates.

## Decision

- Logging: `log/slog` (stdlib since Go 1.21). Callers pass an
  `*slog.Logger`; no logging framework dependency is forced on them.
- Metrics: the OpenTelemetry metrics API
  (`go.opentelemetry.io/otel/metric`) for instruments. Vendor
  neutral — works with whatever exporter (Prometheus, Datadog, etc.)
  the consuming service already wires up.
- Health/readiness: a plain `ReadyCheck(ctx) error` method on
  producer/consumer, not a framework-specific health-check type.

## Consequences

- No Prometheus client dependency directly in this module — services
  that want Prometheus wire an OTel Prometheus exporter themselves.
- Instruments are created against the OTel `Meter` the caller
  supplies (or a no-op default).
