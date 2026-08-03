# 3. Generic Producer/Consumer API, config struct + functional options, no builder

## Status

Accepted

## Context

Two open questions when shaping the producer/consumer API: whether to
use Go generics for a typed call site (vs. a `[]byte` core with
free-function helpers), and what pattern replaces a fluent builder —
builder-style chained APIs are not idiomatic Go.

## Decision

- Use generics: `Producer[K, V]` / `Consumer[K, V]`. This is a
  legitimate use of Go generics (a typed API surface), not the
  generic-container/builder pattern Go idioms warn against — callers
  get a single `Produce(ctx, topic, key K, value V)` call site instead
  of composing marshal calls by hand.
- No builder. Construction is `New(cfg, opts...)`: a plain config
  struct (e.g. `ProducerConfig`) with a `Validate() error` method
  called internally by `New`, and functional options (`WithLogger`,
  `WithMetrics`) for optional cross-cutting concerns. Mandatory fields
  stay real struct fields, not options — avoids the "forgot to set a
  required option" failure mode of pure functional-options APIs.

## Consequences

- `ProducerConfig`/`ConsumerConfig` carry bootstrap servers, acks,
  security, schema registry, key/value format, etc., validated via a
  plain `Validate()` method.
- No fluent builder type exists or is planned for this module.
