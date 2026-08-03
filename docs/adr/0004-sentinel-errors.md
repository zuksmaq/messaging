# 4. Sentinel errors + wrapping, not an exception-style type hierarchy

## Status

Accepted

## Context

Callers need to distinguish error categories (serialization failure,
schema-registry-required, broker/transport errors, configuration
errors) without depending on broker-specific error types leaking
through the abstraction.

## Decision

Use package-level sentinel errors (e.g. `var ErrSerialization =
errors.New(...)`), always wrapped with context via
`fmt.Errorf("...: %w", err)`. Callers branch with `errors.Is`/
`errors.As`. No custom exception-style type hierarchy.

## Consequences

- Sentinels exist only for cases callers actually need to branch on.
- Structured context (topic, partition, offset) goes in the wrapped
  error message, not as fields on a typed error struct.
