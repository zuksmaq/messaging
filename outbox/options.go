package outbox

import (
	"log/slog"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Options holds the optional cross-cutting concerns callers supply via
// functional options. Mandatory settings live on RelayConfig.
type Options struct {
	Logger *slog.Logger
	Meter  metric.Meter
}

// Option configures optional Relay behavior.
type Option func(*Options)

// WithLogger sets the logger used for operational events such as a row
// the broker would not confirm. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *Options) {
		if l != nil {
			o.Logger = l
		}
	}
}

// WithMetrics sets the OpenTelemetry meter instruments are created
// against. Defaults to a no-op meter.
func WithMetrics(m metric.Meter) Option {
	return func(o *Options) {
		if m != nil {
			o.Meter = m
		}
	}
}

// resolveOptions applies opts over the defaults.
func resolveOptions(opts []Option) Options {
	o := Options{
		Logger: slog.Default(),
		Meter:  noop.Meter{},
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
