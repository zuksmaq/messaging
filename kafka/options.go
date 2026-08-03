package kafka

import (
	"log/slog"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// options holds the optional cross-cutting concerns callers supply via
// functional options. Mandatory settings live on ProducerConfig.
type options struct {
	logger *slog.Logger
	meter  metric.Meter
}

// Option configures optional producer behavior.
type Option func(*options)

// WithLogger sets the logger used for operational events such as an
// incomplete flush at shutdown. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithMetrics sets the OpenTelemetry meter instruments are created
// against. Defaults to a no-op meter.
func WithMetrics(m metric.Meter) Option {
	return func(o *options) {
		if m != nil {
			o.meter = m
		}
	}
}

func resolveOptions(opts []Option) options {
	o := options{
		logger: slog.Default(),
		meter:  noop.Meter{},
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
