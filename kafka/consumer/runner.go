package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Dead-letter headers a Runner stamps on a forwarded message, alongside
// the headers the original carried, so the failure can be triaged from
// the dead-letter topic alone.
const (
	// DeadLetterErrorHeader carries the error that made the message
	// poison.
	DeadLetterErrorHeader = "dead-letter-error"
	// DeadLetterTopicHeader carries the topic the message was read from.
	DeadLetterTopicHeader = "dead-letter-source-topic"
	// DeadLetterPartitionHeader carries the partition it was read from.
	DeadLetterPartitionHeader = "dead-letter-source-partition"
	// DeadLetterOffsetHeader carries the offset it was read at.
	DeadLetterOffsetHeader = "dead-letter-source-offset"
)

// Handler processes one received message. Returning an error triggers the
// Runner's PoisonMessageAction; returning nil commits the offset.
//
// A Handler must be idempotent: delivery is at-least-once, so it can see
// the same message twice.
type Handler[K, V any] func(ctx context.Context, msg messaging.ReceivedMessage[K, V]) error

// PoisonMessageAction is what a Runner does with a message that could not
// be deserialized or whose Handler returned an error. Every action logs
// and counts the outcome — none is silent.
type PoisonMessageAction string

const (
	// Skip commits past the message, dropping it. Fastest to recover
	// from, but the message is gone.
	Skip PoisonMessageAction = "skip"
	// DeadLetter forwards the original bytes to the dead-letter topic and
	// commits only once that publish is confirmed messaging.Persisted. A
	// failed publish leaves the offset uncommitted and stops Run.
	DeadLetter PoisonMessageAction = "dead_letter"
	// Halt stops Run without committing, so a restart re-delivers the
	// same message and a human can intervene.
	Halt PoisonMessageAction = "halt"
)

// RunnerConfig configures a Runner's poison-message policy. Its zero
// value selects Halt: nothing is dropped until the caller says so.
type RunnerConfig struct {
	// PoisonAction defaults to Halt.
	PoisonAction PoisonMessageAction

	// DeadLetterTopic is the topic poison messages are forwarded to.
	// Required when PoisonAction is DeadLetter, ignored otherwise.
	DeadLetterTopic string

	// DeadLetterProducer publishes to DeadLetterTopic. It is typed in
	// bytes because a message that failed to deserialize has no decoded
	// key or value left to publish — the original RawKey/RawValue are
	// forwarded as they arrived. Required when PoisonAction is
	// DeadLetter, ignored otherwise.
	DeadLetterProducer messaging.Producer[[]byte, []byte]
}

// Validate reports whether the policy is self-consistent. NewRunner calls
// it and refuses to construct an invalid Runner.
func (c RunnerConfig) Validate() error {
	switch c.PoisonAction {
	case "", Skip, Halt:
		return nil
	case DeadLetter:
		if c.DeadLetterTopic == "" {
			return fmt.Errorf("%w: dead-letter topic is required for the %s action",
				messaging.ErrInvalidConfig, DeadLetter)
		}
		if c.DeadLetterProducer == nil {
			return fmt.Errorf("%w: dead-letter producer is required for the %s action",
				messaging.ErrInvalidConfig, DeadLetter)
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown poison message action %q",
			messaging.ErrInvalidConfig, c.PoisonAction)
	}
}

// withDefaults returns a copy with zero-valued optional fields replaced
// by their defaults.
func (c RunnerConfig) withDefaults() RunnerConfig {
	if c.PoisonAction == "" {
		c.PoisonAction = Halt
	}
	return c
}

// Runner is the hosted consumer loop: it drives a Handler over the
// messages a Consumer delivers, committing each one the Handler accepts
// and applying the configured PoisonMessageAction to the ones it doesn't.
//
// It is exposed as a blocking Run(ctx) error rather than a
// framework-managed service type; the caller decides whether to await it
// or start it with go r.Run(ctx).
type Runner[K, V any] struct {
	consumer messaging.Consumer[K, V]
	handler  Handler[K, V]
	cfg      RunnerConfig
	logger   *slog.Logger

	handled  metric.Int64Counter
	poisoned metric.Int64Counter
}

// NewRunner builds a Runner driving handler over the messages c
// delivers. It returns an error wrapping messaging.ErrInvalidConfig if c
// or handler is nil or if cfg is invalid.
func NewRunner[K, V any](c messaging.Consumer[K, V], handler Handler[K, V], cfg RunnerConfig, opts ...kafka.Option) (*Runner[K, V], error) {
	if c == nil {
		return nil, fmt.Errorf("%w: a consumer is required", messaging.ErrInvalidConfig)
	}
	if handler == nil {
		return nil, fmt.Errorf("%w: a handler is required", messaging.ErrInvalidConfig)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	o := kafka.ResolveOptions(opts)
	r := &Runner[K, V]{
		consumer: c,
		handler:  handler,
		cfg:      cfg.withDefaults(),
		logger:   o.Logger,
	}
	if err := r.initMetrics(o.Meter); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Runner[K, V]) initMetrics(m metric.Meter) error {
	var err error
	if r.handled, err = m.Int64Counter("messaging.runner.handled",
		metric.WithDescription("messages handled and committed")); err != nil {
		return fmt.Errorf("creating handled counter: %w", err)
	}
	if r.poisoned, err = m.Int64Counter("messaging.runner.poisoned",
		metric.WithDescription("poison messages, by the action taken")); err != nil {
		return fmt.Errorf("creating poisoned counter: %w", err)
	}
	return nil
}

// Run consumes, handles and commits in a loop until ctx is cancelled, at
// which point it returns nil, or until an error the policy cannot absorb
// stops it. The offset never advances before the Handler returns without
// error.
func (r *Runner[K, V]) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		msg, err := r.consumer.Consume(ctx)
		switch {
		case err == nil:
		case ctx.Err() != nil:
			// A cancelled context ends the loop: the message, if any, is
			// left uncommitted and re-delivered after a restart.
			return nil
		case errors.Is(err, messaging.ErrDeserialization):
			if err := r.poison(ctx, msg, err); err != nil {
				return err
			}
			continue
		default:
			return fmt.Errorf("consuming: %w", err)
		}

		if err := r.callHandler(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if err := r.poison(ctx, msg, err); err != nil {
				return err
			}
			continue
		}

		if err := r.commit(ctx, msg); err != nil {
			return err
		}
		r.handled.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", msg.Topic)))
	}
}

// callHandler runs the Handler and recovers a panic into an error
// carrying the recovered value, so a panicking Handler is routed
// through the same PoisonMessageAction handling as one that returns an
// error, instead of crashing the process. The stack trace goes to the
// log only — it's diagnostic noise the caller's Handler never intended
// to publish, and doesn't belong riding along in a dead-letter header.
// A panic value that is itself an error is wrapped with %w so callers
// can still errors.Is/As through it.
func (r *Runner[K, V]) callHandler(ctx context.Context, msg messaging.ReceivedMessage[K, V]) (err error) {
	defer func() {
		rec := recover()
		if rec == nil {
			return
		}
		r.logger.ErrorContext(ctx, "handler panicked",
			slog.Any("panic", rec),
			slog.String("stack", string(debug.Stack())))

		if recErr, ok := rec.(error); ok {
			err = fmt.Errorf("handler panicked: %w", recErr)
		} else {
			err = fmt.Errorf("handler panicked: %v", rec)
		}
	}()
	return r.handler(ctx, msg)
}

// poison logs and counts the failure, then applies the configured action.
// A non-nil return stops Run.
func (r *Runner[K, V]) poison(ctx context.Context, msg messaging.ReceivedMessage[K, V], cause error) error {
	action := r.cfg.PoisonAction

	r.poisoned.Add(ctx, 1, metric.WithAttributes(
		attribute.String("topic", msg.Topic),
		attribute.String("action", string(action)),
	))
	r.logger.WarnContext(ctx, "poison message",
		slog.String("action", string(action)),
		slog.String("topic", msg.Topic),
		slog.Int("partition", int(msg.Partition)),
		slog.Int64("offset", msg.Offset),
		slog.String("error", cause.Error()),
	)

	switch action {
	case Skip:
		return r.commit(ctx, msg)
	case DeadLetter:
		if err := r.deadLetter(ctx, msg, cause); err != nil {
			return err
		}
		return r.commit(ctx, msg)
	default:
		return fmt.Errorf("halting on poison message %s[%d]@%d: %w",
			msg.Topic, msg.Partition, msg.Offset, cause)
	}
}

// deadLetter forwards the message as it arrived. The offset is left for
// the caller to commit, and only once this returns nil: an unconfirmed
// publish must not advance it.
func (r *Runner[K, V]) deadLetter(ctx context.Context, msg messaging.ReceivedMessage[K, V], cause error) error {
	topic := r.cfg.DeadLetterTopic

	headers := make(map[string][]byte, len(msg.Headers)+4)
	for k, v := range msg.Headers {
		headers[k] = v
	}
	headers[DeadLetterErrorHeader] = []byte(cause.Error())
	headers[DeadLetterTopicHeader] = []byte(msg.Topic)
	headers[DeadLetterPartitionHeader] = []byte(strconv.Itoa(int(msg.Partition)))
	headers[DeadLetterOffsetHeader] = []byte(strconv.FormatInt(msg.Offset, 10))

	out, err := r.cfg.DeadLetterProducer.Produce(ctx, topic, msg.RawKey, msg.RawValue, headers)
	if err != nil {
		return fmt.Errorf("dead-lettering %s[%d]@%d to %s: %w",
			msg.Topic, msg.Partition, msg.Offset, topic, err)
	}
	if out.Status != messaging.Persisted {
		return fmt.Errorf("dead-lettering %s[%d]@%d to %s: delivery status %s, want %s",
			msg.Topic, msg.Partition, msg.Offset, topic, out.Status, messaging.Persisted)
	}
	return nil
}

func (r *Runner[K, V]) commit(ctx context.Context, msg messaging.ReceivedMessage[K, V]) error {
	if err := r.consumer.Commit(ctx, msg); err != nil {
		return fmt.Errorf("committing %s[%d]@%d: %w", msg.Topic, msg.Partition, msg.Offset, err)
	}
	return nil
}
