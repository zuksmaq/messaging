package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Consumer satisfies the broker-agnostic contract.
var _ messaging.Consumer[string, []byte] = (*Consumer[string, []byte])(nil)

// pollInterval bounds a single poll of the underlying client, so Consume
// notices a cancelled context within roughly that long.
const pollInterval = 250 * time.Millisecond

// Consumer reads messages from Kafka, deserializing keys and values in
// the formats named by its Config. It implements
// messaging.Consumer[K, V].
//
// Auto-commit is off by construction: a consumed message is re-delivered
// after a restart unless the caller commits it, which makes delivery
// at-least-once and handlers' idempotency the caller's responsibility.
type Consumer[K, V any] struct {
	client *ckafka.Consumer
	cfg    Config
	logger *slog.Logger

	// pollMu guards against Close tearing down the client while a
	// Consume call is still polling it: Consume holds the lock only for
	// the duration of one poll, and Close takes it (waiting out any
	// in-flight poll) before touching the client, so the two never run
	// concurrently against the same handle. A plain Mutex is enough —
	// Consume is a single-caller blocking loop, not a concurrent reader.
	pollMu sync.Mutex
	closed bool

	keyDe   kafka.Deserializer[K]
	valueDe kafka.Deserializer[V]

	// registry is nil unless a configured format needs one.
	registry *kafka.SchemaRegistry

	consumed metric.Int64Counter
	failed   metric.Int64Counter
}

// New builds a Consumer from cfg and subscribes it to the
// configured topics. It returns an error wrapping
// messaging.ErrInvalidConfig if cfg is invalid or if K or V cannot be
// decoded from the configured format.
func New[K, V any](cfg Config, opts ...kafka.Option) (*Consumer[K, V], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	registry, err := kafka.NewSchemaRegistry(cfg.SchemaRegistry)
	if err != nil {
		return nil, err
	}

	keyDe, err := kafka.DeserializerFor[K](cfg.KeyFormat, kafka.KeyPart, registry)
	if err != nil {
		_ = registry.Close()
		return nil, fmt.Errorf("key format: %w", err)
	}
	valueDe, err := kafka.DeserializerFor[V](cfg.ValueFormat, kafka.ValuePart, registry)
	if err != nil {
		_ = registry.Close()
		return nil, fmt.Errorf("value format: %w", err)
	}

	o := kafka.ResolveOptions(opts)

	client, err := ckafka.NewConsumer(clientConfig(cfg))
	if err != nil {
		_ = registry.Close()
		return nil, fmt.Errorf("%w: creating consumer: %v", messaging.ErrBroker, err)
	}

	c := &Consumer[K, V]{
		client:   client,
		cfg:      cfg,
		logger:   o.Logger,
		keyDe:    keyDe,
		valueDe:  valueDe,
		registry: registry,
	}
	if err := c.initMetrics(o.Meter); err != nil {
		_ = client.Close()
		_ = registry.Close()
		return nil, err
	}
	if err := client.SubscribeTopics(cfg.Topics, nil); err != nil {
		_ = client.Close()
		_ = registry.Close()
		return nil, fmt.Errorf("%w: subscribing to %v: %v", messaging.ErrBroker, cfg.Topics, err)
	}
	return c, nil
}

func (c *Consumer[K, V]) initMetrics(m metric.Meter) error {
	var err error
	if c.consumed, err = m.Int64Counter("messaging.consumer.consumed",
		metric.WithDescription("messages read and deserialized")); err != nil {
		return fmt.Errorf("creating consumed counter: %w", err)
	}
	if c.failed, err = m.Int64Counter("messaging.consumer.failed",
		metric.WithDescription("messages that could not be deserialized")); err != nil {
		return fmt.Errorf("creating failed counter: %w", err)
	}
	return nil
}

// Consume blocks until the next message on a subscribed topic is
// available, ctx is done, or the client fails fatally.
//
// A message whose key or value cannot be deserialized is returned with a
// non-nil error wrapping messaging.ErrDeserialization; the returned
// ReceivedMessage still carries the topic, partition and offset plus the
// undecoded RawKey/RawValue, so a caller applying a poison-message
// policy can commit past it or dead-letter the original bytes.
func (c *Consumer[K, V]) Consume(ctx context.Context) (messaging.ReceivedMessage[K, V], error) {
	var zero messaging.ReceivedMessage[K, V]
	for {
		if err := ctx.Err(); err != nil {
			return zero, fmt.Errorf("awaiting message: %w", err)
		}

		ev, err := c.poll()
		if err != nil {
			return zero, err
		}
		switch ev := ev.(type) {
		case *ckafka.Message:
			return c.decode(ctx, ev)
		case ckafka.Error:
			// Transient errors (a broker briefly down, a rebalance in
			// progress) resolve themselves and librdkafka re-reports
			// them; only a fatal error means the client is unusable.
			if ev.IsFatal() {
				return zero, fmt.Errorf("%w: consumer client error is fatal: %v", messaging.ErrBroker, ev)
			}
			c.logger.DebugContext(ctx, "kafka consumer client error", slog.String("error", ev.Error()))
		}
	}
}

// poll runs one poll of the underlying client, holding pollMu so a
// concurrent Close waits for it to finish before tearing the client
// down. It reports an error instead of polling once the consumer is
// closed, rather than touching an already-destroyed client handle.
func (c *Consumer[K, V]) poll() (ckafka.Event, error) {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()

	if c.closed {
		return nil, fmt.Errorf("%w: consumer is closed", messaging.ErrBroker)
	}
	return c.client.Poll(int(pollInterval.Milliseconds())), nil
}

// decode turns a broker message into a ReceivedMessage. A nil key or
// value is left as the zero K/V without invoking the deserializer: a nil
// value is a tombstone, not a malformed encoding.
func (c *Consumer[K, V]) decode(ctx context.Context, m *ckafka.Message) (messaging.ReceivedMessage[K, V], error) {
	topic := *m.TopicPartition.Topic
	attrs := metric.WithAttributes(attribute.String("topic", topic))

	out := messaging.ReceivedMessage[K, V]{
		RawKey:    m.Key,
		RawValue:  m.Value,
		Topic:     topic,
		Partition: m.TopicPartition.Partition,
		Offset:    int64(m.TopicPartition.Offset),
		Timestamp: m.Timestamp,
	}
	out.Headers = fromHeaders(m.Headers)

	if m.Key != nil {
		key, err := c.keyDe.Deserialize(topic, m.Key)
		if err != nil {
			c.failed.Add(ctx, 1, attrs)
			return out, fmt.Errorf("deserializing key from topic %q: %w", topic, err)
		}
		out.Key = key
	}

	if m.Value != nil {
		value, err := c.valueDe.Deserialize(topic, m.Value)
		if err != nil {
			c.failed.Add(ctx, 1, attrs)
			return out, fmt.Errorf("deserializing value from topic %q: %w", topic, err)
		}
		out.Value = value
	}

	c.consumed.Add(ctx, 1, attrs)
	return out, nil
}

// Commit advances the group's offset past msg, so a restart resumes at
// the following message. It commits msg.Offset+1 — Kafka stores the next
// offset to read, not the last one read.
func (c *Consumer[K, V]) Commit(_ context.Context, msg messaging.ReceivedMessage[K, V]) error {
	topic := msg.Topic
	next := ckafka.Offset(msg.Offset + 1)

	if _, err := c.client.CommitOffsets([]ckafka.TopicPartition{{
		Topic:     &topic,
		Partition: msg.Partition,
		Offset:    next,
	}}); err != nil {
		return fmt.Errorf("%w: committing offset %d for %s[%d]: %v",
			messaging.ErrBroker, next, topic, msg.Partition, err)
	}
	return nil
}

// ReadyCheck reports whether the consumer can reach the cluster.
func (c *Consumer[K, V]) ReadyCheck(ctx context.Context) error {
	timeout := kafka.ClampTimeout(ctx, c.cfg.ReadyCheckTimeout)
	if timeout <= 0 {
		return fmt.Errorf("%w: no time left for readiness check", messaging.ErrBroker)
	}
	if _, err := c.client.GetMetadata(nil, false, int(timeout.Milliseconds())); err != nil {
		return fmt.Errorf("%w: fetching cluster metadata: %v", messaging.ErrBroker, err)
	}
	return nil
}

// Close leaves the consumer group and releases the client. Uncommitted
// messages are re-delivered to the group; Close never commits on the
// caller's behalf.
//
// Close waits for any in-flight Consume poll to return before touching
// the client — polling and closing the same handle concurrently is a
// use-after-free in the underlying library. If both the client and the
// schema registry fail to close, both errors are reported via
// errors.Join rather than only one silently winning.
func (c *Consumer[K, V]) Close() error {
	c.markClosed()
	return joinCloseErrors(c.client.Close(), c.registry.Close())
}

// markClosed waits out any poll in flight, then marks the consumer
// closed. Split out from Close so the wait-for-in-flight-poll behavior
// is testable without a real client.
func (c *Consumer[K, V]) markClosed() {
	c.pollMu.Lock()
	c.closed = true
	c.pollMu.Unlock()
}

// joinCloseErrors combines Close's two independent failure sources so
// neither is silently dropped when both occur.
func joinCloseErrors(clientErr, registryErr error) error {
	if clientErr != nil {
		clientErr = fmt.Errorf("%w: closing consumer: %v", messaging.ErrBroker, clientErr)
	}
	return errors.Join(clientErr, registryErr)
}

func fromHeaders(h []ckafka.Header) map[string][]byte {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(h))
	for _, hdr := range h {
		out[hdr.Key] = hdr.Value
	}
	return out
}
