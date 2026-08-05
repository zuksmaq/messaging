package producer

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

// Producer satisfies the broker-agnostic contract.
var _ messaging.Producer[string, []byte] = (*Producer[string, []byte])(nil)

// Producer publishes messages to Kafka, serializing keys and values in
// the formats named by its Config. It implements
// messaging.Producer[K, V].
//
// Produce awaits the broker acknowledgement before returning; the
// underlying client is configured idempotent (acks=all), so a
// successful Produce reports messaging.Persisted.
type Producer[K, V any] struct {
	client *ckafka.Producer
	cfg    Config
	logger *slog.Logger

	// drained closes once the background events reader has exited.
	drained chan struct{}

	// closeOnce guards against re-entering the underlying client's
	// flush/close on a second Close call; closeErr caches the result
	// of the first call so every caller sees the same outcome.
	closeOnce sync.Once
	closeErr  error

	keySer   kafka.Serializer[K]
	valueSer kafka.Serializer[V]

	// registry is nil unless a configured format needs one.
	registry *kafka.SchemaRegistry

	produced  metric.Int64Counter
	failed    metric.Int64Counter
	unflushed metric.Int64Counter
	latency   metric.Float64Histogram
}

// New builds a Producer from cfg. It returns an error wrapping
// messaging.ErrInvalidConfig if cfg is invalid or if K or V cannot be
// encoded in the configured format.
func New[K, V any](cfg Config, opts ...kafka.Option) (*Producer[K, V], error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	registry, err := kafka.NewSchemaRegistry(cfg.SchemaRegistry)
	if err != nil {
		return nil, err
	}

	keySer, err := kafka.SerializerFor[K](cfg.KeyFormat, kafka.KeyPart, registry)
	if err != nil {
		_ = registry.Close()
		return nil, fmt.Errorf("key format: %w", err)
	}
	valueSer, err := kafka.SerializerFor[V](cfg.ValueFormat, kafka.ValuePart, registry)
	if err != nil {
		_ = registry.Close()
		return nil, fmt.Errorf("value format: %w", err)
	}

	o := kafka.ResolveOptions(opts)

	client, err := ckafka.NewProducer(clientConfig(cfg))
	if err != nil {
		_ = registry.Close()
		return nil, fmt.Errorf("%w: creating producer: %v", messaging.ErrBroker, err)
	}

	p := &Producer[K, V]{
		client:   client,
		cfg:      cfg,
		logger:   o.Logger,
		drained:  make(chan struct{}),
		keySer:   keySer,
		valueSer: valueSer,
		registry: registry,
	}
	if err := p.initMetrics(o.Meter); err != nil {
		client.Close()
		_ = registry.Close()
		return nil, err
	}
	go p.drainEvents()
	return p, nil
}

// drainEvents consumes the client's event channel until Close shuts the
// client down. Produce passes a per-call delivery channel, so what
// arrives here is broker-level errors and statistics; left unread they
// accumulate and inflate the queue length Flush reports.
//
// Transient errors are logged at debug: librdkafka re-reports them on
// every retry, and the caller already learns about them through the
// error Produce returns. A fatal error means the client is permanently
// unusable, so it is logged at error level.
func (p *Producer[K, V]) drainEvents() {
	defer close(p.drained)

	for ev := range p.client.Events() {
		err, ok := ev.(ckafka.Error)
		if !ok {
			continue
		}
		if err.IsFatal() {
			p.logger.Error("kafka producer client error is fatal", slog.String("error", err.Error()))
			continue
		}
		p.logger.Debug("kafka producer client error", slog.String("error", err.Error()))
	}
}

func (p *Producer[K, V]) initMetrics(m metric.Meter) error {
	var err error
	if p.produced, err = m.Int64Counter("messaging.producer.produced",
		metric.WithDescription("messages acknowledged by the broker")); err != nil {
		return fmt.Errorf("creating produced counter: %w", err)
	}
	if p.failed, err = m.Int64Counter("messaging.producer.failed",
		metric.WithDescription("messages that failed to produce")); err != nil {
		return fmt.Errorf("creating failed counter: %w", err)
	}
	if p.unflushed, err = m.Int64Counter("messaging.producer.unflushed",
		metric.WithDescription("messages still un-acknowledged when the producer closed")); err != nil {
		return fmt.Errorf("creating unflushed counter: %w", err)
	}
	if p.latency, err = m.Float64Histogram("messaging.producer.produce.duration",
		metric.WithDescription("time from Produce call to broker acknowledgement"),
		metric.WithUnit("s")); err != nil {
		return fmt.Errorf("creating latency histogram: %w", err)
	}
	return nil
}

// Produce serializes key and value, publishes to topic, and blocks
// until the broker acknowledges the message or ctx is done.
//
// The returned ProducedMessage carries a DeliveryStatus even when the
// error is non-nil: a timeout reports messaging.PossiblyPersisted,
// because the broker may have committed the write before the
// acknowledgement was lost.
func (p *Producer[K, V]) Produce(ctx context.Context, topic string, key K, value V, headers map[string][]byte) (messaging.ProducedMessage, error) {
	keyBytes, err := p.keySer.Serialize(topic, key)
	if err != nil {
		p.failed.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic)))
		return messaging.ProducedMessage{Topic: topic}, fmt.Errorf("serializing key for topic %q: %w", topic, err)
	}
	valueBytes, err := p.valueSer.Serialize(topic, value)
	if err != nil {
		p.failed.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic)))
		return messaging.ProducedMessage{Topic: topic}, fmt.Errorf("serializing value for topic %q: %w", topic, err)
	}

	ctx, cancel := context.WithTimeout(ctx, p.cfg.ProduceTimeout)
	defer cancel()

	msg := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
		Key:            keyBytes,
		Value:          valueBytes,
		Headers:        toHeaders(headers),
	}

	// A per-call delivery channel keeps concurrent Produce calls from
	// observing each other's acknowledgements.
	deliveryChan := make(chan ckafka.Event, 1)
	start := time.Now()
	if err := p.client.Produce(msg, deliveryChan); err != nil {
		p.failed.Add(ctx, 1, metric.WithAttributes(attribute.String("topic", topic)))
		return messaging.ProducedMessage{Topic: topic, Status: statusFor(err)},
			fmt.Errorf("%w: enqueuing message for topic %q: %v", messaging.ErrBroker, topic, err)
	}

	select {
	case ev := <-deliveryChan:
		return p.report(ctx, topic, ev, start)
	case <-ctx.Done():
		// The message stays queued in librdkafka and may still be
		// delivered, so the caller must not assume it was lost.
		p.failed.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String("topic", topic)))
		return messaging.ProducedMessage{Topic: topic, Status: messaging.PossiblyPersisted},
			fmt.Errorf("%w: awaiting acknowledgement for topic %q: %v", messaging.ErrBroker, topic, ctx.Err())
	}
}

func (p *Producer[K, V]) report(ctx context.Context, topic string, ev ckafka.Event, start time.Time) (messaging.ProducedMessage, error) {
	elapsed := time.Since(start).Seconds()
	attrs := metric.WithAttributes(attribute.String("topic", topic))

	m, ok := ev.(*ckafka.Message)
	if !ok {
		p.failed.Add(ctx, 1, attrs)
		return messaging.ProducedMessage{Topic: topic},
			fmt.Errorf("%w: unexpected delivery event %T for topic %q", messaging.ErrBroker, ev, topic)
	}

	out := messaging.ProducedMessage{
		Topic:     *m.TopicPartition.Topic,
		Partition: m.TopicPartition.Partition,
		Offset:    int64(m.TopicPartition.Offset),
	}

	if err := m.TopicPartition.Error; err != nil {
		out.Status = statusFor(err)
		p.failed.Add(ctx, 1, attrs)
		return out, fmt.Errorf("%w: delivering message to topic %q: %v", messaging.ErrBroker, topic, err)
	}

	out.Status = messaging.Persisted
	p.produced.Add(ctx, 1, attrs)
	p.latency.Record(ctx, elapsed, attrs)
	return out, nil
}

// statusFor maps a client error onto a DeliveryStatus. Timeouts are
// PossiblyPersisted: the write may have landed and only the
// acknowledgement was lost. Everything else is treated as never
// written.
func statusFor(err error) messaging.DeliveryStatus {
	var ke ckafka.Error
	if errors.As(err, &ke) {
		switch ke.Code() {
		case ckafka.ErrMsgTimedOut, ckafka.ErrRequestTimedOut, ckafka.ErrTimedOut, ckafka.ErrTransport:
			return messaging.PossiblyPersisted
		}
	}
	return messaging.NotPersisted
}

// Close flushes buffered messages and releases the client. It waits up
// to the configured FlushTimeout (or ctx's deadline, whichever is
// sooner); any residual un-acknowledged messages are logged and
// counted rather than silently dropped.
//
// Close is idempotent: a second call (concurrent or sequential) never
// re-invokes the underlying client's flush/close — doing so would
// operate on an already-destroyed handle — and instead returns the
// first call's result.
func (p *Producer[K, V]) Close(ctx context.Context) error {
	p.closeOnce.Do(func() { p.closeErr = p.closeClient(ctx) })
	return p.closeErr
}

func (p *Producer[K, V]) closeClient(ctx context.Context) error {
	timeout := kafka.ClampTimeout(ctx, p.cfg.FlushTimeout)

	remaining := p.client.Flush(int(timeout.Milliseconds()))
	p.client.Close()
	<-p.drained

	// A registry failure is reported only when nothing worse happened:
	// un-acknowledged messages are the more urgent news.
	registryErr := p.registry.Close()

	if remaining > 0 {
		p.unflushed.Add(ctx, int64(remaining))
		p.logger.WarnContext(ctx, "producer closed with un-acknowledged messages",
			slog.Int("unflushed", remaining),
			slog.Duration("flush_timeout", timeout))
		return fmt.Errorf("%w: %d message(s) un-acknowledged after %s flush", messaging.ErrBroker, remaining, timeout)
	}
	return registryErr
}

// ReadyCheck reports whether the producer can reach the cluster.
func (p *Producer[K, V]) ReadyCheck(ctx context.Context) error {
	timeout := kafka.ClampTimeout(ctx, p.cfg.ProduceTimeout)
	if timeout <= 0 {
		return fmt.Errorf("%w: no time left for readiness check", messaging.ErrBroker)
	}
	if _, err := p.client.GetMetadata(nil, false, int(timeout.Milliseconds())); err != nil {
		return fmt.Errorf("%w: fetching cluster metadata: %v", messaging.ErrBroker, err)
	}
	return nil
}

func toHeaders(h map[string][]byte) []ckafka.Header {
	if len(h) == 0 {
		return nil
	}
	out := make([]ckafka.Header, 0, len(h))
	for k, v := range h {
		out = append(out, ckafka.Header{Key: k, Value: v})
	}
	return out
}
