package producer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestCloseReportsUnflushedMessages covers shutdown when buffered
// messages cannot be acknowledged inside the flush timeout: the
// residual must be logged and counted, never silently dropped, and
// Close must not hang.
//
// The producer points at a port with nothing listening, so messages
// stay queued in librdkafka for the whole (short) flush window. No
// broker is required.
func TestCloseReportsUnflushedMessages(t *testing.T) {
	t.Parallel()

	const queued = 5

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	reader, meter := testMeter()

	p, err := New[string, []byte](Config{
		BootstrapServers: "127.0.0.1:1",
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatBytes,
		FlushTimeout:     50 * time.Millisecond,
	}, kafka.WithLogger(logger), kafka.WithMetrics(meter))
	if err != nil {
		t.Fatalf("New = %v", err)
	}

	// Enqueue on the underlying client so the messages buffer without
	// this test blocking on acknowledgements that will never arrive.
	topic := "unreachable"
	for range queued {
		msg := &ckafka.Message{
			TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
			Value:          []byte("payload"),
		}
		if err := p.client.Produce(msg, nil); err != nil {
			t.Fatalf("enqueuing message: %v", err)
		}
	}

	start := time.Now()
	err = p.Close(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Close() = nil, want an error reporting the unflushed residual")
	}
	if !errors.Is(err, messaging.ErrBroker) {
		t.Errorf("Close() error = %v, want it to wrap ErrBroker", err)
	}
	if elapsed > 10*time.Second {
		t.Errorf("Close() took %s — it hung rather than honoring the flush timeout", elapsed)
	}

	if got := logBuf.String(); !strings.Contains(got, "un-acknowledged") {
		t.Errorf("log output = %q, want it to mention the un-acknowledged residual", got)
	}

	if got := counterValue(t, reader, "messaging.producer.unflushed"); got != queued {
		t.Errorf("messaging.producer.unflushed = %d, want %d", got, queued)
	}
}

// counterValue collects reader and returns the sum recorded against
// the named Int64 counter.
func counterValue(t *testing.T, reader interface {
	Collect(context.Context, *metricdata.ResourceMetrics) error
}, name string) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q has data type %T, want Sum[int64]", name, m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	t.Fatalf("metric %q was never recorded", name)
	return 0
}
