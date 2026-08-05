package producer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
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

// TestCloseTwiceIsSafeNoOp covers the ordinary defer-plus-explicit-
// cleanup shutdown idiom: a second Close call must not re-invoke the
// underlying client's flush/close, panic, or hang. Queuing messages
// that can never be acknowledged and inspecting the unflushed counter
// proves the second call didn't re-run the flush: a re-invocation
// would double-count the residual.
func TestCloseTwiceIsSafeNoOp(t *testing.T) {
	t.Parallel()

	const queued = 3

	reader, meter := testMeter()
	p, err := New[string, []byte](Config{
		BootstrapServers: "127.0.0.1:1",
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatBytes,
		FlushTimeout:     50 * time.Millisecond,
	}, kafka.WithMetrics(meter))
	if err != nil {
		t.Fatalf("New = %v", err)
	}

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

	firstErr := p.Close(context.Background())
	if firstErr == nil {
		t.Fatal("first Close() = nil, want an error reporting the unflushed residual")
	}

	start := time.Now()
	secondErr := p.Close(context.Background())
	elapsed := time.Since(start)

	if secondErr != firstErr {
		t.Errorf("second Close() = %v, want the cached result from the first call (%v)", secondErr, firstErr)
	}
	if elapsed > time.Second {
		t.Errorf("second Close() took %s, want near-immediate", elapsed)
	}
	if got := counterValue(t, reader, "messaging.producer.unflushed"); got != queued {
		t.Errorf("messaging.producer.unflushed = %d after two Close() calls, want %d (second call must not re-flush)", got, queued)
	}
}

// TestCloseConcurrentIsSafeNoOp covers concurrent Close calls racing
// against each other, as opposed to TestCloseTwiceIsSafeNoOp's
// sequential case. Run with -race, this proves the sync.Once guard
// serializes access to the underlying client rather than merely
// happening to avoid a crash sequentially.
func TestCloseConcurrentIsSafeNoOp(t *testing.T) {
	t.Parallel()

	const queued = 3
	const callers = 10

	reader, meter := testMeter()
	p, err := New[string, []byte](Config{
		BootstrapServers: "127.0.0.1:1",
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatBytes,
		FlushTimeout:     50 * time.Millisecond,
	}, kafka.WithMetrics(meter))
	if err != nil {
		t.Fatalf("New = %v", err)
	}

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

	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = p.Close(context.Background())
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != errs[0] {
			t.Errorf("Close() call %d = %v, want the same cached result as call 0 (%v)", i, err, errs[0])
		}
	}
	if got := counterValue(t, reader, "messaging.producer.unflushed"); got != queued {
		t.Errorf("messaging.producer.unflushed = %d after %d concurrent Close() calls, want %d (only one must actually flush)", got, callers, queued)
	}
}

// TestCloseWithCancelledContextReturnsPromptly covers shutdown under
// an already-cancelled context: Close must notice immediately rather
// than waiting out the full configured FlushTimeout.
func TestCloseWithCancelledContextReturnsPromptly(t *testing.T) {
	t.Parallel()

	p, err := New[string, []byte](Config{
		BootstrapServers: "127.0.0.1:1",
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatBytes,
		FlushTimeout:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_ = p.Close(ctx)
	elapsed := time.Since(start)

	if elapsed > time.Second {
		t.Errorf("Close() with a cancelled context took %s, want near-immediate", elapsed)
	}
}

// TestReadyCheckWithCancelledContextReturnsPromptly covers readiness
// checks under an already-cancelled context: ReadyCheck must notice
// immediately rather than waiting out the full configured
// ProduceTimeout.
func TestReadyCheckWithCancelledContextReturnsPromptly(t *testing.T) {
	t.Parallel()

	p, err := New[string, []byte](Config{
		BootstrapServers: "127.0.0.1:1",
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatBytes,
		ProduceTimeout:   10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	t.Cleanup(func() { _ = p.Close(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err = p.ReadyCheck(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("ReadyCheck() = nil, want an error for a cancelled context")
	}
	if elapsed > time.Second {
		t.Errorf("ReadyCheck() with a cancelled context took %s, want near-immediate", elapsed)
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
