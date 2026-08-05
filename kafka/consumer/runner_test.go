package consumer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	sourceTopic = "orders.v1"
	dlqTopic    = "orders.v1.dlq"
)

var errHandler = errors.New("handler rejected the message")

func TestNewRunnerRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	handler := func(context.Context, messaging.ReceivedMessage[string, string]) error { return nil }

	tests := map[string]struct {
		consumer messaging.Consumer[string, string]
		handler  Handler[string, string]
		cfg      RunnerConfig
	}{
		"nil consumer": {handler: handler},
		"nil handler":  {consumer: &fakeConsumer{}},
		"unknown action": {
			consumer: &fakeConsumer{}, handler: handler,
			cfg: RunnerConfig{PoisonAction: "retry-forever"},
		},
		"dead letter without a topic": {
			consumer: &fakeConsumer{}, handler: handler,
			cfg: RunnerConfig{PoisonAction: DeadLetter, DeadLetterProducer: &fakeProducer{}},
		},
		"dead letter without a producer": {
			consumer: &fakeConsumer{}, handler: handler,
			cfg: RunnerConfig{PoisonAction: DeadLetter, DeadLetterTopic: dlqTopic},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewRunner(tc.consumer, tc.handler, tc.cfg); !errors.Is(err, messaging.ErrInvalidConfig) {
				t.Errorf("NewRunner error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

// TestRunCommitsHandledMessages covers the happy path: every message the
// handler accepts is committed, in order, and counted.
func TestRunCommitsHandledMessages(t *testing.T) {
	t.Parallel()

	c := &fakeConsumer{script: []consumeResult{{msg: received(1)}, {msg: received(2)}}}
	var handled []int64
	reader, meter := testMeter()

	err := run(t, c, func(_ context.Context, msg messaging.ReceivedMessage[string, string]) error {
		handled = append(handled, msg.Offset)
		return nil
	}, RunnerConfig{}, kafka.WithMetrics(meter))

	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if want := []int64{1, 2}; !slices.Equal(handled, want) {
		t.Errorf("handled offsets = %v, want %v", handled, want)
	}
	if want := []int64{1, 2}; !slices.Equal(c.committed, want) {
		t.Errorf("committed offsets = %v, want %v", c.committed, want)
	}
	if got := counterValue(t, reader, "messaging.runner.handled"); got != 2 {
		t.Errorf("messaging.runner.handled = %d, want 2", got)
	}
}

// TestRunCommitsOnlyAfterTheHandlerReturns pins the ordering the ticket
// requires: the offset must not advance while the handler is still
// running.
func TestRunCommitsOnlyAfterTheHandlerReturns(t *testing.T) {
	t.Parallel()

	c := &fakeConsumer{script: []consumeResult{{msg: received(7)}}}

	err := run(t, c, func(context.Context, messaging.ReceivedMessage[string, string]) error {
		if len(c.committed) != 0 {
			t.Errorf("committed offsets = %v while the handler is running, want none", c.committed)
		}
		return nil
	}, RunnerConfig{})

	if err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if want := []int64{7}; !slices.Equal(c.committed, want) {
		t.Errorf("committed offsets = %v, want %v", c.committed, want)
	}
}

// TestRunSkip asserts Skip commits past a poison message — from either a
// handler error or a deserialization failure — logs it, counts it, and
// carries on with the next message.
func TestRunSkip(t *testing.T) {
	t.Parallel()

	tests := map[string]consumeResult{
		"handler error":     {msg: received(1)},
		"deserialize error": {msg: received(1), err: deserializationError()},
	}

	for name, poison := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := &fakeConsumer{script: []consumeResult{poison, {msg: received(2)}}}
			logs, logger := testLogger()
			reader, meter := testMeter()

			err := run(t, c, failOn(1), RunnerConfig{PoisonAction: Skip},
				kafka.WithLogger(logger), kafka.WithMetrics(meter))

			if err != nil {
				t.Fatalf("Run = %v, want nil", err)
			}
			if want := []int64{1, 2}; !slices.Equal(c.committed, want) {
				t.Errorf("committed offsets = %v, want %v — Skip commits past the poison message", c.committed, want)
			}
			assertPoisonReported(t, logs, reader, Skip)
		})
	}
}

// TestRunDeadLetter asserts the original bytes and headers reach the
// dead-letter topic, annotated with the failure, before the offset moves.
func TestRunDeadLetter(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		poison   consumeResult
		wantErr  string
		wantBody string
	}{
		"handler error": {
			poison:   consumeResult{msg: received(1)},
			wantErr:  errHandler.Error(),
			wantBody: "raw-value-1",
		},
		// The decoded value is the zero string here — only the raw bytes
		// can carry the payload to the dead-letter topic.
		"deserialize error": {
			poison:   consumeResult{msg: received(1), err: deserializationError()},
			wantErr:  messaging.ErrDeserialization.Error(),
			wantBody: "raw-value-1",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := &fakeConsumer{script: []consumeResult{tc.poison, {msg: received(2)}}}
			p := &fakeProducer{status: messaging.Persisted}
			logs, logger := testLogger()
			reader, meter := testMeter()

			err := run(t, c, failOn(1), RunnerConfig{
				PoisonAction:       DeadLetter,
				DeadLetterTopic:    dlqTopic,
				DeadLetterProducer: p,
			}, kafka.WithLogger(logger), kafka.WithMetrics(meter))

			if err != nil {
				t.Fatalf("Run = %v, want nil", err)
			}
			if len(p.published) != 1 {
				t.Fatalf("dead-lettered messages = %d, want 1", len(p.published))
			}
			got := p.published[0]
			if got.topic != dlqTopic {
				t.Errorf("dead-letter topic = %q, want %q", got.topic, dlqTopic)
			}
			if string(got.key) != "raw-key-1" || string(got.value) != tc.wantBody {
				t.Errorf("dead-lettered key/value = %q/%q, want raw-key-1/%s", got.key, got.value, tc.wantBody)
			}
			if v := string(got.headers[messaging.EventIDHeader]); v != "evt-1" {
				t.Errorf("dead-lettered %s header = %q, want evt-1", messaging.EventIDHeader, v)
			}
			if v := string(got.headers[DeadLetterErrorHeader]); !strings.Contains(v, tc.wantErr) {
				t.Errorf("dead-lettered %s header = %q, want it to mention %q", DeadLetterErrorHeader, v, tc.wantErr)
			}
			if v := string(got.headers[DeadLetterTopicHeader]); v != sourceTopic {
				t.Errorf("dead-lettered %s header = %q, want %q", DeadLetterTopicHeader, v, sourceTopic)
			}
			if v := string(got.headers[DeadLetterOffsetHeader]); v != "1" {
				t.Errorf("dead-lettered %s header = %q, want 1", DeadLetterOffsetHeader, v)
			}
			if want := []int64{1, 2}; !slices.Equal(c.committed, want) {
				t.Errorf("committed offsets = %v, want %v", c.committed, want)
			}
			assertPoisonReported(t, logs, reader, DeadLetter)
		})
	}
}

// TestRunDeadLetterFailureLeavesOffsetUncommitted covers the guarantee
// that an unconfirmed dead-letter publish never advances the offset: the
// message must be re-delivered rather than lost.
func TestRunDeadLetterFailureLeavesOffsetUncommitted(t *testing.T) {
	t.Parallel()

	tests := map[string]*fakeProducer{
		"publish fails":    {err: errors.New("broker unreachable")},
		"not acknowledged": {status: messaging.NotPersisted},
		"only possibly persisted": {
			status: messaging.PossiblyPersisted,
		},
	}

	for name, p := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := &fakeConsumer{script: []consumeResult{{msg: received(1)}, {msg: received(2)}}}

			err := run(t, c, failOn(1), RunnerConfig{
				PoisonAction:       DeadLetter,
				DeadLetterTopic:    dlqTopic,
				DeadLetterProducer: p,
			})

			if err == nil {
				t.Fatal("Run = nil, want the dead-letter failure to stop the loop")
			}
			if len(c.committed) != 0 {
				t.Errorf("committed offsets = %v, want none after a failed dead-letter publish", c.committed)
			}
		})
	}
}

// TestRunHalt asserts Halt stops the loop without committing, so a
// restart re-delivers the same message, and surfaces the cause.
func TestRunHalt(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		poison   consumeResult
		wantWrap error
	}{
		"handler error":     {poison: consumeResult{msg: received(1)}, wantWrap: errHandler},
		"deserialize error": {poison: consumeResult{msg: received(1), err: deserializationError()}, wantWrap: messaging.ErrDeserialization},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := &fakeConsumer{script: []consumeResult{tc.poison, {msg: received(2)}}}
			logs, logger := testLogger()
			reader, meter := testMeter()

			// The zero RunnerConfig must select Halt: nothing is dropped
			// until the caller asks for it.
			err := run(t, c, failOn(1), RunnerConfig{},
				kafka.WithLogger(logger), kafka.WithMetrics(meter))

			if !errors.Is(err, tc.wantWrap) {
				t.Errorf("Run error = %v, want it to wrap %v", err, tc.wantWrap)
			}
			if len(c.committed) != 0 {
				t.Errorf("committed offsets = %v, want none under Halt", c.committed)
			}
			if c.next != 1 {
				t.Errorf("consumed %d messages, want the loop to stop at the poison message", c.next)
			}
			assertPoisonReported(t, logs, reader, Halt)
		})
	}
}

// TestRunHandlerPanic asserts a Handler panic is recovered and routed
// through the same poison-message handling as a returned error, under
// each configured PoisonMessageAction.
func TestRunHandlerPanic(t *testing.T) {
	t.Parallel()

	t.Run("skip", func(t *testing.T) {
		t.Parallel()

		c := &fakeConsumer{script: []consumeResult{{msg: received(1)}, {msg: received(2)}}}
		logs, logger := testLogger()
		reader, meter := testMeter()

		err := run(t, c, panicOn(1), RunnerConfig{PoisonAction: Skip},
			kafka.WithLogger(logger), kafka.WithMetrics(meter))

		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
		if want := []int64{1, 2}; !slices.Equal(c.committed, want) {
			t.Errorf("committed offsets = %v, want %v — Skip commits past the panicking message", c.committed, want)
		}
		assertPoisonReported(t, logs, reader, Skip)
	})

	t.Run("dead letter", func(t *testing.T) {
		t.Parallel()

		c := &fakeConsumer{script: []consumeResult{{msg: received(1)}, {msg: received(2)}}}
		p := &fakeProducer{status: messaging.Persisted}
		logs, logger := testLogger()
		reader, meter := testMeter()

		err := run(t, c, panicOn(1), RunnerConfig{
			PoisonAction:       DeadLetter,
			DeadLetterTopic:    dlqTopic,
			DeadLetterProducer: p,
		}, kafka.WithLogger(logger), kafka.WithMetrics(meter))

		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
		if len(p.published) != 1 {
			t.Fatalf("dead-lettered messages = %d, want 1", len(p.published))
		}
		if got := p.published[0]; got.topic != dlqTopic {
			t.Errorf("dead-letter topic = %q, want %q", got.topic, dlqTopic)
		}
		if v := string(p.published[0].headers[DeadLetterErrorHeader]); !strings.Contains(v, panicValue) {
			t.Errorf("dead-lettered %s header = %q, want it to mention the recovered panic value %q", DeadLetterErrorHeader, v, panicValue)
		}
		if want := []int64{1, 2}; !slices.Equal(c.committed, want) {
			t.Errorf("committed offsets = %v, want %v", c.committed, want)
		}
		assertPoisonReported(t, logs, reader, DeadLetter)
	})

	t.Run("halt", func(t *testing.T) {
		t.Parallel()

		c := &fakeConsumer{script: []consumeResult{{msg: received(1)}, {msg: received(2)}}}
		logs, logger := testLogger()
		reader, meter := testMeter()

		// The zero RunnerConfig selects Halt.
		err := run(t, c, panicOn(1), RunnerConfig{},
			kafka.WithLogger(logger), kafka.WithMetrics(meter))

		if err == nil {
			t.Fatal("Run = nil, want the recovered panic to stop the loop")
		}
		if !strings.Contains(err.Error(), panicValue) {
			t.Errorf("Run error = %v, want it to mention the recovered panic value %q", err, panicValue)
		}
		if len(c.committed) != 0 {
			t.Errorf("committed offsets = %v, want none under Halt", c.committed)
		}
		if c.next != 1 {
			t.Errorf("consumed %d messages, want the loop to stop at the panicking message", c.next)
		}
		assertPoisonReported(t, logs, reader, Halt)
	})

	t.Run("panic value is an error, preserved through errors.Is", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("panicked with a sentinel")
		c := &fakeConsumer{script: []consumeResult{{msg: received(1)}, {msg: received(2)}}}

		err := run(t, c, func(_ context.Context, msg messaging.ReceivedMessage[string, string]) error {
			if msg.Offset == 1 {
				panic(wantErr)
			}
			return nil
		}, RunnerConfig{})

		if !errors.Is(err, wantErr) {
			t.Errorf("Run error = %v, want it to wrap the panicked error %v", err, wantErr)
		}
	})
}

func TestRunReturnsBrokerAndCommitErrors(t *testing.T) {
	t.Parallel()

	t.Run("consume error", func(t *testing.T) {
		t.Parallel()

		c := &fakeConsumer{script: []consumeResult{{err: fmt.Errorf("%w: client is fatal", messaging.ErrBroker)}}}
		err := run(t, c, failOn(-1), RunnerConfig{})
		if !errors.Is(err, messaging.ErrBroker) {
			t.Errorf("Run error = %v, want it to wrap ErrBroker", err)
		}
	})

	t.Run("commit error", func(t *testing.T) {
		t.Parallel()

		c := &fakeConsumer{
			script:    []consumeResult{{msg: received(1)}},
			commitErr: fmt.Errorf("%w: commit rejected", messaging.ErrBroker),
		}
		err := run(t, c, failOn(-1), RunnerConfig{})
		if !errors.Is(err, messaging.ErrBroker) {
			t.Errorf("Run error = %v, want it to wrap ErrBroker", err)
		}
	})
}

// TestRunStopsOnContextCancellation asserts a cancelled context ends Run
// cleanly, leaving the in-flight message uncommitted.
func TestRunStopsOnContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	c := &fakeConsumer{blockUntilDone: true}
	r, err := NewRunner[string, string](c, failOn(-1), RunnerConfig{})
	if err != nil {
		t.Fatalf("NewRunner = %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run = %v, want nil after cancellation", err)
	}
	if len(c.committed) != 0 {
		t.Errorf("committed offsets = %v, want none after cancellation", c.committed)
	}
}

// run drives a Runner to completion. The fake consumer cancels the
// context once its script is exhausted, which is what ends Run.
func run(t *testing.T, c *fakeConsumer, h Handler[string, string], cfg RunnerConfig, opts ...kafka.Option) error {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.exhausted = cancel

	r, err := NewRunner(messaging.Consumer[string, string](c), h, cfg, opts...)
	if err != nil {
		t.Fatalf("NewRunner = %v", err)
	}
	return r.Run(ctx)
}

// failOn returns a handler that rejects the message at offset bad and
// accepts every other one.
func failOn(bad int64) Handler[string, string] {
	return func(_ context.Context, msg messaging.ReceivedMessage[string, string]) error {
		if msg.Offset == bad {
			return errHandler
		}
		return nil
	}
}

// panicValue is what panicOn's handler panics with.
const panicValue = "handler blew up"

// panicOn returns a handler that panics on the message at offset bad and
// accepts every other one.
func panicOn(bad int64) Handler[string, string] {
	return func(_ context.Context, msg messaging.ReceivedMessage[string, string]) error {
		if msg.Offset == bad {
			panic(panicValue)
		}
		return nil
	}
}

// received builds a message as the ticket-03 consumer delivers one, with
// the raw bytes still attached.
func received(offset int64) messaging.ReceivedMessage[string, string] {
	return messaging.ReceivedMessage[string, string]{
		Message: messaging.Message[string, string]{
			Key:     fmt.Sprintf("key-%d", offset),
			Value:   fmt.Sprintf("value-%d", offset),
			Headers: map[string][]byte{messaging.EventIDHeader: []byte(fmt.Sprintf("evt-%d", offset))},
		},
		RawKey:    []byte(fmt.Sprintf("raw-key-%d", offset)),
		RawValue:  []byte(fmt.Sprintf("raw-value-%d", offset)),
		Topic:     sourceTopic,
		Partition: 0,
		Offset:    offset,
	}
}

// deserializationError is what Consume returns for a message whose value
// could not be decoded.
func deserializationError() error {
	return fmt.Errorf("deserializing value from topic %q: %w", sourceTopic, messaging.ErrDeserialization)
}

// assertPoisonReported checks the outcome was both logged and counted —
// the ticket requires it for every action, not just the loud ones.
func assertPoisonReported(t *testing.T, logs *bytes.Buffer, reader *sdkmetric.ManualReader, action PoisonMessageAction) {
	t.Helper()

	got := logs.String()
	if !strings.Contains(got, "poison message") {
		t.Errorf("log output = %q, want it to report the poison message", got)
	}
	if !strings.Contains(got, string(action)) {
		t.Errorf("log output = %q, want it to name the %s action", got, action)
	}
	if v := counterValue(t, reader, "messaging.runner.poisoned"); v != 1 {
		t.Errorf("messaging.runner.poisoned = %d, want 1", v)
	}
}

type consumeResult struct {
	msg messaging.ReceivedMessage[string, string]
	err error
}

// fakeConsumer replays a scripted sequence of Consume results and records
// what was committed against it.
type fakeConsumer struct {
	script    []consumeResult
	next      int
	committed []int64
	commitErr error

	// exhausted is called when the script runs out, so the Runner's
	// context is cancelled and Run returns.
	exhausted func()

	// blockUntilDone makes Consume wait for cancellation instead of
	// replaying a script.
	blockUntilDone bool
}

func (f *fakeConsumer) Consume(ctx context.Context) (messaging.ReceivedMessage[string, string], error) {
	var zero messaging.ReceivedMessage[string, string]

	if f.blockUntilDone || f.next >= len(f.script) {
		if f.exhausted != nil {
			f.exhausted()
		}
		<-ctx.Done()
		return zero, ctx.Err()
	}

	r := f.script[f.next]
	f.next++
	return r.msg, r.err
}

func (f *fakeConsumer) Commit(_ context.Context, msg messaging.ReceivedMessage[string, string]) error {
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = append(f.committed, msg.Offset)
	return nil
}

type publication struct {
	topic   string
	key     []byte
	value   []byte
	headers map[string][]byte
}

// fakeProducer records what was dead-lettered and reports the configured
// outcome.
type fakeProducer struct {
	published []publication
	status    messaging.DeliveryStatus
	err       error
}

func (f *fakeProducer) Produce(_ context.Context, topic string, key, value []byte, headers map[string][]byte) (messaging.ProducedMessage, error) {
	if f.err != nil {
		return messaging.ProducedMessage{}, f.err
	}
	f.published = append(f.published, publication{topic: topic, key: key, value: value, headers: headers})
	return messaging.ProducedMessage{Topic: topic, Status: f.status}, nil
}

func (f *fakeProducer) Close(context.Context) error { return nil }

// testMeter returns a Meter backed by a manual reader so tests can
// collect and assert on recorded instrument values.
func testMeter() (*sdkmetric.ManualReader, metric.Meter) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return reader, provider.Meter("consumer_test")
}

func testLogger() (*bytes.Buffer, *slog.Logger) {
	var buf bytes.Buffer
	return &buf, slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// counterValue collects reader and returns the sum recorded against the
// named Int64 counter.
func counterValue(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
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
