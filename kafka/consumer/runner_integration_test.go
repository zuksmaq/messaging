//go:build integration

package consumer

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
	"github.com/zuksmaq/messaging/kafka/internal/kafkatest"
)

// poisonValue is the payload the test handler rejects.
const poisonValue = "poison"

// TestRunnerPoisonPolicies drives a real broker through all three
// policies: the same three-message topic, the same handler rejecting the
// middle message, and a different PoisonMessageAction each time.
func TestRunnerPoisonPolicies(t *testing.T) {
	bootstrap := kafkatest.Brokers(t)

	// Skip drops the poison message and keeps going, so the group ends up
	// committed past all three.
	t.Run("skip", func(t *testing.T) {
		topic := "runner-skip"
		seed(t, bootstrap, topic, "first", poisonValue, "third")

		c := newConsumer[string, string](t, bootstrap, topic, kafka.FormatString, kafka.FormatString)
		handled, handler := recordingHandler()
		r := newRunner(t, c, handler, RunnerConfig{PoisonAction: Skip})

		stop := start(t, r)
		waitForCommittedOffset(t, c, topic, 3)
		stop(t)

		if got := handled(); len(got) != 2 || got[0] != "first" || got[1] != "third" {
			t.Errorf("handled values = %v, want [first third] — the poison message must not be handled twice", got)
		}
	})

	// DeadLetter forwards the original bytes before committing past the
	// poison message.
	t.Run("dead letter", func(t *testing.T) {
		topic := "runner-dead-letter"
		dlq := topic + ".dlq"
		seed(t, bootstrap, topic, "first", poisonValue, "third")
		kafkatest.CreateTopic(t, bootstrap, dlq)

		c := newConsumer[string, string](t, bootstrap, topic, kafka.FormatString, kafka.FormatString)
		_, handler := recordingHandler()
		r := newRunner(t, c, handler, RunnerConfig{
			PoisonAction:       DeadLetter,
			DeadLetterTopic:    dlq,
			DeadLetterProducer: newProducer[[]byte, []byte](t, bootstrap, kafka.FormatBytes, kafka.FormatBytes),
		})

		stop := start(t, r)
		waitForCommittedOffset(t, c, topic, 3)
		stop(t)

		// The forwarded message carries the original bytes plus the
		// annotations needed to triage it from the dead-letter topic alone.
		dead := mustConsume(t, newConsumer[[]byte, []byte](t, bootstrap, dlq, kafka.FormatBytes, kafka.FormatBytes))
		if string(dead.Value) != poisonValue {
			t.Errorf("dead-lettered value = %q, want %q", dead.Value, poisonValue)
		}
		if v := string(dead.Headers[DeadLetterTopicHeader]); v != topic {
			t.Errorf("dead-lettered %s = %q, want %q", DeadLetterTopicHeader, v, topic)
		}
		if v := string(dead.Headers[DeadLetterOffsetHeader]); v != "1" {
			t.Errorf("dead-lettered %s = %q, want 1", DeadLetterOffsetHeader, v)
		}
		if v := string(dead.Headers[DeadLetterErrorHeader]); !strings.Contains(v, poisonValue) {
			t.Errorf("dead-lettered %s = %q, want the handler error", DeadLetterErrorHeader, v)
		}
	})

	// Halt stops the loop with the poison message still uncommitted, so a
	// restart re-delivers it: the committed offset stays pointing AT the
	// poison message rather than past it.
	t.Run("halt", func(t *testing.T) {
		topic := "runner-halt"
		seed(t, bootstrap, topic, "first", poisonValue, "third")

		c := newConsumer[string, string](t, bootstrap, topic, kafka.FormatString, kafka.FormatString)
		handled, handler := recordingHandler()
		r := newRunner(t, c, handler, RunnerConfig{PoisonAction: Halt})

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		err := r.Run(ctx)
		if err == nil {
			t.Fatal("Run = nil, want the halt error")
		}
		if !strings.Contains(err.Error(), poisonValue) {
			t.Errorf("Run error = %v, want it to report the poison message", err)
		}
		if got := handled(); len(got) != 1 || got[0] != "first" {
			t.Errorf("handled values = %v, want [first] — Halt stops at the poison message", got)
		}
		// Offset 1 is the poison message, so the next read re-delivers it.
		if got := committedOffset(t, c, topic); got != 1 {
			t.Errorf("committed offset = %d, want 1 so the poison message is re-delivered", got)
		}
	})
}

// TestRunnerStopsOnContextCancellation asserts a cancelled context ends
// Run cleanly against a live broker, leaving the consumer closeable.
func TestRunnerStopsOnContextCancellation(t *testing.T) {
	bootstrap := kafkatest.Brokers(t)
	topic := "runner-cancel"
	seed(t, bootstrap, topic, "first")

	c := newConsumer[string, string](t, bootstrap, topic, kafka.FormatString, kafka.FormatString)
	handled, handler := recordingHandler()
	r := newRunner(t, c, handler, RunnerConfig{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Let the runner drain the topic, then cancel: Run must return rather
	// than stay blocked in Consume waiting for a message that never comes.
	waitForCommittedOffset(t, c, topic, 1)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run = %v, want nil after cancellation", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return within 30s of cancellation")
	}
	if got := handled(); len(got) != 1 {
		t.Errorf("handled values = %v, want the single seeded message", got)
	}
}

// seed creates topic and publishes values to it in order, so their
// offsets are 0..len(values)-1.
func seed(t *testing.T, bootstrap, topic string, values ...string) {
	t.Helper()

	kafkatest.CreateTopic(t, bootstrap, topic)
	p := newProducer[string, string](t, bootstrap, kafka.FormatString, kafka.FormatString)
	for i, v := range values {
		out := mustProduce(t, p, topic, "k", v)
		if out.Offset != int64(i) {
			t.Fatalf("seeded %q at offset %d, want %d", v, out.Offset, i)
		}
	}
}

// recordingHandler returns a handler that rejects poisonValue and records
// every value it accepts, plus an accessor for those values. The handler
// runs on the Runner's goroutine, so the recording is mutex-guarded.
func recordingHandler() (func() []string, Handler[string, string]) {
	var (
		mu      sync.Mutex
		handled []string
	)

	return func() []string {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(handled)
		},
		func(_ context.Context, msg messaging.ReceivedMessage[string, string]) error {
			if msg.Value == poisonValue {
				return errors.New("handler cannot process " + poisonValue)
			}
			mu.Lock()
			defer mu.Unlock()
			handled = append(handled, msg.Value)
			return nil
		}
}

func newRunner(t *testing.T, c messaging.Consumer[string, string], h Handler[string, string], cfg RunnerConfig) *Runner[string, string] {
	t.Helper()

	r, err := NewRunner(c, h, cfg)
	if err != nil {
		t.Fatalf("NewRunner = %v", err)
	}
	return r
}

// start runs r in the background and returns a stop function that
// cancels it and waits for Run to return.
func start(t *testing.T, r *Runner[string, string]) func(*testing.T) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	return func(t *testing.T) {
		t.Helper()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run = %v, want nil after cancellation", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("Run did not return within 30s of cancellation")
		}
	}
}

// waitForCommittedOffset polls until the group's committed offset for
// topic reaches want, so tests synchronize on the durable outcome rather
// than on a sleep.
func waitForCommittedOffset(t *testing.T, c *Consumer[string, string], topic string, want int64) {
	t.Helper()

	deadline := time.Now().Add(60 * time.Second)
	var got int64
	for time.Now().Before(deadline) {
		if got = committedOffset(t, c, topic); got == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("committed offset = %d after 60s, want %d", got, want)
}

// committedOffset returns the group's stored offset for topic's single
// partition, or -1 when nothing has been committed yet.
func committedOffset[K, V any](t *testing.T, c *Consumer[K, V], topic string) int64 {
	t.Helper()

	committed, err := c.client.Committed([]ckafka.TopicPartition{
		{Topic: &topic, Partition: 0},
	}, 30_000)
	if err != nil {
		t.Fatalf("reading committed offsets: %v", err)
	}
	if len(committed) != 1 {
		t.Fatalf("committed partitions = %d, want 1", len(committed))
	}
	if committed[0].Offset == ckafka.OffsetInvalid {
		return -1
	}
	return int64(committed[0].Offset)
}
