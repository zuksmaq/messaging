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
//
// Every assertion on a committed offset is made after the runner has
// stopped. A consumer's librdkafka handle belongs to whichever goroutine
// is polling it, so reading offsets off it while Run is mid-Consume is not
// safe.
func TestRunnerPoisonPolicies(t *testing.T) {
	bootstrap := kafkatest.Brokers(t)

	// Skip drops the poison message and keeps going, so the group ends up
	// committed past all three.
	t.Run("skip", func(t *testing.T) {
		topic := "runner-skip"
		seed(t, bootstrap, topic, "first", poisonValue, "third")

		c := newConsumer[string, string](t, bootstrap, topic, kafka.FormatString, kafka.FormatString)
		handled, handler := signalingHandler()
		stop := start(t, newRunner(t, c, handler, RunnerConfig{PoisonAction: Skip}))

		// Reaching "third" means the poison message was skipped rather
		// than retried or halted on.
		waitForHandled(t, handled, "first", "third")
		stop()

		if got := committedOffset(t, c, topic); got != 3 {
			t.Errorf("committed offset = %d, want 3 — Skip commits past the poison message", got)
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
		handled, handler := signalingHandler()
		stop := start(t, newRunner(t, c, handler, RunnerConfig{
			PoisonAction:       DeadLetter,
			DeadLetterTopic:    dlq,
			DeadLetterProducer: newProducer[[]byte, []byte](t, bootstrap, kafka.FormatBytes, kafka.FormatBytes),
		}))

		waitForHandled(t, handled, "first", "third")
		stop()

		if got := committedOffset(t, c, topic); got != 3 {
			t.Errorf("committed offset = %d, want 3 — the offset moves once the publish is confirmed", got)
		}

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
		handled, handler := signalingHandler()
		r := newRunner(t, c, handler, RunnerConfig{PoisonAction: Halt})

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		// Run returns on its own here — nothing else touches the consumer
		// while it does.
		err := r.Run(ctx)
		if err == nil {
			t.Fatal("Run = nil, want the halt error")
		}
		if !strings.Contains(err.Error(), poisonValue) {
			t.Errorf("Run error = %v, want it to report the poison message", err)
		}
		if got := drain(handled); !slices.Equal(got, []string{"first"}) {
			t.Errorf("handled values = %v, want [first] — Halt stops at the poison message", got)
		}
		// Offset 1 is the poison message, so the next read re-delivers it.
		if got := committedOffset(t, c, topic); got != 1 {
			t.Errorf("committed offset = %d, want 1 so the poison message is re-delivered", got)
		}
	})
}

// TestRunnerStopsOnContextCancellation asserts a cancelled context ends
// Run cleanly against a live broker rather than leaving it blocked in
// Consume waiting for a message that never comes. start's stop function
// fails the test if Run does not return, or returns an error.
func TestRunnerStopsOnContextCancellation(t *testing.T) {
	bootstrap := kafkatest.Brokers(t)
	topic := "runner-cancel"
	seed(t, bootstrap, topic, "first")

	c := newConsumer[string, string](t, bootstrap, topic, kafka.FormatString, kafka.FormatString)
	handled, handler := signalingHandler()
	stop := start(t, newRunner(t, c, handler, RunnerConfig{}))

	// Cancel once the runner has drained the topic, so it is parked in
	// Consume rather than mid-handler.
	waitForHandled(t, handled, "first")
	stop()
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

// signalingHandler returns a handler that rejects poisonValue and reports
// every value it accepts on the returned channel, so tests synchronize on
// the handler rather than polling the broker. The channel is buffered
// deep enough that the handler never blocks on an absent reader.
func signalingHandler() (<-chan string, Handler[string, string]) {
	handled := make(chan string, 16)

	return handled, func(_ context.Context, msg messaging.ReceivedMessage[string, string]) error {
		if msg.Value == poisonValue {
			return errors.New("handler cannot process " + poisonValue)
		}
		handled <- msg.Value
		return nil
	}
}

// waitForHandled reads the values the handler accepted and asserts they
// are exactly want, in order.
func waitForHandled(t *testing.T, handled <-chan string, want ...string) {
	t.Helper()

	timeout := time.After(60 * time.Second)
	got := make([]string, 0, len(want))
	for len(got) < len(want) {
		select {
		case v := <-handled:
			got = append(got, v)
		case <-timeout:
			t.Fatalf("handled %v within 60s, want %v", got, want)
		}
	}
	if !slices.Equal(got, want) {
		t.Errorf("handled values = %v, want %v", got, want)
	}
}

// drain returns the values buffered by the handler so far.
func drain(handled <-chan string) []string {
	var got []string
	for {
		select {
		case v := <-handled:
			got = append(got, v)
		default:
			return got
		}
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

// start runs r in the background and returns an idempotent stop function
// that cancels it and waits for Run to return, asserting it returns nil.
//
// stop is also registered as a cleanup, and must be: t.Cleanup runs LIFO,
// so it fires before the consumer's own Close. A failed assertion that
// skipped the explicit stop would otherwise leave Run polling a client
// that Close then destroys, which segfaults inside librdkafka rather than
// reporting the original failure.
func start(t *testing.T, r *Runner[string, string]) func() {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run = %v, want nil after cancellation", err)
				}
			case <-time.After(30 * time.Second):
				t.Error("Run did not return within 30s of cancellation")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// committedOffset returns the group's stored offset for topic's single
// partition, or -1 when nothing has been committed yet. Call it only
// while no runner is polling the consumer.
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
