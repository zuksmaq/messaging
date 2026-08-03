//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zuksmaq/messaging"
)

// TestConsumeRoundTripPerFormat publishes with the ticket-02 producer and
// reads back through the consumer for each supported key/value format,
// asserting the value survives the round trip and Commit stores the next
// offset to read.
func TestConsumeRoundTripPerFormat(t *testing.T) {
	bootstrap := brokers(t)

	t.Run("bytes key and value", func(t *testing.T) {
		topic := "consume-bytes"
		createTopic(t, bootstrap, topic)

		p := newProducer[[]byte, []byte](t, bootstrap, FormatBytes, FormatBytes)
		produced := mustProduce(t, p, topic, []byte("k1"), []byte("v1"))

		c := newConsumer[[]byte, []byte](t, bootstrap, topic, FormatBytes, FormatBytes)
		got := mustConsume(t, c)

		if string(got.Key) != "k1" || string(got.Value) != "v1" {
			t.Errorf("consumed key/value = %q/%q, want k1/v1", got.Key, got.Value)
		}
		assertCoordinates(t, got, produced)
		commitAndAssertOffset(t, c, got)
	})

	t.Run("string key and value", func(t *testing.T) {
		topic := "consume-string"
		createTopic(t, bootstrap, topic)

		p := newProducer[string, string](t, bootstrap, FormatString, FormatString)
		produced := mustProduce(t, p, topic, "k2", "v2")

		c := newConsumer[string, string](t, bootstrap, topic, FormatString, FormatString)
		got := mustConsume(t, c)

		if got.Key != "k2" || got.Value != "v2" {
			t.Errorf("consumed key/value = %q/%q, want k2/v2", got.Key, got.Value)
		}
		assertCoordinates(t, got, produced)
		commitAndAssertOffset(t, c, got)
	})

	// Key and value formats are independent: a String key with a JSON
	// value must round-trip.
	t.Run("string key with json value", func(t *testing.T) {
		type order struct {
			ID    string `json:"id"`
			Cents int64  `json:"cents"`
		}
		topic := "consume-json"
		createTopic(t, bootstrap, topic)

		want := order{ID: "o-1", Cents: 1999}
		p := newProducer[string, order](t, bootstrap, FormatString, FormatJSON)
		produced := mustProduce(t, p, topic, "k3", want)

		c := newConsumer[string, order](t, bootstrap, topic, FormatString, FormatJSON)
		got := mustConsume(t, c)

		if got.Key != "k3" {
			t.Errorf("consumed key = %q, want k3", got.Key)
		}
		if got.Value != want {
			t.Errorf("consumed value = %+v, want %+v", got.Value, want)
		}
		assertCoordinates(t, got, produced)
		commitAndAssertOffset(t, c, got)
	})

	t.Run("headers survive the round trip", func(t *testing.T) {
		topic := "consume-headers"
		createTopic(t, bootstrap, topic)

		p := newProducer[string, []byte](t, bootstrap, FormatString, FormatBytes)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if _, err := p.Produce(ctx, topic, "k4", []byte("v4"),
			map[string][]byte{messaging.EventIDHeader: []byte("evt-42")}); err != nil {
			t.Fatalf("Produce = %v", err)
		}

		c := newConsumer[string, []byte](t, bootstrap, topic, FormatString, FormatBytes)
		got := mustConsume(t, c)

		if got.EventID() != "evt-42" {
			t.Errorf("EventID() = %q, want evt-42", got.EventID())
		}
	})
}

// TestConsumeTombstone asserts a nil value round-trips as a tombstone
// rather than a deserialization error.
func TestConsumeTombstone(t *testing.T) {
	bootstrap := brokers(t)
	topic := "consume-tombstone"
	createTopic(t, bootstrap, topic)

	p := newProducer[string, []byte](t, bootstrap, FormatString, FormatBytes)
	mustProduce[string, []byte](t, p, topic, "gone", nil)

	c := newConsumer[string, []byte](t, bootstrap, topic, FormatString, FormatBytes)
	got := mustConsume(t, c)

	if got.Key != "gone" {
		t.Errorf("consumed key = %q, want gone", got.Key)
	}
	if !got.Tombstone() {
		t.Errorf("Tombstone() = false for value %v, want true", got.Value)
	}
}

// TestConsumerReadyCheck asserts readiness succeeds against a live
// cluster and fails against an unreachable one.
func TestConsumerReadyCheck(t *testing.T) {
	bootstrap := brokers(t)
	topic := "consume-ready"
	createTopic(t, bootstrap, topic)

	t.Run("reachable", func(t *testing.T) {
		c := newConsumer[string, []byte](t, bootstrap, topic, FormatString, FormatBytes)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := c.ReadyCheck(ctx); err != nil {
			t.Errorf("ReadyCheck = %v, want nil", err)
		}
	})

	t.Run("unreachable", func(t *testing.T) {
		c := newConsumer[string, []byte](t, "127.0.0.1:1", topic, FormatString, FormatBytes)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := c.ReadyCheck(ctx); err == nil {
			t.Error("ReadyCheck = nil, want an error for an unreachable broker")
		}
	})
}

// TestConsumeHonorsContextCancellation asserts Consume returns instead of
// blocking forever once its context is done.
func TestConsumeHonorsContextCancellation(t *testing.T) {
	bootstrap := brokers(t)
	topic := "consume-cancel"
	createTopic(t, bootstrap, topic)

	c := newConsumer[string, []byte](t, bootstrap, topic, FormatString, FormatBytes)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := c.Consume(ctx); err == nil {
		t.Error("Consume = nil error on an empty topic, want the context error")
	}
}

func newConsumer[K, V any](t *testing.T, bootstrap, topic string, keyFmt, valFmt Format) *Consumer[K, V] {
	t.Helper()

	c, err := NewConsumer[K, V](ConsumerConfig{
		BootstrapServers: bootstrap,
		GroupID:          "itest-" + topic,
		Topics:           []string{topic},
		KeyFormat:        keyFmt,
		ValueFormat:      valFmt,
	})
	if err != nil {
		t.Fatalf("NewConsumer = %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Logf("closing consumer: %v", err)
		}
	})
	return c
}

func mustConsume[K, V any](t *testing.T, c *Consumer[K, V]) messaging.ReceivedMessage[K, V] {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	got, err := c.Consume(ctx)
	if err != nil {
		t.Fatalf("Consume = %v", err)
	}
	return got
}

func assertCoordinates[K, V any](t *testing.T, got messaging.ReceivedMessage[K, V], produced messaging.ProducedMessage) {
	t.Helper()

	if got.Topic != produced.Topic {
		t.Errorf("Topic = %q, want %q", got.Topic, produced.Topic)
	}
	if got.Partition != produced.Partition {
		t.Errorf("Partition = %d, want %d", got.Partition, produced.Partition)
	}
	if got.Offset != produced.Offset {
		t.Errorf("Offset = %d, want %d", got.Offset, produced.Offset)
	}
	if got.Timestamp.IsZero() {
		t.Error("Timestamp is zero, want the broker-assigned timestamp")
	}
}

// commitAndAssertOffset commits msg and asserts the group's stored
// offset is msg.Offset+1 — Kafka stores the next offset to read.
func commitAndAssertOffset[K, V any](t *testing.T, c *Consumer[K, V], msg messaging.ReceivedMessage[K, V]) {
	t.Helper()

	if err := c.Commit(context.Background(), msg); err != nil {
		t.Fatalf("Commit = %v", err)
	}

	topic := msg.Topic
	committed, err := c.client.Committed([]ckafka.TopicPartition{
		{Topic: &topic, Partition: msg.Partition},
	}, 30_000)
	if err != nil {
		t.Fatalf("reading committed offsets: %v", err)
	}
	if len(committed) != 1 {
		t.Fatalf("committed partitions = %d, want 1", len(committed))
	}
	if want := ckafka.Offset(msg.Offset + 1); committed[0].Offset != want {
		t.Errorf("committed offset = %v, want %v", committed[0].Offset, want)
	}
}
