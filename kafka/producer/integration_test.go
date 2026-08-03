//go:build integration

package producer

import (
	"context"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
	"github.com/zuksmaq/messaging/kafka/internal/kafkatest"
)

// readOne consumes the first message on topic from the beginning.
func readOne(t *testing.T, bootstrap, topic string) *ckafka.Message {
	t.Helper()

	c, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"group.id":           "itest-" + topic,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		t.Fatalf("creating consumer: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if err := c.Subscribe(topic, nil); err != nil {
		t.Fatalf("subscribing to %q: %v", topic, err)
	}

	msg, err := c.ReadMessage(60 * time.Second)
	if err != nil {
		t.Fatalf("reading from %q: %v", topic, err)
	}
	return msg
}

// TestProduceRoundTripPerFormat publishes with each supported key/value
// format against a real broker and asserts the broker reported
// Persisted and the bytes are readable back off the topic.
func TestProduceRoundTripPerFormat(t *testing.T) {
	bootstrap := kafkatest.Brokers(t)

	t.Run("bytes key and value", func(t *testing.T) {
		topic := "roundtrip-bytes"
		kafkatest.CreateTopic(t, bootstrap, topic)

		p := newProducer[[]byte, []byte](t, bootstrap, kafka.FormatBytes, kafka.FormatBytes)
		got := mustProduce(t, p, topic, []byte("k1"), []byte("v1"))

		assertPersisted(t, got, topic)
		msg := readOne(t, bootstrap, topic)
		if string(msg.Key) != "k1" || string(msg.Value) != "v1" {
			t.Errorf("read key/value = %q/%q, want k1/v1", msg.Key, msg.Value)
		}
	})

	t.Run("string key and value", func(t *testing.T) {
		topic := "roundtrip-string"
		kafkatest.CreateTopic(t, bootstrap, topic)

		p := newProducer[string, string](t, bootstrap, kafka.FormatString, kafka.FormatString)
		got := mustProduce(t, p, topic, "k2", "v2")

		assertPersisted(t, got, topic)
		msg := readOne(t, bootstrap, topic)
		if string(msg.Key) != "k2" || string(msg.Value) != "v2" {
			t.Errorf("read key/value = %q/%q, want k2/v2", msg.Key, msg.Value)
		}
	})

	// Key and value formats are independent: a String key with a JSON
	// value must work.
	t.Run("string key with json value", func(t *testing.T) {
		type order struct {
			ID    string `json:"id"`
			Cents int64  `json:"cents"`
		}
		topic := "roundtrip-json"
		kafkatest.CreateTopic(t, bootstrap, topic)

		p := newProducer[string, order](t, bootstrap, kafka.FormatString, kafka.FormatJSON)
		got := mustProduce(t, p, topic, "k3", order{ID: "o-1", Cents: 1999})

		assertPersisted(t, got, topic)
		msg := readOne(t, bootstrap, topic)
		if string(msg.Key) != "k3" {
			t.Errorf("read key = %q, want k3", msg.Key)
		}
		if want := `{"id":"o-1","cents":1999}`; string(msg.Value) != want {
			t.Errorf("read value = %s, want %s", msg.Value, want)
		}
	})

	t.Run("headers survive the round trip", func(t *testing.T) {
		topic := "roundtrip-headers"
		kafkatest.CreateTopic(t, bootstrap, topic)

		p := newProducer[string, []byte](t, bootstrap, kafka.FormatString, kafka.FormatBytes)
		out, err := p.Produce(context.Background(), topic, "k4", []byte("v4"),
			map[string][]byte{messaging.EventIDHeader: []byte("evt-42")})
		if err != nil {
			t.Fatalf("Produce = %v", err)
		}
		assertPersisted(t, out, topic)

		msg := readOne(t, bootstrap, topic)
		var found string
		for _, h := range msg.Headers {
			if h.Key == messaging.EventIDHeader {
				found = string(h.Value)
			}
		}
		if found != "evt-42" {
			t.Errorf("%s header = %q, want evt-42", messaging.EventIDHeader, found)
		}
	})
}

// TestReadyCheckAgainstRealBroker asserts readiness succeeds against a
// live cluster and fails against an unreachable one.
func TestReadyCheckAgainstRealBroker(t *testing.T) {
	bootstrap := kafkatest.Brokers(t)

	t.Run("reachable", func(t *testing.T) {
		p := newProducer[string, []byte](t, bootstrap, kafka.FormatString, kafka.FormatBytes)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := p.ReadyCheck(ctx); err != nil {
			t.Errorf("ReadyCheck = %v, want nil", err)
		}
	})

	t.Run("unreachable", func(t *testing.T) {
		p := newProducer[string, []byte](t, "127.0.0.1:1", kafka.FormatString, kafka.FormatBytes)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := p.ReadyCheck(ctx); err == nil {
			t.Error("ReadyCheck = nil, want an error for an unreachable broker")
		}
	})
}

func newProducer[K, V any](t *testing.T, bootstrap string, keyFmt, valFmt kafka.Format) *Producer[K, V] {
	t.Helper()

	p, err := New[K, V](Config{
		BootstrapServers: bootstrap,
		KeyFormat:        keyFmt,
		ValueFormat:      valFmt,
		FlushTimeout:     30 * time.Second,
		ProduceTimeout:   60 * time.Second,
	})
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(context.Background()); err != nil {
			t.Logf("closing producer: %v", err)
		}
	})
	return p
}

func mustProduce[K, V any](t *testing.T, p *Producer[K, V], topic string, key K, value V) messaging.ProducedMessage {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	out, err := p.Produce(ctx, topic, key, value, nil)
	if err != nil {
		t.Fatalf("Produce = %v", err)
	}
	return out
}

func assertPersisted(t *testing.T, got messaging.ProducedMessage, topic string) {
	t.Helper()

	if got.Status != messaging.Persisted {
		t.Errorf("DeliveryStatus = %s, want %s", got.Status, messaging.Persisted)
	}
	if got.Topic != topic {
		t.Errorf("Topic = %q, want %q", got.Topic, topic)
	}
	if got.Partition < 0 {
		t.Errorf("Partition = %d, want a real partition", got.Partition)
	}
	if got.Offset < 0 {
		t.Errorf("Offset = %d, want a real offset", got.Offset)
	}
}
