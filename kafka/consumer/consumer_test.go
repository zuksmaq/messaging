package consumer

import (
	"context"
	"errors"
	"testing"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
)

func newTestConsumer(t *testing.T) *Consumer[string, []byte] {
	t.Helper()
	c, err := New[string, []byte](Config{
		BootstrapServers: "localhost:9092",
		GroupID:          "orders",
		Topics:           []string{"orders.v1"},
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatBytes,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestConsumerDecodeSurfacesPartitionError(t *testing.T) {
	t.Parallel()
	c := newTestConsumer(t)
	topic := "orders.v1"

	msg := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    5,
			Error:     errors.New("corrupt record"),
		},
		Key:   []byte("k"),
		Value: []byte("v"),
	}

	out, err := c.decode(context.Background(), msg)
	if !errors.Is(err, messaging.ErrDeserialization) {
		t.Errorf("decode error = %v, want ErrDeserialization", err)
	}
	if !errors.Is(err, messaging.ErrBroker) {
		t.Errorf("decode error = %v, want ErrBroker", err)
	}
	if out.Key != "" || out.Value != nil {
		t.Errorf("decode() Key/Value = %q/%v, want zero: the record must not be decoded", out.Key, out.Value)
	}
}

func TestConsumerDecodePreservesDuplicateHeaders(t *testing.T) {
	t.Parallel()
	c := newTestConsumer(t)
	topic := "orders.v1"

	msg := &ckafka.Message{
		TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
		Key:            []byte("k"),
		Value:          []byte("v"),
		Headers: []ckafka.Header{
			{Key: "trace-id", Value: []byte("first")},
			{Key: "trace-id", Value: []byte("second")},
		},
	}

	out, err := c.decode(context.Background(), msg)
	if err != nil {
		t.Fatalf("decode() error = %v", err)
	}

	if got := string(out.Headers["trace-id"]); got != "second" {
		t.Errorf("Headers[trace-id] = %q, want last-wins %q", got, "second")
	}

	want := []messaging.Header{
		{Key: "trace-id", Value: []byte("first")},
		{Key: "trace-id", Value: []byte("second")},
	}
	if len(out.HeaderList) != len(want) {
		t.Fatalf("HeaderList = %+v, want %+v", out.HeaderList, want)
	}
	for i, h := range want {
		if out.HeaderList[i].Key != h.Key || string(out.HeaderList[i].Value) != string(h.Value) {
			t.Errorf("HeaderList[%d] = %+v, want %+v", i, out.HeaderList[i], h)
		}
	}
}

func TestNewConsumerRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	t.Run("invalid config", func(t *testing.T) {
		t.Parallel()
		_, err := New[string, []byte](Config{})
		if !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("New error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("format type mismatch", func(t *testing.T) {
		t.Parallel()
		_, err := New[int, []byte](Config{
			BootstrapServers: "localhost:9092",
			GroupID:          "orders",
			Topics:           []string{"orders.v1"},
			KeyFormat:        kafka.FormatString,
			ValueFormat:      kafka.FormatBytes,
		})
		if !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("New error = %v, want ErrInvalidConfig", err)
		}
	})

	// Avro without a registry must fail here, not at the first consume.
	t.Run("avro without a schema registry", func(t *testing.T) {
		t.Parallel()
		type order struct{ ID string }
		_, err := New[string, order](Config{
			BootstrapServers: "localhost:9092",
			GroupID:          "orders",
			Topics:           []string{"orders.v1"},
			KeyFormat:        kafka.FormatString,
			ValueFormat:      kafka.FormatAvro,
		})
		if !errors.Is(err, messaging.ErrSchemaRegistryRequired) {
			t.Errorf("New error = %v, want ErrSchemaRegistryRequired", err)
		}
	})
}
