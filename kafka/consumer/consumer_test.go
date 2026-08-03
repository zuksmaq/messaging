package consumer

import (
	"errors"
	"testing"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
)

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
