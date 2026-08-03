//go:build integration

package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
	"github.com/zuksmaq/messaging/kafka/internal/kafkatest"
	"github.com/zuksmaq/messaging/kafka/producer"
)

// avroOrder is the record the Avro round-trip publishes. The codec
// derives its schema from these exported fields.
type avroOrder struct {
	ID    string `avro:"id"`
	Cents int64  `avro:"cents"`
}

// avroCustomer is a second record type, used as a key so the test proves
// the key and value schemas are registered under separate subjects.
type avroCustomer struct {
	Number string `avro:"number"`
}

// TestAvroRoundTrip publishes Avro through the ticket-02 producer and
// reads it back through the consumer against a real Schema Registry,
// covering a registry-less key with an Avro value and Avro on both halves.
func TestAvroRoundTrip(t *testing.T) {
	bootstrap, registryURL := kafkatest.Brokers(t), kafkatest.SchemaRegistry(t)

	// Formats stay independent: a String key alongside an Avro value.
	t.Run("string key with avro value", func(t *testing.T) {
		topic := "avro-value"
		kafkatest.CreateTopic(t, bootstrap, topic)

		want := avroOrder{ID: "o-1", Cents: 1999}
		p := newAvroProducer[string, avroOrder](t, bootstrap, registryURL, kafka.FormatString, kafka.FormatAvro)
		produced := mustProduce(t, p, topic, "k1", want)

		c := newAvroConsumer[string, avroOrder](t, bootstrap, registryURL, topic, kafka.FormatString, kafka.FormatAvro)
		got := mustConsume(t, c)

		if got.Key != "k1" {
			t.Errorf("consumed key = %q, want k1", got.Key)
		}
		if got.Value != want {
			t.Errorf("consumed value = %+v, want %+v", got.Value, want)
		}
		assertCoordinates(t, got, produced)
		commitAndAssertOffset(t, c, got)
	})

	// Key and value carry different Avro schemas, registered under
	// "<topic>-key" and "<topic>-value".
	t.Run("avro key and value", func(t *testing.T) {
		topic := "avro-both"
		kafkatest.CreateTopic(t, bootstrap, topic)

		wantKey := avroCustomer{Number: "c-7"}
		wantValue := avroOrder{ID: "o-2", Cents: 500}
		p := newAvroProducer[avroCustomer, avroOrder](t, bootstrap, registryURL, kafka.FormatAvro, kafka.FormatAvro)
		produced := mustProduce(t, p, topic, wantKey, wantValue)

		c := newAvroConsumer[avroCustomer, avroOrder](t, bootstrap, registryURL, topic, kafka.FormatAvro, kafka.FormatAvro)
		got := mustConsume(t, c)

		if got.Key != wantKey {
			t.Errorf("consumed key = %+v, want %+v", got.Key, wantKey)
		}
		if got.Value != wantValue {
			t.Errorf("consumed value = %+v, want %+v", got.Value, wantValue)
		}
		assertCoordinates(t, got, produced)
		commitAndAssertOffset(t, c, got)
	})
}

// TestAvroSchemaViolationIsASerializationError registers a schema the
// producer's own type is incompatible with, then asserts the rejection
// surfaces as messaging.ErrSerialization rather than as a raw Schema
// Registry client error.
func TestAvroSchemaViolationIsASerializationError(t *testing.T) {
	bootstrap, registryURL := kafkatest.Brokers(t), kafkatest.SchemaRegistry(t)
	topic := "avro-incompatible"
	kafkatest.CreateTopic(t, bootstrap, topic)

	// The registered schema has "id" alone, so avroOrder's schema adds
	// "cents" with no default: not backward-compatible, and the registry
	// refuses to register it.
	kafkatest.RegisterSchema(t, registryURL, topic+"-value", `{
		"type": "record",
		"name": "avroOrder",
		"fields": [
			{"name": "id", "type": "string"}
		]
	}`)

	p := newAvroProducer[string, avroOrder](t, bootstrap, registryURL, kafka.FormatString, kafka.FormatAvro)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	out, err := p.Produce(ctx, topic, "k1", avroOrder{ID: "o-3", Cents: 100}, nil)
	if !errors.Is(err, messaging.ErrSerialization) {
		t.Fatalf("Produce error = %v, want ErrSerialization", err)
	}
	if out.Status != messaging.NotPersisted {
		t.Errorf("DeliveryStatus = %s, want %s", out.Status, messaging.NotPersisted)
	}
}

func newAvroProducer[K, V any](t *testing.T, bootstrap, registryURL string, keyFmt, valFmt kafka.Format) *producer.Producer[K, V] {
	t.Helper()

	p, err := producer.New[K, V](producer.Config{
		BootstrapServers: bootstrap,
		KeyFormat:        keyFmt,
		ValueFormat:      valFmt,
		SchemaRegistry:   kafka.SchemaRegistryConfig{URL: registryURL},
		FlushTimeout:     30 * time.Second,
		ProduceTimeout:   60 * time.Second,
	})
	if err != nil {
		t.Fatalf("producer.New = %v", err)
	}
	t.Cleanup(func() {
		if err := p.Close(context.Background()); err != nil {
			t.Logf("closing producer: %v", err)
		}
	})
	return p
}

func newAvroConsumer[K, V any](t *testing.T, bootstrap, registryURL, topic string, keyFmt, valFmt kafka.Format) *Consumer[K, V] {
	t.Helper()

	c, err := New[K, V](Config{
		BootstrapServers: bootstrap,
		GroupID:          "itest-" + topic,
		Topics:           []string{topic},
		KeyFormat:        keyFmt,
		ValueFormat:      valFmt,
		SchemaRegistry:   kafka.SchemaRegistryConfig{URL: registryURL},
	})
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Logf("closing consumer: %v", err)
		}
	})
	return c
}
