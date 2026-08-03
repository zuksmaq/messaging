package kafka

import (
	"errors"
	"testing"

	"github.com/zuksmaq/messaging"
)

type order struct {
	ID    string
	Cents int64
}

// TestAvroWithoutSchemaRegistry pins the fail-fast contract: the Avro
// format is unusable without a registry, and the caller learns that from
// the sentinel rather than from a failed publish.
func TestAvroWithoutSchemaRegistry(t *testing.T) {
	t.Parallel()

	t.Run("serializer", func(t *testing.T) {
		t.Parallel()
		if _, err := SerializerFor[order](FormatAvro, ValuePart, nil); !errors.Is(err, messaging.ErrSchemaRegistryRequired) {
			t.Errorf("SerializerFor[order](avro) error = %v, want ErrSchemaRegistryRequired", err)
		}
	})

	t.Run("deserializer", func(t *testing.T) {
		t.Parallel()
		if _, err := DeserializerFor[order](FormatAvro, ValuePart, nil); !errors.Is(err, messaging.ErrSchemaRegistryRequired) {
			t.Errorf("DeserializerFor[order](avro) error = %v, want ErrSchemaRegistryRequired", err)
		}
	})

	t.Run("config validation", func(t *testing.T) {
		t.Parallel()
		err := ValidateSchemaRegistry(SchemaRegistryConfig{}, FormatString, FormatAvro)
		if !errors.Is(err, messaging.ErrSchemaRegistryRequired) {
			t.Errorf("ValidateSchemaRegistry(zero, string, avro) = %v, want ErrSchemaRegistryRequired", err)
		}
	})

	t.Run("registry-less formats need no registry", func(t *testing.T) {
		t.Parallel()
		if err := ValidateSchemaRegistry(SchemaRegistryConfig{}, FormatString, FormatJSON); err != nil {
			t.Errorf("ValidateSchemaRegistry(zero, string, json) = %v, want nil", err)
		}
	})
}

// TestAvroRejectsNonStructType asserts the type Avro cannot describe is
// refused at construction, not at the first message.
func TestAvroRejectsNonStructType(t *testing.T) {
	t.Parallel()

	sr, err := NewSchemaRegistry(SchemaRegistryConfig{URL: "http://localhost:8081"})
	if err != nil {
		t.Fatalf("NewSchemaRegistry = %v", err)
	}
	t.Cleanup(func() {
		if err := sr.Close(); err != nil {
			t.Logf("closing schema registry: %v", err)
		}
	})

	t.Run("string", func(t *testing.T) {
		t.Parallel()
		if _, err := SerializerFor[string](FormatAvro, ValuePart, sr); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("SerializerFor[string](avro) error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("pointer to struct", func(t *testing.T) {
		t.Parallel()
		if _, err := DeserializerFor[*order](FormatAvro, ValuePart, sr); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("DeserializerFor[*order](avro) error = %v, want ErrInvalidConfig", err)
		}
	})
}

// TestAvroCodecsBuildForAStruct asserts the happy path builds without
// touching the registry: no network call happens until a message is
// actually encoded or decoded.
func TestAvroCodecsBuildForAStruct(t *testing.T) {
	t.Parallel()

	sr, err := NewSchemaRegistry(SchemaRegistryConfig{URL: "http://localhost:8081"})
	if err != nil {
		t.Fatalf("NewSchemaRegistry = %v", err)
	}
	t.Cleanup(func() {
		if err := sr.Close(); err != nil {
			t.Logf("closing schema registry: %v", err)
		}
	})

	if _, err := SerializerFor[order](FormatAvro, KeyPart, sr); err != nil {
		t.Errorf("SerializerFor[order](avro, KeyPart) = %v, want nil", err)
	}
	if _, err := DeserializerFor[order](FormatAvro, ValuePart, sr); err != nil {
		t.Errorf("DeserializerFor[order](avro, ValuePart) = %v, want nil", err)
	}
}
