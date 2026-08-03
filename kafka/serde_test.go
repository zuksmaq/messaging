package kafka

import (
	"errors"
	"testing"

	"github.com/zuksmaq/messaging"
)

func TestSerializerForRejectsMismatchedType(t *testing.T) {
	t.Parallel()

	t.Run("bytes with string", func(t *testing.T) {
		t.Parallel()
		if _, err := serializerFor[string](FormatBytes); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("serializerFor[string](bytes) error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("string with int", func(t *testing.T) {
		t.Parallel()
		if _, err := serializerFor[int](FormatString); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("serializerFor[int](string) error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("unknown format", func(t *testing.T) {
		t.Parallel()
		if _, err := serializerFor[string]("yaml"); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("serializerFor[string](yaml) error = %v, want ErrInvalidConfig", err)
		}
	})
}

func TestSerializers(t *testing.T) {
	t.Parallel()

	t.Run("bytes passes through", func(t *testing.T) {
		t.Parallel()
		s, err := serializerFor[[]byte](FormatBytes)
		if err != nil {
			t.Fatalf("serializerFor = %v", err)
		}
		got, err := s.Serialize("t", []byte("hello"))
		if err != nil {
			t.Fatalf("Serialize = %v", err)
		}
		if string(got) != "hello" {
			t.Errorf("Serialize = %q, want %q", got, "hello")
		}
	})

	t.Run("string encodes utf8", func(t *testing.T) {
		t.Parallel()
		s, err := serializerFor[string](FormatString)
		if err != nil {
			t.Fatalf("serializerFor = %v", err)
		}
		got, err := s.Serialize("t", "héllo")
		if err != nil {
			t.Fatalf("Serialize = %v", err)
		}
		if string(got) != "héllo" {
			t.Errorf("Serialize = %q, want %q", got, "héllo")
		}
	})

	t.Run("json marshals struct", func(t *testing.T) {
		t.Parallel()
		type payload struct {
			ID int `json:"id"`
		}
		s, err := serializerFor[payload](FormatJSON)
		if err != nil {
			t.Fatalf("serializerFor = %v", err)
		}
		got, err := s.Serialize("t", payload{ID: 7})
		if err != nil {
			t.Fatalf("Serialize = %v", err)
		}
		if string(got) != `{"id":7}` {
			t.Errorf("Serialize = %s, want {\"id\":7}", got)
		}
	})

	t.Run("json reports unmarshalable value", func(t *testing.T) {
		t.Parallel()
		s, err := serializerFor[chan int](FormatJSON)
		if err != nil {
			t.Fatalf("serializerFor = %v", err)
		}
		if _, err := s.Serialize("t", make(chan int)); !errors.Is(err, messaging.ErrSerialization) {
			t.Errorf("Serialize error = %v, want ErrSerialization", err)
		}
	})
}

func TestDeserializerForRejectsMismatchedType(t *testing.T) {
	t.Parallel()

	t.Run("bytes with string", func(t *testing.T) {
		t.Parallel()
		if _, err := deserializerFor[string](FormatBytes); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("deserializerFor[string](bytes) error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("string with int", func(t *testing.T) {
		t.Parallel()
		if _, err := deserializerFor[int](FormatString); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("deserializerFor[int](string) error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("unknown format", func(t *testing.T) {
		t.Parallel()
		if _, err := deserializerFor[string]("yaml"); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("deserializerFor[string](yaml) error = %v, want ErrInvalidConfig", err)
		}
	})
}

func TestDeserializers(t *testing.T) {
	t.Parallel()

	t.Run("bytes passes through", func(t *testing.T) {
		t.Parallel()
		d, err := deserializerFor[[]byte](FormatBytes)
		if err != nil {
			t.Fatalf("deserializerFor = %v", err)
		}
		got, err := d.Deserialize("t", []byte("hello"))
		if err != nil {
			t.Fatalf("Deserialize = %v", err)
		}
		if string(got) != "hello" {
			t.Errorf("Deserialize = %q, want %q", got, "hello")
		}
	})

	t.Run("string decodes utf8", func(t *testing.T) {
		t.Parallel()
		d, err := deserializerFor[string](FormatString)
		if err != nil {
			t.Fatalf("deserializerFor = %v", err)
		}
		got, err := d.Deserialize("t", []byte("héllo"))
		if err != nil {
			t.Fatalf("Deserialize = %v", err)
		}
		if got != "héllo" {
			t.Errorf("Deserialize = %q, want %q", got, "héllo")
		}
	})

	t.Run("json unmarshals struct", func(t *testing.T) {
		t.Parallel()
		type payload struct {
			ID int `json:"id"`
		}
		d, err := deserializerFor[payload](FormatJSON)
		if err != nil {
			t.Fatalf("deserializerFor = %v", err)
		}
		got, err := d.Deserialize("t", []byte(`{"id":7}`))
		if err != nil {
			t.Fatalf("Deserialize = %v", err)
		}
		if got.ID != 7 {
			t.Errorf("Deserialize = %+v, want ID 7", got)
		}
	})

	t.Run("json reports malformed input", func(t *testing.T) {
		t.Parallel()
		d, err := deserializerFor[struct{}](FormatJSON)
		if err != nil {
			t.Fatalf("deserializerFor = %v", err)
		}
		if _, err := d.Deserialize("t", []byte("not json")); !errors.Is(err, messaging.ErrDeserialization) {
			t.Errorf("Deserialize error = %v, want ErrDeserialization", err)
		}
	})
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	t.Run("invalid config", func(t *testing.T) {
		t.Parallel()
		_, err := New[string, []byte](ProducerConfig{})
		if !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("New error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("format type mismatch", func(t *testing.T) {
		t.Parallel()
		_, err := New[int, []byte](ProducerConfig{
			BootstrapServers: "localhost:9092",
			KeyFormat:        FormatString,
			ValueFormat:      FormatBytes,
		})
		if !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("New error = %v, want ErrInvalidConfig", err)
		}
	})
}

func TestNewConsumerRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	t.Run("invalid config", func(t *testing.T) {
		t.Parallel()
		_, err := NewConsumer[string, []byte](ConsumerConfig{})
		if !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("NewConsumer error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("format type mismatch", func(t *testing.T) {
		t.Parallel()
		_, err := NewConsumer[int, []byte](ConsumerConfig{
			BootstrapServers: "localhost:9092",
			GroupID:          "orders",
			Topics:           []string{"orders.v1"},
			KeyFormat:        FormatString,
			ValueFormat:      FormatBytes,
		})
		if !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("NewConsumer error = %v, want ErrInvalidConfig", err)
		}
	})
}
