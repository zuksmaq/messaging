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
		if _, err := SerializerFor[string](FormatBytes); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("SerializerFor[string](bytes) error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("string with int", func(t *testing.T) {
		t.Parallel()
		if _, err := SerializerFor[int](FormatString); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("SerializerFor[int](string) error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("unknown format", func(t *testing.T) {
		t.Parallel()
		if _, err := SerializerFor[string]("yaml"); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("SerializerFor[string](yaml) error = %v, want ErrInvalidConfig", err)
		}
	})
}

func TestSerializers(t *testing.T) {
	t.Parallel()

	t.Run("bytes passes through", func(t *testing.T) {
		t.Parallel()
		s, err := SerializerFor[[]byte](FormatBytes)
		if err != nil {
			t.Fatalf("SerializerFor = %v", err)
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
		s, err := SerializerFor[string](FormatString)
		if err != nil {
			t.Fatalf("SerializerFor = %v", err)
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
		s, err := SerializerFor[payload](FormatJSON)
		if err != nil {
			t.Fatalf("SerializerFor = %v", err)
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
		s, err := SerializerFor[chan int](FormatJSON)
		if err != nil {
			t.Fatalf("SerializerFor = %v", err)
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
		if _, err := DeserializerFor[string](FormatBytes); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("DeserializerFor[string](bytes) error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("string with int", func(t *testing.T) {
		t.Parallel()
		if _, err := DeserializerFor[int](FormatString); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("DeserializerFor[int](string) error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("unknown format", func(t *testing.T) {
		t.Parallel()
		if _, err := DeserializerFor[string]("yaml"); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Errorf("DeserializerFor[string](yaml) error = %v, want ErrInvalidConfig", err)
		}
	})
}

func TestDeserializers(t *testing.T) {
	t.Parallel()

	t.Run("bytes passes through", func(t *testing.T) {
		t.Parallel()
		d, err := DeserializerFor[[]byte](FormatBytes)
		if err != nil {
			t.Fatalf("DeserializerFor = %v", err)
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
		d, err := DeserializerFor[string](FormatString)
		if err != nil {
			t.Fatalf("DeserializerFor = %v", err)
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
		d, err := DeserializerFor[payload](FormatJSON)
		if err != nil {
			t.Fatalf("DeserializerFor = %v", err)
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
		d, err := DeserializerFor[struct{}](FormatJSON)
		if err != nil {
			t.Fatalf("DeserializerFor = %v", err)
		}
		if _, err := d.Deserialize("t", []byte("not json")); !errors.Is(err, messaging.ErrDeserialization) {
			t.Errorf("Deserialize error = %v, want ErrDeserialization", err)
		}
	})
}
