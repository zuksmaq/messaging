package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/zuksmaq/messaging"
)

// Format selects the wire encoding for a message key or value.
type Format string

const (
	// FormatBytes passes []byte through unchanged.
	FormatBytes Format = "bytes"
	// FormatString encodes a string as its UTF-8 bytes.
	FormatString Format = "string"
	// FormatJSON encodes any type with encoding/json.
	FormatJSON Format = "json"
	// FormatAvro encodes a struct as Confluent-framed Avro, registering
	// its schema in a Schema Registry.
	FormatAvro Format = "avro"
)

// NeedsSchemaRegistry reports whether this format can only be encoded
// with a Schema Registry behind it.
func (f Format) NeedsSchemaRegistry() bool { return f == FormatAvro }

// Part names which half of a message a codec handles. Avro derives the
// Schema Registry subject from it ("<topic>-key" or "<topic>-value"), so
// the two halves of a topic carry independent schemas.
type Part int

const (
	// KeyPart is the message key.
	KeyPart Part = iota
	// ValuePart is the message value.
	ValuePart
)

func (p Part) serdeType() serde.Type {
	if p == KeyPart {
		return serde.KeySerde
	}
	return serde.ValueSerde
}

// Serializer encodes a value of type T for the wire.
type Serializer[T any] interface {
	Serialize(topic string, v T) ([]byte, error)
}

// Deserializer decodes a value of type T from the wire.
type Deserializer[T any] interface {
	Deserialize(topic string, b []byte) (T, error)
}

// SerializerFor resolves format to a Serializer[T], reporting
// ErrInvalidConfig if T cannot be encoded in that format.
//
// Format lives on the producer's and consumer's Config rather than
// being passed as a typed Serializer, so the pairing is checked here at
// construction time instead of by the compiler.
//
// part and sr matter only to the formats that need a Schema Registry:
// sr may be nil for the others, and Avro reports
// messaging.ErrSchemaRegistryRequired when it is nil.
func SerializerFor[T any](format Format, part Part, sr *SchemaRegistry) (Serializer[T], error) {
	var zero T
	switch format {
	case FormatBytes:
		if s, ok := any(bytesCodec{}).(Serializer[T]); ok {
			return s, nil
		}
		return nil, fmt.Errorf("%w: format %q requires []byte, got %T", messaging.ErrInvalidConfig, format, zero)
	case FormatString:
		if s, ok := any(stringCodec{}).(Serializer[T]); ok {
			return s, nil
		}
		return nil, fmt.Errorf("%w: format %q requires string, got %T", messaging.ErrInvalidConfig, format, zero)
	case FormatJSON:
		return jsonCodec[T]{}, nil
	case FormatAvro:
		return avroSerializerFor[T](part, sr)
	default:
		return nil, fmt.Errorf("%w: unknown format %q", messaging.ErrInvalidConfig, format)
	}
}

// DeserializerFor resolves format to a Deserializer[T], reporting
// ErrInvalidConfig if T cannot be decoded from that format. It mirrors
// SerializerFor so a consumer rejects a bad type/format pairing at
// construction time.
func DeserializerFor[T any](format Format, part Part, sr *SchemaRegistry) (Deserializer[T], error) {
	var zero T
	switch format {
	case FormatBytes:
		if d, ok := any(bytesCodec{}).(Deserializer[T]); ok {
			return d, nil
		}
		return nil, fmt.Errorf("%w: format %q requires []byte, got %T", messaging.ErrInvalidConfig, format, zero)
	case FormatString:
		if d, ok := any(stringCodec{}).(Deserializer[T]); ok {
			return d, nil
		}
		return nil, fmt.Errorf("%w: format %q requires string, got %T", messaging.ErrInvalidConfig, format, zero)
	case FormatJSON:
		return jsonCodec[T]{}, nil
	case FormatAvro:
		return avroDeserializerFor[T](part, sr)
	default:
		return nil, fmt.Errorf("%w: unknown format %q", messaging.ErrInvalidConfig, format)
	}
}

type bytesCodec struct{}

func (bytesCodec) Serialize(_ string, v []byte) ([]byte, error) { return v, nil }

func (bytesCodec) Deserialize(_ string, b []byte) ([]byte, error) { return b, nil }

type stringCodec struct{}

func (stringCodec) Serialize(_ string, v string) ([]byte, error) { return []byte(v), nil }

func (stringCodec) Deserialize(_ string, b []byte) (string, error) { return string(b), nil }

type jsonCodec[T any] struct{}

func (jsonCodec[T]) Serialize(_ string, v T) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: marshaling %T as json: %v", messaging.ErrSerialization, v, err)
	}
	return b, nil
}

func (jsonCodec[T]) Deserialize(_ string, b []byte) (T, error) {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return v, fmt.Errorf("%w: unmarshaling json into %T: %v", messaging.ErrDeserialization, v, err)
	}
	return v, nil
}
