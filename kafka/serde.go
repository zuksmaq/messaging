package kafka

import (
	"encoding/json"
	"fmt"

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
)

// Serializer encodes a value of type T for the wire.
type Serializer[T any] interface {
	Serialize(topic string, v T) ([]byte, error)
}

// Deserializer decodes a value of type T from the wire.
type Deserializer[T any] interface {
	Deserialize(topic string, b []byte) (T, error)
}

// serializerFor resolves format to a Serializer[T], reporting
// ErrInvalidConfig if T cannot be encoded in that format.
//
// Format lives in ProducerConfig rather than being passed as a typed
// Serializer, so the pairing is checked here at construction time
// instead of by the compiler.
func serializerFor[T any](format Format) (Serializer[T], error) {
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
	default:
		return nil, fmt.Errorf("%w: unknown format %q", messaging.ErrInvalidConfig, format)
	}
}

// deserializerFor resolves format to a Deserializer[T], reporting
// ErrInvalidConfig if T cannot be decoded from that format. It mirrors
// serializerFor so a consumer rejects a bad type/format pairing at
// construction time.
func deserializerFor[T any](format Format) (Deserializer[T], error) {
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
