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
		if s, ok := any(bytesSerializer{}).(Serializer[T]); ok {
			return s, nil
		}
		return nil, fmt.Errorf("%w: format %q requires []byte, got %T", messaging.ErrInvalidConfig, format, zero)
	case FormatString:
		if s, ok := any(stringSerializer{}).(Serializer[T]); ok {
			return s, nil
		}
		return nil, fmt.Errorf("%w: format %q requires string, got %T", messaging.ErrInvalidConfig, format, zero)
	case FormatJSON:
		return jsonSerializer[T]{}, nil
	default:
		return nil, fmt.Errorf("%w: unknown format %q", messaging.ErrInvalidConfig, format)
	}
}

type bytesSerializer struct{}

func (bytesSerializer) Serialize(_ string, v []byte) ([]byte, error) { return v, nil }

type stringSerializer struct{}

func (stringSerializer) Serialize(_ string, v string) ([]byte, error) { return []byte(v), nil }

type jsonSerializer[T any] struct{}

func (jsonSerializer[T]) Serialize(_ string, v T) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: marshaling %T as json: %v", messaging.ErrSerialization, v, err)
	}
	return b, nil
}
