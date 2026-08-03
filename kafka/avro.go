package kafka

import (
	"fmt"
	"reflect"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/zuksmaq/messaging"
)

// Subjects follow the topic-name strategy ("<topic>-key" / "<topic>-value")
// rather than the client's newer default, which resolves the subject by
// querying a topic-to-subject association API that only some registries
// serve. The topic-name strategy is what every Confluent-compatible
// registry understands.

// avroSerializerFor builds an Avro serializer for T, or reports why T and
// the registry cannot support one.
func avroSerializerFor[T any](part Part, sr *SchemaRegistry) (Serializer[T], error) {
	if err := requireRegistry[T](sr); err != nil {
		return nil, err
	}
	conf := avrov2.NewSerializerConfig()
	conf.SubjectNameStrategyType = serde.TopicNameStrategyType
	s, err := avrov2.NewSerializer(sr.client, part.serdeType(), conf)
	if err != nil {
		return nil, fmt.Errorf("%w: creating avro serializer: %v", messaging.ErrInvalidConfig, err)
	}
	return avroSerializer[T]{ser: s}, nil
}

// avroDeserializerFor mirrors avroSerializerFor for the read side.
func avroDeserializerFor[T any](part Part, sr *SchemaRegistry) (Deserializer[T], error) {
	if err := requireRegistry[T](sr); err != nil {
		return nil, err
	}
	conf := avrov2.NewDeserializerConfig()
	conf.SubjectNameStrategyType = serde.TopicNameStrategyType
	d, err := avrov2.NewDeserializer(sr.client, part.serdeType(), conf)
	if err != nil {
		return nil, fmt.Errorf("%w: creating avro deserializer: %v", messaging.ErrInvalidConfig, err)
	}
	return avroDeserializer[T]{de: d}, nil
}

// requireRegistry rejects the two ways an Avro codec cannot be built: no
// registry to talk to, and a T whose schema cannot be derived. Avro maps
// a record onto a struct's exported fields, so T must be a struct — a
// pointer, slice or scalar has no record to describe.
func requireRegistry[T any](sr *SchemaRegistry) error {
	if sr == nil {
		return fmt.Errorf("%w: format %q", messaging.ErrSchemaRegistryRequired, FormatAvro)
	}
	var zero T
	if kind := reflect.TypeOf(&zero).Elem().Kind(); kind != reflect.Struct {
		return fmt.Errorf("%w: format %q requires a struct, got %s", messaging.ErrInvalidConfig, FormatAvro, kind)
	}
	return nil
}

type avroSerializer[T any] struct {
	ser *avrov2.Serializer
}

// Serialize encodes v as Confluent-framed Avro: a magic byte, the id of
// the schema derived from T, then the Avro body. The schema is registered
// under the topic's subject on first use, so a value the registry rejects
// as incompatible fails here rather than reaching the broker.
func (s avroSerializer[T]) Serialize(topic string, v T) ([]byte, error) {
	// avrov2 derives the schema by reflection and requires a pointer.
	b, err := s.ser.Serialize(topic, &v)
	if err != nil {
		return nil, fmt.Errorf("%w: encoding %T as avro for topic %q: %v", messaging.ErrSerialization, v, topic, err)
	}
	return b, nil
}

type avroDeserializer[T any] struct {
	de *avrov2.Deserializer
}

// Deserialize decodes Confluent-framed Avro into a T, fetching the
// writer's schema by the id in the frame.
func (d avroDeserializer[T]) Deserialize(topic string, b []byte) (T, error) {
	var v T
	if err := d.de.DeserializeInto(topic, b, &v); err != nil {
		return v, fmt.Errorf("%w: decoding %T from avro on topic %q: %v", messaging.ErrDeserialization, v, topic, err)
	}
	return v, nil
}
