package messaging

import (
	"time"
)

// EventIDHeader is the header key carrying the producer-assigned
// idempotency id. The outbox relay stamps it from the outbox row id;
// consumers use it as the inbox de-duplication key.
const EventIDHeader = "event-id"

// DeliveryStatus reports how durably a produced message was persisted
// by the broker.
type DeliveryStatus int

const (
	// NotPersisted means the broker gave no acknowledgement that the
	// message was written.
	NotPersisted DeliveryStatus = iota
	// PossiblyPersisted means the broker acknowledged the write but
	// durability is not guaranteed (e.g. a single in-sync replica).
	PossiblyPersisted
	// Persisted means the broker acknowledged the write with the
	// durability guarantees required by the producer's configuration.
	Persisted
)

// String returns the human-readable name of the delivery status.
func (s DeliveryStatus) String() string {
	switch s {
	case NotPersisted:
		return "not_persisted"
	case PossiblyPersisted:
		return "possibly_persisted"
	case Persisted:
		return "persisted"
	default:
		return "unknown"
	}
}

// Message is the logical content of a produced or received record:
// a key, a value, and broker headers.
type Message[K, V any] struct {
	Key     K
	Value   V
	Headers map[string][]byte
}

// ProducedMessage is the broker's acknowledgement of a produced
// message.
type ProducedMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Status    DeliveryStatus
}

// ReceivedMessage is a message read from the broker, carrying its
// content plus the coordinates the broker assigned it.
type ReceivedMessage[K, V any] struct {
	Message[K, V]

	// RawKey and RawValue are the key and value exactly as the broker
	// delivered them, before deserialization. They survive a
	// deserialization failure, so a poison-message policy can forward
	// the original bytes to a dead-letter topic even when Key and Value
	// are still zero.
	RawKey   []byte
	RawValue []byte

	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
}

// Tombstone reports whether this message is a tombstone (a nil
// value), used by log-compacted topics to mark a key for deletion.
func (m ReceivedMessage[K, V]) Tombstone() bool {
	return m.RawValue == nil
}

// EventID returns the EventIDHeader value, or "" if absent.
func (m ReceivedMessage[K, V]) EventID() string {
	if b, ok := m.Headers[EventIDHeader]; ok {
		return string(b)
	}
	return ""
}
