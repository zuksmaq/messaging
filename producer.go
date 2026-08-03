package messaging

import "context"

// Producer publishes keyed messages to a broker. Produce awaits the
// broker's acknowledgement before returning; implementations must
// never fire-and-forget.
type Producer[K, V any] interface {
	// Produce publishes a message and blocks until the broker
	// acknowledges it, returning the assigned coordinates and
	// DeliveryStatus.
	Produce(ctx context.Context, topic string, key K, value V, headers map[string][]byte) (ProducedMessage, error)

	// Close flushes any buffered messages and releases the
	// underlying broker connection.
	Close(ctx context.Context) error
}
