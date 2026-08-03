package messaging

import "context"

// Consumer reads keyed messages from a broker. Auto-commit is
// intentionally absent from the contract: offsets advance only when
// the caller calls Commit after handling a message.
type Consumer[K, V any] interface {
	// Consume blocks until the next message is available.
	Consume(ctx context.Context) (ReceivedMessage[K, V], error)

	// Commit advances the consumer's offset past msg.
	Commit(ctx context.Context, msg ReceivedMessage[K, V]) error
}
