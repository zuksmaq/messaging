package messaging

import "errors"

// Sentinel errors for the categories callers need to branch on via
// errors.Is/errors.As. Wrap these with context as they travel up
// (fmt.Errorf("...: %w", err)); never compare with ==.
var (
	// ErrSerialization indicates a value could not be encoded for
	// the wire.
	ErrSerialization = errors.New("serialization failed")

	// ErrDeserialization indicates a received value could not be
	// decoded from the wire.
	ErrDeserialization = errors.New("deserialization failed")

	// ErrSchemaRegistryRequired indicates the configured wire format
	// requires a schema registry client that was not provided.
	ErrSchemaRegistryRequired = errors.New("schema registry required")

	// ErrInvalidConfig indicates a config's Validate method rejected
	// it.
	ErrInvalidConfig = errors.New("invalid configuration")

	// ErrBroker indicates the underlying broker client returned an
	// error producing or consuming a message.
	ErrBroker = errors.New("broker error")
)
