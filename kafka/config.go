package kafka

import (
	"fmt"
	"time"

	"github.com/zuksmaq/messaging"
)

// SecurityProtocol selects the transport and authentication mechanism
// used to reach the brokers.
type SecurityProtocol string

const (
	// SecurityPlaintext is unauthenticated, unencrypted transport.
	SecurityPlaintext SecurityProtocol = "plaintext"
	// SecuritySSL is TLS transport without SASL.
	SecuritySSL SecurityProtocol = "ssl"
	// SecuritySASLPlaintext is SASL authentication over plaintext.
	SecuritySASLPlaintext SecurityProtocol = "sasl_plaintext"
	// SecuritySASLSSL is SASL authentication over TLS.
	SecuritySASLSSL SecurityProtocol = "sasl_ssl"
)

// Security holds the broker connection credentials. Mechanism,
// Username and Password are required for the SASL protocols and
// ignored otherwise.
type Security struct {
	Protocol  SecurityProtocol
	Mechanism string
	Username  string
	Password  string
	// CALocation is an optional path to a CA certificate bundle for
	// the TLS protocols; empty uses the system trust store.
	CALocation string
}

func (s Security) sasl() bool {
	return s.Protocol == SecuritySASLPlaintext || s.Protocol == SecuritySASLSSL
}

// Validate reports whether the security settings are self-consistent.
// An empty Protocol is valid: it selects SecurityPlaintext.
func (s Security) Validate() error {
	switch s.Protocol {
	case "", SecurityPlaintext, SecuritySSL, SecuritySASLPlaintext, SecuritySASLSSL:
	default:
		return fmt.Errorf("%w: unknown security protocol %q", messaging.ErrInvalidConfig, s.Protocol)
	}
	if s.sasl() {
		if s.Mechanism == "" {
			return fmt.Errorf("%w: security mechanism is required for protocol %q", messaging.ErrInvalidConfig, s.Protocol)
		}
		if s.Username == "" || s.Password == "" {
			return fmt.Errorf("%w: security username and password are required for protocol %q", messaging.ErrInvalidConfig, s.Protocol)
		}
	}
	return nil
}

// Defaults applied by New and NewConsumer when the corresponding field
// is zero.
const (
	DefaultFlushTimeout      = 30 * time.Second
	DefaultProduceTimeout    = 30 * time.Second
	DefaultReadyCheckTimeout = 30 * time.Second
)

// ProducerConfig configures a Producer. BootstrapServers, KeyFormat
// and ValueFormat are mandatory; the remaining fields have documented
// defaults applied by New.
type ProducerConfig struct {
	// BootstrapServers is the comma-separated broker list.
	BootstrapServers string

	// KeyFormat and ValueFormat are independent — a String key with a
	// JSON value is valid.
	KeyFormat   Format
	ValueFormat Format

	// Security defaults to SecurityPlaintext when Protocol is empty.
	Security Security

	// FlushTimeout bounds how long Close waits for un-acknowledged
	// messages. Defaults to DefaultFlushTimeout.
	FlushTimeout time.Duration

	// ProduceTimeout bounds how long Produce waits for a broker
	// acknowledgement when ctx has no earlier deadline. Defaults to
	// DefaultProduceTimeout.
	ProduceTimeout time.Duration
}

// Validate reports whether the mandatory fields are present and the
// optional ones are self-consistent. New calls it and refuses to
// construct an invalid producer.
func (c ProducerConfig) Validate() error {
	if c.BootstrapServers == "" {
		return fmt.Errorf("%w: bootstrap servers are required", messaging.ErrInvalidConfig)
	}
	if c.KeyFormat == "" {
		return fmt.Errorf("%w: key format is required", messaging.ErrInvalidConfig)
	}
	if c.ValueFormat == "" {
		return fmt.Errorf("%w: value format is required", messaging.ErrInvalidConfig)
	}
	if c.FlushTimeout < 0 {
		return fmt.Errorf("%w: flush timeout must not be negative", messaging.ErrInvalidConfig)
	}
	if c.ProduceTimeout < 0 {
		return fmt.Errorf("%w: produce timeout must not be negative", messaging.ErrInvalidConfig)
	}
	return c.Security.Validate()
}

// OffsetReset selects where a consumer group starts reading a partition
// for which it has no committed offset.
type OffsetReset string

const (
	// OffsetEarliest starts at the oldest retained message.
	OffsetEarliest OffsetReset = "earliest"
	// OffsetLatest starts at the next message produced.
	OffsetLatest OffsetReset = "latest"
)

// ConsumerConfig configures a Consumer. BootstrapServers, GroupID,
// Topics, KeyFormat and ValueFormat are mandatory; the remaining fields
// have documented defaults applied by NewConsumer.
//
// There is deliberately no auto-commit setting: offsets advance only
// when the caller calls Commit.
type ConsumerConfig struct {
	// BootstrapServers is the comma-separated broker list.
	BootstrapServers string

	// GroupID is the consumer group the consumer joins.
	GroupID string

	// Topics are subscribed to at construction time.
	Topics []string

	// KeyFormat and ValueFormat are independent — a String key with a
	// JSON value is valid.
	KeyFormat   Format
	ValueFormat Format

	// Security defaults to SecurityPlaintext when Protocol is empty.
	Security Security

	// OffsetReset defaults to OffsetEarliest, so a new group reads the
	// backlog rather than silently skipping it.
	OffsetReset OffsetReset

	// ReadyCheckTimeout bounds ReadyCheck when ctx has no earlier
	// deadline. Defaults to DefaultReadyCheckTimeout.
	ReadyCheckTimeout time.Duration
}

// Validate reports whether the mandatory fields are present and the
// optional ones are self-consistent. NewConsumer calls it and refuses to
// construct an invalid consumer.
func (c ConsumerConfig) Validate() error {
	if c.BootstrapServers == "" {
		return fmt.Errorf("%w: bootstrap servers are required", messaging.ErrInvalidConfig)
	}
	if c.GroupID == "" {
		return fmt.Errorf("%w: group id is required", messaging.ErrInvalidConfig)
	}
	if len(c.Topics) == 0 {
		return fmt.Errorf("%w: at least one topic is required", messaging.ErrInvalidConfig)
	}
	for _, t := range c.Topics {
		if t == "" {
			return fmt.Errorf("%w: topics must not contain an empty name", messaging.ErrInvalidConfig)
		}
	}
	if c.KeyFormat == "" {
		return fmt.Errorf("%w: key format is required", messaging.ErrInvalidConfig)
	}
	if c.ValueFormat == "" {
		return fmt.Errorf("%w: value format is required", messaging.ErrInvalidConfig)
	}
	switch c.OffsetReset {
	case "", OffsetEarliest, OffsetLatest:
	default:
		return fmt.Errorf("%w: unknown offset reset %q", messaging.ErrInvalidConfig, c.OffsetReset)
	}
	if c.ReadyCheckTimeout < 0 {
		return fmt.Errorf("%w: ready check timeout must not be negative", messaging.ErrInvalidConfig)
	}
	return c.Security.Validate()
}

// withDefaults returns a copy with zero-valued optional fields
// replaced by their defaults.
func (c ConsumerConfig) withDefaults() ConsumerConfig {
	if c.Security.Protocol == "" {
		c.Security.Protocol = SecurityPlaintext
	}
	if c.OffsetReset == "" {
		c.OffsetReset = OffsetEarliest
	}
	if c.ReadyCheckTimeout == 0 {
		c.ReadyCheckTimeout = DefaultReadyCheckTimeout
	}
	return c
}

// withDefaults returns a copy with zero-valued optional fields
// replaced by their defaults.
func (c ProducerConfig) withDefaults() ProducerConfig {
	if c.Security.Protocol == "" {
		c.Security.Protocol = SecurityPlaintext
	}
	if c.FlushTimeout == 0 {
		c.FlushTimeout = DefaultFlushTimeout
	}
	if c.ProduceTimeout == 0 {
		c.ProduceTimeout = DefaultProduceTimeout
	}
	return c
}
