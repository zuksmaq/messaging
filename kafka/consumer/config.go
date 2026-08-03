package consumer

import (
	"fmt"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
)

// DefaultReadyCheckTimeout is applied by New when ReadyCheckTimeout is
// zero.
const DefaultReadyCheckTimeout = 30 * time.Second

// OffsetReset selects where a consumer group starts reading a partition
// for which it has no committed offset.
type OffsetReset string

const (
	// OffsetEarliest starts at the oldest retained message.
	OffsetEarliest OffsetReset = "earliest"
	// OffsetLatest starts at the next message produced.
	OffsetLatest OffsetReset = "latest"
)

// Config configures a Consumer. BootstrapServers, GroupID, Topics,
// KeyFormat and ValueFormat are mandatory; the remaining fields have
// documented defaults applied by New.
//
// There is deliberately no auto-commit setting: offsets advance only
// when the caller calls Commit.
type Config struct {
	// BootstrapServers is the comma-separated broker list.
	BootstrapServers string

	// GroupID is the consumer group the consumer joins.
	GroupID string

	// Topics are subscribed to at construction time.
	Topics []string

	// KeyFormat and ValueFormat are independent — a String key with a
	// JSON value is valid.
	KeyFormat   kafka.Format
	ValueFormat kafka.Format

	// Security defaults to kafka.SecurityPlaintext when Protocol is
	// empty.
	Security kafka.Security

	// SchemaRegistry is required when KeyFormat or ValueFormat needs one
	// (kafka.FormatAvro) and ignored otherwise.
	SchemaRegistry kafka.SchemaRegistryConfig

	// OffsetReset defaults to OffsetEarliest, so a new group reads the
	// backlog rather than silently skipping it.
	OffsetReset OffsetReset

	// ReadyCheckTimeout bounds ReadyCheck when ctx has no earlier
	// deadline. Defaults to DefaultReadyCheckTimeout.
	ReadyCheckTimeout time.Duration
}

// Validate reports whether the mandatory fields are present and the
// optional ones are self-consistent. New calls it and refuses to
// construct an invalid consumer.
func (c Config) Validate() error {
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
	if err := kafka.ValidateSchemaRegistry(c.SchemaRegistry, c.KeyFormat, c.ValueFormat); err != nil {
		return err
	}
	return c.Security.Validate()
}

// withDefaults returns a copy with zero-valued optional fields
// replaced by their defaults.
func (c Config) withDefaults() Config {
	c.Security = c.Security.WithDefaults()
	if c.OffsetReset == "" {
		c.OffsetReset = OffsetEarliest
	}
	if c.ReadyCheckTimeout == 0 {
		c.ReadyCheckTimeout = DefaultReadyCheckTimeout
	}
	return c
}

// clientConfig maps a Config onto librdkafka settings. Auto-commit is
// hard-wired off — there is no config knob for it.
func clientConfig(cfg Config) *ckafka.ConfigMap {
	cm := &ckafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"group.id":           cfg.GroupID,
		"enable.auto.commit": false,
		"auto.offset.reset":  string(cfg.OffsetReset),
	}
	// SetKey's error is unreachable here: the keys are literals and the
	// values are plain strings.
	for k, v := range cfg.Security.Settings() {
		_ = cm.SetKey(k, v)
	}
	return cm
}
