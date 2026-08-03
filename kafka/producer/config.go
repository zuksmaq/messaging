package producer

import (
	"fmt"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
)

// Defaults applied by New when the corresponding field is zero.
const (
	DefaultFlushTimeout   = 30 * time.Second
	DefaultProduceTimeout = 30 * time.Second
)

// Config configures a Producer. BootstrapServers, KeyFormat and
// ValueFormat are mandatory; the remaining fields have documented
// defaults applied by New.
type Config struct {
	// BootstrapServers is the comma-separated broker list.
	BootstrapServers string

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
func (c Config) Validate() error {
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
	if err := kafka.ValidateSchemaRegistry(c.SchemaRegistry, c.KeyFormat, c.ValueFormat); err != nil {
		return err
	}
	return c.Security.Validate()
}

// withDefaults returns a copy with zero-valued optional fields
// replaced by their defaults.
func (c Config) withDefaults() Config {
	c.Security = c.Security.WithDefaults()
	if c.FlushTimeout == 0 {
		c.FlushTimeout = DefaultFlushTimeout
	}
	if c.ProduceTimeout == 0 {
		c.ProduceTimeout = DefaultProduceTimeout
	}
	return c
}

// clientConfig maps a Config onto librdkafka settings. Idempotence is
// always on: it pins acks=all and a max-in-flight consistent with
// ordering, so a delivered message is durably replicated.
func clientConfig(cfg Config) *ckafka.ConfigMap {
	cm := &ckafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"enable.idempotence": true,
	}
	// SetKey's error is unreachable here: the keys are literals and the
	// values are plain strings.
	for k, v := range cfg.Security.Settings() {
		_ = cm.SetKey(k, v)
	}
	return cm
}
