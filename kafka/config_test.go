package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/zuksmaq/messaging"
)

func TestProducerConfigValidate(t *testing.T) {
	t.Parallel()

	valid := ProducerConfig{
		BootstrapServers: "localhost:9092",
		KeyFormat:        FormatString,
		ValueFormat:      FormatJSON,
	}

	tests := []struct {
		name    string
		mutate  func(*ProducerConfig)
		wantErr bool
	}{
		{name: "valid minimal", mutate: func(*ProducerConfig) {}},
		{
			name:    "missing bootstrap servers",
			mutate:  func(c *ProducerConfig) { c.BootstrapServers = "" },
			wantErr: true,
		},
		{
			name:    "missing key format",
			mutate:  func(c *ProducerConfig) { c.KeyFormat = "" },
			wantErr: true,
		},
		{
			name:    "missing value format",
			mutate:  func(c *ProducerConfig) { c.ValueFormat = "" },
			wantErr: true,
		},
		{
			name:    "negative flush timeout",
			mutate:  func(c *ProducerConfig) { c.FlushTimeout = -time.Second },
			wantErr: true,
		},
		{
			name:    "negative produce timeout",
			mutate:  func(c *ProducerConfig) { c.ProduceTimeout = -time.Second },
			wantErr: true,
		},
		{
			name:    "unknown security protocol",
			mutate:  func(c *ProducerConfig) { c.Security.Protocol = "carrier-pigeon" },
			wantErr: true,
		},
		{
			name: "sasl without mechanism",
			mutate: func(c *ProducerConfig) {
				c.Security = Security{Protocol: SecuritySASLSSL, Username: "u", Password: "p"}
			},
			wantErr: true,
		},
		{
			name: "sasl without credentials",
			mutate: func(c *ProducerConfig) {
				c.Security = Security{Protocol: SecuritySASLSSL, Mechanism: "PLAIN"}
			},
			wantErr: true,
		},
		{
			name: "sasl fully specified",
			mutate: func(c *ProducerConfig) {
				c.Security = Security{Protocol: SecuritySASLSSL, Mechanism: "PLAIN", Username: "u", Password: "p"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			tt.mutate(&cfg)

			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("Validate() = nil, want error")
				}
				if !errors.Is(err, messaging.ErrInvalidConfig) {
					t.Errorf("Validate() error = %v, want it to wrap ErrInvalidConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestConsumerConfigValidate(t *testing.T) {
	t.Parallel()

	valid := ConsumerConfig{
		BootstrapServers: "localhost:9092",
		GroupID:          "orders",
		Topics:           []string{"orders.v1"},
		KeyFormat:        FormatString,
		ValueFormat:      FormatJSON,
	}

	tests := []struct {
		name    string
		mutate  func(*ConsumerConfig)
		wantErr bool
	}{
		{name: "valid minimal", mutate: func(*ConsumerConfig) {}},
		{
			name:    "missing bootstrap servers",
			mutate:  func(c *ConsumerConfig) { c.BootstrapServers = "" },
			wantErr: true,
		},
		{
			name:    "missing group id",
			mutate:  func(c *ConsumerConfig) { c.GroupID = "" },
			wantErr: true,
		},
		{
			name:    "no topics",
			mutate:  func(c *ConsumerConfig) { c.Topics = nil },
			wantErr: true,
		},
		{
			name:    "empty topic name",
			mutate:  func(c *ConsumerConfig) { c.Topics = []string{"orders.v1", ""} },
			wantErr: true,
		},
		{
			name:    "missing key format",
			mutate:  func(c *ConsumerConfig) { c.KeyFormat = "" },
			wantErr: true,
		},
		{
			name:    "missing value format",
			mutate:  func(c *ConsumerConfig) { c.ValueFormat = "" },
			wantErr: true,
		},
		{
			name:    "unknown offset reset",
			mutate:  func(c *ConsumerConfig) { c.OffsetReset = "whenever" },
			wantErr: true,
		},
		{
			name:   "explicit offset reset",
			mutate: func(c *ConsumerConfig) { c.OffsetReset = OffsetLatest },
		},
		{
			name:    "negative ready check timeout",
			mutate:  func(c *ConsumerConfig) { c.ReadyCheckTimeout = -time.Second },
			wantErr: true,
		},
		{
			name:    "unknown security protocol",
			mutate:  func(c *ConsumerConfig) { c.Security.Protocol = "carrier-pigeon" },
			wantErr: true,
		},
		{
			name: "sasl without credentials",
			mutate: func(c *ConsumerConfig) {
				c.Security = Security{Protocol: SecuritySASLSSL, Mechanism: "PLAIN"}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			tt.mutate(&cfg)

			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("Validate() = nil, want error")
				}
				if !errors.Is(err, messaging.ErrInvalidConfig) {
					t.Errorf("Validate() error = %v, want it to wrap ErrInvalidConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestConsumerConfigWithDefaults(t *testing.T) {
	t.Parallel()

	got := ConsumerConfig{
		BootstrapServers: "localhost:9092",
		GroupID:          "orders",
		Topics:           []string{"orders.v1"},
		KeyFormat:        FormatString,
		ValueFormat:      FormatJSON,
	}.withDefaults()

	if got.Security.Protocol != SecurityPlaintext {
		t.Errorf("Security.Protocol = %q, want %q", got.Security.Protocol, SecurityPlaintext)
	}
	if got.OffsetReset != OffsetEarliest {
		t.Errorf("OffsetReset = %q, want %q", got.OffsetReset, OffsetEarliest)
	}
	if got.ReadyCheckTimeout != DefaultReadyCheckTimeout {
		t.Errorf("ReadyCheckTimeout = %s, want %s", got.ReadyCheckTimeout, DefaultReadyCheckTimeout)
	}
}

// Auto-commit must be off unconditionally: there is no config knob for
// it, and the client settings must reflect that.
func TestConsumerClientConfigDisablesAutoCommit(t *testing.T) {
	t.Parallel()

	cm := consumerClientConfig(ConsumerConfig{
		BootstrapServers: "localhost:9092",
		GroupID:          "orders",
		Topics:           []string{"orders.v1"},
		KeyFormat:        FormatString,
		ValueFormat:      FormatJSON,
	}.withDefaults())

	got, err := cm.Get("enable.auto.commit", true)
	if err != nil {
		t.Fatalf("Get(enable.auto.commit) = %v", err)
	}
	if got != false {
		t.Errorf("enable.auto.commit = %v, want false", got)
	}
}

func TestProducerConfigWithDefaults(t *testing.T) {
	t.Parallel()

	got := ProducerConfig{
		BootstrapServers: "localhost:9092",
		KeyFormat:        FormatString,
		ValueFormat:      FormatJSON,
	}.withDefaults()

	if got.Security.Protocol != SecurityPlaintext {
		t.Errorf("Security.Protocol = %q, want %q", got.Security.Protocol, SecurityPlaintext)
	}
	if got.FlushTimeout != DefaultFlushTimeout {
		t.Errorf("FlushTimeout = %s, want %s", got.FlushTimeout, DefaultFlushTimeout)
	}
	if got.ProduceTimeout != DefaultProduceTimeout {
		t.Errorf("ProduceTimeout = %s, want %s", got.ProduceTimeout, DefaultProduceTimeout)
	}
}

func TestProducerConfigWithDefaultsPreservesExplicitValues(t *testing.T) {
	t.Parallel()

	got := ProducerConfig{
		BootstrapServers: "localhost:9092",
		KeyFormat:        FormatString,
		ValueFormat:      FormatJSON,
		Security:         Security{Protocol: SecuritySSL},
		FlushTimeout:     time.Second,
		ProduceTimeout:   2 * time.Second,
	}.withDefaults()

	if got.Security.Protocol != SecuritySSL {
		t.Errorf("Security.Protocol = %q, want %q", got.Security.Protocol, SecuritySSL)
	}
	if got.FlushTimeout != time.Second {
		t.Errorf("FlushTimeout = %s, want 1s", got.FlushTimeout)
	}
	if got.ProduceTimeout != 2*time.Second {
		t.Errorf("ProduceTimeout = %s, want 2s", got.ProduceTimeout)
	}
}
