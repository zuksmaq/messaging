package consumer

import (
	"errors"
	"testing"
	"time"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	valid := Config{
		BootstrapServers: "localhost:9092",
		GroupID:          "orders",
		Topics:           []string{"orders.v1"},
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatJSON,
	}

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{name: "valid minimal", mutate: func(*Config) {}},
		{
			name:    "missing bootstrap servers",
			mutate:  func(c *Config) { c.BootstrapServers = "" },
			wantErr: true,
		},
		{
			name:    "missing group id",
			mutate:  func(c *Config) { c.GroupID = "" },
			wantErr: true,
		},
		{
			name:    "no topics",
			mutate:  func(c *Config) { c.Topics = nil },
			wantErr: true,
		},
		{
			name:    "empty topic name",
			mutate:  func(c *Config) { c.Topics = []string{"orders.v1", ""} },
			wantErr: true,
		},
		{
			name:    "missing key format",
			mutate:  func(c *Config) { c.KeyFormat = "" },
			wantErr: true,
		},
		{
			name:    "missing value format",
			mutate:  func(c *Config) { c.ValueFormat = "" },
			wantErr: true,
		},
		{
			name:    "unknown offset reset",
			mutate:  func(c *Config) { c.OffsetReset = "whenever" },
			wantErr: true,
		},
		{
			name:   "explicit offset reset",
			mutate: func(c *Config) { c.OffsetReset = OffsetLatest },
		},
		{
			name:    "negative ready check timeout",
			mutate:  func(c *Config) { c.ReadyCheckTimeout = -time.Second },
			wantErr: true,
		},
		{
			name:    "unknown security protocol",
			mutate:  func(c *Config) { c.Security.Protocol = "carrier-pigeon" },
			wantErr: true,
		},
		{
			name: "sasl without credentials",
			mutate: func(c *Config) {
				c.Security = kafka.Security{Protocol: kafka.SecuritySASLSSL, Mechanism: "PLAIN"}
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

func TestConfigWithDefaults(t *testing.T) {
	t.Parallel()

	got := Config{
		BootstrapServers: "localhost:9092",
		GroupID:          "orders",
		Topics:           []string{"orders.v1"},
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatJSON,
	}.withDefaults()

	if got.Security.Protocol != kafka.SecurityPlaintext {
		t.Errorf("Security.Protocol = %q, want %q", got.Security.Protocol, kafka.SecurityPlaintext)
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

// Auto-commit must be off unconditionally: there is no config knob for
// it, and the client settings must reflect that.
func TestConsumerClientConfigDisablesAutoCommit(t *testing.T) {
	t.Parallel()

	cm := clientConfig(Config{
		BootstrapServers: "localhost:9092",
		GroupID:          "orders",
		Topics:           []string{"orders.v1"},
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatJSON,
	}.withDefaults())

	got, err := cm.Get("enable.auto.commit", true)
	if err != nil {
		t.Fatalf("Get(enable.auto.commit) = %v", err)
	}
	if got != false {
		t.Errorf("enable.auto.commit = %v, want false", got)
	}
}
