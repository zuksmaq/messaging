package producer

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
			name:    "negative flush timeout",
			mutate:  func(c *Config) { c.FlushTimeout = -time.Second },
			wantErr: true,
		},
		{
			name:    "negative produce timeout",
			mutate:  func(c *Config) { c.ProduceTimeout = -time.Second },
			wantErr: true,
		},
		{
			name:    "unknown security protocol",
			mutate:  func(c *Config) { c.Security.Protocol = "carrier-pigeon" },
			wantErr: true,
		},
		{
			name: "sasl without mechanism",
			mutate: func(c *Config) {
				c.Security = kafka.Security{Protocol: kafka.SecuritySASLSSL, Username: "u", Password: "p"}
			},
			wantErr: true,
		},
		{
			name: "sasl without credentials",
			mutate: func(c *Config) {
				c.Security = kafka.Security{Protocol: kafka.SecuritySASLSSL, Mechanism: "PLAIN"}
			},
			wantErr: true,
		},
		{
			name: "sasl fully specified",
			mutate: func(c *Config) {
				c.Security = kafka.Security{Protocol: kafka.SecuritySASLSSL, Mechanism: "PLAIN", Username: "u", Password: "p"}
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

func TestConfigWithDefaults(t *testing.T) {
	t.Parallel()

	got := Config{
		BootstrapServers: "localhost:9092",
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatJSON,
	}.withDefaults()

	if got.Security.Protocol != kafka.SecurityPlaintext {
		t.Errorf("Security.Protocol = %q, want %q", got.Security.Protocol, kafka.SecurityPlaintext)
	}
	if got.FlushTimeout != DefaultFlushTimeout {
		t.Errorf("FlushTimeout = %s, want %s", got.FlushTimeout, DefaultFlushTimeout)
	}
	if got.ProduceTimeout != DefaultProduceTimeout {
		t.Errorf("ProduceTimeout = %s, want %s", got.ProduceTimeout, DefaultProduceTimeout)
	}
}

func TestConfigWithDefaultsPreservesExplicitValues(t *testing.T) {
	t.Parallel()

	got := Config{
		BootstrapServers: "localhost:9092",
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatJSON,
		Security:         kafka.Security{Protocol: kafka.SecuritySSL},
		FlushTimeout:     time.Second,
		ProduceTimeout:   2 * time.Second,
	}.withDefaults()

	if got.Security.Protocol != kafka.SecuritySSL {
		t.Errorf("Security.Protocol = %q, want %q", got.Security.Protocol, kafka.SecuritySSL)
	}
	if got.FlushTimeout != time.Second {
		t.Errorf("FlushTimeout = %s, want 1s", got.FlushTimeout)
	}
	if got.ProduceTimeout != 2*time.Second {
		t.Errorf("ProduceTimeout = %s, want 2s", got.ProduceTimeout)
	}
}
