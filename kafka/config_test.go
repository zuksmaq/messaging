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
