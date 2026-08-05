package kafka

import (
	"errors"
	"testing"

	"github.com/zuksmaq/messaging"
)

func TestSchemaRegistryConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  SchemaRegistryConfig
		want error
	}{
		{
			name: "unconfigured is valid",
			cfg:  SchemaRegistryConfig{},
		},
		{
			name: "url alone is valid",
			cfg:  SchemaRegistryConfig{URL: "http://localhost:8081"},
		},
		{
			name: "basic auth pair is valid",
			cfg:  SchemaRegistryConfig{URL: "http://localhost:8081", Username: "u", Password: "p"},
		},
		{
			name: "username without password",
			cfg:  SchemaRegistryConfig{URL: "http://localhost:8081", Username: "u"},
			want: messaging.ErrInvalidConfig,
		},
		{
			name: "password without username",
			cfg:  SchemaRegistryConfig{URL: "http://localhost:8081", Password: "p"},
			want: messaging.ErrInvalidConfig,
		},
		{
			name: "client certificate without its private half",
			cfg:  SchemaRegistryConfig{URL: "https://localhost:8081", CertificateLocation: "/tls/client.pem"},
			want: messaging.ErrInvalidConfig,
		},
		{
			name: "negative cache capacity",
			cfg:  SchemaRegistryConfig{URL: "http://localhost:8081", SchemaCacheCapacity: -1},
			want: messaging.ErrInvalidConfig,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.Validate()
			if tt.want == nil {
				if err != nil {
					t.Errorf("Validate = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("Validate = %v, want %v", err, tt.want)
			}
		})
	}
}

// TestNewSchemaRegistryWithoutURL asserts an unconfigured block yields no
// client, so the registry-less formats never pay for one.
func TestNewSchemaRegistryWithoutURL(t *testing.T) {
	t.Parallel()

	sr, err := NewSchemaRegistry(SchemaRegistryConfig{})
	if err != nil {
		t.Fatalf("NewSchemaRegistry = %v", err)
	}
	if sr != nil {
		t.Errorf("NewSchemaRegistry = %v, want nil for an unconfigured registry", sr)
	}
	if err := sr.Close(); err != nil {
		t.Errorf("Close on a nil registry = %v, want nil", err)
	}
}

func TestNewSchemaRegistryRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	_, err := NewSchemaRegistry(SchemaRegistryConfig{URL: "http://localhost:8081", Username: "u"})
	if !errors.Is(err, messaging.ErrInvalidConfig) {
		t.Errorf("NewSchemaRegistry error = %v, want ErrInvalidConfig", err)
	}
}

// TestZeroValueSchemaRegistryCloseIsSafe covers a &SchemaRegistry{} built
// by a DI container bypassing NewSchemaRegistry: it has no client, so
// Close must no-op rather than dereference a nil interface.
func TestZeroValueSchemaRegistryCloseIsSafe(t *testing.T) {
	t.Parallel()

	sr := &SchemaRegistry{}
	if err := sr.Close(); err != nil {
		t.Errorf("Close on a zero-value registry = %v, want nil", err)
	}
}
