package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/zuksmaq/messaging"
)

// DefaultSchemaCacheCapacity is the schema cache size applied when
// SchemaRegistryConfig leaves SchemaCacheCapacity zero.
const DefaultSchemaCacheCapacity = 1000

// SchemaRegistryConfig describes how to reach a Confluent Schema
// Registry. It is a distinct config block on the producer's and
// consumer's Config, required only when a key or value format needs a
// registry (FormatAvro) and ignored otherwise.
type SchemaRegistryConfig struct {
	// URL is the registry's base URL, e.g. "http://localhost:8081".
	// Setting it is what marks this block as configured.
	URL string

	// Username and Password authenticate with HTTP basic auth. Both or
	// neither must be set.
	Username string
	Password string

	// CALocation is an optional path to a CA certificate bundle for an
	// https URL; empty uses the system trust store.
	CALocation string

	// CertificateLocation and KeyLocation are an optional client
	// certificate and private key for mutual TLS. Both or neither must
	// be set.
	CertificateLocation string
	KeyLocation         string

	// SchemaCacheCapacity bounds how many schemas the client caches
	// locally. Defaults to DefaultSchemaCacheCapacity.
	SchemaCacheCapacity int
}

// Configured reports whether this block names a registry at all.
func (c SchemaRegistryConfig) Configured() bool { return c.URL != "" }

// Validate reports whether the settings are self-consistent. An
// unconfigured block is valid: only a format that needs a registry makes
// one mandatory, which is what ValidateSchemaRegistry checks.
func (c SchemaRegistryConfig) Validate() error {
	if !c.Configured() {
		return nil
	}
	if (c.Username == "") != (c.Password == "") {
		return fmt.Errorf("%w: schema registry username and password must be set together", messaging.ErrInvalidConfig)
	}
	if (c.CertificateLocation == "") != (c.KeyLocation == "") {
		return fmt.Errorf("%w: schema registry certificate and key locations must be set together", messaging.ErrInvalidConfig)
	}
	if c.SchemaCacheCapacity < 0 {
		return fmt.Errorf("%w: schema cache capacity must not be negative", messaging.ErrInvalidConfig)
	}
	return nil
}

// ValidateSchemaRegistry reports a configuration error when any of
// formats can only be encoded with a registry behind it and cfg does not
// describe one. The producer's and consumer's Validate both call it, so
// the misconfiguration surfaces at construction rather than at the first
// publish or consume.
func ValidateSchemaRegistry(cfg SchemaRegistryConfig, formats ...Format) error {
	for _, f := range formats {
		if f.NeedsSchemaRegistry() && !cfg.Configured() {
			return fmt.Errorf("%w: format %q needs a schema registry url", messaging.ErrSchemaRegistryRequired, f)
		}
	}
	return cfg.Validate()
}

// SchemaRegistry is a Schema Registry client, shared by the key and value
// codecs of one producer or consumer. Pass it to SerializerFor and
// DeserializerFor.
type SchemaRegistry struct {
	client schemaregistry.Client
}

// NewSchemaRegistry builds a client from cfg. A cfg that names no URL
// describes no registry and yields a nil *SchemaRegistry, which the
// Bytes, String and JSON formats are content with; Avro reports
// messaging.ErrSchemaRegistryRequired for it.
func NewSchemaRegistry(cfg SchemaRegistryConfig) (*SchemaRegistry, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if !cfg.Configured() {
		return nil, nil
	}

	conf := schemaregistry.NewConfig(cfg.URL)
	if cfg.Username != "" {
		conf.BasicAuthUserInfo = cfg.Username + ":" + cfg.Password
		conf.BasicAuthCredentialsSource = "USER_INFO"
	}
	conf.SslCaLocation = cfg.CALocation
	conf.SslCertificateLocation = cfg.CertificateLocation
	conf.SslKeyLocation = cfg.KeyLocation
	conf.CacheCapacity = cfg.SchemaCacheCapacity
	if conf.CacheCapacity == 0 {
		conf.CacheCapacity = DefaultSchemaCacheCapacity
	}

	client, err := schemaregistry.NewClient(conf)
	if err != nil {
		return nil, fmt.Errorf("%w: creating schema registry client for %q: %v", messaging.ErrInvalidConfig, cfg.URL, err)
	}
	return &SchemaRegistry{client: client}, nil
}

// Close releases the client's cached schemas and connections. It is safe
// to call on a nil *SchemaRegistry, so a producer or consumer built for a
// registry-less format can close unconditionally.
func (r *SchemaRegistry) Close() error {
	if r == nil {
		return nil
	}
	if err := r.client.Close(); err != nil {
		return fmt.Errorf("closing schema registry client: %w", err)
	}
	return nil
}
