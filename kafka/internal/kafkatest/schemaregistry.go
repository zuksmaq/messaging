//go:build integration

package kafkatest

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
)

// registryImage supplies the Schema Registry the Avro tests run against.
//
// Redpanda's registry speaks the Confluent Schema Registry HTTP API,
// including its compatibility checks, and ships in one small image;
// cp-schema-registry needs a broker of its own to store schemas in and a
// docker network to reach it over. Where the schemas are stored is
// invisible to the code under test, so the cheaper container wins. The
// broker under test stays Confluent's — only the registry differs.
const registryImage = "redpandadata/redpanda:v24.2.7"

// SchemaRegistry starts a Schema Registry for the duration of the test and
// returns its base URL.
func SchemaRegistry(t *testing.T) string {
	t.Helper()

	ctx := context.Background()
	container, err := tcredpanda.Run(ctx, registryImage)
	if err != nil {
		t.Fatalf("starting schema registry container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminating schema registry container: %v", err)
		}
	})

	url, err := container.SchemaRegistryAddress(ctx)
	if err != nil {
		t.Fatalf("resolving schema registry address: %v", err)
	}
	return url
}

// RegisterSchema registers schema under subject and pins the subject to
// BACKWARD compatibility, so a test can fix both what the registry already
// holds and how strictly it judges the next schema offered for it —
// neither left to the registry's defaults.
func RegisterSchema(t *testing.T, registryURL, subject, schema string) {
	t.Helper()

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(registryURL))
	if err != nil {
		t.Fatalf("creating schema registry client: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("closing schema registry client: %v", err)
		}
	})

	if _, err := client.Register(subject, schemaregistry.SchemaInfo{
		Schema:     schema,
		SchemaType: "AVRO",
	}, true); err != nil {
		t.Fatalf("registering schema for subject %q: %v", subject, err)
	}
	if _, err := client.UpdateCompatibility(subject, schemaregistry.Backward); err != nil {
		t.Fatalf("setting compatibility for subject %q: %v", subject, err)
	}
}
