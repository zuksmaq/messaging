//go:build integration

// Package kafkatest stands up the broker the tagged integration tests
// run against. It is built only under the integration tag, so the
// testcontainers dependency stays out of ordinary builds.
package kafkatest

import (
	"context"
	"testing"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// Brokers starts a single-node Kafka container for the duration of the
// test and returns its bootstrap servers string.
func Brokers(t *testing.T) string {
	t.Helper()

	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1")
	if err != nil {
		t.Fatalf("starting kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminating kafka container: %v", err)
		}
	})

	seeds, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("resolving broker addresses: %v", err)
	}
	return seeds[0]
}

// CreateTopic creates a single-partition topic, tolerating one that
// already exists.
func CreateTopic(t *testing.T, bootstrap, topic string) {
	t.Helper()

	admin, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": bootstrap})
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	results, err := admin.CreateTopics(ctx, []ckafka.TopicSpecification{
		{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
	})
	if err != nil {
		t.Fatalf("creating topic %q: %v", topic, err)
	}
	for _, r := range results {
		if r.Error.Code() != ckafka.ErrNoError && r.Error.Code() != ckafka.ErrTopicAlreadyExists {
			t.Fatalf("creating topic %q: %v", topic, r.Error)
		}
	}
}
