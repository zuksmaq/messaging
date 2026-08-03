//go:build integration

package outbox_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/zuksmaq/messaging"
)

// publish is one call the fake producer recorded.
type publish struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

// EventID returns the idempotency header the relay stamped.
func (p publish) EventID() string {
	return string(p.Headers[messaging.EventIDHeader])
}

// fakeProducer is the seam standing in for a real broker: it records
// what the relay published and returns the status the test asked for, so
// these tests need a database but not Kafka.
type fakeProducer struct {
	// beforeProduce runs before each call is recorded, letting a test
	// synchronize with the relay goroutine. Set before Run starts and
	// never changed, so it needs no locking.
	beforeProduce func()

	mu sync.Mutex
	// status decides what Produce reports for a given value. A nil
	// status persists everything. Guarded by mu because a test may
	// change it while the relay is publishing.
	status    func(value []byte) messaging.DeliveryStatus
	published []publish
}

// setStatus changes what Produce reports from the next call on.
func (f *fakeProducer) setStatus(status func(value []byte) messaging.DeliveryStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = status
}

// Produce records the call and reports the configured status.
func (f *fakeProducer) Produce(_ context.Context, topic string, key, value []byte, headers map[string][]byte) (messaging.ProducedMessage, error) {
	if f.beforeProduce != nil {
		f.beforeProduce()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	recorded := publish{Topic: topic, Key: key, Value: value, Headers: make(map[string][]byte, len(headers))}
	for k, v := range headers {
		recorded.Headers[k] = v
	}
	f.published = append(f.published, recorded)

	status := messaging.Persisted
	if f.status != nil {
		status = f.status(value)
	}
	return messaging.ProducedMessage{
		Topic:     topic,
		Partition: 0,
		Offset:    int64(len(f.published)),
		Status:    status,
	}, nil
}

// Close satisfies messaging.Producer.
func (f *fakeProducer) Close(context.Context) error { return nil }

// calls returns a copy of what was published so far.
func (f *fakeProducer) calls() []publish {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]publish(nil), f.published...)
}

// values returns the published values as strings, in publish order.
func (f *fakeProducer) values() []string {
	calls := f.calls()
	out := make([]string, len(calls))
	for i, c := range calls {
		out[i] = string(c.Value)
	}
	return out
}

// eventID is the event-id header value an outbox row id produces.
func eventID(rowID int64) string {
	return strconv.FormatInt(rowID, 10)
}

// assertEventIDsMatchRowIDs checks the event-id header carries the
// outbox row id, which is what makes it a stable de-duplication key.
func assertEventIDsMatchRowIDs(t *testing.T, calls []publish, wantIDs []int64) {
	t.Helper()

	if len(calls) != len(wantIDs) {
		t.Fatalf("published %d messages, want %d", len(calls), len(wantIDs))
	}
	for i, c := range calls {
		if want := eventID(wantIDs[i]); c.EventID() != want {
			t.Errorf("publish %d event-id = %q, want %q", i, c.EventID(), want)
		}
	}
}
