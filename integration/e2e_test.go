//go:build integration

// End-to-end proof of the pattern the library exists for: a business
// write and its event committed together through the outbox, relayed to a
// real broker by a real producer, consumed by a hosted Runner whose
// handler dedups on the inbox before acting.
//
// Every layer is the real component — outbox.Relay, kafka producer and
// consumer, consumer.Runner, inbox — against a real Postgres and a real
// broker. The only stand-ins are the two business tables the handler
// writes, and the one producer that publishes for real and then reports a
// crash, which is the only way to reach the state a relay killed
// mid-publish leaves behind.
//
// Delivery is at-least-once, so the tests that matter most here are the
// ones that deliver an event twice and assert the business effect landed
// once.
package integration_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/inbox"
	inboxpg "github.com/zuksmaq/messaging/inbox/postgres"
	"github.com/zuksmaq/messaging/kafka/consumer"
)

// orderPayload is the staged value, opaque to the outbox and to the
// broker alike.
var orderPayload = []byte(`{"placed":true}`)

// delivery is one message the handler saw and what it decided about it.
type delivery struct {
	eventID string
	orderID string
	payload string

	// skipped is true when the inbox already had the event id, so the
	// handler did no work.
	skipped bool
}

// TestOutboxRelayThroughKafkaToInboxHandler is the happy path end to end:
// one order placed, one event relayed, one receipt written, one inbox row,
// and an offset committed past it.
func TestOutboxRelayThroughKafkaToInboxHandler(t *testing.T) {
	reset(t)
	const (
		topic   = "e2e-happy-path"
		group   = "e2e-happy-path"
		orderID = "order-1"
	)
	createTopic(t, topic)

	deliveries, handler := inboxHandler(t, nil)
	c, closeConsumer := newConsumer(t, group, topic)
	stopRunner := startRunner(t, c, handler)
	startRelay(t, newProducer(t))

	placeOrder(t, newOutbox(t), topic, orderID, orderPayload)

	got := awaitDeliveries(t, deliveries, 1)[0]
	if got.skipped {
		t.Error("first delivery was skipped, want the handler to do the work")
	}
	if got.orderID != orderID {
		t.Errorf("delivered key = %q, want %q", got.orderID, orderID)
	}
	if got.payload != string(orderPayload) {
		t.Errorf("delivered value = %q, want %q", got.payload, orderPayload)
	}
	// The relay stamps the outbox row id, so an event id is proof the
	// message came through the outbox rather than from anywhere else.
	if got.eventID == "" {
		t.Errorf("delivered event id is empty, want the outbox row id")
	}

	if n := receiptsFor(t, orderID); n != 1 {
		t.Errorf("receipts for %s = %d, want 1", orderID, n)
	}
	if n := inboxRows(t); n != 1 {
		t.Errorf("inbox rows = %d, want 1", n)
	}
	waitFor(t, "the outbox to drain", func() bool { return outboxRows(t) == 0 })

	// Freeing the group makes re-joining it a straight read of what the
	// runner committed. The runner has to stop first: closing a consumer
	// its Run is still polling destroys the client under librdkafka and
	// segfaults rather than failing.
	stopRunner()
	closeConsumer()
	assertCommittedPast(t, group, topic)
}

// TestDuplicateDeliveryLeavesTheBusinessEffectOnce republishes a
// delivered event under its own event id — the shape a broker-level
// redelivery takes — and asserts the handler recognises it and does
// nothing, so the receipt is not written twice.
func TestDuplicateDeliveryLeavesTheBusinessEffectOnce(t *testing.T) {
	reset(t)
	const (
		topic   = "e2e-duplicate"
		group   = "e2e-duplicate"
		orderID = "order-2"
	)
	createTopic(t, topic)

	deliveries, handler := inboxHandler(t, nil)
	c, _ := newConsumer(t, group, topic)
	startRunner(t, c, handler)
	p := newProducer(t)
	startRelay(t, p)

	placeOrder(t, newOutbox(t), topic, orderID, orderPayload)
	first := awaitDeliveries(t, deliveries, 1)[0]

	// Same event id, same content: as far as the consumer can tell, the
	// broker delivered the message twice.
	out, err := p.Produce(context.Background(), topic, []byte(orderID), orderPayload,
		map[string][]byte{messaging.EventIDHeader: []byte(first.eventID)})
	if err != nil {
		t.Fatalf("republishing event %s: %v", first.eventID, err)
	}
	if out.Status != messaging.Persisted {
		t.Fatalf("republished delivery status = %s, want %s", out.Status, messaging.Persisted)
	}

	second := awaitDeliveries(t, deliveries, 1)[0]
	if !second.skipped {
		t.Error("second delivery did the work again, want it skipped on the inbox check")
	}
	if second.eventID != first.eventID {
		t.Errorf("second delivery event id = %q, want %q", second.eventID, first.eventID)
	}

	if n := receiptsFor(t, orderID); n != 1 {
		t.Errorf("receipts for %s = %d, want 1: the inbox should have absorbed the duplicate", orderID, n)
	}
	if n := inboxRows(t); n != 1 {
		t.Errorf("inbox rows = %d, want 1", n)
	}
}

// TestRelayCrashBetweenPublishAndDeleteRepublishes covers the window that
// makes the outbox at-least-once rather than exactly-once: the broker has
// the message but the row is still staged, so the next relay publishes it
// again. The proof is that the consumer sees the event twice and the
// receipt is still written once.
func TestRelayCrashBetweenPublishAndDeleteRepublishes(t *testing.T) {
	reset(t)
	const (
		topic   = "e2e-relay-crash"
		group   = "e2e-relay-crash"
		orderID = "order-3"
	)
	createTopic(t, topic)

	deliveries, handler := inboxHandler(t, nil)
	c, _ := newConsumer(t, group, topic)
	startRunner(t, c, handler)

	crashed := make(chan struct{}, 1)
	stopCrashingRelay := startRelay(t, &crashingProducer{real: newProducer(t), crashed: crashed})

	placeOrder(t, newOutbox(t), topic, orderID, orderPayload)

	// The message is on the broker and the relay never got to the delete.
	<-crashed
	stopCrashingRelay()
	if n := outboxRows(t); n != 1 {
		t.Fatalf("outbox rows after the crash = %d, want 1 still staged", n)
	}

	// A restarted relay finds the row and publishes it again.
	startRelay(t, newProducer(t))
	waitFor(t, "the restarted relay to drain the outbox", func() bool { return outboxRows(t) == 0 })

	got := awaitDeliveries(t, deliveries, 2)
	if got[0].skipped {
		t.Error("first delivery was skipped, want the handler to do the work")
	}
	if !got[1].skipped {
		t.Error("republished delivery did the work again, want it skipped on the inbox check")
	}
	if got[0].eventID != got[1].eventID {
		t.Errorf("event ids = %q and %q, want the republished row to keep its id",
			got[0].eventID, got[1].eventID)
	}

	if n := receiptsFor(t, orderID); n != 1 {
		t.Errorf("receipts for %s = %d, want 1: the inbox should have absorbed the republish", orderID, n)
	}
	if n := inboxRows(t); n != 1 {
		t.Errorf("inbox rows = %d, want 1", n)
	}
}

// TestFailedHandlerCommitsNeitherInboxRowNorOffset pins the ordering the
// pattern depends on: the inbox row and the business effect ride the
// handler's own transaction, and the offset moves only after the handler
// returns without error. A handler that gets all the way through
// MarkProcessed and then fails must leave nothing behind — not the
// receipt, not the inbox row, not a committed offset — or the event would
// be lost while looking handled.
func TestFailedHandlerCommitsNeitherInboxRowNorOffset(t *testing.T) {
	reset(t)
	const (
		topic   = "e2e-handler-failure"
		group   = "e2e-handler-failure"
		orderID = "order-4"
	)
	createTopic(t, topic)

	// The relay's work is done once the row is published, so it can stop
	// before the runner starts.
	stopRelay := startRelay(t, newProducer(t))
	placeOrder(t, newOutbox(t), topic, orderID, orderPayload)
	waitFor(t, "the outbox to drain", func() bool { return outboxRows(t) == 0 })
	stopRelay()

	// The hook fails the handler with its transaction still open, after
	// both the receipt and the inbox row are written to it. Run is in the
	// foreground below, so the handler shares this goroutine and eventID
	// needs no synchronization.
	var eventID string
	_, failing := inboxHandler(t, func(id string) error {
		eventID = id
		return errors.New("handler failed before committing")
	})

	c, closeConsumer := newConsumer(t, group, topic)

	// Run in the foreground: the default Halt policy stops it on the
	// handler's error, so there is nothing to cancel and nothing else
	// touching the consumer while it returns.
	ctx, cancel := context.WithTimeout(context.Background(), arrivalWindow)
	defer cancel()

	err := newRunner(t, c, failing).Run(ctx)
	if err == nil {
		t.Fatal("Run = nil, want the halt error")
	}
	if !strings.Contains(err.Error(), "handler failed") {
		t.Errorf("Run error = %v, want it to report the handler failure", err)
	}
	if eventID == "" {
		t.Fatal("the handler never reached the commit, want it to have done the work first")
	}

	if n := receiptsFor(t, orderID); n != 0 {
		t.Errorf("receipts for %s = %d, want 0: the handler's transaction was rolled back", orderID, n)
	}
	if n := inboxRows(t); n != 0 {
		t.Errorf("inbox rows = %d, want 0: the inbox row rides the same rollback", n)
	}

	closeConsumer()
	assertRedelivers(t, group, topic, eventID)
}

// inboxHandler is the consumer half of the pattern: check the inbox,
// do the work, record the event, and commit the two together. It reports
// every delivery it saw on the returned channel so tests can synchronize
// on the handler rather than poll the broker.
//
// The handler deliberately commits its own transaction and leaves the
// offset to the Runner: the inbox row is what makes a repeat delivery
// harmless, so it has to be durable before the offset moves.
//
// beforeCommit, when non-nil, runs with the transaction still open and
// its error becomes the handler's, which is how a test reaches the state
// of a handler that failed part-way through its own work.
func inboxHandler(t *testing.T, beforeCommit func(eventID string) error) (<-chan delivery, consumer.Handler[[]byte, []byte]) {
	t.Helper()

	ib, err := inbox.New(inboxpg.Dialect{})
	if err != nil {
		t.Fatalf("building inbox: %v", err)
	}
	deliveries := make(chan delivery, 16)

	return deliveries, func(ctx context.Context, msg messaging.ReceivedMessage[[]byte, []byte]) error {
		seen := delivery{
			eventID: msg.EventID(),
			orderID: string(msg.Key),
			payload: string(msg.Value),
		}
		if seen.eventID == "" {
			return fmt.Errorf("%s[%d]@%d carries no %s header",
				msg.Topic, msg.Partition, msg.Offset, messaging.EventIDHeader)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("beginning handler transaction: %w", err)
		}
		defer func() { _ = tx.Rollback() }()

		processed, err := ib.HasProcessed(ctx, tx, seen.eventID)
		if err != nil {
			return err
		}
		if processed {
			// A duplicate delivery of work that already committed. Doing
			// nothing and letting the offset move is the whole point.
			seen.skipped = true
			deliveries <- seen
			return nil
		}

		if _, err := tx.ExecContext(ctx, insertReceiptSQL, seen.orderID); err != nil {
			return fmt.Errorf("writing the receipt for %s: %w", seen.orderID, err)
		}

		recorded, err := ib.MarkProcessed(ctx, tx, seen.eventID)
		if err != nil {
			return err
		}
		if !recorded {
			// Another consumer in the group recorded the id while this
			// handler was working: unreachable with the single consumer
			// these tests run, but the contract is that the loser must not
			// commit, so returning an error rolls the work back and leaves
			// the offset for the re-delivery.
			return fmt.Errorf("event %s was recorded by another consumer", seen.eventID)
		}

		if beforeCommit != nil {
			if err := beforeCommit(seen.eventID); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("committing the handler transaction: %w", err)
		}

		deliveries <- seen
		return nil
	}
}

// crashingProducer publishes through the real producer and then reports a
// failure, which is what a relay killed between the publish and the delete
// looks like from the outside: the broker has the message, the outbox row
// is still staged.
type crashingProducer struct {
	real    messaging.Producer[[]byte, []byte]
	crashed chan struct{}
}

func (p *crashingProducer) Produce(ctx context.Context, topic string, key, value []byte, headers map[string][]byte) (messaging.ProducedMessage, error) {
	out, err := p.real.Produce(ctx, topic, key, value, headers)
	if err != nil {
		return out, err
	}
	p.crashed <- struct{}{}
	return out, errors.New("relay killed before deleting the row")
}

// Close is a no-op: the real producer is closed by its own test cleanup.
func (p *crashingProducer) Close(context.Context) error { return nil }
