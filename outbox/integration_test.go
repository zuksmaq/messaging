//go:build integration

// Integration tests for the outbox against real databases and a fake
// Producer. They exercise the public API only — Enqueue on the caller's
// transaction, Relay.Run, and each dialect's claim SQL — and assert on
// observable state: what the producer received and which rows remain
// staged.
//
// Every test runs against every dbtest.Backend, so a dialect is only done
// when the whole relay loop holds against its database, not just when its
// SQL parses.
package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/outbox"
	"github.com/zuksmaq/messaging/outbox/internal/dbtest"
)

// pollInterval keeps the tests quick; the relay's default of a second
// would dominate their runtime.
const pollInterval = 20 * time.Millisecond

// event is one staged event a test enqueues.
type event struct {
	topic   string
	key     string
	value   string
	headers map[string][]byte
}

// eachBackend runs test once per database, each in its own container.
func eachBackend(t *testing.T, test func(t *testing.T, b dbtest.Backend, db *sql.DB)) {
	t.Helper()

	for _, b := range dbtest.Backends() {
		t.Run(b.Name, func(t *testing.T) {
			test(t, b, b.Start(t))
		})
	}
}

// TestEnqueueCommitsWithTheCallersTransaction proves the outbox row and
// the caller's own business write share one transaction: rolling back
// takes the outbox row with it, so there is no window where an event is
// published for work that never committed.
func TestEnqueueCommitsWithTheCallersTransaction(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ctx := context.Background()

		if _, err := db.ExecContext(ctx, b.CreateOrdersSQL); err != nil {
			t.Fatalf("creating business table: %v", err)
		}
		ob, err := outbox.New(b.Dialect)
		if err != nil {
			t.Fatalf("building outbox: %v", err)
		}

		stage := func(t *testing.T, orderID string, commit bool) {
			t.Helper()

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("beginning transaction: %v", err)
			}
			defer func() { _ = tx.Rollback() }()

			if _, err := tx.ExecContext(ctx, b.InsertOrderSQL, orderID); err != nil {
				t.Fatalf("inserting business row: %v", err)
			}
			if err := ob.Enqueue(ctx, tx, "orders", []byte(orderID), []byte(`{"placed":true}`), nil); err != nil {
				t.Fatalf("enqueueing: %v", err)
			}
			if commit {
				if err := tx.Commit(); err != nil {
					t.Fatalf("committing: %v", err)
				}
			}
		}

		t.Run("rollback discards the outbox row too", func(t *testing.T) {
			stage(t, "rolled-back", false)

			if n := b.Count(t, db); n != 0 {
				t.Errorf("outbox rows after rollback = %d, want 0", n)
			}
			if n := countOrders(t, db); n != 0 {
				t.Errorf("business rows after rollback = %d, want 0", n)
			}
		})

		t.Run("commit keeps both", func(t *testing.T) {
			stage(t, "committed", true)

			if n := b.Count(t, db); n != 1 {
				t.Errorf("outbox rows after commit = %d, want 1", n)
			}
			if n := countOrders(t, db); n != 1 {
				t.Errorf("business rows after commit = %d, want 1", n)
			}
		})
	})
}

// TestEnqueueRejectsOverLimitTopicIdenticallyOnBothDialects covers the
// shared topic length limit: a topic longer than outbox.MaxTopicLength
// (the SQL Server dialect's NVARCHAR(255) column width) must be rejected
// by Enqueue itself, identically on Postgres and SQL Server, before any
// statement reaches the database. Without this guard, Postgres accepts an
// over-limit topic silently while SQL Server fails the insert — rolling
// back the caller's own unrelated business write only on that dialect.
func TestEnqueueRejectsOverLimitTopicIdenticallyOnBothDialects(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ctx := context.Background()

		if _, err := db.ExecContext(ctx, b.CreateOrdersSQL); err != nil {
			t.Fatalf("creating business table: %v", err)
		}
		ob, err := outbox.New(b.Dialect)
		if err != nil {
			t.Fatalf("building outbox: %v", err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("beginning transaction: %v", err)
		}
		defer func() { _ = tx.Rollback() }()

		if _, err := tx.ExecContext(ctx, b.InsertOrderSQL, "order-1"); err != nil {
			t.Fatalf("inserting business row: %v", err)
		}

		overLimit := strings.Repeat("t", outbox.MaxTopicLength+1)
		if err := ob.Enqueue(ctx, tx, overLimit, nil, nil, nil); !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Fatalf("Enqueue(over-limit topic) = %v, want an error wrapping ErrInvalidConfig", err)
		}

		if err := tx.Rollback(); err != nil {
			t.Fatalf("rolling back: %v", err)
		}
		if n := b.Count(t, db); n != 0 {
			t.Errorf("outbox rows after a rejected over-limit topic = %d, want 0", n)
		}

		// NVARCHAR(255) is a 255-*character* column, so the limit must
		// count runes, not UTF-8 bytes: a topic of exactly MaxTopicLength
		// multi-byte characters — whose byte length is more than double
		// that — must still be accepted.
		atLimit := strings.Repeat("é", outbox.MaxTopicLength)
		tx2, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("beginning transaction: %v", err)
		}
		defer func() { _ = tx2.Rollback() }()

		if err := ob.Enqueue(ctx, tx2, atLimit, nil, nil, nil); err != nil {
			t.Fatalf("Enqueue(%d-character multi-byte topic) = %v, want no error", outbox.MaxTopicLength, err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatalf("committing: %v", err)
		}
		if n := b.Count(t, db); n != 1 {
			t.Errorf("outbox rows after an at-limit multi-byte topic = %d, want 1", n)
		}
	})
}

// TestRelayPublishesStagedRowsThenDeletesThem covers the happy path: a
// batch staged in one transaction is published in id order with the
// row's id stamped as the event id, and the rows are gone afterwards.
func TestRelayPublishesStagedRowsThenDeletesThem(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ids := enqueueAll(t, b, db, []event{
			{topic: "orders", key: "k1", value: "v1", headers: map[string][]byte{"trace": []byte("abc")}},
			{topic: "orders", key: "k2", value: "v2"},
			{topic: "shipments", key: "k3", value: "v3"},
		})

		p := &fakeProducer{}
		startRelay(t, b, db, p, 10, 0)

		dbtest.WaitFor(t, "the outbox to drain", func() bool { return b.Count(t, db) == 0 })

		calls := p.calls()
		assertEventIDsMatchRowIDs(t, calls, ids)

		want := []publish{
			{Topic: "orders", Key: []byte("k1"), Value: []byte("v1")},
			{Topic: "orders", Key: []byte("k2"), Value: []byte("v2")},
			{Topic: "shipments", Key: []byte("k3"), Value: []byte("v3")},
		}
		for i, w := range want {
			got := calls[i]
			if got.Topic != w.Topic || string(got.Key) != string(w.Key) || string(got.Value) != string(w.Value) {
				t.Errorf("publish %d = %s/%q/%q, want %s/%q/%q",
					i, got.Topic, got.Key, got.Value, w.Topic, w.Key, w.Value)
			}
		}

		// The staged headers survive alongside the event id the relay adds.
		if got := string(calls[0].Headers["trace"]); got != "abc" {
			t.Errorf("staged header trace = %q, want abc", got)
		}
		if len(calls[1].Headers) != 1 {
			t.Errorf("headers on a row staged without any = %v, want only the event id", calls[1].Headers)
		}
	})
}

// TestRelayStopsAtTheFirstUnconfirmedRow covers the ordering guarantee:
// a row the producer would not confirm stays staged, and so does every
// row behind it, rather than the relay skipping ahead and reordering a
// key's events.
func TestRelayStopsAtTheFirstUnconfirmedRow(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ids := enqueueAll(t, b, db, []event{
			{topic: "orders", key: "k1", value: "v1"},
			{topic: "orders", key: "k2", value: "v2"},
			{topic: "orders", key: "k3", value: "v3"},
		})

		// The second row is acknowledged but not durably, which must not be
		// good enough to delete it.
		p := &fakeProducer{status: func(value []byte) messaging.DeliveryStatus {
			if string(value) == "v2" {
				return messaging.PossiblyPersisted
			}
			return messaging.Persisted
		}}
		// A high MaxAttempts keeps this test's fast poll interval from
		// quarantining v2 before the assertions below run: the ordering
		// guarantee this test covers only holds up to that threshold,
		// which TestRelayQuarantinesAPermanentlyUnpublishableRow covers.
		startRelay(t, b, db, p, 10, 1000)

		dbtest.WaitFor(t, "the confirmed row to be deleted", func() bool { return b.Count(t, db) == 2 })

		if got := b.IDs(t, db); !slices.Equal(got, ids[1:]) {
			t.Errorf("staged ids = %v, want %v", got, ids[1:])
		}
		// Give the relay several more polls to prove it stays stuck at v2
		// instead of eventually publishing past it.
		time.Sleep(10 * pollInterval)
		if slices.Contains(p.values(), "v3") {
			t.Errorf("v3 was published despite v2 never being confirmed: %v", p.values())
		}
		if n := b.Count(t, db); n != 2 {
			t.Errorf("staged rows = %d, want 2", n)
		}

		// Once the producer confirms it, the batch drains in order from the
		// row it stopped at.
		p.setStatus(nil)
		dbtest.WaitFor(t, "the outbox to drain", func() bool { return b.Count(t, db) == 0 })

		values := p.values()
		if got := values[len(values)-2:]; !slices.Equal(got, []string{"v2", "v3"}) {
			t.Errorf("last published values = %v, want [v2 v3]", got)
		}
	})
}

// TestRelayQuarantinesAPermanentlyUnpublishableRow covers the poison-row
// policy: a row whose publish never gets confirmed is quarantined after
// MaxAttempts instead of blocking every row staged after it forever, and
// the rows behind it are still delivered.
func TestRelayQuarantinesAPermanentlyUnpublishableRow(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ids := enqueueAll(t, b, db, []event{
			{topic: "orders", key: "k1", value: "poison"},
			{topic: "orders", key: "k2", value: "v2"},
			{topic: "orders", key: "k3", value: "v3"},
		})

		p := &fakeProducer{status: func(value []byte) messaging.DeliveryStatus {
			if string(value) == "poison" {
				return messaging.PossiblyPersisted
			}
			return messaging.Persisted
		}}

		relay, err := outbox.NewRelay(outbox.RelayConfig{
			DB:           db,
			Dialect:      b.Dialect,
			Producer:     p,
			BatchSize:    10,
			PollInterval: pollInterval,
			MaxAttempts:  3,
		})
		if err != nil {
			t.Fatalf("building relay: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- relay.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Errorf("Run = %v, want nil", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("Run did not return within 5s of cancelling its context")
			}
		})

		dbtest.WaitFor(t, "the poison row to be quarantined", func() bool {
			return len(b.QuarantinedIDs(t, db)) == 1
		})
		if got := b.QuarantinedIDs(t, db); !slices.Equal(got, ids[:1]) {
			t.Errorf("quarantined ids = %v, want %v", got, ids[:1])
		}

		dbtest.WaitFor(t, "the rows behind the poison row to drain", func() bool { return b.Count(t, db) == 1 })

		// Only the quarantined row remains staged; it was never deleted.
		if got := b.IDs(t, db); !slices.Equal(got, ids[:1]) {
			t.Errorf("staged ids = %v, want only the quarantined row %v", got, ids[:1])
		}
		if !slices.Contains(p.values(), "v2") || !slices.Contains(p.values(), "v3") {
			t.Errorf("rows staged after the poison row were not published: %v", p.values())
		}
	})
}

// TestConcurrentRelaysClaimDisjointRows proves each dialect's claim SQL —
// Postgres FOR UPDATE SKIP LOCKED, SQL Server UPDLOCK/READPAST — lets two
// relays work the same table at once. Each producer blocks on its first
// publish until both relays have reached one: that can only happen if the
// second relay claimed rows the first had not locked, so a claim that
// blocked instead would fail the test.
func TestConcurrentRelaysClaimDisjointRows(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ids := enqueueAll(t, b, db, []event{
			{topic: "orders", key: "k1", value: "v1"},
			{topic: "orders", key: "k2", value: "v2"},
			{topic: "orders", key: "k3", value: "v3"},
			{topic: "orders", key: "k4", value: "v4"},
		})

		var (
			mu         sync.Mutex
			arrived    int
			released   = make(chan struct{})
			serialized bool
		)
		// enter blocks the calling relay until the other one is also
		// mid-publish. Giving up means the other relay could not get a batch
		// while this one held its rows — a serialized claim — which the
		// assertion below reports rather than letting the test hang.
		enter := func() {
			mu.Lock()
			arrived++
			if arrived == 2 {
				close(released)
			}
			mu.Unlock()

			select {
			case <-released:
			case <-time.After(5 * time.Second):
				mu.Lock()
				serialized = true
				mu.Unlock()
			}
		}

		producers := make([]*fakeProducer, 2)
		for i := range producers {
			var once sync.Once
			p := &fakeProducer{}
			p.beforeProduce = func() { once.Do(enter) }
			producers[i] = p
			// Two rows each, so both relays have work to claim.
			startRelay(t, b, db, p, 2, 0)
		}

		dbtest.WaitFor(t, "the outbox to drain", func() bool { return b.Count(t, db) == 0 })

		mu.Lock()
		blocked := serialized
		mu.Unlock()
		if blocked {
			t.Error("the two relays never published concurrently: a claim blocked on the other relay's locked rows instead of skipping them")
		}

		// Every row published exactly once, across both relays.
		var gotIDs []string
		for _, p := range producers {
			for _, c := range p.calls() {
				gotIDs = append(gotIDs, c.EventID())
			}
		}
		slices.Sort(gotIDs)
		want := make([]string, len(ids))
		for i, id := range ids {
			want[i] = eventID(id)
		}
		slices.Sort(want)
		if !slices.Equal(gotIDs, want) {
			t.Errorf("published event ids = %v, want each of %v exactly once", gotIDs, want)
		}
	})
}

// TestRunReturnsWhenContextIsCancelled covers shutdown: Run is a
// blocking loop, so cancelling its context must end it promptly and
// without error.
func TestRunReturnsWhenContextIsCancelled(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		enqueueAll(t, b, db, []event{{topic: "orders", key: "k1", value: "v1"}})

		relay, err := outbox.NewRelay(outbox.RelayConfig{
			DB:           db,
			Dialect:      b.Dialect,
			Producer:     &fakeProducer{},
			BatchSize:    10,
			PollInterval: time.Hour, // parked between polls, so this tests the wait path
		})
		if err != nil {
			t.Fatalf("building relay: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- relay.Run(ctx) }()

		// Let it finish its first poll, then stop it.
		dbtest.WaitFor(t, "the first poll to publish", func() bool { return b.Count(t, db) == 0 })
		cancel()

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run after cancel = %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return within 5s of cancelling its context")
		}
	})
}

// startRelay runs a relay against db for the duration of the test,
// stopping it during cleanup. maxAttempts is passed through as
// RelayConfig.MaxAttempts; 0 selects the default.
func startRelay(t *testing.T, b dbtest.Backend, db *sql.DB, p *fakeProducer, batchSize, maxAttempts int) {
	t.Helper()

	relay, err := outbox.NewRelay(outbox.RelayConfig{
		DB:           db,
		Dialect:      b.Dialect,
		Producer:     p,
		BatchSize:    batchSize,
		PollInterval: pollInterval,
		MaxAttempts:  maxAttempts,
	})
	if err != nil {
		t.Fatalf("building relay: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run = %v, want nil", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return within 5s of cancelling its context")
		}
	})
}

// enqueueAll stages events in a single transaction and returns the ids
// the database assigned them, in order.
func enqueueAll(t *testing.T, b dbtest.Backend, db *sql.DB, events []event) []int64 {
	t.Helper()

	ob, err := outbox.New(b.Dialect)
	if err != nil {
		t.Fatalf("building outbox: %v", err)
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("beginning transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, e := range events {
		if err := ob.Enqueue(ctx, tx, e.topic, []byte(e.key), []byte(e.value), e.headers); err != nil {
			t.Fatalf("enqueueing %s: %v", e.value, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("committing staged events: %v", err)
	}

	ids := b.IDs(t, db)
	if len(ids) != len(events) {
		t.Fatalf("staged %d rows, want %d", len(ids), len(events))
	}
	return ids
}

func countOrders(t *testing.T, db *sql.DB) int {
	t.Helper()

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM orders`).Scan(&n); err != nil {
		t.Fatalf("counting business rows: %v", err)
	}
	return n
}
