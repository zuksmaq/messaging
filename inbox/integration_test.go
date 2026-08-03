//go:build integration

// Integration tests for the inbox against real databases. They exercise
// the public API only — HasProcessed and MarkProcessed on the caller's
// own transaction — and assert on observable state: what each call
// returned, and which rows survive a commit or a rollback.
//
// Every test runs against every dbtest.Backend, so a dialect is only done
// when the whole dedup contract holds against its database.
package inbox_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/inbox"
	"github.com/zuksmaq/messaging/inbox/internal/dbtest"
)

// eachBackend runs test once per database, each in its own container.
func eachBackend(t *testing.T, test func(t *testing.T, b dbtest.Backend, db *sql.DB)) {
	t.Helper()

	for _, b := range dbtest.Backends() {
		t.Run(b.Name, func(t *testing.T) {
			test(t, b, b.Start(t))
		})
	}
}

// TestMarkProcessedCommitsWithTheCallersTransaction proves the inbox row
// and the handler's own business write share one transaction: rolling
// back takes the inbox row with it, so an event is never recorded as
// handled when the work it recorded never landed.
func TestMarkProcessedCommitsWithTheCallersTransaction(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ctx := context.Background()
		in := newInbox(t, b)
		createOrders(t, b, db)

		handle := func(t *testing.T, eventID string, commit bool) {
			t.Helper()

			tx := begin(t, db)
			defer func() { _ = tx.Rollback() }()

			if _, err := tx.ExecContext(ctx, b.InsertOrderSQL, eventID); err != nil {
				t.Fatalf("inserting business row: %v", err)
			}
			recorded, err := in.MarkProcessed(ctx, tx, eventID)
			if err != nil {
				t.Fatalf("marking processed: %v", err)
			}
			if !recorded {
				t.Fatalf("MarkProcessed(%q) = false, want true for an unseen event", eventID)
			}
			if commit {
				if err := tx.Commit(); err != nil {
					t.Fatalf("committing: %v", err)
				}
			}
		}

		t.Run("rollback discards the inbox row too", func(t *testing.T) {
			handle(t, "rolled-back", false)

			if n := b.Count(t, db); n != 0 {
				t.Errorf("inbox rows after rollback = %d, want 0", n)
			}
			if n := countOrders(t, db); n != 0 {
				t.Errorf("business rows after rollback = %d, want 0", n)
			}
			// The event is still unhandled, so a re-delivery must be able to
			// do the work again.
			if has := hasProcessed(t, db, in, "rolled-back"); has {
				t.Error("HasProcessed after a rolled-back MarkProcessed = true, want false")
			}
		})

		t.Run("commit keeps both", func(t *testing.T) {
			handle(t, "committed", true)

			if n := b.Count(t, db); n != 1 {
				t.Errorf("inbox rows after commit = %d, want 1", n)
			}
			if n := countOrders(t, db); n != 1 {
				t.Errorf("business rows after commit = %d, want 1", n)
			}
			if has := hasProcessed(t, db, in, "committed"); !has {
				t.Error("HasProcessed after a committed MarkProcessed = false, want true")
			}
		})
	})
}

// TestHasProcessedReportsOnlyCommittedWork pins the visibility rule the
// Dialect contract requires: an unseen id and an id another transaction
// has staged but not committed both read false, and neither read blocks.
func TestHasProcessedReportsOnlyCommittedWork(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ctx := context.Background()
		in := newInbox(t, b)

		if has := hasProcessed(t, db, in, "never-seen"); has {
			t.Error("HasProcessed for an unseen event = true, want false")
		}

		// Stage the id in a transaction and hold it open.
		staging := begin(t, db)
		defer func() { _ = staging.Rollback() }()
		if _, err := in.MarkProcessed(ctx, staging, "in-flight"); err != nil {
			t.Fatalf("marking processed: %v", err)
		}

		// A reader in another transaction must neither see the staged row
		// nor block on it — a read that blocked here would stall every
		// consumer behind an in-flight handler, so the timeout is the
		// assertion.
		type readResult struct {
			has bool
			err error
		}
		result := make(chan readResult, 1)
		reader := begin(t, db)
		defer func() { _ = reader.Rollback() }()
		go func() {
			has, err := in.HasProcessed(ctx, reader, "in-flight")
			result <- readResult{has: has, err: err}
		}()

		select {
		case got := <-result:
			if got.err != nil {
				t.Fatalf("checking an in-flight event: %v", got.err)
			}
			if got.has {
				t.Error("HasProcessed for a staged-but-uncommitted event = true, want false")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("HasProcessed blocked on an event another transaction had staged but not committed")
		}

		// Once the staging transaction commits, it reads true.
		if err := staging.Commit(); err != nil {
			t.Fatalf("committing: %v", err)
		}
		if has := hasProcessed(t, db, in, "in-flight"); !has {
			t.Error("HasProcessed after the staging transaction committed = false, want true")
		}
	})
}

// TestMarkProcessedTwiceNeitherErrorsNorDuplicates covers the re-delivery
// a caller did not guard with HasProcessed: recording an id that is
// already committed reports that this transaction did not record it, and
// leaves one row.
func TestMarkProcessedTwiceNeitherErrorsNorDuplicates(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ctx := context.Background()
		in := newInbox(t, b)

		mark := func(t *testing.T) bool {
			t.Helper()

			tx := begin(t, db)
			defer func() { _ = tx.Rollback() }()

			recorded, err := in.MarkProcessed(ctx, tx, "evt-1")
			if err != nil {
				t.Fatalf("marking processed: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("committing: %v", err)
			}
			return recorded
		}

		if recorded := mark(t); !recorded {
			t.Error("first MarkProcessed = false, want true")
		}
		if recorded := mark(t); recorded {
			t.Error("second MarkProcessed = true, want false: the id was already recorded")
		}
		if n := b.Count(t, db); n != 1 {
			t.Errorf("inbox rows after marking the same event twice = %d, want 1", n)
		}
	})
}

// TestConcurrentDeliveriesSettleOnOneWinner documents the
// duplicate-delivery race: two transactions both check HasProcessed
// before either commits.
//
// Both reads report false — neither can see the other's staged row — so
// both go on to do the work. The database settles it at MarkProcessed:
// the second call blocks until the first transaction finishes and then
// reports false, which is the loser's instruction to roll back its
// business write and let the event be re-delivered. Exactly one delivery
// commits.
func TestConcurrentDeliveriesSettleOnOneWinner(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ctx := context.Background()
		in := newInbox(t, b)
		createOrders(t, b, db)

		first, second := begin(t, db), begin(t, db)
		defer func() { _ = first.Rollback() }()
		defer func() { _ = second.Rollback() }()

		// Both deliveries check before either commits, and both see an
		// unhandled event.
		deliveries := []struct {
			name string
			tx   *sql.Tx
		}{{"first", first}, {"second", second}}
		for _, d := range deliveries {
			has, err := in.HasProcessed(ctx, d.tx, "evt-1")
			if err != nil {
				t.Fatalf("%s HasProcessed: %v", d.name, err)
			}
			if has {
				t.Fatalf("%s HasProcessed before either committed = true, want false", d.name)
			}
		}

		// The first delivery does its work and records the event, but has
		// not committed yet.
		if _, err := first.ExecContext(ctx, b.InsertOrderSQL, "order-from-first"); err != nil {
			t.Fatalf("first business write: %v", err)
		}
		firstRecorded, err := in.MarkProcessed(ctx, first, "evt-1")
		if err != nil {
			t.Fatalf("first MarkProcessed: %v", err)
		}
		if !firstRecorded {
			t.Fatal("first MarkProcessed = false, want true")
		}

		// The second delivery now tries to record the same event. It must
		// wait on the first transaction rather than racing past it, so it
		// runs in its own goroutine.
		type markResult struct {
			recorded bool
			err      error
		}
		secondDone := make(chan markResult, 1)
		go func() {
			if _, err := second.ExecContext(ctx, b.InsertOrderSQL, "order-from-second"); err != nil {
				secondDone <- markResult{err: err}
				return
			}
			recorded, err := in.MarkProcessed(ctx, second, "evt-1")
			secondDone <- markResult{recorded: recorded, err: err}
		}()

		select {
		case got := <-secondDone:
			t.Fatalf("second MarkProcessed returned %v/%v before the first transaction committed, want it to wait", got.recorded, got.err)
		case <-time.After(500 * time.Millisecond):
			// Still waiting on the first transaction, which is the point.
		}

		if err := first.Commit(); err != nil {
			t.Fatalf("committing the first delivery: %v", err)
		}

		select {
		case got := <-secondDone:
			if got.err != nil {
				t.Fatalf("second MarkProcessed: %v", got.err)
			}
			if got.recorded {
				t.Error("second MarkProcessed = true, want false: the first delivery recorded the event")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("second MarkProcessed did not return after the first transaction committed")
		}

		// Losing means rolling back, which is what keeps the work
		// single-shot.
		if err := second.Rollback(); err != nil {
			t.Fatalf("rolling back the second delivery: %v", err)
		}

		if n := b.Count(t, db); n != 1 {
			t.Errorf("inbox rows after the race = %d, want 1", n)
		}
		if n := countOrders(t, db); n != 1 {
			t.Errorf("business rows after the race = %d, want 1: the event was handled twice", n)
		}
		if has := hasProcessed(t, db, in, "evt-1"); !has {
			t.Error("HasProcessed after the race = false, want true: the re-delivery must now be skipped")
		}
	})
}

// TestManyConcurrentDeliveriesNeverCollide races deliveries of one event
// with no ordering imposed between their existence checks and their
// inserts, which is the interleaving
// TestConcurrentDeliveriesSettleOnOneWinner cannot reach: there the first
// delivery finishes recording before the second starts, so the two never
// check at the same instant.
//
// Exactly one delivery still wins and no delivery gets an error. The
// no-error half is the point: MarkProcessed promises that recording an
// already-recorded id reports false rather than failing, and a dialect
// whose conditional insert is not serialized against a concurrent one
// breaks that promise with a duplicate-key error instead — which a caller
// has no way to tell from a real fault.
func TestManyConcurrentDeliveriesNeverCollide(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ctx := context.Background()
		in := newInbox(t, b)

		// Every transaction is open before any of them records, so they
		// contend on the same unrecorded id.
		const deliveries = 10
		txs := make([]*sql.Tx, deliveries)
		for i := range txs {
			txs[i] = begin(t, db)
		}

		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners int
			errs    []error
			start   = make(chan struct{})
		)
		for _, tx := range txs {
			wg.Add(1)
			go func(tx *sql.Tx) {
				defer wg.Done()
				<-start

				recorded, err := in.MarkProcessed(ctx, tx, "evt-1")

				mu.Lock()
				switch {
				case err != nil:
					errs = append(errs, err)
				case recorded:
					winners++
				}
				mu.Unlock()

				// Only the winner may commit; every loser rolls back and waits
				// for the re-delivery.
				if err != nil || !recorded {
					_ = tx.Rollback()
					return
				}
				if err := tx.Commit(); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}(tx)
		}
		close(start)
		wg.Wait()

		for _, err := range errs {
			t.Errorf("MarkProcessed under contention: %v", err)
		}
		if winners != 1 {
			t.Errorf("deliveries that recorded the event = %d, want 1", winners)
		}
		if n := b.Count(t, db); n != 1 {
			t.Errorf("inbox rows after %d concurrent deliveries = %d, want 1", deliveries, n)
		}
		if has := hasProcessed(t, db, in, "evt-1"); !has {
			t.Error("HasProcessed after the race = false, want true")
		}
	})
}

// TestDedupKeyIsTheTransportAgnosticEventID proves the key is the same
// event id the rest of the library moves around: the header the outbox
// relay stamps, read off a messaging.ReceivedMessage and passed straight
// in as a string. No broker type reaches the inbox API.
func TestDedupKeyIsTheTransportAgnosticEventID(t *testing.T) {
	eachBackend(t, func(t *testing.T, b dbtest.Backend, db *sql.DB) {
		ctx := context.Background()
		in := newInbox(t, b)

		// What a consumer holds: a message carrying the relay's event-id
		// header.
		received := messaging.ReceivedMessage[string, string]{
			Message: messaging.Message[string, string]{
				Key:     "k1",
				Value:   "v1",
				Headers: map[string][]byte{messaging.EventIDHeader: []byte("42")},
			},
			Topic: "orders",
		}
		eventID := received.EventID()
		if eventID != "42" {
			t.Fatalf("ReceivedMessage.EventID() = %q, want 42", eventID)
		}

		tx := begin(t, db)
		defer func() { _ = tx.Rollback() }()
		if _, err := in.MarkProcessed(ctx, tx, eventID); err != nil {
			t.Fatalf("marking processed: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("committing: %v", err)
		}

		// The same id, arriving on a redelivery of the same message, is
		// recognised.
		if has := hasProcessed(t, db, in, received.EventID()); !has {
			t.Error("HasProcessed for the message's event id = false, want true")
		}
	})
}

// newInbox builds an inbox for the backend's dialect.
func newInbox(t *testing.T, b dbtest.Backend) *inbox.Inbox {
	t.Helper()

	in, err := inbox.New(b.Dialect)
	if err != nil {
		t.Fatalf("building inbox: %v", err)
	}
	return in
}

// begin opens a transaction, rolled back during cleanup if the test left
// it open.
func begin(t *testing.T, db *sql.DB) *sql.Tx {
	t.Helper()

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("beginning transaction: %v", err)
	}
	return tx
}

// hasProcessed asks in a transaction of its own, which is how a consumer
// checking a fresh delivery sees the table.
func hasProcessed(t *testing.T, db *sql.DB, in *inbox.Inbox, eventID string) bool {
	t.Helper()

	ctx := context.Background()
	tx := begin(t, db)
	defer func() { _ = tx.Rollback() }()

	has, err := in.HasProcessed(ctx, tx, eventID)
	if err != nil {
		t.Fatalf("checking whether %q was processed: %v", eventID, err)
	}
	return has
}

func createOrders(t *testing.T, b dbtest.Backend, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(b.CreateOrdersSQL); err != nil {
		t.Fatalf("creating business table: %v", err)
	}
}

func countOrders(t *testing.T, db *sql.DB) int {
	t.Helper()

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM orders`).Scan(&n); err != nil {
		t.Fatalf("counting business rows: %v", err)
	}
	return n
}
