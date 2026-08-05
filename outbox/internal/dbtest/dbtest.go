//go:build integration

// Package dbtest stands up the databases the tagged integration tests run
// against, one Backend per outbox dialect. It is built only under the
// integration tag, so the testcontainers dependencies stay out of
// ordinary builds.
//
// The tests assert identical behaviour across every Backend: a dialect is
// only done when the whole relay loop holds against it, not just its own
// SQL.
package dbtest

import (
	"database/sql"
	"testing"
	"time"

	"github.com/zuksmaq/messaging/outbox"
)

// Backend is one database an integration test runs against. Besides the
// dialect under test it carries the little SQL the tests own themselves —
// the business table they enqueue alongside — because placeholders and
// column types differ per database.
type Backend struct {
	// Name labels the subtest.
	Name string

	// Dialect is the outbox dialect this database is exercising.
	Dialect outbox.Dialect

	// Start starts a container for the duration of the test and returns a
	// pool with the outbox table already created.
	Start func(t *testing.T) *sql.DB

	// Table is the outbox table Dialect targets.
	Table string

	// CreateOrdersSQL creates the tests' stand-in business table.
	CreateOrdersSQL string

	// InsertOrderSQL inserts one business row, taking the id as its only
	// parameter.
	InsertOrderSQL string
}

// Backends returns every database the outbox supports, so a test can run
// itself against all of them.
func Backends() []Backend {
	return []Backend{postgresBackend(), sqlServerBackend()}
}

// Count returns how many outbox rows remain.
func (b Backend) Count(t *testing.T, db *sql.DB) int {
	t.Helper()

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM ` + b.Table).Scan(&n); err != nil {
		t.Fatalf("counting outbox rows: %v", err)
	}
	return n
}

// IDs returns the ids of the outbox rows still staged, in id order.
func (b Backend) IDs(t *testing.T, db *sql.DB) []int64 {
	t.Helper()

	rows, err := db.Query(`SELECT id FROM ` + b.Table + ` ORDER BY id`)
	if err != nil {
		t.Fatalf("listing outbox rows: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scanning outbox id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("listing outbox rows: %v", err)
	}
	return ids
}

// QuarantinedIDs returns the ids of the outbox rows marked quarantined,
// in id order.
func (b Backend) QuarantinedIDs(t *testing.T, db *sql.DB) []int64 {
	t.Helper()

	rows, err := db.Query(`SELECT id FROM ` + b.Table + ` WHERE quarantined_at IS NOT NULL ORDER BY id`)
	if err != nil {
		t.Fatalf("listing quarantined outbox rows: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scanning quarantined outbox id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("listing quarantined outbox rows: %v", err)
	}
	return ids
}

// WaitFor polls cond until it holds, failing the test if it has not
// within a few seconds. The relay is a background loop, so tests assert
// on where it gets to rather than on individual polls.
func WaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// open opens a pool on dsn and closes it during cleanup.
func open(t *testing.T, driver, dsn, createTableSQL string) *sql.DB {
	t.Helper()

	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("closing database: %v", err)
		}
	})

	if _, err := db.Exec(createTableSQL); err != nil {
		t.Fatalf("creating outbox table: %v", err)
	}
	return db
}
