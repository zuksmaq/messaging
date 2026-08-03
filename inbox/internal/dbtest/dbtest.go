//go:build integration

// Package dbtest stands up the databases the tagged integration tests run
// against, one Backend per inbox dialect. It is built only under the
// integration tag, so the testcontainers dependencies stay out of
// ordinary builds.
//
// The tests assert identical behaviour across every Backend: a dialect is
// only done when the whole dedup contract holds against it, not just when
// its SQL parses.
package dbtest

import (
	"database/sql"
	"testing"

	"github.com/zuksmaq/messaging/inbox"
)

// Backend is one database an integration test runs against. Besides the
// dialect under test it carries the little SQL the tests own themselves —
// the business table they write alongside the inbox row — because
// placeholders and column types differ per database.
type Backend struct {
	// Name labels the subtest.
	Name string

	// Dialect is the inbox dialect this database is exercising.
	Dialect inbox.Dialect

	// Start starts a container for the duration of the test and returns a
	// pool with the inbox table already created.
	Start func(t *testing.T) *sql.DB

	// Table is the inbox table Dialect targets.
	Table string

	// CreateOrdersSQL creates the tests' stand-in business table.
	CreateOrdersSQL string

	// InsertOrderSQL inserts one business row, taking the id as its only
	// parameter.
	InsertOrderSQL string
}

// Backends returns every database the inbox supports, so a test can run
// itself against all of them.
func Backends() []Backend {
	return []Backend{postgresBackend()}
}

// Count returns how many inbox rows exist.
func (b Backend) Count(t *testing.T, db *sql.DB) int {
	t.Helper()

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM ` + b.Table).Scan(&n); err != nil {
		t.Fatalf("counting inbox rows: %v", err)
	}
	return n
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
		t.Fatalf("creating inbox table: %v", err)
	}
	return db
}
