//go:build integration

// Package pgtest stands up the Postgres database the tagged integration
// tests run against. It is built only under the integration tag, so the
// testcontainers dependency stays out of ordinary builds.
package pgtest

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver under test
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/zuksmaq/messaging/outbox/postgres"
)

// image is the Postgres image every helper here starts.
const image = "postgres:16-alpine"

// DB starts a Postgres container for the duration of the test, creates
// the outbox table in it, and returns a connection pool.
func DB(t *testing.T) *sql.DB {
	t.Helper()

	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, image,
		tcpostgres.WithDatabase("messaging"),
		tcpostgres.WithUsername("messaging"),
		tcpostgres.WithPassword("messaging"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("starting postgres container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminating postgres container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("resolving connection string: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("opening database: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("closing database: %v", err)
		}
	})

	if _, err := db.ExecContext(ctx, postgres.CreateTableSQL); err != nil {
		t.Fatalf("creating outbox table: %v", err)
	}
	return db
}

// Count returns how many outbox rows remain.
func Count(t *testing.T, db *sql.DB) int {
	t.Helper()

	var n int
	if err := db.QueryRow(`SELECT count(*) FROM ` + postgres.Table).Scan(&n); err != nil {
		t.Fatalf("counting outbox rows: %v", err)
	}
	return n
}

// IDs returns the ids of the outbox rows still staged, in id order.
func IDs(t *testing.T, db *sql.DB) []int64 {
	t.Helper()

	rows, err := db.Query(`SELECT id FROM ` + postgres.Table + ` ORDER BY id`)
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
