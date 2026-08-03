//go:build integration

package dbtest

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver under test
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/zuksmaq/messaging/outbox/postgres"
)

// postgresImage is the Postgres image the backend starts.
const postgresImage = "postgres:16-alpine"

func postgresBackend() Backend {
	return Backend{
		Name:            "postgres",
		Dialect:         postgres.Dialect{},
		Start:           startPostgres,
		Table:           postgres.Table,
		CreateOrdersSQL: `CREATE TABLE orders (id TEXT PRIMARY KEY)`,
		InsertOrderSQL:  `INSERT INTO orders (id) VALUES ($1)`,
	}
}

// startPostgres starts a Postgres container for the duration of the test,
// creates the outbox table in it, and returns a connection pool.
func startPostgres(t *testing.T) *sql.DB {
	t.Helper()

	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, postgresImage,
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
	return open(t, "pgx", dsn, postgres.CreateTableSQL)
}
