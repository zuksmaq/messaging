//go:build integration

package dbtest

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/microsoft/go-mssqldb" // database/sql driver under test
	tcmssql "github.com/testcontainers/testcontainers-go/modules/mssql"
	"github.com/zuksmaq/messaging/outbox/sqlserver"
)

// sqlServerImage is the SQL Server image the backend starts.
const sqlServerImage = "mcr.microsoft.com/mssql/server:2022-latest"

func sqlServerBackend() Backend {
	return Backend{
		Name:    "sqlserver",
		Dialect: sqlserver.Dialect{},
		Start:   startSQLServer,
		Table:   sqlserver.Table,
		// SQL Server has no unbounded TEXT key type, and its placeholders
		// are ordinal rather than dollar-numbered.
		CreateOrdersSQL: `CREATE TABLE orders (id NVARCHAR(100) PRIMARY KEY)`,
		InsertOrderSQL:  `INSERT INTO orders (id) VALUES (@p1)`,
	}
}

// startSQLServer starts a SQL Server container for the duration of the
// test, creates the outbox table in it, and returns a connection pool.
func startSQLServer(t *testing.T) *sql.DB {
	t.Helper()

	ctx := context.Background()
	container, err := tcmssql.Run(ctx, sqlServerImage, tcmssql.WithAcceptEULA())
	if err != nil {
		t.Fatalf("starting sql server container: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("terminating sql server container: %v", err)
		}
	})

	dsn, err := container.ConnectionString(ctx, "encrypt=disable")
	if err != nil {
		t.Fatalf("resolving connection string: %v", err)
	}
	return open(t, "sqlserver", dsn, sqlserver.CreateTableSQL)
}
