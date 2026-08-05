// Package sqlserver supplies the SQL Server SQL the inbox core needs,
// reading with READPAST so readers never stall behind an in-flight
// insert, and letting the primary key itself settle which of two
// concurrent inserts of one event id wins.
package sqlserver

import (
	"errors"

	mssql "github.com/microsoft/go-mssqldb"
)

// Table is the inbox table every statement here targets.
const Table = "messaging_inbox"

// SQL Server error numbers InsertSQL fails with when event_id already
// has a row: primaryKeyViolation for the primary key constraint this
// table declares, duplicateKeyRow if a caller's own migration ever added
// a unique index instead.
const (
	primaryKeyViolation = 2627
	duplicateKeyRow     = 2601
)

// CreateTableSQL creates the inbox table. It is exported so callers can
// run it from their own migrations; this package never runs it.
//
// SQL Server has no CREATE TABLE IF NOT EXISTS, so the existence check is
// explicit — and, run from two connections against a database where the
// table does not exist yet, both can pass that check before either
// creates it. The TRY/CATCH swallows the second creator's "already an
// object with that name" error (2714) instead of letting it fail the
// caller outright. The event id is the primary key, which is what makes
// recording it a race the database decides rather than one the
// application has to lock for; it is a bounded NVARCHAR because SQL
// Server cannot index an unbounded one.
const CreateTableSQL = `IF OBJECT_ID(N'` + Table + `', N'U') IS NULL
BEGIN
	BEGIN TRY
		CREATE TABLE ` + Table + ` (
			event_id     NVARCHAR(255) NOT NULL PRIMARY KEY,
			processed_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
		);
	END TRY
	BEGIN CATCH
		IF ERROR_NUMBER() <> 2714
			THROW;
	END CATCH
END`

// Dialect is the SQL Server inbox.Dialect. Its zero value is ready to
// use.
type Dialect struct{}

// SelectSQL looks for the event id @p1.
//
// READPAST is what keeps the read from blocking, and unlike Postgres it
// is not optional: a plain SELECT would wait on the exclusive key lock of
// an id a concurrent handler has inserted but not committed, stalling
// every consumer queued behind that handler. Skipping the locked row
// reports it as unprocessed, which is exactly what HasProcessed owes its
// caller — committed work only. ROWLOCK keeps the engine from taking a
// page lock, which READPAST would skip wholesale and hide committed ids
// sharing the page.
func (Dialect) SelectSQL() string {
	return `SELECT TOP (1) 1 FROM ` + Table + ` WITH (READPAST, ROWLOCK) WHERE event_id = @p1`
}

// InsertSQL records the event id @p1 with a plain insert.
//
// SQL Server has no ON CONFLICT DO NOTHING, so unlike Postgres this
// relies on IsDuplicateKeyError rather than the statement itself to tell
// a losing insert from a real failure. An earlier version checked
// existence first under UPDLOCK/HOLDLOCK, but those hints hold a lock
// over the row's entire empty key-range gap until the transaction ends —
// serializing every other event id landing in that same gap, not just a
// second insert of this one. A plain insert only ever contends with
// another insert of the same id, which is the one case that must
// serialize.
func (Dialect) InsertSQL() string {
	return `INSERT INTO ` + Table + ` (event_id) VALUES (@p1)`
}

// IsDuplicateKeyError reports whether err is the primary key violation
// InsertSQL fails with when another transaction already recorded the
// event id — the signal MarkProcessed treats as losing the race rather
// than a fault.
func (Dialect) IsDuplicateKeyError(err error) bool {
	var sqlErr mssql.Error
	if !errors.As(err, &sqlErr) {
		return false
	}
	switch sqlErr.Number {
	case primaryKeyViolation, duplicateKeyRow:
		return true
	default:
		return false
	}
}
