// Package sqlserver supplies the SQL Server SQL the inbox core needs,
// reading with READPAST and inserting under UPDLOCK/HOLDLOCK so
// concurrent deliveries of one event settle on a single winner without
// stalling readers.
package sqlserver

// Table is the inbox table every statement here targets.
const Table = "messaging_inbox"

// CreateTableSQL creates the inbox table. It is exported so callers can
// run it from their own migrations; this package never runs it.
//
// SQL Server has no CREATE TABLE IF NOT EXISTS, so the existence check is
// explicit. The event id is the primary key, which is what makes
// recording it a race the database decides rather than one the
// application has to lock for; it is a bounded NVARCHAR because SQL
// Server cannot index an unbounded one.
const CreateTableSQL = `IF OBJECT_ID(N'` + Table + `', N'U') IS NULL
CREATE TABLE ` + Table + ` (
	event_id     NVARCHAR(255) NOT NULL PRIMARY KEY,
	processed_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
)`

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

// InsertSQL records the event id @p1, reporting one row affected only if
// this statement is the one that inserted it.
//
// SQL Server has no ON CONFLICT DO NOTHING, so the conditional insert is
// written out and the lock hints do the work the conflict clause does on
// Postgres. UPDLOCK makes the existence check take a lock no other
// checker can share, and HOLDLOCK holds it — over the empty range too —
// until this transaction ends. So a second delivery of the same id waits
// here rather than passing the check alongside the first: once the
// winner commits, the row is visible, NOT EXISTS is false, and the loser
// inserts nothing and is told it lost instead of failing on a primary
// key violation.
func (Dialect) InsertSQL() string {
	return `INSERT INTO ` + Table + ` (event_id)
	SELECT @p1
	WHERE NOT EXISTS (
		SELECT 1 FROM ` + Table + ` WITH (UPDLOCK, HOLDLOCK, ROWLOCK) WHERE event_id = @p1
	)`
}
