// Package postgres supplies the Postgres SQL the inbox core needs,
// recording event ids with ON CONFLICT DO NOTHING so concurrent
// deliveries of one event settle on a single winner.
package postgres

// Table is the inbox table every statement here targets.
const Table = "messaging_inbox"

// CreateTableSQL creates the inbox table. It is exported so callers can
// run it from their own migrations; this package never runs it.
//
// The event id is the primary key, which is what makes recording it a
// race the database decides rather than one the application has to lock
// for.
const CreateTableSQL = `CREATE TABLE IF NOT EXISTS ` + Table + ` (
	event_id     TEXT PRIMARY KEY,
	processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Dialect is the Postgres inbox.Dialect. Its zero value is ready to use.
type Dialect struct{}

// SelectSQL looks for the event id $1. Under Postgres' default read
// committed isolation this neither sees nor blocks on a row a concurrent
// transaction has inserted but not committed, so it reports on committed
// work only.
func (Dialect) SelectSQL() string {
	return `SELECT 1 FROM ` + Table + ` WHERE event_id = $1`
}

// InsertSQL records the event id $1, reporting one row affected only if
// this statement is the one that inserted it. Against a concurrent
// uncommitted insert of the same id, ON CONFLICT DO NOTHING waits for
// that transaction to finish and then reports no rows, so the loser of
// the race is told it lost rather than failing on a unique violation.
func (Dialect) InsertSQL() string {
	return `INSERT INTO ` + Table + ` (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING`
}
