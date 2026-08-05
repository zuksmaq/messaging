// Package postgres supplies the Postgres SQL the outbox core needs,
// claiming batches with FOR UPDATE SKIP LOCKED so concurrent relays
// take disjoint rows.
package postgres

// Table is the outbox table every statement here targets.
const Table = "messaging_outbox"

// CreateTableSQL creates the outbox table. It is exported so callers can
// run it from their own migrations; this package never runs it.
const CreateTableSQL = `CREATE TABLE IF NOT EXISTS ` + Table + ` (
	id             BIGSERIAL PRIMARY KEY,
	topic          TEXT NOT NULL,
	"key"          BYTEA,
	"value"        BYTEA,
	headers        JSONB,
	attempts       INT NOT NULL DEFAULT 0,
	quarantined_at TIMESTAMPTZ,
	created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// Dialect is the Postgres outbox.Dialect. Its zero value is ready to
// use.
type Dialect struct{}

// InsertSQL stages one row.
func (Dialect) InsertSQL() string {
	return `INSERT INTO ` + Table + ` (topic, "key", "value", headers) VALUES ($1, $2, $3, $4)`
}

// ClaimSQL locks up to $1 rows in id order, skipping the rows another
// relay's transaction already holds and rows already quarantined. The
// locks live until the claiming transaction ends, which is what keeps
// two relays off the same row.
func (Dialect) ClaimSQL() string {
	return `SELECT id, topic, "key", "value", headers, attempts FROM ` + Table + `
	WHERE quarantined_at IS NULL
	ORDER BY id
	LIMIT $1
	FOR UPDATE SKIP LOCKED`
}

// DeleteSQL deletes the row with id $1.
func (Dialect) DeleteSQL() string {
	return `DELETE FROM ` + Table + ` WHERE id = $1`
}

// IncrementAttemptsSQL records a failed publish attempt against the row
// with id $1.
func (Dialect) IncrementAttemptsSQL() string {
	return `UPDATE ` + Table + ` SET attempts = attempts + 1 WHERE id = $1`
}

// QuarantineSQL marks the row with id $1 as quarantined, excluding it
// from future ClaimSQL results.
func (Dialect) QuarantineSQL() string {
	return `UPDATE ` + Table + ` SET quarantined_at = now(), attempts = attempts + 1 WHERE id = $1`
}
