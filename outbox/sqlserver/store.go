// Package sqlserver supplies the SQL Server SQL the outbox core needs,
// claiming batches with UPDLOCK/READPAST so concurrent relays take
// disjoint rows.
//
// Unlike Postgres, SQL Server has no session-level idle-in-transaction
// timeout to set, so Dialect does not implement outbox's optional
// claim-lease-timeout interface: a relay abandoning a claim here is
// reclaimed only once SQL Server notices the dropped connection.
// Configure TCP keepalive on the connection (e.g. this driver's
// "keepAlive" DSN parameter, in seconds) to bound that detection window.
package sqlserver

// Table is the outbox table every statement here targets.
const Table = "messaging_outbox"

// CreateTableSQL creates the outbox table. It is exported so callers can
// run it from their own migrations; this package never runs it.
//
// SQL Server has no CREATE TABLE IF NOT EXISTS, so the existence check is
// explicit. headers is binary rather than a text type because the core
// passes and scans it as bytes: SQL Server would implicitly convert those
// bytes to a text column by reinterpreting them as UCS-2, which corrupts
// the JSON.
const CreateTableSQL = `IF OBJECT_ID(N'` + Table + `', N'U') IS NULL
CREATE TABLE ` + Table + ` (
	id             BIGINT IDENTITY(1,1) PRIMARY KEY,
	topic          NVARCHAR(255) NOT NULL,
	[key]          VARBINARY(MAX),
	[value]        VARBINARY(MAX),
	headers        VARBINARY(MAX),
	attempts       INT NOT NULL DEFAULT 0,
	quarantined_at DATETIME2 NULL,
	created_at     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
)`

// Dialect is the SQL Server outbox.Dialect. Its zero value is ready to
// use.
type Dialect struct{}

// InsertSQL stages one row.
func (Dialect) InsertSQL() string {
	return `INSERT INTO ` + Table + ` (topic, [key], [value], headers) VALUES (@p1, @p2, @p3, @p4)`
}

// ClaimSQL locks up to @p1 rows in id order, skipping the rows another
// relay's transaction already holds. The locks live until the claiming
// transaction ends, which is what keeps two relays off the same row.
//
// UPDLOCK takes the update locks that outlast the SELECT, READPAST steps
// over rows another relay has locked instead of blocking on them, and
// ROWLOCK keeps the engine from escalating to a page lock that would hide
// unclaimed rows behind a claimed one. Rows already quarantined are
// excluded.
func (Dialect) ClaimSQL() string {
	return `SELECT TOP (@p1) id, topic, [key], [value], headers, attempts
	FROM ` + Table + ` WITH (UPDLOCK, READPAST, ROWLOCK)
	WHERE quarantined_at IS NULL
	ORDER BY id`
}

// DeleteSQL deletes the row with id @p1.
func (Dialect) DeleteSQL() string {
	return `DELETE FROM ` + Table + ` WHERE id = @p1`
}

// IncrementAttemptsSQL records a failed publish attempt against the row
// with id @p1.
func (Dialect) IncrementAttemptsSQL() string {
	return `UPDATE ` + Table + ` SET attempts = attempts + 1 WHERE id = @p1`
}

// QuarantineSQL marks the row with id @p1 as quarantined, excluding it
// from future ClaimSQL results.
func (Dialect) QuarantineSQL() string {
	return `UPDATE ` + Table + ` SET quarantined_at = SYSUTCDATETIME(), attempts = attempts + 1 WHERE id = @p1`
}
