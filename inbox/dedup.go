// Package inbox makes a consumer idempotent by recording the events it
// has already handled in the caller's own database transaction.
//
// A handler asks HasProcessed before doing its work and calls
// MarkProcessed after; both run against an already-open *sql.Tx, so the
// inbox row commits atomically with the business writes it accounts for
// and a rollback discards the two together. There is deliberately no
// variant that opens its own transaction — that would reintroduce the
// dual write the pattern exists to avoid.
//
// The de-duplication key is the transport-agnostic event id: the string
// the outbox relay stamps as messaging.EventIDHeader and a consumer
// reads back off messaging.ReceivedMessage.EventID. Nothing here knows
// which broker delivered the message.
package inbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/zuksmaq/messaging"
)

// MaxEventIDLength is the longest event id HasProcessed and MarkProcessed
// accept. It matches the SQL Server dialect's NVARCHAR(255) event_id
// column (see inbox/sqlserver.CreateTableSQL) so an over-limit event id
// is rejected the same way regardless of which dialect is configured.
// The outbox module enforces the same numeric limit on its own bounded
// column.
const MaxEventIDLength = 255

// Dialect supplies the SQL that differs per database. The core package
// holds no SQL of its own; inbox/postgres implements this.
type Dialect interface {
	// SelectSQL returns at most one row if the event id in its one
	// parameter has been recorded. It must not block on an id another
	// transaction has staged but not yet committed — HasProcessed
	// reports on committed work only.
	SelectSQL() string

	// InsertSQL records the event id in its one parameter, inserting
	// nothing if that id is already recorded. Its rows-affected count is
	// how MarkProcessed tells the two cases apart, so exactly one
	// transaction may report an insert for a given id.
	InsertSQL() string
}

// Inbox dedups events for a single dialect.
type Inbox struct {
	dialect Dialect
}

// New builds an Inbox writing d's SQL. It returns an error wrapping
// messaging.ErrInvalidConfig if d is nil.
func New(d Dialect) (*Inbox, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: a dialect is required", messaging.ErrInvalidConfig)
	}
	return &Inbox{dialect: d}, nil
}

// HasProcessed reports whether eventID was recorded by a transaction
// that has committed.
//
// An id another transaction has staged but not yet committed reads as
// false, which is what makes the duplicate-delivery race visible rather
// than hidden: two concurrent deliveries of one event both see false and
// both go on to do the work. MarkProcessed is where that race is
// settled.
func (i *Inbox) HasProcessed(ctx context.Context, tx *sql.Tx, eventID string) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("%w: a transaction is required", messaging.ErrInvalidConfig)
	}
	if eventID == "" {
		return false, fmt.Errorf("%w: an event id is required", messaging.ErrInvalidConfig)
	}
	if n := utf8.RuneCountInString(eventID); n > MaxEventIDLength {
		return false, fmt.Errorf("%w: event id %q is %d characters, exceeds the %d-character limit", messaging.ErrInvalidConfig, eventID, n, MaxEventIDLength)
	}

	var found int
	err := tx.QueryRowContext(ctx, i.dialect.SelectSQL(), eventID).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("checking the inbox for event %q: %w", eventID, err)
	}
	return true, nil
}

// MarkProcessed records eventID on tx, the caller's own already-open
// transaction, so the record commits with the handler's business writes
// and disappears with a rollback.
//
// It reports whether this transaction is the one that recorded the id.
// False means another transaction got there first — either one that had
// already committed, or a concurrent one this call waited on — and the
// caller must not commit its work: roll back and let the event be
// re-delivered, by which point HasProcessed reports true and the
// delivery is skipped. That is the whole resolution of the
// duplicate-delivery race, and it is why recording an id twice is not an
// error: a caller that skipped HasProcessed still cannot write a
// duplicate row, it just learns that it lost.
func (i *Inbox) MarkProcessed(ctx context.Context, tx *sql.Tx, eventID string) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("%w: a transaction is required", messaging.ErrInvalidConfig)
	}
	if eventID == "" {
		return false, fmt.Errorf("%w: an event id is required", messaging.ErrInvalidConfig)
	}
	if n := utf8.RuneCountInString(eventID); n > MaxEventIDLength {
		return false, fmt.Errorf("%w: event id %q is %d characters, exceeds the %d-character limit", messaging.ErrInvalidConfig, eventID, n, MaxEventIDLength)
	}

	result, err := tx.ExecContext(ctx, i.dialect.InsertSQL(), eventID)
	if err != nil {
		return false, fmt.Errorf("recording event %q in the inbox: %w", eventID, err)
	}
	recorded, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("counting the inbox rows written for event %q: %w", eventID, err)
	}
	return recorded > 0, nil
}
