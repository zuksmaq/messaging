// Package outbox stages events in the caller's own database
// transaction and relays them to a broker at-least-once.
//
// Enqueue writes an outbox row against an already-open *sql.Tx, so the
// row commits atomically with the business writes it belongs to. A
// separate Relay polls the table, publishes each staged row, and
// deletes only the rows the broker confirmed durable.
//
// Keys and values are staged as bytes: the outbox is deliberately
// unaware of wire formats, so callers serialize before staging and the
// Relay publishes through a messaging.Producer[[]byte, []byte].
package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/zuksmaq/messaging"
)

// MaxTopicLength is the longest topic name Enqueue accepts. It matches
// the SQL Server dialect's NVARCHAR(255) topic column (see
// outbox/sqlserver.CreateTableSQL) so an over-limit topic is rejected the
// same way regardless of which dialect is configured, instead of a value
// the Postgres dialect would accept silently rolling back the caller's
// business transaction only on SQL Server. The inbox module enforces the
// same numeric limit on its own bounded column.
const MaxTopicLength = 255

// Row is a staged event. ID is assigned by the database and becomes the
// messaging.EventIDHeader value the Relay stamps on the published
// message.
type Row struct {
	ID      int64
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte

	// Attempts is how many times a publish of this row has failed so
	// far. The Relay uses it to decide when to quarantine the row.
	Attempts int
}

// Dialect supplies the SQL that differs per database. The core package
// holds no SQL of its own; outbox/postgres and outbox/sqlserver
// implement this.
type Dialect interface {
	// InsertSQL stages one row. Its parameters are, in order, the
	// topic, key, value and JSON-encoded headers.
	InsertSQL() string

	// ClaimSQL selects up to its one parameter's worth of rows in id
	// order, under a row lock that skips rows another relay already
	// holds, excluding rows already quarantined. It returns the id,
	// topic, key, value, JSON-encoded headers and attempts columns, in
	// that order.
	ClaimSQL() string

	// DeleteSQL deletes the row whose id is its one parameter.
	DeleteSQL() string

	// IncrementAttemptsSQL records a failed publish attempt against the
	// row whose id is its one parameter.
	IncrementAttemptsSQL() string

	// QuarantineSQL marks the row whose id is its one parameter as
	// quarantined, excluding it from future ClaimSQL results.
	QuarantineSQL() string
}

// Outbox stages events for a single dialect.
type Outbox struct {
	dialect Dialect
}

// New builds an Outbox writing d's SQL. It returns an error wrapping
// messaging.ErrInvalidConfig if d is nil.
func New(d Dialect) (*Outbox, error) {
	if d == nil {
		return nil, fmt.Errorf("%w: a dialect is required", messaging.ErrInvalidConfig)
	}
	return &Outbox{dialect: d}, nil
}

// Enqueue stages an event on tx, the caller's own already-open
// transaction: the row becomes visible to a Relay only once the caller
// commits, and disappears with a rollback.
//
// It returns an error wrapping messaging.ErrInvalidConfig if o was not
// built with New, or if headers keys messaging.EventIDHeader, which the
// Relay reserves for the row's own id.
func (o *Outbox) Enqueue(ctx context.Context, tx *sql.Tx, topic string, key, value []byte, headers map[string][]byte) error {
	if o.dialect == nil {
		return fmt.Errorf("%w: outbox was not constructed with New", messaging.ErrInvalidConfig)
	}
	if tx == nil {
		return fmt.Errorf("%w: a transaction is required", messaging.ErrInvalidConfig)
	}
	if topic == "" {
		return fmt.Errorf("%w: a topic is required", messaging.ErrInvalidConfig)
	}
	if n := utf8.RuneCountInString(topic); n > MaxTopicLength {
		return fmt.Errorf("%w: topic %q is %d characters, exceeds the %d-character limit", messaging.ErrInvalidConfig, topic, n, MaxTopicLength)
	}
	if _, ok := headers[messaging.EventIDHeader]; ok {
		return fmt.Errorf("%w: header %q is reserved for the outbox's event id", messaging.ErrInvalidConfig, messaging.EventIDHeader)
	}

	encoded, err := encodeHeaders(headers)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, o.dialect.InsertSQL(), topic, key, value, encoded); err != nil {
		return fmt.Errorf("staging outbox row for topic %q: %w", topic, err)
	}
	return nil
}

// encodeHeaders renders headers for the JSON headers column. Values are
// bytes, which encoding/json represents as base64 strings.
func encodeHeaders(headers map[string][]byte) ([]byte, error) {
	if len(headers) == 0 {
		return nil, nil
	}
	encoded, err := json.Marshal(headers)
	if err != nil {
		return nil, fmt.Errorf("%w: encoding outbox headers: %w", messaging.ErrSerialization, err)
	}
	return encoded, nil
}

// decodeHeaders reverses encodeHeaders. A NULL or absent column decodes
// to a nil map.
func decodeHeaders(encoded []byte) (map[string][]byte, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	var headers map[string][]byte
	if err := json.Unmarshal(encoded, &headers); err != nil {
		return nil, fmt.Errorf("%w: decoding outbox headers: %w", messaging.ErrDeserialization, err)
	}
	return headers, nil
}
