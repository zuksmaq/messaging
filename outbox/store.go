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

	"github.com/zuksmaq/messaging"
)

// Row is a staged event. ID is assigned by the database and becomes the
// messaging.EventIDHeader value the Relay stamps on the published
// message.
type Row struct {
	ID      int64
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
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
	// holds. It returns the id, topic, key, value and JSON-encoded
	// headers columns, in that order.
	ClaimSQL() string

	// DeleteSQL deletes the row whose id is its one parameter.
	DeleteSQL() string
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
func (o *Outbox) Enqueue(ctx context.Context, tx *sql.Tx, topic string, key, value []byte, headers map[string][]byte) error {
	if tx == nil {
		return fmt.Errorf("%w: a transaction is required", messaging.ErrInvalidConfig)
	}
	if topic == "" {
		return fmt.Errorf("%w: a topic is required", messaging.ErrInvalidConfig)
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
