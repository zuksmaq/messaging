package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/outbox"
	"github.com/zuksmaq/messaging/outbox/postgres"
	"github.com/zuksmaq/messaging/outbox/sqlserver"
)

// nopProducer satisfies messaging.Producer for the config tests, which
// never publish anything.
type nopProducer struct{}

func (nopProducer) Produce(context.Context, string, []byte, []byte, map[string][]byte) (messaging.ProducedMessage, error) {
	return messaging.ProducedMessage{Status: messaging.Persisted}, nil
}

func (nopProducer) Close(context.Context) error { return nil }

func TestRelayConfigValidate(t *testing.T) {
	// A non-nil pool is enough: Validate must not touch the database.
	db := &sql.DB{}

	valid := outbox.RelayConfig{
		DB:       db,
		Dialect:  postgres.Dialect{},
		Producer: nopProducer{},
	}

	tests := []struct {
		name    string
		cfg     outbox.RelayConfig
		wantErr bool
	}{
		{name: "mandatory fields only", cfg: valid},
		{
			name: "batch size and poll interval set",
			cfg: outbox.RelayConfig{
				DB: db, Dialect: postgres.Dialect{}, Producer: nopProducer{},
				BatchSize: 50, PollInterval: time.Second,
			},
		},
		{
			name:    "no database",
			cfg:     outbox.RelayConfig{Dialect: postgres.Dialect{}, Producer: nopProducer{}},
			wantErr: true,
		},
		{
			name:    "no dialect",
			cfg:     outbox.RelayConfig{DB: db, Producer: nopProducer{}},
			wantErr: true,
		},
		{
			name:    "no producer",
			cfg:     outbox.RelayConfig{DB: db, Dialect: postgres.Dialect{}},
			wantErr: true,
		},
		{
			name: "negative batch size",
			cfg: outbox.RelayConfig{
				DB: db, Dialect: postgres.Dialect{}, Producer: nopProducer{}, BatchSize: -1,
			},
			wantErr: true,
		},
		{
			name: "negative poll interval",
			cfg: outbox.RelayConfig{
				DB: db, Dialect: postgres.Dialect{}, Producer: nopProducer{}, PollInterval: -time.Second,
			},
			wantErr: true,
		},
		{
			name: "lease timeout set",
			cfg: outbox.RelayConfig{
				DB: db, Dialect: postgres.Dialect{}, Producer: nopProducer{}, LeaseTimeout: 30 * time.Second,
			},
		},
		{
			name: "negative lease timeout",
			cfg: outbox.RelayConfig{
				DB: db, Dialect: postgres.Dialect{}, Producer: nopProducer{}, LeaseTimeout: -time.Second,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cfg.Validate()
			if tt.wantErr {
				if !errors.Is(err, messaging.ErrInvalidConfig) {
					t.Fatalf("Validate() = %v, want an error wrapping ErrInvalidConfig", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}

			// NewRelay accepts exactly what Validate accepts.
			if _, err := outbox.NewRelay(tt.cfg); err != nil {
				t.Fatalf("NewRelay() = %v, want nil", err)
			}
		})
	}
}

func TestNewRelayRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	if _, err := outbox.NewRelay(outbox.RelayConfig{}); !errors.Is(err, messaging.ErrInvalidConfig) {
		t.Fatalf("NewRelay(zero config) = %v, want an error wrapping ErrInvalidConfig", err)
	}
}

func TestNewRequiresDialect(t *testing.T) {
	t.Parallel()

	if _, err := outbox.New(nil); !errors.Is(err, messaging.ErrInvalidConfig) {
		t.Fatalf("New(nil) = %v, want an error wrapping ErrInvalidConfig", err)
	}
}

// TestEnqueueRejectsMissingArguments covers the guards that do not need a
// database: there is deliberately no Enqueue that opens its own
// transaction, so a caller must pass one.
func TestEnqueueRejectsMissingArguments(t *testing.T) {
	t.Parallel()

	ob, err := outbox.New(postgres.Dialect{})
	if err != nil {
		t.Fatalf("New() = %v", err)
	}

	t.Run("no transaction", func(t *testing.T) {
		t.Parallel()

		err := ob.Enqueue(context.Background(), nil, "orders", nil, nil, nil)
		if !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Fatalf("Enqueue(nil tx) = %v, want an error wrapping ErrInvalidConfig", err)
		}
	})

	t.Run("no topic", func(t *testing.T) {
		t.Parallel()

		// A non-nil transaction value is enough: the topic is checked
		// before the statement runs.
		err := ob.Enqueue(context.Background(), &sql.Tx{}, "", nil, nil, nil)
		if !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Fatalf("Enqueue(empty topic) = %v, want an error wrapping ErrInvalidConfig", err)
		}
	})

	t.Run("reserved event id header", func(t *testing.T) {
		t.Parallel()

		headers := map[string][]byte{messaging.EventIDHeader: []byte("caller-supplied")}
		err := ob.Enqueue(context.Background(), &sql.Tx{}, "orders", nil, nil, headers)
		if !errors.Is(err, messaging.ErrInvalidConfig) {
			t.Fatalf("Enqueue(reserved header) = %v, want an error wrapping ErrInvalidConfig", err)
		}
	})
}

// TestZeroValueOutboxFailsSafely covers an &Outbox{} built by a DI
// container bypassing New: it has no dialect, so Enqueue must return a
// configuration error rather than dereference a nil interface.
func TestZeroValueOutboxFailsSafely(t *testing.T) {
	t.Parallel()

	var ob outbox.Outbox
	err := ob.Enqueue(context.Background(), &sql.Tx{}, "orders", nil, nil, nil)
	if !errors.Is(err, messaging.ErrInvalidConfig) {
		t.Fatalf("Enqueue on a zero-value Outbox = %v, want an error wrapping ErrInvalidConfig", err)
	}
}

// TestZeroValueRelayRunFailsSafely covers an &Relay{} built by a DI
// container bypassing NewRelay: it has no database, dialect or producer,
// so Run must return a configuration error rather than panic.
func TestZeroValueRelayRunFailsSafely(t *testing.T) {
	t.Parallel()

	var r outbox.Relay
	err := r.Run(context.Background())
	if !errors.Is(err, messaging.ErrInvalidConfig) {
		t.Fatalf("Run on a zero-value Relay = %v, want an error wrapping ErrInvalidConfig", err)
	}
}

// TestPostgresDialectSQL pins the parts of the Postgres statements the
// core relies on: the claim must take a lock that skips rows another
// relay holds, or two relays would duplicate work.
func TestPostgresDialectSQL(t *testing.T) {
	t.Parallel()

	var d outbox.Dialect = postgres.Dialect{}

	for _, tt := range []struct{ name, sql, want string }{
		{"claim locks", d.ClaimSQL(), "FOR UPDATE SKIP LOCKED"},
		{"claim is ordered", d.ClaimSQL(), "ORDER BY id"},
		{"claim is bounded", d.ClaimSQL(), "LIMIT $1"},
		{"insert targets the table", d.InsertSQL(), postgres.Table},
		{"delete is by id", d.DeleteSQL(), "WHERE id = $1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if !strings.Contains(tt.sql, tt.want) {
				t.Errorf("SQL %q does not contain %q", tt.sql, tt.want)
			}
		})
	}
}

// TestPostgresLeaseSQL pins the claim lease/timeout mechanism: Relay
// sets this per claiming transaction so Postgres itself aborts one left
// idle past timeout, releasing the row locks FOR UPDATE took instead of
// leaving them held until something else notices the dead connection.
func TestPostgresLeaseSQL(t *testing.T) {
	t.Parallel()

	got := postgres.Dialect{}.LeaseSQL(30 * time.Second)
	if !strings.Contains(got, "idle_in_transaction_session_timeout") {
		t.Errorf("LeaseSQL(30s) = %q, want it to set idle_in_transaction_session_timeout", got)
	}
	if !strings.Contains(got, "30000") {
		t.Errorf("LeaseSQL(30s) = %q, want the timeout expressed in milliseconds", got)
	}
}

// TestSQLServerDialectSQL pins the same guarantees for SQL Server:
// UPDLOCK holds the claim's locks past the SELECT, READPAST skips the
// rows another relay holds instead of blocking on them, and ROWLOCK stops
// a lock escalation from hiding unclaimed rows behind a claimed one.
func TestSQLServerDialectSQL(t *testing.T) {
	t.Parallel()

	var d outbox.Dialect = sqlserver.Dialect{}

	for _, tt := range []struct{ name, sql, want string }{
		{"claim locks", d.ClaimSQL(), "UPDLOCK, READPAST, ROWLOCK"},
		{"claim is ordered", d.ClaimSQL(), "ORDER BY id"},
		{"claim is bounded", d.ClaimSQL(), "TOP (@p1)"},
		{"insert targets the table", d.InsertSQL(), sqlserver.Table},
		{"delete is by id", d.DeleteSQL(), "WHERE id = @p1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if !strings.Contains(tt.sql, tt.want) {
				t.Errorf("SQL %q does not contain %q", tt.sql, tt.want)
			}
		})
	}
}
