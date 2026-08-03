package inbox_test

import (
	"context"
	"errors"
	"testing"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/inbox"
	"github.com/zuksmaq/messaging/inbox/postgres"
)

func TestNewRequiresADialect(t *testing.T) {
	if _, err := inbox.New(nil); !errors.Is(err, messaging.ErrInvalidConfig) {
		t.Errorf("New(nil) = %v, want an error wrapping ErrInvalidConfig", err)
	}
	if _, err := inbox.New(postgres.Dialect{}); err != nil {
		t.Errorf("New(postgres.Dialect{}) = %v, want no error", err)
	}
}

// TestArgumentsAreRejectedBeforeTouchingTheDatabase covers the guards
// both methods share. A nil transaction is the interesting one: the API
// only works against the caller's own transaction, so there is nothing
// sensible to fall back to.
func TestArgumentsAreRejectedBeforeTouchingTheDatabase(t *testing.T) {
	in, err := inbox.New(postgres.Dialect{})
	if err != nil {
		t.Fatalf("building inbox: %v", err)
	}

	tests := []struct {
		name    string
		eventID string
	}{
		{name: "no transaction", eventID: "evt-1"},
		{name: "no event id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			// A nil *sql.Tx would panic if either method reached it, so
			// these calls returning an error is itself the assertion that
			// the guards run first.
			if _, err := in.HasProcessed(ctx, nil, tt.eventID); !errors.Is(err, messaging.ErrInvalidConfig) {
				t.Errorf("HasProcessed = %v, want an error wrapping ErrInvalidConfig", err)
			}
			if _, err := in.MarkProcessed(ctx, nil, tt.eventID); !errors.Is(err, messaging.ErrInvalidConfig) {
				t.Errorf("MarkProcessed = %v, want an error wrapping ErrInvalidConfig", err)
			}
		})
	}
}
