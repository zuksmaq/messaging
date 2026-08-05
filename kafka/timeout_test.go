package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/zuksmaq/messaging/kafka"
)

func TestClampTimeout(t *testing.T) {
	tests := []struct {
		name  string
		ctx   func() (context.Context, context.CancelFunc)
		base  time.Duration
		check func(t *testing.T, got time.Duration)
	}{
		{
			name: "no deadline",
			ctx:  func() (context.Context, context.CancelFunc) { return context.Background(), func() {} },
			base: 5 * time.Second,
			check: func(t *testing.T, got time.Duration) {
				if got != 5*time.Second {
					t.Errorf("got %s, want %s", got, 5*time.Second)
				}
			},
		},
		{
			name: "deadline shorter than base",
			ctx:  func() (context.Context, context.CancelFunc) { return context.WithTimeout(context.Background(), 50*time.Millisecond) },
			base: 5 * time.Second,
			check: func(t *testing.T, got time.Duration) {
				if got <= 0 || got > 50*time.Millisecond {
					t.Errorf("got %s, want (0, 50ms]", got)
				}
			},
		},
		{
			name: "deadline longer than base",
			ctx:  func() (context.Context, context.CancelFunc) { return context.WithTimeout(context.Background(), 5*time.Second) },
			base: 50 * time.Millisecond,
			check: func(t *testing.T, got time.Duration) {
				if got != 50*time.Millisecond {
					t.Errorf("got %s, want %s", got, 50*time.Millisecond)
				}
			},
		},
		{
			name: "already cancelled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			base: 5 * time.Second,
			check: func(t *testing.T, got time.Duration) {
				if got != 0 {
					t.Errorf("got %s, want 0", got)
				}
			},
		},
		{
			name: "already expired deadline",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			base: 5 * time.Second,
			check: func(t *testing.T, got time.Duration) {
				if got != 0 {
					t.Errorf("got %s, want 0", got)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.ctx()
			defer cancel()

			tt.check(t, kafka.ClampTimeout(ctx, tt.base))
		})
	}
}
