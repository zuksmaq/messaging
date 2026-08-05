package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/zuksmaq/messaging/kafka"
)

func TestClampTimeout_NoDeadline(t *testing.T) {
	got := kafka.ClampTimeout(context.Background(), 5*time.Second)
	if got != 5*time.Second {
		t.Errorf("got %s, want %s", got, 5*time.Second)
	}
}

func TestClampTimeout_DeadlineShorterThanBase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	got := kafka.ClampTimeout(ctx, 5*time.Second)
	if got <= 0 || got > 50*time.Millisecond {
		t.Errorf("got %s, want (0, 50ms]", got)
	}
}

func TestClampTimeout_DeadlineLongerThanBase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got := kafka.ClampTimeout(ctx, 50*time.Millisecond)
	if got != 50*time.Millisecond {
		t.Errorf("got %s, want %s", got, 50*time.Millisecond)
	}
}

func TestClampTimeout_AlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := kafka.ClampTimeout(ctx, 5*time.Second)
	if got != 0 {
		t.Errorf("got %s, want 0", got)
	}
}

func TestClampTimeout_AlreadyExpiredDeadline(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	got := kafka.ClampTimeout(ctx, 5*time.Second)
	if got != 0 {
		t.Errorf("got %s, want 0", got)
	}
}
