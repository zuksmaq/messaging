package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
)

// TestCommitWithCancelledContextReturnsPromptly covers Commit under an
// already-cancelled context: it must notice immediately and return
// ctx.Err() rather than attempting the broker call and waiting out the
// full configured CommitTimeout.
func TestCommitWithCancelledContextReturnsPromptly(t *testing.T) {
	t.Parallel()

	c, err := New[string, []byte](Config{
		BootstrapServers: "127.0.0.1:1",
		GroupID:          "commit-test",
		Topics:           []string{"commit-test"},
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatBytes,
		CommitTimeout:    10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err = c.Commit(ctx, messaging.ReceivedMessage[string, []byte]{Topic: "commit-test"})
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Errorf("Commit() error = %v, want context.Canceled", err)
	}
	if elapsed > time.Second {
		t.Errorf("Commit() with a cancelled context took %s, want near-immediate", elapsed)
	}
}

// TestCommitWithExpiredContextReturnsPromptly covers Commit under a
// context whose deadline has already passed.
func TestCommitWithExpiredContextReturnsPromptly(t *testing.T) {
	t.Parallel()

	c, err := New[string, []byte](Config{
		BootstrapServers: "127.0.0.1:1",
		GroupID:          "commit-test",
		Topics:           []string{"commit-test"},
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatBytes,
		CommitTimeout:    10 * time.Second,
	})
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	start := time.Now()
	err = c.Commit(ctx, messaging.ReceivedMessage[string, []byte]{Topic: "commit-test"})
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Commit() error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed > time.Second {
		t.Errorf("Commit() with an expired context took %s, want near-immediate", elapsed)
	}
}
