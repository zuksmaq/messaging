package consumer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zuksmaq/messaging"
	"github.com/zuksmaq/messaging/kafka"
)

// TestJoinCloseErrors covers Close's error-combination logic directly:
// a synthetic client-close failure and registry-close failure must
// both surface, not just one.
func TestJoinCloseErrors(t *testing.T) {
	t.Parallel()

	clientErr := errors.New("client boom")
	registryErr := errors.New("registry boom")

	tests := map[string]struct {
		clientErr   error
		registryErr error
		wantNil     bool
		wantBroker  bool
		wantMention []string
	}{
		"both fail": {
			clientErr: clientErr, registryErr: registryErr,
			wantBroker:  true,
			wantMention: []string{"client boom", "registry boom"},
		},
		"only client fails": {
			clientErr: clientErr,
			wantBroker:  true,
			wantMention: []string{"client boom"},
		},
		"only registry fails": {
			registryErr: registryErr,
			wantMention: []string{"registry boom"},
		},
		"neither fails": {
			wantNil: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := joinCloseErrors(tc.clientErr, tc.registryErr)

			if tc.wantNil {
				if got != nil {
					t.Fatalf("joinCloseErrors = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("joinCloseErrors = nil, want a non-nil error")
			}
			if tc.wantBroker && !errors.Is(got, messaging.ErrBroker) {
				t.Errorf("joinCloseErrors() = %v, want it to wrap ErrBroker", got)
			}
			for _, want := range tc.wantMention {
				if !strings.Contains(got.Error(), want) {
					t.Errorf("joinCloseErrors() = %q, want it to mention %q", got.Error(), want)
				}
			}
		})
	}
}

// TestCloseWaitsForInFlightConsume races Close against a concurrent
// Consume loop: run under -race, this proves pollMu actually serializes
// access to the client rather than merely happening not to crash.
func TestCloseWaitsForInFlightConsume(t *testing.T) {
	t.Parallel()

	c, err := New[string, []byte](Config{
		BootstrapServers: "127.0.0.1:1",
		GroupID:          "race-test",
		Topics:           []string{"unreachable"},
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatBytes,
	})
	if err != nil {
		t.Fatalf("New = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if _, err := c.Consume(ctx); err != nil {
				return
			}
		}
	}()

	// Give Consume a moment to start polling before racing Close
	// against it.
	time.Sleep(10 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- c.Close() }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Close() = %v, want nil against an unreachable broker with no registry", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return — it may be deadlocked waiting on an in-flight poll")
	}

	cancel()
	wg.Wait()
}

// TestConsumeAfterCloseReturnsPromptly asserts Consume never touches the
// client once Close has run, instead of polling an already-destroyed
// handle.
func TestConsumeAfterCloseReturnsPromptly(t *testing.T) {
	t.Parallel()

	c, err := New[string, []byte](Config{
		BootstrapServers: "127.0.0.1:1",
		GroupID:          "post-close-test",
		Topics:           []string{"unreachable"},
		KeyFormat:        kafka.FormatString,
		ValueFormat:      kafka.FormatBytes,
	})
	if err != nil {
		t.Fatalf("New = %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	start := time.Now()
	_, err = c.Consume(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Error("Consume() after Close() = nil error, want an error")
	}
	if elapsed > time.Second {
		t.Errorf("Consume() after Close() took %s, want near-immediate", elapsed)
	}
}
