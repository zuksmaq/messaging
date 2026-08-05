package kafka

import (
	"context"
	"time"
)

// ClampTimeout returns base, shortened to whatever time remains on
// ctx's deadline if that's sooner. A context that is already
// cancelled or past its deadline clamps to zero.
func ClampTimeout(ctx context.Context, base time.Duration) time.Duration {
	if ctx.Err() != nil {
		return 0
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < base {
			base = remaining
		}
	}
	if base < 0 {
		base = 0
	}
	return base
}
