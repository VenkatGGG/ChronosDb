package hlc

import (
	"context"
	"errors"
	"sync"
)

// Reader exposes the minimal clock operations needed by lease and read logic.
type Reader interface {
	Now() Timestamp
	OffsetExceeded() bool
}

// Waiter exposes the ability to block until a timestamp becomes locally safe.
type Waiter interface {
	Reader
	WaitUntil(context.Context, Timestamp) error
}

// ErrClockOffsetExceeded reports that the clock is outside the configured max offset.
var ErrClockOffsetExceeded = errors.New("clock max offset exceeded")

// ManualClock is a deterministic HLC-backed test clock with wait support.
type ManualClock struct {
	mu             sync.Mutex
	now            Timestamp
	offsetExceeded bool
	waiters        []chan struct{}
}

// NewManualClock creates a deterministic manual clock.
func NewManualClock(initial Timestamp) *ManualClock {
	return &ManualClock{now: initial}
}

// Now returns the current local timestamp.
func (c *ManualClock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// OffsetExceeded reports whether the clock is currently outside max offset.
func (c *ManualClock) OffsetExceeded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offsetExceeded
}

// SetOffsetExceeded changes the clock health flag.
func (c *ManualClock) SetOffsetExceeded(exceeded bool) {
	c.mu.Lock()
	c.offsetExceeded = exceeded
	waiters := c.popWaitersLocked()
	c.mu.Unlock()
	closeAll(waiters)
}

// Set forces the clock to a specific timestamp.
func (c *ManualClock) Set(ts Timestamp) {
	c.mu.Lock()
	c.now = ts
	waiters := c.popWaitersLocked()
	c.mu.Unlock()
	closeAll(waiters)
}

// AdvanceTo advances the clock if the target is greater than the current time.
func (c *ManualClock) AdvanceTo(ts Timestamp) {
	c.mu.Lock()
	if ts.Compare(c.now) <= 0 {
		c.mu.Unlock()
		return
	}
	c.now = ts
	waiters := c.popWaitersLocked()
	c.mu.Unlock()
	closeAll(waiters)
}

// Tick advances the clock by one logical tick and returns the new timestamp.
func (c *ManualClock) Tick() Timestamp {
	c.mu.Lock()
	c.now = c.now.Next()
	now := c.now
	waiters := c.popWaitersLocked()
	c.mu.Unlock()
	closeAll(waiters)
	return now
}

// WaitUntil blocks until the local timestamp reaches target or the context fails.
func (c *ManualClock) WaitUntil(ctx context.Context, target Timestamp) error {
	for {
		c.mu.Lock()
		if c.offsetExceeded {
			c.mu.Unlock()
			return ErrClockOffsetExceeded
		}
		if c.now.Compare(target) >= 0 {
			c.mu.Unlock()
			return nil
		}
		ch := make(chan struct{})
		c.waiters = append(c.waiters, ch)
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
}

// CommitWait blocks until the local clock has reached target.
func CommitWait(ctx context.Context, clock Waiter, target Timestamp) error {
	return clock.WaitUntil(ctx, target)
}

func (c *ManualClock) popWaitersLocked() []chan struct{} {
	waiters := c.waiters
	c.waiters = nil
	return waiters
}

func closeAll(waiters []chan struct{}) {
	for _, ch := range waiters {
		close(ch)
	}
}
