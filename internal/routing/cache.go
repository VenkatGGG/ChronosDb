package routing

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/meta"
)

// ErrorCode is the canonical routing error set that invalidates cache entries.
type ErrorCode string

const (
	ErrorCodeRangeNotHere    ErrorCode = "RANGE_NOT_HERE"
	ErrorCodeRangeSplit      ErrorCode = "RANGE_SPLIT"
	ErrorCodeLeaderChanged   ErrorCode = "LEADER_CHANGED"
	ErrorCodeDescriptorStale ErrorCode = "DESCRIPTOR_STALE"
)

// RoutingError carries the information required to invalidate a stale cache entry.
type RoutingError struct {
	Code    ErrorCode
	RangeID uint64
}

// Config controls cache ttl and jitter.
type Config struct {
	TTLSeconds     time.Duration
	JitterFraction float64
	Now            func() time.Time
	Rand           *rand.Rand
}

type entry struct {
	desc      meta.RangeDescriptor
	expiresAt time.Time
}

// RangeCache is the client-side descriptor cache used by the gateway and KV client.
type RangeCache struct {
	mu     sync.RWMutex
	now    func() time.Time
	ttl    time.Duration
	jitter float64
	rand   *rand.Rand
	byID   map[uint64]entry
}

// NewRangeCache constructs a descriptor cache with TTL+jitter expiration.
func NewRangeCache(cfg Config) *RangeCache {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	ttl := cfg.TTLSeconds
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	jitter := cfg.JitterFraction
	if jitter <= 0 {
		jitter = 0.1
	}
	rng := cfg.Rand
	if rng == nil {
		rng = rand.New(rand.NewSource(1))
	}
	return &RangeCache{
		now:    now,
		ttl:    ttl,
		jitter: jitter,
		rand:   rng,
		byID:   make(map[uint64]entry),
	}
}

// Upsert inserts or replaces one descriptor.
func (c *RangeCache) Upsert(desc meta.RangeDescriptor) error {
	if err := desc.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byID[desc.RangeID] = entry{
		desc:      desc,
		expiresAt: c.now().Add(c.jitteredTTL()),
	}
	return nil
}

// Lookup resolves the descriptor containing key if it exists and is not expired.
func (c *RangeCache) Lookup(key []byte) (meta.RangeDescriptor, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	for rangeID, entry := range c.byID {
		if now.After(entry.expiresAt) {
			delete(c.byID, rangeID)
			continue
		}
		if entry.desc.ContainsKey(key) {
			return entry.desc, true
		}
	}
	return meta.RangeDescriptor{}, false
}

// Invalidate removes one cached descriptor by range id.
func (c *RangeCache) Invalidate(rangeID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.byID, rangeID)
}

// HandleRoutingError applies the error-driven invalidation policy.
func (c *RangeCache) HandleRoutingError(err RoutingError) error {
	switch err.Code {
	case ErrorCodeRangeNotHere, ErrorCodeRangeSplit, ErrorCodeLeaderChanged, ErrorCodeDescriptorStale:
		c.Invalidate(err.RangeID)
		return nil
	default:
		return fmt.Errorf("routing error %q does not define cache invalidation", err.Code)
	}
}

func (c *RangeCache) jitteredTTL() time.Duration {
	if c.jitter <= 0 {
		return c.ttl
	}
	delta := c.jitter * (2*c.rand.Float64() - 1)
	jittered := float64(c.ttl) * (1 + delta)
	return time.Duration(math.Max(float64(time.Second), jittered))
}
