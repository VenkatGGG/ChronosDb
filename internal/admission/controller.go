package admission

import (
	"errors"
	"fmt"
	"sync"
)

// Priority is the scheduler admission tier.
type Priority string

const (
	PriorityCritical   Priority = "critical"
	PriorityNormal     Priority = "normal"
	PriorityBackground Priority = "background"
)

// WorkKind identifies the admitted work class.
type WorkKind string

const (
	WorkRaftProgress     WorkKind = "raft_progress"
	WorkLeaseMaintenance WorkKind = "lease_maintenance"
	WorkIntentResolution WorkKind = "intent_resolution"
	WorkGC               WorkKind = "gc"
	WorkSnapshotSend     WorkKind = "snapshot_send"
	WorkRebalance        WorkKind = "rebalance"
	WorkCompaction       WorkKind = "compaction"
)

// ErrRejected reports that the request does not fit in the current admission budget.
var ErrRejected = errors.New("admission rejected")

// Config defines controller limits and compaction escalation thresholds.
type Config struct {
	TotalSlots                      int
	CriticalReserve                 int
	NormalReserve                   int
	CompactionL0Threshold           int
	CompactionPendingBytesThreshold uint64
}

// Request is one admission decision request.
type Request struct {
	Priority Priority
	Work     WorkKind
	Units    int
}

// Grant is a held admission slot reservation.
type Grant struct {
	controller *Controller
	priority   Priority
	units      int
	released   bool
}

// CompactionPressure describes the storage-engine pressure used to escalate compaction priority.
type CompactionPressure struct {
	L0Files                int
	PendingCompactionBytes uint64
}

// Snapshot reports the current admitted load.
type Snapshot struct {
	TotalInFlight      int
	CriticalInFlight   int
	NormalInFlight     int
	BackgroundInFlight int
}

// Controller enforces tiered admission with reserved capacity.
type Controller struct {
	mu    sync.Mutex
	cfg   Config
	usage map[Priority]int
	total int
}

// NewController constructs an admission controller.
func NewController(cfg Config) (*Controller, error) {
	switch {
	case cfg.TotalSlots <= 0:
		return nil, fmt.Errorf("admission: total slots must be positive")
	case cfg.CriticalReserve < 0 || cfg.NormalReserve < 0:
		return nil, fmt.Errorf("admission: reserves must be non-negative")
	case cfg.CriticalReserve+cfg.NormalReserve >= cfg.TotalSlots:
		return nil, fmt.Errorf("admission: reserves must leave at least one general slot")
	}
	return &Controller{
		cfg: cfg,
		usage: map[Priority]int{
			PriorityCritical:   0,
			PriorityNormal:     0,
			PriorityBackground: 0,
		},
	}, nil
}

// Acquire reserves slots for one request if capacity allows.
func (c *Controller) Acquire(req Request) (Grant, error) {
	if req.Units <= 0 {
		req.Units = 1
	}
	if err := validatePriority(req.Priority); err != nil {
		return Grant{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	allowed := c.allowedUnits(req.Priority)
	if c.total+req.Units > allowed {
		return Grant{}, fmt.Errorf("%w: %s work exceeds %s capacity", ErrRejected, req.Work, req.Priority)
	}
	c.usage[req.Priority] += req.Units
	c.total += req.Units
	return Grant{
		controller: c,
		priority:   req.Priority,
		units:      req.Units,
	}, nil
}

// Snapshot returns the current in-flight accounting.
func (c *Controller) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Snapshot{
		TotalInFlight:      c.total,
		CriticalInFlight:   c.usage[PriorityCritical],
		NormalInFlight:     c.usage[PriorityNormal],
		BackgroundInFlight: c.usage[PriorityBackground],
	}
}

// PriorityForWork returns the effective admission priority for one work item.
func (c *Controller) PriorityForWork(work WorkKind, base Priority, pressure CompactionPressure) (Priority, error) {
	if err := validatePriority(base); err != nil {
		return "", err
	}
	if work != WorkCompaction {
		return base, nil
	}
	if pressure.L0Files >= c.cfg.CompactionL0Threshold && c.cfg.CompactionL0Threshold > 0 {
		return PriorityNormal, nil
	}
	if pressure.PendingCompactionBytes >= c.cfg.CompactionPendingBytesThreshold && c.cfg.CompactionPendingBytesThreshold > 0 {
		return PriorityNormal, nil
	}
	return base, nil
}

// Release returns the reserved slots to the controller.
func (g *Grant) Release() {
	if g == nil || g.controller == nil || g.released {
		return
	}
	g.controller.mu.Lock()
	defer g.controller.mu.Unlock()
	g.controller.usage[g.priority] -= g.units
	g.controller.total -= g.units
	g.released = true
}

func (c *Controller) allowedUnits(priority Priority) int {
	switch priority {
	case PriorityCritical:
		return c.cfg.TotalSlots
	case PriorityNormal:
		return c.cfg.TotalSlots - c.cfg.CriticalReserve
	case PriorityBackground:
		return c.cfg.TotalSlots - c.cfg.CriticalReserve - c.cfg.NormalReserve
	default:
		return 0
	}
}

func validatePriority(priority Priority) error {
	switch priority {
	case PriorityCritical, PriorityNormal, PriorityBackground:
		return nil
	default:
		return fmt.Errorf("admission: unknown priority %q", priority)
	}
}
