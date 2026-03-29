package multiraft

import (
	"fmt"

	"github.com/VenkatGGG/ChronosDb/internal/replica"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	raft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// MessageEnvelope couples an outbound Raft message with its local range id.
type MessageEnvelope struct {
	RangeID uint64
	Message raftpb.Message
}

// ProcessResult contains the side effects of one scheduler drain.
type ProcessResult struct {
	Messages   []MessageEnvelope
	ReadStates map[uint64][]raft.ReadState
}

// GroupConfig configures one range hosted by the scheduler.
type GroupConfig struct {
	RangeID       uint64
	ReplicaID     uint64
	Peers         []uint64
	ElectionTick  int
	HeartbeatTick int
}

type group struct {
	mem     *raft.MemoryStorage
	raw     *raft.RawNode
	replica *replica.StateMachine
}

// Scheduler hosts one or more RawNode groups for a single store.
type Scheduler struct {
	engine *storage.Engine
	groups map[uint64]*group
}

// NewScheduler constructs a shared local MultiRaft scheduler.
func NewScheduler(engine *storage.Engine) *Scheduler {
	return &Scheduler{
		engine: engine,
		groups: make(map[uint64]*group),
	}
}

// AddGroup adds one range group to the scheduler.
func (s *Scheduler) AddGroup(cfg GroupConfig) error {
	if cfg.RangeID == 0 {
		return fmt.Errorf("add group: range id must be non-zero")
	}
	if cfg.ReplicaID == 0 {
		return fmt.Errorf("add group: replica id must be non-zero")
	}
	if _, ok := s.groups[cfg.RangeID]; ok {
		return fmt.Errorf("add group: range %d already exists", cfg.RangeID)
	}
	electionTick := cfg.ElectionTick
	if electionTick == 0 {
		electionTick = 10
	}
	heartbeatTick := cfg.HeartbeatTick
	if heartbeatTick == 0 {
		heartbeatTick = 1
	}
	if electionTick <= heartbeatTick {
		return fmt.Errorf("add group: election tick must be greater than heartbeat tick")
	}

	replicaState, err := replica.OpenStateMachine(cfg.RangeID, cfg.ReplicaID, s.engine)
	if err != nil {
		return err
	}
	mem, err := s.engine.RehydrateRaftStorage(cfg.RangeID)
	if err != nil {
		return err
	}
	appliedIndex, err := s.engine.LoadRangeAppliedIndex(cfg.RangeID)
	if err != nil {
		return err
	}
	if err := restoreConfState(mem, appliedIndex, cfg.Peers); err != nil {
		return err
	}
	rawApplied := appliedIndex
	lastIndex, err := mem.LastIndex()
	if err != nil {
		return err
	}
	if lastIndex == 0 || rawApplied > lastIndex {
		rawApplied = 0
	}

	rawNode, err := raft.NewRawNode(&raft.Config{
		ID:                       cfg.ReplicaID,
		Storage:                  mem,
		Applied:                  rawApplied,
		ElectionTick:             electionTick,
		HeartbeatTick:            heartbeatTick,
		MaxSizePerMsg:            1 << 20,
		MaxCommittedSizePerReady: 1 << 20,
		MaxInflightMsgs:          256,
		CheckQuorum:              true,
		PreVote:                  true,
		ReadOnlyOption:           raft.ReadOnlySafe,
	})
	if err != nil {
		return err
	}
	if isFreshRange(mem) && len(cfg.Peers) > 0 {
		peers := make([]raft.Peer, 0, len(cfg.Peers))
		for _, peerID := range cfg.Peers {
			peers = append(peers, raft.Peer{ID: peerID})
		}
		if err := rawNode.Bootstrap(peers); err != nil {
			return err
		}
	}

	s.groups[cfg.RangeID] = &group{
		mem:     mem,
		raw:     rawNode,
		replica: replicaState,
	}
	return nil
}

// EnsureGroup makes sure a local RawNode exists for the range, creating it if needed.
func (s *Scheduler) EnsureGroup(cfg GroupConfig) error {
	if _, ok := s.groups[cfg.RangeID]; ok {
		return nil
	}
	return s.AddGroup(cfg)
}

// Tick advances all hosted ranges by one logical Raft tick.
func (s *Scheduler) Tick() {
	for _, group := range s.groups {
		group.raw.Tick()
	}
}

// Campaign asks one range to begin leader election.
func (s *Scheduler) Campaign(rangeID uint64) error {
	group, err := s.lookup(rangeID)
	if err != nil {
		return err
	}
	return group.raw.Campaign()
}

// Propose appends one application command to the range's Raft log.
func (s *Scheduler) Propose(rangeID uint64, data []byte) error {
	group, err := s.lookup(rangeID)
	if err != nil {
		return err
	}
	return group.raw.Propose(data)
}

// ProposeConfChange appends one membership change to the range's Raft log.
func (s *Scheduler) ProposeConfChange(rangeID uint64, cc raftpb.ConfChange) error {
	group, err := s.lookup(rangeID)
	if err != nil {
		return err
	}
	return group.raw.ProposeConfChange(cc)
}

// ReadIndex starts a linearizable read-index request on the range.
func (s *Scheduler) ReadIndex(rangeID uint64, requestCtx []byte) error {
	group, err := s.lookup(rangeID)
	if err != nil {
		return err
	}
	group.raw.ReadIndex(requestCtx)
	return nil
}

// TransferLeader asks the current leader to transfer leadership to transferee.
func (s *Scheduler) TransferLeader(rangeID, transferee uint64) error {
	group, err := s.lookup(rangeID)
	if err != nil {
		return err
	}
	group.raw.TransferLeader(transferee)
	return nil
}

// Step delivers one Raft message to the correct local group.
func (s *Scheduler) Step(rangeID uint64, msg raftpb.Message) error {
	group, err := s.lookup(rangeID)
	if err != nil {
		return err
	}
	return group.raw.Step(msg)
}

// Replica returns the local replica state machine for a range.
func (s *Scheduler) Replica(rangeID uint64) (*replica.StateMachine, error) {
	group, err := s.lookup(rangeID)
	if err != nil {
		return nil, err
	}
	return group.replica, nil
}

// Leader returns the scheduler's current known leader for a range.
func (s *Scheduler) Leader(rangeID uint64) (uint64, error) {
	group, err := s.lookup(rangeID)
	if err != nil {
		return 0, err
	}
	return group.raw.Status().Lead, nil
}

// ProcessReady drains all groups with pending Ready state and persists them in one batch.
func (s *Scheduler) ProcessReady() (ProcessResult, error) {
	result := ProcessResult{
		ReadStates: make(map[uint64][]raft.ReadState),
	}
	batch := s.engine.NewWriteBatch()
	defer batch.Close()

	type advanceItem struct {
		group       *group
		ready       raft.Ready
		delta       replica.ApplyDelta
		confChanges []raftpb.ConfChange
	}
	var advanceItems []advanceItem
	syncWrite := false

	for rangeID, group := range s.groups {
		if !group.raw.HasReady() {
			continue
		}
		ready := group.raw.Ready()
		if ready.MustSync {
			syncWrite = true
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			if err := group.mem.ApplySnapshot(ready.Snapshot); err != nil {
				return ProcessResult{}, err
			}
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			if err := group.mem.SetHardState(ready.HardState); err != nil {
				return ProcessResult{}, err
			}
			if err := batch.SetRaftHardState(rangeID, ready.HardState); err != nil {
				return ProcessResult{}, err
			}
		}
		if len(ready.Entries) > 0 {
			if err := group.mem.Append(ready.Entries); err != nil {
				return ProcessResult{}, err
			}
			if err := batch.AppendRaftEntries(rangeID, ready.Entries); err != nil {
				return ProcessResult{}, err
			}
		}

		var (
			delta       replica.ApplyDelta
			confChanges []raftpb.ConfChange
			err         error
		)
		if len(ready.CommittedEntries) > 0 {
			delta, confChanges, err = group.replica.StageEntriesWithConfChanges(batch, ready.CommittedEntries)
			if err != nil {
				return ProcessResult{}, err
			}
		}

		if len(ready.ReadStates) > 0 {
			states := make([]raft.ReadState, len(ready.ReadStates))
			copy(states, ready.ReadStates)
			result.ReadStates[rangeID] = append(result.ReadStates[rangeID], states...)
		}
		for _, message := range ready.Messages {
			result.Messages = append(result.Messages, MessageEnvelope{
				RangeID: rangeID,
				Message: message,
			})
		}

		advanceItems = append(advanceItems, advanceItem{
			group:       group,
			ready:       ready,
			delta:       delta,
			confChanges: confChanges,
		})
	}

	if err := batch.Commit(syncWrite); err != nil {
		return ProcessResult{}, err
	}
	for _, item := range advanceItems {
		for _, cc := range item.confChanges {
			item.group.raw.ApplyConfChange(cc)
		}
		item.group.replica.CommitApply(item.delta)
		item.group.raw.Advance(item.ready)
	}
	return result, nil
}

func (s *Scheduler) lookup(rangeID uint64) (*group, error) {
	group, ok := s.groups[rangeID]
	if !ok {
		return nil, fmt.Errorf("range %d not found", rangeID)
	}
	return group, nil
}

func isFreshRange(mem *raft.MemoryStorage) bool {
	lastIndex, err := mem.LastIndex()
	if err != nil {
		return false
	}
	return lastIndex == 0
}

func restoreConfState(mem *raft.MemoryStorage, appliedIndex uint64, peers []uint64) error {
	if len(peers) == 0 {
		return nil
	}
	_, confState, err := mem.InitialState()
	if err != nil {
		return err
	}
	if len(confState.Voters) > 0 || len(confState.Learners) > 0 {
		return nil
	}
	if appliedIndex == 0 {
		return nil
	}
	lastIndex, err := mem.LastIndex()
	if err != nil {
		return err
	}
	if lastIndex == 0 || appliedIndex > lastIndex {
		return nil
	}
	_, err = mem.CreateSnapshot(appliedIndex, &raftpb.ConfState{
		Voters: append([]uint64(nil), peers...),
	}, nil)
	return err
}
