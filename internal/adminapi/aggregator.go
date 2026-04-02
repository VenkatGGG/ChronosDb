package adminapi

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrKeyNotLocated reports that no merged range descriptor spans the requested key.
var ErrKeyNotLocated = errors.New("key not located")

// ErrNodeNotFound reports that the requested node is absent from the merged snapshot.
var ErrNodeNotFound = errors.New("node not found")

// ErrRangeNotFound reports that the requested range is absent from the merged snapshot.
var ErrRangeNotFound = errors.New("range not found")

// ErrScenarioRunNotFound reports that the requested retained scenario run is absent.
var ErrScenarioRunNotFound = errors.New("scenario run not found")

// NodeTarget identifies one node admin endpoint for snapshot polling.
type NodeTarget struct {
	NodeID  uint64
	BaseURL string
}

// AggregatorConfig configures cluster snapshot polling.
type AggregatorConfig struct {
	Targets           []NodeTarget
	Client            *http.Client
	EventLimitPerNode int
	Now               func() time.Time
}

// Aggregator polls node-level admin snapshots and merges them into one cluster view.
type Aggregator struct {
	targets           []NodeTarget
	client            *http.Client
	eventLimitPerNode int
	now               func() time.Time
}

// NewAggregator constructs a cluster snapshot aggregator.
func NewAggregator(cfg AggregatorConfig) (*Aggregator, error) {
	if len(cfg.Targets) == 0 {
		return nil, fmt.Errorf("adminapi: at least one node target is required")
	}
	targets := make([]NodeTarget, 0, len(cfg.Targets))
	seen := make(map[uint64]struct{}, len(cfg.Targets))
	for _, target := range cfg.Targets {
		target.BaseURL = strings.TrimRight(target.BaseURL, "/")
		if target.BaseURL == "" {
			return nil, fmt.Errorf("adminapi: node target base url must not be empty")
		}
		parsed, err := url.Parse(target.BaseURL)
		if err != nil {
			return nil, fmt.Errorf("adminapi: invalid base url %q: %w", target.BaseURL, err)
		}
		if parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("adminapi: base url %q must include scheme and host", target.BaseURL)
		}
		if target.NodeID != 0 {
			if _, ok := seen[target.NodeID]; ok {
				return nil, fmt.Errorf("adminapi: duplicate node target %d", target.NodeID)
			}
			seen[target.NodeID] = struct{}{}
		}
		targets = append(targets, target)
	}
	client := cfg.Client
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Second}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now().UTC
	}
	return &Aggregator{
		targets:           targets,
		client:            client,
		eventLimitPerNode: cfg.EventLimitPerNode,
		now:               now,
	}, nil
}

// Snapshot returns a merged cluster snapshot across all configured node targets.
func (a *Aggregator) Snapshot(ctx context.Context) (ClusterSnapshot, error) {
	type result struct {
		snapshot ClusterSnapshot
		err      error
	}

	results := make(chan result, len(a.targets))
	var wg sync.WaitGroup
	for _, target := range a.targets {
		target := target
		wg.Add(1)
		go func() {
			defer wg.Done()
			snapshot, err := a.fetchSnapshot(ctx, target)
			results <- result{snapshot: snapshot, err: err}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	merged := ClusterSnapshot{
		GeneratedAt: a.now(),
		Nodes:       make([]NodeView, 0, len(a.targets)),
		Ranges:      make([]RangeView, 0),
		Events:      make([]ClusterEvent, 0),
	}
	rangeIndex := make(map[uint64]int)
	for result := range results {
		if result.err != nil {
			return ClusterSnapshot{}, result.err
		}
		merged.Nodes = append(merged.Nodes, result.snapshot.Nodes...)
		for _, view := range result.snapshot.Ranges {
			a.mergeRangeView(&merged, rangeIndex, view)
		}
		for _, event := range result.snapshot.Events {
			merged.Events = append(merged.Events, NormalizeEvent(event))
		}
	}
	sort.Slice(merged.Nodes, func(i, j int) bool {
		return merged.Nodes[i].NodeID < merged.Nodes[j].NodeID
	})
	sort.Slice(merged.Ranges, func(i, j int) bool {
		return merged.Ranges[i].RangeID < merged.Ranges[j].RangeID
	})
	sort.SliceStable(merged.Events, func(i, j int) bool {
		return merged.Events[i].Timestamp.Before(merged.Events[j].Timestamp)
	})
	return merged, nil
}

// LocateKey resolves a logical key to the merged containing range descriptor.
func (a *Aggregator) LocateKey(ctx context.Context, raw string) (KeyLocationView, error) {
	key, encoding, err := parseLookupKey(raw)
	if err != nil {
		return KeyLocationView{}, err
	}
	snapshot, err := a.Snapshot(ctx)
	if err != nil {
		return KeyLocationView{}, err
	}
	for _, view := range snapshot.Ranges {
		match, err := rangeContainsKey(view, key)
		if err != nil {
			return KeyLocationView{}, err
		}
		if match {
			return KeyLocationView{
				Key:      hex.EncodeToString(key),
				Encoding: encoding,
				Range:    view,
			}, nil
		}
	}
	return KeyLocationView{}, ErrKeyNotLocated
}

// Topology returns a graph-friendly placement view derived from the merged snapshot.
func (a *Aggregator) Topology(ctx context.Context) (ClusterTopologyView, error) {
	snapshot, err := a.Snapshot(ctx)
	if err != nil {
		return ClusterTopologyView{}, err
	}
	edges := make([]TopologyEdgeView, 0)
	for _, view := range snapshot.Ranges {
		for _, replica := range view.Replicas {
			edges = append(edges, TopologyEdgeView{
				NodeID:      replica.NodeID,
				RangeID:     view.RangeID,
				ReplicaID:   replica.ReplicaID,
				Role:        replica.Role,
				Leaseholder: replica.ReplicaID == view.LeaseholderReplicaID,
			})
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].RangeID != edges[j].RangeID {
			return edges[i].RangeID < edges[j].RangeID
		}
		return edges[i].ReplicaID < edges[j].ReplicaID
	})
	return ClusterTopologyView{
		GeneratedAt: snapshot.GeneratedAt,
		Nodes:       append([]NodeView(nil), snapshot.Nodes...),
		Ranges:      cloneRangeViews(snapshot.Ranges),
		Edges:       edges,
	}, nil
}

// NodeDetail returns the authoritative drilldown view for one node.
func (a *Aggregator) NodeDetail(ctx context.Context, nodeID uint64) (NodeDetailView, error) {
	snapshot, err := a.Snapshot(ctx)
	if err != nil {
		return NodeDetailView{}, err
	}
	node, ok := findNode(snapshot.Nodes, nodeID)
	if !ok {
		return NodeDetailView{}, ErrNodeNotFound
	}
	hosted := make([]NodeHostedRangeView, 0)
	hostedRangeIDs := make(map[uint64]struct{})
	for _, view := range snapshot.Ranges {
		for _, replica := range view.Replicas {
			if replica.NodeID != nodeID {
				continue
			}
			hosted = append(hosted, NodeHostedRangeView{
				RangeID:       view.RangeID,
				Generation:    view.Generation,
				StartKey:      view.StartKey,
				EndKey:        view.EndKey,
				ReplicaID:     replica.ReplicaID,
				ReplicaRole:   replica.Role,
				Leaseholder:   replica.ReplicaID == view.LeaseholderReplicaID,
				PlacementMode: view.PlacementMode,
			})
			hostedRangeIDs[view.RangeID] = struct{}{}
		}
	}
	sort.Slice(hosted, func(i, j int) bool {
		if hosted[i].RangeID != hosted[j].RangeID {
			return hosted[i].RangeID < hosted[j].RangeID
		}
		return hosted[i].ReplicaID < hosted[j].ReplicaID
	})
	events := correlateEvents(snapshot.Events, func(event ClusterEvent) bool {
		if event.NodeID == nodeID {
			return true
		}
		if event.RangeID == 0 {
			return false
		}
		_, ok := hostedRangeIDs[event.RangeID]
		return ok
	})
	return NodeDetailView{
		Node:         node,
		HostedRanges: hosted,
		RecentEvents: events,
	}, nil
}

// RangeDetail returns the authoritative drilldown view for one range.
func (a *Aggregator) RangeDetail(ctx context.Context, rangeID uint64) (RangeDetailView, error) {
	snapshot, err := a.Snapshot(ctx)
	if err != nil {
		return RangeDetailView{}, err
	}
	nodeIndex := make(map[uint64]NodeView, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodeIndex[node.NodeID] = node
	}
	for _, view := range snapshot.Ranges {
		if view.RangeID != rangeID {
			continue
		}
		replicaNodes := make([]RangeReplicaNodeView, 0, len(view.Replicas))
		for _, replica := range view.Replicas {
			replicaNode := RangeReplicaNodeView{
				Replica:     replica,
				Leaseholder: replica.ReplicaID == view.LeaseholderReplicaID,
			}
			if node, ok := nodeIndex[replica.NodeID]; ok {
				copyNode := node
				replicaNode.Node = &copyNode
			}
			replicaNodes = append(replicaNodes, replicaNode)
		}
		sort.Slice(replicaNodes, func(i, j int) bool {
			return replicaNodes[i].Replica.ReplicaID < replicaNodes[j].Replica.ReplicaID
		})
		return RangeDetailView{
			Range:        cloneRangeView(view),
			ReplicaNodes: replicaNodes,
			RecentEvents: correlateEvents(snapshot.Events, func(event ClusterEvent) bool {
				return event.RangeID == rangeID
			}),
		}, nil
	}
	return RangeDetailView{}, ErrRangeNotFound
}

// CorrelateScenarioDetail projects one retained scenario onto the current live topology.
func (a *Aggregator) CorrelateScenarioDetail(ctx context.Context, detail ScenarioRunDetail) (ScenarioLiveCorrelation, error) {
	snapshot, err := a.Snapshot(ctx)
	if err != nil {
		return ScenarioLiveCorrelation{}, err
	}
	nodeIndex := make(map[uint64]NodeView, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodeIndex[node.NodeID] = node
	}
	nodes := make([]NodeView, 0, len(detail.Manifest.Nodes))
	missing := make([]uint64, 0)
	relatedNodeSet := make(map[uint64]struct{}, len(detail.Manifest.Nodes))
	for _, nodeID := range detail.Manifest.Nodes {
		if node, ok := nodeIndex[nodeID]; ok {
			nodes = append(nodes, node)
			relatedNodeSet[nodeID] = struct{}{}
			continue
		}
		missing = append(missing, nodeID)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})
	sort.Slice(missing, func(i, j int) bool {
		return missing[i] < missing[j]
	})
	ranges := make([]RangeView, 0)
	seenRanges := make(map[uint64]struct{})
	for _, view := range snapshot.Ranges {
		if rangeTouchesNodes(view, relatedNodeSet) {
			if _, ok := seenRanges[view.RangeID]; ok {
				continue
			}
			seenRanges[view.RangeID] = struct{}{}
			ranges = append(ranges, cloneRangeView(view))
		}
	}
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].RangeID < ranges[j].RangeID
	})
	return ScenarioLiveCorrelation{
		GeneratedAt:    snapshot.GeneratedAt,
		Source:         "manifest_nodes_current_topology",
		Nodes:          nodes,
		Ranges:         ranges,
		MissingNodeIDs: missing,
	}, nil
}

func (a *Aggregator) fetchSnapshot(ctx context.Context, target NodeTarget) (ClusterSnapshot, error) {
	path := target.BaseURL + "/admin/snapshot"
	if a.eventLimitPerNode > 0 {
		path += fmt.Sprintf("?event_limit=%d", a.eventLimitPerNode)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return ClusterSnapshot{}, err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return ClusterSnapshot{}, fmt.Errorf("adminapi: fetch snapshot from %s: %w", target.BaseURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ClusterSnapshot{}, fmt.Errorf("adminapi: fetch snapshot from %s: status %d", target.BaseURL, resp.StatusCode)
	}
	var snapshot ClusterSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		return ClusterSnapshot{}, fmt.Errorf("adminapi: decode snapshot from %s: %w", target.BaseURL, err)
	}
	if len(snapshot.Nodes) != 1 {
		return ClusterSnapshot{}, fmt.Errorf("adminapi: snapshot from %s returned %d nodes, want 1", target.BaseURL, len(snapshot.Nodes))
	}
	if target.NodeID != 0 && snapshot.Nodes[0].NodeID != target.NodeID {
		return ClusterSnapshot{}, fmt.Errorf("adminapi: snapshot from %s reported node %d, want %d", target.BaseURL, snapshot.Nodes[0].NodeID, target.NodeID)
	}
	return snapshot, nil
}

func (a *Aggregator) mergeRangeView(snapshot *ClusterSnapshot, index map[uint64]int, incoming RangeView) {
	pos, ok := index[incoming.RangeID]
	if !ok {
		snapshot.Ranges = append(snapshot.Ranges, cloneRangeView(incoming))
		index[incoming.RangeID] = len(snapshot.Ranges) - 1
		return
	}
	current := &snapshot.Ranges[pos]
	if incoming.Generation > current.Generation {
		replacements := mergeReplicaViews(nil, incoming.Replicas)
		*current = cloneRangeView(incoming)
		current.Replicas = replacements
		return
	}
	if incoming.Generation < current.Generation {
		return
	}
	current.Replicas = mergeReplicaViews(current.Replicas, incoming.Replicas)
	if current.LeaseholderReplicaID == 0 && incoming.LeaseholderReplicaID != 0 {
		current.LeaseholderReplicaID = incoming.LeaseholderReplicaID
		current.LeaseholderNodeID = incoming.LeaseholderNodeID
	}
	if current.Source == "" {
		current.Source = incoming.Source
	}
	if current.PlacementMode == "" && incoming.PlacementMode != "" {
		current.PlacementMode = incoming.PlacementMode
		current.PreferredRegions = append([]string(nil), incoming.PreferredRegions...)
		current.LeasePreferences = append([]string(nil), incoming.LeasePreferences...)
	}
}

func mergeReplicaViews(current, incoming []ReplicaView) []ReplicaView {
	out := append([]ReplicaView(nil), current...)
	index := make(map[uint64]int, len(out))
	for i, replica := range out {
		index[replica.ReplicaID] = i
	}
	for _, replica := range incoming {
		if pos, ok := index[replica.ReplicaID]; ok {
			out[pos] = replica
			continue
		}
		index[replica.ReplicaID] = len(out)
		out = append(out, replica)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ReplicaID < out[j].ReplicaID
	})
	return out
}

func cloneRangeView(view RangeView) RangeView {
	copyView := view
	copyView.Replicas = append([]ReplicaView(nil), view.Replicas...)
	copyView.PreferredRegions = append([]string(nil), view.PreferredRegions...)
	copyView.LeasePreferences = append([]string(nil), view.LeasePreferences...)
	return copyView
}

func cloneRangeViews(views []RangeView) []RangeView {
	out := make([]RangeView, 0, len(views))
	for _, view := range views {
		out = append(out, cloneRangeView(view))
	}
	return out
}

func findNode(nodes []NodeView, nodeID uint64) (NodeView, bool) {
	for _, node := range nodes {
		if node.NodeID == nodeID {
			return node, true
		}
	}
	return NodeView{}, false
}

func correlateEvents(events []ClusterEvent, match func(ClusterEvent) bool) []ClusterEvent {
	correlated := make([]ClusterEvent, 0)
	seen := make(map[string]struct{})
	for _, event := range events {
		if !match(event) {
			continue
		}
		event = NormalizeEvent(event)
		if _, ok := seen[event.ID]; ok {
			continue
		}
		seen[event.ID] = struct{}{}
		correlated = append(correlated, event)
	}
	sort.SliceStable(correlated, func(i, j int) bool {
		return correlated[i].Timestamp.After(correlated[j].Timestamp)
	})
	return correlated
}

func rangeTouchesNodes(view RangeView, nodeIDs map[uint64]struct{}) bool {
	if len(nodeIDs) == 0 {
		return false
	}
	for _, replica := range view.Replicas {
		if _, ok := nodeIDs[replica.NodeID]; ok {
			return true
		}
	}
	return false
}

func parseLookupKey(raw string) ([]byte, string, error) {
	if raw == "" {
		return nil, "", fmt.Errorf("adminapi: lookup key must not be empty")
	}
	if strings.HasPrefix(raw, "hex:") {
		key, err := hex.DecodeString(strings.TrimPrefix(raw, "hex:"))
		if err != nil {
			return nil, "", fmt.Errorf("adminapi: invalid hex lookup key: %w", err)
		}
		return key, "hex", nil
	}
	return bytes.Clone([]byte(raw)), "utf8", nil
}
