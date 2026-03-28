package sql

import "fmt"

// FlowDistribution describes how a stage is placed across the cluster.
type FlowDistribution string

const (
	DistributionGatewayOnly     FlowDistribution = "gateway_only"
	DistributionLeaseholderOnly FlowDistribution = "leaseholder_only"
	DistributionByRange         FlowDistribution = "by_range"
)

// OperatorKind is the physical operator family scheduled inside one flow stage.
type OperatorKind string

const (
	OperatorKVScan     OperatorKind = "kv_scan"
	OperatorKVInsert   OperatorKind = "kv_insert"
	OperatorMerge      OperatorKind = "merge"
	OperatorHashJoin   OperatorKind = "hash_join"
	OperatorAggregate  OperatorKind = "aggregate"
	OperatorProjection OperatorKind = "projection"
)

// KeySpan is the KV span assigned to a scan or mutation processor.
type KeySpan struct {
	StartKey       []byte
	EndKey         []byte
	StartInclusive bool
	EndInclusive   bool
}

// AggregationSpec is the future aggregation contract for distributed SQL.
type AggregationSpec struct {
	GroupBy []string
	Funcs   []string
}

// JoinSpec is the future join contract for distributed SQL.
type JoinSpec struct {
	Type      string
	LeftKeys  []string
	RightKeys []string
}

// ProcessorSpec is one physical operator instance within a flow stage.
type ProcessorSpec struct {
	Kind        OperatorKind
	Table       *TableDescriptor
	Projection  []ColumnDescriptor
	Spans       []KeySpan
	Aggregation *AggregationSpec
	Join        *JoinSpec
}

// FlowStage is one placement boundary in the physical flow.
type FlowStage struct {
	ID               int
	Name             string
	Distribution     FlowDistribution
	PreferredRegions []string
	HomeRegion       string
	InputStageIDs    []int
	Processors       []ProcessorSpec
}

// FlowPlan is the distributed execution shape derived from a bound SQL plan.
type FlowPlan struct {
	RootStageID int
	Stages      []FlowStage
}

// FlowPlanner builds distributed SQL stage graphs from physical SQL plans.
type FlowPlanner struct{}

// NewFlowPlanner constructs a distributed flow planner.
func NewFlowPlanner() *FlowPlanner {
	return &FlowPlanner{}
}

// Build maps one SQL physical plan onto a distributed flow skeleton.
func (p *FlowPlanner) Build(plan Plan) (FlowPlan, error) {
	switch typed := plan.(type) {
	case PointLookupPlan:
		return p.buildPointLookup(typed), nil
	case RangeScanPlan:
		return p.buildRangeScan(typed), nil
	case InsertPlan:
		return p.buildInsert(typed), nil
	default:
		return FlowPlan{}, fmt.Errorf("sql flow planner: unsupported plan type %T", plan)
	}
}

func (p *FlowPlanner) buildPointLookup(plan PointLookupPlan) FlowPlan {
	return FlowPlan{
		RootStageID: 1,
		Stages: []FlowStage{
			{
				ID:               1,
				Name:             "point_lookup",
				Distribution:     DistributionLeaseholderOnly,
				PreferredRegions: leasePreferredRegions(plan.Table),
				HomeRegion:       homeRegion(plan.Table),
				Processors: []ProcessorSpec{
					{
						Kind:       OperatorKVScan,
						Table:      &plan.Table,
						Projection: append([]ColumnDescriptor(nil), plan.Projection...),
						Spans: []KeySpan{
							{
								StartKey:       append([]byte(nil), plan.Key...),
								EndKey:         append([]byte(nil), plan.Key...),
								StartInclusive: true,
								EndInclusive:   true,
							},
						},
					},
				},
			},
		},
	}
}

func (p *FlowPlanner) buildRangeScan(plan RangeScanPlan) FlowPlan {
	scanStage := FlowStage{
		ID:               1,
		Name:             "distributed_scan",
		Distribution:     DistributionByRange,
		PreferredRegions: scanPreferredRegions(plan.Table),
		HomeRegion:       homeRegion(plan.Table),
		Processors: []ProcessorSpec{
			{
				Kind:       OperatorKVScan,
				Table:      &plan.Table,
				Projection: append([]ColumnDescriptor(nil), plan.Projection...),
				Spans: []KeySpan{
					{
						StartKey:       append([]byte(nil), plan.StartKey...),
						EndKey:         append([]byte(nil), plan.EndKey...),
						StartInclusive: plan.StartInclusive,
						EndInclusive:   plan.EndInclusive,
					},
				},
			},
		},
	}
	mergeStage := FlowStage{
		ID:            2,
		Name:          "gateway_merge",
		Distribution:  DistributionGatewayOnly,
		InputStageIDs: []int{1},
		Processors: []ProcessorSpec{
			{
				Kind:       OperatorMerge,
				Table:      &plan.Table,
				Projection: append([]ColumnDescriptor(nil), plan.Projection...),
			},
			{
				Kind:       OperatorProjection,
				Table:      &plan.Table,
				Projection: append([]ColumnDescriptor(nil), plan.Projection...),
			},
		},
	}
	return FlowPlan{
		RootStageID: mergeStage.ID,
		Stages:      []FlowStage{scanStage, mergeStage},
	}
}

func (p *FlowPlanner) buildInsert(plan InsertPlan) FlowPlan {
	return FlowPlan{
		RootStageID: 1,
		Stages: []FlowStage{
			{
				ID:               1,
				Name:             "insert_put",
				Distribution:     DistributionLeaseholderOnly,
				PreferredRegions: leasePreferredRegions(plan.Table),
				HomeRegion:       homeRegion(plan.Table),
				Processors: []ProcessorSpec{
					{
						Kind:  OperatorKVInsert,
						Table: &plan.Table,
						Spans: []KeySpan{
							{
								StartKey:       append([]byte(nil), plan.Key...),
								EndKey:         append([]byte(nil), plan.Key...),
								StartInclusive: true,
								EndInclusive:   true,
							},
						},
					},
				},
			},
		},
	}
}

func leasePreferredRegions(table TableDescriptor) []string {
	compiled, ok, err := table.CompiledPlacement()
	if !ok || err != nil {
		return nil
	}
	return append([]string(nil), compiled.LeasePreferences...)
}

func scanPreferredRegions(table TableDescriptor) []string {
	compiled, ok, err := table.CompiledPlacement()
	if !ok || err != nil {
		return nil
	}
	return append([]string(nil), compiled.PreferredRegions...)
}

func homeRegion(table TableDescriptor) string {
	if table.PlacementPolicy == nil {
		return ""
	}
	return table.PlacementPolicy.HomeRegion
}
