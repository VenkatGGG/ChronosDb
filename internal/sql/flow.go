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

// AggregateMode identifies whether an aggregate stage is partial or final.
type AggregateMode string

const (
	AggregateModePartial AggregateMode = "partial"
	AggregateModeFinal   AggregateMode = "final"
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
	Mode    AggregateMode
	GroupBy []string
	Funcs   []string
}

// JoinSpec is the future join contract for distributed SQL.
type JoinSpec struct {
	Type      string
	LeftKeys  []string
	RightKeys []string
}

// ResultColumn is one typed output column produced by a stage, fragment, or plan.
type ResultColumn struct {
	Name     string
	Type     ColumnType
	Nullable bool
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
	FragmentID       int
	Name             string
	Distribution     FlowDistribution
	PreferredRegions []string
	HomeRegion       string
	InputStageIDs    []int
	Processors       []ProcessorSpec
	ResultSchema     []ResultColumn
}

// FlowFragment is one explicit execution fragment boundary.
type FlowFragment struct {
	ID               int
	Name             string
	Distribution     FlowDistribution
	PreferredRegions []string
	HomeRegion       string
	StageIDs         []int
	ResultSchema     []ResultColumn
}

// FlowPlan is the distributed execution shape derived from a bound SQL plan.
type FlowPlan struct {
	RootStageID    int
	RootFragmentID int
	Stages         []FlowStage
	Fragments      []FlowFragment
	ResultSchema   []ResultColumn
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
	case AggregatePlan:
		return p.buildAggregate(typed), nil
	case HashJoinPlan:
		return p.buildHashJoin(typed), nil
	default:
		return FlowPlan{}, fmt.Errorf("sql flow planner: unsupported plan type %T", plan)
	}
}

func (p *FlowPlanner) buildPointLookup(plan PointLookupPlan) FlowPlan {
	resultSchema := schemaFromColumns(plan.Projection)
	return assembleFlowPlan(1, []FlowStage{
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
			ResultSchema: resultSchema,
		},
	}, resultSchema)
}

func (p *FlowPlanner) buildRangeScan(plan RangeScanPlan) FlowPlan {
	resultSchema := schemaFromColumns(plan.Projection)
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
		ResultSchema: resultSchema,
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
		ResultSchema: resultSchema,
	}
	return assembleFlowPlan(mergeStage.ID, []FlowStage{scanStage, mergeStage}, resultSchema)
}

func (p *FlowPlanner) buildInsert(plan InsertPlan) FlowPlan {
	return assembleFlowPlan(1, []FlowStage{
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
	}, nil)
}

func (p *FlowPlanner) buildAggregate(plan AggregatePlan) FlowPlan {
	resultSchema := schemaFromAggregate(plan)
	scanStage := FlowStage{
		ID:               1,
		Name:             "distributed_partial_aggregate",
		Distribution:     DistributionByRange,
		PreferredRegions: scanPreferredRegions(plan.Input.Table),
		HomeRegion:       homeRegion(plan.Input.Table),
		Processors: []ProcessorSpec{
			{
				Kind:       OperatorKVScan,
				Table:      &plan.Input.Table,
				Projection: append([]ColumnDescriptor(nil), plan.Input.Projection...),
				Spans: []KeySpan{
					{
						StartKey:       append([]byte(nil), plan.Input.StartKey...),
						EndKey:         append([]byte(nil), plan.Input.EndKey...),
						StartInclusive: plan.Input.StartInclusive,
						EndInclusive:   plan.Input.EndInclusive,
					},
				},
			},
			{
				Kind:  OperatorAggregate,
				Table: &plan.Input.Table,
				Aggregation: &AggregationSpec{
					Mode:    AggregateModePartial,
					GroupBy: aggregateColumnNames(plan.GroupBy),
					Funcs:   aggregateFuncNames(plan.Aggregates),
				},
			},
		},
		ResultSchema: resultSchema,
	}
	finalStage := FlowStage{
		ID:            2,
		Name:          "gateway_finalize_aggregate",
		Distribution:  DistributionGatewayOnly,
		InputStageIDs: []int{1},
		Processors: []ProcessorSpec{
			{
				Kind:  OperatorMerge,
				Table: &plan.Input.Table,
			},
			{
				Kind:  OperatorAggregate,
				Table: &plan.Input.Table,
				Aggregation: &AggregationSpec{
					Mode:    AggregateModeFinal,
					GroupBy: aggregateColumnNames(plan.GroupBy),
					Funcs:   aggregateFuncNames(plan.Aggregates),
				},
			},
		},
		ResultSchema: resultSchema,
	}
	return assembleFlowPlan(finalStage.ID, []FlowStage{scanStage, finalStage}, resultSchema)
}

func (p *FlowPlanner) buildHashJoin(plan HashJoinPlan) FlowPlan {
	leftSchema := schemaFromColumns(plan.LeftScan.Projection)
	rightSchema := schemaFromColumns(plan.RightScan.Projection)
	resultSchema := schemaFromJoinProjection(plan.Projection)
	leftStage := FlowStage{
		ID:               1,
		Name:             "left_join_scan",
		Distribution:     DistributionByRange,
		PreferredRegions: scanPreferredRegions(plan.Left.Table),
		HomeRegion:       homeRegion(plan.Left.Table),
		Processors: []ProcessorSpec{
			{
				Kind:       OperatorKVScan,
				Table:      &plan.Left.Table,
				Projection: append([]ColumnDescriptor(nil), plan.LeftScan.Projection...),
				Spans: []KeySpan{
					{
						StartKey:       append([]byte(nil), plan.LeftScan.StartKey...),
						EndKey:         append([]byte(nil), plan.LeftScan.EndKey...),
						StartInclusive: plan.LeftScan.StartInclusive,
						EndInclusive:   plan.LeftScan.EndInclusive,
					},
				},
			},
		},
		ResultSchema: leftSchema,
	}
	rightStage := FlowStage{
		ID:               2,
		Name:             "right_join_scan",
		Distribution:     DistributionByRange,
		PreferredRegions: scanPreferredRegions(plan.Right.Table),
		HomeRegion:       homeRegion(plan.Right.Table),
		Processors: []ProcessorSpec{
			{
				Kind:       OperatorKVScan,
				Table:      &plan.Right.Table,
				Projection: append([]ColumnDescriptor(nil), plan.RightScan.Projection...),
				Spans: []KeySpan{
					{
						StartKey:       append([]byte(nil), plan.RightScan.StartKey...),
						EndKey:         append([]byte(nil), plan.RightScan.EndKey...),
						StartInclusive: plan.RightScan.StartInclusive,
						EndInclusive:   plan.RightScan.EndInclusive,
					},
				},
			},
		},
		ResultSchema: rightSchema,
	}
	joinStage := FlowStage{
		ID:            3,
		Name:          "gateway_hash_join",
		Distribution:  DistributionGatewayOnly,
		InputStageIDs: []int{1, 2},
		Processors: []ProcessorSpec{
			{
				Kind: OperatorMerge,
			},
			{
				Kind: OperatorMerge,
			},
			{
				Kind: OperatorHashJoin,
				Join: &JoinSpec{
					Type:      plan.Join.Type,
					LeftKeys:  append([]string(nil), plan.Join.LeftKeys...),
					RightKeys: append([]string(nil), plan.Join.RightKeys...),
				},
				Projection: joinProjectionColumns(plan.Projection),
			},
		},
		ResultSchema: resultSchema,
	}
	return assembleFlowPlan(joinStage.ID, []FlowStage{leftStage, rightStage, joinStage}, resultSchema)
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

func aggregateColumnNames(columns []ColumnDescriptor) []string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return names
}

func aggregateFuncNames(aggregates []AggregateExpr) []string {
	names := make([]string, 0, len(aggregates))
	for _, aggregate := range aggregates {
		names = append(names, aggregate.String())
	}
	return names
}

func joinProjectionColumns(projection []JoinProjection) []ColumnDescriptor {
	columns := make([]ColumnDescriptor, 0, len(projection))
	for _, item := range projection {
		columns = append(columns, item.Output)
	}
	return columns
}

func assembleFlowPlan(rootStageID int, stages []FlowStage, resultSchema []ResultColumn) FlowPlan {
	fragments := make([]FlowFragment, 0, len(stages))
	for i := range stages {
		stages[i].FragmentID = stages[i].ID
		fragments = append(fragments, FlowFragment{
			ID:               stages[i].FragmentID,
			Name:             stages[i].Name + "_fragment",
			Distribution:     stages[i].Distribution,
			PreferredRegions: append([]string(nil), stages[i].PreferredRegions...),
			HomeRegion:       stages[i].HomeRegion,
			StageIDs:         []int{stages[i].ID},
			ResultSchema:     copyResultSchema(stages[i].ResultSchema),
		})
	}
	return FlowPlan{
		RootStageID:    rootStageID,
		RootFragmentID: rootStageID,
		Stages:         stages,
		Fragments:      fragments,
		ResultSchema:   copyResultSchema(resultSchema),
	}
}

func schemaFromColumns(columns []ColumnDescriptor) []ResultColumn {
	schema := make([]ResultColumn, 0, len(columns))
	for _, column := range columns {
		schema = append(schema, ResultColumn{
			Name:     column.Name,
			Type:     column.Type,
			Nullable: column.Nullable,
		})
	}
	return schema
}

func schemaFromAggregate(plan AggregatePlan) []ResultColumn {
	schema := make([]ResultColumn, 0, len(plan.GroupBy)+len(plan.Aggregates))
	for _, column := range plan.GroupBy {
		schema = append(schema, ResultColumn{
			Name:     column.Name,
			Type:     column.Type,
			Nullable: column.Nullable,
		})
	}
	for _, aggregate := range plan.Aggregates {
		name := aggregate.Alias
		if name == "" {
			name = aggregate.String()
		}
		nullable := false
		if aggregate.Func == AggregateFuncSum {
			nullable = true
		}
		schema = append(schema, ResultColumn{
			Name:     name,
			Type:     ColumnTypeInt,
			Nullable: nullable,
		})
	}
	return schema
}

func schemaFromJoinProjection(projection []JoinProjection) []ResultColumn {
	schema := make([]ResultColumn, 0, len(projection))
	for _, item := range projection {
		schema = append(schema, ResultColumn{
			Name:     item.Output.Name,
			Type:     item.Output.Type,
			Nullable: item.Output.Nullable,
		})
	}
	return schema
}

func copyResultSchema(schema []ResultColumn) []ResultColumn {
	if len(schema) == 0 {
		return nil
	}
	return append([]ResultColumn(nil), schema...)
}
