package sql

import "testing"

func TestFlowPlannerBuildPointLookup(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select id, name from users where id = 7")
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}

	flow, err := NewFlowPlanner().Build(plan)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if flow.RootStageID != 1 || flow.RootFragmentID != 1 || len(flow.Stages) != 1 {
		t.Fatalf("unexpected point-lookup flow shape: %+v", flow)
	}
	if len(flow.Fragments) != 1 || flow.Fragments[0].ID != 1 {
		t.Fatalf("unexpected point-lookup fragments: %+v", flow.Fragments)
	}
	stage := flow.Stages[0]
	if stage.Distribution != DistributionLeaseholderOnly {
		t.Fatalf("distribution = %q, want %q", stage.Distribution, DistributionLeaseholderOnly)
	}
	if stage.FragmentID != 1 {
		t.Fatalf("fragment id = %d, want 1", stage.FragmentID)
	}
	if stage.HomeRegion != "us-east1" {
		t.Fatalf("home region = %q, want us-east1", stage.HomeRegion)
	}
	if len(stage.PreferredRegions) != 1 || stage.PreferredRegions[0] != "us-east1" {
		t.Fatalf("preferred regions = %+v, want [us-east1]", stage.PreferredRegions)
	}
	if len(stage.Processors) != 1 || stage.Processors[0].Kind != OperatorKVScan {
		t.Fatalf("unexpected processor shape: %+v", stage.Processors)
	}
	if len(flow.ResultSchema) != 2 || flow.ResultSchema[0].Name != "id" || flow.ResultSchema[1].Name != "name" {
		t.Fatalf("unexpected point-lookup result schema: %+v", flow.ResultSchema)
	}
}

func TestFlowPlannerBuildRangeScan(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select * from users where id >= 10 and id < 20")
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}

	flow, err := NewFlowPlanner().Build(plan)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if flow.RootStageID != 2 || flow.RootFragmentID != 2 || len(flow.Stages) != 2 {
		t.Fatalf("unexpected range-scan flow shape: %+v", flow)
	}
	if len(flow.Fragments) != 2 {
		t.Fatalf("range-scan fragments = %+v, want 2 fragments", flow.Fragments)
	}
	if flow.Stages[0].Distribution != DistributionByRange {
		t.Fatalf("scan distribution = %q, want %q", flow.Stages[0].Distribution, DistributionByRange)
	}
	if len(flow.Stages[0].PreferredRegions) != 3 {
		t.Fatalf("scan preferred regions = %+v, want three preferred regions", flow.Stages[0].PreferredRegions)
	}
	if flow.Stages[0].HomeRegion != "us-east1" {
		t.Fatalf("scan home region = %q, want us-east1", flow.Stages[0].HomeRegion)
	}
	if flow.Stages[1].Processors[0].Kind != OperatorMerge {
		t.Fatalf("merge operator kind = %q, want %q", flow.Stages[1].Processors[0].Kind, OperatorMerge)
	}
	if flow.Stages[1].Processors[1].Kind != OperatorProjection {
		t.Fatalf("projection operator kind = %q, want %q", flow.Stages[1].Processors[1].Kind, OperatorProjection)
	}
	if len(flow.ResultSchema) != 3 {
		t.Fatalf("range-scan result schema = %+v, want 3 columns", flow.ResultSchema)
	}
}

func TestFlowPlannerBuildInsert(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("insert into users (id, name, email) values (1, 'alice', 'a@example.com')")
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}

	flow, err := NewFlowPlanner().Build(plan)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if flow.RootStageID != 1 || flow.RootFragmentID != 1 || len(flow.Stages) != 1 {
		t.Fatalf("unexpected insert flow shape: %+v", flow)
	}
	if flow.Stages[0].HomeRegion != "us-east1" {
		t.Fatalf("insert home region = %q, want us-east1", flow.Stages[0].HomeRegion)
	}
	if flow.Stages[0].Processors[0].Kind != OperatorKVInsert {
		t.Fatalf("insert operator kind = %q, want %q", flow.Stages[0].Processors[0].Kind, OperatorKVInsert)
	}
	if len(flow.ResultSchema) != 0 {
		t.Fatalf("insert result schema = %+v, want empty", flow.ResultSchema)
	}
}

func TestFlowPlannerBuildUpsert(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("upsert into users (id, name, email) values (1, 'alice', 'a@example.com')")
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}

	flow, err := NewFlowPlanner().Build(plan)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if flow.RootStageID != 1 || flow.RootFragmentID != 1 || len(flow.Stages) != 1 {
		t.Fatalf("unexpected upsert flow shape: %+v", flow)
	}
	if flow.Stages[0].Processors[0].Kind != OperatorKVUpsert {
		t.Fatalf("upsert operator kind = %q, want %q", flow.Stages[0].Processors[0].Kind, OperatorKVUpsert)
	}
	if len(flow.ResultSchema) != 0 {
		t.Fatalf("upsert result schema = %+v, want empty", flow.ResultSchema)
	}
}

func TestFlowPlannerBuildDeleteReturning(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("delete from users where id = 7 returning id, name")
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}

	flow, err := NewFlowPlanner().Build(plan)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if len(flow.ResultSchema) != 2 || flow.ResultSchema[0].Name != "id" || flow.ResultSchema[1].Name != "name" {
		t.Fatalf("delete returning schema = %+v, want [id name]", flow.ResultSchema)
	}
}

func TestFlowPlannerBuildUpdateReturning(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("update users set name = 'ally' where id = 7 returning id, name")
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}

	flow, err := NewFlowPlanner().Build(plan)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if len(flow.ResultSchema) != 2 || flow.ResultSchema[0].Name != "id" || flow.ResultSchema[1].Name != "name" {
		t.Fatalf("update returning schema = %+v, want [id name]", flow.ResultSchema)
	}
}

func TestFlowPlannerBuildDelete(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("delete from users where id >= 10 and id < 20")
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}

	flow, err := NewFlowPlanner().Build(plan)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if flow.RootStageID != 1 || flow.RootFragmentID != 1 || len(flow.Stages) != 1 {
		t.Fatalf("unexpected delete flow shape: %+v", flow)
	}
	stage := flow.Stages[0]
	if stage.Distribution != DistributionByRange {
		t.Fatalf("delete distribution = %q, want %q", stage.Distribution, DistributionByRange)
	}
	if len(stage.Processors) != 1 || stage.Processors[0].Kind != OperatorKVDelete {
		t.Fatalf("delete processor shape = %+v, want one kv_delete", stage.Processors)
	}
	if len(flow.ResultSchema) != 0 {
		t.Fatalf("delete result schema = %+v, want empty", flow.ResultSchema)
	}
}

func TestFlowPlannerBuildUpdate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("update users set name = 'ally' where id >= 10 and id < 20")
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}

	flow, err := NewFlowPlanner().Build(plan)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if flow.RootStageID != 1 || flow.RootFragmentID != 1 || len(flow.Stages) != 1 {
		t.Fatalf("unexpected update flow shape: %+v", flow)
	}
	stage := flow.Stages[0]
	if stage.Distribution != DistributionByRange {
		t.Fatalf("update distribution = %q, want %q", stage.Distribution, DistributionByRange)
	}
	if len(stage.Processors) != 1 || stage.Processors[0].Kind != OperatorKVUpdate {
		t.Fatalf("update processor shape = %+v, want one kv_update", stage.Processors)
	}
	if len(flow.ResultSchema) != 0 {
		t.Fatalf("update result schema = %+v, want empty", flow.ResultSchema)
	}
}

func TestFlowPlannerBuildAggregate(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select name, count(*) from users group by name")
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}

	flow, err := NewFlowPlanner().Build(plan)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if flow.RootStageID != 2 || flow.RootFragmentID != 2 || len(flow.Stages) != 2 {
		t.Fatalf("unexpected aggregate flow shape: %+v", flow)
	}
	if len(flow.Fragments) != 2 {
		t.Fatalf("aggregate fragments = %+v, want 2 fragments", flow.Fragments)
	}
	stage := flow.Stages[0]
	if stage.Distribution != DistributionByRange {
		t.Fatalf("aggregate scan distribution = %q, want %q", stage.Distribution, DistributionByRange)
	}
	if len(stage.Processors) != 2 {
		t.Fatalf("aggregate partial stage processors = %d, want 2", len(stage.Processors))
	}
	if stage.Processors[0].Kind != OperatorKVScan {
		t.Fatalf("first partial operator = %q, want %q", stage.Processors[0].Kind, OperatorKVScan)
	}
	if stage.Processors[1].Kind != OperatorAggregate {
		t.Fatalf("second partial operator = %q, want %q", stage.Processors[1].Kind, OperatorAggregate)
	}
	if stage.Processors[1].Aggregation == nil || stage.Processors[1].Aggregation.Mode != AggregateModePartial {
		t.Fatalf("partial aggregate spec = %+v, want partial mode", stage.Processors[1].Aggregation)
	}
	if len(stage.Processors[1].Aggregation.GroupBy) != 1 || stage.Processors[1].Aggregation.GroupBy[0] != "name" {
		t.Fatalf("partial aggregate group by = %+v, want [name]", stage.Processors[1].Aggregation.GroupBy)
	}
	if len(stage.Processors[1].Aggregation.Funcs) != 1 || stage.Processors[1].Aggregation.Funcs[0] != "count(*)" {
		t.Fatalf("partial aggregate funcs = %+v, want [count(*)]", stage.Processors[1].Aggregation.Funcs)
	}

	finalStage := flow.Stages[1]
	if finalStage.Distribution != DistributionGatewayOnly {
		t.Fatalf("final aggregate distribution = %q, want %q", finalStage.Distribution, DistributionGatewayOnly)
	}
	if len(finalStage.Processors) != 2 {
		t.Fatalf("final aggregate stage processors = %d, want 2", len(finalStage.Processors))
	}
	if finalStage.Processors[0].Kind != OperatorMerge {
		t.Fatalf("first final operator = %q, want %q", finalStage.Processors[0].Kind, OperatorMerge)
	}
	if finalStage.Processors[1].Kind != OperatorAggregate {
		t.Fatalf("second final operator = %q, want %q", finalStage.Processors[1].Kind, OperatorAggregate)
	}
	if finalStage.Processors[1].Aggregation == nil || finalStage.Processors[1].Aggregation.Mode != AggregateModeFinal {
		t.Fatalf("final aggregate spec = %+v, want final mode", finalStage.Processors[1].Aggregation)
	}
	if len(flow.ResultSchema) != 2 || flow.ResultSchema[0].Name != "name" || flow.ResultSchema[1].Name != "count" {
		t.Fatalf("aggregate result schema = %+v, want [name count]", flow.ResultSchema)
	}
}

func TestFlowPlannerBuildHashJoin(t *testing.T) {
	t.Parallel()

	planner := testPlanner(t)
	plan, err := planner.Plan("select u.name, o.sales from users u join orders o on u.id = o.user_id")
	if err != nil {
		t.Fatalf("plan query: %v", err)
	}

	flow, err := NewFlowPlanner().Build(plan)
	if err != nil {
		t.Fatalf("build flow: %v", err)
	}
	if flow.RootStageID != 3 || flow.RootFragmentID != 3 || len(flow.Stages) != 3 {
		t.Fatalf("unexpected hash-join flow shape: %+v", flow)
	}
	if len(flow.Fragments) != 3 {
		t.Fatalf("hash-join fragments = %+v, want 3 fragments", flow.Fragments)
	}
	if flow.Stages[0].Distribution != DistributionByRange || flow.Stages[1].Distribution != DistributionByRange {
		t.Fatalf("join scan distributions = (%q,%q), want by_range", flow.Stages[0].Distribution, flow.Stages[1].Distribution)
	}
	joinStage := flow.Stages[2]
	if joinStage.Distribution != DistributionGatewayOnly {
		t.Fatalf("join stage distribution = %q, want %q", joinStage.Distribution, DistributionGatewayOnly)
	}
	if len(joinStage.InputStageIDs) != 2 || joinStage.InputStageIDs[0] != 1 || joinStage.InputStageIDs[1] != 2 {
		t.Fatalf("join inputs = %+v, want [1 2]", joinStage.InputStageIDs)
	}
	if len(joinStage.Processors) != 3 {
		t.Fatalf("join stage processors = %d, want 3", len(joinStage.Processors))
	}
	if joinStage.Processors[2].Kind != OperatorHashJoin {
		t.Fatalf("join operator kind = %q, want %q", joinStage.Processors[2].Kind, OperatorHashJoin)
	}
	if joinStage.Processors[2].Join == nil {
		t.Fatalf("join spec is nil")
	}
	if joinStage.Processors[2].Join.LeftKeys[0] != "u.id" || joinStage.Processors[2].Join.RightKeys[0] != "o.user_id" {
		t.Fatalf("join keys = %+v, %+v", joinStage.Processors[2].Join.LeftKeys, joinStage.Processors[2].Join.RightKeys)
	}
	if len(joinStage.Processors[2].Projection) != 2 {
		t.Fatalf("join projection count = %d, want 2", len(joinStage.Processors[2].Projection))
	}
	if len(flow.ResultSchema) != 2 || flow.ResultSchema[0].Name != "name" || flow.ResultSchema[1].Name != "sales" {
		t.Fatalf("join result schema = %+v, want [name sales]", flow.ResultSchema)
	}
}
