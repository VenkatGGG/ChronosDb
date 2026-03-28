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
	if flow.RootStageID != 1 || len(flow.Stages) != 1 {
		t.Fatalf("unexpected point-lookup flow shape: %+v", flow)
	}
	stage := flow.Stages[0]
	if stage.Distribution != DistributionLeaseholderOnly {
		t.Fatalf("distribution = %q, want %q", stage.Distribution, DistributionLeaseholderOnly)
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
	if flow.RootStageID != 2 || len(flow.Stages) != 2 {
		t.Fatalf("unexpected range-scan flow shape: %+v", flow)
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
	if flow.RootStageID != 1 || len(flow.Stages) != 1 {
		t.Fatalf("unexpected insert flow shape: %+v", flow)
	}
	if flow.Stages[0].HomeRegion != "us-east1" {
		t.Fatalf("insert home region = %q, want us-east1", flow.Stages[0].HomeRegion)
	}
	if flow.Stages[0].Processors[0].Kind != OperatorKVInsert {
		t.Fatalf("insert operator kind = %q, want %q", flow.Stages[0].Processors[0].Kind, OperatorKVInsert)
	}
}
