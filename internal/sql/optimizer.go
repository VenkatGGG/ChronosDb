package sql

import (
	"fmt"
	"math"
	"sort"

	"github.com/VenkatGGG/ChronosDb/internal/storage"
)

// CostEstimate is the coarse cost vector attached to a physical plan candidate.
type CostEstimate struct {
	KVReads        uint64
	KVWrites       uint64
	EstimatedRows  uint64
	EstimatedBytes uint64
	CPUCost        float64
	Score          float64
}

// PlanCandidate is one physical implementation choice considered by the optimizer.
type PlanCandidate struct {
	Name string
	Plan Plan
	Cost CostEstimate
}

// OptimizedPlan captures the chosen plan and the considered alternatives.
type OptimizedPlan struct {
	Selected   PlanCandidate
	Candidates []PlanCandidate
}

// Optimizer ranks physical plan candidates with a coarse cost model.
type Optimizer struct{}

// NewOptimizer constructs a planner optimizer.
func NewOptimizer() *Optimizer {
	return &Optimizer{}
}

// Choose ranks candidates and returns the lowest-cost plan.
func (o *Optimizer) Choose(candidates []PlanCandidate) (OptimizedPlan, error) {
	if len(candidates) == 0 {
		return OptimizedPlan{}, fmt.Errorf("sql optimizer: no candidates to choose from")
	}
	ranked := append([]PlanCandidate(nil), candidates...)
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].Cost.Score == ranked[j].Cost.Score {
			return ranked[i].Name < ranked[j].Name
		}
		return ranked[i].Cost.Score < ranked[j].Cost.Score
	})
	return OptimizedPlan{
		Selected:   ranked[0],
		Candidates: ranked,
	}, nil
}

func (o *Optimizer) costPointLookup(table TableDescriptor, projection []ColumnDescriptor) CostEstimate {
	stats := table.StatsOrDefaults()
	bytes := projectedBytes(stats, projection)
	score := 1.0 + float64(bytes)/4096.0
	return CostEstimate{
		KVReads:        1,
		EstimatedRows:  1,
		EstimatedBytes: bytes,
		CPUCost:        1,
		Score:          score,
	}
}

func (o *Optimizer) costRangeScan(table TableDescriptor, projection []ColumnDescriptor, predicate boundPredicate, singleton bool) CostEstimate {
	stats := table.StatsOrDefaults()
	rows := estimateRangeRows(stats, predicate, singleton)
	bytes := rows * projectedBytes(stats, projection)
	cpu := 2.0 + math.Log2(float64(rows)+1)
	score := float64(rows) + cpu + float64(bytes)/2048.0
	return CostEstimate{
		KVReads:        max(1, rows),
		EstimatedRows:  rows,
		EstimatedBytes: bytes,
		CPUCost:        cpu,
		Score:          score,
	}
}

func (o *Optimizer) costInsert(table TableDescriptor, valueSize int) CostEstimate {
	stats := table.StatsOrDefaults()
	bytes := uint64(valueSize)
	if bytes == 0 {
		bytes = stats.AverageRowBytes
	}
	score := 2.0 + float64(bytes)/2048.0
	return CostEstimate{
		KVWrites:       1,
		EstimatedRows:  1,
		EstimatedBytes: bytes,
		CPUCost:        1,
		Score:          score,
	}
}

func projectedBytes(stats TableStats, projection []ColumnDescriptor) uint64 {
	if len(projection) == 0 {
		return stats.AverageRowBytes
	}
	perColumn := max(1, stats.AverageRowBytes/uint64(len(projection)))
	bytes := perColumn * uint64(len(projection))
	if bytes == 0 {
		return stats.AverageRowBytes
	}
	return bytes
}

func estimateRangeRows(stats TableStats, predicate boundPredicate, singleton bool) uint64 {
	if singleton {
		return 1
	}
	switch {
	case predicate.lower != nil && predicate.upper != nil:
		if predicate.lower.value.Type == ColumnTypeInt && predicate.upper.value.Type == ColumnTypeInt {
			lower := predicate.lower.value.Int64
			upper := predicate.upper.value.Int64
			if upper < lower {
				return 1
			}
			span := uint64(upper-lower) + 1
			if span > stats.EstimatedRows {
				return stats.EstimatedRows
			}
			return max(1, span)
		}
		return max(1, stats.EstimatedRows/10)
	case predicate.lower != nil || predicate.upper != nil:
		return max(1, stats.EstimatedRows/2)
	default:
		return max(1, stats.EstimatedRows)
	}
}

func makeSelectCandidates(o *Optimizer, table TableDescriptor, projection []ColumnDescriptor, predicate boundPredicate) ([]PlanCandidate, error) {
	prefix := storage.GlobalTablePrimaryPrefix(table.ID)
	if predicate.equality != nil {
		encoded, err := encodePrimaryKeyValue(*predicate.equality)
		if err != nil {
			return nil, err
		}
		key := storage.GlobalTablePrimaryKey(table.ID, encoded)
		return []PlanCandidate{
			{
				Name: "point_lookup",
				Plan: PointLookupPlan{
					Table:      table,
					Projection: projection,
					Key:        key,
				},
				Cost: o.costPointLookup(table, projection),
			},
			{
				Name: "singleton_range_scan",
				Plan: RangeScanPlan{
					Table:          table,
					Projection:     projection,
					StartKey:       key,
					EndKey:         storage.PrefixEnd(key),
					StartInclusive: true,
					EndInclusive:   false,
				},
				Cost: o.costRangeScan(table, projection, predicate, true),
			},
		}, nil
	}

	startKey := prefix
	endKey := storage.PrefixEnd(prefix)
	startInclusive := true
	endInclusive := false
	if predicate.lower != nil {
		encoded, err := encodePrimaryKeyValue(predicate.lower.value)
		if err != nil {
			return nil, err
		}
		startKey = storage.GlobalTablePrimaryKey(table.ID, encoded)
		startInclusive = predicate.lower.inclusive
	}
	if predicate.upper != nil {
		encoded, err := encodePrimaryKeyValue(predicate.upper.value)
		if err != nil {
			return nil, err
		}
		endKey = storage.GlobalTablePrimaryKey(table.ID, encoded)
		endInclusive = predicate.upper.inclusive
	}
	return []PlanCandidate{
		{
			Name: "range_scan",
			Plan: RangeScanPlan{
				Table:          table,
				Projection:     projection,
				StartKey:       startKey,
				EndKey:         endKey,
				StartInclusive: startInclusive,
				EndInclusive:   endInclusive,
			},
			Cost: o.costRangeScan(table, projection, predicate, false),
		},
	}, nil
}

func makeInsertCandidate(o *Optimizer, table TableDescriptor, plan InsertPlan) PlanCandidate {
	return PlanCandidate{
		Name: "insert_put",
		Plan: plan,
		Cost: o.costInsert(table, len(plan.Value)),
	}
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
