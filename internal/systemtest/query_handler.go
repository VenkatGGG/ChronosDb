package systemtest

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/hlc"
	"github.com/VenkatGGG/ChronosDb/internal/pgwire"
	chronossql "github.com/VenkatGGG/ChronosDb/internal/sql"
	"github.com/VenkatGGG/ChronosDb/internal/storage"
	"github.com/VenkatGGG/ChronosDb/internal/txn"
)

const defaultTxnDeadline = 30 * time.Second

type runtimeQueryHandler struct {
	planning *pgwire.PlanningHandler
	kv       *kvClient

	mu       sync.Mutex
	sessions map[*pgwire.Session]*sessionTxnState
}

type sessionTxnState struct {
	mu              sync.Mutex
	record          txn.Record
	writes          map[string]pendingTxnWrite
	failed          bool
	heartbeatCancel context.CancelFunc
}

type pendingTxnWrite struct {
	key       []byte
	value     []byte
	tombstone bool
	rangeID   uint64
}

type scanRow struct {
	key []byte
	row map[string]chronossql.Value
}

func newRuntimeQueryHandler(planner *chronossql.Planner, flowPlanner *chronossql.FlowPlanner, kv *kvClient) *runtimeQueryHandler {
	return &runtimeQueryHandler{
		planning: pgwire.NewPlanningHandler(planner, flowPlanner),
		kv:       kv,
		sessions: make(map[*pgwire.Session]*sessionTxnState),
	}
}

func (h *runtimeQueryHandler) HandleSimpleQuery(ctx context.Context, session *pgwire.Session, query string) (pgwire.QueryResult, error) {
	switch normalizeTransactionControl(query) {
	case "BEGIN":
		return h.beginSessionTxn(session)
	case "COMMIT":
		return h.commitSessionTxn(ctx, session)
	case "ROLLBACK":
		return h.rollbackSessionTxn(ctx, session)
	}

	if session != nil && session.TxStatus() == pgwire.TxFailedTransaction {
		return pgwire.QueryResult{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "25P02",
			Message:  "current transaction is aborted, commands ignored until end of transaction block",
		}
	}

	optimized, _, result, err := h.planning.DescribeQuery(query)
	if err != nil {
		h.failActiveTxn(ctx, session)
		return pgwire.QueryResult{}, err
	}

	var execErr error
	switch plan := optimized.Selected.Plan.(type) {
	case chronossql.PointLookupPlan:
		result, execErr = h.executePointLookup(ctx, session, result, plan)
	case chronossql.RangeScanPlan:
		result, execErr = h.executeRangeScan(ctx, session, result, plan)
	case chronossql.InsertPlan:
		result, execErr = h.executeInsert(ctx, session, result, plan)
	case chronossql.DeletePlan:
		result, execErr = h.executeDelete(ctx, session, result, plan)
	case chronossql.UpdatePlan:
		result, execErr = h.executeUpdate(ctx, session, result, plan)
	case chronossql.AggregatePlan:
		result, execErr = h.executeAggregateQuery(ctx, session, result, plan)
	case chronossql.HashJoinPlan:
		result, execErr = h.executeHashJoinQuery(ctx, session, result, plan)
	default:
		return result, nil
	}
	if execErr != nil {
		h.failActiveTxn(ctx, session)
		return pgwire.QueryResult{}, asQueryError(execErr)
	}
	return result, nil
}

func (h *runtimeQueryHandler) CloseSession(ctx context.Context, session *pgwire.Session) error {
	if session == nil {
		return nil
	}
	_, ok := h.lookupSessionState(session)
	if !ok {
		return nil
	}
	_, err := h.rollbackSessionTxn(ctx, session)
	return err
}

func (h *runtimeQueryHandler) beginSessionTxn(session *pgwire.Session) (pgwire.QueryResult, error) {
	if session == nil {
		return pgwire.QueryResult{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "08006",
			Message:  "session is not available",
		}
	}
	if session.TxStatus() != pgwire.TxIdle {
		return pgwire.QueryResult{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "25001",
			Message:  "transaction already in progress",
		}
	}

	now := txnNow()
	state := newTxnState(now)
	h.storeSessionState(session, state)
	h.startHeartbeat(state)
	session.SetTxStatus(pgwire.TxInTransaction)
	return pgwire.QueryResult{CommandTag: "BEGIN"}, nil
}

func (h *runtimeQueryHandler) commitSessionTxn(ctx context.Context, session *pgwire.Session) (pgwire.QueryResult, error) {
	if session == nil {
		return pgwire.QueryResult{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "08006",
			Message:  "session is not available",
		}
	}
	if session.TxStatus() == pgwire.TxFailedTransaction {
		return pgwire.QueryResult{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "25P02",
			Message:  "current transaction is aborted, commands ignored until end of transaction block",
		}
	}
	state, ok := h.lookupSessionState(session)
	if !ok {
		session.SetTxStatus(pgwire.TxIdle)
		return pgwire.QueryResult{CommandTag: "COMMIT"}, nil
	}

	h.stopHeartbeat(state)
	if err := h.commitTxnState(ctx, state); err != nil {
		return pgwire.QueryResult{}, err
	}

	h.deleteSessionState(session)
	session.SetTxStatus(pgwire.TxIdle)
	return pgwire.QueryResult{CommandTag: "COMMIT"}, nil
}

func (h *runtimeQueryHandler) rollbackSessionTxn(ctx context.Context, session *pgwire.Session) (pgwire.QueryResult, error) {
	if session == nil {
		return pgwire.QueryResult{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "08006",
			Message:  "session is not available",
		}
	}
	state, ok := h.lookupSessionState(session)
	if !ok {
		session.SetTxStatus(pgwire.TxIdle)
		return pgwire.QueryResult{CommandTag: "ROLLBACK"}, nil
	}

	h.stopHeartbeat(state)
	if err := h.abortTxnState(ctx, state); err != nil {
		return pgwire.QueryResult{}, err
	}

	h.deleteSessionState(session)
	session.SetTxStatus(pgwire.TxIdle)
	return pgwire.QueryResult{CommandTag: "ROLLBACK"}, nil
}

func (h *runtimeQueryHandler) failActiveTxn(ctx context.Context, session *pgwire.Session) {
	if session == nil || session.TxStatus() != pgwire.TxInTransaction {
		return
	}
	state, ok := h.lookupSessionState(session)
	if !ok {
		session.SetTxStatus(pgwire.TxFailedTransaction)
		return
	}

	h.stopHeartbeat(state)
	record, writes := state.snapshot()
	if record.AnchorRangeID != 0 {
		record.Status = txn.StatusAborted
		_ = h.kv.PutTxnRecord(ctx, record)
	}
	_ = h.resolveTxnRecord(ctx, record, writes)

	state.mu.Lock()
	state.record = record
	state.failed = true
	clear(state.writes)
	state.mu.Unlock()
	session.SetTxStatus(pgwire.TxFailedTransaction)
}

func (h *runtimeQueryHandler) executePointLookup(ctx context.Context, session *pgwire.Session, result pgwire.QueryResult, plan chronossql.PointLookupPlan) (pgwire.QueryResult, error) {
	if pending, ok := h.pendingWriteForKey(session, plan.Key); ok {
		if pending.tombstone {
			return result, nil
		}
		row, err := chronossql.DecodeRowValue(plan.Table, pending.value)
		if err != nil {
			return pgwire.QueryResult{}, wrapExecutionError("decode row payload", err)
		}
		projected, err := chronossql.ProjectRowText(plan.Projection, row)
		if err != nil {
			return pgwire.QueryResult{}, wrapExecutionError("project row payload", err)
		}
		result.Rows = [][][]byte{projected}
		result.CommandTag = "SELECT 1"
		return result, nil
	}

	value, found, err := h.kv.GetLatest(ctx, plan.Key)
	if err != nil {
		return pgwire.QueryResult{}, err
	}
	if !found {
		return result, nil
	}
	row, err := chronossql.DecodeRowValue(plan.Table, value)
	if err != nil {
		return pgwire.QueryResult{}, wrapExecutionError("decode row payload", err)
	}
	projected, err := chronossql.ProjectRowText(plan.Projection, row)
	if err != nil {
		return pgwire.QueryResult{}, wrapExecutionError("project row payload", err)
	}
	result.Rows = [][][]byte{projected}
	result.CommandTag = "SELECT 1"
	return result, nil
}

func (h *runtimeQueryHandler) executeRangeScan(ctx context.Context, session *pgwire.Session, result pgwire.QueryResult, plan chronossql.RangeScanPlan) (pgwire.QueryResult, error) {
	rows, err := h.scanRows(ctx, session, plan)
	if err != nil {
		return pgwire.QueryResult{}, err
	}
	result.Rows = make([][][]byte, 0, len(rows))
	for _, row := range rows {
		projected, err := chronossql.ProjectRowText(plan.Projection, row)
		if err != nil {
			return pgwire.QueryResult{}, wrapExecutionError("project row payload", err)
		}
		result.Rows = append(result.Rows, projected)
	}
	result.CommandTag = fmt.Sprintf("SELECT %d", len(result.Rows))
	return result, nil
}

func (h *runtimeQueryHandler) executeInsert(ctx context.Context, session *pgwire.Session, result pgwire.QueryResult, plan chronossql.InsertPlan) (pgwire.QueryResult, error) {
	if session == nil || session.TxStatus() != pgwire.TxInTransaction {
		if err := h.kv.OnePhasePut(ctx, plan.Key, plan.Value); err != nil {
			return pgwire.QueryResult{}, err
		}
		return result, nil
	}
	state, ok := h.lookupSessionState(session)
	if !ok {
		return pgwire.QueryResult{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "08006",
			Message:  "transaction state is not available",
		}
	}
	if err := h.stageWrite(ctx, state, plan.Key, plan.Value, false); err != nil {
		return pgwire.QueryResult{}, err
	}
	return result, nil
}

func (h *runtimeQueryHandler) executeDelete(ctx context.Context, session *pgwire.Session, result pgwire.QueryResult, plan chronossql.DeletePlan) (pgwire.QueryResult, error) {
	targets, err := h.scanRowSet(ctx, session, plan.Input)
	if err != nil {
		return pgwire.QueryResult{}, err
	}
	if len(targets) == 0 {
		result.CommandTag = "DELETE 0"
		return result, nil
	}

	if session == nil || session.TxStatus() != pgwire.TxInTransaction {
		state := newTxnState(txnNow())
		for _, target := range targets {
			if err := h.stageWrite(ctx, state, target.key, nil, true); err != nil {
				_ = h.abortTxnState(ctx, state)
				return pgwire.QueryResult{}, err
			}
		}
		if err := h.commitTxnState(ctx, state); err != nil {
			return pgwire.QueryResult{}, err
		}
		result.CommandTag = fmt.Sprintf("DELETE %d", len(targets))
		return result, nil
	}

	state, ok := h.lookupSessionState(session)
	if !ok {
		return pgwire.QueryResult{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "08006",
			Message:  "transaction state is not available",
		}
	}
	for _, target := range targets {
		if err := h.stageWrite(ctx, state, target.key, nil, true); err != nil {
			return pgwire.QueryResult{}, err
		}
	}
	result.CommandTag = fmt.Sprintf("DELETE %d", len(targets))
	return result, nil
}

func (h *runtimeQueryHandler) executeUpdate(ctx context.Context, session *pgwire.Session, result pgwire.QueryResult, plan chronossql.UpdatePlan) (pgwire.QueryResult, error) {
	targets, err := h.scanRowSet(ctx, session, plan.Input)
	if err != nil {
		return pgwire.QueryResult{}, err
	}
	if len(targets) == 0 {
		result.CommandTag = "UPDATE 0"
		return result, nil
	}

	if session == nil || session.TxStatus() != pgwire.TxInTransaction {
		state := newTxnState(txnNow())
		for _, target := range targets {
			updatedPayload, err := rewriteUpdatedRow(plan.Table, target.row, plan.Assignments)
			if err != nil {
				_ = h.abortTxnState(ctx, state)
				return pgwire.QueryResult{}, err
			}
			if err := h.stageWrite(ctx, state, target.key, updatedPayload, false); err != nil {
				_ = h.abortTxnState(ctx, state)
				return pgwire.QueryResult{}, err
			}
		}
		if err := h.commitTxnState(ctx, state); err != nil {
			return pgwire.QueryResult{}, err
		}
		result.CommandTag = fmt.Sprintf("UPDATE %d", len(targets))
		return result, nil
	}

	state, ok := h.lookupSessionState(session)
	if !ok {
		return pgwire.QueryResult{}, pgwire.Error{
			Severity: "ERROR",
			Code:     "08006",
			Message:  "transaction state is not available",
		}
	}
	for _, target := range targets {
		updatedPayload, err := rewriteUpdatedRow(plan.Table, target.row, plan.Assignments)
		if err != nil {
			return pgwire.QueryResult{}, err
		}
		if err := h.stageWrite(ctx, state, target.key, updatedPayload, false); err != nil {
			return pgwire.QueryResult{}, err
		}
	}
	result.CommandTag = fmt.Sprintf("UPDATE %d", len(targets))
	return result, nil
}

func (h *runtimeQueryHandler) executeAggregateQuery(ctx context.Context, session *pgwire.Session, result pgwire.QueryResult, plan chronossql.AggregatePlan) (pgwire.QueryResult, error) {
	rows, err := h.executeAggregate(ctx, session, plan)
	if err != nil {
		return pgwire.QueryResult{}, err
	}
	result.Rows = rows
	result.CommandTag = fmt.Sprintf("SELECT %d", len(rows))
	return result, nil
}

func (h *runtimeQueryHandler) executeHashJoinQuery(ctx context.Context, session *pgwire.Session, result pgwire.QueryResult, plan chronossql.HashJoinPlan) (pgwire.QueryResult, error) {
	rows, err := h.executeHashJoin(ctx, session, plan)
	if err != nil {
		return pgwire.QueryResult{}, err
	}
	result.Rows = rows
	result.CommandTag = fmt.Sprintf("SELECT %d", len(rows))
	return result, nil
}

func (h *runtimeQueryHandler) executeAggregate(ctx context.Context, session *pgwire.Session, plan chronossql.AggregatePlan) ([][][]byte, error) {
	rows, err := h.scanRows(ctx, session, plan.Input)
	if err != nil {
		return nil, err
	}
	type aggregateState struct {
		groupValues []chronossql.Value
		counts      []int64
		sums        []int64
		sumSeen     []bool
	}
	groups := make(map[string]*aggregateState)
	order := make([]string, 0)

	ensureState := func(groupKey string, row map[string]chronossql.Value) *aggregateState {
		state, ok := groups[groupKey]
		if ok {
			return state
		}
		state = &aggregateState{
			groupValues: make([]chronossql.Value, len(plan.GroupBy)),
			counts:      make([]int64, len(plan.Aggregates)),
			sums:        make([]int64, len(plan.Aggregates)),
			sumSeen:     make([]bool, len(plan.Aggregates)),
		}
		for i, column := range plan.GroupBy {
			value, _ := lookupValue(row, column.Name)
			state.groupValues[i] = value
		}
		groups[groupKey] = state
		order = append(order, groupKey)
		return state
	}

	if len(rows) == 0 && len(plan.GroupBy) == 0 {
		ensureState("", nil)
	}

	for _, row := range rows {
		groupKey := aggregateGroupKey(plan.GroupBy, row)
		state := ensureState(groupKey, row)
		for i, aggregate := range plan.Aggregates {
			switch aggregate.Func {
			case chronossql.AggregateFuncCount:
				if aggregate.Input == nil {
					state.counts[i]++
					continue
				}
				if _, ok := lookupValue(row, aggregate.Input.Name); ok {
					state.counts[i]++
				}
			case chronossql.AggregateFuncSum:
				if aggregate.Input == nil {
					continue
				}
				value, ok := lookupValue(row, aggregate.Input.Name)
				if !ok {
					continue
				}
				state.sums[i] += value.Int64
				state.sumSeen[i] = true
			}
		}
	}

	sort.Strings(order)
	out := make([][][]byte, 0, len(order))
	for _, key := range order {
		state := groups[key]
		row := make([][]byte, 0, len(plan.GroupBy)+len(plan.Aggregates))
		for _, value := range state.groupValues {
			formatted, err := chronossql.FormatValueText(value)
			if err != nil {
				return nil, wrapExecutionError("format aggregate group value", err)
			}
			row = append(row, formatted)
		}
		for i, aggregate := range plan.Aggregates {
			switch aggregate.Func {
			case chronossql.AggregateFuncCount:
				formatted, err := chronossql.FormatValueText(chronossql.Value{
					Type:  chronossql.ColumnTypeInt,
					Int64: state.counts[i],
				})
				if err != nil {
					return nil, wrapExecutionError("format aggregate count", err)
				}
				row = append(row, formatted)
			case chronossql.AggregateFuncSum:
				if !state.sumSeen[i] {
					row = append(row, nil)
					continue
				}
				formatted, err := chronossql.FormatValueText(chronossql.Value{
					Type:  chronossql.ColumnTypeInt,
					Int64: state.sums[i],
				})
				if err != nil {
					return nil, wrapExecutionError("format aggregate sum", err)
				}
				row = append(row, formatted)
			default:
				return nil, wrapExecutionError("aggregate", fmt.Errorf("unsupported aggregate func %q", aggregate.Func))
			}
		}
		out = append(out, row)
	}
	return out, nil
}

func (h *runtimeQueryHandler) executeHashJoin(ctx context.Context, session *pgwire.Session, plan chronossql.HashJoinPlan) ([][][]byte, error) {
	leftRows, err := h.scanRows(ctx, session, plan.LeftScan)
	if err != nil {
		return nil, err
	}
	rightRows, err := h.scanRows(ctx, session, plan.RightScan)
	if err != nil {
		return nil, err
	}
	rightBuckets := make(map[string][]map[string]chronossql.Value, len(rightRows))
	for _, row := range rightRows {
		key, ok := joinKey(plan.Join.RightKeys, row)
		if !ok {
			continue
		}
		rightBuckets[key] = append(rightBuckets[key], row)
	}

	out := make([][][]byte, 0)
	for _, leftRow := range leftRows {
		key, ok := joinKey(plan.Join.LeftKeys, leftRow)
		if !ok {
			continue
		}
		matches := rightBuckets[key]
		for _, rightRow := range matches {
			projected, err := projectJoinRow(plan, leftRow, rightRow)
			if err != nil {
				return nil, err
			}
			out = append(out, projected)
		}
	}
	return out, nil
}

func (h *runtimeQueryHandler) scanRows(ctx context.Context, session *pgwire.Session, plan chronossql.RangeScanPlan) ([]map[string]chronossql.Value, error) {
	scanned, err := h.scanRowSet(ctx, session, plan)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]chronossql.Value, 0, len(scanned))
	for _, item := range scanned {
		rows = append(rows, item.row)
	}
	return rows, nil
}

func (h *runtimeQueryHandler) scanRowSet(ctx context.Context, session *pgwire.Session, plan chronossql.RangeScanPlan) ([]scanRow, error) {
	scanned, err := h.kv.ScanRange(ctx, plan.StartKey, plan.EndKey, plan.StartInclusive, plan.EndInclusive)
	if err != nil {
		return nil, err
	}
	rowByKey := make(map[string]kvScanRow, len(scanned))
	for _, item := range scanned {
		rowByKey[string(item.LogicalKey)] = item
	}
	for _, pending := range h.pendingWritesInSpan(session, plan.StartKey, plan.EndKey, plan.StartInclusive, plan.EndInclusive) {
		if pending.tombstone {
			delete(rowByKey, string(pending.key))
			continue
		}
		rowByKey[string(pending.key)] = kvScanRow{
			LogicalKey: append([]byte(nil), pending.key...),
			Value:      append([]byte(nil), pending.value...),
		}
	}
	keys := make([][]byte, 0, len(rowByKey))
	for _, row := range rowByKey {
		keys = append(keys, append([]byte(nil), row.LogicalKey...))
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	rows := make([]scanRow, 0, len(keys))
	for _, key := range keys {
		row, err := chronossql.DecodeRowValue(plan.Table, rowByKey[string(key)].Value)
		if err != nil {
			return nil, wrapExecutionError("decode scanned row", err)
		}
		rows = append(rows, scanRow{
			key: append([]byte(nil), key...),
			row: row,
		})
	}
	return rows, nil
}

func (h *runtimeQueryHandler) ensureAnchoredRecord(ctx context.Context, state *sessionTxnState, rangeID uint64) (txn.Record, error) {
	now := txnNow()

	state.mu.Lock()
	record := state.record
	var err error
	needsPersist := false
	if record.AnchorRangeID == 0 {
		record, err = record.Anchor(rangeID, now)
		needsPersist = err == nil
	} else {
		touched := containsRangeID(record.TouchedRanges, rangeID)
		record, err = record.Heartbeat(now)
		if err == nil && !touched {
			record, err = record.TouchRange(rangeID)
		}
		needsPersist = err == nil
	}
	if err == nil {
		state.record = record
	}
	state.mu.Unlock()
	if err != nil {
		return txn.Record{}, err
	}
	if needsPersist {
		if err := h.kv.PutTxnRecord(ctx, record); err != nil {
			return txn.Record{}, err
		}
	}
	return record, nil
}

func (h *runtimeQueryHandler) stageWrite(ctx context.Context, state *sessionTxnState, key, value []byte, tombstone bool) error {
	desc, err := h.kv.LookupRange(ctx, key)
	if err != nil {
		return err
	}
	record, err := h.ensureAnchoredRecord(ctx, state, desc.RangeID)
	if err != nil {
		return err
	}
	decision, err := h.kv.AcquireLock(ctx, key, record, storage.IntentStrengthExclusive)
	if err != nil {
		return err
	}
	if decision.Kind == txn.LockDecisionWait {
		contention, mapErr := txn.ContentionErrorFromDecision(record, decision, 25)
		if mapErr != nil {
			return mapErr
		}
		return contention
	}

	intent := storage.Intent{
		TxnID:          record.ID,
		Epoch:          record.Epoch,
		WriteTimestamp: record.WriteTS,
		Strength:       storage.IntentStrengthExclusive,
		Tombstone:      tombstone,
	}
	if !tombstone {
		intent.Value = append([]byte(nil), value...)
	}
	record, err = record.TrackIntent(txn.IntentRef{
		RangeID:  desc.RangeID,
		Key:      append([]byte(nil), key...),
		Strength: storage.IntentStrengthExclusive,
	})
	if err != nil {
		_ = h.kv.ReleaseLock(ctx, key, record.ID)
		return err
	}
	if err := h.kv.PutTxnRecord(ctx, record); err != nil {
		_ = h.kv.ReleaseLock(ctx, key, record.ID)
		return err
	}
	if err := h.kv.PutIntent(ctx, key, intent); err != nil {
		_ = h.kv.ReleaseLock(ctx, key, record.ID)
		return err
	}

	state.mu.Lock()
	state.record = record
	state.writes[string(key)] = pendingTxnWrite{
		key:       append([]byte(nil), key...),
		value:     append([]byte(nil), value...),
		tombstone: tombstone,
		rangeID:   desc.RangeID,
	}
	state.mu.Unlock()
	return nil
}

func (h *runtimeQueryHandler) resolveTxnRecord(ctx context.Context, record txn.Record, writes []pendingTxnWrite) error {
	if len(record.RequiredIntents) > 0 {
		return h.kv.ResolveTxnRecord(ctx, record)
	}
	for _, write := range writes {
		_ = h.kv.DeleteIntent(ctx, write.key)
		_ = h.kv.ReleaseLock(ctx, write.key, record.ID)
	}
	return nil
}

func (h *runtimeQueryHandler) commitTxnState(ctx context.Context, state *sessionTxnState) error {
	record, writes := state.snapshot()
	if record.AnchorRangeID == 0 || len(writes) == 0 {
		return nil
	}

	now := txnNow()
	if record.LastHeartbeatTS.IsZero() || now.Compare(record.LastHeartbeatTS) > 0 {
		updated, err := record.Heartbeat(now)
		if err == nil {
			record = updated
		}
	}
	record.WriteTS = maxTimestamp(record.WriteTS, maxTimestamp(record.MinCommitTS, now))

	if len(writes) > 1 || len(record.TouchedRanges) > 1 {
		required, err := intentSetFromRecord(record)
		if err != nil {
			return wrapExecutionError("build staged intent set", err)
		}
		staged, err := record.StageForParallelCommit(required)
		if err != nil {
			return wrapExecutionError("stage parallel commit", err)
		}
		if err := h.kv.PutTxnRecord(ctx, staged); err != nil {
			return wrapExecutionError("persist staged txn record", err)
		}
		recovered, _, err := txn.RecoverAfterCoordinatorFailure(staged, required, buildObservedIntents(staged, writes))
		if err != nil {
			return wrapExecutionError("recover staged txn record", err)
		}
		record = recovered
	}
	record.Status = txn.StatusCommitted
	if err := h.kv.PutTxnRecord(ctx, record); err != nil {
		return wrapExecutionError("persist committed txn record", err)
	}
	if err := h.resolveTxnRecord(ctx, record, writes); err != nil {
		return wrapExecutionError("resolve committed txn", err)
	}
	return nil
}

func (h *runtimeQueryHandler) abortTxnState(ctx context.Context, state *sessionTxnState) error {
	record, writes := state.snapshot()
	if record.AnchorRangeID != 0 {
		record.Status = txn.StatusAborted
		if err := h.kv.PutTxnRecord(ctx, record); err != nil {
			return wrapExecutionError("persist aborted txn record", err)
		}
	}
	if err := h.resolveTxnRecord(ctx, record, writes); err != nil {
		return wrapExecutionError("resolve aborted txn", err)
	}
	return nil
}

func (h *runtimeQueryHandler) storeSessionState(session *pgwire.Session, state *sessionTxnState) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sessions[session] = state
}

func (h *runtimeQueryHandler) lookupSessionState(session *pgwire.Session) (*sessionTxnState, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	state, ok := h.sessions[session]
	return state, ok
}

func (h *runtimeQueryHandler) deleteSessionState(session *pgwire.Session) {
	h.mu.Lock()
	state := h.sessions[session]
	delete(h.sessions, session)
	h.mu.Unlock()
	h.stopHeartbeat(state)
}

func (h *runtimeQueryHandler) startHeartbeat(state *sessionTxnState) {
	if state == nil {
		return
	}
	state.mu.Lock()
	if state.heartbeatCancel != nil {
		state.mu.Unlock()
		return
	}
	heartbeatCtx, cancel := context.WithCancel(context.Background())
	state.heartbeatCancel = cancel
	state.mu.Unlock()

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				state.mu.Lock()
				record := state.record
				if state.failed || record.AnchorRangeID == 0 || record.IsTerminal() {
					state.mu.Unlock()
					continue
				}
				next, err := record.Heartbeat(txnNow())
				if err != nil {
					state.mu.Unlock()
					continue
				}
				state.record = next
				state.mu.Unlock()
				_ = h.kv.PutTxnRecord(context.Background(), next)
			}
		}
	}()
}

func (h *runtimeQueryHandler) stopHeartbeat(state *sessionTxnState) {
	if state == nil {
		return
	}
	state.mu.Lock()
	cancel := state.heartbeatCancel
	state.heartbeatCancel = nil
	state.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (h *runtimeQueryHandler) pendingWriteForKey(session *pgwire.Session, key []byte) (pendingTxnWrite, bool) {
	state, ok := h.lookupSessionState(session)
	if !ok {
		return pendingTxnWrite{}, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	write, ok := state.writes[string(key)]
	if !ok {
		return pendingTxnWrite{}, false
	}
	return clonePendingWrite(write), true
}

func (h *runtimeQueryHandler) pendingWritesInSpan(session *pgwire.Session, startKey, endKey []byte, startInclusive, endInclusive bool) []pendingTxnWrite {
	state, ok := h.lookupSessionState(session)
	if !ok {
		return nil
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	writes := make([]pendingTxnWrite, 0, len(state.writes))
	for _, write := range state.writes {
		if keyWithinSpan(write.key, startKey, endKey, startInclusive, endInclusive) {
			writes = append(writes, clonePendingWrite(write))
		}
	}
	return writes
}

func normalizeTransactionControl(query string) string {
	trimmed := strings.TrimSpace(query)
	trimmed = strings.TrimRight(trimmed, ";")
	trimmed = strings.TrimSpace(trimmed)
	trimmed = strings.Join(strings.Fields(trimmed), " ")
	switch strings.ToUpper(trimmed) {
	case "BEGIN", "START TRANSACTION":
		return "BEGIN"
	case "COMMIT", "END":
		return "COMMIT"
	case "ROLLBACK", "ABORT":
		return "ROLLBACK"
	default:
		return ""
	}
}

func newTxnState(now hlc.Timestamp) *sessionTxnState {
	return &sessionTxnState{
		record: txn.Record{
			ID:         newTxnID(),
			Status:     txn.StatusPending,
			ReadTS:     now,
			WriteTS:    now,
			Priority:   1,
			DeadlineTS: hlc.Timestamp{WallTime: now.WallTime + uint64(defaultTxnDeadline)},
		},
		writes: make(map[string]pendingTxnWrite),
	}
}

func buildIntentSet(writes []pendingTxnWrite) (txn.IntentSet, error) {
	var set txn.IntentSet
	for _, write := range writes {
		if err := set.Add(txn.IntentRef{
			RangeID:  write.rangeID,
			Key:      append([]byte(nil), write.key...),
			Strength: storage.IntentStrengthExclusive,
		}); err != nil {
			return txn.IntentSet{}, err
		}
	}
	return set, nil
}

func intentSetFromRecord(record txn.Record) (txn.IntentSet, error) {
	var set txn.IntentSet
	for _, ref := range record.RequiredIntents {
		if err := set.Add(ref); err != nil {
			return txn.IntentSet{}, err
		}
	}
	return set, nil
}

func buildObservedIntents(record txn.Record, writes []pendingTxnWrite) []txn.ObservedIntent {
	observed := make([]txn.ObservedIntent, 0, len(writes))
	for _, write := range writes {
		observed = append(observed, txn.ObservedIntent{
			Ref: txn.IntentRef{
				RangeID:  write.rangeID,
				Key:      append([]byte(nil), write.key...),
				Strength: storage.IntentStrengthExclusive,
			},
			Intent: storage.Intent{
				TxnID:          record.ID,
				Epoch:          record.Epoch,
				WriteTimestamp: record.WriteTS,
				Strength:       storage.IntentStrengthExclusive,
				Tombstone:      write.tombstone,
				Value:          append([]byte(nil), write.value...),
			},
			Present: true,
		})
	}
	return observed
}

func clonePendingWrite(write pendingTxnWrite) pendingTxnWrite {
	return pendingTxnWrite{
		key:       append([]byte(nil), write.key...),
		value:     append([]byte(nil), write.value...),
		tombstone: write.tombstone,
		rangeID:   write.rangeID,
	}
}

func rewriteUpdatedRow(table chronossql.TableDescriptor, base map[string]chronossql.Value, assignments []chronossql.UpdateAssignment) ([]byte, error) {
	row := make(map[string]chronossql.Value, len(base)+len(assignments))
	for name, value := range base {
		row[name] = cloneSQLValue(value)
	}
	for _, assignment := range assignments {
		row[strings.ToLower(strings.TrimSpace(assignment.Column.Name))] = cloneSQLValue(assignment.Value)
	}
	payload, err := chronossql.EncodeRowValue(table, row)
	if err != nil {
		return nil, wrapExecutionError("encode updated row", err)
	}
	return payload, nil
}

func cloneSQLValue(value chronossql.Value) chronossql.Value {
	cloned := value
	if value.Type == chronossql.ColumnTypeBytes && value.Bytes != nil {
		cloned.Bytes = append([]byte(nil), value.Bytes...)
	}
	return cloned
}

func (s *sessionTxnState) snapshot() (txn.Record, []pendingTxnWrite) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writes := make([]pendingTxnWrite, 0, len(s.writes))
	for _, write := range s.writes {
		writes = append(writes, clonePendingWrite(write))
	}
	return s.record, writes
}

func keyWithinSpan(key, startKey, endKey []byte, startInclusive, endInclusive bool) bool {
	if cmp := bytes.Compare(key, startKey); cmp < 0 || (cmp == 0 && !startInclusive) {
		return false
	}
	if len(endKey) == 0 {
		return true
	}
	cmp := bytes.Compare(key, endKey)
	if endInclusive {
		return cmp <= 0
	}
	return cmp < 0
}

func containsRangeID(ranges []uint64, rangeID uint64) bool {
	for _, existing := range ranges {
		if existing == rangeID {
			return true
		}
	}
	return false
}

func txnNow() hlc.Timestamp {
	return hlc.Timestamp{WallTime: uint64(time.Now().UTC().UnixNano())}
}

func maxTimestamp(left, right hlc.Timestamp) hlc.Timestamp {
	if left.Compare(right) >= 0 {
		return left
	}
	return right
}

func asQueryError(err error) error {
	if err == nil {
		return nil
	}
	if wireErr, ok := err.(pgwire.Error); ok {
		return wireErr
	}
	txnErr, ok := err.(*txn.Error)
	if !ok {
		return err
	}
	code := "XX000"
	switch txnErr.Code {
	case txn.CodeTxnRetrySerialization, txn.CodeTxnRetryWriteTooOld, txn.CodeContentionBackoff:
		code = "40001"
	case txn.CodeTxnAborted:
		code = "25P02"
	}
	return pgwire.Error{
		Severity: "ERROR",
		Code:     code,
		Message:  txnErr.Message,
	}
}

func aggregateGroupKey(columns []chronossql.ColumnDescriptor, row map[string]chronossql.Value) string {
	if len(columns) == 0 {
		return ""
	}
	var builder strings.Builder
	for i, column := range columns {
		if i > 0 {
			builder.WriteString("|")
		}
		value, _ := lookupValue(row, column.Name)
		builder.WriteString(column.Name)
		builder.WriteString("=")
		builder.WriteString(valueKey(value))
	}
	return builder.String()
}

func joinKey(qualifiedColumns []string, row map[string]chronossql.Value) (string, bool) {
	if len(qualifiedColumns) == 0 {
		return "", false
	}
	parts := make([]string, 0, len(qualifiedColumns))
	for _, qualified := range qualifiedColumns {
		column := qualified
		if idx := strings.IndexByte(qualified, '.'); idx >= 0 {
			column = qualified[idx+1:]
		}
		value, ok := lookupValue(row, column)
		if !ok {
			return "", false
		}
		parts = append(parts, valueKey(value))
	}
	return strings.Join(parts, "|"), true
}

func projectJoinRow(plan chronossql.HashJoinPlan, leftRow, rightRow map[string]chronossql.Value) ([][]byte, error) {
	out := make([][]byte, 0, len(plan.Projection))
	for _, projection := range plan.Projection {
		var (
			row map[string]chronossql.Value
			ok  bool
			val chronossql.Value
		)
		switch strings.ToLower(strings.TrimSpace(projection.SourceAlias)) {
		case strings.ToLower(strings.TrimSpace(plan.Left.Alias)):
			row = leftRow
		case strings.ToLower(strings.TrimSpace(plan.Right.Alias)):
			row = rightRow
		default:
			return nil, wrapExecutionError("join projection", fmt.Errorf("unknown join source alias %q", projection.SourceAlias))
		}
		val, ok = lookupValue(row, projection.SourceColumn.Name)
		if !ok {
			if projection.Output.Nullable {
				out = append(out, nil)
				continue
			}
			return nil, wrapExecutionError("join projection", fmt.Errorf("missing join column %q", projection.SourceColumn.Name))
		}
		formatted, err := chronossql.FormatValueText(val)
		if err != nil {
			return nil, wrapExecutionError("format join value", err)
		}
		out = append(out, formatted)
	}
	return out, nil
}

func lookupValue(row map[string]chronossql.Value, column string) (chronossql.Value, bool) {
	if row == nil {
		return chronossql.Value{}, false
	}
	value, ok := row[strings.ToLower(strings.TrimSpace(column))]
	return value, ok
}

func valueKey(value chronossql.Value) string {
	switch value.Type {
	case chronossql.ColumnTypeInt:
		return fmt.Sprintf("i:%d", value.Int64)
	case chronossql.ColumnTypeBytes:
		return "b:" + string(value.Bytes)
	case chronossql.ColumnTypeString:
		fallthrough
	default:
		return "s:" + value.String
	}
}

func wrapExecutionError(scope string, err error) pgwire.Error {
	return pgwire.Error{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  fmt.Sprintf("%s: %v", scope, err),
	}
}
