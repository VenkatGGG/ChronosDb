package appcompat

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/VenkatGGG/ChronosDb/internal/pgclient"
)

const (
	defaultUser             = "chronos"
	defaultIterations       = 10
	defaultStatementTimeout = 5 * time.Second
	defaultBaseUserID int64 = 200000
	defaultConflictID int64 = 1200000
)

var operationOrder = []string{
	"insert_user",
	"select_user",
	"update_user",
	"upsert_user",
	"tx_update_user",
	"conflict_upsert_user",
	"scan_users",
	"delete_user",
	"verify_absent",
}

// Config drives one app-compatibility workload run.
type Config struct {
	Addr             string
	User             string
	Iterations       int
	StatementTimeout time.Duration
	BaseUserID       int64
	BaseConflictID   int64
}

// Report summarizes one seeded CRUD workload run.
type Report struct {
	Addr              string             `json:"addr"`
	User              string             `json:"user"`
	Iterations        int                `json:"iterations"`
	StartedAt         time.Time          `json:"started_at"`
	FinishedAt        time.Time          `json:"finished_at"`
	Compatible        bool               `json:"compatible"`
	TotalFailures     int                `json:"total_failures"`
	MissingOperations []string           `json:"missing_operations,omitempty"`
	Operations        []OperationSummary `json:"operations"`
}

// OperationSummary captures latency, command-tag, and error-class aggregates
// for one logical CRUD workload step.
type OperationSummary struct {
	Name           string         `json:"name"`
	Count          int            `json:"count"`
	Successes      int            `json:"successes"`
	Failures       int            `json:"failures"`
	CommandTags    map[string]int `json:"command_tags,omitempty"`
	ErrorClasses   map[string]int `json:"error_classes,omitempty"`
	LastError      string         `json:"last_error,omitempty"`
	AverageMs      float64        `json:"average_ms"`
	P50Ms          float64        `json:"p50_ms"`
	P95Ms          float64        `json:"p95_ms"`
	MaxMs          float64        `json:"max_ms"`
}

type workloadSample struct {
	UserID        int64
	ConflictID    int64
	Name          string
	Email         string
	UpdatedName   string
	UpdatedEmail  string
	UpsertedName  string
	UpsertedEmail string
	TxName        string
	ConflictName  string
}

type opStats struct {
	count        int
	successes    int
	failures     int
	durations    []time.Duration
	commandTags  map[string]int
	errorClasses map[string]int
	lastError    string
}

// Run executes the prepared-statement CRUD workload against one running node.
func Run(ctx context.Context, cfg Config) (Report, error) {
	cfg = applyDefaults(cfg)
	if cfg.Addr == "" {
		return Report{}, fmt.Errorf("appcompat: addr is required")
	}

	dialCtx, cancelDial := context.WithTimeout(ctx, cfg.StatementTimeout)
	defer cancelDial()
	client, err := pgclient.Dial(dialCtx, cfg.Addr, cfg.User)
	if err != nil {
		return Report{}, err
	}
	defer client.Close()

	startedAt := time.Now().UTC()
	stats := make(map[string]*opStats, len(operationOrder))
	for _, name := range operationOrder {
		stats[name] = &opStats{
			commandTags:  make(map[string]int),
			errorClasses: make(map[string]int),
		}
	}

	for i := 0; i < cfg.Iterations; i++ {
		sample := buildSample(cfg, i)
		if !runPreparedOp(ctx, cfg, stats["insert_user"], func(stepCtx context.Context) (pgclient.Result, error) {
			return client.ExtendedQuery(stepCtx, "insert into users (id, name, email) values ($1, $2, $3) returning id, name, email", sample.UserID, sample.Name, sample.Email)
		}, func(result pgclient.Result) error {
			return expectOneRow(result, "INSERT 0 1", []string{formatInt(sample.UserID), sample.Name, sample.Email})
		}) {
			continue
		}
		if !runPreparedOp(ctx, cfg, stats["select_user"], func(stepCtx context.Context) (pgclient.Result, error) {
			return client.ExtendedQuery(stepCtx, "select id, name, email from users where id = $1", sample.UserID)
		}, func(result pgclient.Result) error {
			return expectOneRow(result, "SELECT 1", []string{formatInt(sample.UserID), sample.Name, sample.Email})
		}) {
			continue
		}
		if !runPreparedOp(ctx, cfg, stats["update_user"], func(stepCtx context.Context) (pgclient.Result, error) {
			return client.ExtendedQuery(stepCtx, "update users set name = $1, email = $2 where id = $3 returning id, name, email", sample.UpdatedName, sample.UpdatedEmail, sample.UserID)
		}, func(result pgclient.Result) error {
			return expectOneRow(result, "UPDATE 1", []string{formatInt(sample.UserID), sample.UpdatedName, sample.UpdatedEmail})
		}) {
			continue
		}
		if !runPreparedOp(ctx, cfg, stats["upsert_user"], func(stepCtx context.Context) (pgclient.Result, error) {
			return client.ExtendedQuery(stepCtx, "upsert into users (id, name, email) values ($1, $2, $3) returning id, name, email", sample.UserID, sample.UpsertedName, sample.UpsertedEmail)
		}, func(result pgclient.Result) error {
			return expectOneRow(result, "UPSERT 1", []string{formatInt(sample.UserID), sample.UpsertedName, sample.UpsertedEmail})
		}) {
			continue
		}
		if !runPreparedOp(ctx, cfg, stats["tx_update_user"], func(stepCtx context.Context) (pgclient.Result, error) {
			if _, err := client.SimpleQuery(stepCtx, "begin"); err != nil {
				return pgclient.Result{}, err
			}
			result, err := client.ExtendedQuery(stepCtx, "update users set name = $1 where id = $2 returning id, name", sample.TxName, sample.UserID)
			if err != nil {
				_, _ = client.SimpleQuery(stepCtx, "rollback")
				return pgclient.Result{}, err
			}
			checkResult, checkErr := client.ExtendedQuery(stepCtx, "select id, name from users where id = $1", sample.UserID)
			if checkErr != nil {
				_, _ = client.SimpleQuery(stepCtx, "rollback")
				return pgclient.Result{}, checkErr
			}
			if err := expectOneRow(checkResult, "SELECT 1", []string{formatInt(sample.UserID), sample.TxName}); err != nil {
				_, _ = client.SimpleQuery(stepCtx, "rollback")
				return pgclient.Result{}, err
			}
			if _, err := client.SimpleQuery(stepCtx, "commit"); err != nil {
				return pgclient.Result{}, err
			}
			return result, nil
		}, func(result pgclient.Result) error {
			return expectOneRow(result, "UPDATE 1", []string{formatInt(sample.UserID), sample.TxName})
		}) {
			continue
		}
		if !runPreparedOp(ctx, cfg, stats["conflict_upsert_user"], func(stepCtx context.Context) (pgclient.Result, error) {
			return client.ExtendedQuery(stepCtx, "insert into users (id, name, email) values ($1, $2, $3) on conflict (email) do update set name = excluded.name returning id, name, email", sample.ConflictID, sample.ConflictName, sample.UpsertedEmail)
		}, func(result pgclient.Result) error {
			return expectOneRow(result, "INSERT 0 1", []string{formatInt(sample.UserID), sample.ConflictName, sample.UpsertedEmail})
		}) {
			continue
		}
		if !runPreparedOp(ctx, cfg, stats["scan_users"], func(stepCtx context.Context) (pgclient.Result, error) {
			return client.ExtendedQuery(stepCtx, "select id, name from users where id >= $1 and id < $2 order by id desc limit $3", sample.UserID, sample.UserID+1, int64(1))
		}, func(result pgclient.Result) error {
			return expectOneRow(result, "SELECT 1", []string{formatInt(sample.UserID), sample.ConflictName})
		}) {
			continue
		}
		if !runPreparedOp(ctx, cfg, stats["delete_user"], func(stepCtx context.Context) (pgclient.Result, error) {
			return client.ExtendedQuery(stepCtx, "delete from users where id = $1 returning id, name, email", sample.UserID)
		}, func(result pgclient.Result) error {
			return expectOneRow(result, "DELETE 1", []string{formatInt(sample.UserID), sample.ConflictName, sample.UpsertedEmail})
		}) {
			continue
		}
		runPreparedOp(ctx, cfg, stats["verify_absent"], func(stepCtx context.Context) (pgclient.Result, error) {
			return client.ExtendedQuery(stepCtx, "select id, name from users where id = $1", sample.UserID)
		}, func(result pgclient.Result) error {
			if result.CommandTag != "SELECT 0" {
				return fmt.Errorf("command tag = %q, want SELECT 0", result.CommandTag)
			}
			if len(result.Rows) != 0 {
				return fmt.Errorf("rows = %#v, want none", result.Rows)
			}
			return nil
		})
	}

	report := Report{
		Addr:       cfg.Addr,
		User:       cfg.User,
		Iterations: cfg.Iterations,
		StartedAt:  startedAt,
		FinishedAt: time.Now().UTC(),
		Operations: summarizeStats(stats),
	}
	report.Compatible = true
	for _, summary := range report.Operations {
		report.TotalFailures += summary.Failures
		if summary.Successes != cfg.Iterations {
			report.Compatible = false
			report.MissingOperations = append(report.MissingOperations, summary.Name)
		}
	}
	return report, nil
}

// SummaryString renders a compact human-readable workload summary.
func SummaryString(report Report) string {
	var builder strings.Builder
	status := "PASS"
	if !report.Compatible {
		status = "FAIL"
	}
	fmt.Fprintf(&builder, "ChronosDB app-compat workload: %s\n", status)
	fmt.Fprintf(&builder, "addr=%s iterations=%d failures=%d\n", report.Addr, report.Iterations, report.TotalFailures)
	for _, op := range report.Operations {
		fmt.Fprintf(&builder, "- %s: ok=%d/%d fail=%d avg=%.2fms p95=%.2fms max=%.2fms",
			op.Name, op.Successes, op.Count, op.Failures, op.AverageMs, op.P95Ms, op.MaxMs)
		if len(op.ErrorClasses) > 0 {
			fmt.Fprintf(&builder, " errors=%v", op.ErrorClasses)
		}
		if op.LastError != "" {
			fmt.Fprintf(&builder, " last_error=%q", op.LastError)
		}
		builder.WriteByte('\n')
	}
	if len(report.MissingOperations) > 0 {
		fmt.Fprintf(&builder, "missing-success coverage: %s\n", strings.Join(report.MissingOperations, ", "))
	}
	return builder.String()
}

func applyDefaults(cfg Config) Config {
	if cfg.User == "" {
		cfg.User = defaultUser
	}
	if cfg.Iterations <= 0 {
		cfg.Iterations = defaultIterations
	}
	if cfg.StatementTimeout <= 0 {
		cfg.StatementTimeout = defaultStatementTimeout
	}
	if cfg.BaseUserID == 0 {
		cfg.BaseUserID = defaultBaseUserID
	}
	if cfg.BaseConflictID == 0 {
		cfg.BaseConflictID = defaultConflictID
	}
	return cfg
}

func buildSample(cfg Config, iteration int) workloadSample {
	userID := cfg.BaseUserID + int64(iteration)
	return workloadSample{
		UserID:        userID,
		ConflictID:    cfg.BaseConflictID + int64(iteration),
		Name:          fmt.Sprintf("compat-%d", userID),
		Email:         fmt.Sprintf("compat-%d@example.com", userID),
		UpdatedName:   fmt.Sprintf("updated-%d", userID),
		UpdatedEmail:  fmt.Sprintf("updated-%d@example.com", userID),
		UpsertedName:  fmt.Sprintf("upserted-%d", userID),
		UpsertedEmail: fmt.Sprintf("upserted-%d@example.com", userID),
		TxName:        fmt.Sprintf("tx-%d", userID),
		ConflictName:  fmt.Sprintf("conflict-%d", userID),
	}
}

func runPreparedOp(
	parent context.Context,
	cfg Config,
	stats *opStats,
	run func(context.Context) (pgclient.Result, error),
	validate func(pgclient.Result) error,
) bool {
	stats.count++
	stepCtx, cancel := context.WithTimeout(parent, cfg.StatementTimeout)
	startedAt := time.Now()
	result, err := run(stepCtx)
	cancel()
	stats.durations = append(stats.durations, time.Since(startedAt))
	if err == nil && validate != nil {
		err = validate(result)
	}
	if err != nil {
		stats.failures++
		stats.lastError = err.Error()
		stats.errorClasses[classifyError(err)]++
		return false
	}
	stats.successes++
	if result.CommandTag != "" {
		stats.commandTags[result.CommandTag]++
	}
	return true
}

func classifyError(err error) string {
	var serverErr pgclient.ServerError
	if errors.As(err, &serverErr) {
		if class := serverErr.Class(); class != "" {
			return class
		}
		if serverErr.Code != "" {
			return serverErr.Code
		}
		return "server"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "client"
}

func summarizeStats(stats map[string]*opStats) []OperationSummary {
	out := make([]OperationSummary, 0, len(operationOrder))
	for _, name := range operationOrder {
		stat := stats[name]
		if stat == nil {
			continue
		}
		out = append(out, OperationSummary{
			Name:         name,
			Count:        stat.count,
			Successes:    stat.successes,
			Failures:     stat.failures,
			CommandTags:  cloneIntMap(stat.commandTags),
			ErrorClasses: cloneIntMap(stat.errorClasses),
			LastError:    stat.lastError,
			AverageMs:    averageDurationMs(stat.durations),
			P50Ms:        percentileDurationMs(stat.durations, 0.50),
			P95Ms:        percentileDurationMs(stat.durations, 0.95),
			MaxMs:        maxDurationMs(stat.durations),
		})
	}
	return out
}

func expectOneRow(result pgclient.Result, commandTag string, want []string) error {
	if result.CommandTag != commandTag {
		return fmt.Errorf("command tag = %q, want %q", result.CommandTag, commandTag)
	}
	if len(result.Rows) != 1 {
		return fmt.Errorf("rows = %d, want 1", len(result.Rows))
	}
	row := result.Rows[0]
	if len(row) != len(want) {
		return fmt.Errorf("row width = %d, want %d", len(row), len(want))
	}
	for i := range want {
		if row[i] != want[i] {
			return fmt.Errorf("row[%d] = %q, want %q", i, row[i], want[i])
		}
	}
	return nil
}

func cloneIntMap(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func averageDurationMs(samples []time.Duration) float64 {
	if len(samples) == 0 {
		return 0
	}
	var total time.Duration
	for _, sample := range samples {
		total += sample
	}
	return float64(total) / float64(len(samples)) / float64(time.Millisecond)
}

func percentileDurationMs(samples []time.Duration, percentile float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	if percentile <= 0 {
		percentile = 0
	} else if percentile > 1 {
		percentile = 1
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(float64(len(sorted)-1) * percentile)
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return float64(sorted[index]) / float64(time.Millisecond)
}

func maxDurationMs(samples []time.Duration) float64 {
	if len(samples) == 0 {
		return 0
	}
	max := samples[0]
	for _, sample := range samples[1:] {
		if sample > max {
			max = sample
		}
	}
	return float64(max) / float64(time.Millisecond)
}

func formatInt(value int64) string {
	return fmt.Sprintf("%d", value)
}
