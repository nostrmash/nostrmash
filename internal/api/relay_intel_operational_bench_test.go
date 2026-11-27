//go:build relayintelbench

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

// TestOperationalRelayIntelligence measures populated Relay Intelligence
// workloads. It does not run in CI. Reproduce with:
//
//	scripts/relay_intel_operational_bench.sh
//
// RELAY_INTEL_OPERATIONAL_SCALE is small, medium, or both (default both).
func TestOperationalRelayIntelligence(t *testing.T) {
	if os.Getenv("RELAY_INTEL_OPERATIONAL_BENCH") == "" {
		t.Skip("set RELAY_INTEL_OPERATIONAL_BENCH=1 to run the operational benchmark")
	}
	scales := operationalScales()
	outDir := operationalOutDir()
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, accounts := range scales {
		t.Run(fmt.Sprintf("accounts_%d", accounts), func(t *testing.T) {
			runOperationalScale(t, accounts, outDir)
		})
	}
}

// TestOperationalProbeCompletion measures the atomic completion path without
// the account-scale fixture. It stays skipped unless the probe-only variable
// is set, so the account-scale command does not run it twice.
func TestOperationalProbeCompletion(t *testing.T) {
	if os.Getenv("RELAY_INTEL_OPERATIONAL_PROBE_ONLY") == "" {
		t.Skip("set RELAY_INTEL_OPERATIONAL_PROBE_ONLY=1 to measure atomic completion without the account-scale fixture")
	}
	ctx := context.Background()
	pool, schema := openOperationalPool(t, ctx, &queryCounter{})
	if err := dbmigrate.Migrate(ctx, pool, "bench"); err != nil {
		t.Fatal(err)
	}
	out := map[string]any{
		"generated_at":          time.Now().UTC().Format(time.RFC3339),
		"commit":                strings.TrimSpace(commandOutput("git", "rev-parse", "HEAD")),
		"implementation_commit": strings.TrimSpace(commandOutput("git", "rev-parse", "origin/feat/relay-capability-probes")),
		"core_commit":           reviewedCoreCommit,
		"core_incorporated":     commandOK("git", "merge-base", "--is-ancestor", reviewedCoreCommit, "HEAD"),
		"schema":                schema,
		"scope":                 "Atomic completion on an empty disposable schema. This is not the account-scale benchmark.",
	}
	measureAtomicCompletion(t, ctx, pool, out)
	dir := operationalOutDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	raw, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "probe-completion.json")
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", path)
}

func operationalScales() []int {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("RELAY_INTEL_OPERATIONAL_SCALE"))) {
	case "small":
		return []int{10000}
	case "medium":
		return []int{100000}
	case "", "both":
		return []int{10000, 100000}
	default:
		return []int{10000, 100000}
	}
}

func operationalOutDir() string {
	dir := strings.TrimSpace(os.Getenv("RELAY_INTEL_OPERATIONAL_OUT"))
	if dir == "" {
		dir = filepath.Join("benchmarks", "relay-intelligence", "operational")
	}
	if filepath.IsAbs(dir) {
		return dir
	}
	root := strings.TrimSpace(commandOutput("git", "rev-parse", "--show-toplevel"))
	if root == "" {
		return dir
	}
	return filepath.Join(root, dir)
}

func operationalIntEnv(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return fallback
	}
	return n
}

type queryCounter struct {
	n atomic.Int64
}

func (q *queryCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	q.n.Add(1)
	return ctx
}

func (q *queryCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (q *queryCounter) snapshot() int64 { return q.n.Load() }

// reviewedCoreCommit is the retention-index and membership-metadata commit.
// The final account-scale run requires this commit in the capability branch.
const reviewedCoreCommit = "31df4c808129f633593d7bcd605cb178dad40727"

type opReport struct {
	mu                   sync.Mutex
	path                 string
	GeneratedAt          string         `json:"generated_at"`
	Commit               string         `json:"commit"`
	ImplementationCommit string         `json:"implementation_commit"`
	CoreCommit           string         `json:"core_commit"`
	CoreIncorporated     bool           `json:"core_incorporated"`
	Scale                string         `json:"scale"`
	Accounts             int            `json:"accounts"`
	Environment          map[string]any `json:"environment"`
	Fixture              map[string]any `json:"fixture,omitempty"`
	API                  map[string]any `json:"api,omitempty"`
	Maintenance          map[string]any `json:"maintenance,omitempty"`
	Probes               map[string]any `json:"probes,omitempty"`
	Synthetic            map[string]any `json:"synthetic,omitempty"`
	Storage              map[string]any `json:"storage,omitempty"`
	WAL                  map[string]any `json:"wal,omitempty"`
	Mixed                []any          `json:"mixed,omitempty"`
	Plans                []string       `json:"plans,omitempty"`
	Notes                []string       `json:"notes,omitempty"`
}

func newOpReport(path, scale string, accounts int) *opReport {
	commit := strings.TrimSpace(commandOutput("git", "rev-parse", "HEAD"))
	implementation := strings.TrimSpace(commandOutput("git", "rev-parse", "origin/feat/relay-capability-probes"))
	return &opReport{
		path:                 path,
		GeneratedAt:          time.Now().UTC().Format(time.RFC3339),
		Commit:               commit,
		ImplementationCommit: implementation,
		CoreCommit:           reviewedCoreCommit,
		CoreIncorporated:     commandOK("git", "merge-base", "--is-ancestor", reviewedCoreCommit, "HEAD"),
		Scale:                scale,
		Accounts:             accounts,
		Environment:          map[string]any{},
		API:                  map[string]any{},
		Maintenance:          map[string]any{},
		Probes:               map[string]any{},
		Synthetic:            map[string]any{},
		Storage:              map[string]any{},
		WAL:                  map[string]any{},
		Notes:                []string{},
	}
}

func (r *opReport) note(text string) {
	r.mu.Lock()
	r.Notes = append(r.Notes, text)
	r.mu.Unlock()
	r.save()
}

func (r *opReport) save() {
	r.mu.Lock()
	defer r.mu.Unlock()
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(r.path, append(raw, '\n'), 0o644)
}

type latencySummary struct {
	Name            string  `json:"name"`
	Samples         int     `json:"samples"`
	Errors          int     `json:"errors"`
	Meaningful      int     `json:"meaningful"`
	ErrorRate       float64 `json:"error_rate"`
	MedianMS        float64 `json:"median_ms,omitempty"`
	P95MS           float64 `json:"p95_ms,omitempty"`
	P99MS           float64 `json:"p99_ms,omitempty"`
	MaxMS           float64 `json:"max_ms,omitempty"`
	ThroughputPerS  float64 `json:"throughput_per_s,omitempty"`
	QueryMedian     float64 `json:"query_count_median,omitempty"`
	AllocBytesPerOp uint64  `json:"alloc_bytes_per_op,omitempty"`
	CPUSeconds      float64 `json:"cpu_seconds,omitempty"`
	Cache           string  `json:"cache"`
	PostgresCache   string  `json:"postgres_cache"`
	Note            string  `json:"note,omitempty"`
	Proof           any     `json:"proof,omitempty"`
}

func summarizeLatency(name, cache, pgCache string, samples []time.Duration, errors, meaningful int, queries []int, elapsed time.Duration, allocPerOp uint64, cpu time.Duration, note string, proof any) latencySummary {
	out := latencySummary{
		Name:            name,
		Samples:         len(samples) + errors,
		Errors:          errors,
		Meaningful:      meaningful,
		Cache:           cache,
		PostgresCache:   pgCache,
		Note:            note,
		Proof:           proof,
		AllocBytesPerOp: allocPerOp,
	}
	total := len(samples) + errors
	if total > 0 {
		out.ErrorRate = float64(errors) / float64(total)
	}
	if len(samples) == 0 {
		return out
	}
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	out.MedianMS = ms(ordered[len(ordered)/2])
	out.P95MS = ms(ordered[percentileIndex(len(ordered), 0.95)])
	if len(ordered) >= 100 {
		out.P99MS = ms(ordered[percentileIndex(len(ordered), 0.99)])
	}
	out.MaxMS = ms(ordered[len(ordered)-1])
	if elapsed > 0 {
		out.ThroughputPerS = float64(len(samples)) / elapsed.Seconds()
	}
	if len(queries) > 0 {
		qs := append([]int(nil), queries...)
		sort.Ints(qs)
		out.QueryMedian = float64(qs[len(qs)/2])
	}
	if cpu > 0 {
		out.CPUSeconds = cpu.Seconds()
	}
	return out
}

func percentileIndex(n int, p float64) int {
	if n <= 1 {
		return 0
	}
	idx := int(float64(n-1) * p)
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func cpuNow() time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return time.Duration(usage.Utime.Sec)*time.Second + time.Duration(usage.Utime.Usec)*time.Microsecond +
		time.Duration(usage.Stime.Sec)*time.Second + time.Duration(usage.Stime.Usec)*time.Microsecond
}

func commandOutput(name string, args ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return string(out)
}

func commandOK(name string, args ...string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, name, args...).Run() == nil
}

func openOperationalPool(t *testing.T, ctx context.Context, counter *queryCounter) (*pgxpool.Pool, string) {
	t.Helper()
	dbURL := dbtest.DatabaseURL(t, "operational relay intelligence")
	admin, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("bench_ri_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, pgx.Identifier{schema}.Sanitize())); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = time.Minute
	cfg.ConnConfig.Tracer = counter
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		quoted := pgx.Identifier{schema}.Sanitize()
		tx, err := admin.Begin(dropCtx)
		if err == nil {
			_, _ = tx.Exec(dropCtx, `SELECT pg_advisory_xact_lock($1)`, dbmigrate.MigrationAdvisoryLockID)
			_, _ = tx.Exec(dropCtx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, quoted))
			_ = tx.Commit(dropCtx)
		}
		admin.Close()
	})
	return pool, schema
}

func collectEnvironment(ctx context.Context, pool *pgxpool.Pool, schema string) map[string]any {
	env := map[string]any{
		"go":             runtime.Version(),
		"goos":           runtime.GOOS,
		"goarch":         runtime.GOARCH,
		"gomaxprocs":     runtime.GOMAXPROCS(0),
		"cpu_brand":      strings.TrimSpace(commandOutput("sysctl", "-n", "machdep.cpu.brand_string")),
		"logical_cpus":   strings.TrimSpace(commandOutput("sysctl", "-n", "hw.logicalcpu")),
		"physical_cpus":  strings.TrimSpace(commandOutput("sysctl", "-n", "hw.physicalcpu")),
		"memsize_bytes":  strings.TrimSpace(commandOutput("sysctl", "-n", "hw.memsize")),
		"os":             strings.TrimSpace(commandOutput("sw_vers", "-productVersion")),
		"schema":         schema,
		"pool_max_conns": 20,
		"pool_min_conns": 2,
	}
	var version, shared, work, maint, effective, maxConn, syncCommit, walCompression, maxWal string
	_ = pool.QueryRow(ctx, `SHOW server_version`).Scan(&version)
	_ = pool.QueryRow(ctx, `SHOW shared_buffers`).Scan(&shared)
	_ = pool.QueryRow(ctx, `SHOW work_mem`).Scan(&work)
	_ = pool.QueryRow(ctx, `SHOW maintenance_work_mem`).Scan(&maint)
	_ = pool.QueryRow(ctx, `SHOW effective_cache_size`).Scan(&effective)
	_ = pool.QueryRow(ctx, `SHOW max_connections`).Scan(&maxConn)
	_ = pool.QueryRow(ctx, `SHOW synchronous_commit`).Scan(&syncCommit)
	_ = pool.QueryRow(ctx, `SHOW wal_compression`).Scan(&walCompression)
	_ = pool.QueryRow(ctx, `SHOW max_wal_size`).Scan(&maxWal)
	var others int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()`).Scan(&others)
	var dbSize string
	_ = pool.QueryRow(ctx, `SELECT pg_size_pretty(pg_database_size(current_database()))`).Scan(&dbSize)
	env["postgres_version"] = version
	env["shared_buffers"] = shared
	env["work_mem"] = work
	env["maintenance_work_mem"] = maint
	env["effective_cache_size"] = effective
	env["max_connections"] = maxConn
	env["synchronous_commit"] = syncCommit
	env["wal_compression"] = walCompression
	env["max_wal_size"] = maxWal
	env["other_sessions"] = others
	env["database_size_before"] = dbSize
	env["postgres_cache_note"] = "API timings are not cold-database measurements. Shared buffers stay warm from seeding and ANALYZE. Buffer hits versus reads are in the execution plans."
	dockerCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	docker := exec.CommandContext(dockerCtx, "docker", "stats", "--no-stream", "--format", "{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}")
	if out, err := docker.Output(); err != nil {
		env["docker_stats"] = "unavailable: " + err.Error()
		env["docker_vm"] = "Docker Desktop VM from the running virtualization process: 14 CPUs, 8092 MiB. The compose file sets no per-container memory or CPU limit, so Postgres is limited by that VM."
	} else {
		env["docker_stats"] = strings.TrimSpace(string(out))
	}
	return env
}

func walLSN(ctx context.Context, pool *pgxpool.Pool) string {
	var lsn string
	if err := pool.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		return ""
	}
	return lsn
}

func walBytes(ctx context.Context, pool *pgxpool.Pool, start, end string) int64 {
	if start == "" || end == "" {
		return -1
	}
	var n int64
	if err := pool.QueryRow(ctx, `SELECT pg_wal_lsn_diff($1::pg_lsn, $2::pg_lsn)`, end, start).Scan(&n); err != nil {
		return -1
	}
	return n
}

func relationSizes(ctx context.Context, pool *pgxpool.Pool) []map[string]any {
	rows, err := pool.Query(ctx, `
		SELECT c.relname,
		       pg_total_relation_size(c.oid),
		       pg_relation_size(c.oid),
		       pg_indexes_size(c.oid),
		       COALESCE(c.reltuples, 0)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema()
		  AND c.relkind = 'r'
		  AND c.relname NOT LIKE 'bench_%'
		ORDER BY pg_total_relation_size(c.oid) DESC
		LIMIT 40
	`)
	if err != nil {
		return []map[string]any{{"error": err.Error()}}
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var name string
		var total, heap, indexes int64
		var tuples float64
		if err := rows.Scan(&name, &total, &heap, &indexes, &tuples); err != nil {
			return append(out, map[string]any{"error": err.Error()})
		}
		out = append(out, map[string]any{
			"relation": name, "total_bytes": total, "heap_bytes": heap, "index_bytes": indexes, "reltuples": tuples,
		})
	}
	return out
}
