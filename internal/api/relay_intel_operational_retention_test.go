//go:build relayintelbench

package api

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/relayintel"
)

// purgeSupersededSQL is the statement executed by retentiondb.PurgeSupersededReplaceableEvents.
// Parameter order matches the generated query: limit, kinds, superseded_before, dead_grace_before.
const purgeSupersededSQL = `
WITH unqueue_winners AS (
    DELETE FROM replaceable_event_purge p
    WHERE EXISTS (
        SELECT 1 FROM replaceable_state rs
        WHERE rs.event_id = p.event_id
    )
    RETURNING p.event_id
),
gone AS (
    SELECT p.event_id
    FROM replaceable_event_purge p
    WHERE NOT EXISTS (
        SELECT 1 FROM events e WHERE e.id = p.event_id
    )
    LIMIT $1
),
removed_gone AS (
    DELETE FROM replaceable_event_purge p
    USING gone g
    WHERE p.event_id = g.event_id
),
candidates AS (
    SELECT p.event_id
    FROM replaceable_event_purge p
    JOIN events e ON e.id = p.event_id
    WHERE e.kind = ANY($2::int[])
      AND e.first_seen_at < $3
      AND NOT EXISTS (
        SELECT 1 FROM replaceable_state rs
        WHERE rs.event_id = p.event_id
      )
      AND NOT EXISTS (
        SELECT 1 FROM unqueue_winners uw
        WHERE uw.event_id = p.event_id
      )
      AND NOT EXISTS (
        SELECT 1
        FROM jobs j
        WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
          AND (
            j.status IN ('pending', 'running')
            OR (j.status = 'dead' AND j.updated_at > $4)
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM replaceable_state rs
        WHERE rs.pubkey = e.pubkey
          AND rs.kind = e.kind
          AND rs.created_at = e.created_at
          AND rs.event_id > e.id
          AND (
            (e.kind >= 30000 AND e.kind < 40000 AND rs.d_tag = COALESCE((
                SELECT et.value
                FROM event_tags et
                WHERE et.event_id = e.id
                  AND et.tag_name = 'd'
                  AND et.value_index = 0
                ORDER BY et.tag_index ASC
                LIMIT 1
            ), ''))
            OR e.kind < 30000
            OR e.kind >= 40000
          )
      )
      AND NOT EXISTS (
        SELECT 1
        FROM relay_intel_repairs repair
        WHERE repair.winner_event_id = e.id
          AND repair.status IN ('pending', 'queued', 'running')
          AND (
            EXISTS (
                SELECT 1
                FROM replaceable_state rs
                WHERE rs.pubkey = repair.pubkey
                  AND rs.kind = repair.kind
                  AND rs.d_tag = repair.d_tag
                  AND rs.event_id IN (repair.winner_event_id, repair.loser_event_id)
            )
            OR NOT EXISTS (
                SELECT 1
                FROM replaceable_state rs
                WHERE rs.pubkey = repair.pubkey
                  AND rs.kind = repair.kind
                  AND rs.d_tag = repair.d_tag
            )
          )
      )
    ORDER BY p.created_at ASC, p.event_id ASC
    LIMIT $1
),
deleted_events AS (
    DELETE FROM events e
    USING candidates c
    WHERE e.id = c.event_id
)
DELETE FROM replaceable_event_purge p
USING candidates c
WHERE p.event_id = c.event_id
`

func retentionKinds() []int32 {
	return []int32{
		0, 3, 10000, 10001, 10002, 10003, 10006, 10007, 10009, 10012, 10013, 10015, 10019, 10030, 10050, 10051, 10063, 10102,
		30000, 30002, 30004, 30005, 30008, 30009, 30015, 30023, 30311, 30315, 34235, 34236,
		39000, 39001, 39002, 39003,
	}
}

func measureRetentionBreakdown(t *testing.T, ctx context.Context, pool *pgxpool.Pool, planPath string) map[string]any {
	t.Helper()
	t.Log("retention execution plans")
	before := time.Now().Add(-time.Hour)
	kinds := retentionKinds()
	out := map[string]any{
		"label": "diagnostic measurements with individual checks removed are not a deployable configuration",
	}
	out["authoritative_winner_protection"] = timeCount(ctx, pool, `
		SELECT count(*) FROM replaceable_event_purge p
		WHERE EXISTS (SELECT 1 FROM replaceable_state rs WHERE rs.event_id = p.event_id)`)
	out["candidate_selection"] = timeCount(ctx, pool, `
		SELECT count(*) FROM (
			SELECT p.event_id
			FROM replaceable_event_purge p
			JOIN events e ON e.id = p.event_id
			WHERE e.kind = ANY($1::int[]) AND e.first_seen_at < $2
			  AND NOT EXISTS (SELECT 1 FROM replaceable_state rs WHERE rs.event_id = p.event_id)
			  AND NOT EXISTS (
			    SELECT 1 FROM jobs j
			    WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
			      AND (j.status IN ('pending', 'running') OR (j.status = 'dead' AND j.updated_at > $3))
			  )
			  AND NOT EXISTS (
			    SELECT 1 FROM replaceable_state rs
			    WHERE rs.pubkey = e.pubkey AND rs.kind = e.kind AND rs.created_at = e.created_at AND rs.event_id > e.id
			  )
			  AND NOT EXISTS (
			    SELECT 1 FROM relay_intel_repairs repair
			    WHERE repair.winner_event_id = e.id AND repair.status IN ('pending', 'queued', 'running')
			  )
			ORDER BY p.created_at, p.event_id
			LIMIT 100
		) selected`, kinds, before, before)
	out["pending_derivation_protection"] = timeCount(ctx, pool, `
		SELECT count(*) FROM replaceable_event_purge p
		JOIN events e ON e.id = p.event_id
		WHERE EXISTS (
			SELECT 1 FROM jobs j
			WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
			  AND (j.status IN ('pending', 'running') OR (j.status = 'dead' AND j.updated_at > $1))
		)`, before)
	out["pending_repair_protection"] = timeCount(ctx, pool, `
		SELECT count(*) FROM replaceable_event_purge p
		JOIN events e ON e.id = p.event_id
		WHERE EXISTS (
			SELECT 1 FROM relay_intel_repairs repair
			WHERE repair.winner_event_id = e.id AND repair.status IN ('pending', 'queued', 'running')
		)`)
	out["diagnostic_without_job_or_repair_check"] = timeCount(ctx, pool, `
		SELECT count(*) FROM replaceable_event_purge p
		JOIN events e ON e.id = p.event_id
		WHERE e.kind = ANY($1::int[]) AND e.first_seen_at < $2
		  AND NOT EXISTS (SELECT 1 FROM replaceable_state rs WHERE rs.event_id = p.event_id)`, kinds, before)
	out["diagnostic_without_repair_check"] = timeCount(ctx, pool, `
		SELECT count(*) FROM replaceable_event_purge p
		JOIN events e ON e.id = p.event_id
		WHERE e.kind = ANY($1::int[]) AND e.first_seen_at < $2
		  AND NOT EXISTS (SELECT 1 FROM replaceable_state rs WHERE rs.event_id = p.event_id)
		  AND NOT EXISTS (
		    SELECT 1 FROM jobs j
		    WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
		      AND (j.status IN ('pending', 'running') OR (j.status = 'dead' AND j.updated_at > $3))
		  )`, kinds, before, before)
	out["dependent_rows_for_next_100"] = timeDependentRows(ctx, pool)
	missing := missingEventFKIndexes(ctx, pool)
	out["events_fk_without_leading_index"] = missing

	var plan strings.Builder
	plan.WriteString("\n## replaceable retention purge\n")
	plan.WriteString("Rolled back. This is EXPLAIN (ANALYZE, BUFFERS, WAL) of the production purge statement.\n")
	started := time.Now()
	text, err := explainRollback(ctx, pool, "EXPLAIN (ANALYZE, BUFFERS, WAL) "+purgeSupersededSQL, int32(100), kinds, before, before)
	plan.WriteString(text)
	if err != nil {
		fmt.Fprintf(&plan, "error: %v\n", err)
		out["purge_plan_error"] = err.Error()
	}
	out["purge_plan_wall_ms"] = time.Since(started).Milliseconds()
	out["purge_plan_trigger_lines"] = triggerLines(text)
	plan.WriteString("\n## diagnostic candidate selection without deletes\n")
	selection, selErr := explainRollback(ctx, pool, `
		EXPLAIN (ANALYZE, BUFFERS)
		SELECT p.event_id
		FROM replaceable_event_purge p
		JOIN events e ON e.id = p.event_id
		WHERE e.kind = ANY($1::int[]) AND e.first_seen_at < $2
		  AND NOT EXISTS (SELECT 1 FROM replaceable_state rs WHERE rs.event_id = p.event_id)
		ORDER BY p.created_at, p.event_id
		LIMIT 100`, kinds, before)
	plan.WriteString(selection)
	if selErr != nil {
		fmt.Fprintf(&plan, "error: %v\n", selErr)
	}
	if err := os.WriteFile(planPath, appendFile(planPath, plan.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	cause := "The production purge plan is in the plans file. Compare purge_plan_wall_ms with candidate_selection."
	proposal := "Keep the winner, repair, and derivation-job checks. Change only the operation that dominates purge_plan_wall_ms."
	if len(missing) > 0 {
		cause = "Foreign keys to events(id) have no leading index, so each deleted event can seq-scan those child tables inside the cascade triggers."
		proposal = "Add a leading index on each listed foreign-key column. Do not remove the winner, repair, or derivation-job checks, and do not ship the diagnostic queries that omit them."
	}
	out["suspected_root_cause"] = cause
	out["proposed_minimal_correction"] = proposal
	out["reproduction"] = "RELAY_INTEL_OPERATIONAL_BENCH=1 go test -tags relayintelbench ./internal/api/ -run TestOperationalRelayIntelligence -count=1"
	return out
}

func timeCount(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) map[string]any {
	started := time.Now()
	var n int64
	err := pool.QueryRow(ctx, sql, args...).Scan(&n)
	result := map[string]any{"wall_ms": time.Since(started).Milliseconds(), "rows": n}
	if err != nil {
		result["error"] = err.Error()
	}
	return result
}

func timeDependentRows(ctx context.Context, pool *pgxpool.Pool) map[string]any {
	started := time.Now()
	var tags, relays, follows, memberships, heads int64
	err := pool.QueryRow(ctx, `
		WITH picked AS (
			SELECT event_id FROM replaceable_event_purge
			ORDER BY created_at, event_id
			LIMIT 100
		)
		SELECT
			(SELECT count(*) FROM event_tags t JOIN picked p ON p.event_id = t.event_id),
			(SELECT count(*) FROM event_relays r JOIN picked p ON p.event_id = r.event_id),
			(SELECT count(*) FROM follower_edges f JOIN picked p ON p.event_id = f.source_event_id),
			(SELECT count(*) FROM relay_role_memberships m JOIN picked p ON p.event_id = m.source_event_id),
			(SELECT count(*) FROM relay_config_heads h JOIN picked p ON p.event_id = h.event_id)
	`).Scan(&tags, &relays, &follows, &memberships, &heads)
	result := map[string]any{
		"wall_ms":    time.Since(started).Milliseconds(),
		"event_tags": tags, "event_relays": relays, "follower_edges": follows,
		"relay_role_memberships": memberships, "relay_config_heads": heads,
	}
	if err != nil {
		result["error"] = err.Error()
	}
	return result
}

func missingEventFKIndexes(ctx context.Context, pool *pgxpool.Pool) []string {
	rows, err := pool.Query(ctx, `
		SELECT c.conrelid::regclass::text, a.attname
		FROM pg_constraint c
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
		WHERE c.contype = 'f'
		  AND c.confrelid = 'events'::regclass
		  AND NOT EXISTS (
		    SELECT 1 FROM pg_index i
		    WHERE i.indrelid = c.conrelid
		      AND i.indkey[0] = c.conkey[1]
		  )
		ORDER BY 1, 2`)
	if err != nil {
		return []string{"error: " + err.Error()}
	}
	defer rows.Close()
	var missing []string
	for rows.Next() {
		var table, column string
		if rows.Scan(&table, &column) == nil {
			missing = append(missing, table+"."+column)
		}
	}
	if missing == nil {
		missing = []string{}
	}
	return missing
}

func explainRollback(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return b.String(), err
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		return b.String(), err
	}
	return b.String(), tx.Rollback(ctx)
}

func triggerLines(plan string) []string {
	var lines []string
	for _, line := range strings.Split(plan, "\n") {
		if strings.Contains(line, "Trigger") || strings.Contains(line, "Execution Time") || strings.Contains(line, "Planning Time") {
			lines = append(lines, strings.TrimSpace(line))
		}
	}
	return lines
}

func appendFile(path, extra string) []byte {
	existing, _ := os.ReadFile(path)
	return append(existing, []byte(extra)...)
}

type retentionIndex struct {
	name   string
	table  string
	column string
	lock   string
}

func retentionEventIndexes() []retentionIndex {
	return []retentionIndex{
		{name: "idx_relay_role_memberships_source_event", table: "relay_role_memberships", column: "source_event_id",
			lock: `UPDATE relay_role_memberships SET marker = marker WHERE ctid = (SELECT ctid FROM relay_role_memberships LIMIT 1)`},
		{name: "idx_relay_config_heads_event_id", table: "relay_config_heads", column: "event_id",
			lock: `UPDATE relay_config_heads SET updated_at = updated_at WHERE ctid = (SELECT ctid FROM relay_config_heads LIMIT 1)`},
		{name: "idx_dm_relay_lists_latest_event_id", table: "dm_relay_lists_latest", column: "event_id"},
		{name: "idx_nutzap_info_latest_event_id", table: "nutzap_info_latest", column: "event_id"},
	}
}

func listRetentionEventIndexes(ctx context.Context, pool *pgxpool.Pool) []string {
	names := make([]string, 0, 4)
	for _, index := range retentionEventIndexes() {
		names = append(names, index.name)
	}
	rows, err := pool.Query(ctx, `
		SELECT indexname FROM pg_indexes
		WHERE schemaname = current_schema() AND indexname = ANY($1::text[])
		ORDER BY indexname`, names)
	if err != nil {
		return []string{"error: " + err.Error()}
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var name string
		if rows.Scan(&name) == nil {
			found = append(found, name)
		}
	}
	if found == nil {
		found = []string{}
	}
	return found
}

func dropRetentionEventIndexes(ctx context.Context, pool *pgxpool.Pool) []string {
	var dropped []string
	for _, index := range retentionEventIndexes() {
		if _, err := pool.Exec(ctx, "DROP INDEX IF EXISTS "+index.name); err != nil {
			dropped = append(dropped, index.name+": "+err.Error())
			continue
		}
		dropped = append(dropped, index.name)
	}
	return dropped
}

func measurePopulatedIndexBuild(ctx context.Context, pool *pgxpool.Pool, planPath string) map[string]any {
	var section strings.Builder
	section.WriteString("\n## populated event-id index build\n")
	section.WriteString("Disposable schema. These statements replay the index definitions from core commit " + reviewedCoreCommit + ". They are not a production migration.\n")
	out := map[string]any{
		"core_commit": reviewedCoreCommit,
		"label":       "Indexes were created on this populated disposable schema. CREATE INDEX takes ShareLock. The membership index is then rebuilt with CREATE INDEX CONCURRENTLY so a writer can be compared.",
	}
	lookups := map[string]string{
		"relay_role_memberships": `
			EXPLAIN (ANALYZE, BUFFERS)
			SELECT count(*) FROM relay_role_memberships
			WHERE source_event_id = (
				SELECT event_id FROM replaceable_event_purge ORDER BY created_at, event_id LIMIT 1
			)`,
		"relay_config_heads": `
			EXPLAIN (ANALYZE, BUFFERS)
			SELECT count(*) FROM relay_config_heads
			WHERE event_id = (
				SELECT event_id FROM replaceable_event_purge ORDER BY created_at, event_id LIMIT 1
			)`,
	}
	before := map[string]any{}
	for table, sql := range lookups {
		plan, err := explainRollback(ctx, pool, sql)
		section.WriteString("\n### lookup before index " + table + "\n")
		section.WriteString(plan)
		item := planFlags(plan)
		if err != nil {
			item["error"] = err.Error()
			fmt.Fprintf(&section, "error: %v\n", err)
		}
		before[table] = item
	}
	out["lookup_before_index"] = before

	wal0 := walLSN(ctx, pool)
	started := time.Now()
	builds := map[string]any{}
	for _, index := range retentionEventIndexes() {
		builds[index.name] = buildIndex(ctx, pool, index)
	}
	out["blocking_create"] = builds
	out["blocking_create_wall_ms"] = time.Since(started).Milliseconds()
	out["blocking_create_wal_bytes"] = walBytes(ctx, pool, wal0, walLSN(ctx, pool))

	after := map[string]any{}
	for table, sql := range lookups {
		plan, err := explainRollback(ctx, pool, sql)
		section.WriteString("\n### lookup after index " + table + "\n")
		section.WriteString(plan)
		item := planFlags(plan)
		if err != nil {
			item["error"] = err.Error()
			fmt.Fprintf(&section, "error: %v\n", err)
		}
		after[table] = item
	}
	out["lookup_after_index"] = after
	out["concurrent_membership_rebuild"] = rebuildMembershipIndexConcurrently(ctx, pool)
	out["index_sizes"] = retentionIndexSizes(ctx, pool)
	if err := os.WriteFile(planPath, appendFile(planPath, section.String()), 0o644); err != nil {
		out["plan_write_error"] = err.Error()
	}
	return out
}

func buildIndex(ctx context.Context, pool *pgxpool.Pool, index retentionIndex) map[string]any {
	wal0 := walLSN(ctx, pool)
	errCh := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := pool.Exec(ctx, fmt.Sprintf("CREATE INDEX %s ON %s (%s)", index.name, index.table, index.column))
		errCh <- err
	}()
	var writeWait int64
	var writeErr string
	if index.lock != "" {
		time.Sleep(100 * time.Millisecond)
		writeStart := time.Now()
		_, err := pool.Exec(ctx, index.lock)
		writeWait = time.Since(writeStart).Milliseconds()
		if err != nil {
			writeErr = err.Error()
		}
	}
	err := <-errCh
	result := map[string]any{
		"wall_ms":       time.Since(started).Milliseconds(),
		"write_wait_ms": writeWait,
		"wal_bytes":     walBytes(ctx, pool, wal0, walLSN(ctx, pool)),
		"bytes":         relationSize(ctx, pool, index.name),
	}
	if writeErr != "" {
		result["write_error"] = writeErr
	}
	if err != nil {
		result["error"] = err.Error()
	}
	return result
}

func rebuildMembershipIndexConcurrently(ctx context.Context, pool *pgxpool.Pool) map[string]any {
	if _, err := pool.Exec(ctx, "DROP INDEX IF EXISTS idx_relay_role_memberships_source_event"); err != nil {
		return map[string]any{"error": err.Error()}
	}
	errCh := make(chan error, 1)
	started := time.Now()
	go func() {
		errCh <- execSimple(ctx, pool, "CREATE INDEX CONCURRENTLY idx_relay_role_memberships_source_event ON relay_role_memberships (source_event_id)")
	}()
	time.Sleep(100 * time.Millisecond)
	writeStart := time.Now()
	_, writeErr := pool.Exec(ctx, `UPDATE relay_role_memberships SET marker = marker WHERE ctid = (SELECT ctid FROM relay_role_memberships LIMIT 1)`)
	writeWait := time.Since(writeStart).Milliseconds()
	err := <-errCh
	result := map[string]any{
		"wall_ms":       time.Since(started).Milliseconds(),
		"write_wait_ms": writeWait,
		"bytes":         relationSize(ctx, pool, "idx_relay_role_memberships_source_event"),
		"note":          "CREATE INDEX CONCURRENTLY uses the simple query protocol. write_wait_ms is one concurrent update of a single membership row.",
	}
	if writeErr != nil {
		result["write_error"] = writeErr.Error()
	}
	if err != nil {
		result["error"] = err.Error()
		_, rebuildErr := pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS idx_relay_role_memberships_source_event ON relay_role_memberships (source_event_id)")
		if rebuildErr != nil {
			result["rebuild_error"] = rebuildErr.Error()
		}
	}
	return result
}

func execSimple(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	reader := conn.Conn().PgConn().Exec(ctx, sql)
	defer reader.Close()
	_, err = reader.ReadAll()
	return err
}

func retentionIndexSizes(ctx context.Context, pool *pgxpool.Pool) map[string]any {
	sizes := map[string]any{}
	var total int64
	for _, index := range retentionEventIndexes() {
		size := relationSize(ctx, pool, index.name)
		sizes[index.name] = size
		if size > 0 {
			total += size
		}
	}
	sizes["total_bytes"] = total
	return sizes
}

func relationSize(ctx context.Context, pool *pgxpool.Pool, name string) int64 {
	var size int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(pg_relation_size(to_regclass($1)), 0)`, name).Scan(&size); err != nil {
		return -1
	}
	return size
}

func planFlags(plan string) map[string]any {
	return map[string]any{
		"seq_scan":       strings.Contains(plan, "Seq Scan"),
		"index_scan":     strings.Contains(plan, "Index Scan") || strings.Contains(plan, "Index Only Scan") || strings.Contains(plan, "Bitmap Index Scan"),
		"execution_time": firstMatchingLine(plan, "Execution Time"),
		"buffers":        bufferTotals(plan),
	}
}

func bufferTotals(plan string) map[string]int {
	hit, read, written := 0, 0, 0
	for _, line := range strings.Split(plan, "\n") {
		hit += intAfter(line, "shared hit=")
		read += intAfter(line, "shared read=")
		written += intAfter(line, "shared written=")
	}
	return map[string]int{"shared_hit": hit, "shared_read": read, "shared_written": written}
}

func scanLines(plan, kind string) []string {
	var lines []string
	for _, line := range strings.Split(plan, "\n") {
		if strings.Contains(line, kind) {
			lines = append(lines, strings.TrimSpace(line))
		}
	}
	if lines == nil {
		lines = []string{}
	}
	return lines
}

func firstMatchingLine(plan, needle string) string {
	for _, line := range strings.Split(plan, "\n") {
		if strings.Contains(line, needle) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

func intAfter(line, key string) int {
	i := strings.Index(line, key)
	if i < 0 {
		return 0
	}
	n := 0
	for _, c := range line[i+len(key):] {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func membershipColumnPresent(ctx context.Context, pool *pgxpool.Pool) bool {
	var present bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = 'relay_config_heads'
			  AND column_name = 'membership_count'
		)`).Scan(&present)
	return err == nil && present
}

func measureMembershipMetadata(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]any {
	t.Helper()
	if !membershipColumnPresent(ctx, pool) {
		return map[string]any{
			"measured":    false,
			"core_commit": reviewedCoreCommit,
			"blocker":     "relay_config_heads.membership_count is absent. Core commit " + reviewedCoreCommit + " is not in this schema, so historical heads were not discovered or processed and no reconciliation throughput is reported.",
		}
	}
	var unresolved int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_config_heads WHERE membership_count < 0`).Scan(&unresolved); err != nil {
		return map[string]any{"measured": false, "error": err.Error()}
	}
	out := map[string]any{
		"measured": true, "unreconciled_heads": unresolved,
		"discovery": "membership_count < 0 is the historical unreconciled state",
	}
	var functionReady bool
	_ = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE p.proname = 'relay_intel_projection_ready' AND n.nspname = current_schema()
		)`).Scan(&functionReady)
	out["projection_ready_function"] = functionReady
	if unresolved == 0 {
		out["note"] = "No head has membership_count < 0. There is no historical set to process."
		return out
	}
	rows, err := pool.Query(ctx, `
		SELECT pubkey, source_kind, d_tag, event_id, created_at
		FROM relay_config_heads
		WHERE membership_count < 0
		ORDER BY pubkey, source_kind, d_tag
		LIMIT 200`)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	defer rows.Close()
	var heads []storedHead
	for rows.Next() {
		var row storedHead
		if err := rows.Scan(&row.pubkey, &row.kind, &row.dTag, &row.eventID, &row.created); err != nil {
			out["error"] = err.Error()
			return out
		}
		heads = append(heads, row)
	}
	if len(heads) == 0 {
		return out
	}
	if functionReady {
		var ready bool
		err := pool.QueryRow(ctx, `SELECT relay_intel_projection_ready($1, $2, $3, $4, true)`,
			heads[0].pubkey, heads[0].kind, heads[0].dTag, heads[0].eventID).Scan(&ready)
		out["sample_ready_before"] = ready
		if err != nil {
			out["sample_ready_before_error"] = err.Error()
		}
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	started := time.Now()
	processed, failures := 0, 0
	interrupted := len(heads)
	var failure string
	for i, head := range heads {
		if i == 20 && len(heads) > 21 {
			cancel()
			interrupted = i
			break
		}
		err := projectStoredHead(child, pool, head)
		if err != nil {
			failures++
			if failure == "" {
				failure = err.Error()
			}
			continue
		}
		processed++
	}
	if interrupted < len(heads) {
		dead, deadCancel := context.WithCancel(ctx)
		deadCancel()
		out["canceled_project_error"] = errorText(projectStoredHead(dead, pool, heads[interrupted]))
	}
	interruptWall := time.Since(started)
	resumeStart := time.Now()
	resumed := 0
	for _, head := range heads[interrupted:] {
		if err := projectStoredHead(ctx, pool, head); err != nil {
			failures++
			if failure == "" {
				failure = err.Error()
			}
			continue
		}
		resumed++
	}
	var still int64
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM relay_config_heads
		WHERE membership_count < 0 AND event_id = ANY($1::text[])`, retentionHeadIDs(heads)).Scan(&still)
	out["batch_limit"] = len(heads)
	out["processed_before_interrupt"] = processed
	out["interrupt_at"] = interrupted
	out["interrupt_wall_ms"] = interruptWall.Milliseconds()
	out["resumed"] = resumed
	out["resume_wall_ms"] = time.Since(resumeStart).Milliseconds()
	out["failures"] = failures
	out["still_unreconciled_in_batch"] = still
	out["reconciled_in_batch"] = int64(len(heads)) - still
	if len(heads) > 0 && interruptWall > 0 {
		out["heads_per_second_before_interrupt"] = float64(processed) / interruptWall.Seconds()
	}
	if failure != "" {
		out["failure"] = failure
	}
	out["accepted"] = failures == 0 && still == 0 && processed+resumed == len(heads) && processed > 0
	if still > 0 {
		out["note"] = "Project ran on the historical heads. membership_count stayed below 0, so this build did not write the membership metadata."
	}
	return out
}

type storedHead struct {
	pubkey, dTag, eventID string
	kind                  int
	created               int64
}

func projectStoredHead(ctx context.Context, pool *pgxpool.Pool, head storedHead) error {
	var content string
	if err := pool.QueryRow(ctx, `SELECT COALESCE(content, '') FROM events WHERE id = $1`, head.eventID).Scan(&content); err != nil {
		return err
	}
	rows, err := pool.Query(ctx, `
		SELECT tag_index, tag_name, value
		FROM event_tags
		WHERE event_id = $1
		ORDER BY tag_index, value_index`, head.eventID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var tags [][]string
	var current = -1
	for rows.Next() {
		var index int
		var name, value string
		if err := rows.Scan(&index, &name, &value); err != nil {
			return err
		}
		if index != current {
			tags = append(tags, []string{name, value})
			current = index
			continue
		}
		tags[len(tags)-1] = append(tags[len(tags)-1], value)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := relayintel.Project(ctx, tx, relayintel.EventInput{
		Pubkey: head.pubkey, EventID: head.eventID, Kind: head.kind,
		CreatedAt: head.created, Content: content, Tags: tags,
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func retentionHeadIDs(heads []storedHead) []string {
	ids := make([]string, len(heads))
	for i, head := range heads {
		ids[i] = head.eventID
	}
	return ids
}
