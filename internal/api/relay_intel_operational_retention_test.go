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
          AND rs.d_tag = CASE
                WHEN e.kind >= 30000 AND e.kind < 40000 THEN COALESCE((
                    SELECT et.value
                    FROM event_tags et
                    WHERE et.event_id = e.id
                      AND et.tag_name = 'd'
                      AND et.value_index = 0
                    ORDER BY et.tag_index ASC
                    LIMIT 1
                ), '')
                ELSE ''
              END
          AND rs.created_at = e.created_at
          AND rs.event_id > e.id
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
	section.WriteString("Disposable schema. These statements replay the index definitions from core commit " + frozenCoreCommit + ". They are not a production migration.\n")
	out := map[string]any{
		"core_commit": frozenCoreCommit,
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
			"core_commit": frozenCoreCommit,
			"blocker":     "relay_config_heads.membership_count is absent. Frozen core " + frozenCoreCommit + " is not in this schema, so historical heads were not discovered or processed.",
		}
	}
	deletedTag, err := pool.Exec(ctx, `
		WITH chosen AS (
			SELECT h.event_id, h.pubkey, h.created_at
			FROM relay_config_heads h
			WHERE h.membership_count = -1
			  AND NOT EXISTS (
			      SELECT 1 FROM deletion_events de
			      WHERE de.target_event_id = h.event_id AND de.deleter_pubkey = h.pubkey
			  )
			ORDER BY h.pubkey, h.source_kind, h.d_tag
			LIMIT 20
		)
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		SELECT 'bd' || substr(md5(event_id), 1, 40), pubkey, event_id, created_at, 1
		FROM chosen
		ON CONFLICT (event_id) DO NOTHING
	`)
	if err != nil {
		return map[string]any{"measured": false, "error": err.Error()}
	}
	unreadableTag, err := pool.Exec(ctx, `
		UPDATE events
		SET raw_json = '{"tags":"not-an-array"}'
		WHERE id = (
			SELECT h.event_id
			FROM relay_config_heads h
			WHERE h.membership_count = -1
			  AND NOT EXISTS (
			      SELECT 1 FROM deletion_events de
			      WHERE de.target_event_id = h.event_id AND de.deleter_pubkey = h.pubkey
			  )
			ORDER BY h.pubkey DESC, h.source_kind DESC, h.d_tag DESC
			LIMIT 1
		)
	`)
	if err != nil {
		return map[string]any{"measured": false, "error": err.Error()}
	}
	before := membershipCensus(ctx, pool)
	beforeStatus, statusErr := relayintel.ReadProjectionReconcileStatus(ctx, pool)
	out := map[string]any{
		"measured":                true,
		"path":                    "BackfillBatch",
		"deleted_heads_marked":    deletedTag.RowsAffected(),
		"unreadable_event_marked": unreadableTag.RowsAffected(),
		"before":                  before,
		"backlog_before":          before["unknown_live"],
		"pending_before":          beforeStatus.PendingEligible,
		"status_error":            errorText(statusErr),
		"note":                    "Seeded heads omit membership_count, so they keep the migration default of -1. BackfillBatch reconciles those heads from the authoritative event. pending_before is 0 until the reconcile cursor exists; backlog_before is the live unknown count. Twenty deleted heads and one unreadable event are marked inside this measurement. Deleted heads stay out of the eligible backlog. The unreadable event must become membership_count -2, which is not an empty configuration.",
	}
	if err := relayintel.ResetProjectionReconcile(ctx, pool); err != nil {
		out["reset_error"] = err.Error()
		return out
	}
	started := time.Now()
	batches := 0
	var failure string
	run := func(c context.Context) error {
		err := relayintel.BackfillBatch(c, pool, 200)
		if err != nil {
			return err
		}
		batches++
		return nil
	}
	for i := 0; i < 2; i++ {
		if err := run(ctx); err != nil {
			failure = err.Error()
			break
		}
	}
	dead, cancel := context.WithCancel(ctx)
	cancel()
	out["canceled_backfill_error"] = errorText(run(dead))
	resumeStart := time.Now()
	stalled := 0
	lastPending := -1
	for failure == "" && time.Since(started) < 90*time.Minute {
		status, err := relayintel.ReadProjectionReconcileStatus(ctx, pool)
		if err != nil {
			failure = err.Error()
			break
		}
		if status.PendingEligible == 0 && status.UpgradeComplete {
			break
		}
		if status.PendingEligible == lastPending {
			stalled++
			if stalled > 5 {
				failure = "reconciliation made no progress"
				break
			}
		} else {
			stalled = 0
		}
		lastPending = status.PendingEligible
		if batches%20 == 0 {
			t.Logf("membership reconciliation batches=%d pending=%d", batches, status.PendingEligible)
		}
		if err := run(ctx); err != nil {
			failure = err.Error()
			break
		}
	}
	steadyStart := time.Now()
	steadyErr := relayintel.BackfillBatch(ctx, pool, 200)
	afterStatus, _ := relayintel.ReadProjectionReconcileStatus(ctx, pool)
	after := membershipCensus(ctx, pool)
	elapsed := time.Since(started)
	processed := before["unknown"] - after["unknown"]
	out["batches"] = batches
	out["wall_ms"] = elapsed.Milliseconds()
	out["resume_wall_ms"] = time.Since(resumeStart).Milliseconds()
	out["after"] = after
	out["pending_after"] = afterStatus.PendingEligible
	out["upgrade_complete"] = afterStatus.UpgradeComplete
	out["integrity_recorded"] = afterStatus.IntegrityRecorded
	out["steady_state_wall_ms"] = time.Since(steadyStart).Milliseconds()
	out["steady_state_error"] = errorText(steadyErr)
	mismatches, checked, digestErr := membershipDigestMismatches(ctx, pool)
	out["digest_mismatches"] = mismatches
	out["digest_heads_checked"] = checked
	out["digest_error"] = digestErr
	out["deleted_configs_rewritten"] = deletedConfigsRewritten(ctx, pool)
	if processed > 0 && elapsed > 0 {
		out["heads_per_second"] = float64(processed) / elapsed.Seconds()
	}
	if failure != "" {
		out["failure"] = failure
	}
	out["accepted"] = failure == "" && digestErr == "" && afterStatus.UpgradeComplete && afterStatus.PendingEligible == 0 &&
		mismatches == 0 && deletedConfigsRewritten(ctx, pool) == 0 &&
		after["unknown_live"] == 0 && after["empty_nonzero"] == 0 && after["truncated_unknown"] == 0 &&
		after["unreadable"] >= 1
	return out
}

func membershipCensus(ctx context.Context, pool *pgxpool.Pool) map[string]int64 {
	var unknown, unreadable, zero, emptyZero, emptyNonzero, truncatedOK, truncatedUnknown, positive, unknownLive int64
	err := pool.QueryRow(ctx, `
		SELECT
			count(*) FILTER (WHERE h.membership_count = -1),
			count(*) FILTER (WHERE h.membership_count = -2),
			count(*) FILTER (WHERE h.membership_count = 0),
			count(*) FILTER (WHERE h.explicitly_empty AND h.membership_count = 0),
			count(*) FILTER (WHERE h.explicitly_empty AND h.membership_count > 0),
			count(*) FILTER (WHERE h.truncated AND h.membership_count >= 0),
			count(*) FILTER (WHERE h.truncated AND h.membership_count < 0),
			count(*) FILTER (WHERE h.membership_count > 0),
			count(*) FILTER (
				WHERE h.membership_count = -1
				  AND EXISTS (SELECT 1 FROM events e WHERE e.id = h.event_id)
				  AND NOT EXISTS (
				      SELECT 1 FROM deletion_events de
				      WHERE de.target_event_id = h.event_id AND de.deleter_pubkey = h.pubkey
				  )
			)
		FROM relay_config_heads h
	`).Scan(&unknown, &unreadable, &zero, &emptyZero, &emptyNonzero, &truncatedOK, &truncatedUnknown, &positive, &unknownLive)
	out := map[string]int64{
		"unknown": unknown, "unreadable": unreadable, "zero": zero,
		"empty_zero": emptyZero, "empty_nonzero": emptyNonzero,
		"truncated_recorded": truncatedOK, "truncated_unknown": truncatedUnknown,
		"positive": positive, "unknown_live": unknownLive,
	}
	if err != nil {
		out["error_sentinel"] = 1
	}
	return out
}

func membershipDigestMismatches(ctx context.Context, pool *pgxpool.Pool) (int64, int64, string) {
	var eligible int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_config_heads WHERE membership_count >= 0`).Scan(&eligible); err != nil {
		return -1, 0, err.Error()
	}
	checked := eligible
	if checked > 400 {
		checked = 400
	}
	var n int64
	err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM (
			SELECT pubkey, source_kind, d_tag, membership_count, membership_digest
			FROM relay_config_heads
			WHERE membership_count >= 0
			ORDER BY pubkey, source_kind, d_tag
			LIMIT $1
		) h
		CROSS JOIN LATERAL relay_intel_membership_digest(h.pubkey, h.source_kind, h.d_tag) d
		WHERE h.membership_count IS DISTINCT FROM d.membership_count
		   OR h.membership_digest IS DISTINCT FROM d.membership_digest`, checked).Scan(&n)
	if err != nil {
		return -1, checked, err.Error()
	}
	return n, checked, ""
}

func deletedConfigsRewritten(ctx context.Context, pool *pgxpool.Pool) int64 {
	var n int64
	err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM relay_config_heads h
		JOIN deletion_events de
		  ON de.target_event_id = h.event_id AND de.deleter_pubkey = h.pubkey
		WHERE h.membership_count >= 0
	`).Scan(&n)
	if err != nil {
		return -1
	}
	return n
}
