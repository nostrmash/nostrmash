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
