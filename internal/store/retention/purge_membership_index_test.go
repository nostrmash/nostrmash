package retention_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/store/retention"
)

// TestPurgeDoesNotSeqScanRelayMemberships measures the same 100-event purge
// against one fixture before and after the relay-intelligence foreign-key
// indexes. The unindexed run is the cascade shape Agent C timed; the indexed
// run is the correction in migration 000145.
func TestPurgeDoesNotSeqScanRelayMemberships(t *testing.T) {
	ctx, pool, svc := setupRetention(t)
	old := time.Now().Add(-48 * time.Hour).UTC()
	const memberships = 100000
	insertEvent(t, ctx, pool, "purge-winner", "purge-owner", 10002, 20, old)
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ('purge-owner', 10002, '', 'purge-winner', 20, 1)
	`); err != nil {
		t.Fatal(err)
	}
	insertEvent(t, ctx, pool, "purge-anchor", "purge-anchor-owner", 10002, 1, old)
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_role_memberships (
			pubkey, role, url_key, source_kind, d_tag, endpoint_url, endpoint_type,
			source_event_id, created_at, declaration_source, public_safe
		)
		SELECT 'm' || i, 'public_outbox', 'k' || i, 10002, '', 'wss://m.example', 'nostr_relay',
		       'purge-anchor', 1, 'modern', true
		FROM generate_series(1, $1) AS i
	`, memberships); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_config_heads (
			pubkey, source_kind, d_tag, event_id, created_at, declaration_source
		)
		SELECT 'h' || i, 10002, '', 'purge-anchor', 1, 'modern'
		FROM generate_series(1, $1) AS i
	`, memberships); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("purge-loser-%d", i)
		insertEvent(t, ctx, pool, id, fmt.Sprintf("purge-owner-%d", i), 10002, 10, old)
		if _, err := pool.Exec(ctx, `
			INSERT INTO replaceable_event_purge (event_id, created_at) VALUES ($1, 10)
		`, id); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := pool.Exec(ctx, `
		DROP INDEX IF EXISTS idx_relay_role_memberships_source_event;
		DROP INDEX IF EXISTS idx_relay_config_heads_event_id;
		DROP INDEX IF EXISTS idx_dm_relay_lists_latest_event_id;
		DROP INDEX IF EXISTS idx_nutzap_info_latest_event_id
	`); err != nil {
		t.Fatal(err)
	}
	before := timePurge(t, ctx, pool, svc)
	if _, err := pool.Exec(ctx, `
		CREATE INDEX idx_relay_role_memberships_source_event ON relay_role_memberships (source_event_id);
		CREATE INDEX idx_relay_config_heads_event_id ON relay_config_heads (event_id);
		CREATE INDEX idx_dm_relay_lists_latest_event_id ON dm_relay_lists_latest (event_id);
		CREATE INDEX idx_nutzap_info_latest_event_id ON nutzap_info_latest (event_id)
	`); err != nil {
		t.Fatal(err)
	}
	after := timePurge(t, ctx, pool, svc)
	t.Logf("purge 100 events, %d memberships and heads: unindexed %s (membership seq=%d head seq=%d) indexed %s (membership seq=%d head seq=%d)",
		memberships, before.elapsed, before.memberSeq, before.headSeq, after.elapsed, after.memberSeq, after.headSeq)

	var plan string
	// Every stored membership points at the anchor, so looking that value up is
	// a full-table predicate and a sequential scan is the right plan. A purge
	// probes event ids that match nothing. That lookup has to use the index.
	rows, err := pool.Query(ctx, `EXPLAIN SELECT 1 FROM relay_role_memberships WHERE source_event_id = 'purge-loser-0'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan += line + "\n"
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "idx_relay_role_memberships_source_event") {
		t.Fatalf("membership lookup plan = %s", plan)
	}
	if before.elapsed < after.elapsed*3 {
		t.Fatalf("unindexed purge %s was not slower than indexed %s", before.elapsed, after.elapsed)
	}
	// Table counters are flushed asynchronously. When they are visible, the
	// unindexed purge must be the one sequential-scanning both child tables.
	if before.memberSeq > 0 || before.headSeq > 0 {
		if after.memberSeq != 0 || after.headSeq != 0 {
			t.Fatalf("indexed cascade seq scans memberships=%d heads=%d", after.memberSeq, after.headSeq)
		}
		if before.memberSeq == 0 || before.headSeq == 0 {
			t.Fatalf("unindexed scans memberships=%d heads=%d", before.memberSeq, before.headSeq)
		}
	}
}

type purgeMeasurement struct {
	elapsed   time.Duration
	memberSeq int64
	headSeq   int64
}

func timePurge(t *testing.T, ctx context.Context, pool *pgxpool.Pool, svc *retention.Retention) purgeMeasurement {
	t.Helper()
	if _, err := pool.Exec(ctx, `ANALYZE relay_role_memberships, relay_config_heads`); err != nil {
		t.Fatal(err)
	}
	schema := ""
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		SELECT pg_stat_reset_single_table_counters(c.oid)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname IN ('relay_role_memberships', 'relay_config_heads')
	`, schema); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT pg_stat_clear_snapshot()`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	deleted, err := svc.PurgeSupersededReplaceableEvents(ctx, time.Now().Add(-time.Hour), time.Now().Add(-time.Hour), 100)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 100 {
		t.Fatalf("deleted = %d", deleted)
	}
	if _, err := pool.Exec(ctx, `SELECT pg_stat_clear_snapshot()`); err != nil {
		t.Fatal(err)
	}
	var measured purgeMeasurement
	measured.elapsed = elapsed
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(sum(seq_scan), 0)
		FROM pg_stat_user_tables
		WHERE schemaname = $1 AND relname = 'relay_role_memberships'
	`, schema).Scan(&measured.memberSeq); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(sum(seq_scan), 0)
		FROM pg_stat_user_tables
		WHERE schemaname = $1 AND relname = 'relay_config_heads'
	`, schema).Scan(&measured.headSeq); err != nil {
		t.Fatal(err)
	}
	return measured
}
