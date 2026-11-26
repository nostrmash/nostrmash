package relayintel

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

// TestPopulatedRelayIntelCost measures repair, reconcile, and a canonical
// read on a synthetic dataset. It does not run in CI. Reproduce with:
//
//	RELAY_INTEL_POPULATED_BENCH=1 go test ./internal/relayintel/ -run TestPopulatedRelayIntelCost -v -count=1
func TestPopulatedRelayIntelCost(t *testing.T) {
	if os.Getenv("RELAY_INTEL_POPULATED_BENCH") == "" {
		t.Skip("set RELAY_INTEL_POPULATED_BENCH=1 to measure a populated database")
	}
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "populated relay intel"), "relaypop")
	if err := dbmigrate.Migrate(ctx, pool, "bench"); err != nil {
		t.Fatal(err)
	}
	accounts := 10000
	if raw := os.Getenv("RELAY_INTEL_POPULATED_ACCOUNTS"); raw != "" {
		fmt.Sscan(raw, &accounts)
	}
	if accounts < 100 {
		accounts = 100
	}
	seedStarted := time.Now()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		SELECT lpad(to_hex(i), 64, '0'),
		       lpad(to_hex(1000000 + i), 64, '0'),
		       1700000000,
		       10002,
		       repeat('ab', 64),
		       '',
		       jsonb_build_object('id', lpad(to_hex(i), 64, '0'), 'kind', 10002)
		FROM generate_series(1, $1) AS i
	`, accounts); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		SELECT lpad(to_hex(1000000 + i), 64, '0'), 10002, '', lpad(to_hex(i), 64, '0'), 1700000000, 1
		FROM generate_series(1, $1) AS i
	`, accounts); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		SELECT lpad(to_hex(5000000 + i), 64, '0'),
		       lpad(to_hex(2000000 + i), 64, '0'),
		       1600000000,
		       0,
		       repeat('ab', 64),
		       '',
		       jsonb_build_object('kind', 0)
		FROM generate_series(1, $1) AS i
	`, accounts); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		SELECT lpad(to_hex(2000000 + i), 64, '0'), 0, '', lpad(to_hex(5000000 + i), 64, '0'), 1600000000, 1
		FROM generate_series(1, $1) AS i
	`, accounts); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_config_heads (
			pubkey, source_kind, d_tag, event_id, created_at, declaration_source, truncated
		)
		SELECT lpad(to_hex(1000000 + i), 64, '0'), 10002, '', lpad(to_hex(i), 64, '0'), 1700000000, 'modern',
		       (i % 100 = 0)
		FROM generate_series(1, $1) AS i
	`, accounts); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_role_memberships (
			pubkey, role, url_key, source_kind, d_tag, endpoint_url, endpoint_type,
			source_event_id, created_at, declaration_source, public_safe
		)
		SELECT lpad(to_hex(1000000 + i), 64, '0'),
		       role,
		       'relay-' || ((i % 80) + 1),
		       10002,
		       '',
		       'wss://relay-' || ((i % 80) + 1) || '.example',
		       'nostr_relay',
		       lpad(to_hex(i), 64, '0'),
		       1700000000,
		       'modern',
		       true
		FROM generate_series(1, $1) AS i
		CROSS JOIN (VALUES ('public_inbox'), ('public_outbox')) AS roles(role)
	`, accounts); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		SELECT lpad(to_hex(8000000 + i), 64, '0'),
		       lpad(to_hex(1000000 + i), 64, '0'),
		       1700000000,
		       10002,
		       repeat('ab', 64),
		       '',
		       jsonb_build_object('kind', 10002)
		FROM generate_series(1, 200) AS i
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE replaceable_state rs
		SET event_id = lpad(to_hex(8000000 + i), 64, '0')
		FROM generate_series(1, 200) AS i
		WHERE rs.pubkey = lpad(to_hex(1000000 + i), 64, '0')
		  AND rs.kind = 10002
		  AND rs.d_tag = ''
	`); err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded %d accounts in %s", accounts, time.Since(seedStarted).Round(time.Millisecond))

	var version, sharedBuffers, workMem string
	if err := pool.QueryRow(ctx, `SHOW server_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SHOW shared_buffers`).Scan(&sharedBuffers)
	_ = pool.QueryRow(ctx, `SHOW work_mem`).Scan(&workMem)
	t.Logf("postgres %s shared_buffers=%s work_mem=%s", version, sharedBuffers, workMem)

	samples := make([]time.Duration, 0, 20)
	for i := 0; i < 20; i++ {
		started := time.Now()
		var repaired int
		if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(200)`).Scan(&repaired); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, time.Since(started))
		if i == 0 {
			t.Logf("first tie-repair batch fixed %d rows", repaired)
		}
	}
	logLatency(t, "repair_replaceable_tie(200)", samples)

	if _, err := pool.Exec(ctx, `
		UPDATE relay_config_heads SET needs_replay = true
		WHERE pubkey IN (
			SELECT pubkey FROM relay_config_heads WHERE source_kind = 10002 ORDER BY pubkey LIMIT 100
		)
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE relay_intel_work_cursor
		SET idle = false, next_run_at = now(), cursor_kind = 0, cursor_pubkey = '', cursor_d_tag = ''
		WHERE work_name = 'projection_reconcile'
	`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := BackfillBatch(ctx, pool, 200); err != nil {
		t.Fatal(err)
	}
	t.Logf("reconcile one page of stale heads: %s", time.Since(started).Round(time.Millisecond))

	explainQuery(t, ctx, pool, `
		EXPLAIN (ANALYZE, BUFFERS)
		SELECT rs.pubkey
		FROM replaceable_state rs
		LEFT JOIN relay_config_heads h
		  ON h.pubkey = rs.pubkey AND h.source_kind = rs.kind AND h.d_tag = rs.d_tag
		WHERE rs.kind = ANY(ARRAY[3, 10002, 10006, 10007, 10012, 10013, 10050, 10051, 10063, 10102, 30002]::int[])
		  AND (h.event_id IS NULL OR h.event_id <> rs.event_id OR h.needs_replay)
		ORDER BY rs.kind, rs.pubkey, rs.d_tag
		LIMIT 200
	`)
	explainQuery(t, ctx, pool, `
		EXPLAIN (ANALYZE, BUFFERS)
		SELECT event_id, truncated, needs_replay
		FROM relay_config_heads
		WHERE pubkey = lpad(to_hex(1000100), 64, '0') AND source_kind = 10002 AND d_tag = ''
	`)

	readSamples := make([]time.Duration, 0, 100)
	for i := 0; i < 100; i++ {
		pubkey := fmt.Sprintf("%064x", 1000000+((i*97)%accounts)+1)
		started := time.Now()
		var id string
		var truncated bool
		err := pool.QueryRow(ctx, `
			SELECT event_id, truncated FROM relay_config_heads
			WHERE pubkey = $1 AND source_kind = 10002 AND d_tag = ''
		`, pubkey).Scan(&id, &truncated)
		if err != nil {
			t.Fatal(err)
		}
		readSamples = append(readSamples, time.Since(started))
	}
	logLatency(t, "canonical head read", readSamples)

	started = time.Now()
	if err := RefreshStats(ctx, pool, ""); err != nil {
		t.Fatal(err)
	}
	t.Logf("statistics refresh: %s", time.Since(started).Round(time.Millisecond))
	var duration int64
	var computed time.Time
	if err := pool.QueryRow(ctx, `SELECT last_duration_ms, computed_at FROM relay_intel_snapshot WHERE id`).Scan(&duration, &computed); err != nil {
		t.Fatal(err)
	}
	t.Logf("snapshot last_duration_ms=%d computed_at=%s", duration, computed.UTC().Format(time.RFC3339))
}

func logLatency(t *testing.T, name string, samples []time.Duration) {
	t.Helper()
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	t.Logf("%s n=%d median=%s p95=%s max=%s", name, len(ordered), ordered[len(ordered)/2].Round(time.Microsecond), ordered[percentile(len(ordered), 0.95)].Round(time.Microsecond), ordered[len(ordered)-1].Round(time.Microsecond))
}

func percentile(n int, p float64) int {
	idx := int(float64(n-1) * p)
	if idx < 0 {
		return 0
	}
	if idx >= n {
		return n - 1
	}
	return idx
}

func explainQuery(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string) {
	t.Helper()
	rows, err := pool.Query(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		t.Log(line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
