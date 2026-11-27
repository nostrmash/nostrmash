package relayintel

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func TestUpgradeFromDevelopPreservesHistoricalRows(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "develop upgrade"), "updev")
	if err := dbmigrate.MigrateMatching(ctx, pool, "test", func(name string) bool {
		return name <= "000135_relay_intel_deletion_durability.sql"
	}); err != nil {
		t.Fatal(err)
	}
	eventID := strings.Repeat("ab", 32)
	pubkey := strings.Repeat("cd", 32)
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ($1, $2, 10, 10002, repeat('e', 128), '', jsonb_build_object('id', $1::text, 'kind', 10002))
	`, eventID, pubkey); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO jobs (job_type, payload, idempotency_key)
		VALUES ('derive_event_bundle', jsonb_build_object('event_id', $1::text), 'upgrade-job')
	`, eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_registry (url_key, normalized_url)
		VALUES ('wss://historical.example', 'wss://historical.example')
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_config_heads (pubkey, source_kind, d_tag, event_id, created_at, declaration_source)
		VALUES ($1, 10002, '', $2, 10, 'modern')
	`, pubkey, eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_relays (event_id, relay_url, pubkey)
		VALUES ($1, 'wss://historical.example', $2)
	`, eventID, pubkey); err != nil {
		t.Fatal(err)
	}

	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	var gotEvent, gotJob, gotRelay, gotHead, gotSeen string
	var members int
	var digest string
	if err := pool.QueryRow(ctx, `SELECT id FROM events WHERE id = $1`, eventID).Scan(&gotEvent); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT idempotency_key FROM jobs WHERE idempotency_key = 'upgrade-job'`).Scan(&gotJob); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT normalized_url FROM relay_registry WHERE url_key = 'wss://historical.example'`).Scan(&gotRelay); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT event_id, membership_count, membership_digest
		FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002
	`, pubkey).Scan(&gotHead, &members, &digest); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT relay_url FROM event_relays WHERE event_id = $1`, eventID).Scan(&gotSeen); err != nil {
		t.Fatal(err)
	}
	if gotEvent != eventID || gotJob != "upgrade-job" || gotRelay != "wss://historical.example" || gotHead != eventID || gotSeen != "wss://historical.example" {
		t.Fatalf("historical rows changed event=%s job=%s relay=%s head=%s seen=%s", gotEvent, gotJob, gotRelay, gotHead, gotSeen)
	}
	if members != -1 || digest != "" {
		t.Fatalf("historical membership was rewritten count=%d digest=%q", members, digest)
	}
	assertProbeSchema(t, ctx, pool)
}

func TestUpgradeFromCoreAppliesOnlyProbeMigrations(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "core upgrade"), "upcore")
	if err := dbmigrate.MigrateMatching(ctx, pool, "test", func(name string) bool {
		return !probeMigration(name)
	}); err != nil {
		t.Fatal(err)
	}
	eventID := strings.Repeat("12", 32)
	pubkey := strings.Repeat("34", 32)
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ($1, $2, 20, 1, repeat('a', 128), 'historical', jsonb_build_object('id', $1::text))
	`, eventID, pubkey); err != nil {
		t.Fatal(err)
	}
	pending, err := dbmigrate.Pending(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	wantPending := []string{
		"000138_relay_probe_attempts.sql",
		"000139_relay_probe_identity_samples.sql",
		"000146_relay_probe_evidence_fence.sql",
		"000147_relay_probe_followup_lease.sql",
		"000148_relay_probe_followup_ownership.sql",
		"000150_relay_probe_privacy_ownership.sql",
		"000151_relay_probe_completion_digest.sql",
	}
	if strings.Join(pending, ",") != strings.Join(wantPending, ",") {
		t.Fatalf("pending after core schema = %v", pending)
	}
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	pending, err = dbmigrate.Pending(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after probe upgrade = %v", pending)
	}
	var got string
	if err := pool.QueryRow(ctx, `SELECT id FROM events WHERE id = $1`, eventID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != eventID {
		t.Fatalf("core event changed: %s", got)
	}
	assertProbeSchema(t, ctx, pool)
	var membership, fence bool
	if err := pool.QueryRow(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM schema_migrations_audit WHERE migration_id = 'migrations/000145_relay_intel_membership_and_retention.sql'),
			EXISTS (SELECT 1 FROM schema_migrations_audit WHERE migration_id = 'migrations/000146_relay_probe_evidence_fence.sql')
	`).Scan(&membership, &fence); err != nil {
		t.Fatal(err)
	}
	if !membership || !fence {
		t.Fatalf("audit missing core or probe migration membership=%t fence=%t", membership, fence)
	}
}

func TestInterruptedMigrationRestartsWithoutPartialState(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "interrupted upgrade"), "upstop")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := dbmigrate.Migrate(cancelled, pool, "test"); err == nil {
		t.Fatal("a cancelled migration committed")
	}
	var audit bool
	if err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = 'schema_migrations_audit'
		)
	`).Scan(&audit); err != nil {
		t.Fatal(err)
	}
	if audit {
		t.Fatal("cancelled migration left an audit table")
	}
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	var checksum string
	if err := pool.QueryRow(ctx, `
		SELECT checksum FROM schema_migrations_audit
		WHERE migration_id = 'migrations/000151_relay_probe_completion_digest.sql'
	`).Scan(&checksum); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE schema_migrations_audit SET checksum = 'bad'
		WHERE migration_id = 'migrations/000151_relay_probe_completion_digest.sql'
	`); err != nil {
		t.Fatal(err)
	}
	if err := dbmigrate.Migrate(ctx, pool, "test"); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("checksum validation = %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE schema_migrations_audit SET checksum = $1
		WHERE migration_id = 'migrations/000151_relay_probe_completion_digest.sql'
	`, checksum); err != nil {
		t.Fatal(err)
	}
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	pending, err := dbmigrate.Pending(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending after restart = %v", pending)
	}
}

func assertProbeSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var digest, epoch, ready, claim bool
	err := pool.QueryRow(ctx, `
		SELECT
			EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = current_schema() AND table_name = 'relay_probe_attempts' AND column_name = 'result_digest'
			),
			EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = current_schema() AND table_name = 'relay_probe_identity_epoch'
			),
			EXISTS (
				SELECT 1 FROM pg_proc p
				JOIN pg_namespace n ON n.oid = p.pronamespace
				WHERE n.nspname = current_schema() AND p.proname = 'relay_intel_projection_ready'
			),
			EXISTS (
				SELECT 1 FROM pg_proc p
				JOIN pg_namespace n ON n.oid = p.pronamespace
				WHERE n.nspname = current_schema() AND p.proname = 'claim_relay_probe'
			)
	`).Scan(&digest, &epoch, &ready, &claim)
	if err != nil {
		t.Fatal(err)
	}
	if !digest || !epoch || !ready || !claim {
		t.Fatalf("schema readiness digest=%t epoch=%t ready=%t claim=%t", digest, epoch, ready, claim)
	}
}

func probeMigration(name string) bool {
	switch name {
	case "000138_relay_probe_attempts.sql",
		"000139_relay_probe_identity_samples.sql",
		"000146_relay_probe_evidence_fence.sql",
		"000147_relay_probe_followup_lease.sql",
		"000148_relay_probe_followup_ownership.sql",
		"000150_relay_probe_privacy_ownership.sql",
		"000151_relay_probe_completion_digest.sql":
		return true
	default:
		return false
	}
}
