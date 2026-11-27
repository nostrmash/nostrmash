package relayintel

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
	"github.com/nostrmash/nostrmash/migrations"
)

func TestHistoricalMembershipMetadataUpgrade(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay intel upgrade"), "relayupgrade")
	applyMigrationsThrough(t, ctx, pool, "000144_relay_intel_projection_invariants.sql")

	currentPub := pubkeyTag("aa")
	missingPub := pubkeyTag("ab")
	stalePub := pubkeyTag("ac")
	emptyPub := pubkeyTag("ad")
	truncatedPub := pubkeyTag("ae")
	racePub := pubkeyTag("af")
	badPub := pubkeyTag("b0")
	deletedPub := pubkeyTag("b1")
	privatePub := pubkeyTag("b2")
	dmPub := pubkeyTag("b3")

	currentID := idHex(1)
	missingID := idHex(2)
	staleOld := idHex(3)
	staleNew := idHex(4)
	emptyID := idHex(5)
	truncatedID := idHex(6)
	raceOld := idHex(7)
	raceNew := idHex(8)
	badID := idHex(9)
	deletedID := idHex(10)
	deletionID := idHex(11)
	privateID := idHex(12)
	dmID := idHex(13)

	insertPlainEvent(t, ctx, pool, currentID, currentPub, KindPublicRelayList, 20, "", [][]string{
		{"r", "wss://kept.example", "write"},
		{"r", "wss://also.example", "write"},
	})
	insertPlainEvent(t, ctx, pool, missingID, missingPub, KindPublicRelayList, 20, "", [][]string{
		{"r", "wss://one.example", "read"},
		{"r", "wss://two.example", "write"},
	})
	insertPlainEvent(t, ctx, pool, staleOld, stalePub, KindPublicRelayList, 10, "", [][]string{{"r", "wss://old.example", "write"}})
	insertPlainEvent(t, ctx, pool, staleNew, stalePub, KindPublicRelayList, 30, "", [][]string{{"r", "wss://new.example", "write"}})
	insertPlainEvent(t, ctx, pool, emptyID, emptyPub, KindPublicRelayList, 20, "", nil)
	truncatedTags := make([][]string, 0, 33)
	for i := 1; i <= 33; i++ {
		truncatedTags = append(truncatedTags, []string{"r", fmt.Sprintf("wss://relay-%02d.example", i), "write"})
	}
	insertPlainEvent(t, ctx, pool, truncatedID, truncatedPub, KindPublicRelayList, 20, "", truncatedTags)
	insertPlainEvent(t, ctx, pool, raceOld, racePub, KindPublicRelayList, 10, "", [][]string{{"r", "wss://race-old.example", "write"}})
	insertPlainEvent(t, ctx, pool, raceNew, racePub, KindPublicRelayList, 40, "", [][]string{{"r", "wss://race-new.example", "write"}})
	insertRawEvent(t, ctx, pool, badID, badPub, KindBlockedRelays, 20, `{"tags":"bad"}`)
	insertPlainEvent(t, ctx, pool, deletedID, deletedPub, KindSearchRelays, 20, "", [][]string{{"relay", "wss://gone.example"}})
	insertPlainEvent(t, ctx, pool, deletionID, deletedPub, 5, 30, "", [][]string{{"e", deletedID}})
	insertPlainEvent(t, ctx, pool, privateID, privatePub, KindPrivateRelays, 20, "secret", [][]string{{"relay", "wss://hidden.example"}})
	insertPlainEvent(t, ctx, pool, dmID, dmPub, KindDMRelays, 20, "", [][]string{{"relay", "wss://dm.example"}})

	insertState(t, ctx, pool, currentPub, KindPublicRelayList, currentID, 20)
	insertState(t, ctx, pool, missingPub, KindPublicRelayList, missingID, 20)
	insertState(t, ctx, pool, stalePub, KindPublicRelayList, staleNew, 30)
	insertState(t, ctx, pool, emptyPub, KindPublicRelayList, emptyID, 20)
	insertState(t, ctx, pool, truncatedPub, KindPublicRelayList, truncatedID, 20)
	insertState(t, ctx, pool, racePub, KindPublicRelayList, raceOld, 10)
	insertState(t, ctx, pool, badPub, KindBlockedRelays, badID, 20)
	insertState(t, ctx, pool, deletedPub, KindSearchRelays, deletedID, 20)
	insertState(t, ctx, pool, privatePub, KindPrivateRelays, privateID, 20)
	insertState(t, ctx, pool, dmPub, KindDMRelays, dmID, 20)

	insertHead(t, ctx, pool, currentPub, KindPublicRelayList, currentID, 20, false, false, false)
	insertHead(t, ctx, pool, missingPub, KindPublicRelayList, missingID, 20, false, false, false)
	insertHead(t, ctx, pool, stalePub, KindPublicRelayList, staleOld, 10, false, false, false)
	insertHead(t, ctx, pool, emptyPub, KindPublicRelayList, emptyID, 20, true, false, false)
	insertHead(t, ctx, pool, truncatedPub, KindPublicRelayList, truncatedID, 20, false, false, true)
	insertHead(t, ctx, pool, racePub, KindPublicRelayList, raceOld, 10, false, false, false)
	insertHead(t, ctx, pool, badPub, KindBlockedRelays, badID, 20, false, false, false)
	insertHead(t, ctx, pool, privatePub, KindPrivateRelays, privateID, 20, false, true, false)
	insertHead(t, ctx, pool, dmPub, KindDMRelays, dmID, 20, false, false, false)

	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_role_memberships (
			pubkey, role, url_key, source_kind, d_tag, endpoint_url, endpoint_type,
			source_event_id, created_at, declaration_source, public_safe
		) VALUES
			($1, 'public_outbox', 'kept', 10002, '', 'wss://kept.example', 'nostr_relay', $2, 20, 'modern', true),
			($1, 'public_outbox', 'extra', 10002, '', 'wss://extra.example', 'nostr_relay', $2, 20, 'modern', true)
	`, currentPub, currentID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, 30, 1)
	`, deletionID, deletedPub, deletedID); err != nil {
		t.Fatal(err)
	}
	for _, kind := range BackfillKinds() {
		if _, err := pool.Exec(ctx, `
			INSERT INTO relay_intel_backfill_cursor (source_kind, done)
			VALUES ($1, true)
		`, kind); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_intel_work_cursor (work_name, idle, next_run_at, cursor_kind, cursor_pubkey, cursor_d_tag)
		VALUES ('projection_reconcile', true, now() + interval '1 hour', 0, '', '')
	`); err != nil {
		t.Fatal(err)
	}

	applyMigrationFile(t, ctx, pool, "000145_relay_intel_membership_and_retention.sql")
	var unknown int
	if err := pool.QueryRow(ctx, `SELECT membership_count FROM relay_config_heads WHERE pubkey = $1`, currentPub).Scan(&unknown); err != nil {
		t.Fatal(err)
	}
	if unknown != -1 {
		t.Fatalf("pre-upgrade head count = %d", unknown)
	}

	t.Cleanup(func() { SetReconcileRowHook(nil) })
	SetReconcileRowHook(func(pubkey string, kind int, eventID string) {
		if pubkey == racePub && kind == KindPublicRelayList && eventID == raceOld {
			if _, err := pool.Exec(ctx, `
				UPDATE replaceable_state
				SET event_id = $2, created_at = 40, updated_at = now()
				WHERE pubkey = $1 AND kind = 10002 AND d_tag = ''
			`, racePub, raceNew); err != nil {
				t.Errorf("concurrent canonical update: %v", err)
			}
		}
	})

	if err := BackfillBatch(ctx, pool, 1); err != nil {
		t.Fatal(err)
	}
	if headCount(t, ctx, pool, currentPub, KindPublicRelayList) < 0 {
		t.Fatal("first batch did not record the first head")
	}
	if headCount(t, ctx, pool, missingPub, KindPublicRelayList) != -1 {
		t.Fatal("first batch consumed the following head")
	}

	var status ProjectionReconcileStatus
	for i := 0; i < 40; i++ {
		if err := BackfillBatch(ctx, pool, 1); err != nil {
			t.Fatal(err)
		}
		var err error
		status, err = ReadProjectionReconcileStatus(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		if status.UpgradeComplete && status.Idle && status.IntegrityRecorded {
			break
		}
	}
	if !status.UpgradeComplete || !status.IntegrityRecorded {
		t.Fatalf("upgrade status = %+v", status)
	}

	assertDigest(t, ctx, pool, currentPub, KindPublicRelayList, 2)
	if membershipURL(t, ctx, pool, currentPub, "wss://extra.example") {
		t.Fatal("reconciliation kept a membership that is not in the authoritative event")
	}
	assertDigest(t, ctx, pool, missingPub, KindPublicRelayList, 2)
	if !membershipURL(t, ctx, pool, missingPub, "wss://one.example") || !membershipURL(t, ctx, pool, missingPub, "wss://two.example") {
		t.Fatal("missing memberships were not rebuilt from the event")
	}
	var staleHead string
	if err := pool.QueryRow(ctx, `
		SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002
	`, stalePub).Scan(&staleHead); err != nil {
		t.Fatal(err)
	}
	if staleHead != staleNew || membershipURL(t, ctx, pool, stalePub, "wss://old.example") || !membershipURL(t, ctx, pool, stalePub, "wss://new.example") {
		t.Fatalf("stale head = %s", staleHead)
	}
	var emptyCount int
	var emptyFlag bool
	if err := pool.QueryRow(ctx, `
		SELECT membership_count, explicitly_empty FROM relay_config_heads WHERE pubkey = $1
	`, emptyPub).Scan(&emptyCount, &emptyFlag); err != nil {
		t.Fatal(err)
	}
	if emptyCount != 0 || !emptyFlag {
		t.Fatalf("empty count=%d flag=%v", emptyCount, emptyFlag)
	}
	var truncCount int
	var trunc bool
	if err := pool.QueryRow(ctx, `
		SELECT membership_count, truncated FROM relay_config_heads WHERE pubkey = $1
	`, truncatedPub).Scan(&truncCount, &trunc); err != nil {
		t.Fatal(err)
	}
	if truncCount != MaxEndpointsPerEvent || !trunc {
		t.Fatalf("truncated count=%d flag=%v", truncCount, trunc)
	}
	var privateCount int
	var clientSide bool
	if err := pool.QueryRow(ctx, `
		SELECT membership_count, client_side_only FROM relay_config_heads WHERE pubkey = $1
	`, privatePub).Scan(&privateCount, &clientSide); err != nil {
		t.Fatal(err)
	}
	if privateCount != 0 || !clientSide {
		t.Fatalf("private count=%d client_side=%v", privateCount, clientSide)
	}
	var dmCount int
	if err := pool.QueryRow(ctx, `SELECT membership_count FROM relay_config_heads WHERE pubkey = $1`, dmPub).Scan(&dmCount); err != nil {
		t.Fatal(err)
	}
	if dmCount < 1 || !membershipURL(t, ctx, pool, dmPub, "wss://dm.example") {
		t.Fatalf("dm count=%d", dmCount)
	}
	var badCount int
	if err := pool.QueryRow(ctx, `SELECT membership_count FROM relay_config_heads WHERE pubkey = $1`, badPub).Scan(&badCount); err != nil {
		t.Fatal(err)
	}
	if badCount != membershipUnreadable {
		t.Fatalf("malformed count = %d", badCount)
	}
	var deletedHeads int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_config_heads WHERE pubkey = $1`, deletedPub).Scan(&deletedHeads); err != nil {
		t.Fatal(err)
	}
	if deletedHeads != 0 {
		t.Fatal("deleted event was projected")
	}
	var raceHead string
	if err := pool.QueryRow(ctx, `
		SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002
	`, racePub).Scan(&raceHead); err != nil {
		t.Fatal(err)
	}
	if raceHead != raceNew || !membershipURL(t, ctx, pool, racePub, "wss://race-new.example") || membershipURL(t, ctx, pool, racePub, "wss://race-old.example") {
		t.Fatalf("race head = %s", raceHead)
	}

	var before time.Time
	if err := pool.QueryRow(ctx, `SELECT updated_at FROM relay_config_heads WHERE pubkey = $1`, currentPub).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE relay_intel_work_cursor
		SET idle = false, next_run_at = now(), cursor_kind = 0, cursor_pubkey = '', cursor_d_tag = ''
		WHERE work_name = 'projection_reconcile'
	`); err != nil {
		t.Fatal(err)
	}
	if err := BackfillBatch(ctx, pool, 50); err != nil {
		t.Fatal(err)
	}
	var after time.Time
	if err := pool.QueryRow(ctx, `SELECT updated_at FROM relay_config_heads WHERE pubkey = $1`, currentPub).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.Equal(before) {
		t.Fatal("completed head was reconciled again")
	}
	var repairs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_intel_repairs WHERE pubkey = $1`, currentPub).Scan(&repairs); err != nil {
		t.Fatal(err)
	}
	if repairs != 0 {
		t.Fatalf("completed head entered repairs: %d", repairs)
	}
	done, err := ReadProjectionReconcileStatus(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if !done.UpgradeComplete || done.PendingEligible != 0 {
		t.Fatalf("status after restart = %+v", done)
	}
}

func applyMigrationsThrough(t *testing.T, ctx context.Context, pool *pgxpool.Pool, last string) {
	t.Helper()
	var names []string
	for _, name := range migrationNames(t) {
		if name > last {
			break
		}
		names = append(names, name)
	}
	applyMigrationFiles(t, ctx, pool, names)
}

func applyMigrationFile(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) {
	t.Helper()
	applyMigrationFiles(t, ctx, pool, []string{name})
}

func applyMigrationFiles(t *testing.T, ctx context.Context, pool *pgxpool.Pool, names []string) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var schema string
	if err := tx.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	// 000092 scans pg_constraint across the database. Another test's schema
	// drop during that scan fails with "could not open relation with OID"
	// unless this transaction holds the same lock as Migrate and test cleanup.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, dbmigrate.MigrationAdvisoryLockID); err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		data, err := migrations.Files.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := dbmigrate.SetMigrationSearchPath(ctx, tx, schema, name == "000016_search_hardening.sql"); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, string(data)); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func migrationNames(t *testing.T) []string {
	t.Helper()
	entries, err := fs.ReadDir(migrations.Files, ".")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}

func pubkeyTag(prefix string) string {
	return prefix + strings.Repeat("0", 64-len(prefix))
}

func idHex(n int) string {
	return fmt.Sprintf("%064x", n)
}

func insertPlainEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, pubkey string, kind int, created int64, content string, tags [][]string) {
	t.Helper()
	if tags == nil {
		tags = [][]string{}
	}
	raw, err := json.Marshal(map[string]any{
		"id": id, "pubkey": pubkey, "created_at": created, "kind": kind,
		"content": content, "tags": tags, "sig": strings.Repeat("c", 128),
	})
	if err != nil {
		t.Fatal(err)
	}
	insertRawEvent(t, ctx, pool, id, pubkey, kind, created, string(raw))
	if _, err := pool.Exec(ctx, `UPDATE events SET content = $2 WHERE id = $1`, id, content); err != nil {
		t.Fatal(err)
	}
}

func insertRawEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, pubkey string, kind int, created int64, raw string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ($1, $2, $3, $4, $5, '', $6::jsonb)
	`, id, pubkey, created, kind, strings.Repeat("c", 128), raw); err != nil {
		t.Fatal(err)
	}
}

func insertState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string, kind int, eventID string, created int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, $2, '', $3, $4, 1)
	`, pubkey, kind, eventID, created); err != nil {
		t.Fatal(err)
	}
}

func insertHead(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string, kind int, eventID string, created int64, empty, clientSide, truncated bool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_config_heads (
			pubkey, source_kind, d_tag, event_id, created_at, declaration_source,
			explicitly_empty, client_side_only, truncated
		) VALUES ($1, $2, '', $3, $4, 'modern', $5, $6, $7)
	`, pubkey, kind, eventID, created, empty, clientSide, truncated); err != nil {
		t.Fatal(err)
	}
}

func headCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string, kind int) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT membership_count FROM relay_config_heads
		WHERE pubkey = $1 AND source_kind = $2 AND d_tag = ''
	`, pubkey, kind).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func assertDigest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string, kind int, want int) {
	t.Helper()
	var matched bool
	if err := pool.QueryRow(ctx, `
		SELECT h.membership_count = d.membership_count
		   AND h.membership_digest = d.membership_digest
		   AND h.membership_count = $4
		FROM relay_config_heads h
		CROSS JOIN LATERAL relay_intel_membership_digest(h.pubkey, h.source_kind, h.d_tag) d
		WHERE h.pubkey = $1 AND h.source_kind = $2 AND h.d_tag = $3
	`, pubkey, kind, "", want).Scan(&matched); err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatalf("%s kind %d digest does not match the projected rows", pubkey, kind)
	}
}

func membershipURL(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey, url string) bool {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM relay_role_memberships
		WHERE pubkey = $1 AND endpoint_url = $2
	`, pubkey, url).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}
