package relayintel_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/derivation"
	"github.com/nostrmash/nostrmash/internal/model"
	"github.com/nostrmash/nostrmash/internal/relayintel"
	"github.com/nostrmash/nostrmash/internal/store"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func TestHistoricalTieRepairProtectsTheLowerID(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay intel repair"), "relayrepair")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	pubkey := strings.Repeat("ab", 32)
	lower := strings.Repeat("a", 64)
	higher := strings.Repeat("b", 64)
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	pgStore := store.NewPostgresStore(pool)
	for _, id := range []string{lower, higher} {
		raw, _ := json.Marshal(map[string]any{
			"id": id, "pubkey": pubkey, "created_at": 1000, "kind": relayintel.KindPublicRelayList,
			"tags": [][]string{{"r", "wss://kept.example"}}, "content": "", "sig": strings.Repeat("c", 128),
		})
		event := model.Event{
			ID: id, Pubkey: pubkey, CreatedAt: 1000, Kind: relayintel.KindPublicRelayList,
			Sig: strings.Repeat("c", 128), Content: "", RawJSON: raw,
			FirstSeenAt: now, InsertedAt: now,
		}
		if err := pgStore.InsertCanonicalEvent(ctx, event, [][]string{{"r", "wss://kept.example"}}, "wss://relay.example", now); err != nil {
			t.Fatal(err)
		}
	}
	handlers := derivation.NewHandlers(pool)
	if err := handlers.UpdateReplaceableState(ctx, higher); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_config_heads (
			pubkey, source_kind, d_tag, event_id, created_at, declaration_source
		) VALUES ($1, $2, '', $3, 1000, 'modern')
	`, pubkey, relayintel.KindPublicRelayList, higher); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_role_memberships (
			pubkey, role, url_key, source_kind, d_tag, endpoint_url, endpoint_type,
			source_event_id, created_at, declaration_source, public_safe
		) VALUES ($1, 'public_outbox', 'kept', $2, '', 'wss://kept.example', 'nostr_relay', $3, 1000, 'modern', true)
	`, pubkey, relayintel.KindPublicRelayList, higher); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_event_purge (event_id, created_at) VALUES ($1, 1000)
	`, lower); err != nil {
		t.Fatal(err)
	}

	var repaired int
	if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(10)`).Scan(&repaired); err != nil {
		t.Fatal(err)
	}
	if repaired != 1 {
		t.Fatalf("repaired = %d", repaired)
	}
	var winner string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM replaceable_state WHERE pubkey = $1 AND kind = $2`, pubkey, relayintel.KindPublicRelayList).Scan(&winner); err != nil {
		t.Fatal(err)
	}
	if winner != lower {
		t.Fatalf("winner = %s", winner)
	}
	var purgedLower int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM replaceable_event_purge WHERE event_id = $1`, lower).Scan(&purgedLower); err != nil {
		t.Fatal(err)
	}
	if purgedLower != 0 {
		t.Fatal("the lower-id winner stayed on the purge queue")
	}
	var jobs int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key = $1`, "relay-intel-repair:"+lower).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Fatalf("repair jobs = %d", jobs)
	}
	var status string
	var truncated bool
	var headID string
	if err := pool.QueryRow(ctx, `
		SELECT r.status, h.truncated, h.event_id
		FROM relay_intel_repairs r
		JOIN relay_config_heads h ON h.event_id = r.winner_event_id
		WHERE r.winner_event_id = $1
	`, lower).Scan(&status, &truncated, &headID); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || !truncated || headID != lower {
		t.Fatalf("status=%s truncated=%v head=%s", status, truncated, headID)
	}
	var staleMemberships int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_role_memberships WHERE source_event_id = $1`, higher).Scan(&staleMemberships); err != nil {
		t.Fatal(err)
	}
	if staleMemberships != 0 {
		t.Fatal("memberships still point at the losing event")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := relayintel.Project(ctx, tx, relayintel.EventInput{
		Pubkey: pubkey, EventID: lower, Kind: relayintel.KindPublicRelayList, CreatedAt: 1000,
		Tags: [][]string{{"r", "wss://kept.example"}},
	}); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(10)`).Scan(&repaired); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, lower).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "done" {
		t.Fatalf("repair status after replay = %s", status)
	}
}
