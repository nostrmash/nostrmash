package relayintel_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/derivation"
	"github.com/nostrmash/nostrmash/internal/jobs"
	"github.com/nostrmash/nostrmash/internal/model"
	"github.com/nostrmash/nostrmash/internal/relayintel"
	"github.com/nostrmash/nostrmash/internal/store"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func TestHistoricalTieRepairProtectsTheLowerID(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	pubkey := strings.Repeat("ab", 32)
	lower := strings.Repeat("a", 64)
	higher := strings.Repeat("b", 64)
	insertStoredEvent(t, ctx, pool, storedEvent{
		id: lower, pubkey: pubkey, kind: relayintel.KindPublicRelayList, created: 1000,
		tags: [][]string{{"r", "wss://kept.example"}},
	})
	insertStoredEvent(t, ctx, pool, storedEvent{
		id: higher, pubkey: pubkey, kind: relayintel.KindPublicRelayList, created: 1000,
		tags: [][]string{{"r", "wss://kept.example"}},
	})
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

	if _, err := pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'succeeded', finished_at = now() - interval '2 days', attempts = 1
		WHERE idempotency_key LIKE 'derive_event_bundle:%'
	`); err != nil {
		t.Fatal(err)
	}
	kept, err := store.NewPostgresStore(pool).Retention.PurgeSupersededReplaceableEvents(
		ctx, time.Now().Add(time.Hour), time.Now().Add(-time.Hour), 20,
	)
	if err != nil {
		t.Fatal(err)
	}
	if kept != 0 {
		t.Fatalf("purged the lower-id winner before repair: %d", kept)
	}

	repaired := callRepair(t, ctx, pool)
	if repaired != 1 {
		t.Fatalf("repaired = %d", repaired)
	}
	if winner := stateWinner(t, ctx, pool, pubkey, relayintel.KindPublicRelayList, ""); winner != lower {
		t.Fatalf("winner = %s", winner)
	}
	var purgedLower int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM replaceable_event_purge WHERE event_id = $1`, lower).Scan(&purgedLower); err != nil {
		t.Fatal(err)
	}
	if purgedLower != 0 {
		t.Fatal("the lower-id winner stayed on the purge queue")
	}
	if jobsFor(t, ctx, pool, lower) != 1 {
		t.Fatalf("repair jobs = %d", jobsFor(t, ctx, pool, lower))
	}
	var status string
	var replay bool
	var headID string
	if err := pool.QueryRow(ctx, `
		SELECT r.status, h.needs_replay, h.event_id
		FROM relay_intel_repairs r
		JOIN relay_config_heads h ON h.pubkey = r.pubkey AND h.source_kind = r.kind AND h.d_tag = r.d_tag
		WHERE r.winner_event_id = $1
	`, lower).Scan(&status, &replay, &headID); err != nil {
		t.Fatal(err)
	}
	if status != "queued" || !replay || headID != lower {
		t.Fatalf("status=%s replay=%v head=%s", status, replay, headID)
	}
	var staleMemberships int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_role_memberships WHERE source_event_id = $1`, higher).Scan(&staleMemberships); err != nil {
		t.Fatal(err)
	}
	if staleMemberships != 0 {
		t.Fatal("memberships still point at the losing event")
	}
	for i := 0; i < 4; i++ {
		callRepair(t, ctx, pool)
	}
	if jobsFor(t, ctx, pool, lower) != 1 {
		t.Fatal("retries created another derivation job")
	}
	var suffixed int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM jobs
		WHERE idempotency_key LIKE $1 OR idempotency_key LIKE 'relay-intel-repair:%'
	`, "derive_event_bundle:"+lower+":%").Scan(&suffixed); err != nil {
		t.Fatal(err)
	}
	if suffixed != 0 {
		t.Fatalf("timestamp-suffixed jobs = %d", suffixed)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE jobs SET status = 'succeeded', finished_at = now() - interval '2 days'
		WHERE idempotency_key = $1
	`, "derive_event_bundle:"+lower); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE replaceable_state SET event_id = $1 WHERE pubkey = $2 AND kind = $3 AND d_tag = ''
	`, higher, pubkey, relayintel.KindPublicRelayList); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_event_purge (event_id, created_at) VALUES ($1, 1000)
		ON CONFLICT (event_id) DO NOTHING
	`, lower); err != nil {
		t.Fatal(err)
	}
	kept, err = store.NewPostgresStore(pool).Retention.PurgeSupersededReplaceableEvents(
		ctx, time.Now().Add(time.Hour), time.Now().Add(-time.Hour), 20,
	)
	if err != nil {
		t.Fatal(err)
	}
	if kept != 0 {
		t.Fatalf("purged the winner while repair was queued: %d", kept)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE replaceable_state SET event_id = $1 WHERE pubkey = $2 AND kind = $3 AND d_tag = ''
	`, lower, pubkey, relayintel.KindPublicRelayList); err != nil {
		t.Fatal(err)
	}

	finishDerivation(t, ctx, pool, lower)
	callRepair(t, ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, lower).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("repair status after derivation = %s", status)
	}
	if err := pool.QueryRow(ctx, `SELECT needs_replay FROM relay_config_heads WHERE event_id = $1`, lower).Scan(&replay); err != nil {
		t.Fatal(err)
	}
	if replay {
		t.Fatal("completed projection still needs replay")
	}
}

func TestRepairLifecycleEdges(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	pubkey := strings.Repeat("cd", 32)
	lower := strings.Repeat("1", 64)
	higher := strings.Repeat("2", 64)
	insertStoredEvent(t, ctx, pool, storedEvent{id: lower, pubkey: pubkey, kind: 0, created: 50, tags: [][]string{{"d", "nope"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: higher, pubkey: pubkey, kind: 0, created: 50})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, 0, '', $2, 50, 1)
	`, pubkey, higher); err != nil {
		t.Fatal(err)
	}
	relayintel.SetProjectionEnabled(false)
	t.Cleanup(func() { relayintel.SetProjectionEnabled(true) })
	if callRepair(t, ctx, pool) != 1 {
		t.Fatal("profile tie was not repaired")
	}
	if winner := stateWinner(t, ctx, pool, pubkey, 0, ""); winner != lower {
		t.Fatalf("profile winner = %s", winner)
	}
	var heads int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_config_heads WHERE pubkey = $1`, pubkey).Scan(&heads); err != nil {
		t.Fatal(err)
	}
	if heads != 0 {
		t.Fatal("profile repair required a relay projection")
	}
	// The lower id carries an unrelated d tag. Ordinary kinds ignore it, so
	// derivation still writes the profile on the empty coordinate.
	finishDerivation(t, ctx, pool, lower)
	callRepair(t, ctx, pool)
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, lower).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("profile repair status = %s", status)
	}
	var profile string
	if err := pool.QueryRow(ctx, `SELECT metadata_event_id FROM profiles_latest WHERE pubkey = $1`, pubkey).Scan(&profile); err != nil {
		t.Fatal(err)
	}
	if profile != lower {
		t.Fatalf("profile projection = %s", profile)
	}
	var coordinates int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM replaceable_state WHERE pubkey = $1 AND kind = 0`, pubkey).Scan(&coordinates); err != nil {
		t.Fatal(err)
	}
	if coordinates != 1 {
		t.Fatalf("stray d tag created %d profile coordinates", coordinates)
	}

	resetRepairCursor(t, ctx, pool)
	emptyLow := strings.Repeat("3", 64)
	emptyHigh := strings.Repeat("4", 64)
	namedHigh := strings.Repeat("5", 64)
	for _, item := range []storedEvent{
		{id: emptyLow, pubkey: pubkey, kind: relayintel.KindRelaySet, created: 80, tags: [][]string{{"d", ""}, {"r", "wss://empty.example"}}},
		{id: emptyHigh, pubkey: pubkey, kind: relayintel.KindRelaySet, created: 80, tags: [][]string{{"d", ""}, {"r", "wss://empty-old.example"}}},
		{id: namedHigh, pubkey: pubkey, kind: relayintel.KindRelaySet, created: 80, tags: [][]string{{"d", "named"}, {"r", "wss://named.example"}}},
	} {
		insertStoredEvent(t, ctx, pool, item)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, $2, '', $3, 80, 1), ($1, $2, 'named', $4, 80, 1)
	`, pubkey, relayintel.KindRelaySet, emptyHigh, namedHigh); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	if stateWinner(t, ctx, pool, pubkey, relayintel.KindRelaySet, "") != emptyLow {
		t.Fatal("empty addressable coordinate was not repaired")
	}
	if stateWinner(t, ctx, pool, pubkey, relayintel.KindRelaySet, "named") != namedHigh {
		t.Fatal("a different d tag changed the named coordinate")
	}

	resetRepairCursor(t, ctx, pool)
	delLower := strings.Repeat("6", 64)
	delHigher := strings.Repeat("7", 64)
	delEvent := strings.Repeat("8", 64)
	insertStoredEvent(t, ctx, pool, storedEvent{id: delLower, pubkey: pubkey, kind: relayintel.KindDMRelays, created: 90, tags: [][]string{{"relay", "wss://dm.example"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: delHigher, pubkey: pubkey, kind: relayintel.KindDMRelays, created: 90, tags: [][]string{{"relay", "wss://dm-old.example"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: delEvent, pubkey: pubkey, kind: 5, created: 91, tags: [][]string{{"e", delLower}}})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, $2, '', $3, 90, 1)
	`, pubkey, relayintel.KindDMRelays, delHigher); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, 91, 1)
	`, delEvent, pubkey, delLower); err != nil {
		t.Fatal(err)
	}
	if n := callRepair(t, ctx, pool); n != 0 {
		t.Fatalf("deleted candidate repaired = %d", n)
	}
	if stateWinner(t, ctx, pool, pubkey, relayintel.KindDMRelays, "") != delHigher {
		t.Fatal("deleted lower id became canonical")
	}

	resetRepairCursor(t, ctx, pool)
	liveLower := strings.Repeat("9", 64)
	liveHigher := strings.Repeat("e", 64)
	newer := strings.Repeat("f", 64)
	insertStoredEvent(t, ctx, pool, storedEvent{id: liveLower, pubkey: pubkey, kind: relayintel.KindSearchRelays, created: 100, tags: [][]string{{"relay", "wss://search.example"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: liveHigher, pubkey: pubkey, kind: relayintel.KindSearchRelays, created: 100, tags: [][]string{{"relay", "wss://search-old.example"}}})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, $2, '', $3, 100, 1)
	`, pubkey, relayintel.KindSearchRelays, liveHigher); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	insertStoredEvent(t, ctx, pool, storedEvent{id: newer, pubkey: pubkey, kind: relayintel.KindSearchRelays, created: 200, tags: [][]string{{"relay", "wss://search-new.example"}}})
	if err := derivation.NewHandlers(pool).UpdateReplaceableState(ctx, newer); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	if stateWinner(t, ctx, pool, pubkey, relayintel.KindSearchRelays, "") != newer {
		t.Fatal("repair restored an older winner over a newer event")
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, liveLower).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "superseded" {
		t.Fatalf("superseded status = %s", status)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE jobs SET status = 'dead', attempts = max_attempts, finished_at = now(), last_error = 'stopped'
		WHERE idempotency_key = $1
	`, "derive_event_bundle:"+emptyLow); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE relay_intel_repairs SET status = 'queued', finished_at = NULL WHERE winner_event_id = $1
	`, emptyLow); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, emptyLow).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("exhausted repair status = %s", status)
	}
	if jobsFor(t, ctx, pool, emptyLow) != 1 {
		t.Fatal("exhausted repair created another job")
	}
}

func TestRepairSurvivesConcurrentWorkersAndRestart(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	pubkey := strings.Repeat("11", 32)
	lower := strings.Repeat("a", 64)
	higher := strings.Repeat("b", 64)
	insertStoredEvent(t, ctx, pool, storedEvent{id: lower, pubkey: pubkey, kind: relayintel.KindFavoriteRelays, created: 10, tags: [][]string{{"relay", "wss://fav.example"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: higher, pubkey: pubkey, kind: relayintel.KindFavoriteRelays, created: 10, tags: [][]string{{"relay", "wss://fav-old.example"}}})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, $2, '', $3, 10, 1)
	`, pubkey, relayintel.KindFavoriteRelays, higher); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			var n int
			errCh <- pool.QueryRow(ctx, `SELECT repair_replaceable_tie(10)`).Scan(&n)
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if stateWinner(t, ctx, pool, pubkey, relayintel.KindFavoriteRelays, "") != lower {
		t.Fatal("concurrent repair left the higher id")
	}
	if jobsFor(t, ctx, pool, lower) != 1 {
		t.Fatal("concurrent repair duplicated the job")
	}
	q := jobs.NewQueue(pool)
	claimed, err := q.ClaimAvailable(ctx, "repair-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	var runningID int64
	for _, job := range claimed {
		var payload struct {
			EventID string `json:"event_id"`
		}
		_ = json.Unmarshal(job.Payload, &payload)
		if payload.EventID == lower {
			runningID = job.ID
		}
	}
	if runningID == 0 {
		t.Fatal("winner job was not claimed")
	}
	callRepair(t, ctx, pool)
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, lower).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "running" {
		t.Fatalf("interrupted status = %s", status)
	}
	if err := derivation.EnsureRegisteredDerivations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := derivation.NewHandlers(pool).DeriveEventBundle(ctx, lower); err != nil {
		t.Fatal(err)
	}
	if err := q.CompleteJob(ctx, runningID, "repair-a"); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, lower).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "completed" {
		t.Fatalf("resumed status = %s", status)
	}
	if jobsFor(t, ctx, pool, lower) != 1 {
		t.Fatal("resume created another job")
	}
}

func TestReconcileIgnoresUnrelatedEventsAndTruncation(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	pubkey := strings.Repeat("22", 32)
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		SELECT lpad(i::text, 64, '0'), $1, 1, 0, repeat('c', 128), '', jsonb_build_object('id', lpad(i::text, 64, '0'), 'kind', 0)
		FROM generate_series(1, 2000) AS i
	`, pubkey); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		SELECT $1, 0, lpad(i::text, 8, '0'), lpad(i::text, 64, '0'), 1, 1
		FROM generate_series(1, 2000) AS i
	`, pubkey); err != nil {
		t.Fatal(err)
	}
	relayID := strings.Repeat("a", 64)
	tags := make([][]string, 0, 40)
	for i := 0; i < 40; i++ {
		tags = append(tags, []string{"r", fmt.Sprintf("wss://relay-%02d.example", i)})
	}
	insertStoredEvent(t, ctx, pool, storedEvent{id: relayID, pubkey: pubkey, kind: relayintel.KindPublicRelayList, created: 5000, tags: tags})
	if err := derivation.NewHandlers(pool).UpdateReplaceableState(ctx, relayID); err != nil {
		t.Fatal(err)
	}
	if err := relayintel.BackfillBatch(ctx, pool, 50); err != nil {
		t.Fatal(err)
	}
	var truncated, replay bool
	var head string
	if err := pool.QueryRow(ctx, `
		SELECT event_id, truncated, needs_replay FROM relay_config_heads
		WHERE pubkey = $1 AND source_kind = $2 AND d_tag = ''
	`, pubkey, relayintel.KindPublicRelayList).Scan(&head, &truncated, &replay); err != nil {
		t.Fatal(err)
	}
	if head != relayID || !truncated || replay {
		t.Fatalf("head=%s truncated=%v replay=%v", head, truncated, replay)
	}
	var updated time.Time
	if err := pool.QueryRow(ctx, `SELECT updated_at FROM relay_config_heads WHERE event_id = $1`, relayID).Scan(&updated); err != nil {
		t.Fatal(err)
	}
	if err := relayintel.BackfillBatch(ctx, pool, 50); err != nil {
		t.Fatal(err)
	}
	var again time.Time
	if err := pool.QueryRow(ctx, `SELECT updated_at FROM relay_config_heads WHERE event_id = $1`, relayID).Scan(&again); err != nil {
		t.Fatal(err)
	}
	if !again.Equal(updated) {
		t.Fatal("truncated projection was written again")
	}
	var idle bool
	if err := pool.QueryRow(ctx, `SELECT idle FROM relay_intel_work_cursor WHERE work_name = 'projection_reconcile'`).Scan(&idle); err != nil {
		t.Fatal(err)
	}
	if !idle {
		t.Fatal("reconcile did not go idle after the relay page")
	}

	staleID := strings.Repeat("b", 64)
	freshID := strings.Repeat("c", 64)
	other := strings.Repeat("33", 32)
	insertStoredEvent(t, ctx, pool, storedEvent{id: staleID, pubkey: other, kind: relayintel.KindDMRelays, created: 10, tags: [][]string{{"relay", "wss://old.example"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: freshID, pubkey: other, kind: relayintel.KindDMRelays, created: 11, tags: [][]string{{"relay", "wss://new.example"}}})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, $2, '', $3, 11, 1)
	`, other, relayintel.KindDMRelays, freshID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_config_heads (pubkey, source_kind, d_tag, event_id, created_at, declaration_source)
		VALUES ($1, $2, '', $3, 10, 'modern')
	`, other, relayintel.KindDMRelays, staleID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE relay_intel_work_cursor
		SET idle = false, next_run_at = now(), cursor_kind = 0, cursor_pubkey = '', cursor_d_tag = ''
		WHERE work_name = 'projection_reconcile'
	`); err != nil {
		t.Fatal(err)
	}
	if err := relayintel.BackfillBatch(ctx, pool, 50); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = $2
	`, other, relayintel.KindDMRelays).Scan(&head); err != nil {
		t.Fatal(err)
	}
	if head != freshID {
		t.Fatalf("stale head = %s", head)
	}

	deletedPub := strings.Repeat("44", 32)
	deletedID := strings.Repeat("d", 64)
	kind5 := strings.Repeat("e", 64)
	insertStoredEvent(t, ctx, pool, storedEvent{id: deletedID, pubkey: deletedPub, kind: relayintel.KindSearchRelays, created: 12, tags: [][]string{{"relay", "wss://gone.example"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: kind5, pubkey: deletedPub, kind: 5, created: 13, tags: [][]string{{"a", "10007:" + deletedPub}}})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, $2, '', $3, 12, 1)
	`, deletedPub, relayintel.KindSearchRelays, deletedID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `SELECT backfill_deletion_addresses(20)`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE relay_intel_work_cursor
		SET idle = false, next_run_at = now(), cursor_kind = 0, cursor_pubkey = '', cursor_d_tag = ''
		WHERE work_name = 'projection_reconcile'
	`); err != nil {
		t.Fatal(err)
	}
	if err := relayintel.BackfillBatch(ctx, pool, 50); err != nil {
		t.Fatal(err)
	}
	var resurrected int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_config_heads WHERE pubkey = $1`, deletedPub).Scan(&resurrected); err != nil {
		t.Fatal(err)
	}
	if resurrected != 0 {
		t.Fatal("deleted configuration was projected")
	}
}

func TestDeletionBackfillIgnoresOverflowAndStrayDTags(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	pubkey := strings.Repeat("55", 32)
	bad := strings.Repeat("a", 64)
	good := strings.Repeat("b", 64)
	named := strings.Repeat("c", 64)
	insertStoredEvent(t, ctx, pool, storedEvent{id: bad, pubkey: pubkey, kind: 5, created: 1, tags: [][]string{{"a", "9999999999999:" + pubkey}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: good, pubkey: pubkey, kind: 5, created: 2, tags: [][]string{{"a", "3:" + pubkey + ":unrelated"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: named, pubkey: pubkey, kind: 5, created: 3, tags: [][]string{{"a", "30002:" + pubkey + ":named"}, {"a", "30002:" + pubkey}}})
	var inserted int
	if err := pool.QueryRow(ctx, `SELECT backfill_deletion_addresses(20)`).Scan(&inserted); err != nil {
		t.Fatal(err)
	}
	if inserted < 3 {
		t.Fatalf("inserted = %d", inserted)
	}
	var stray int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM deletion_addresses WHERE event_id = $1 AND kind = 3 AND d_tag = ''
	`, good).Scan(&stray); err != nil {
		t.Fatal(err)
	}
	if stray != 1 {
		t.Fatal("ordinary replaceable deletion kept a stray d tag")
	}
	var emptyD, namedD int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE d_tag = ''), count(*) FILTER (WHERE d_tag = 'named')
		FROM deletion_addresses WHERE event_id = $1 AND kind = 30002
	`, named).Scan(&emptyD, &namedD); err != nil {
		t.Fatal(err)
	}
	if emptyD != 1 || namedD != 1 {
		t.Fatalf("addressable coordinates empty=%d named=%d", emptyD, namedD)
	}
	var overflow int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM deletion_addresses WHERE event_id = $1`, bad).Scan(&overflow); err != nil {
		t.Fatal(err)
	}
	if overflow != 0 {
		t.Fatal("overflowing kind was stored")
	}
}

func TestRepairKeepsARunningDerivationLock(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	pubkey := strings.Repeat("ab", 32)
	lower := strings.Repeat("1", 64)
	higher := strings.Repeat("9", 64)
	newer := strings.Repeat("c", 64)
	insertStoredEvent(t, ctx, pool, storedEvent{id: lower, pubkey: pubkey, kind: relayintel.KindPublicRelayList, created: 40, tags: [][]string{{"r", "wss://kept.example"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: higher, pubkey: pubkey, kind: relayintel.KindPublicRelayList, created: 40, tags: [][]string{{"r", "wss://dropped.example"}}})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, 10002, '', $2, 40, 1)
	`, pubkey, higher); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'running', attempts = 1, locked_by = 'worker-a', locked_at = now()
		WHERE idempotency_key = $1
	`, "derive_event_bundle:"+lower); err != nil {
		t.Fatal(err)
	}
	if callRepair(t, ctx, pool) != 1 {
		t.Fatal("running derivation was not recorded")
	}
	var status, lockedBy string
	if err := pool.QueryRow(ctx, `SELECT status, locked_by FROM jobs WHERE idempotency_key = $1`, "derive_event_bundle:"+lower).Scan(&status, &lockedBy); err != nil {
		t.Fatal(err)
	}
	if status != "running" || lockedBy != "worker-a" {
		t.Fatalf("job status=%s locked_by=%s", status, lockedBy)
	}
	if jobsFor(t, ctx, pool, lower) != 1 {
		t.Fatal("repair duplicated a derivation job")
	}
	var repairStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, lower).Scan(&repairStatus); err != nil {
		t.Fatal(err)
	}
	if repairStatus != "running" {
		t.Fatalf("repair status = %s", repairStatus)
	}
	callRepair(t, ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT status, locked_by FROM jobs WHERE idempotency_key = $1`, "derive_event_bundle:"+lower).Scan(&status, &lockedBy); err != nil {
		t.Fatal(err)
	}
	if status != "running" || lockedBy != "worker-a" {
		t.Fatal("a second repair pass stole the lock")
	}

	if _, err := pool.Exec(ctx, `
		UPDATE jobs SET status = 'pending', locked_by = NULL, locked_at = NULL WHERE idempotency_key = $1
	`, "derive_event_bundle:"+lower); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE replaceable_state SET event_id = $2 WHERE pubkey = $1 AND kind = 10002
	`, pubkey, higher); err != nil {
		t.Fatal(err)
	}
	resetRepairCursor(t, ctx, pool)
	callRepair(t, ctx, pool)
	if jobsFor(t, ctx, pool, lower) != 1 {
		t.Fatal("a pending job was duplicated")
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM jobs WHERE idempotency_key = $1`, "derive_event_bundle:"+lower).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "pending" {
		t.Fatalf("pending job status = %s", status)
	}

	if _, err := pool.Exec(ctx, `UPDATE jobs SET status = 'succeeded', finished_at = now() WHERE idempotency_key = $1`, "derive_event_bundle:"+lower); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE replaceable_state SET event_id = $2 WHERE pubkey = $1 AND kind = 10002`, pubkey, higher); err != nil {
		t.Fatal(err)
	}
	resetRepairCursor(t, ctx, pool)
	callRepair(t, ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT status, COALESCE(locked_by, '') FROM jobs WHERE idempotency_key = $1`, "derive_event_bundle:"+lower).Scan(&status, &lockedBy); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || lockedBy != "" {
		t.Fatalf("completed derivation was not replayed safely: status=%s lock=%q", status, lockedBy)
	}

	insertStoredEvent(t, ctx, pool, storedEvent{id: newer, pubkey: pubkey, kind: relayintel.KindPublicRelayList, created: 80, tags: [][]string{{"r", "wss://newer.example"}}})
	if _, err := pool.Exec(ctx, `
		UPDATE replaceable_state SET event_id = $2, created_at = 80 WHERE pubkey = $1 AND kind = 10002
	`, pubkey, newer); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, lower).Scan(&repairStatus); err != nil {
		t.Fatal(err)
	}
	if repairStatus != "superseded" {
		t.Fatalf("superseded status = %s", repairStatus)
	}
	if jobsFor(t, ctx, pool, lower) != 1 {
		t.Fatal("a superseded repair started another replay")
	}

	if _, err := pool.Exec(ctx, `
		UPDATE replaceable_state SET event_id = $2, created_at = 40 WHERE pubkey = $1 AND kind = 10002
	`, pubkey, lower); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE jobs SET status = 'dead', attempts = max_attempts, finished_at = now() WHERE idempotency_key = $1
	`, "derive_event_bundle:"+lower); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE relay_intel_repairs SET status = 'queued', finished_at = NULL, attempts = 5 WHERE winner_event_id = $1
	`, lower); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	callRepair(t, ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, lower).Scan(&repairStatus); err != nil {
		t.Fatal(err)
	}
	if repairStatus != "failed" {
		t.Fatalf("exhausted status = %s", repairStatus)
	}
	if jobsFor(t, ctx, pool, lower) != 1 {
		t.Fatal("exhausted repair entered another retry")
	}
}

func TestDeletedRepairUsesTheRemainingCanonicalEvent(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	pubkey := strings.Repeat("ef", 32)
	loser := strings.Repeat("b", 64)
	winner := strings.Repeat("a", 64)
	third := strings.Repeat("d", 64)
	insertStoredEvent(t, ctx, pool, storedEvent{id: winner, pubkey: pubkey, kind: relayintel.KindPublicRelayList, created: 10, tags: [][]string{{"r", "wss://winner.example"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: loser, pubkey: pubkey, kind: relayintel.KindPublicRelayList, created: 10, tags: [][]string{{"r", "wss://loser.example"}}})
	insertStoredEvent(t, ctx, pool, storedEvent{id: third, pubkey: pubkey, kind: relayintel.KindPublicRelayList, created: 30, tags: [][]string{{"r", "wss://third.example"}}})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, 10002, '', $2, 10, 1)
	`, pubkey, loser); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	if _, err := pool.Exec(ctx, `
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, 40, 1)
	`, strings.Repeat("e", 64), pubkey, winner); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	if got := stateWinner(t, ctx, pool, pubkey, 10002, ""); got != third {
		t.Fatalf("remaining winner = %s", got)
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, winner).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "queued" {
		t.Fatalf("replacement repair status before derivation = %s", status)
	}
	finishDerivation(t, ctx, pool, third)
	callRepair(t, ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, winner).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "deleted" {
		t.Fatalf("deleted repair status = %s", status)
	}
	var head string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002`, pubkey).Scan(&head); err != nil {
		t.Fatal(err)
	}
	if head != third {
		t.Fatalf("head = %s", head)
	}
}

func TestDeletedWinnerProjectionReconciliation(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	next := 100
	id := func() string {
		next++
		return fmt.Sprintf("%064x", next)
	}
	pubkey := func() string { return id() }

	seed := func(t *testing.T, owner, winner, other string, winnerAt, otherAt int64) {
		t.Helper()
		insertStoredEvent(t, ctx, pool, storedEvent{
			id: winner, pubkey: owner, kind: relayintel.KindPublicRelayList, created: winnerAt,
			tags: [][]string{{"r", "wss://deleted.example"}},
		})
		insertStoredEvent(t, ctx, pool, storedEvent{
			id: other, pubkey: owner, kind: relayintel.KindPublicRelayList, created: otherAt,
			tags: [][]string{{"r", "wss://kept.example"}},
		})
		if _, err := pool.Exec(ctx, `
			INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
			VALUES ($1, 10002, '', $2, $3, 1)
		`, owner, winner, winnerAt); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO relay_config_heads (
				pubkey, source_kind, d_tag, event_id, created_at, declaration_source
			) VALUES ($1, 10002, '', $2, $3, 'modern')
		`, owner, winner, winnerAt); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO relay_role_memberships (
				pubkey, role, url_key, source_kind, d_tag, endpoint_url, endpoint_type,
				source_event_id, created_at, declaration_source, public_safe
			) VALUES ($1, 'public_outbox', 'deleted', 10002, '', 'wss://deleted.example', 'nostr_relay', $2, $3, 'modern', true)
		`, owner, winner, winnerAt); err != nil {
			t.Fatal(err)
		}
	}
	deleteEvent := func(t *testing.T, owner, target string, created int64) {
		t.Helper()
		tomb := id()
		insertStoredEvent(t, ctx, pool, storedEvent{id: tomb, pubkey: owner, kind: 5, created: created})
		if _, err := pool.Exec(ctx, `
			INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
			VALUES ($1, $2, $3, $4, 1)
		`, tomb, owner, target, created); err != nil {
			t.Fatal(err)
		}
	}
	settle := func(t *testing.T, owner, winner, replacement string) {
		t.Helper()
		callRepair(t, ctx, pool)
		if got := stateWinner(t, ctx, pool, owner, 10002, ""); got != replacement {
			t.Fatalf("canonical event = %s, want %s", got, replacement)
		}
		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, winner).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == "deleted" || status == "completed" {
			t.Fatalf("repair finished before the replacement was derived: %s", status)
		}
		finishDerivation(t, ctx, pool, replacement)
		callRepair(t, ctx, pool)
		if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, winner).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "deleted" {
			t.Fatalf("repair status = %s", status)
		}
		var head, list string
		var replay bool
		if err := pool.QueryRow(ctx, `
			SELECT event_id, needs_replay FROM relay_config_heads
			WHERE pubkey = $1 AND source_kind = 10002 AND d_tag = ''
		`, owner).Scan(&head, &replay); err != nil {
			t.Fatal(err)
		}
		if head != replacement || replay {
			t.Fatalf("head=%s replay=%v", head, replay)
		}
		if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_lists_latest WHERE pubkey = $1`, owner).Scan(&list); err != nil {
			t.Fatal(err)
		}
		if list != replacement {
			t.Fatalf("relay list = %s", list)
		}
		var stale int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM relay_role_memberships
			WHERE pubkey = $1 AND source_event_id IS DISTINCT FROM $2
		`, owner, replacement).Scan(&stale); err != nil {
			t.Fatal(err)
		}
		if stale != 0 {
			t.Fatal("a membership still cites another event")
		}
		var keptURL string
		if err := pool.QueryRow(ctx, `
			SELECT endpoint_url FROM relay_role_memberships WHERE pubkey = $1 AND role = 'public_outbox'
		`, owner).Scan(&keptURL); err != nil {
			t.Fatal(err)
		}
		if keptURL != "wss://kept.example" {
			t.Fatalf("outbox = %s", keptURL)
		}
		var tomb int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM deletion_events WHERE target_event_id = $1 AND deleter_pubkey = $2
		`, winner, owner).Scan(&tomb); err != nil {
			t.Fatal(err)
		}
		if tomb != 1 {
			t.Fatal("deletion tombstone was removed")
		}
	}

	t.Run("newer remaining candidate", func(t *testing.T) {
		owner, winner, other := pubkey(), id(), id()
		seed(t, owner, winner, other, 10, 30)
		deleteEvent(t, owner, winner, 40)
		settle(t, owner, winner, other)
	})
	t.Run("older remaining candidate", func(t *testing.T) {
		owner, winner, other := pubkey(), id(), id()
		seed(t, owner, winner, other, 30, 10)
		deleteEvent(t, owner, winner, 40)
		settle(t, owner, winner, other)
	})
	t.Run("same timestamp higher id", func(t *testing.T) {
		owner := pubkey()
		winner := fmt.Sprintf("%064x", 1)
		other := fmt.Sprintf("%064x", 2)
		seed(t, owner, winner, other, 10, 10)
		deleteEvent(t, owner, winner, 40)
		settle(t, owner, winner, other)
	})
	t.Run("address deletion covers every candidate", func(t *testing.T) {
		owner, winner, other := pubkey(), id(), id()
		seed(t, owner, winner, other, 20, 10)
		tomb := id()
		insertStoredEvent(t, ctx, pool, storedEvent{id: tomb, pubkey: owner, kind: 5, created: 50})
		if _, err := pool.Exec(ctx, `
			INSERT INTO deletion_addresses (event_id, pubkey, kind, d_tag, created_at)
			VALUES ($1, $2, 10002, '', 50)
		`, tomb, owner); err != nil {
			t.Fatal(err)
		}
		callRepair(t, ctx, pool)
		var stateCount, headCount, memberCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM replaceable_state WHERE pubkey = $1 AND kind = 10002`, owner).Scan(&stateCount); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002`, owner).Scan(&headCount); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_role_memberships WHERE pubkey = $1`, owner).Scan(&memberCount); err != nil {
			t.Fatal(err)
		}
		if stateCount != 0 || headCount != 0 || memberCount != 0 {
			t.Fatalf("state=%d head=%d memberships=%d", stateCount, headCount, memberCount)
		}
		var addresses int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM deletion_addresses WHERE pubkey = $1 AND kind = 10002`, owner).Scan(&addresses); err != nil {
			t.Fatal(err)
		}
		if addresses != 1 {
			t.Fatal("address tombstone was removed")
		}
	})
	t.Run("no remaining candidate", func(t *testing.T) {
		owner, winner := pubkey(), id()
		insertStoredEvent(t, ctx, pool, storedEvent{
			id: winner, pubkey: owner, kind: relayintel.KindPublicRelayList, created: 10,
			tags: [][]string{{"r", "wss://only.example"}},
		})
		if _, err := pool.Exec(ctx, `
			INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
			VALUES ($1, 10002, '', $2, 10, 1)
		`, owner, winner); err != nil {
			t.Fatal(err)
		}
		deleteEvent(t, owner, winner, 20)
		callRepair(t, ctx, pool)
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM replaceable_state WHERE pubkey = $1`, owner).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatal("deleted coordinate still has canonical state")
		}
	})
	t.Run("concurrent newer event", func(t *testing.T) {
		owner, winner, other := pubkey(), id(), id()
		seed(t, owner, winner, other, 20, 10)
		deleteEvent(t, owner, winner, 30)
		callRepair(t, ctx, pool)
		if stateWinner(t, ctx, pool, owner, 10002, "") != other {
			t.Fatal("replacement was not selected")
		}
		newer := id()
		insertStoredEvent(t, ctx, pool, storedEvent{
			id: newer, pubkey: owner, kind: relayintel.KindPublicRelayList, created: 50,
			tags: [][]string{{"r", "wss://newer.example"}},
		})
		finishDerivation(t, ctx, pool, newer)
		callRepair(t, ctx, pool)
		if stateWinner(t, ctx, pool, owner, 10002, "") != newer {
			t.Fatal("concurrent event did not become canonical")
		}
		var status, head string
		if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, winner).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "deleted" && status != "superseded" {
			t.Fatalf("status = %s", status)
		}
		if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002`, owner).Scan(&head); err != nil {
			t.Fatal(err)
		}
		if head != newer {
			t.Fatalf("head = %s", head)
		}
	})
	t.Run("concurrent deletion of the replacement", func(t *testing.T) {
		owner, winner, other := pubkey(), id(), id()
		seed(t, owner, winner, other, 20, 10)
		deleteEvent(t, owner, winner, 30)
		callRepair(t, ctx, pool)
		deleteEvent(t, owner, other, 40)
		callRepair(t, ctx, pool)
		var stateCount int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM replaceable_state WHERE pubkey = $1 AND kind = 10002`, owner).Scan(&stateCount); err != nil {
			t.Fatal(err)
		}
		if stateCount != 0 {
			t.Fatal("a deleted replacement stayed canonical")
		}
	})
	t.Run("replay after restart", func(t *testing.T) {
		owner, winner, other := pubkey(), id(), id()
		seed(t, owner, winner, other, 30, 10)
		deleteEvent(t, owner, winner, 40)
		callRepair(t, ctx, pool)
		callRepair(t, ctx, pool)
		if jobsFor(t, ctx, pool, other) != 1 {
			t.Fatal("restart created another replacement job")
		}
		if stateWinner(t, ctx, pool, owner, 10002, "") != other {
			t.Fatal("restart moved canonical state")
		}
		finishDerivation(t, ctx, pool, other)
		callRepair(t, ctx, pool)
		callRepair(t, ctx, pool)
		var status, head string
		if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, winner).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "deleted" {
			t.Fatalf("status = %s", status)
		}
		if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002`, owner).Scan(&head); err != nil {
			t.Fatal(err)
		}
		if head != other {
			t.Fatalf("head = %s", head)
		}
	})
	t.Run("retention keeps the replacement", func(t *testing.T) {
		owner, winner, other := pubkey(), id(), id()
		seed(t, owner, winner, other, 30, 10)
		deleteEvent(t, owner, winner, 40)
		callRepair(t, ctx, pool)
		if _, err := pool.Exec(ctx, `
			INSERT INTO replaceable_event_purge (event_id, created_at) VALUES ($1, 10)
			ON CONFLICT (event_id) DO NOTHING
		`, other); err != nil {
			t.Fatal(err)
		}
		deleted, err := store.NewPostgresStore(pool).Retention.PurgeSupersededReplaceableEvents(
			ctx, time.Now().Add(time.Hour), time.Now().Add(-time.Hour), 50,
		)
		if err != nil {
			t.Fatal(err)
		}
		if deleted != 0 {
			t.Fatalf("purged events during reconciliation: %d", deleted)
		}
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)`, other).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatal("retention deleted the canonical replacement")
		}
		finishDerivation(t, ctx, pool, other)
		callRepair(t, ctx, pool)
	})
	t.Run("inspection diagnostics and optimization agree", func(t *testing.T) {
		owner, winner, other := pubkey(), id(), id()
		seed(t, owner, winner, other, 30, 10)
		deleteEvent(t, owner, winner, 40)
		settle(t, owner, winner, other)
		svc := relayintel.NewService(pool, relayintel.Options{})
		cfg, err := svc.Config(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Account.ModernEventID != other {
			t.Fatalf("config event = %s", cfg.Account.ModernEventID)
		}
		diag, err := svc.Diagnostics(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := svc.Optimize(ctx, relayintel.PlanRequest{Pubkey: owner})
		if err != nil {
			t.Fatal(err)
		}
		for _, report := range append(append([]relayintel.SourceReport{}, cfg.Sources...), append(diag.Sources, plan.Sources...)...) {
			if report.Kind == relayintel.KindPublicRelayList && report.CanonicalEventID != other {
				t.Fatalf("source canonical = %s status=%s", report.CanonicalEventID, report.Status)
			}
			if report.ProjectionEventID == winner || report.CanonicalEventID == winner {
				t.Fatalf("deleted winner still reported: %+v", report)
			}
		}
	})
	t.Run("ordinary older replay does not replace a live head", func(t *testing.T) {
		owner, winner, other := pubkey(), id(), id()
		seed(t, owner, winner, other, 30, 10)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := relayintel.Project(ctx, tx, relayintel.EventInput{
			Pubkey: owner, EventID: other, Kind: relayintel.KindPublicRelayList, CreatedAt: 10,
			Tags: [][]string{{"r", "wss://kept.example"}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var head string
		if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002`, owner).Scan(&head); err != nil {
			t.Fatal(err)
		}
		if head != winner {
			t.Fatalf("live head changed to %s", head)
		}
	})
}

func TestNonRelayRepairChecksDependentProjections(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	owner := fmt.Sprintf("%064x", 80)
	newer := fmt.Sprintf("%064x", 81)
	older := fmt.Sprintf("%064x", 82)
	insertStoredEvent(t, ctx, pool, storedEvent{
		id: newer, pubkey: owner, kind: 0, created: 30, content: `{"name":"new"}`,
	})
	insertStoredEvent(t, ctx, pool, storedEvent{
		id: older, pubkey: owner, kind: 0, created: 10, content: `{"name":"old"}`,
	})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, 0, '', $2, 30, 1)
	`, owner, newer); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO profiles_latest (
			pubkey, metadata_event_id, metadata_created_at, name, profile_json, derivation_version
		) VALUES ($1, $2, 30, 'new', '{"name":"new"}', 1)
	`, owner, newer); err != nil {
		t.Fatal(err)
	}
	tomb := fmt.Sprintf("%064x", 83)
	insertStoredEvent(t, ctx, pool, storedEvent{id: tomb, pubkey: owner, kind: 5, created: 40})
	if _, err := pool.Exec(ctx, `
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, 40, 1)
	`, tomb, owner, newer); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	if stateWinner(t, ctx, pool, owner, 0, "") != older {
		t.Fatal("profile canonical state did not move to the remaining event")
	}
	var profile string
	if err := pool.QueryRow(ctx, `SELECT metadata_event_id FROM profiles_latest WHERE pubkey = $1`, owner).Scan(&profile); err != nil {
		t.Fatal(err)
	}
	if profile == older {
		t.Fatal("profile projection changed before derivation")
	}
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, newer).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status == "deleted" || status == "completed" {
		t.Fatalf("profile repair completed with a stale projection: %s", status)
	}
	callRepair(t, ctx, pool)
	if jobsFor(t, ctx, pool, older) != 1 {
		t.Fatal("interrupted profile repair created another job")
	}
	finishDerivation(t, ctx, pool, older)
	if err := pool.QueryRow(ctx, `SELECT metadata_event_id FROM profiles_latest WHERE pubkey = $1`, owner).Scan(&profile); err != nil {
		t.Fatal(err)
	}
	if profile != older {
		t.Fatalf("profile projection = %s", profile)
	}
	if _, err := pool.Exec(ctx, `UPDATE profiles_latest SET metadata_event_id = $2 WHERE pubkey = $1`, owner, newer); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, newer).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status == "deleted" || status == "completed" {
		t.Fatal("stale profile projection was accepted")
	}
	finishDerivation(t, ctx, pool, older)
	callRepair(t, ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, newer).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "deleted" {
		t.Fatalf("profile repair status = %s", status)
	}

	contactOwner := fmt.Sprintf("%064x", 90)
	contactNew := fmt.Sprintf("%064x", 91)
	contactOld := fmt.Sprintf("%064x", 92)
	follow := fmt.Sprintf("%064x", 93)
	insertStoredEvent(t, ctx, pool, storedEvent{
		id: contactNew, pubkey: contactOwner, kind: 3, created: 30,
		tags: [][]string{{"p", follow}},
	})
	insertStoredEvent(t, ctx, pool, storedEvent{
		id: contactOld, pubkey: contactOwner, kind: 3, created: 10,
		tags: [][]string{{"p", follow}, {"r", "wss://legacy.example"}},
	})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, 3, '', $2, 30, 1)
	`, contactOwner, contactNew); err != nil {
		t.Fatal(err)
	}
	contactTomb := fmt.Sprintf("%064x", 94)
	insertStoredEvent(t, ctx, pool, storedEvent{id: contactTomb, pubkey: contactOwner, kind: 5, created: 40})
	if _, err := pool.Exec(ctx, `
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, 40, 1)
	`, contactTomb, contactOwner, contactNew); err != nil {
		t.Fatal(err)
	}
	callRepair(t, ctx, pool)
	finishDerivation(t, ctx, pool, contactOld)
	callRepair(t, ctx, pool)
	var contactEvent string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM contact_lists_latest WHERE pubkey = $1`, contactOwner).Scan(&contactEvent); err != nil {
		t.Fatal(err)
	}
	if contactEvent != contactOld {
		t.Fatalf("contact list = %s", contactEvent)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, contactNew).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "deleted" {
		t.Fatalf("contact repair status = %s", status)
	}

	live := fmt.Sprintf("%064x", 95)
	insertStoredEvent(t, ctx, pool, storedEvent{
		id: live, pubkey: owner, kind: 0, created: 60, content: `{"name":"live"}`,
	})
	finishDerivation(t, ctx, pool, live)
	if stateWinner(t, ctx, pool, owner, 0, "") != live {
		t.Fatal("a newer profile did not supersede the repaired event")
	}
}

func TestBackfillDoesNotDeadlockOnOneConnection(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	var schema string
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dbtest.DatabaseURL(t, "relay intel repair"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	tiny, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tiny.Close)
	limited, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := relayintel.BackfillBatch(limited, tiny, 10); err != nil {
		t.Fatal(err)
	}
	if err := relayintel.RepairCanonicalBatch(limited, tiny, 10); err != nil {
		t.Fatal(err)
	}
}

type storedEvent struct {
	id, pubkey string
	kind       int
	created    int64
	content    string
	tags       [][]string
}

func repairPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay intel repair"), "relayrepair")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	return pool
}

func insertStoredEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, item storedEvent) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id": item.id, "pubkey": item.pubkey, "created_at": item.created, "kind": item.kind,
		"tags": item.tags, "content": item.content, "sig": strings.Repeat("c", 128),
	})
	if err != nil {
		t.Fatal(err)
	}
	event := model.Event{
		ID: item.id, Pubkey: item.pubkey, CreatedAt: item.created, Kind: item.kind,
		Sig: strings.Repeat("c", 128), Content: item.content, RawJSON: raw,
		FirstSeenAt: time.Now().UTC(), InsertedAt: time.Now().UTC(),
	}
	if err := store.NewPostgresStore(pool).InsertCanonicalEvent(ctx, event, item.tags, "wss://relay.example", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

func callRepair(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(10)`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func stateWinner(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string, kind int, dTag string) string {
	t.Helper()
	var id string
	if err := pool.QueryRow(ctx, `
		SELECT event_id FROM replaceable_state WHERE pubkey = $1 AND kind = $2 AND d_tag = $3
	`, pubkey, kind, dTag).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func jobsFor(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE idempotency_key = $1`, "derive_event_bundle:"+eventID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func resetRepairCursor(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		UPDATE relay_intel_work_cursor
		SET idle = false, next_run_at = now(), cursor_pubkey = '', cursor_kind = 0, cursor_d_tag = ''
		WHERE work_name = 'tie_repair'
	`); err != nil {
		t.Fatal(err)
	}
}

func finishDerivation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string) {
	t.Helper()
	if err := derivation.EnsureRegisteredDerivations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := derivation.NewHandlers(pool).DeriveEventBundle(ctx, eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'succeeded', finished_at = now(), locked_at = NULL, locked_by = NULL
		WHERE idempotency_key = $1
	`, "derive_event_bundle:"+eventID); err != nil {
		t.Fatal(err)
	}
}
