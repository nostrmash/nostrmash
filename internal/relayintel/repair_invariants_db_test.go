package relayintel_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/relayintel"
)

func TestRepairRequiresRealProjections(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)

	t.Run("missing profile", func(t *testing.T) {
		owner, newer, older := repairIDs(1)
		insertStoredEvent(t, ctx, pool, storedEvent{id: newer, pubkey: owner, kind: 0, created: 30, content: `{"name":"new"}`})
		insertStoredEvent(t, ctx, pool, storedEvent{id: older, pubkey: owner, kind: 0, created: 10, content: `{"name":"old"}`})
		insertCanonical(t, ctx, pool, owner, 0, "", newer, 30)
		insertProfile(t, ctx, pool, owner, newer, 30)
		insertEventTombstone(t, ctx, pool, owner, newer, repairHex(19))
		resetRepairCursor(t, ctx, pool)
		callRepair(t, ctx, pool)
		if stateWinner(t, ctx, pool, owner, 0, "") != older {
			t.Fatal("canonical winner did not move to the remaining profile")
		}
		markJobSucceeded(t, ctx, pool, older)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, newer); status == "deleted" || status == "completed" {
			t.Fatalf("missing profile completed the repair: %s", status)
		}
		finishDerivation(t, ctx, pool, older)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, newer); status != "deleted" {
			t.Fatalf("status after the profile existed = %s", status)
		}
		var profile string
		if err := pool.QueryRow(ctx, `SELECT metadata_event_id FROM profiles_latest WHERE pubkey = $1`, owner).Scan(&profile); err != nil {
			t.Fatal(err)
		}
		if profile != older {
			t.Fatalf("profile = %s", profile)
		}
	})

	t.Run("missing relay head", func(t *testing.T) {
		owner, newer, older := repairIDs(2)
		insertStoredEvent(t, ctx, pool, storedEvent{id: newer, pubkey: owner, kind: relayintel.KindPublicRelayList, created: 30, tags: [][]string{{"r", "wss://new.example"}}})
		insertStoredEvent(t, ctx, pool, storedEvent{id: older, pubkey: owner, kind: relayintel.KindPublicRelayList, created: 10, tags: [][]string{{"r", "wss://old.example"}}})
		insertCanonical(t, ctx, pool, owner, relayintel.KindPublicRelayList, "", newer, 30)
		insertEventTombstone(t, ctx, pool, owner, newer, repairHex(29))
		resetRepairCursor(t, ctx, pool)
		callRepair(t, ctx, pool)
		markJobSucceeded(t, ctx, pool, older)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, newer); status == "deleted" || status == "completed" {
			t.Fatalf("missing head completed the repair: %s", status)
		}
		var heads int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002`, owner).Scan(&heads); err != nil {
			t.Fatal(err)
		}
		if heads != 0 {
			t.Fatal("a missing projection was filled without derivation")
		}
		finishDerivation(t, ctx, pool, older)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, newer); status != "deleted" {
			t.Fatalf("status after the head existed = %s", status)
		}
	})

	t.Run("wrong membership and replay flag", func(t *testing.T) {
		owner, lower, higher := repairIDs(3)
		insertStoredEvent(t, ctx, pool, storedEvent{id: lower, pubkey: owner, kind: relayintel.KindFavoriteRelays, created: 10, tags: [][]string{{"relay", "wss://fav.example"}}})
		insertStoredEvent(t, ctx, pool, storedEvent{id: higher, pubkey: owner, kind: relayintel.KindFavoriteRelays, created: 10, tags: [][]string{{"relay", "wss://other.example"}}})
		insertCanonical(t, ctx, pool, owner, relayintel.KindFavoriteRelays, "", higher, 10)
		resetRepairCursor(t, ctx, pool)
		callRepair(t, ctx, pool)
		finishDerivation(t, ctx, pool, lower)
		if _, err := pool.Exec(ctx, `
			INSERT INTO relay_role_memberships (
				pubkey, role, url_key, source_kind, d_tag, endpoint_url, endpoint_type,
				source_event_id, created_at, declaration_source, public_safe
			) VALUES ($1, 'favorite', 'stale', $2, '', 'wss://stale.example', 'nostr_relay', $3, 10, 'modern', true)
		`, owner, relayintel.KindFavoriteRelays, higher); err != nil {
			t.Fatal(err)
		}
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, lower); status == "completed" {
			t.Fatal("a membership from another event completed the repair")
		}
		if _, err := pool.Exec(ctx, `DELETE FROM relay_role_memberships WHERE source_event_id = $1`, higher); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE relay_config_heads SET needs_replay = true
			WHERE pubkey = $1 AND source_kind = $2
		`, owner, relayintel.KindFavoriteRelays); err != nil {
			t.Fatal(err)
		}
		markJobSucceeded(t, ctx, pool, lower)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, lower); status == "completed" {
			t.Fatal("needs_replay completed the repair")
		}
		if _, err := pool.Exec(ctx, `
			UPDATE relay_config_heads SET needs_replay = false
			WHERE pubkey = $1 AND source_kind = $2
		`, owner, relayintel.KindFavoriteRelays); err != nil {
			t.Fatal(err)
		}
		markJobSucceeded(t, ctx, pool, lower)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, lower); status != "completed" {
			t.Fatalf("status after the projection matched = %s", status)
		}
	})

	t.Run("empty and truncated projections", func(t *testing.T) {
		owner, lower, higher := repairIDs(4)
		insertStoredEvent(t, ctx, pool, storedEvent{id: lower, pubkey: owner, kind: relayintel.KindPublicRelayList, created: 10})
		insertStoredEvent(t, ctx, pool, storedEvent{id: higher, pubkey: owner, kind: relayintel.KindPublicRelayList, created: 10, tags: [][]string{{"r", "wss://filled.example"}}})
		insertCanonical(t, ctx, pool, owner, relayintel.KindPublicRelayList, "", higher, 10)
		resetRepairCursor(t, ctx, pool)
		callRepair(t, ctx, pool)
		finishDerivation(t, ctx, pool, lower)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, lower); status != "completed" {
			t.Fatalf("empty projection status = %s", status)
		}
		var empty bool
		var members int
		if err := pool.QueryRow(ctx, `
			SELECT h.explicitly_empty, count(m.*)
			FROM relay_config_heads h
			LEFT JOIN relay_role_memberships m
			  ON m.pubkey = h.pubkey AND m.source_kind = h.source_kind AND m.d_tag = h.d_tag
			WHERE h.pubkey = $1 AND h.source_kind = 10002
			GROUP BY h.explicitly_empty
		`, owner).Scan(&empty, &members); err != nil {
			t.Fatal(err)
		}
		if !empty || members != 0 {
			t.Fatalf("empty=%v members=%d", empty, members)
		}

		truncOwner, truncLower, truncHigher := repairIDs(5)
		tags := make([][]string, 0, 40)
		for i := 0; i < 40; i++ {
			tags = append(tags, []string{"r", fmt.Sprintf("wss://relay-%02d.example", i)})
		}
		insertStoredEvent(t, ctx, pool, storedEvent{id: truncLower, pubkey: truncOwner, kind: relayintel.KindPublicRelayList, created: 10, tags: tags})
		insertStoredEvent(t, ctx, pool, storedEvent{id: truncHigher, pubkey: truncOwner, kind: relayintel.KindPublicRelayList, created: 10, tags: [][]string{{"r", "wss://short.example"}}})
		insertCanonical(t, ctx, pool, truncOwner, relayintel.KindPublicRelayList, "", truncHigher, 10)
		resetRepairCursor(t, ctx, pool)
		callRepair(t, ctx, pool)
		finishDerivation(t, ctx, pool, truncLower)
		var truncated bool
		var replay bool
		if err := pool.QueryRow(ctx, `
			SELECT truncated, needs_replay FROM relay_config_heads
			WHERE pubkey = $1 AND source_kind = 10002
		`, truncOwner).Scan(&truncated, &replay); err != nil {
			t.Fatal(err)
		}
		if !truncated || replay {
			t.Fatalf("truncated=%v replay=%v", truncated, replay)
		}
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, truncLower); status != "completed" {
			t.Fatalf("truncated projection status = %s", status)
		}
	})

	t.Run("pending replacement does not spend attempts", func(t *testing.T) {
		owner, newer, older := repairIDs(6)
		insertStoredEvent(t, ctx, pool, storedEvent{id: newer, pubkey: owner, kind: 0, created: 20, content: `{"name":"new"}`})
		insertStoredEvent(t, ctx, pool, storedEvent{id: older, pubkey: owner, kind: 0, created: 20, content: `{"name":"old"}`})
		insertCanonical(t, ctx, pool, owner, 0, "", newer, 20)
		insertProfile(t, ctx, pool, owner, newer, 20)
		insertEventTombstone(t, ctx, pool, owner, newer, repairHex(69))
		resetRepairCursor(t, ctx, pool)
		callRepair(t, ctx, pool)
		for i := 0; i < 3; i++ {
			callRepair(t, ctx, pool)
		}
		var attempts int
		if err := pool.QueryRow(ctx, `SELECT attempts FROM relay_intel_repairs WHERE winner_event_id = $1`, newer).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if attempts != 0 {
			t.Fatalf("attempts = %d", attempts)
		}
		if jobsFor(t, ctx, pool, older) != 1 {
			t.Fatal("pending replacement duplicated the derivation job")
		}
		finishDerivation(t, ctx, pool, older)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, newer); status != "deleted" {
			t.Fatalf("status = %s", status)
		}
	})

	t.Run("same timestamp higher id remains", func(t *testing.T) {
		owner, lower, higher := repairIDs(7)
		insertStoredEvent(t, ctx, pool, storedEvent{id: lower, pubkey: owner, kind: relayintel.KindSearchRelays, created: 15, tags: [][]string{{"relay", "wss://search-low.example"}}})
		insertStoredEvent(t, ctx, pool, storedEvent{id: higher, pubkey: owner, kind: relayintel.KindSearchRelays, created: 15, tags: [][]string{{"relay", "wss://search-high.example"}}})
		insertCanonical(t, ctx, pool, owner, relayintel.KindSearchRelays, "", lower, 15)
		insertEventTombstone(t, ctx, pool, owner, lower, repairHex(79))
		resetRepairCursor(t, ctx, pool)
		callRepair(t, ctx, pool)
		if stateWinner(t, ctx, pool, owner, relayintel.KindSearchRelays, "") != higher {
			t.Fatal("same-timestamp higher id was not selected")
		}
		finishDerivation(t, ctx, pool, higher)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, lower); status != "deleted" {
			t.Fatalf("status = %s", status)
		}
		var head string
		if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = $2`, owner, relayintel.KindSearchRelays).Scan(&head); err != nil {
			t.Fatal(err)
		}
		if head != higher {
			t.Fatalf("head = %s", head)
		}
	})

	t.Run("older replacement", func(t *testing.T) {
		owner, newer, older := repairIDs(8)
		insertStoredEvent(t, ctx, pool, storedEvent{id: newer, pubkey: owner, kind: relayintel.KindBlockedRelays, created: 40, tags: [][]string{{"relay", "wss://blocked-new.example"}}})
		insertStoredEvent(t, ctx, pool, storedEvent{id: older, pubkey: owner, kind: relayintel.KindBlockedRelays, created: 5, tags: [][]string{{"relay", "wss://blocked-old.example"}}})
		finishDerivation(t, ctx, pool, newer)
		insertEventTombstone(t, ctx, pool, owner, newer, repairHex(89))
		resetRepairCursor(t, ctx, pool)
		callRepair(t, ctx, pool)
		if stateWinner(t, ctx, pool, owner, relayintel.KindBlockedRelays, "") != older {
			t.Fatal("older remaining event was not selected")
		}
		var replay bool
		var head string
		if err := pool.QueryRow(ctx, `
			SELECT event_id, needs_replay FROM relay_config_heads WHERE pubkey = $1 AND source_kind = $2
		`, owner, relayintel.KindBlockedRelays).Scan(&head, &replay); err != nil {
			t.Fatal(err)
		}
		if head != older || !replay {
			t.Fatalf("head=%s replay=%v", head, replay)
		}
		finishDerivation(t, ctx, pool, older)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, newer); status != "deleted" {
			t.Fatalf("status = %s", status)
		}
	})

	t.Run("no remaining event", func(t *testing.T) {
		owner := repairHex(90)
		only := repairHex(91)
		insertStoredEvent(t, ctx, pool, storedEvent{id: only, pubkey: owner, kind: relayintel.KindPublicRelayList, created: 12, tags: [][]string{{"r", "wss://only.example"}}})
		finishDerivation(t, ctx, pool, only)
		insertEventTombstone(t, ctx, pool, owner, only, repairHex(92))
		resetRepairCursor(t, ctx, pool)
		callRepair(t, ctx, pool)
		if _, ok := canonicalPresent(t, ctx, pool, owner, relayintel.KindPublicRelayList, ""); ok {
			t.Fatal("deleted winner stayed canonical")
		}
		var heads, lists, members int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_config_heads WHERE pubkey = $1`, owner).Scan(&heads); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_lists_latest WHERE pubkey = $1`, owner).Scan(&lists); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_role_memberships WHERE pubkey = $1`, owner).Scan(&members); err != nil {
			t.Fatal(err)
		}
		if heads != 0 || lists != 0 || members != 0 {
			t.Fatalf("heads=%d lists=%d members=%d", heads, lists, members)
		}
		var tombstones int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM deletion_events WHERE target_event_id = $1`, only).Scan(&tombstones); err != nil {
			t.Fatal(err)
		}
		if tombstones != 1 {
			t.Fatal("the tombstone was removed")
		}
	})

	t.Run("contact deletion removes follower edges", func(t *testing.T) {
		owner := repairHex(100)
		eventID := repairHex(101)
		followed := repairHex(102)
		insertStoredEvent(t, ctx, pool, storedEvent{
			id: eventID, pubkey: owner, kind: 3, created: 15,
			tags: [][]string{{"p", followed}},
		})
		finishDerivation(t, ctx, pool, eventID)
		var edges int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM follower_edges WHERE follower_pubkey = $1`, owner).Scan(&edges); err != nil {
			t.Fatal(err)
		}
		if edges != 1 {
			t.Fatalf("edges before deletion = %d", edges)
		}
		insertEventTombstone(t, ctx, pool, owner, eventID, repairHex(103))
		resetRepairCursor(t, ctx, pool)
		callRepair(t, ctx, pool)
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM follower_edges WHERE follower_pubkey = $1`, owner).Scan(&edges); err != nil {
			t.Fatal(err)
		}
		if edges != 0 {
			t.Fatalf("edges after deletion = %d", edges)
		}
		var contacts int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM contact_lists_latest WHERE pubkey = $1`, owner).Scan(&contacts); err != nil {
			t.Fatal(err)
		}
		if contacts != 0 {
			t.Fatal("contact projection remained")
		}
		assertCountsMatchEdges(t, ctx, pool, owner)
		assertCountsMatchEdges(t, ctx, pool, followed)
		if _, ok := canonicalPresent(t, ctx, pool, owner, 3, ""); ok {
			t.Fatal("deleted contact list stayed canonical")
		}
	})

	t.Run("disabled role projection", func(t *testing.T) {
		relayintel.SetProjectionEnabled(false)
		t.Cleanup(func() { relayintel.SetProjectionEnabled(true) })
		owner, lower, higher := repairIDs(11)
		insertStoredEvent(t, ctx, pool, storedEvent{id: lower, pubkey: owner, kind: relayintel.KindFavoriteRelays, created: 8, tags: [][]string{{"relay", "wss://fav.example"}}})
		insertStoredEvent(t, ctx, pool, storedEvent{id: higher, pubkey: owner, kind: relayintel.KindFavoriteRelays, created: 8, tags: [][]string{{"relay", "wss://other.example"}}})
		insertCanonical(t, ctx, pool, owner, relayintel.KindFavoriteRelays, "", higher, 8)
		resetRepairCursor(t, ctx, pool)
		var n int
		if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(10, false)`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		markJobSucceeded(t, ctx, pool, lower)
		if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(10, false)`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if status := repairStatus(t, ctx, pool, lower); status != "completed" {
			t.Fatalf("disabled role projection status = %s", status)
		}
		var heads int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_config_heads WHERE pubkey = $1`, owner).Scan(&heads); err != nil {
			t.Fatal(err)
		}
		if heads != 0 {
			t.Fatal("disabled role projection required a head")
		}

		profileOwner, profileLower, profileHigher := repairIDs(12)
		insertStoredEvent(t, ctx, pool, storedEvent{id: profileLower, pubkey: profileOwner, kind: 0, created: 8, content: `{"name":"low"}`})
		insertStoredEvent(t, ctx, pool, storedEvent{id: profileHigher, pubkey: profileOwner, kind: 0, created: 8, content: `{"name":"high"}`})
		insertCanonical(t, ctx, pool, profileOwner, 0, "", profileHigher, 8)
		resetRepairCursor(t, ctx, pool)
		if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(10, false)`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		markJobSucceeded(t, ctx, pool, profileLower)
		if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(10, false)`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if status := repairStatus(t, ctx, pool, profileLower); status == "completed" {
			t.Fatal("a missing profile completed while role projection was disabled")
		}
	})
}

func TestPartialMembershipBlocksRepair(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	owner, lower, higher := repairIDs(13)
	insertStoredEvent(t, ctx, pool, storedEvent{
		id: lower, pubkey: owner, kind: relayintel.KindPublicRelayList, created: 10,
		tags: [][]string{{"r", "wss://in.example", "read"}, {"r", "wss://out.example", "write"}},
	})
	insertStoredEvent(t, ctx, pool, storedEvent{
		id: higher, pubkey: owner, kind: relayintel.KindPublicRelayList, created: 10,
		tags: [][]string{{"r", "wss://short.example", "write"}},
	})
	insertCanonical(t, ctx, pool, owner, relayintel.KindPublicRelayList, "", higher, 10)
	resetRepairCursor(t, ctx, pool)
	callRepair(t, ctx, pool)
	finishDerivation(t, ctx, pool, lower)
	var matched bool
	if err := pool.QueryRow(ctx, `
			SELECT h.membership_count = d.membership_count
			   AND h.membership_digest = d.membership_digest
			   AND h.membership_count = 2
			FROM relay_config_heads h
			CROSS JOIN LATERAL relay_intel_membership_digest(h.pubkey, h.source_kind, h.d_tag) d
			WHERE h.pubkey = $1 AND h.source_kind = 10002
		`, owner).Scan(&matched); err != nil {
		t.Fatal(err)
	}
	if !matched {
		t.Fatal("stored membership digest does not match the projected rows")
	}
	reject := func(reason string) {
		t.Helper()
		markJobSucceeded(t, ctx, pool, lower)
		callRepair(t, ctx, pool)
		if status := repairStatus(t, ctx, pool, lower); status == "completed" {
			t.Fatalf("%s completed the repair", reason)
		}
	}
	if _, err := pool.Exec(ctx, `
			DELETE FROM relay_role_memberships
			WHERE pubkey = $1 AND role = 'public_inbox'
		`, owner); err != nil {
		t.Fatal(err)
	}
	reject("a missing membership")
	finishDerivation(t, ctx, pool, lower)
	if _, err := pool.Exec(ctx, `
			INSERT INTO relay_role_memberships (
				pubkey, role, url_key, source_kind, d_tag, endpoint_url, endpoint_type,
				source_event_id, created_at, declaration_source, public_safe
			) VALUES ($1, 'public_outbox', 'extra', 10002, '', 'wss://extra.example', 'nostr_relay', $2, 10, 'modern', true)
		`, owner, lower); err != nil {
		t.Fatal(err)
	}
	reject("an unexpected membership")
	if _, err := pool.Exec(ctx, `DELETE FROM relay_role_memberships WHERE pubkey = $1 AND url_key = 'extra'`, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
			UPDATE relay_role_memberships SET role = 'search'
			WHERE pubkey = $1 AND role = 'public_outbox'
		`, owner); err != nil {
		t.Fatal(err)
	}
	reject("the wrong role")
	finishDerivation(t, ctx, pool, lower)
	markJobSucceeded(t, ctx, pool, lower)
	callRepair(t, ctx, pool)
	if status := repairStatus(t, ctx, pool, lower); status != "completed" {
		t.Fatalf("restored projection status = %s", status)
	}
}

func repairIDs(n int) (owner, left, right string) {
	return repairHex(n*10 + 1), repairHex(n*10 + 2), repairHex(n*10 + 3)
}

func repairHex(n int) string {
	return fmt.Sprintf("%064x", n)
}

func insertCanonical(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string, kind int, dTag, eventID string, created int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, $4, $5, 1)
	`, pubkey, kind, dTag, eventID, created); err != nil {
		t.Fatal(err)
	}
}

func insertProfile(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey, eventID string, created int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO profiles_latest (
			pubkey, metadata_event_id, metadata_created_at, name, profile_json, derivation_version
		) VALUES ($1, $2, $3, 'name', '{"name":"name"}', 1)
	`, pubkey, eventID, created); err != nil {
		t.Fatal(err)
	}
}

func insertEventTombstone(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey, target, deletionID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, 100, 1)
	`, deletionID, pubkey, target); err != nil {
		t.Fatal(err)
	}
}

func markJobSucceeded(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'succeeded', finished_at = now(), locked_at = NULL, locked_by = NULL
		WHERE idempotency_key = $1
	`, "derive_event_bundle:"+eventID); err != nil {
		t.Fatal(err)
	}
}

func repairStatus(t *testing.T, ctx context.Context, pool *pgxpool.Pool, winner string) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_intel_repairs WHERE winner_event_id = $1`, winner).Scan(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func canonicalPresent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string, kind int, dTag string) (string, bool) {
	t.Helper()
	var id string
	err := pool.QueryRow(ctx, `
		SELECT event_id FROM replaceable_state WHERE pubkey = $1 AND kind = $2 AND d_tag = $3
	`, pubkey, kind, dTag).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return id, true
}

func assertCountsMatchEdges(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) {
	t.Helper()
	var followers, following int
	err := pool.QueryRow(ctx, `
		SELECT follower_count, following_count FROM profile_public_stats WHERE pubkey = $1
	`, pubkey).Scan(&followers, &following)
	if err != nil {
		return
	}
	var edgeFollowers, edgeFollowing int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM follower_edges WHERE followed_pubkey = $1`, pubkey).Scan(&edgeFollowers); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM follower_edges WHERE follower_pubkey = $1`, pubkey).Scan(&edgeFollowing); err != nil {
		t.Fatal(err)
	}
	if followers != edgeFollowers || following != edgeFollowing {
		t.Fatalf("counts for %s followers=%d/%d following=%d/%d", pubkey[:8], followers, edgeFollowers, following, edgeFollowing)
	}
}
