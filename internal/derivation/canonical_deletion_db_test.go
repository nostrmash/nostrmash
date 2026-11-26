package derivation_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/derivation"
	"github.com/nostrmash/nostrmash/internal/store"
	"github.com/nostrmash/nostrmash/internal/store/retention"
	"github.com/nostrmash/nostrmash/internal/testutil/derivationbootstrap"
)

func TestDeletedEventsStayOutOfCanonicalState(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "canonical-deletion")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	base := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)

	t.Run("replay into a missing coordinate", func(t *testing.T) {
		pubkey := "11"
		eventID := "deleted-missing"
		insertPlain(t, ctx, pgStore, eventID, pubkey, 0, 100, nil, `{"name":"gone"}`, base)
		if err := handlers.UpdateReplaceableState(ctx, eventID); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectProfilesLatest(ctx, eventID); err != nil {
			t.Fatal(err)
		}
		tombstoneEvent(t, ctx, pool, pubkey, eventID, "tomb-missing", 200)
		if _, err := pool.Exec(ctx, `DELETE FROM replaceable_state WHERE pubkey = $1`, pubkey); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM profiles_latest WHERE pubkey = $1`, pubkey); err != nil {
			t.Fatal(err)
		}
		if err := handlers.UpdateReplaceableState(ctx, eventID); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectProfilesLatest(ctx, eventID); err != nil {
			t.Fatal(err)
		}
		if _, ok := canonicalID(t, ctx, pool, pubkey, 0, ""); ok {
			t.Fatal("deleted replay became canonical")
		}
		var profiles int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM profiles_latest WHERE pubkey = $1 AND metadata_event_id = $2`, pubkey, eventID).Scan(&profiles); err != nil {
			t.Fatal(err)
		}
		if profiles != 0 {
			t.Fatal("deleted replay restored the profile")
		}
	})

	t.Run("replay into an existing coordinate", func(t *testing.T) {
		pubkey := "22"
		older := "kept-existing"
		newer := "deleted-existing"
		insertPlain(t, ctx, pgStore, older, pubkey, 10002, 100, [][]string{{"r", "wss://kept.example"}}, "", base)
		insertPlain(t, ctx, pgStore, newer, pubkey, 10002, 300, [][]string{{"r", "wss://gone.example"}}, "", base)
		if err := handlers.UpdateReplaceableState(ctx, older); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectRelayListsLatest(ctx, older); err != nil {
			t.Fatal(err)
		}
		tombstoneEvent(t, ctx, pool, pubkey, newer, "tomb-existing", 400)
		if err := handlers.UpdateReplaceableState(ctx, newer); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectRelayListsLatest(ctx, newer); err != nil {
			t.Fatal(err)
		}
		if got, ok := canonicalID(t, ctx, pool, pubkey, 10002, ""); !ok || got != older {
			t.Fatalf("canonical = %s present=%v", got, ok)
		}
		var head, list string
		if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002`, pubkey).Scan(&head); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_lists_latest WHERE pubkey = $1`, pubkey).Scan(&list); err != nil {
			t.Fatal(err)
		}
		if head != older || list != older {
			t.Fatalf("head=%s list=%s", head, list)
		}
	})

	t.Run("address deletion and a later event", func(t *testing.T) {
		pubkey := "33"
		covered := "address-covered"
		later := "address-later"
		kind5 := "address-kind5"
		insertPlain(t, ctx, pgStore, covered, pubkey, 30002, 100, [][]string{{"d", "room"}, {"r", "wss://old.example"}}, "", base)
		insertPlain(t, ctx, pgStore, kind5, pubkey, 5, 250, [][]string{{"a", "30002:" + pubkey + ":room"}}, "", base)
		if err := handlers.UpdateReplaceableState(ctx, covered); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM replaceable_state WHERE pubkey = $1 AND kind = 30002`, pubkey); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO deletion_addresses (event_id, pubkey, kind, d_tag, created_at)
			VALUES ($1, $2, 30002, 'room', 250)
		`, kind5, pubkey); err != nil {
			t.Fatal(err)
		}
		if err := handlers.UpdateReplaceableState(ctx, covered); err != nil {
			t.Fatal(err)
		}
		if _, ok := canonicalID(t, ctx, pool, pubkey, 30002, "room"); ok {
			t.Fatal("address deletion did not cover the retained event")
		}
		insertPlain(t, ctx, pgStore, later, pubkey, 30002, 400, [][]string{{"d", "room"}, {"r", "wss://new.example"}}, "", base)
		if err := handlers.UpdateReplaceableState(ctx, later); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectRelayListsLatest(ctx, later); err != nil {
			t.Fatal(err)
		}
		if got, ok := canonicalID(t, ctx, pool, pubkey, 30002, "room"); !ok || got != later {
			t.Fatalf("later canonical = %s present=%v", got, ok)
		}
		var head string
		if err := pool.QueryRow(ctx, `
			SELECT event_id FROM relay_config_heads
			WHERE pubkey = $1 AND source_kind = 30002 AND d_tag = 'room'
		`, pubkey).Scan(&head); err != nil {
			t.Fatal(err)
		}
		if head != later {
			t.Fatalf("head = %s", head)
		}
	})

	t.Run("event deletion arrives first", func(t *testing.T) {
		pubkey := "44"
		eventID := "deleted-before-insert"
		tombstoneEvent(t, ctx, pool, pubkey, eventID, "tomb-before", 50)
		insertPlain(t, ctx, pgStore, eventID, pubkey, 0, 40, nil, `{"name":"late"}`, base)
		if err := handlers.UpdateReplaceableState(ctx, eventID); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectProfilesLatest(ctx, eventID); err != nil {
			t.Fatal(err)
		}
		if _, ok := canonicalID(t, ctx, pool, pubkey, 0, ""); ok {
			t.Fatal("a tombstone that arrived first did not block insertion")
		}
		var profiles int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM profiles_latest WHERE pubkey = $1`, pubkey).Scan(&profiles); err != nil {
			t.Fatal(err)
		}
		if profiles != 0 {
			t.Fatal("profile projection restored an event deleted before it arrived")
		}
	})

	t.Run("ordinary stray d tags share one coordinate", func(t *testing.T) {
		pubkey := "55"
		first := "stray-a"
		second := "stray-b"
		insertPlain(t, ctx, pgStore, first, pubkey, 0, 100, [][]string{{"d", "nope"}}, `{"name":"a"}`, base)
		insertPlain(t, ctx, pgStore, second, pubkey, 0, 100, [][]string{{"d", "other"}}, `{"name":"b"}`, base)
		if err := handlers.UpdateReplaceableState(ctx, second); err != nil {
			t.Fatal(err)
		}
		if err := handlers.UpdateReplaceableState(ctx, first); err != nil {
			t.Fatal(err)
		}
		if got, ok := canonicalID(t, ctx, pool, pubkey, 0, ""); !ok || got != first {
			t.Fatalf("canonical = %s present=%v", got, ok)
		}
		var rows int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM replaceable_state WHERE pubkey = $1 AND kind = 0`, pubkey).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Fatalf("coordinates = %d", rows)
		}
	})

	t.Run("addressable empty and unusual identifiers stay distinct", func(t *testing.T) {
		pubkey := "66"
		emptyID := "addr-empty"
		weirdID := "addr-weird"
		insertPlain(t, ctx, pgStore, emptyID, pubkey, 30002, 100, [][]string{{"d", ""}, {"r", "wss://empty.example"}}, "", base)
		insertPlain(t, ctx, pgStore, weirdID, pubkey, 30002, 100, [][]string{{"d", "weird id/with spaces"}, {"r", "wss://weird.example"}}, "", base)
		if err := handlers.UpdateReplaceableState(ctx, emptyID); err != nil {
			t.Fatal(err)
		}
		if err := handlers.UpdateReplaceableState(ctx, weirdID); err != nil {
			t.Fatal(err)
		}
		if got, ok := canonicalID(t, ctx, pool, pubkey, 30002, ""); !ok || got != emptyID {
			t.Fatalf("empty coordinate = %s present=%v", got, ok)
		}
		if got, ok := canonicalID(t, ctx, pool, pubkey, 30002, "weird id/with spaces"); !ok || got != weirdID {
			t.Fatalf("unusual coordinate = %s present=%v", got, ok)
		}
	})

	t.Run("retention and replay", func(t *testing.T) {
		pubkey := "77"
		eventID := "deleted-purge"
		seen := time.Now().UTC().Add(-48 * time.Hour)
		insertPlain(t, ctx, pgStore, eventID, pubkey, 0, 100, nil, `{"name":"purge"}`, seen)
		tombstoneEvent(t, ctx, pool, pubkey, eventID, "tomb-purge", 200)
		if err := handlers.UpdateReplaceableState(ctx, eventID); err != nil {
			t.Fatal(err)
		}
		var queued int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM replaceable_event_purge WHERE event_id = $1`, eventID).Scan(&queued); err != nil {
			t.Fatal(err)
		}
		if queued != 1 {
			t.Fatal("deleted non-winner was not queued for purge")
		}
		if _, err := pool.Exec(ctx, `
			UPDATE jobs SET status = 'succeeded', finished_at = now(), locked_at = NULL, locked_by = NULL
			WHERE idempotency_key = $1
		`, "derive_event_bundle:"+eventID); err != nil {
			t.Fatal(err)
		}
		removed, err := retention.New(pool).PurgeSupersededReplaceableEvents(ctx, time.Now().UTC().Add(-time.Hour), time.Now().UTC().Add(-time.Hour), 20)
		if err != nil {
			t.Fatal(err)
		}
		if removed != 1 {
			t.Fatalf("purged = %d", removed)
		}
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)`, eventID).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			t.Fatal("purge left the deleted event")
		}
		insertPlain(t, ctx, pgStore, eventID, pubkey, 0, 100, nil, `{"name":"purge"}`, seen)
		if err := handlers.UpdateReplaceableState(ctx, eventID); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectProfilesLatest(ctx, eventID); err != nil {
			t.Fatal(err)
		}
		if _, ok := canonicalID(t, ctx, pool, pubkey, 0, ""); ok {
			t.Fatal("replay after purge became canonical")
		}
		var profiles int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM profiles_latest WHERE pubkey = $1`, pubkey).Scan(&profiles); err != nil {
			t.Fatal(err)
		}
		if profiles != 0 {
			t.Fatal("replay after purge restored a profile")
		}
	})

	t.Run("deleted empty kind 3 does not clear a live head", func(t *testing.T) {
		pubkey := "88"
		live := "kind3-live"
		empty := "kind3-empty-deleted"
		insertPlain(t, ctx, pgStore, live, pubkey, 3, 100, nil, `{"wss://keep.example":{"read":true,"write":true}}`, base)
		if err := handlers.UpdateReplaceableState(ctx, live); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectContactListsLatest(ctx, live); err != nil {
			t.Fatal(err)
		}
		insertPlain(t, ctx, pgStore, empty, pubkey, 3, 300, nil, "", base)
		tombstoneEvent(t, ctx, pool, pubkey, empty, "tomb-kind3", 400)
		if err := handlers.ProjectContactListsLatest(ctx, empty); err != nil {
			t.Fatal(err)
		}
		var head string
		if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 3`, pubkey).Scan(&head); err != nil {
			t.Fatal(err)
		}
		if head != live {
			t.Fatalf("head = %s", head)
		}
	})
}

func TestConcurrentDeletionBlocksCanonicalInsertion(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "canonical-lock")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	pubkey := "99"
	eventID := "locked-deleted"
	insertPlain(t, ctx, pgStore, eventID, pubkey, 0, 100, nil, `{"name":"race"}`, time.Now().UTC())

	lockCtx, cancelLock := context.WithCancel(ctx)
	defer cancelLock()
	tx, err := pool.Begin(lockCtx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT relay_intel_lock_coordinate($1, 0, '')`, pubkey); err != nil {
		t.Fatal(err)
	}

	writeCtx, cancelWrite := context.WithTimeout(ctx, 15*time.Second)
	defer cancelWrite()
	done := make(chan error, 1)
	go func() {
		done <- handlers.UpdateReplaceableState(writeCtx, eventID)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		if err := pool.QueryRow(ctx, `
			SELECT count(*)
			FROM pg_locks l
			JOIN pg_stat_activity a ON a.pid = l.pid
			WHERE l.locktype = 'advisory'
			  AND NOT l.granted
			  AND a.query ILIKE '%relay_intel_lock_coordinate%'
		`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("canonical insert did not wait on the coordinate lock")
		}
		time.Sleep(20 * time.Millisecond)
	}

	tombstoneEvent(t, ctx, pool, pubkey, eventID, "tomb-locked", 200)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, ok := canonicalID(t, ctx, pool, pubkey, 0, ""); ok {
		t.Fatal("concurrent deletion lost to the waiting insert")
	}
}

func TestProfileProjectionFollowsCanonicalAuthority(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "profile-authority")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	base := time.Date(2026, 11, 26, 13, 0, 0, 0, time.UTC)
	t.Cleanup(func() { derivation.SetProjectionReadBarrier(nil) })

	t.Run("stale write loses", func(t *testing.T) {
		pubkey := "profile-stale"
		older := "profile-old"
		newer := "profile-new"
		insertPlain(t, ctx, pgStore, older, pubkey, 0, 10, nil, `{"name":"old"}`, base)
		insertPlain(t, ctx, pgStore, newer, pubkey, 0, 20, nil, `{"name":"new"}`, base)
		if err := handlers.UpdateReplaceableState(ctx, older); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectProfilesLatest(ctx, older); err != nil {
			t.Fatal(err)
		}
		derivation.SetProjectionReadBarrier(func(winner string) {
			if winner != older {
				return
			}
			derivation.SetProjectionReadBarrier(nil)
			if err := handlers.UpdateReplaceableState(ctx, newer); err != nil {
				t.Errorf("newer canonical: %v", err)
			}
			if err := handlers.ProjectProfilesLatest(ctx, newer); err != nil {
				t.Errorf("newer profile: %v", err)
			}
		})
		if err := handlers.ProjectProfilesLatest(ctx, older); err != nil {
			t.Fatal(err)
		}
		var profile, name string
		if err := pool.QueryRow(ctx, `SELECT metadata_event_id, name FROM profiles_latest WHERE pubkey = $1`, pubkey).Scan(&profile, &name); err != nil {
			t.Fatal(err)
		}
		if profile != newer || name != "new" {
			t.Fatalf("profile=%s name=%s", profile, name)
		}
	})

	t.Run("deleted winner yields to the remaining canonical event", func(t *testing.T) {
		pubkey := "profile-replaced"
		older := "profile-remain"
		newer := "profile-deleted"
		insertPlain(t, ctx, pgStore, older, pubkey, 0, 10, nil, `{"name":"remain"}`, base)
		insertPlain(t, ctx, pgStore, newer, pubkey, 0, 30, nil, `{"name":"deleted"}`, base)
		if err := handlers.UpdateReplaceableState(ctx, newer); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectProfilesLatest(ctx, newer); err != nil {
			t.Fatal(err)
		}
		tombstoneEvent(t, ctx, pool, pubkey, newer, "tomb-profile", 40)
		if err := handlers.ProjectProfilesLatest(ctx, older); err != nil {
			t.Fatal(err)
		}
		var profile string
		if err := pool.QueryRow(ctx, `SELECT metadata_event_id FROM profiles_latest WHERE pubkey = $1`, pubkey).Scan(&profile); err != nil {
			t.Fatal(err)
		}
		if profile != newer {
			t.Fatalf("ordinary older event replaced a newer profile before canonical repair: %s", profile)
		}
		if _, err := pool.Exec(ctx, `
			UPDATE replaceable_state SET event_id = $2, created_at = 10, updated_at = now()
			WHERE pubkey = $1 AND kind = 0 AND d_tag = ''
		`, pubkey, older); err != nil {
			t.Fatal(err)
		}
		if err := handlers.ProjectProfilesLatest(ctx, older); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT metadata_event_id FROM profiles_latest WHERE pubkey = $1`, pubkey).Scan(&profile); err != nil {
			t.Fatal(err)
		}
		if profile != older {
			t.Fatalf("reconciled profile = %s", profile)
		}
	})

	t.Run("missing canonical state skips deleted events", func(t *testing.T) {
		pubkey := "profile-missing-state"
		older := "profile-fallback"
		newer := "profile-fallback-deleted"
		insertPlain(t, ctx, pgStore, older, pubkey, 0, 10, nil, `{"name":"fallback"}`, base)
		insertPlain(t, ctx, pgStore, newer, pubkey, 0, 30, nil, `{"name":"nope"}`, base)
		tombstoneEvent(t, ctx, pool, pubkey, newer, "tomb-fallback", 40)
		if err := handlers.ProjectProfilesLatest(ctx, newer); err != nil {
			t.Fatal(err)
		}
		var profile string
		if err := pool.QueryRow(ctx, `SELECT metadata_event_id FROM profiles_latest WHERE pubkey = $1`, pubkey).Scan(&profile); err != nil {
			t.Fatal(err)
		}
		if profile != older {
			t.Fatalf("fallback profile = %s", profile)
		}

		only := "profile-only-deleted"
		onlyPubkey := "profile-only"
		insertPlain(t, ctx, pgStore, only, onlyPubkey, 0, 10, nil, `{"name":"only"}`, base)
		tombstoneEvent(t, ctx, pool, onlyPubkey, only, "tomb-only", 20)
		if err := handlers.ProjectProfilesLatest(ctx, only); err != nil {
			t.Fatal(err)
		}
		var profiles int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM profiles_latest WHERE pubkey = $1`, onlyPubkey).Scan(&profiles); err != nil {
			t.Fatal(err)
		}
		if profiles != 0 {
			t.Fatal("deleted event created a profile without canonical state")
		}
	})
}

func TestListProjectionFollowsCanonicalAuthority(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "list-authority")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	base := time.Date(2026, 11, 26, 14, 0, 0, 0, time.UTC)

	pubkey := "list-stale"
	older := "list-old"
	newer := "list-new"
	insertPlain(t, ctx, pgStore, older, pubkey, 10002, 10, [][]string{{"r", "wss://old.example"}}, "", base)
	insertPlain(t, ctx, pgStore, newer, pubkey, 10002, 20, [][]string{{"r", "wss://new.example"}}, "", base)
	if err := handlers.ProjectRelayListsLatest(ctx, older); err != nil {
		t.Fatal(err)
	}
	if err := handlers.UpdateReplaceableState(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectRelayListsLatest(ctx, newer); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectRelayListsLatest(ctx, older); err != nil {
		t.Fatal(err)
	}
	var list, head string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_lists_latest WHERE pubkey = $1`, pubkey).Scan(&list); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10002`, pubkey).Scan(&head); err != nil {
		t.Fatal(err)
	}
	if list != newer || head != newer {
		t.Fatalf("list=%s head=%s", list, head)
	}

	contactPub := "contact-stale"
	contactOld := "contact-old"
	contactNew := "contact-new"
	followed := "followed-pubkey"
	insertPlain(t, ctx, pgStore, contactOld, contactPub, 3, 10, [][]string{{"p", followed}}, "", base)
	insertPlain(t, ctx, pgStore, contactNew, contactPub, 3, 20, [][]string{{"p", "other-followed"}}, "", base)
	if err := handlers.ProjectContactListsLatest(ctx, contactOld); err != nil {
		t.Fatal(err)
	}
	if err := handlers.UpdateReplaceableState(ctx, contactNew); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectContactListsLatest(ctx, contactNew); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectContactListsLatest(ctx, contactOld); err != nil {
		t.Fatal(err)
	}
	var contact string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM contact_lists_latest WHERE pubkey = $1`, contactPub).Scan(&contact); err != nil {
		t.Fatal(err)
	}
	if contact != contactNew {
		t.Fatalf("contact list = %s", contact)
	}

	dmPub := "dm-stale"
	dmOld := "dm-old"
	dmNew := "dm-new"
	insertPlain(t, ctx, pgStore, dmOld, dmPub, 10050, 10, [][]string{{"relay", "wss://dm-old.example"}}, "", base)
	insertPlain(t, ctx, pgStore, dmNew, dmPub, 10050, 30, [][]string{{"relay", "wss://dm-new.example"}}, "", base)
	if err := handlers.UpdateReplaceableState(ctx, dmNew); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectExtendedLists(ctx, dmNew); err != nil {
		t.Fatal(err)
	}
	tombstoneEvent(t, ctx, pool, dmPub, dmNew, "tomb-dm", 40)
	if _, err := pool.Exec(ctx, `
		UPDATE replaceable_state SET event_id = $2, created_at = 10, updated_at = now()
		WHERE pubkey = $1 AND kind = 10050
	`, dmPub, dmOld); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectExtendedLists(ctx, dmOld); err != nil {
		t.Fatal(err)
	}
	var dm string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM dm_relay_lists_latest WHERE pubkey = $1`, dmPub).Scan(&dm); err != nil {
		t.Fatal(err)
	}
	if dm != dmOld {
		t.Fatalf("dm list = %s", dm)
	}
	if err := handlers.ProjectExtendedLists(ctx, dmNew); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT event_id FROM dm_relay_lists_latest WHERE pubkey = $1`, dmPub).Scan(&dm); err != nil {
		t.Fatal(err)
	}
	if dm != dmOld {
		t.Fatalf("deleted dm list overwrote the canonical list: %s", dm)
	}
	var headDM string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM relay_config_heads WHERE pubkey = $1 AND source_kind = 10050`, dmPub).Scan(&headDM); err != nil {
		t.Fatal(err)
	}
	if headDM != dmOld {
		t.Fatalf("dm head = %s", headDM)
	}
}

func TestNutzapProjectionFollowsCanonicalAuthority(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "nutzap-authority")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	base := time.Date(2026, 11, 26, 15, 0, 0, 0, time.UTC)
	t.Cleanup(func() { derivation.SetProjectionReadBarrier(nil) })

	pubkey := "nutzap-stale"
	older := "nutzap-old"
	newer := "nutzap-new"
	insertPlain(t, ctx, pgStore, older, pubkey, 10019, 10, [][]string{{"relay", "wss://old.example"}, {"pubkey", "old-key"}}, "", base)
	insertPlain(t, ctx, pgStore, newer, pubkey, 10019, 20, [][]string{{"relay", "wss://new.example"}, {"pubkey", "new-key"}}, "", base)
	if err := handlers.UpdateReplaceableState(ctx, older); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectExtendedLists(ctx, older); err != nil {
		t.Fatal(err)
	}
	derivation.SetProjectionReadBarrier(func(winner string) {
		if winner != older {
			return
		}
		derivation.SetProjectionReadBarrier(nil)
		if err := handlers.UpdateReplaceableState(ctx, newer); err != nil {
			t.Errorf("newer canonical: %v", err)
		}
		if err := handlers.ProjectExtendedLists(ctx, newer); err != nil {
			t.Errorf("newer nutzap: %v", err)
		}
	})
	if err := handlers.ProjectExtendedLists(ctx, older); err != nil {
		t.Fatal(err)
	}
	var eventID, p2pk string
	if err := pool.QueryRow(ctx, `SELECT event_id, p2pk FROM nutzap_info_latest WHERE pubkey = $1`, pubkey).Scan(&eventID, &p2pk); err != nil {
		t.Fatal(err)
	}
	if eventID != newer || p2pk != "new-key" {
		t.Fatalf("event=%s p2pk=%s", eventID, p2pk)
	}

	tombstoneEvent(t, ctx, pool, pubkey, newer, "tomb-nutzap", 40)
	if _, err := pool.Exec(ctx, `
		UPDATE replaceable_state SET event_id = $2, created_at = 10, updated_at = now()
		WHERE pubkey = $1 AND kind = 10019 AND d_tag = ''
	`, pubkey, older); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectExtendedLists(ctx, older); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT event_id, p2pk FROM nutzap_info_latest WHERE pubkey = $1`, pubkey).Scan(&eventID, &p2pk); err != nil {
		t.Fatal(err)
	}
	if eventID != older || p2pk != "old-key" {
		t.Fatalf("reconciled event=%s p2pk=%s", eventID, p2pk)
	}
}

func insertPlain(t *testing.T, ctx context.Context, pgStore *store.PostgresStore, id, pubkey string, kind int, created int64, tags [][]string, content string, seen time.Time) {
	t.Helper()
	event := newEventForTest(id, pubkey, created, kind, tags, content, seen)
	if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.example", seen); err != nil {
		t.Fatal(err)
	}
}

func tombstoneEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey, target, deletionID string, created int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, $4, 1)
	`, deletionID, pubkey, target, created); err != nil {
		t.Fatal(err)
	}
}

func canonicalID(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string, kind int, dTag string) (string, bool) {
	t.Helper()
	var id string
	err := pool.QueryRow(ctx, `
		SELECT event_id FROM replaceable_state
		WHERE pubkey = $1 AND kind = $2 AND d_tag = $3
	`, pubkey, kind, dTag).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false
	}
	if err != nil {
		t.Fatal(err)
	}
	return id, true
}
