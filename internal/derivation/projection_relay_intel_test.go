package derivation_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nostrmash/nostrmash/internal/derivation"
	"github.com/nostrmash/nostrmash/internal/store"
	"github.com/nostrmash/nostrmash/internal/testutil/derivationbootstrap"
)

func TestRelayRoleProjectionKeepsMarkersAndLegacyApart(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "relay-intel")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	base := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	author := strings.Repeat("ab", 32)
	lowID := strings.Repeat("11", 32)
	highID := strings.Repeat("22", 32)
	older := newEventForTest(lowID, author, base.Unix(), 10002, [][]string{
		{"r", "wss://read.example.com", "read"},
		{"r", "wss://write.example.com", "write"},
		{"client", "nostrmash"},
	}, "", base)
	newerSameTime := newEventForTest(highID, author, base.Unix(), 10002, [][]string{
		{"r", "wss://both.example.com"},
	}, "", base)
	if err := insertEvents(t, ctx, pgStore, older, newerSameTime); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectRelayListsLatest(ctx, older.ID); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectRelayListsLatest(ctx, newerSameTime.ID); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectRelayListsLatest(ctx, older.ID); err != nil {
		t.Fatal(err)
	}

	var relays []string
	if err := pool.QueryRow(ctx, `SELECT ARRAY(SELECT jsonb_array_elements_text(relays_json) FROM relay_lists_latest WHERE pubkey = $1)`, author).Scan(&relays); err != nil {
		t.Fatal(err)
	}
	if strings.Join(relays, ",") != "wss://both.example.com" {
		t.Fatalf("relay_lists_latest = %#v", relays)
	}
	rows, err := pool.Query(ctx, `
		SELECT role, endpoint_url, marker
		FROM relay_role_memberships
		WHERE pubkey = $1
		ORDER BY role, endpoint_url
	`, author)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var role, endpoint, marker string
		if err := rows.Scan(&role, &endpoint, &marker); err != nil {
			t.Fatal(err)
		}
		got = append(got, role+"|"+endpoint+"|"+marker)
	}
	want := "public_inbox|wss://both.example.com| public_outbox|wss://both.example.com|"
	if strings.Join(got, " ") != want {
		t.Fatalf("memberships = %#v", got)
	}

	emptyID := strings.Repeat("33", 32)
	empty := newEventForTest(emptyID, author, base.Add(time.Minute).Unix(), 10002, nil, "", base)
	legacyID := strings.Repeat("44", 32)
	legacy := newEventForTest(legacyID, author, base.Add(2*time.Minute).Unix(), 3, [][]string{
		{"p", author, "wss://hint.example.com"},
	}, `{"wss://legacy.example.com":{"read":true,"write":true}}`, base)
	privateID := strings.Repeat("55", 32)
	private := newEventForTest(privateID, author, base.Unix(), 10013, [][]string{
		{"relay", "wss://secret.example.com"},
	}, "ciphertext", base)
	if err := insertEvents(t, ctx, pgStore, empty, legacy, private); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectRelayListsLatest(ctx, empty.ID); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectContactListsLatest(ctx, legacy.ID); err != nil {
		t.Fatal(err)
	}
	if err := handlers.ProjectSurfaces(ctx, private.ID); err != nil {
		t.Fatal(err)
	}
	var modernCount, legacyCount, privateMembers int
	var explicitlyEmpty, clientSide bool
	if err := pool.QueryRow(ctx, `
		SELECT explicitly_empty,
		       (SELECT count(*) FROM relay_role_memberships WHERE pubkey = $1 AND source_kind = 10002),
		       (SELECT count(*) FROM relay_role_memberships WHERE pubkey = $1 AND source_kind = 3)
		FROM relay_config_heads
		WHERE pubkey = $1 AND source_kind = 10002
	`, author).Scan(&explicitlyEmpty, &modernCount, &legacyCount); err != nil {
		t.Fatal(err)
	}
	if !explicitlyEmpty || modernCount != 0 || legacyCount != 2 {
		t.Fatalf("empty modern=%v modern_members=%d legacy_members=%d", explicitlyEmpty, modernCount, legacyCount)
	}
	if err := pool.QueryRow(ctx, `
		SELECT client_side_only,
		       (SELECT count(*) FROM relay_role_memberships WHERE pubkey = $1 AND source_kind = 10013)
		FROM relay_config_heads
		WHERE pubkey = $1 AND source_kind = 10013
	`, author).Scan(&clientSide, &privateMembers); err != nil {
		t.Fatal(err)
	}
	if !clientSide || privateMembers != 0 {
		t.Fatalf("private head=%v members=%d", clientSide, privateMembers)
	}
	var registry int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_registry WHERE relay_url LIKE '%secret.example%'`).Scan(&registry); err != nil {
		t.Fatal(err)
	}
	if registry != 0 {
		t.Fatal("private relay was inserted into the registry")
	}
}
