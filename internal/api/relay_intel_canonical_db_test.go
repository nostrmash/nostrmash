package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/derivation"
	"github.com/nostrmash/nostrmash/internal/model"
	"github.com/nostrmash/nostrmash/internal/relayintel"
	"github.com/nostrmash/nostrmash/internal/store"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func TestCanonicalAuthorityAcrossRelayAPI(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay intel canonical"), "relaycanon")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	if err := derivation.EnsureRegisteredDerivations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { relayintel.SetResolveBarrier(nil) })

	t.Run("older remaining winner", func(t *testing.T) {
		assertCanonicalAgreement(t, ctx, pool, "a1", 10, 30, false)
	})
	t.Run("same timestamp higher id", func(t *testing.T) {
		assertCanonicalAgreement(t, ctx, pool, "a2", 10, 10, true)
	})
	t.Run("canonical event cannot be loaded", func(t *testing.T) {
		pubkey, canonical, projection := seedDivergedProjection(t, ctx, pool, "a3", 10, 30, false)
		if _, err := pool.Exec(ctx, `UPDATE events SET raw_json = '"not-an-event"'::jsonb WHERE id = $1`, canonical); err != nil {
			t.Fatal(err)
		}
		cfg, _, plan := readRelayAPI(t, pool, pubkey)
		body := mustJSON(cfg) + mustJSON(plan)
		if strings.Contains(body, "wss://deleted.example") {
			t.Fatalf("unavailable canonical event kept the deleted projection: %s", body)
		}
		for _, item := range []map[string]any{cfg, plan} {
			src := sourceByKind(t, item, relayintel.KindPublicRelayList)
			if src["status"] != "unreadable" || src["canonical_event_id"] != canonical || src["projection_event_id"] != projection {
				t.Fatalf("source = %s", mustJSON(src))
			}
		}
		proposal := plan["proposal"].(map[string]any)
		if proposal["ready_to_sign"] != false || proposal["public_relays"] != nil {
			t.Fatalf("proposal = %s", mustJSON(proposal))
		}
	})
	t.Run("canonical state changes during the read", func(t *testing.T) {
		pubkey, canonical, projection := seedDivergedProjection(t, ctx, pool, "a4", 10, 30, false)
		third := fmt.Sprintf("%064x", 0xA400)
		insertRelayEvent(t, ctx, pool, third, pubkey, 50, [][]string{{"r", "wss://third.example", "write"}})
		handlers := &Handlers{relayIntel: relayintel.NewService(pool, relayintel.Options{})}
		mux := relayMux(handlers)
		relayintel.SetResolveBarrier(func() {
			relayintel.SetResolveBarrier(nil)
			if _, err := pool.Exec(ctx, `
				UPDATE replaceable_state
				SET event_id = $2, created_at = 50, updated_at = now()
				WHERE pubkey = $1 AND kind = 10002 AND d_tag = ''
			`, pubkey, third); err != nil {
				t.Errorf("change canonical state: %v", err)
			}
		})
		first := postJSON(t, mux, map[string]any{"pubkey": pubkey, "preset": "balanced", "roles": []string{"public_outbox"}})
		src := sourceByKind(t, first, relayintel.KindPublicRelayList)
		if src["canonical_event_id"] != canonical || src["status"] != "inconsistent" {
			t.Fatalf("in-flight source = %s", mustJSON(src))
		}
		if strings.Contains(mustJSON(first), "wss://third.example") || strings.Contains(mustJSON(first), "wss://deleted.example") {
			t.Fatalf("in-flight response mixed canonical state: %s", mustJSON(first))
		}
		cfg, _, plan := readRelayAPI(t, pool, pubkey)
		for _, body := range []map[string]any{cfg, plan} {
			got := sourceByKind(t, body, relayintel.KindPublicRelayList)
			if got["canonical_event_id"] != third || got["projection_event_id"] != projection || got["status"] != "inconsistent" {
				t.Fatalf("source after the change = %s", mustJSON(got))
			}
		}
		assertRoleURL(t, cfg["account"].(map[string]any), "public_outbox", "wss://third.example")
	})
}

func assertCanonicalAgreement(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tag string, canonicalAt, projectionAt int64, higherID bool) {
	t.Helper()
	pubkey, canonical, projection := seedDivergedProjection(t, ctx, pool, tag, canonicalAt, projectionAt, higherID)
	cfg, diag, plan := readRelayAPI(t, pool, pubkey)
	for _, body := range []map[string]any{cfg, diag, plan} {
		src := sourceByKind(t, body, relayintel.KindPublicRelayList)
		if src["canonical_event_id"] != canonical || src["projection_event_id"] != projection || src["status"] != "inconsistent" || src["requires_refresh"] != true {
			t.Fatalf("source = %s", mustJSON(src))
		}
		if strings.Contains(mustJSON(body), "wss://deleted.example") {
			t.Fatalf("deleted projection was used: %s", mustJSON(body))
		}
	}
	assertRoleURL(t, cfg["account"].(map[string]any), "public_outbox", "wss://kept.example")
}

func readRelayAPI(t *testing.T, pool *pgxpool.Pool, pubkey string) (map[string]any, map[string]any, map[string]any) {
	t.Helper()
	handlers := &Handlers{relayIntel: relayintel.NewService(pool, relayintel.Options{})}
	mux := relayMux(handlers)
	cfg := getJSON(t, mux, "/api/v1/profiles/"+pubkey+"/relay-config")
	diag := getJSON(t, mux, "/api/v1/profiles/"+pubkey+"/relay-diagnostics")
	plan := postJSON(t, mux, map[string]any{"pubkey": pubkey, "preset": "balanced", "roles": []string{"public_outbox"}})
	return cfg, diag, plan
}

func relayMux(handlers *Handlers) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/relays/optimize", func(w http.ResponseWriter, r *http.Request) {
		handlers.PostRelayOptimize(w, r)
	})
	mux.HandleFunc("GET /api/v1/profiles/{pubkey}/relay-config", func(w http.ResponseWriter, r *http.Request) {
		handlers.GetProfileRelayConfig(w, r)
	})
	mux.HandleFunc("GET /api/v1/profiles/{pubkey}/relay-diagnostics", func(w http.ResponseWriter, r *http.Request) {
		handlers.GetProfileRelayDiagnostics(w, r)
	})
	return mux
}

func seedDivergedProjection(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tag string, canonicalAt, projectionAt int64, higherID bool) (pubkey, canonical, projection string) {
	t.Helper()
	base := map[string]int{"a1": 0xA100, "a2": 0xA200, "a3": 0xA300, "a4": 0xA400}[tag]
	pubkey = fmt.Sprintf("%064x", base+1)
	low := fmt.Sprintf("%064x", base+2)
	high := fmt.Sprintf("%064x", base+3)
	if higherID {
		canonical, projection = high, low
	} else {
		canonical, projection = low, high
	}
	insertRelayEvent(t, ctx, pool, canonical, pubkey, canonicalAt, [][]string{{"r", "wss://kept.example", "write"}})
	insertRelayEvent(t, ctx, pool, projection, pubkey, projectionAt, [][]string{{"r", "wss://deleted.example", "write"}})
	if _, err := pool.Exec(ctx, `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		VALUES ($1, 10002, '', $2, $3, 1)
	`, pubkey, canonical, canonicalAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_config_heads (
			pubkey, source_kind, d_tag, event_id, created_at, declaration_source
		) VALUES ($1, 10002, '', $2, $3, 'modern')
	`, pubkey, projection, projectionAt); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_role_memberships (
			pubkey, role, url_key, source_kind, d_tag, endpoint_url, endpoint_type,
			source_event_id, created_at, declaration_source, public_safe
		) VALUES ($1, 'public_outbox', 'deleted', 10002, '', 'wss://deleted.example', 'nostr_relay', $2, $3, 'modern', true)
	`, pubkey, projection, projectionAt); err != nil {
		t.Fatal(err)
	}
	tomb := fmt.Sprintf("%064x", base+9)
	if _, err := pool.Exec(ctx, `
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		VALUES ($1, $2, $3, 80, 1)
	`, tomb, pubkey, projection); err != nil {
		t.Fatal(err)
	}
	return pubkey, canonical, projection
}

func insertRelayEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, pubkey string, created int64, tags [][]string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id": id, "pubkey": pubkey, "created_at": created, "kind": relayintel.KindPublicRelayList,
		"tags": tags, "content": "", "sig": strings.Repeat("c", 128),
	})
	if err != nil {
		t.Fatal(err)
	}
	event := model.Event{
		ID: id, Pubkey: pubkey, CreatedAt: created, Kind: relayintel.KindPublicRelayList,
		Sig: strings.Repeat("c", 128), Content: "", RawJSON: raw,
		FirstSeenAt: time.Now().UTC(), InsertedAt: time.Now().UTC(),
	}
	if err := store.NewPostgresStore(pool).InsertCanonicalEvent(ctx, event, tags, "wss://index.example", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

func sourceByKind(t *testing.T, body map[string]any, kind int) map[string]any {
	t.Helper()
	sources, _ := body["sources"].([]any)
	for _, item := range sources {
		row := item.(map[string]any)
		if int(row["kind"].(float64)) == kind {
			return row
		}
	}
	t.Fatalf("missing source kind %d in %s", kind, mustJSON(body))
	return nil
}
