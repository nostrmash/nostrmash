package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/derivation"
	"github.com/nostrmash/nostrmash/internal/model"
	"github.com/nostrmash/nostrmash/internal/relayintel"
	"github.com/nostrmash/nostrmash/internal/store"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func TestRelayOptimizeAPIAgainstPostgreSQL(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay intel optimize"), "relayopt")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	if err := derivation.EnsureRegisteredDerivations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	handlers := &Handlers{relayIntel: relayintel.NewService(pool, relayintel.Options{})}
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
	mux.HandleFunc("GET /api/v1/profiles/{pubkey}/relay-recommendations", func(w http.ResponseWriter, r *http.Request) {
		handlers.GetProfileRelayRecommendations(w, r)
	})
	now := time.Now().Unix()
	ownerKey := newKey(t)
	friendKey := newKey(t)

	public := publish(t, ctx, pool, ownerKey, relayintel.KindPublicRelayList, now-100, "", [][]string{
		{"r", "wss://in.example", "read"},
		{"r", "wss://out.example", "write"},
	})
	legacy := publish(t, ctx, pool, ownerKey, relayintel.KindLegacyContact, now-400, "", [][]string{
		{"p", friendKey.pub},
		{"r", "wss://legacy.example"},
	})
	publish(t, ctx, pool, ownerKey, relayintel.KindDMRelays, now-90, "", [][]string{{"relay", "wss://dm.example"}})
	publish(t, ctx, pool, ownerKey, relayintel.KindSearchRelays, now-90, "", [][]string{{"relay", "wss://search.example"}})
	publish(t, ctx, pool, ownerKey, relayintel.KindBlockedRelays, now-90, "", [][]string{{"relay", "wss://blocked.example"}})
	publish(t, ctx, pool, ownerKey, relayintel.KindFavoriteRelays, now-90, "", [][]string{{"relay", "wss://favorite.example"}})
	publish(t, ctx, pool, ownerKey, relayintel.KindRelaySet, now-90, "", [][]string{{"d", "reading"}, {"relay", "wss://set.example"}})
	publish(t, ctx, pool, ownerKey, relayintel.KindPrivateRelays, now-90, "nip44-ciphertext", nil)
	publish(t, ctx, pool, ownerKey, relayintel.KindKeyPackageList, now-300, "", [][]string{{"relay", "wss://old-keypackage.example"}})
	publish(t, ctx, pool, friendKey, relayintel.KindPublicRelayList, now-80, "", [][]string{{"r", "wss://friend.example"}})
	var follows int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM follower_edges WHERE follower_pubkey = $1 AND followed_pubkey = $2
	`, ownerKey.pub, friendKey.pub).Scan(&follows); err != nil {
		t.Fatal(err)
	}
	if follows != 1 {
		t.Fatalf("follow edges = %d", follows)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_role_stats (
			url_key, role, endpoint_url, endpoint_type, distinct_users, trust_weighted,
			active_users, modern_users, legacy_users, net_user_delta_7d, computed_at
		)
		SELECT url_key, role, endpoint_url, endpoint_type, 12, 4, 8, 10, 2, 1, now()
		FROM relay_role_memberships
		ON CONFLICT (url_key, role) DO NOTHING
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_intel_snapshot (id, computed_at)
		VALUES (true, now())
		ON CONFLICT (id) DO UPDATE SET computed_at = now()
	`); err != nil {
		t.Fatal(err)
	}

	before := headIDs(t, ctx, pool, ownerKey.pub)
	cfg := getJSON(t, mux, "/api/v1/profiles/"+ownerKey.pub+"/relay-config")
	if cfg["account"].(map[string]any)["modern_event_id"] != public.id {
		t.Fatalf("config modern event = %v", cfg["account"].(map[string]any)["modern_event_id"])
	}
	if cfg["account"].(map[string]any)["legacy_event_id"] != legacy.id {
		t.Fatalf("config legacy event = %v", cfg["account"].(map[string]any)["legacy_event_id"])
	}
	account := cfg["account"].(map[string]any)
	assertRoleURL(t, account, "public_inbox", "wss://in.example")
	assertRoleURL(t, account, "public_outbox", "wss://out.example")
	assertRoleURL(t, account, "dm_inbox", "wss://dm.example")
	assertRoleURL(t, account, "search", "wss://search.example")
	assertRoleURL(t, account, "blocked", "wss://blocked.example")
	assertRoleURL(t, account, "favorite", "wss://favorite.example")
	sets := account["relay_sets"].([]any)
	if len(sets) != 1 || sets[0].(map[string]any)["d_tag"] != "reading" {
		t.Fatalf("relay sets = %v", sets)
	}
	if roleByName(t, account, "private_storage")["availability"] != "client_side" {
		t.Fatal("encrypted private configuration was indexed as a public list")
	}
	if strings.Contains(mustJSON(cfg), "nip44-ciphertext") {
		t.Fatal("private configuration content was returned")
	}

	diag := getJSON(t, mux, "/api/v1/profiles/"+ownerKey.pub+"/relay-diagnostics")
	if _, ok := diag["findings"].([]any); !ok {
		t.Fatalf("diagnostics = %s", mustJSON(diag))
	}
	recs := getJSON(t, mux, "/api/v1/profiles/"+ownerKey.pub+"/relay-recommendations?preset=balanced")
	if recs["preset"] != "balanced" || len(recs["roles"].([]any)) == 0 {
		t.Fatalf("recommendations = %s", mustJSON(recs))
	}
	if cohort, _ := recs["cohort"].(map[string]any); cohort["used"] == "none" || cohort["size"].(float64) < 1 {
		t.Fatalf("cohort = %v", recs["cohort"])
	}

	newer := sign(t, ownerKey.priv, relayintel.KindPublicRelayList, now-1, [][]string{
		{"r", "wss://in.example", "read"},
		{"r", "wss://out.example", "write"},
		{"r", "wss://extra.example", "read"},
	}, "")
	body := map[string]any{
		"pubkey":        ownerKey.pub,
		"preset":        "balanced",
		"roles":         []string{"public_inbox", "public_outbox", "dm_inbox", "search"},
		"source_events": []json.RawMessage{newer},
		"constraints": map[string]any{
			"max":             map[string]int{"public_inbox": 3, "public_outbox": 3, "dm_inbox": 2, "search": 2},
			"pinned":          []string{"wss://in.example"},
			"excluded":        []string{"wss://blocked.example"},
			"retain_existing": true,
			"by_role":         map[string]any{"search": map[string]any{"max": 1}},
		},
	}
	plan := postJSON(t, mux, body)
	assertOptimizeContract(t, plan)
	if !selected(plan, "public_inbox", "wss://in.example") {
		t.Fatalf("pinned inbox missing: %s", mustJSON(plan))
	}
	if selected(plan, "public_inbox", "wss://blocked.example") || selected(plan, "search", "wss://blocked.example") {
		t.Fatal("a blocked relay was selected")
	}
	proposal := plan["proposal"].(map[string]any)
	publicOut := proposal["public_relays"].(map[string]any)
	if publicOut["kind"].(float64) != 10002 || publicOut["based_on_event_id"] == public.id {
		t.Fatalf("replacement did not use the supplied source: %v", publicOut)
	}
	if _, ok := publicOut["sig"]; ok {
		t.Fatal("the server signed a replacement")
	}
	reparsed, err := relayintel.Parse(relayintel.EventInput{
		Pubkey: ownerKey.pub, EventID: "reparsed", Kind: 10002, CreatedAt: now,
		Tags: stringTags(publicOut["tags"]),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !memberURL(reparsed.Members, "wss://in.example") || !memberURL(reparsed.Members, "wss://extra.example") {
		t.Fatal("reparsed replacement lost a relay")
	}
	ready, _ := proposal["ready_to_sign"].(bool)
	satisfied, _ := plan["constraints_satisfied"].(bool)
	blocking, _ := proposal["blocking_reasons"].([]any)
	if ready != (proposal["proposal_safe"] == true && proposal["source_complete"] == true && satisfied && len(blocking) == 0 && proposal["requires_refresh"] != true && proposal["requires_review"] != true && publicOut != nil) {
		t.Fatalf("signing eligibility does not match the result: %s", mustJSON(plan))
	}
	if after := headIDs(t, ctx, pool, ownerKey.pub); after != before {
		t.Fatalf("optimize changed stored heads: %s -> %s", before, after)
	}

	stale := sign(t, ownerKey.priv, relayintel.KindPublicRelayList, now-500, [][]string{{"r", "wss://stale.example"}}, "")
	stalePlan := postJSON(t, mux, map[string]any{
		"pubkey": ownerKey.pub, "preset": "balanced", "roles": []string{"public_inbox", "public_outbox"},
		"source_events": []json.RawMessage{stale},
	})
	if !sourceStatus(stalePlan, relayintel.KindPublicRelayList, "stale_client") {
		t.Fatalf("stale source = %s", mustJSON(stalePlan["sources"]))
	}
	if selected(stalePlan, "public_inbox", "wss://stale.example") {
		t.Fatal("an older supplied event replaced the indexed list")
	}

	t.Run("stale projection", func(t *testing.T) {
		handlers.relayIntel = relayintel.NewService(pool, relayintel.Options{})
		older := publish(t, ctx, pool, ownerKey, relayintel.KindPublicRelayList, now-250, "", [][]string{{"r", "wss://old-head.example"}})
		if _, err := pool.Exec(ctx, `
			UPDATE relay_config_heads
			SET event_id = $2, created_at = $3
			WHERE pubkey = $1 AND source_kind = 10002 AND d_tag = ''
		`, ownerKey.pub, older.id, now-250); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = pool.Exec(ctx, `
				UPDATE relay_config_heads SET event_id = $2, created_at = $3
				WHERE pubkey = $1 AND source_kind = 10002 AND d_tag = ''
			`, ownerKey.pub, public.id, now-100)
		})
		got := getJSON(t, mux, "/api/v1/profiles/"+ownerKey.pub+"/relay-config")
		if !sourceStatus(got, relayintel.KindPublicRelayList, "stale") {
			t.Fatalf("sources = %s", mustJSON(got["sources"]))
		}
		if got["account"].(map[string]any)["modern_event_id"] != public.id {
			t.Fatal("a stale projection replaced the canonical event")
		}
	})

	t.Run("explicit empty and legacy fallback", func(t *testing.T) {
		emptyKey := newKey(t)
		publish(t, ctx, pool, emptyKey, relayintel.KindPublicRelayList, now-20, "", [][]string{})
		emptyCfg := getJSON(t, mux, "/api/v1/profiles/"+emptyKey.pub+"/relay-config")
		if emptyCfg["account"].(map[string]any)["modern_public"] != "empty" {
			t.Fatalf("empty list = %v", emptyCfg["account"])
		}
		legacyKey := newKey(t)
		publish(t, ctx, pool, legacyKey, relayintel.KindLegacyContact, now-20, `{"wss://only-legacy.example":{"read":true,"write":true}}`, nil)
		legacyCfg := getJSON(t, mux, "/api/v1/profiles/"+legacyKey.pub+"/relay-config")
		legacyAccount := legacyCfg["account"].(map[string]any)
		if legacyAccount["modern_event_id"] != nil && legacyAccount["modern_event_id"] != "" {
			t.Fatalf("legacy account invented a modern list: %v", legacyAccount["modern_event_id"])
		}
		assertRoleURL(t, legacyAccount, "public_inbox", "wss://only-legacy.example")
	})

	t.Run("deleted coordinate creation and recreation", func(t *testing.T) {
		key := newKey(t)
		search := publish(t, ctx, pool, key, relayintel.KindSearchRelays, now-30, "", [][]string{{"relay", "wss://gone-search.example"}})
		publish(t, ctx, pool, key, 5, now-10, "", [][]string{{"e", search.id}})
		deleted := getJSON(t, mux, "/api/v1/profiles/"+key.pub+"/relay-config")
		if !sourceStatus(deleted, relayintel.KindSearchRelays, "deleted") {
			t.Fatalf("deleted source = %s", mustJSON(deleted["sources"]))
		}
		created := postJSON(t, mux, map[string]any{
			"pubkey": key.pub, "preset": "minimal", "roles": []string{"public_inbox", "public_outbox"},
			"allow_create": true,
			"constraints":  map[string]any{"pinned": []string{"wss://created.example"}, "min": map[string]int{"public_inbox": 1, "public_outbox": 1}, "max": map[string]int{"public_inbox": 1, "public_outbox": 1}},
		})
		createdEvent := created["proposal"].(map[string]any)["public_relays"].(map[string]any)
		if createdEvent["change"] != "creation" || created["proposal"].(map[string]any)["requires_review"] != true {
			t.Fatalf("creation = %s", mustJSON(created["proposal"]))
		}
		if created["proposal"].(map[string]any)["ready_to_sign"] == true {
			t.Fatal("a newly created list was marked ready without client review")
		}
		recreated := postJSON(t, mux, map[string]any{
			"pubkey": key.pub, "preset": "minimal", "roles": []string{"search"}, "allow_recreate": true,
			"constraints": map[string]any{"pinned": []string{"wss://recreated.example"}, "min": map[string]int{"search": 1}, "max": map[string]int{"search": 1}},
		})
		lists := recreated["proposal"].(map[string]any)["lists"].([]any)
		if len(lists) != 1 || lists[0].(map[string]any)["change"] != "recreation" {
			t.Fatalf("recreation = %s", mustJSON(recreated["proposal"]))
		}
	})

	t.Run("blocked pin diversity and unverified keypackage", func(t *testing.T) {
		conflict := postJSON(t, mux, map[string]any{
			"pubkey": ownerKey.pub, "preset": "balanced", "roles": []string{"public_inbox"},
			"constraints": map[string]any{"pinned": []string{"wss://blocked.example"}, "min": map[string]int{"public_inbox": 1}, "max": map[string]int{"public_inbox": 1}},
		})
		if conflict["constraints_satisfied"] == true || (!hasReason(conflict, "blocked_by_account") && !hasReason(conflict, "pin_missing")) {
			t.Fatalf("blocked pin = %s", mustJSON(conflict))
		}
		diverse := postJSON(t, mux, map[string]any{
			"pubkey": ownerKey.pub, "preset": "decentralization", "roles": []string{"public_outbox"},
			"constraints": map[string]any{
				"require_operator_diversity": true,
				"min":                        map[string]int{"public_outbox": 1},
				"max":                        map[string]int{"public_outbox": 1},
				"pinned":                     []string{"wss://out.example"},
			},
		})
		if diverse["constraints_satisfied"] == true || (!hasReason(diverse, "operator_concentration") && !hasReason(diverse, "operator_ownership_unknown")) {
			t.Fatalf("diversity = %s", mustJSON(diverse))
		}
		keypackage := postJSON(t, mux, map[string]any{
			"pubkey": ownerKey.pub, "preset": "balanced", "roles": []string{"public_outbox", "keypackage"},
			"constraints": map[string]any{
				"by_role": map[string]any{"keypackage": map[string]any{"min_capability_confidence": 0.8}},
			},
		})
		if keypackage["constraints_satisfied"] == true || !hasReason(keypackage, "keypackage_unverified") {
			t.Fatalf("keypackage = %s", mustJSON(keypackage))
		}
		if keypackage["proposal"].(map[string]any)["ready_to_sign"] == true {
			t.Fatal("unverified KeyPackage evidence was ready to sign")
		}
	})

	t.Run("unreadable source and unchanged optimization", func(t *testing.T) {
		key := newKey(t)
		stored := publish(t, ctx, pool, key, relayintel.KindPublicRelayList, now-15, "", [][]string{{"r", "wss://readable.example"}})
		if _, err := pool.Exec(ctx, `UPDATE events SET raw_json = '{}'::jsonb WHERE id = $1`, stored.id); err != nil {
			t.Fatal(err)
		}
		unsafe := postJSON(t, mux, map[string]any{
			"pubkey": key.pub, "preset": "balanced", "roles": []string{"public_inbox", "public_outbox"},
		})
		if unsafe["proposal"].(map[string]any)["proposal_safe"] == true || unsafe["proposal"].(map[string]any)["ready_to_sign"] == true {
			t.Fatalf("unreadable source = %s", mustJSON(unsafe["proposal"]))
		}
		same := postJSON(t, mux, map[string]any{
			"pubkey": ownerKey.pub, "preset": "balanced", "roles": []string{"public_inbox", "public_outbox"},
			"constraints": map[string]any{
				"retain_existing": true,
				"min":             map[string]int{"public_inbox": 1, "public_outbox": 1},
				"max":             map[string]int{"public_inbox": 1, "public_outbox": 1},
				"by_role": map[string]any{
					"public_inbox":  map[string]any{"pinned": []string{"wss://in.example"}},
					"public_outbox": map[string]any{"pinned": []string{"wss://out.example"}},
				},
			},
		})
		if same["proposal"].(map[string]any)["public_relays"] != nil {
			t.Fatalf("unchanged optimization emitted a replacement: %s", mustJSON(same["proposal"]))
		}
		if same["proposal"].(map[string]any)["ready_to_sign"] == true {
			t.Fatal("an optimization with nothing to sign was ready to sign")
		}
		if after := headIDs(t, ctx, pool, ownerKey.pub); after != before {
			t.Fatalf("optimization changed stored configuration: %s", after)
		}
	})
}

type testKey struct {
	priv *btcec.PrivateKey
	pub  string
}

type storedID struct{ id string }

func newKey(t *testing.T) testKey {
	t.Helper()
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	return testKey{priv: priv, pub: hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))}
}

func sign(t *testing.T, priv *btcec.PrivateKey, kind int, created int64, tags [][]string, content string) json.RawMessage {
	t.Helper()
	if tags == nil {
		tags = [][]string{}
	}
	pub := hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))
	payload, err := json.Marshal([]any{0, pub, created, kind, tags, content})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	sig, err := schnorr.Sign(priv, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{
		"id": hex.EncodeToString(sum[:]), "pubkey": pub, "created_at": created,
		"kind": kind, "tags": tags, "content": content, "sig": hex.EncodeToString(sig.Serialize()),
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func publish(t *testing.T, ctx context.Context, pool *pgxpool.Pool, key testKey, kind int, created int64, content string, tags [][]string) storedID {
	t.Helper()
	raw := sign(t, key.priv, kind, created, tags, content)
	var body struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	event := model.Event{
		ID: body.ID, Pubkey: key.pub, CreatedAt: created, Kind: kind,
		Sig: strings.Repeat("c", 128), Content: content, RawJSON: raw,
		FirstSeenAt: time.Now().UTC(), InsertedAt: time.Now().UTC(),
	}
	if tags == nil {
		tags = [][]string{}
	}
	if err := store.NewPostgresStore(pool).InsertCanonicalEvent(ctx, event, tags, "wss://index.example", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := derivation.NewHandlers(pool).DeriveEventBundle(ctx, body.ID); err != nil {
		t.Fatal(err)
	}
	return storedID{id: body.ID}
}

func getJSON(t *testing.T, mux *http.ServeMux, path string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s status %d: %s", path, rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func postJSON(t *testing.T, mux *http.ServeMux, body map[string]any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/relays/optimize", bytes.NewReader(encoded))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("optimize status %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	validateOptimizeSchema(t, out)
	assertSigningContract(t, out)
	return out
}

func assertRoleURL(t *testing.T, account map[string]any, role, url string) {
	t.Helper()
	item := roleByName(t, account, role)
	for _, relay := range item["relays"].([]any) {
		if relay.(map[string]any)["url"] == url {
			return
		}
	}
	t.Fatalf("role %s does not contain %s: %s", role, url, mustJSON(item))
}

func roleByName(t *testing.T, account map[string]any, role string) map[string]any {
	t.Helper()
	for _, item := range account["roles"].([]any) {
		row := item.(map[string]any)
		if row["role"] == role {
			return row
		}
	}
	t.Fatalf("missing role %s", role)
	return nil
}

func selected(plan map[string]any, role, url string) bool {
	for _, item := range plan["roles"].([]any) {
		row := item.(map[string]any)
		if row["role"] != role {
			continue
		}
		selection := row["selection"].(map[string]any)
		for _, relay := range selection["selected"].([]any) {
			if relay.(map[string]any)["url"] == url {
				return true
			}
		}
	}
	return false
}

func sourceStatus(body map[string]any, kind int, status string) bool {
	sources, _ := body["sources"].([]any)
	for _, item := range sources {
		row := item.(map[string]any)
		if int(row["kind"].(float64)) == kind && row["status"] == status {
			return true
		}
	}
	return false
}

func hasReason(plan map[string]any, code string) bool {
	if reasons, ok := plan["unsatisfied_constraints"].([]any); ok && reasonCode(reasons, code) {
		return true
	}
	for _, item := range plan["roles"].([]any) {
		selection := item.(map[string]any)["selection"].(map[string]any)
		if reasonCode(selection["unsatisfied_constraints"], code) {
			return true
		}
	}
	proposal, _ := plan["proposal"].(map[string]any)
	return reasonCode(proposal["blocking_reasons"], code)
}

func reasonCode(value any, code string) bool {
	items, _ := value.([]any)
	for _, item := range items {
		if item.(map[string]any)["code"] == code {
			return true
		}
	}
	return false
}

func memberURL(members []relayintel.Membership, url string) bool {
	for _, member := range members {
		if member.EndpointURL == url {
			return true
		}
	}
	return false
}

func stringTags(value any) [][]string {
	rows, _ := value.([]any)
	out := make([][]string, 0, len(rows))
	for _, row := range rows {
		parts, _ := row.([]any)
		tag := make([]string, len(parts))
		for i, part := range parts {
			tag[i], _ = part.(string)
		}
		out = append(out, tag)
	}
	return out
}

func headIDs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) string {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT source_kind, d_tag, event_id
		FROM relay_config_heads
		WHERE pubkey = $1
		ORDER BY source_kind, d_tag
	`, pubkey)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var kind int
		var dTag, id string
		if err := rows.Scan(&kind, &dTag, &id); err != nil {
			t.Fatal(err)
		}
		b.WriteString(strings.Join([]string{strings.TrimSpace(dTag), id}, ":"))
		b.WriteByte('\n')
	}
	return b.String()
}

func assertOptimizeContract(t *testing.T, plan map[string]any) {
	t.Helper()
	root := moduleRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "docs", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	paths, _ := doc["paths"].(map[string]any)
	route, _ := paths["/api/v1/relays/optimize"].(map[string]any)
	post, _ := route["post"].(map[string]any)
	responses, _ := post["responses"].(map[string]any)
	ok, _ := responses["200"].(map[string]any)
	content, _ := ok["content"].(map[string]any)
	media, _ := content["application/json"].(map[string]any)
	example, _ := media["example"].(map[string]any)
	if len(example) == 0 {
		t.Fatal("openapi optimize response has no example")
	}
	assertExampleShape(t, example, plan)
	validateOptimizeSchema(t, plan)
	assertSigningContract(t, plan)
	if plan["algorithm"] == "" || plan["optimizer_version"] == "" || plan["tie_break"] == "" {
		t.Fatalf("version fields = %s", mustJSON(plan))
	}
	if _, ok := plan["constraints_satisfied"].(bool); !ok {
		t.Fatal("constraints_satisfied is not a boolean")
	}
	proposal, _ := plan["proposal"].(map[string]any)
	if _, ok := proposal["sig"]; ok {
		t.Fatal("proposal carries a signature")
	}
}

func assertExampleShape(t *testing.T, example, got map[string]any) {
	t.Helper()
	for key, sample := range example {
		value, ok := got[key]
		if !ok {
			t.Fatalf("response is missing documented field %s", key)
		}
		switch sample := sample.(type) {
		case map[string]any:
			child, ok := value.(map[string]any)
			if !ok {
				t.Fatalf("documented object %s has type %T", key, value)
			}
			assertExampleShape(t, sample, child)
		case []any:
			if _, ok := value.([]any); !ok {
				t.Fatalf("documented array %s has type %T", key, value)
			}
		}
	}
}

func validateOptimizeSchema(t *testing.T, plan map[string]any) {
	t.Helper()
	doc := openAPIDocument(t)
	paths, _ := doc["paths"].(map[string]any)
	route, _ := paths["/api/v1/relays/optimize"].(map[string]any)
	post, _ := route["post"].(map[string]any)
	responses, _ := post["responses"].(map[string]any)
	ok, _ := responses["200"].(map[string]any)
	content, _ := ok["content"].(map[string]any)
	media, _ := content["application/json"].(map[string]any)
	schema, _ := media["schema"].(map[string]any)
	if len(schema) == 0 {
		t.Fatal("openapi optimize response has no schema")
	}
	validateSchema(t, doc, schema, plan, "response")
}

func assertSigningContract(t *testing.T, plan map[string]any) {
	t.Helper()
	proposal, ok := plan["proposal"].(map[string]any)
	if !ok {
		t.Fatal("proposal is not an object")
	}
	ready, _ := proposal["ready_to_sign"].(bool)
	safe, _ := proposal["proposal_safe"].(bool)
	complete, _ := proposal["source_complete"].(bool)
	refresh, _ := proposal["requires_refresh"].(bool)
	review, _ := proposal["requires_review"].(bool)
	satisfied, _ := plan["constraints_satisfied"].(bool)
	blocking, _ := proposal["blocking_reasons"].([]any)
	hasEvent := proposal["public_relays"] != nil
	if lists, ok := proposal["lists"].([]any); ok && len(lists) > 0 {
		hasEvent = true
	}
	wantReady := safe && complete && satisfied && len(blocking) == 0 && !refresh && !review && hasEvent
	if ready != wantReady {
		t.Fatalf("ready_to_sign=%v, eligibility=%v: %s", ready, wantReady, mustJSON(proposal))
	}
	roles, _ := plan["roles"].([]any)
	roleUnsatisfied := false
	for _, item := range roles {
		selection, _ := item.(map[string]any)["selection"].(map[string]any)
		if selection["constraints_satisfied"] == false {
			roleUnsatisfied = true
		}
		if satisfied && selection["constraints_satisfied"] != true {
			t.Fatalf("satisfied plan has an unsatisfied role: %s", mustJSON(item))
		}
	}
	topUnsatisfied := false
	if reasons, ok := plan["unsatisfied_constraints"].([]any); ok && len(reasons) > 0 {
		topUnsatisfied = true
	}
	if satisfied && topUnsatisfied {
		t.Fatal("satisfied plan still lists unsatisfied constraints")
	}
	if !satisfied && !roleUnsatisfied && !topUnsatisfied {
		t.Fatal("constraints_satisfied is false without an unsatisfied constraint")
	}
	checkEvent := func(event map[string]any) {
		t.Helper()
		if _, ok := event["sig"]; ok {
			t.Fatal("proposal event includes a signature")
		}
		if _, ok := event["id"]; ok {
			t.Fatal("proposal event includes an id")
		}
		for _, key := range []string{"kind", "pubkey", "content", "tags"} {
			if _, ok := event[key]; !ok {
				t.Fatalf("proposal event is missing %s", key)
			}
		}
		change, _ := event["change"].(string)
		if change == "creation" || change == "recreation" {
			if proposal["requires_review"] != true || ready {
				t.Fatalf("%s event is eligible to sign: %s", change, mustJSON(proposal))
			}
		}
	}
	if event, ok := proposal["public_relays"].(map[string]any); ok {
		checkEvent(event)
	}
	if lists, ok := proposal["lists"].([]any); ok {
		for _, item := range lists {
			checkEvent(item.(map[string]any))
		}
	}
}

func openAPIDocument(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(moduleRoot(t), "docs", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func validateSchema(t *testing.T, doc, schema map[string]any, value any, path string) {
	t.Helper()
	if ref, ok := schema["$ref"].(string); ok {
		schema = schemaByRef(t, doc, ref)
	}
	if value == nil {
		if schema["nullable"] == true {
			return
		}
		t.Fatalf("%s is null", path)
	}
	switch schema["type"] {
	case "object":
		obj, ok := value.(map[string]any)
		if !ok {
			t.Fatalf("%s has type %T, want object", path, value)
		}
		props, _ := schema["properties"].(map[string]any)
		if required, ok := schema["required"].([]any); ok {
			for _, key := range required {
				name, _ := key.(string)
				if _, exists := obj[name]; !exists {
					t.Fatalf("%s is missing %s", path, name)
				}
			}
		}
		additional, limited := schema["additionalProperties"]
		for key, child := range obj {
			prop, ok := props[key].(map[string]any)
			if !ok {
				if limited && additional == false {
					t.Fatalf("%s has undocumented field %s", path, key)
				}
				if addSchema, ok := additional.(map[string]any); ok {
					validateSchema(t, doc, addSchema, child, path+"."+key)
				}
				continue
			}
			validateSchema(t, doc, prop, child, path+"."+key)
		}
	case "array":
		items, ok := value.([]any)
		if !ok {
			t.Fatalf("%s has type %T, want array", path, value)
		}
		itemSchema, _ := schema["items"].(map[string]any)
		if itemSchema == nil {
			return
		}
		for i, item := range items {
			validateSchema(t, doc, itemSchema, item, fmt.Sprintf("%s[%d]", path, i))
		}
	case "string":
		text, ok := value.(string)
		if !ok {
			t.Fatalf("%s has type %T, want string", path, value)
		}
		if schema["format"] == "date-time" {
			if _, err := time.Parse(time.RFC3339Nano, text); err != nil {
				t.Fatalf("%s is not a timestamp: %s", path, text)
			}
		}
		assertEnum(t, schema, text, path)
	case "boolean":
		if _, ok := value.(bool); !ok {
			t.Fatalf("%s has type %T, want boolean", path, value)
		}
	case "integer":
		number, ok := value.(float64)
		if !ok || math.Trunc(number) != number {
			t.Fatalf("%s has type %T, want integer", path, value)
		}
	case "number":
		if _, ok := value.(float64); !ok {
			t.Fatalf("%s has type %T, want number", path, value)
		}
	}
}

func assertEnum(t *testing.T, schema map[string]any, value, path string) {
	t.Helper()
	enum, ok := schema["enum"].([]any)
	if !ok {
		return
	}
	for _, item := range enum {
		if item == value {
			return
		}
	}
	t.Fatalf("%s = %v, not one of %v", path, value, enum)
}

func schemaByRef(t *testing.T, doc map[string]any, ref string) map[string]any {
	t.Helper()
	const prefix = "#/components/schemas/"
	if !strings.HasPrefix(ref, prefix) {
		t.Fatalf("unsupported schema ref %s", ref)
	}
	components, _ := doc["components"].(map[string]any)
	schemas, _ := components["schemas"].(map[string]any)
	schema, _ := schemas[strings.TrimPrefix(ref, prefix)].(map[string]any)
	if schema == nil {
		t.Fatalf("missing schema %s", ref)
	}
	return schema
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

func mustJSON(value any) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
