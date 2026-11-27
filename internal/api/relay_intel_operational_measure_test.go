//go:build relayintelbench

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/config"
	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/derivation"
	"github.com/nostrmash/nostrmash/internal/jobs"
	"github.com/nostrmash/nostrmash/internal/meili"
	"github.com/nostrmash/nostrmash/internal/model"
	"github.com/nostrmash/nostrmash/internal/relayintel"
	"github.com/nostrmash/nostrmash/internal/store"
	"github.com/nostrmash/nostrmash/internal/store/retention"
)

const pgCacheNote = "postgres shared buffers were not flushed; this is not a cold-database measurement"

func runOperationalScale(t *testing.T, accounts int, outDir string) {
	t.Helper()
	ctx := context.Background()
	counter := &queryCounter{}
	pool, schema := openOperationalPool(t, ctx, counter)
	scale := "small"
	if accounts >= 100000 {
		scale = "medium"
	}
	report := newOpReport(filepath.Join(outDir, "results-"+scale+".json"), scale, accounts)
	partial := os.Getenv("RELAY_INTEL_OPERATIONAL_PARTIAL") == "1"
	report.note(fmt.Sprintf("Benchmark tooling commit %s. Tested implementation %s. Frozen core %s incorporated=%t.", report.Commit, report.ImplementationCommit, report.CoreCommit, report.CoreIncorporated))
	if partial {
		report.note("PARTIAL benchmark. This output is not operational sign-off.")
	}
	report.Environment = collectEnvironment(ctx, pool, schema)
	report.Environment["seed_session_work_mem"] = "64MB on the seed connection only"
	report.save()
	t.Logf("migrating schema %s", schema)
	if err := dbmigrate.Migrate(ctx, pool, "bench"); err != nil {
		t.Fatal(err)
	}
	membershipColumn := membershipColumnPresent(ctx, pool)
	report.Environment["core_membership_column"] = membershipColumn
	report.Environment["core_incorporated"] = report.CoreIncorporated
	report.Environment["tested_implementation_commit"] = integratedImplementationCommit
	report.FinalSignoff = report.CoreIncorporated && membershipColumn && !partial
	report.save()
	if os.Getenv("RELAY_INTEL_OPERATIONAL_REQUIRE_FINAL") == "1" && !report.FinalSignoff {
		t.Fatalf("final benchmark refused: implementation %s containing core %s must be in HEAD and relay_config_heads.membership_count must exist (incorporated=%t column=%t partial=%t)", integratedImplementationCommit, frozenCoreCommit, report.CoreIncorporated, membershipColumn, partial)
	}
	if err := derivation.EnsureRegisteredDerivations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	fixture := seedOperationalFixture(t, ctx, pool, accounts)
	report.mu.Lock()
	report.Fixture = fixture.Counts
	report.Fixture["seed_ms"] = fixture.SeedMS
	report.Fixture["popular_relay"] = fixture.PopularRelay
	report.Fixture["target_accounts"] = accounts
	report.mu.Unlock()
	report.save()

	people := publishBenchPersonas(t, ctx, pool)
	drained := drainJobs(t, ctx, pool, "bench-fixture", 400)
	report.note(fmt.Sprintf("fixture derivation queue drain processed %d jobs in %s", drained.Processed, drained.Elapsed.Round(time.Millisecond)))
	report.mu.Lock()
	report.Fixture["personas"] = people.public()
	report.mu.Unlock()

	measureMaintenance(t, ctx, pool, counter, report, people, true, "")
	handlers := newBenchHandlers(t, pool)
	measureAPI(t, ctx, pool, counter, handlers, people, fixture.PopularRelay, report)
	measurePlans(t, ctx, pool, people, filepath.Join(outDir, "plans-"+scale+".txt"), report)
	measureMaintenance(t, ctx, pool, counter, report, people, false, filepath.Join(outDir, "plans-"+scale+".txt"))
	measureProbes(t, ctx, pool, report)
	measureSynthetic(t, ctx, pool, counter, report)
	report.mu.Lock()
	report.Storage = map[string]any{"after_population": relationSizes(ctx, pool)}
	report.mu.Unlock()
	report.save()
	measureMixed(t, ctx, pool, counter, people, report)
	report.mu.Lock()
	report.Storage["after_mixed"] = relationSizes(ctx, pool)
	report.mu.Unlock()
	report.save()
	t.Logf("wrote %s", report.path)
}

type benchPeople struct {
	Dense      testKey
	Sparse     testKey
	Creator    testKey
	Privacy    testKey
	Multi      testKey
	Empty      testKey
	Legacy     testKey
	Truncated  testKey
	Shared     testKey
	Missing    testKey
	NoteID     string
	ProfileID  string
	SourceRaw  json.RawMessage
	SourcePubs string
}

func (p benchPeople) public() map[string]string {
	return map[string]string{
		"dense": p.Dense.pub, "sparse": p.Sparse.pub, "creator": p.Creator.pub,
		"privacy": p.Privacy.pub, "multi": p.Multi.pub, "empty": p.Empty.pub,
		"legacy": p.Legacy.pub, "truncated": p.Truncated.pub, "shared": p.Shared.pub,
		"missing": p.Missing.pub, "note_id": p.NoteID,
	}
}

func publishBenchPersonas(t *testing.T, ctx context.Context, pool *pgxpool.Pool) benchPeople {
	t.Helper()
	now := time.Now().Unix()
	people := benchPeople{
		Dense: newKey(t), Sparse: newKey(t), Creator: newKey(t), Privacy: newKey(t),
		Multi: newKey(t), Empty: newKey(t), Legacy: newKey(t), Truncated: newKey(t),
		Shared: newKey(t), Missing: newKey(t),
	}
	follows := make([][]string, 0, 400)
	for i := 2000; i < 2400; i++ {
		follows = append(follows, []string{"p", fmt.Sprintf("%064x", 100000+i)})
	}
	sparseFollows := [][]string{
		{"p", fmt.Sprintf("%064x", 100000+2100)},
		{"p", fmt.Sprintf("%064x", 100000+2101)},
		{"p", fmt.Sprintf("%064x", 100000+2102)},
	}
	publish(t, ctx, pool, people.Dense, 0, now-500, `{"name":"dense","display_name":"Dense"}`, nil)
	people.ProfileID = publish(t, ctx, pool, people.Dense, 0, now-400, `{"name":"dense","display_name":"Dense"}`, nil).id
	publish(t, ctx, pool, people.Dense, relayintel.KindPublicRelayList, now-300, "", [][]string{
		{"r", "wss://r0.op0.example", "read"},
		{"r", "wss://r1.op1.example", "read"},
		{"r", "wss://r2.op2.example", "write"},
		{"r", "wss://r3.op3.example", "write"},
		{"r", "wss://r8.op8.example"},
	})
	publish(t, ctx, pool, people.Dense, relayintel.KindDMRelays, now-290, "", [][]string{{"relay", "wss://r4.op4.example"}, {"relay", "wss://r5.op5.example"}})
	publish(t, ctx, pool, people.Dense, relayintel.KindSearchRelays, now-290, "", [][]string{{"relay", "wss://r6.op6.example"}})
	publish(t, ctx, pool, people.Dense, relayintel.KindBlockedRelays, now-290, "", [][]string{{"relay", "wss://r19.op19.example"}})
	publish(t, ctx, pool, people.Dense, relayintel.KindFavoriteRelays, now-290, "", [][]string{{"relay", "wss://r9.op9.example"}})
	publish(t, ctx, pool, people.Dense, 3, now-280, "", follows)
	people.NoteID = publish(t, ctx, pool, people.Dense, 1, now-10, "operational benchmark note", [][]string{{"t", "relay"}}).id

	publish(t, ctx, pool, people.Sparse, relayintel.KindPublicRelayList, now-200, "", [][]string{
		{"r", "wss://r40.op0.example", "read"},
		{"r", "wss://r41.op1.example", "write"},
	})
	publish(t, ctx, pool, people.Sparse, 3, now-190, "", sparseFollows)

	publish(t, ctx, pool, people.Creator, relayintel.KindPublicRelayList, now-200, "", [][]string{
		{"r", "wss://r0.op0.example", "read"},
		{"r", "wss://r2.op2.example", "write"},
		{"r", "wss://r3.op3.example", "write"},
		{"r", "wss://r10.op10.example", "write"},
		{"r", "wss://r11.op11.example", "write"},
	})
	publish(t, ctx, pool, people.Creator, 3, now-180, "", sparseFollows)

	publish(t, ctx, pool, people.Privacy, relayintel.KindPublicRelayList, now-200, "", [][]string{
		{"r", "wss://r1.op1.example", "read"},
		{"r", "wss://r12.op12.example", "write"},
	})
	publish(t, ctx, pool, people.Privacy, relayintel.KindDMRelays, now-180, "", [][]string{{"relay", "wss://r4.op4.example"}, {"relay", "wss://r13.op13.example"}})
	publish(t, ctx, pool, people.Privacy, relayintel.KindBlockedRelays, now-180, "", [][]string{{"relay", "wss://r19.op19.example"}})
	publish(t, ctx, pool, people.Privacy, 3, now-170, "", sparseFollows)

	publish(t, ctx, pool, people.Multi, relayintel.KindPublicRelayList, now-200, "", [][]string{
		{"r", "wss://r0.op0.example", "read"},
		{"r", "wss://r1.op1.example", "read"},
		{"r", "wss://r2.op2.example", "write"},
		{"r", "wss://r8.op8.example"},
	})
	publish(t, ctx, pool, people.Multi, relayintel.KindDMRelays, now-180, "", [][]string{{"relay", "wss://r4.op4.example"}})
	publish(t, ctx, pool, people.Multi, relayintel.KindSearchRelays, now-180, "", [][]string{{"relay", "wss://r6.op6.example"}, {"relay", "wss://r14.op14.example"}})
	publish(t, ctx, pool, people.Multi, relayintel.KindFavoriteRelays, now-180, "", [][]string{{"relay", "wss://r9.op9.example"}})
	publish(t, ctx, pool, people.Multi, relayintel.KindBlockedRelays, now-180, "", [][]string{{"relay", "wss://r19.op19.example"}})
	publish(t, ctx, pool, people.Multi, relayintel.KindRelaySet, now-180, "", [][]string{{"d", "reading"}, {"relay", "wss://r15.op15.example"}})
	publish(t, ctx, pool, people.Multi, 3, now-170, "", follows[:40])

	publish(t, ctx, pool, people.Empty, relayintel.KindPublicRelayList, now-100, "", [][]string{})
	publish(t, ctx, pool, people.Legacy, relayintel.KindLegacyContact, now-100, `{"wss://r16.op16.example":{"read":true,"write":true},"wss://r17.op17.example":{"read":true,"write":false}}`, [][]string{{"p", fmt.Sprintf("%064x", 100000+2200)}})
	publish(t, ctx, pool, people.Truncated, relayintel.KindPublicRelayList, now-100, "", [][]string{
		{"r", "wss://r20.op20.example", "read"},
		{"r", "wss://r21.op21.example", "write"},
	})
	if _, err := pool.Exec(ctx, `
		UPDATE relay_config_heads SET truncated = true
		WHERE pubkey = $1 AND source_kind = 10002
	`, people.Truncated.pub); err != nil {
		t.Fatal(err)
	}
	publish(t, ctx, pool, people.Shared, relayintel.KindPublicRelayList, now-100, "", [][]string{
		{"r", "wss://r2.op2.example", "write"},
		{"r", "wss://r3.op3.example", "write"},
		{"r", "wss://r1.op1.example", "read"},
	})
	publish(t, ctx, pool, people.Shared, 3, now-90, "", sparseFollows)
	people.SourceRaw = sign(t, people.Dense.priv, relayintel.KindPublicRelayList, now-1, [][]string{
		{"r", "wss://r0.op0.example", "read"},
		{"r", "wss://r1.op1.example", "read"},
		{"r", "wss://r2.op2.example", "write"},
		{"r", "wss://r3.op3.example", "write"},
		{"r", "wss://r8.op8.example"},
		{"r", "wss://r18.op18.example", "read"},
	}, "")
	var source struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(people.SourceRaw, &source); err != nil {
		t.Fatal(err)
	}
	people.SourcePubs = source.ID
	return people
}

func newBenchHandlers(t *testing.T, pool *pgxpool.Pool) Handlers {
	t.Helper()
	pgStore := store.NewPostgresStore(pool)
	handlers, err := NewHandlersWithOptions(pgStore, HandlersOptions{
		Pool:         pool,
		MaxBatchSize: 50,
		RelayIntel:   config.RelayIntelConfig{Enabled: true, OptimizeConcurrency: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	return handlers
}

type httpResult struct {
	Status int
	Body   map[string]any
	Raw    string
}

func callGET(fn func(http.ResponseWriter, *http.Request), path, pubkey string) httpResult {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if pubkey != "" {
		req.SetPathValue("pubkey", pubkey)
	}
	if id := pathID(path); id != "" {
		req.SetPathValue("id", id)
	}
	rec := httptest.NewRecorder()
	fn(rec, req)
	return decodeRecorder(rec)
}

func pathID(path string) string {
	const marker = "/api/v1/events/"
	if strings.HasPrefix(path, marker) {
		return strings.TrimPrefix(path, marker)
	}
	return ""
}

func callPOST(fn func(http.ResponseWriter, *http.Request), body map[string]any) httpResult {
	return callPOSTContext(context.Background(), fn, body)
}

func callPOSTContext(ctx context.Context, fn func(http.ResponseWriter, *http.Request), body map[string]any) httpResult {
	encoded, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/relays/optimize", bytes.NewReader(encoded)).WithContext(ctx)
	rec := httptest.NewRecorder()
	fn(rec, req)
	return decodeRecorder(rec)
}

func decodeRecorder(rec *httptest.ResponseRecorder) httpResult {
	out := httpResult{Status: rec.Code, Raw: rec.Body.String()}
	_ = json.Unmarshal(rec.Body.Bytes(), &out.Body)
	return out
}

func planProof(body map[string]any) map[string]any {
	proof := map[string]any{}
	if body == nil {
		return proof
	}
	proof["preset"] = body["preset"]
	if cohort, ok := body["cohort"].(map[string]any); ok {
		proof["cohort_size"] = cohort["size"]
		proof["cohort_used"] = cohort["used"]
		proof["cohort_truncated"] = cohort["truncated"]
	}
	roles, _ := body["roles"].([]any)
	proof["roles"] = len(roles)
	selected := 0
	for _, item := range roles {
		row, _ := item.(map[string]any)
		selection, _ := row["selection"].(map[string]any)
		if selection == nil {
			continue
		}
		chosen, _ := selection["selected"].([]any)
		selected += len(chosen)
	}
	proof["selected"] = selected
	if proposal, ok := body["proposal"].(map[string]any); ok {
		if public, ok := proposal["public_relays"].(map[string]any); ok {
			proof["proposal_kind"] = public["kind"]
			proof["based_on_event_id"] = public["based_on_event_id"]
		}
		proof["proposal_safe"] = proposal["proposal_safe"]
	}
	proof["constraints_satisfied"] = body["constraints_satisfied"]
	if sources, ok := body["sources"].([]any); ok {
		proof["sources"] = len(sources)
	}
	return proof
}

func planMeaningful(body map[string]any, requirePersonalized bool) bool {
	proof := planProof(body)
	selected, _ := proof["selected"].(int)
	if selected < 1 {
		return false
	}
	if !requirePersonalized {
		return true
	}
	return proof["cohort_used"] == "personalized"
}

func measureAPI(t *testing.T, ctx context.Context, pool *pgxpool.Pool, counter *queryCounter, base Handlers, people benchPeople, relayURL string, report *opReport) {
	t.Helper()
	t.Log("api measurements")
	results := map[string]any{}
	put := func(key string, value latencySummary) {
		results[key] = value
	}
	svc := func() *relayintel.Service { return relayintel.NewService(pool, relayintel.Options{}) }
	base.relayIntel = svc()
	put("roles_cold", timeGET(t, "roles", "application cache cold", 40, func() httpResult {
		h := base
		h.relayIntel = svc()
		return callGET(h.GetRelayRoles, "/api/v1/relays/roles", "")
	}, func(body map[string]any) bool {
		roles, _ := body["roles"].([]any)
		return len(roles) > 0
	}, counter))
	base.relayIntel = svc()
	put("roles_warm", timeGET(t, "roles", "application cache warm", 100, func() httpResult {
		return callGET(base.GetRelayRoles, "/api/v1/relays/roles", "")
	}, func(body map[string]any) bool {
		roles, _ := body["roles"].([]any)
		return len(roles) > 0
	}, counter))
	put("rankings_cold", timeGET(t, "rankings", "application cache cold", 40, func() httpResult {
		h := base
		h.relayIntel = svc()
		return callGET(h.GetRelayRankings, "/api/v1/relays/rankings?role=public_inbox&limit=20", "")
	}, rankingsMeaningful, counter))
	base.relayIntel = svc()
	put("rankings_warm", timeGET(t, "rankings", "application cache warm", 100, func() httpResult {
		return callGET(base.GetRelayRankings, "/api/v1/relays/rankings?role=public_inbox&limit=20", "")
	}, rankingsMeaningful, counter))
	capPath := "/api/v1/relays/capabilities?relay_url=" + relayURL
	put("capabilities_cold", timeGET(t, "capabilities", "application cache cold", 40, func() httpResult {
		h := base
		h.relayIntel = svc()
		return callGET(h.GetRelayCapabilities, capPath, "")
	}, capabilitiesMeaningful, counter))
	base.relayIntel = svc()
	put("capabilities_warm", timeGET(t, "capabilities", "application cache warm", 100, func() httpResult {
		return callGET(base.GetRelayCapabilities, capPath, "")
	}, capabilitiesMeaningful, counter))

	for _, item := range []struct {
		name, pubkey, path string
		ok                 func(map[string]any) bool
	}{
		{"config_dense", people.Dense.pub, "/api/v1/profiles/" + people.Dense.pub + "/relay-config", configHasRelays},
		{"diagnostics_dense", people.Dense.pub, "/api/v1/profiles/" + people.Dense.pub + "/relay-diagnostics", diagnosticsOK},
		{"recommendations_dense", people.Dense.pub, "/api/v1/profiles/" + people.Dense.pub + "/relay-recommendations?preset=balanced", func(b map[string]any) bool { return planMeaningful(b, true) }},
		{"recommendations_sparse", people.Sparse.pub, "/api/v1/profiles/" + people.Sparse.pub + "/relay-recommendations?preset=balanced", func(b map[string]any) bool { return planMeaningful(b, true) }},
		{"config_empty", people.Empty.pub, "/api/v1/profiles/" + people.Empty.pub + "/relay-config", func(b map[string]any) bool { return b["account"] != nil }},
		{"config_missing", people.Missing.pub, "/api/v1/profiles/" + people.Missing.pub + "/relay-config", func(b map[string]any) bool { return b["account"] != nil }},
	} {
		put(item.name+"_cold", timeGET(t, item.name, "application cache cold", 20, func() httpResult {
			h := base
			h.relayIntel = svc()
			return callGET(endpointHandler(h, item.path), item.path, item.pubkey)
		}, item.ok, counter))
		h := base
		h.relayIntel = svc()
		handler := endpointHandler(h, item.path)
		put(item.name+"_warm", timeGET(t, item.name, "application cache warm", 40, func() httpResult {
			return callGET(handler, item.path, item.pubkey)
		}, item.ok, counter))
	}
	put("profile_existing", timeGET(t, "profile", "existing profile handler", 40, func() httpResult {
		return callGET(base.GetProfileByPubkey, "/api/v1/profiles/"+people.Dense.pub, people.Dense.pub)
	}, func(body map[string]any) bool { return body["pubkey"] == people.Dense.pub }, counter))
	put("event_existing", timeGET(t, "event", "existing event handler", 40, func() httpResult {
		return callGET(base.GetEventByID, "/api/v1/events/"+people.NoteID, "")
	}, func(body map[string]any) bool { return body["event"] != nil || body["id"] != nil || len(body) > 0 }, counter))

	scenarios := optimizeScenarios(people)
	for _, scenario := range scenarios {
		put(scenario.name, timePOST(t, scenario.name, scenario.samples, scenario.body, scenario.personalized, counter, base, svc))
	}
	results["optimize_concurrency"] = measureOptimizeConcurrency(t, ctx, pool, counter, people)
	report.mu.Lock()
	report.API = results
	report.mu.Unlock()
	report.save()
}

func rankingsMeaningful(body map[string]any) bool {
	relays, _ := body["relays"].([]any)
	return len(relays) > 0
}

func capabilitiesMeaningful(body map[string]any) bool {
	caps, _ := body["capabilities"].(map[string]any)
	return len(caps) > 0 && body["relay_url"] != ""
}

func configHasRelays(body map[string]any) bool {
	account, _ := body["account"].(map[string]any)
	roles, _ := account["roles"].([]any)
	for _, item := range roles {
		row, _ := item.(map[string]any)
		relays, _ := row["relays"].([]any)
		if len(relays) > 0 {
			return true
		}
	}
	return false
}

func diagnosticsOK(body map[string]any) bool {
	_, ok := body["findings"].([]any)
	return ok && body["pubkey"] != ""
}

func endpointHandler(h Handlers, path string) func(http.ResponseWriter, *http.Request) {
	switch {
	case strings.Contains(path, "/relay-config"):
		return h.GetProfileRelayConfig
	case strings.Contains(path, "/relay-diagnostics"):
		return h.GetProfileRelayDiagnostics
	case strings.Contains(path, "/relay-recommendations"):
		return h.GetProfileRelayRecommendations
	default:
		return h.GetRelayRoles
	}
}

type optimizeScenario struct {
	name         string
	body         map[string]any
	personalized bool
	samples      int
}

func optimizeScenarios(people benchPeople) []optimizeScenario {
	roles := []string{"public_inbox", "public_outbox", "dm_inbox", "search"}
	return []optimizeScenario{
		{name: "optimize_balanced", samples: 30, personalized: true, body: map[string]any{"pubkey": people.Dense.pub, "preset": "balanced", "roles": roles}},
		{name: "optimize_creator", samples: 30, personalized: true, body: map[string]any{"pubkey": people.Creator.pub, "preset": "creator", "roles": []string{"public_inbox", "public_outbox"}}},
		{name: "optimize_privacy", samples: 30, personalized: true, body: map[string]any{"pubkey": people.Privacy.pub, "preset": "privacy", "roles": []string{"public_inbox", "public_outbox", "dm_inbox"}}},
		{name: "optimize_multi_role", samples: 30, personalized: true, body: map[string]any{"pubkey": people.Multi.pub, "preset": "balanced", "roles": roles}},
		{name: "optimize_signed_source", samples: 20, personalized: true, body: map[string]any{"pubkey": people.Dense.pub, "preset": "balanced", "roles": []string{"public_inbox", "public_outbox"}, "source_events": []json.RawMessage{people.SourceRaw}}},
		{name: "optimize_constraints", samples: 20, personalized: true, body: map[string]any{
			"pubkey": people.Dense.pub, "preset": "balanced", "roles": roles,
			"constraints": map[string]any{
				"max":             map[string]int{"public_inbox": 4, "public_outbox": 4, "dm_inbox": 2, "search": 2},
				"pinned":          []string{"wss://r1.op1.example"},
				"excluded":        []string{"wss://r19.op19.example"},
				"retain_existing": true,
				"by_role":         map[string]any{"public_inbox": map[string]any{"max": 3}, "search": map[string]any{"max": 1}},
			},
		}},
		{name: "optimize_shared_keypackage", samples: 20, personalized: true, body: map[string]any{"pubkey": people.Shared.pub, "preset": "balanced", "roles": []string{"public_inbox", "public_outbox", "keypackage"}}},
		{name: "optimize_sparse", samples: 20, personalized: true, body: map[string]any{"pubkey": people.Sparse.pub, "preset": "balanced", "roles": []string{"public_inbox", "public_outbox"}}},
		{name: "optimize_dense_cohort", samples: 20, personalized: true, body: map[string]any{"pubkey": people.Dense.pub, "preset": "balanced", "roles": []string{"public_inbox", "public_outbox"}}},
		{name: "optimize_max_lists", samples: 15, personalized: true, body: map[string]any{
			"pubkey": people.Dense.pub, "preset": "decentralization",
			"roles": []string{"public_inbox", "public_outbox", "dm_inbox", "search", "keypackage"},
			"constraints": map[string]any{
				"max": map[string]int{"public_inbox": 8, "public_outbox": 8, "dm_inbox": 4, "search": 4, "keypackage": 4},
			},
		}},
	}
}

func timeGET(t *testing.T, name, cache string, n int, call func() httpResult, meaningful func(map[string]any) bool, counter *queryCounter) latencySummary {
	t.Helper()
	return timeCalls(t, name, cache, n, func() (httpResult, bool) {
		got := call()
		if got.Status != http.StatusOK {
			return got, false
		}
		return got, meaningful(got.Body)
	}, counter)
}

func timePOST(t *testing.T, name string, n int, body map[string]any, personalized bool, counter *queryCounter, base Handlers, svc func() *relayintel.Service) latencySummary {
	t.Helper()
	h := base
	h.relayIntel = svc()
	return timeCalls(t, name, "optimize is not application-cached", n, func() (httpResult, bool) {
		got := callPOST(h.PostRelayOptimize, body)
		if got.Status != http.StatusOK {
			return got, false
		}
		return got, planMeaningful(got.Body, personalized)
	}, counter)
}

func timeCalls(t *testing.T, name, cache string, n int, call func() (httpResult, bool), counter *queryCounter) latencySummary {
	t.Helper()
	samples := make([]time.Duration, 0, n)
	queries := make([]int, 0, n)
	errors := 0
	meaningful := 0
	var proof map[string]any
	var note string
	startCPU := cpuNow()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	for i := 0; i < n; i++ {
		q0 := counter.snapshot()
		one := time.Now()
		got, ok := call()
		elapsed := time.Since(one)
		q1 := counter.snapshot()
		if got.Status != http.StatusOK || !ok {
			errors++
			if note == "" {
				raw := got.Raw
				if len(raw) > 400 {
					raw = raw[:400]
				}
				note = fmt.Sprintf("non-meaningful status=%d body=%s", got.Status, raw)
			}
			continue
		}
		meaningful++
		samples = append(samples, elapsed)
		queries = append(queries, int(q1-q0))
		if proof == nil {
			proof = planProof(got.Body)
			proof["status"] = got.Status
		}
	}
	elapsed := time.Since(started)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	alloc := uint64(0)
	if meaningful > 0 && after.TotalAlloc > before.TotalAlloc {
		alloc = (after.TotalAlloc - before.TotalAlloc) / uint64(meaningful)
	}
	summary := summarizeLatency(name, cache, pgCacheNote, samples, errors, meaningful, queries, elapsed, alloc, cpuNow()-startCPU, note, proof)
	t.Logf("%s n=%d meaningful=%d errors=%d median=%.3fms p95=%.3fms", name, summary.Samples, meaningful, errors, summary.MedianMS, summary.P95MS)
	return summary
}

func measureMaintenance(t *testing.T, ctx context.Context, pool *pgxpool.Pool, counter *queryCounter, report *opReport, people benchPeople, statsOnly bool, planPath string) {
	t.Helper()
	if statsOnly {
		t.Log("statistics refresh")
		before := walLSN(ctx, pool)
		started := time.Now()
		if err := relayintel.RefreshStats(ctx, pool, "", 0); err != nil {
			t.Fatal(err)
		}
		elapsed := time.Since(started)
		var duration int64
		_ = pool.QueryRow(ctx, `SELECT last_duration_ms FROM relay_intel_snapshot WHERE id`).Scan(&duration)
		skipStarted := time.Now()
		err := relayintel.RefreshStats(ctx, pool, "", 10*time.Minute)
		report.mu.Lock()
		report.Maintenance["statistics_refresh"] = map[string]any{
			"wall_ms": elapsed.Milliseconds(), "last_duration_ms": duration,
			"wal_bytes": walBytes(ctx, pool, before, walLSN(ctx, pool)),
			"queries":   counter.snapshot(),
		}
		report.Maintenance["statistics_refresh_skipped"] = map[string]any{
			"wall_ms": time.Since(skipStarted).Milliseconds(),
			"error":   errorText(err),
		}
		report.mu.Unlock()
		report.save()
		return
	}
	t.Log("maintenance measurements")
	out := map[string]any{}
	out["effective_membership_one"] = timeSQL(t, ctx, pool, 50, `
		SELECT count(*) FROM relay_effective_memberships WHERE pubkey = $1 AND public_safe`, people.Dense.pub)
	var cohort []string
	rows, err := pool.Query(ctx, `
		SELECT followed_pubkey FROM follower_edges WHERE follower_pubkey = $1 LIMIT 400
	`, people.Dense.pub)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			t.Fatal(err)
		}
		cohort = append(cohort, pubkey)
	}
	rows.Close()
	out["effective_membership_cohort"] = timeSQL(t, ctx, pool, 15, `
		SELECT count(*) FROM relay_effective_memberships
		WHERE public_safe AND role = 'public_inbox' AND pubkey = ANY($1::text[])`, cohort)
	out["effective_known"] = timeSQL(t, ctx, pool, 20, `
		SELECT count(*) FROM relay_effective_known WHERE role = 'public_inbox' AND pubkey = ANY($1::text[])`, cohort)
	out["canonical_replaceable"] = timeSQL(t, ctx, pool, 100, `
		SELECT event_id FROM replaceable_state WHERE pubkey = $1 AND kind = 10002 AND d_tag = ''`, people.Dense.pub)
	out["canonical_head"] = timeSQL(t, ctx, pool, 100, `
		SELECT event_id, truncated FROM relay_config_heads
		WHERE pubkey = $1 AND source_kind = 10002 AND d_tag = ''`, people.Dense.pub)
	out["candidate_shortlist"] = timeSQL(t, ctx, pool, 40, `
		SELECT url_key, endpoint_url, distinct_users, trust_weighted
		FROM relay_role_stats
		WHERE role = 'public_inbox'
		ORDER BY trust_weighted DESC, distinct_users DESC, url_key
		LIMIT 80`)
	out["global_role_count"] = timeSQL(t, ctx, pool, 10, `
		SELECT role, count(*) FROM relay_role_stats GROUP BY role`)
	out["membership_metadata"] = measureMembershipMetadata(t, ctx, pool)

	resetTieCursor(t, ctx, pool)
	var repaired int
	repairStart := time.Now()
	wal0 := walLSN(ctx, pool)
	if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(200)`).Scan(&repaired); err != nil {
		t.Fatal(err)
	}
	var lastRows int64
	_ = pool.QueryRow(ctx, `SELECT last_rows FROM relay_intel_work_cursor WHERE work_name = 'tie_repair'`).Scan(&lastRows)
	out["tie_repair_successful_page"] = map[string]any{
		"wall_ms": time.Since(repairStart).Milliseconds(), "repaired": repaired, "last_rows": lastRows,
		"wal_bytes": walBytes(ctx, pool, wal0, walLSN(ctx, pool)),
	}
	noopStart := time.Now()
	var repairedNoop int
	if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(200)`).Scan(&repairedNoop); err != nil {
		t.Fatal(err)
	}
	var noopRows int64
	_ = pool.QueryRow(ctx, `SELECT last_rows FROM relay_intel_work_cursor WHERE work_name = 'tie_repair'`).Scan(&noopRows)
	out["tie_repair_cursor_page"] = map[string]any{
		"wall_ms": time.Since(noopStart).Milliseconds(), "repaired": repairedNoop, "last_rows": noopRows,
		"note": "Separate from the successful page. repaired is the number of ties fixed on this page, not a blend with the previous call.",
	}
	if _, err := pool.Exec(ctx, `
		UPDATE relay_intel_work_cursor
		SET idle = true, next_run_at = now() + interval '1 hour', cursor_pubkey = '', cursor_kind = 0, cursor_d_tag = ''
		WHERE work_name = 'tie_repair'
	`); err != nil {
		t.Fatal(err)
	}
	idleStart := time.Now()
	var repairedIdle int
	if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie(200)`).Scan(&repairedIdle); err != nil {
		t.Fatal(err)
	}
	out["tie_repair_idle_check"] = map[string]any{
		"wall_ms": time.Since(idleStart).Milliseconds(), "repaired": repairedIdle,
		"note": "Tie scan is skipped while the cursor is idle. The function still inspects deletion tombstones.",
	}
	lifecycle := drainJobs(t, ctx, pool, "bench-repair", 300)
	out["repair_lifecycle_derivation"] = map[string]any{
		"processed": lifecycle.Processed, "errors": lifecycle.Errors, "wall_ms": lifecycle.Elapsed.Milliseconds(),
	}

	if _, err := pool.Exec(ctx, `
		UPDATE relay_config_heads SET needs_replay = true
		WHERE pubkey IN (
			SELECT pubkey FROM relay_config_heads
			WHERE source_kind = 10002 AND NOT explicitly_empty
			  AND pubkey <> ALL($1::text[])
			ORDER BY pubkey
			LIMIT 100
		)
	`, personaPubs(people)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_intel_work_cursor (work_name, idle, next_run_at, cursor_kind, cursor_pubkey, cursor_d_tag)
		VALUES ('projection_reconcile', false, now(), 0, '', '')
		ON CONFLICT (work_name) DO UPDATE
		SET idle = false, next_run_at = now(), cursor_kind = 0, cursor_pubkey = '', cursor_d_tag = ''
	`); err != nil {
		t.Fatal(err)
	}
	staleStart := time.Now()
	wal1 := walLSN(ctx, pool)
	if err := relayintel.BackfillBatch(ctx, pool, 200); err != nil {
		out["stale_projection_backfill"] = map[string]any{"error": err.Error()}
	} else {
		var replay int64
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_config_heads WHERE needs_replay`).Scan(&replay)
		out["stale_projection_backfill"] = map[string]any{
			"wall_ms": time.Since(staleStart).Milliseconds(), "needs_replay_remaining": replay,
			"wal_bytes": walBytes(ctx, pool, wal1, walLSN(ctx, pool)),
			"note":      "One BackfillBatch reconciles stale heads and projects one kind page. It is not a full-table rewrite.",
		}
	}
	if _, err := pool.Exec(ctx, `
		DELETE FROM relay_config_heads
		WHERE ctid IN (
			SELECT ctid FROM relay_config_heads
			WHERE source_kind = 10002 AND NOT explicitly_empty
			  AND pubkey <> ALL($1::text[])
			LIMIT 40
		)
	`, personaPubs(people)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE relay_intel_work_cursor
		SET idle = false, next_run_at = now(), cursor_kind = 0, cursor_pubkey = '', cursor_d_tag = ''
		WHERE work_name = 'projection_reconcile'
	`); err != nil {
		t.Fatal(err)
	}
	missingStart := time.Now()
	if err := relayintel.BackfillBatch(ctx, pool, 200); err != nil {
		out["missing_projection_reconcile"] = map[string]any{"error": err.Error()}
	} else {
		out["missing_projection_reconcile"] = map[string]any{"wall_ms": time.Since(missingStart).Milliseconds()}
	}

	delStart := time.Now()
	var inserted int
	if err := pool.QueryRow(ctx, `SELECT backfill_deletion_addresses(200)`).Scan(&inserted); err != nil {
		out["deletion_address_backfill"] = map[string]any{"error": err.Error()}
	} else {
		out["deletion_address_backfill"] = map[string]any{"wall_ms": time.Since(delStart).Milliseconds(), "inserted": inserted}
	}
	delNoop := time.Now()
	var insertedNoop int
	if err := pool.QueryRow(ctx, `SELECT backfill_deletion_addresses(200)`).Scan(&insertedNoop); err != nil {
		out["deletion_address_noop"] = map[string]any{"error": err.Error()}
	} else {
		out["deletion_address_noop"] = map[string]any{
			"wall_ms": time.Since(delNoop).Milliseconds(), "inserted": insertedNoop,
			"note": "Second call after the missing addresses were inserted. This is the no-op scan, not the successful insert.",
		}
	}
	retStart := time.Now()
	var due int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM relay_probe_followups
		WHERE finished_at IS NULL AND status = 'pending' AND check_after <= now()
	`).Scan(&due); err != nil {
		out["retention_candidates"] = map[string]any{"error": err.Error()}
	} else {
		out["retention_candidates"] = map[string]any{"wall_ms": time.Since(retStart).Milliseconds(), "due": due}
	}
	if _, err := pool.Exec(ctx, `
		UPDATE events SET first_seen_at = now() - interval '3 days'
		WHERE id IN (SELECT event_id FROM replaceable_event_purge)
	`); err != nil {
		t.Fatal(err)
	}
	if planPath != "" {
		out["retention_indexes_before_rebuild"] = listRetentionEventIndexes(ctx, pool)
		out["retention_indexes_removed_for_baseline"] = dropRetentionEventIndexes(ctx, pool)
		for key, value := range measureRetentionBreakdown(t, ctx, pool, planPath) {
			out[key] = value
		}
		out["populated_index_build"] = measurePopulatedIndexBuild(ctx, pool, planPath)
		indexedStart := time.Now()
		indexedPlan, indexedErr := explainRollback(ctx, pool, "EXPLAIN (ANALYZE, BUFFERS, WAL) "+purgeSupersededSQL, int32(100), retentionKinds(), time.Now().Add(-time.Hour), time.Now().Add(-time.Hour))
		if err := os.WriteFile(planPath, appendFile(planPath, "\n## replaceable retention purge after event-id indexes\nRolled back. Indexes were built on this populated disposable schema.\n"+indexedPlan), 0o644); err != nil {
			t.Fatal(err)
		}
		out["indexed_purge_plan_wall_ms"] = time.Since(indexedStart).Milliseconds()
		out["indexed_purge_plan_trigger_lines"] = triggerLines(indexedPlan)
		out["indexed_purge_buffers"] = bufferTotals(indexedPlan)
		out["indexed_purge_seq_scans"] = scanLines(indexedPlan, "Seq Scan")
		out["indexed_purge_index_scans"] = scanLines(indexedPlan, "Index Scan")
		if indexedErr != nil {
			out["indexed_purge_plan_error"] = indexedErr.Error()
		}
	}
	purgeStart := time.Now()
	wal2 := walLSN(ctx, pool)
	purgeDB := dbCounters(ctx, pool)
	purged, err := retention.New(pool).PurgeSupersededReplaceableEvents(ctx, time.Now().Add(-time.Hour), time.Now().Add(-time.Hour), 100)
	if err != nil {
		out["replaceable_retention"] = map[string]any{"error": err.Error()}
	} else {
		out["replaceable_retention"] = map[string]any{
			"wall_ms": time.Since(purgeStart).Milliseconds(), "deleted": purged,
			"wal_bytes": walBytes(ctx, pool, wal2, walLSN(ctx, pool)),
			"buffers":   dbDelta(purgeDB, dbCounters(ctx, pool)),
			"limit":     100,
			"indexes":   listRetentionEventIndexes(ctx, pool),
		}
	}
	report.mu.Lock()
	for key, value := range out {
		report.Maintenance[key] = value
	}
	report.mu.Unlock()
	report.save()
}

func personaPubs(people benchPeople) []string {
	return []string{
		people.Dense.pub, people.Sparse.pub, people.Creator.pub, people.Privacy.pub,
		people.Multi.pub, people.Empty.pub, people.Legacy.pub, people.Truncated.pub, people.Shared.pub,
	}
}

func resetTieCursor(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_intel_work_cursor (work_name, idle, next_run_at, cursor_pubkey, cursor_kind, cursor_d_tag)
		VALUES ('tie_repair', false, now(), '', 0, '')
		ON CONFLICT (work_name) DO UPDATE
		SET idle = false, next_run_at = now(), cursor_pubkey = '', cursor_kind = 0, cursor_d_tag = ''
	`); err != nil {
		t.Fatal(err)
	}
}

type sqlSummary struct {
	Samples  int     `json:"samples"`
	MedianMS float64 `json:"median_ms"`
	P95MS    float64 `json:"p95_ms"`
	MaxMS    float64 `json:"max_ms"`
}

func timeSQL(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int, sql string, args ...any) sqlSummary {
	t.Helper()
	samples := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		started := time.Now()
		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			t.Fatalf("sql: %v", err)
		}
		for rows.Next() {
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, time.Since(started))
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return sqlSummary{Samples: n, MedianMS: ms(samples[n/2]), P95MS: ms(samples[percentileIndex(n, 0.95)]), MaxMS: ms(samples[n-1])}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type drainResult struct {
	Processed int
	Errors    int
	Elapsed   time.Duration
}

func drainJobs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, worker string, limit int) drainResult {
	t.Helper()
	queue := jobs.NewQueue(pool)
	handlers := derivation.NewHandlers(pool)
	started := time.Now()
	processed := 0
	errors := 0
	for processed+errors < limit {
		claimed, err := queue.ClaimAvailable(ctx, worker, 20)
		if err != nil {
			t.Fatalf("claim jobs: %v", err)
		}
		if len(claimed) == 0 {
			break
		}
		for _, job := range claimed {
			err := derivation.ProcessJob(ctx, handlers, derivation.Job{JobType: job.JobType, Payload: job.Payload})
			if err != nil {
				errors++
				_, _ = queue.FailJob(ctx, job.ID, worker, err.Error(), time.Second)
				continue
			}
			if err := queue.CompleteJob(ctx, job.ID, worker); err != nil {
				errors++
				continue
			}
			processed++
		}
	}
	return drainResult{Processed: processed, Errors: errors, Elapsed: time.Since(started)}
}

func measurePlans(t *testing.T, ctx context.Context, pool *pgxpool.Pool, people benchPeople, path string, report *opReport) {
	t.Helper()
	var b strings.Builder
	writePlan := func(title, sql string, args ...any) {
		b.WriteString("\n## ")
		b.WriteString(title)
		b.WriteString("\n")
		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			fmt.Fprintf(&b, "error: %v\n", err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				fmt.Fprintf(&b, "scan: %v\n", err)
				return
			}
			b.WriteString(line)
			b.WriteByte('\n')
		}
	}
	writePlan("effective membership one account", `
		EXPLAIN (ANALYZE, BUFFERS, WAL)
		SELECT endpoint_url FROM relay_effective_memberships
		WHERE pubkey = $1 AND public_safe AND role = 'public_inbox'`, people.Dense.pub)
	writePlan("canonical head", `
		EXPLAIN (ANALYZE, BUFFERS, WAL)
		SELECT event_id, truncated, needs_replay FROM relay_config_heads
		WHERE pubkey = $1 AND source_kind = 10002 AND d_tag = ''`, people.Dense.pub)
	writePlan("candidate shortlist", `
		EXPLAIN (ANALYZE, BUFFERS, WAL)
		SELECT url_key, endpoint_url, distinct_users, trust_weighted
		FROM relay_role_stats
		WHERE role = 'public_inbox'
		ORDER BY trust_weighted DESC, distinct_users DESC, url_key
		LIMIT 80`)
	writePlan("cohort follows", `
		EXPLAIN (ANALYZE, BUFFERS, WAL)
		SELECT followed_pubkey FROM follower_edges
		WHERE follower_pubkey = $1
		ORDER BY followed_pubkey
		LIMIT 401`, people.Dense.pub)
	writePlan("reconcile candidates", `
		EXPLAIN (ANALYZE, BUFFERS, WAL)
		SELECT rs.pubkey
		FROM replaceable_state rs
		LEFT JOIN relay_config_heads h
		  ON h.pubkey = rs.pubkey AND h.source_kind = rs.kind AND h.d_tag = rs.d_tag
		WHERE rs.kind = ANY(ARRAY[3, 10002, 10006, 10007, 10012, 10013, 10050, 10051, 10063, 10102, 30002]::int[])
		  AND (h.event_id IS NULL OR h.event_id <> rs.event_id OR h.needs_replay)
		ORDER BY rs.kind, rs.pubkey, rs.d_tag
		LIMIT 200`)
	writePlan("synthetic identity lookup", `
		EXPLAIN (ANALYZE, BUFFERS, WAL)
		SELECT 1 FROM relay_probe_identities WHERE pubkey = $1`, people.Dense.pub)
	writePlan("synthetic identity epoch", `
		EXPLAIN (ANALYZE, BUFFERS, WAL)
		SELECT epoch FROM relay_probe_identity_epoch WHERE id`)
	writePlan("due retention followups", `
		EXPLAIN (ANALYZE, BUFFERS, WAL)
		SELECT id FROM relay_probe_followups
		WHERE finished_at IS NULL AND status IN ('pending', 'retryable') AND check_after <= now()
		ORDER BY check_after, id
		LIMIT 20`)
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	report.mu.Lock()
	report.Plans = append(report.Plans, path)
	report.mu.Unlock()
}

func measureProbes(t *testing.T, ctx context.Context, pool *pgxpool.Pool, report *opReport) {
	t.Helper()
	t.Log("probe scheduling")
	out := map[string]any{}
	out["budget_window_note"] = "Each scenario starts by deleting relay_probe_attempts from the last 2 hours and all relay_probe_leases. Seeded history is 3 hours old and stays in place, outside the 1 hour budget window. The delete simulates a new hour so a successful-claim sample cannot exhaust the global cap before a later scenario."
	resetProbeWindow(t, ctx, pool)
	success := make([]time.Duration, 0, 120)
	wal0 := walLSN(ctx, pool)
	claimed, stopped := claimMany(t, ctx, pool, 60, "claim", successFilter(&success))
	out["successful_claims_window_1_wal_bytes"] = walBytes(ctx, pool, wal0, walLSN(ctx, pool))
	if stopped >= 0 {
		out["successful_claims_window_1_stopped"] = stopped
	}
	globalBefore := attemptCounts(ctx, pool, "wss://global-extra.bench.example", "global-extra.bench.example")
	globalStarted := time.Now()
	globalToken := claim(t, ctx, pool, "wss://global-extra.bench.example", "write_accept", "global-extra.bench.example", "bench", 60, 1)
	out["global_budget_rejection"] = map[string]any{
		"before": globalBefore, "wall_ms": ms(time.Since(globalStarted)),
		"token_empty": globalToken == "", "expected": "recent attempts >= global limit of 60",
		"window_1_claims": claimed,
	}

	resetProbeWindow(t, ctx, pool)
	wal1 := walLSN(ctx, pool)
	claimed2, stopped2 := claimMany(t, ctx, pool, 60, "claimb", successFilter(&success))
	out["successful_claims_window_2_wal_bytes"] = walBytes(ctx, pool, wal1, walLSN(ctx, pool))
	if stopped2 >= 0 {
		out["successful_claims_window_2_stopped"] = stopped2
	}
	rejected := 120 - len(success)
	out["successful_claims"] = summarizeLatency("successful lease claim", "none", pgCacheNote, success, rejected, len(success), nil, sumDurations(success), 0, 0, "two budget hours, 60 distinct hosts each; an empty token stops that hour and is excluded from the latency sample", map[string]any{"claimed": claimed + claimed2})

	resetProbeWindow(t, ctx, pool)
	activeKey := "wss://active.bench.example"
	activeHost := "active.bench.example"
	active := claim(t, ctx, pool, activeKey, "write_accept", activeHost, "bench", 60, 1)
	if active == "" {
		out["active_lease_rejection"] = map[string]any{"setup_rejected": true, "before": attemptCounts(ctx, pool, activeKey, activeHost)}
	} else {
		before := attemptCounts(ctx, pool, activeKey, activeHost)
		started := time.Now()
		again := claim(t, ctx, pool, activeKey, "write_accept", activeHost, "other", 60, 1)
		out["active_lease_rejection"] = map[string]any{
			"wall_ms": ms(time.Since(started)), "token_empty": again == "", "expected": "active lease", "before": before,
		}
	}
	cooldownKey := "wss://cooldown.bench.example"
	cooldownHost := "cooldown.bench.example"
	first := claim(t, ctx, pool, cooldownKey, "search", cooldownHost, "bench", 60, 3600)
	if first == "" {
		out["cooldown_rejection"] = map[string]any{"setup_rejected": true, "before": attemptCounts(ctx, pool, cooldownKey, cooldownHost)}
	} else {
		if _, err := pool.Exec(ctx, `DELETE FROM relay_probe_leases WHERE lease_token = $1`, first); err != nil {
			t.Fatal(err)
		}
		before := attemptCounts(ctx, pool, cooldownKey, cooldownHost)
		started := time.Now()
		again := claim(t, ctx, pool, cooldownKey, "search", cooldownHost, "bench", 60, 3600)
		out["cooldown_rejection"] = map[string]any{
			"wall_ms": ms(time.Since(started)), "token_empty": again == "", "expected": "recent attempt inside min_seconds", "before": before,
		}
	}
	host := "shared-budget.bench.example"
	hostOK := 0
	for i := 0; i < 4; i++ {
		if claim(t, ctx, pool, fmt.Sprintf("wss://h%d.shared-budget.bench.example", i), "write_accept", host, "bench", 60, 1) != "" {
			hostOK++
		}
	}
	fifthKey := "wss://h4.shared-budget.bench.example"
	beforeHost := attemptCounts(ctx, pool, fifthKey, host)
	started := time.Now()
	fifth := claim(t, ctx, pool, fifthKey, "write_accept", host, "bench", 60, 1)
	out["per_host_budget_rejection"] = map[string]any{
		"successful_before_rejection": hostOK, "wall_ms": ms(time.Since(started)),
		"token_empty": fifth == "", "expected": "host attempt count in the last hour reached 4", "before": beforeHost,
	}

	resetProbeWindow(t, ctx, pool)
	expireKey := "wss://expire.bench.example"
	expireHost := "expire.bench.example"
	expired := claim(t, ctx, pool, expireKey, "write_accept", expireHost, "bench", 60, 1)
	if expired == "" {
		out["expired_lease_reclaim"] = map[string]any{"setup_rejected": true}
	} else {
		if _, err := pool.Exec(ctx, `UPDATE relay_probe_leases SET expires_at = now() - interval '1 minute' WHERE lease_token = $1`, expired); err != nil {
			t.Fatal(err)
		}
		blockedStart := time.Now()
		blocked := claim(t, ctx, pool, expireKey, "write_accept", expireHost, "bench", 60, 1)
		if _, err := pool.Exec(ctx, `
			UPDATE relay_probe_attempts
			SET started_at = now() - interval '30 seconds'
			WHERE lease_token = $1
		`, expired); err != nil {
			t.Fatal(err)
		}
		before := attemptCounts(ctx, pool, expireKey, expireHost)
		reclaimStart := time.Now()
		reclaimed := claim(t, ctx, pool, expireKey, "write_accept", expireHost, "bench", 60, 1)
		out["expired_lease_reclaim"] = map[string]any{
			"wall_ms": ms(time.Since(reclaimStart)), "obtained": reclaimed != "" && reclaimed != expired,
			"before":                         before,
			"still_inside_cooldown_rejected": blocked == "",
			"cooldown_wall_ms":               ms(time.Since(blockedStart)),
			"note":                           "claim_relay_probe checks the recent-attempt cooldown before it replaces an expired lease. The successful reclaim ages that attempt by 30 seconds, which is outside the 1 second minimum used here, and sets the lease expiry in the past.",
		}
	}

	var wg sync.WaitGroup
	var wins atomicInt
	contended := make([]time.Duration, 8)
	claimErr := make([]string, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			started := time.Now()
			var token string
			err := pool.QueryRow(ctx, `SELECT claim_relay_probe($1, $2, $3, $4, $5, $6, 120)`, "wss://contend.bench.example", "write_accept", "contend.bench.example", fmt.Sprintf("w%d", n), 60, 1).Scan(&token)
			contended[n] = time.Since(started)
			if err != nil {
				claimErr[n] = err.Error()
				return
			}
			if token != "" {
				wins.add()
			}
		}(i)
	}
	wg.Wait()
	out["concurrent_same_url"] = map[string]any{"successes": wins.value(), "workers": 8, "latencies_ms": durationsMS(contended[:]), "errors": nonempty(claimErr[:])}

	var distinctWins atomicInt
	distinctLat := make([]time.Duration, 8)
	distinctErr := make([]string, 8)
	distinctStart := time.Now()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			started := time.Now()
			var token string
			err := pool.QueryRow(ctx, `SELECT claim_relay_probe($1, $2, $3, $4, $5, $6, 120)`, fmt.Sprintf("wss://par-%d.bench.example", n), "write_accept", fmt.Sprintf("par-%d.bench.example", n), "bench", 60, 1).Scan(&token)
			distinctLat[n] = time.Since(started)
			if err != nil {
				distinctErr[n] = err.Error()
				return
			}
			if token != "" {
				distinctWins.add()
			}
		}(i)
	}
	wg.Wait()
	out["concurrent_distinct_urls"] = map[string]any{"successes": distinctWins.value(), "workers": 8, "wall_ms": time.Since(distinctStart).Milliseconds(), "latencies_ms": durationsMS(distinctLat), "errors": nonempty(distinctErr)}

	measureAtomicCompletion(t, ctx, pool, out)
	var attempts, leases, samplesAll int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_attempts`).Scan(&attempts)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_leases`).Scan(&leases)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_capability_samples`).Scan(&samplesAll)
	out["rows_after"] = map[string]any{"attempts": attempts, "leases": leases, "samples": samplesAll}
	report.mu.Lock()
	report.Probes = out
	report.mu.Unlock()
	report.save()
}

type atomicInt struct {
	mu sync.Mutex
	n  int
}

func (a *atomicInt) add() {
	a.mu.Lock()
	a.n++
	a.mu.Unlock()
}

func (a *atomicInt) value() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.n
}

func (a *atomicInt) max(n int) {
	a.mu.Lock()
	if n > a.n {
		a.n = n
	}
	a.mu.Unlock()
}

func resetProbeWindow(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `DELETE FROM relay_probe_attempts WHERE started_at > now() - interval '2 hours'`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM relay_probe_leases`); err != nil {
		t.Fatal(err)
	}
}

func successFilter(dst *[]time.Duration) func(time.Duration) {
	return func(elapsed time.Duration) {
		*dst = append(*dst, elapsed)
	}
}

func claimMany(t *testing.T, ctx context.Context, pool *pgxpool.Pool, n int, prefix string, record func(time.Duration)) (claimed int, stoppedAt int) {
	t.Helper()
	stoppedAt = -1
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("wss://%s-%d.bench.example", prefix, i)
		host := fmt.Sprintf("%s-%d.bench.example", prefix, i)
		started := time.Now()
		token := claim(t, ctx, pool, key, "write_accept", host, "bench", 60, 1)
		if token == "" {
			stoppedAt = i
			return claimed, stoppedAt
		}
		claimed++
		record(time.Since(started))
	}
	return claimed, stoppedAt
}

func attemptCounts(ctx context.Context, pool *pgxpool.Pool, urlKey, host string) map[string]any {
	var global, hostN, keyN, leaseN int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_attempts WHERE started_at > now() - interval '1 hour'`).Scan(&global)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_attempts WHERE host = $1 AND started_at > now() - interval '1 hour'`, host).Scan(&hostN)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_attempts WHERE url_key = $1 AND started_at > now() - interval '1 hour'`, urlKey).Scan(&keyN)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_leases WHERE url_key = $1 AND expires_at > now()`, urlKey).Scan(&leaseN)
	return map[string]any{
		"attempts_last_hour": global, "host_attempts_last_hour": hostN,
		"key_attempts_last_hour": keyN, "active_leases_for_key": leaseN,
	}
}

func claim(t *testing.T, ctx context.Context, pool *pgxpool.Pool, urlKey, capability, host, worker string, global, minSeconds int) string {
	t.Helper()
	var token string
	if err := pool.QueryRow(ctx, `SELECT claim_relay_probe($1, $2, $3, $4, $5, $6, 120)`, urlKey, capability, host, worker, global, minSeconds).Scan(&token); err != nil {
		t.Fatalf("claim: %v", err)
	}
	return token
}

func sumDurations(samples []time.Duration) time.Duration {
	var total time.Duration
	for _, sample := range samples {
		total += sample
	}
	return total
}

func nonempty(items []string) []string {
	out := make([]string, 0)
	for _, item := range items {
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func durationsMS(samples []time.Duration) []float64 {
	out := make([]float64, len(samples))
	for i, sample := range samples {
		out[i] = ms(sample)
	}
	return out
}

func measureSynthetic(t *testing.T, ctx context.Context, pool *pgxpool.Pool, counter *queryCounter, report *opReport) {
	t.Helper()
	t.Log("synthetic identity lookups")
	relayintel.ClearSyntheticCaches()
	out := map[string]any{}
	ordinary := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		ordinary = append(ordinary, fmt.Sprintf("%064x", 500000000+i))
	}
	out["distinct_ordinary_cold"] = timeLookups(ctx, pool, counter, ordinary)
	out["repeated_ordinary"] = timeLookups(ctx, pool, counter, ordinary)
	relayintel.ClearSyntheticCaches()
	pubkey := fmt.Sprintf("%064x", 424242)
	if _, err := pool.Exec(ctx, `INSERT INTO relay_probe_identities (pubkey) VALUES ($1) ON CONFLICT DO NOTHING`, pubkey); err != nil {
		t.Fatal(err)
	}
	out["positive_cold"] = timeLookups(ctx, pool, counter, []string{pubkey})
	out["positive_cache_hit"] = timeLookups(ctx, pool, counter, []string{pubkey})
	relayintel.ClearSyntheticCaches()
	absent := fmt.Sprintf("%064x", 434343)
	out["negative_cold"] = timeLookups(ctx, pool, counter, []string{absent})
	repeatedMiss := make([]string, 1000)
	for i := range repeatedMiss {
		repeatedMiss[i] = absent
	}
	out["negative_cache_hit_1000"] = timeLookups(ctx, pool, counter, repeatedMiss)
	var epochBefore, epochAfter int64
	_ = pool.QueryRow(ctx, `SELECT epoch FROM relay_probe_identity_epoch WHERE id`).Scan(&epochBefore)
	registered := fmt.Sprintf("%064x", 454545)
	if err := relayintel.RegisterSyntheticIdentity(ctx, pool, registered); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SELECT epoch FROM relay_probe_identity_epoch WHERE id`).Scan(&epochAfter)
	out["epoch_before_registry_change"] = epochBefore
	out["epoch_after_registry_change"] = epochAfter
	out["cached_miss_after_registry_change"] = timeLookups(ctx, pool, counter, []string{absent})
	relayintel.ClearSyntheticCaches()
	out["registered_identity_other_worker"] = timeLookups(ctx, pool, counter, []string{registered})
	out["registered_identity_positive_cache"] = timeLookups(ctx, pool, counter, []string{registered})
	time.Sleep(16 * time.Second)
	out["negative_after_ttl"] = timeLookups(ctx, pool, counter, []string{absent})
	fresh := fmt.Sprintf("%064x", 444444)
	relayintel.ClearSyntheticCaches()
	if err := relayintel.RegisterSyntheticIdentity(ctx, pool, fresh); err != nil {
		t.Fatal(err)
	}
	relayintel.ClearSyntheticCaches()
	out["newly_registered_other_worker"] = timeLookups(ctx, pool, counter, []string{fresh})
	started := time.Now()
	if err := func() error {
		relayintel.LoadSyntheticIdentities(ctx, pool)
		return nil
	}(); err != nil {
		out["registry_load_error"] = err.Error()
	}
	out["registry_load_ms"] = time.Since(started).Milliseconds()
	var identities int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_identities`).Scan(&identities)
	out["registry_rows_before_growth"] = identities
	growthStart := time.Now()
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_identities (pubkey)
		SELECT lpad(to_hex(700000000 + i), 64, '0')
		FROM generate_series(1, 2000) AS i
		ON CONFLICT DO NOTHING
	`); err != nil {
		t.Fatal(err)
	}
	out["registry_insert_2000_ms"] = time.Since(growthStart).Milliseconds()
	relayintel.ClearSyntheticCaches()
	out["lookup_after_registry_growth"] = timeLookups(ctx, pool, counter, []string{fmt.Sprintf("%064x", 700000001), fmt.Sprintf("%064x", 800000001)})

	var wg sync.WaitGroup
	concStarted := time.Now()
	queriesBefore := counter.snapshot()
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				relayintel.SyntheticAuthor(ctx, pool, fmt.Sprintf("%064x", 900000000+n*100+j))
			}
		}(i)
	}
	wg.Wait()
	out["concurrent_800_lookups_ms"] = time.Since(concStarted).Milliseconds()
	out["concurrent_800_queries"] = counter.snapshot() - queriesBefore

	deriveOrdinary := deriveAuthors(t, ctx, pool, counter, 25, false)
	deriveSynthetic := deriveAuthors(t, ctx, pool, counter, 25, true)
	out["derivation_ordinary_authors"] = deriveOrdinary
	out["derivation_synthetic_authors"] = deriveSynthetic
	if client := optionalMeili(t); client != nil {
		handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{MeiliClient: client})
		id := deriveAuthors(t, ctx, pool, counter, 1, false)["last_event_id"]
		if eventID, ok := id.(string); ok && eventID != "" {
			started := time.Now()
			err := handlers.SyncMeilisearch(ctx, eventID)
			out["search_sync_one_ordinary"] = map[string]any{"wall_ms": time.Since(started).Milliseconds(), "error": errorText(err)}
		}
	} else {
		out["search_sync"] = "Meilisearch was not reachable at 127.0.0.1:7700, so HTTP indexing was not measured. DeriveEventBundle still marks the event only when a client is configured; the synthetic gate inside SyncMeilisearch was not on that path."
	}
	report.mu.Lock()
	report.Synthetic = out
	report.mu.Unlock()
	report.save()
}

func timeLookups(ctx context.Context, pool *pgxpool.Pool, counter *queryCounter, pubkeys []string) map[string]any {
	before := counter.snapshot()
	started := time.Now()
	hits := 0
	for _, pubkey := range pubkeys {
		if relayintel.SyntheticAuthor(ctx, pool, pubkey) {
			hits++
		}
	}
	queries := counter.snapshot() - before
	return map[string]any{
		"keys": len(pubkeys), "positive": hits, "queries": queries,
		"queries_per_key": float64(queries) / float64(max(len(pubkeys), 1)),
		"wall_ms":         time.Since(started).Milliseconds(),
		"per_lookup_us":   time.Since(started).Microseconds() / int64(max(len(pubkeys), 1)),
	}
}

func deriveAuthors(t *testing.T, ctx context.Context, pool *pgxpool.Pool, counter *queryCounter, n int, synthetic bool) map[string]any {
	t.Helper()
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	started := time.Now()
	queriesBefore := counter.snapshot()
	errors := 0
	var last string
	for i := 0; i < n; i++ {
		key := newKey(t)
		if synthetic {
			if err := relayintel.RegisterSyntheticIdentity(ctx, pool, key.pub); err != nil {
				t.Fatal(err)
			}
			relayintel.ClearSyntheticCaches()
		}
		raw := sign(t, key.priv, 1, time.Now().Unix(), nil, "bench note")
		var body struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(raw, &body)
		event := model.Event{
			ID: body.ID, Pubkey: key.pub, CreatedAt: time.Now().Unix(), Kind: 1,
			Sig: strings.Repeat("c", 128), Content: "bench note", RawJSON: raw,
		}
		if err := pgStore.InsertCanonicalEvent(ctx, event, nil, "wss://index.example", time.Now().UTC()); err != nil {
			errors++
			continue
		}
		if err := handlers.DeriveEventBundle(ctx, body.ID); err != nil {
			errors++
			continue
		}
		last = body.ID
	}
	out := map[string]any{
		"events": n, "errors": errors, "wall_ms": time.Since(started).Milliseconds(), "last_event_id": last,
		"synthetic": synthetic, "queries": counter.snapshot() - queriesBefore,
	}
	if synthetic {
		out["note"] = "Each author is registered through the production epoch path, then the process cache is cleared so derivation performs the other-worker lookup."
	}
	return out
}

func optionalMeili(t *testing.T) *meili.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:7700/health", nil)
	if err != nil {
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	resp.Body.Close()
	if resp.StatusCode >= 500 {
		return nil
	}
	client, err := meili.NewClient(meili.Config{Enabled: true, URL: "http://127.0.0.1:7700", MasterKey: "nostrmash-dev-key"})
	if err != nil || client == nil || !client.Enabled() {
		return nil
	}
	return client
}

type dbSnap struct {
	Xact, BlksRead, BlksHit, TupIns, TupUpd, Deadlocks, TempFiles, TempBytes int64
}

func dbCounters(ctx context.Context, pool *pgxpool.Pool) dbSnap {
	var snap dbSnap
	_ = pool.QueryRow(ctx, `
		SELECT xact_commit, blks_read, blks_hit, tup_inserted, tup_updated, deadlocks, temp_files, temp_bytes
		FROM pg_stat_database WHERE datname = current_database()
	`).Scan(&snap.Xact, &snap.BlksRead, &snap.BlksHit, &snap.TupIns, &snap.TupUpd, &snap.Deadlocks, &snap.TempFiles, &snap.TempBytes)
	return snap
}

func dbDelta(before, after dbSnap) map[string]int64 {
	return map[string]int64{
		"xact_commit":  after.Xact - before.Xact,
		"blks_read":    after.BlksRead - before.BlksRead,
		"blks_hit":     after.BlksHit - before.BlksHit,
		"tup_inserted": after.TupIns - before.TupIns,
		"tup_updated":  after.TupUpd - before.TupUpd,
		"deadlocks":    after.Deadlocks - before.Deadlocks,
		"temp_files":   after.TempFiles - before.TempFiles,
		"temp_bytes":   after.TempBytes - before.TempBytes,
	}
}
