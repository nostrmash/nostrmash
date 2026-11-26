package relayintel

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCohortCapStopsOneAccountFromDominating(t *testing.T) {
	follows := make([]string, 0, 20)
	followers := make([]string, 0, 1)
	for i := 0; i < 20; i++ {
		follows = append(follows, pubkeyN(i))
	}
	followers = append(followers, pubkeyN(0))
	signals := CohortSignals{
		Viewer:         strings.Repeat("aa", 32),
		Follows:        follows,
		Followers:      followers,
		Interaction:    map[string]float64{pubkeyN(0): 100},
		Trust:          map[string]float64{pubkeyN(0): 1},
		TrustAvailable: true,
	}
	cohort := BuildCohort(signals, 500)
	if len(cohort.Members) != 20 {
		t.Fatalf("size = %d", len(cohort.Members))
	}
	for _, member := range cohort.Members {
		if member.Weight > cohortCapWeight+0.001 {
			t.Fatalf("member %s weight %f exceeds the cap", member.Pubkey, member.Weight)
		}
	}
}

func TestScoreWithholdsThinEvidenceAndHardFailures(t *testing.T) {
	thin := ScoreRelay(ScoreInput{Role: RolePublicInbox})
	if thin.Score != nil || thin.ConfidenceLabel != "insufficient" {
		t.Fatalf("thin score = %#v label=%s", thin.Score, thin.ConfidenceLabel)
	}
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	unsupported := ScoreRelay(ScoreInput{
		Role: RoleSearch,
		Evidence: Evidence{Now: now, Observations: []Observation{{
			Capability: CapSearch, Status: StatusUnsupported, Confidence: 0.9,
			Source: "active_probe", ObservedAt: now, SampleCount: 2,
		}}},
	})
	if unsupported.Eligible || unsupported.IneligibleReason != "search_unsupported" {
		t.Fatalf("search = eligible:%v reason:%s", unsupported.Eligible, unsupported.IneligibleReason)
	}
	pinnedInput := ScoreInput{
		Role:   RoleSearch,
		Pinned: true,
		Evidence: Evidence{Now: now, Observations: []Observation{{
			Capability: CapSearch, Status: StatusUnsupported, Confidence: 0.9,
			Source: "active_probe", ObservedAt: now, SampleCount: 2,
		}}},
	}
	pinned := ScoreRelay(pinnedInput)
	if !pinned.Eligible || pinned.Score == nil {
		t.Fatalf("pinned ineligible relay was dropped: %+v", pinned)
	}
	authOff := false
	rejected := ScoreRelay(ScoreInput{
		Role:     RolePublicInbox,
		AuthRead: "require",
		Evidence: Evidence{Now: now, NIP11: &NIP11Facts{OK: true, FetchedAt: now, AuthRequired: &authOff}},
	})
	if rejected.Eligible || rejected.IneligibleReason != "auth_rejected" {
		t.Fatalf("auth require = %+v", rejected.IneligibleReason)
	}
}

func TestCapabilitiesDoNotTreatEOSEOrAdvertisementsAsProof(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	ok := true
	caps := ResolveCapabilities(Evidence{
		Now: now,
		Probe: ProbeSnapshot{
			At: &now, Status: "eose", ConnectOK: &ok, SubscribeOK: &ok, EOSEOK: &ok,
		},
		NIP11: &NIP11Facts{OK: true, FetchedAt: now, SupportedNIPs: []int{17, 42, 50}},
	})
	if caps[CapRead].Status != StatusObserved {
		t.Fatalf("read = %s", caps[CapRead].Status)
	}
	if caps[CapSearch].Status != StatusAdvertised || caps[CapGiftWrap].Status != StatusAdvertised {
		t.Fatalf("search=%s giftwrap=%s", caps[CapSearch].Status, caps[CapGiftWrap].Status)
	}
	if caps[CapWriteAccept].Status == StatusVerified || caps[CapWriteRetrieve].Status == StatusVerified {
		t.Fatal("a read probe verified a write")
	}
}

func TestSelectComplementarySetBeatsTopIndividuals(t *testing.T) {
	members := []CohortMember{
		{Pubkey: "a", Weight: 0.25, HasDeclaration: true},
		{Pubkey: "b", Weight: 0.25, HasDeclaration: true},
		{Pubkey: "c", Weight: 0.25, HasDeclaration: true},
		{Pubkey: "d", Weight: 0.25, HasDeclaration: true},
		{Pubkey: "unknown", Weight: 0.5, HasDeclaration: false},
	}
	x := scored("wss://x.example", 0.90, "host:x", map[string]float64{"a": 1, "b": 1})
	y := scored("wss://y.example", 0.85, "host:y", map[string]float64{"a": 1, "b": 1})
	z := scored("wss://z.example", 0.40, "host:z", map[string]float64{"c": 1})
	w := scored("wss://w.example", 0.39, "host:w", map[string]float64{"d": 1})
	individual := coverageOf([]ScoredRelay{x, y}, map[string]float64{"a": 0.25, "b": 0.25, "c": 0.25, "d": 0.25}, 1)
	result := Select(SelectRequest{
		Role: RolePublicInbox, Objective: ObjectiveCoverage,
		Min: 2, Max: 2, DiversityFactor: 1, Hysteresis: 0, QualityBias: 0.01,
		Members: members, Relays: []ScoredRelay{x, y, z, w},
	})
	if result.ProposedCoverage <= individual {
		t.Fatalf("complementary coverage %.3f did not beat top-two coverage %.3f; selected %#v", result.ProposedCoverage, individual, result.Selected)
	}
	if result.UnknownWeight <= 0 {
		t.Fatal("accounts without a stored declaration were treated as known")
	}
	if containsURL(result, "wss://x.example") && containsURL(result, "wss://y.example") {
		t.Fatalf("selected the overlapping top two: %#v", result.Selected)
	}
}

func TestSelectKeepsPinsAndReportsConflicts(t *testing.T) {
	pin := ScoredRelay{URL: "wss://pin.example", Eligible: false, Pinned: true, OperatorKey: "host:pin"}
	other := scored("wss://other.example", 0.8, "host:other", map[string]float64{"a": 1})
	result := Select(SelectRequest{
		Role: RolePublicInbox, Objective: ObjectiveCoverage,
		Min: 1, Max: 1, Relays: []ScoredRelay{pin, other},
	})
	if !containsURL(result, pin.URL) || result.ConstraintsSatisfied {
		t.Fatalf("pin result = %+v", result)
	}
	if !reasonCode(result.Unsatisfied, "pinned_ineligible") {
		t.Fatalf("unsatisfied = %#v", result.Unsatisfied)
	}
	preferred := ScoredRelay{URL: "wss://nope.example", Eligible: false, Preferred: true}
	miss := Select(SelectRequest{
		Role: RoleSearch, Objective: ObjectiveQuality, Min: 1, Max: 1,
		Relays: []ScoredRelay{other, preferred},
	})
	if containsURL(miss, preferred.URL) || !reasonCode(miss.Unsatisfied, "preferred_ineligible") {
		t.Fatalf("preferred = %+v", miss)
	}
	short := Select(SelectRequest{
		Role: RoleDMInbox, Objective: ObjectiveQuality, Min: 2, Max: 2,
		Relays: []ScoredRelay{other},
	})
	if short.ConstraintsSatisfied || !reasonCode(short.Unsatisfied, "minimum_count_unmet") {
		t.Fatalf("minimum = %+v", short)
	}
}

func TestSelectReplacesOneRelayInsideAFullList(t *testing.T) {
	members := []CohortMember{
		{Pubkey: "a", Weight: 0.25, HasDeclaration: true},
		{Pubkey: "b", Weight: 0.25, HasDeclaration: true},
		{Pubkey: "c", Weight: 0.25, HasDeclaration: true},
		{Pubkey: "d", Weight: 0.25, HasDeclaration: true},
	}
	a := scored("wss://a.example", 0.8, "host:a", map[string]float64{"a": 1})
	b := scored("wss://b.example", 0.8, "host:b", map[string]float64{"b": 1})
	c := scored("wss://c.example", 0.8, "host:c", map[string]float64{"c": 1})
	d := scored("wss://d.example", 0.2, "host:d", map[string]float64{"a": 1})
	e := scored("wss://e.example", 0.7, "host:e", map[string]float64{"d": 1})
	for _, relay := range []*ScoredRelay{&a, &b, &c, &d} {
		relay.Current = true
	}
	result := Select(SelectRequest{
		Role: RolePublicInbox, Objective: ObjectiveCoverage,
		Min: 4, Max: 4, RetainExisting: true, Hysteresis: 0.05, QualityBias: 0.01,
		Members: members, Relays: []ScoredRelay{a, b, c, d, e},
	})
	if !containsURL(result, "wss://e.example") || containsURL(result, "wss://d.example") {
		t.Fatalf("full list was not improved: %#v", result.Selected)
	}
	if !containsURL(result, "wss://a.example") || !containsURL(result, "wss://b.example") || !containsURL(result, "wss://c.example") {
		t.Fatalf("swap removed the wrong relays: %#v", result.Selected)
	}
}

func TestSelectAvoidsChurnForTinyOverlap(t *testing.T) {
	current := scored("wss://current.example", 0.5, "host:current", map[string]float64{"a": 1})
	current.Current = true
	overlap := scored("wss://overlap.example", 0.95, "host:overlap", map[string]float64{"a": 1})
	result := Select(SelectRequest{
		Role: RolePublicOutbox, Objective: ObjectiveCoverage,
		Min: 1, Max: 2, RetainExisting: true, Hysteresis: 0.2, QualityBias: 0.01,
		Members: []CohortMember{{Pubkey: "a", Weight: 1, HasDeclaration: true}},
		Relays:  []ScoredRelay{current, overlap},
	})
	if len(result.Selected) != 1 || result.Selected[0].URL != current.URL {
		t.Fatalf("churned to %#v", result.Selected)
	}
}

func TestDiagnoseDoesNotRemoveOnOneFailure(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	recent := now.Add(-time.Hour)
	fail := 1.0
	low := 0.2
	down := false
	cfg := AccountConfig{
		ModernPublic: AvailabilityPresent,
		Roles: []RoleConfig{{
			Role: RolePublicInbox, Availability: AvailabilityPresent,
			Relays: []DeclaredRelay{{URL: "wss://down.example", PublicSafe: true}},
		}},
	}
	once := Diagnose(cfg, map[string]RelayHealth{
		"wss://down.example": {FailRate: &low, LastProbeAt: &recent, LastStatus: "connect_failed", LastEOSEOK: &down},
	}, now)
	if findingCode(once, "repeated_connection_failures") || !findingCode(once, "unreachable") {
		t.Fatalf("one failure = %#v", once)
	}
	repeated := Diagnose(cfg, map[string]RelayHealth{
		"wss://down.example": {FailRate: &fail, LastProbeAt: &recent, LastStatus: "connect_failed", LastEOSEOK: &down},
	}, now)
	if !findingCode(repeated, "repeated_connection_failures") {
		t.Fatalf("repeated = %#v", repeated)
	}
	for _, finding := range repeated {
		if finding.Code == "repeated_connection_failures" && finding.Severity != SeverityWarning {
			t.Fatalf("severity = %s", finding.Severity)
		}
	}
}

func TestRecommendationsMergeNIP65AndKeepSensitiveActions(t *testing.T) {
	event := NIP65Event("ab", [][]string{{"client", "nostrmash"}, {"r", "wss://old.example"}}, "source", 10,
		[]string{"wss://shared.example", "wss://in.example"},
		[]string{"wss://shared.example", "wss://out.example"})
	if event.Kind != KindPublicRelayList {
		t.Fatalf("kind = %d", event.Kind)
	}
	foundClient := false
	var markers []string
	for _, tag := range event.Tags {
		if tag[0] == "client" && tag[1] == "nostrmash" {
			foundClient = true
		}
		if tag[0] == "r" && strings.Contains(tag[1], "old.example") {
			t.Fatal("source r tag was copied into the replacement")
		}
		if tag[0] == "r" {
			markers = append(markers, strings.Join(tag, "|"))
		}
	}
	if !foundClient {
		t.Fatal("unknown non-r tag was dropped")
	}
	if !containsString(markers, "r|wss://shared.example") || !containsString(markers, "r|wss://in.example|read") || !containsString(markers, "r|wss://out.example|write") {
		t.Fatalf("markers = %#v", markers)
	}
	recs := BuildRecommendations(RoleBlossom, nil, []SelectedRelay{{URL: "https://cdn.example"}}, map[string]ScoredRelay{
		"https://cdn.example": {URL: "https://cdn.example", Eligible: true},
	}, nil)
	if len(recs) != 1 || recs[0].Action != ActionInvestigate || !containsString(recs[0].ReasonCodes, "blossom_popularity_not_sufficient") {
		t.Fatalf("blossom = %+v", recs)
	}
	dm := BuildRecommendations(RoleDMInbox,
		[]DeclaredRelay{{URL: "wss://old-dm.example"}},
		nil,
		map[string]ScoredRelay{"wss://old-dm.example": {URL: "wss://old-dm.example", Eligible: true}},
		nil)
	if len(dm) != 1 || dm[0].Action != ActionRemove || !dm[0].Attention || !strings.Contains(dm[0].Limitations[0], "Historical") {
		t.Fatalf("dm = %+v", dm)
	}
	uncertain := ScoredRelay{URL: "wss://flaky.example", Reasons: []Reason{{Code: "connection_failures"}}}
	look := BuildRecommendations(RolePublicInbox,
		[]DeclaredRelay{{URL: uncertain.URL}},
		nil,
		map[string]ScoredRelay{uncertain.URL: uncertain},
		nil)
	if look[0].Action != ActionInvestigate {
		t.Fatalf("uncertain health became %s", look[0].Action)
	}
}

func TestUnknownPresetIsNotSilent(t *testing.T) {
	if _, ok := LookupPreset("not-a-preset"); ok {
		t.Fatal("unknown preset was accepted")
	}
	if preset, ok := LookupPreset(""); !ok || preset.Name != PresetBalanced {
		t.Fatalf("empty preset = %+v ok=%v", preset, ok)
	}
}

func TestHostDialableRejectsMetadataAndLinkLocal(t *testing.T) {
	if err := HostDialable("metadata.google.internal", true); err == nil {
		t.Fatal("metadata host was allowed")
	}
	if err := HostDialable("169.254.169.254", true); err == nil {
		t.Fatal("link-local metadata address was allowed")
	}
	if err := HostDialable("127.0.0.1", false); err == nil {
		t.Fatal("loopback was allowed without the private-network flag")
	}
	if err := HostDialable("127.0.0.1", true); err != nil {
		t.Fatalf("explicit private mode rejected loopback: %v", err)
	}
	if err := HostDialable("fe80::1", true); err == nil {
		t.Fatal("link-local IPv6 was allowed")
	}
}

func pubkeyN(i int) string {
	return fmt.Sprintf("%064x", i+1)
}

func scored(url string, score float64, operator string, covers map[string]float64) ScoredRelay {
	value := score
	return ScoredRelay{URL: url, Score: &value, Eligible: true, OperatorKey: operator, Covers: covers}
}

func containsURL(result SelectResult, url string) bool {
	for _, item := range result.Selected {
		if item.URL == url {
			return true
		}
	}
	return false
}

func reasonCode(reasons []Reason, code string) bool {
	for _, reason := range reasons {
		if reason.Code == code {
			return true
		}
	}
	return false
}

func findingCode(findings []Finding, code string) bool {
	for _, finding := range findings {
		if finding.Code == code {
			return true
		}
	}
	return false
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
