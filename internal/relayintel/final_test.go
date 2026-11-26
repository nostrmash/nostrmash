package relayintel

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestKeepInvestigationsDoesNotRestoreExcludedRelays(t *testing.T) {
	selected := []SelectedRelay{{URL: "wss://keep.example", Current: true}}
	recs := []Recommendation{
		{Action: ActionInvestigate, Current: "wss://banned.example"},
		{Action: ActionInvestigate, Current: "wss://unsure.example"},
	}
	scored := map[string]ScoredRelay{
		"wss://banned.example": {URL: "wss://banned.example", IneligibleReason: "excluded"},
		"wss://unsure.example": {URL: "wss://unsure.example", Eligible: true},
	}
	got, _ := keepInvestigations(selected, recs, scored)
	if containsSelected(got, "wss://banned.example") || !containsSelected(got, "wss://unsure.example") {
		t.Fatalf("restored selection = %#v", got)
	}
}

func TestExcludedCurrentRelayIsNotSelected(t *testing.T) {
	keep := scored("wss://keep.example", 0.6, "host:keep", map[string]float64{"a": 1})
	keep.Current = true
	banned := scored("wss://banned.example", 0.9, "host:banned", map[string]float64{"b": 1})
	banned.Current = true
	banned.Eligible = false
	banned.IneligibleReason = "excluded"
	result := Select(SelectRequest{
		Role: RolePublicInbox, Objective: ObjectiveCoverage,
		Min: 1, Max: 2, RetainExisting: true, Hysteresis: 0.08, QualityBias: 0.15,
		Members: []CohortMember{
			{Pubkey: "a", Weight: 0.5, HasDeclaration: true},
			{Pubkey: "b", Weight: 0.5, HasDeclaration: true},
		},
		Relays: []ScoredRelay{keep, banned},
	})
	if containsURL(result, banned.URL) || !containsURL(result, keep.URL) {
		t.Fatalf("selected %#v", result.Selected)
	}
	if result.ConstraintsSatisfied || !reasonCode(result.Unsatisfied, "excluded") {
		t.Fatalf("constraints = %+v", result.Unsatisfied)
	}
}

func TestPinKeepsPaymentConflictVisible(t *testing.T) {
	pay := true
	relay := ScoreRelay(ScoreInput{
		Role: RolePublicInbox, Pinned: true, FreeOnly: true, PaymentRequired: &pay,
	})
	if !relay.Eligible || relay.IneligibleReason != "payment_required" {
		t.Fatalf("pin hid payment: eligible=%v reason=%s", relay.Eligible, relay.IneligibleReason)
	}
	if !reasonCodeOn(relay.Reasons, "pinned_ineligible") {
		t.Fatalf("reasons = %#v", relay.Reasons)
	}
	authOff := false
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	auth := ScoreRelay(ScoreInput{
		Role: RolePublicInbox, Pinned: true, AuthRead: "require",
		Evidence: Evidence{Now: now, NIP11: &NIP11Facts{OK: true, FetchedAt: now, AuthRequired: &authOff}},
	})
	if !auth.Eligible || auth.IneligibleReason != "auth_rejected" || !reasonCodeOn(auth.Reasons, "pinned_ineligible") {
		t.Fatalf("pin hid auth: %+v", auth.IneligibleReason)
	}
}

func TestFirstEmptyDTagIsTheCoordinate(t *testing.T) {
	parsed := mustParse(t, sampleEvent(KindRelaySet, "", [][]string{
		{"d", ""},
		{"d", "later"},
		{"relay", "wss://a.example"},
	}))
	if parsed.Head.DTag != "" {
		t.Fatalf("empty first d became %q", parsed.Head.DTag)
	}
	spaced := mustParse(t, sampleEvent(KindRelaySet, "", [][]string{
		{"d", " room "},
		{"relay", "wss://a.example"},
	}))
	if spaced.Head.DTag != " room " {
		t.Fatalf("d was trimmed to %q", spaced.Head.DTag)
	}
	if firstD([][]string{{"d"}, {"d", "later"}}) != "" {
		t.Fatal("a later d tag replaced an empty first tag")
	}
}

func TestHistoricalRelayTagAgreesWithThePatch(t *testing.T) {
	pubkey := strings.Repeat("ab", 32)
	raw, err := json.Marshal(map[string]any{
		"id": "abc", "pubkey": pubkey, "created_at": 10, "kind": KindDMRelays, "content": "",
		"tags": [][]string{{"r", "wss://old.example"}, {"relay", "wss://keep.example"}, {"client", "nostrmash"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	input := PatchInput{
		Source: raw, Origin: "indexed", ExpectedPubkey: pubkey, ExpectedKind: KindDMRelays,
		ListOptimized: true, TagName: "relay", Desired: []string{"wss://keep.example"},
	}
	result := confirmProposal(Patch(input), input)
	if !result.Safe || result.Event == nil {
		t.Fatalf("patch = %+v", result)
	}
	parsed, err := Parse(EventInput{Kind: KindDMRelays, Pubkey: pubkey, Tags: result.Event.Tags, EndpointLimit: 80})
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range parsed.Members {
		if strings.Contains(member.EndpointURL, "old.example") {
			t.Fatalf("removed relay remained in the replacement: %#v", result.Event.Tags)
		}
	}
	foundClient := false
	for _, tag := range result.Event.Tags {
		if len(tag) > 0 && tag[0] == "client" {
			foundClient = true
		}
	}
	if !foundClient {
		t.Fatal("unrelated client tag was dropped")
	}
	result.Event.Tags = append(result.Event.Tags, []string{"r", "wss://sneaky.example"})
	rejected := confirmProposal(result, input)
	if rejected.Safe || rejected.Event != nil || !reasonCodeOn(rejected.Blocking, "proposal_mismatch") {
		t.Fatalf("mismatched proposal was signable: %+v", rejected)
	}
}

func TestDeletedCoordinateIsNotOrdinaryCreation(t *testing.T) {
	pubkey := strings.Repeat("cd", 32)
	key := sourceKey(KindDMRelays, "")
	_, resolved, notes := applyAuthoritative(pubkey, nil, nil, nil, nil, deletionView{
		ByCoord: map[string]int64{key: 50},
	})
	if resolved[key].Origin != "deleted" {
		t.Fatalf("origin = %+v notes=%#v", resolved[key], notes)
	}
	blocked := Patch(PatchInput{
		Origin: "deleted", AllowCreate: false, ExpectedPubkey: pubkey, ExpectedKind: KindDMRelays,
		ListOptimized: true, TagName: "relay", Desired: []string{"wss://dm.example"},
	})
	if blocked.Safe || blocked.Event != nil {
		t.Fatalf("deleted list was created: %+v", blocked)
	}
	recreated := Patch(PatchInput{
		Origin: "deleted", AllowCreate: true, ExpectedPubkey: pubkey, ExpectedKind: KindDMRelays,
		ListOptimized: true, TagName: "relay", Desired: []string{"wss://dm.example"},
	})
	if recreated.Origin != "recreate" || !recreated.RequiresReview || recreated.Event == nil || recreated.Event.Change != "recreation" {
		t.Fatalf("recreation = %+v", recreated)
	}
}

func TestKeyPackagePinsStayOnTheSharedOutbox(t *testing.T) {
	maxOutbox := 4
	minKey := 5
	pins := []string{"wss://messaging.example"}
	constraints := Constraints{ByRole: map[string]RoleConstraints{
		RolePublicOutbox: {Max: &maxOutbox},
		RoleKeyPackage:   {Pinned: &pins, Min: &minKey},
	}}
	roles, notes := foldSharedRoles(
		[]string{RolePublicOutbox, RoleKeyPackage},
		AccountConfig{KeyPackageDiscovery: "nip65_outbox"},
		&constraints,
	)
	for _, role := range roles {
		if role == RoleKeyPackage {
			t.Fatalf("keypackage stayed a separate list: %v", roles)
		}
	}
	merged := constraints.forRole(RolePublicOutbox)
	if !stringIn(merged.Pinned, "wss://messaging.example") {
		t.Fatalf("pins = %#v", merged.Pinned)
	}
	if merged.Max[RolePublicOutbox] != 4 {
		t.Fatalf("max = %#v", merged.Max)
	}
	if !reasonCodeOn(notes, "keypackage_follows_outbox") || !reasonCodeOn(notes, "shared_constraint_conflict") {
		t.Fatalf("notes = %#v", notes)
	}
}

func TestQualitySwapClearsBalancedHysteresis(t *testing.T) {
	members := []CohortMember{
		{Pubkey: "a", Weight: 1.0 / 3, HasDeclaration: true},
		{Pubkey: "b", Weight: 1.0 / 3, HasDeclaration: true},
		{Pubkey: "c", Weight: 1.0 / 3, HasDeclaration: true},
	}
	a := scored("wss://a.example", 0.5, "host:a", map[string]float64{"a": 1})
	b := scored("wss://b.example", 0.5, "host:b", map[string]float64{"b": 1})
	c := scored("wss://c.example", 0.2, "host:c", map[string]float64{"c": 1})
	better := scored("wss://better.example", 1, "host:better", map[string]float64{"c": 1})
	for _, relay := range []*ScoredRelay{&a, &b, &c} {
		relay.Current = true
	}
	req := SelectRequest{
		Role: RoleDMInbox, Objective: ObjectiveQuality,
		Min: 3, Max: 3, RetainExisting: true, Hysteresis: 0.08, QualityBias: 0.15, DiversityFactor: 0.85,
		Members: members, Relays: []ScoredRelay{a, b, c, better},
	}
	result := Select(req)
	if !containsURL(result, better.URL) || containsURL(result, c.URL) {
		t.Fatalf("quality swap did not happen: %#v", result.Selected)
	}
	again := Select(req)
	if urlsOfSelected(result) != urlsOfSelected(again) {
		t.Fatalf("not deterministic: %s vs %s", urlsOfSelected(result), urlsOfSelected(again))
	}

	tiny := scored("wss://tiny.example", 0.21, "host:tiny", map[string]float64{"c": 1})
	tinyResult := Select(SelectRequest{
		Role: RoleDMInbox, Objective: ObjectiveQuality,
		Min: 3, Max: 3, RetainExisting: true, Hysteresis: 0.08, QualityBias: 0.15, DiversityFactor: 0.85,
		Members: members, Relays: []ScoredRelay{a, b, c, tiny},
	})
	if containsURL(tinyResult, tiny.URL) {
		t.Fatalf("tiny quality change swapped: %#v", tinyResult.Selected)
	}
}

func TestSwapRejectsOperatorConcentrationAndPinnedSlots(t *testing.T) {
	members := []CohortMember{
		{Pubkey: "a", Weight: 1.0 / 3, HasDeclaration: true},
		{Pubkey: "b", Weight: 1.0 / 3, HasDeclaration: true},
		{Pubkey: "c", Weight: 1.0 / 3, HasDeclaration: true},
	}
	a := scored("wss://a.example", 0.5, "nip11:op-a", map[string]float64{"a": 1})
	b := scored("wss://b.example", 0.5, "nip11:op-b", map[string]float64{"b": 1})
	c := scored("wss://c.example", 0.5, "nip11:op-c", map[string]float64{"c": 1})
	concentrated := scored("wss://same.example", 1, "nip11:op-a", map[string]float64{"c": 1})
	for _, relay := range []*ScoredRelay{&a, &b, &c} {
		relay.Current = true
	}
	crowded := Select(SelectRequest{
		Role: RolePublicOutbox, Objective: ObjectiveCoverage,
		Min: 3, Max: 3, RetainExisting: true, Hysteresis: 0.03, QualityBias: 0.15, DiversityFactor: 0.45,
		Members: members, Relays: []ScoredRelay{a, b, c, concentrated},
	})
	if containsURL(crowded, concentrated.URL) {
		t.Fatalf("concentration was accepted: %#v", crowded.Selected)
	}

	c.Pinned = true
	replacement := scored("wss://replacement.example", 1, "nip11:op-d", map[string]float64{"c": 1})
	pinned := Select(SelectRequest{
		Role: RolePublicOutbox, Objective: ObjectiveCoverage,
		Min: 3, Max: 3, RetainExisting: true, Hysteresis: 0.03, QualityBias: 0.15, DiversityFactor: 0.85,
		Members: members, Relays: []ScoredRelay{a, b, c, replacement},
	})
	if !containsURL(pinned, c.URL) || containsURL(pinned, replacement.URL) {
		t.Fatalf("pin was displaced: %#v", pinned.Selected)
	}
}

func TestReplacementIsOneAction(t *testing.T) {
	current := scored("wss://old.example", 0.2, "host:old", map[string]float64{"a": 1})
	next := scored("wss://new.example", 0.9, "host:new", map[string]float64{"a": 1})
	recs := BuildRecommendations(RoleDMInbox,
		[]DeclaredRelay{{URL: current.URL}},
		[]SelectedRelay{{URL: next.URL}},
		map[string]ScoredRelay{current.URL: current, next.URL: next},
		nil,
	)
	if len(recs) != 1 || recs[0].Action != ActionReplace || recs[0].Current != current.URL || recs[0].Suggested != next.URL {
		t.Fatalf("recommendations = %#v", recs)
	}
	if recs[0].Benefit < 0.6 {
		t.Fatalf("benefit = %f", recs[0].Benefit)
	}
}

func TestBlockedRelayIsNotKeptByAPin(t *testing.T) {
	blocked := scored("wss://blocked.example", 0.9, "host:blocked", map[string]float64{"a": 1})
	blocked.Pinned = true
	blocked.Eligible = false
	blocked.IneligibleReason = "blocked_by_account"
	other := scored("wss://other.example", 0.4, "host:other", map[string]float64{"a": 1})
	result := Select(SelectRequest{
		Role: RolePublicInbox, Min: 1, Max: 1,
		Relays: []ScoredRelay{blocked, other},
	})
	if containsURL(result, blocked.URL) || result.ConstraintsSatisfied || !reasonCode(result.Unsatisfied, "blocked_by_account") {
		t.Fatalf("blocked pin = %#v unsatisfied=%#v", result.Selected, result.Unsatisfied)
	}
}

func TestReconcileDropsAProhibitedRelayAndKeepsAPin(t *testing.T) {
	score := 0.5
	selection := SelectResult{
		Selected:             []SelectedRelay{{URL: "wss://banned.example"}, {URL: "wss://ok.example"}, {URL: "wss://pin.example"}},
		ConstraintsSatisfied: true,
	}
	scored := map[string]ScoredRelay{
		"wss://banned.example": {URL: "wss://banned.example", Eligible: false, IneligibleReason: "excluded"},
		"wss://ok.example":     {URL: "wss://ok.example", Eligible: true, Score: &score},
		"wss://pin.example":    {URL: "wss://pin.example", Eligible: true, Pinned: true, IneligibleReason: "payment_required", Score: &score},
	}
	got := reconcileSelection(selection, scored, nil, 4)
	if containsSelected(got.Selected, "wss://banned.example") || !containsSelected(got.Selected, "wss://ok.example") || !containsSelected(got.Selected, "wss://pin.example") {
		t.Fatalf("final selection = %#v", got.Selected)
	}
	if got.ConstraintsSatisfied || !reasonCode(got.Unsatisfied, "excluded") || !reasonCode(got.Unsatisfied, "pinned_ineligible") {
		t.Fatalf("unsatisfied = %#v", got.Unsatisfied)
	}
}

func TestOverMaxDropsTheLeastUsefulRelay(t *testing.T) {
	a := scored("wss://a.example", 0.5, "host:a", map[string]float64{"a": 1})
	b := scored("wss://b.example", 0.5, "host:b", map[string]float64{"b": 1})
	c := scored("wss://c.example", 0.5, "host:c", map[string]float64{"c": 1})
	for _, relay := range []*ScoredRelay{&a, &b, &c} {
		relay.Current = true
	}
	result := Select(SelectRequest{
		Role: RolePublicInbox, Objective: ObjectiveCoverage,
		Min: 1, Max: 2, RetainExisting: true, Hysteresis: 0.08, QualityBias: 0.15, DiversityFactor: 1,
		Members: []CohortMember{
			{Pubkey: "a", Weight: 0.5, HasDeclaration: true},
			{Pubkey: "b", Weight: 0.4, HasDeclaration: true},
			{Pubkey: "c", Weight: 0.1, HasDeclaration: true},
		},
		Relays: []ScoredRelay{a, b, c},
	})
	if containsURL(result, c.URL) || !containsURL(result, a.URL) || !containsURL(result, b.URL) {
		t.Fatalf("dropped the wrong relay: %#v", result.Selected)
	}
}

func TestBuildProposalAgreesAndDoesNotTreatAWarningAsUnsafe(t *testing.T) {
	pubkey := strings.Repeat("ab", 32)
	raw := configEvent("abc", pubkey, 10, []string{"wss://old.example", "wss://keep.example"})
	account := AccountConfig{Pubkey: pubkey}
	resolved := map[string]resolvedSource{
		sourceKey(KindPublicRelayList, ""): {Raw: raw, Origin: "indexed", Complete: true, EventID: "abc", Kind: KindPublicRelayList},
	}
	proposal := buildProposal(account, []builtRole{
		{role: RolePublicInbox, urls: []string{"wss://keep.example"}, kind: KindPublicRelayList, emit: true},
		{role: RolePublicOutbox, urls: []string{"wss://keep.example"}, kind: KindPublicRelayList, emit: true},
	}, resolved, false, false, nil, []Reason{{Code: "keypackage_follows_outbox", Text: "republish the key package"}}, "")
	if !proposal.Safe || proposal.RequiresReview || proposal.PublicRelays == nil {
		t.Fatalf("proposal = %+v", proposal)
	}
	if !reasonCodeOn(proposal.Warnings, "keypackage_follows_outbox") {
		t.Fatalf("warnings = %#v", proposal.Warnings)
	}
	parsed, err := Parse(EventInput{
		Pubkey: pubkey, EventID: "abc", Kind: KindPublicRelayList, Tags: proposal.PublicRelays.Tags, EndpointLimit: 80,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range parsed.Members {
		if strings.Contains(member.EndpointURL, "old.example") {
			t.Fatalf("old relay remained: %#v", proposal.PublicRelays.Tags)
		}
	}
	blocked := buildProposal(account, []builtRole{
		{role: RoleDMInbox, urls: []string{"wss://dm.example"}, kind: KindDMRelays, tag: "relay", emit: true},
	}, map[string]resolvedSource{
		sourceKey(KindDMRelays, ""): {Origin: "deleted", Kind: KindDMRelays},
	}, true, false, nil, nil, "")
	if blocked.Safe || len(blocked.Lists) != 0 {
		t.Fatalf("deleted list was created: %+v", blocked)
	}
}

func TestShortlistReservesAGlobalRelay(t *testing.T) {
	mandatory := map[string]preparedURL{
		"wss://pin.example": {URL: "wss://pin.example"},
	}
	optional := map[string]preparedURL{}
	rank := map[string]int{}
	for i := 0; i < 12; i++ {
		url := fmt.Sprintf("wss://cohort-%02d.example", i)
		optional[url] = preparedURL{URL: url}
		rank[url] = 1000 - i
	}
	optional["wss://global.example"] = preparedURL{URL: "wss://global.example", Global: true}
	got, truncated, err := prioritizeCandidates(mandatory, optional, rank, 8)
	if err != nil {
		t.Fatal(err)
	}
	if !truncated || !containsPrepared(got, "wss://pin.example") || !containsPrepared(got, "wss://global.example") {
		t.Fatalf("truncated=%v shortlist=%#v", truncated, got)
	}
	heavy := map[string]preparedURL{
		"wss://heavy.example": {URL: "wss://heavy.example"},
		"wss://light.example": {URL: "wss://light.example"},
	}
	ordered, _, err := prioritizeCandidates(nil, heavy, map[string]int{
		"wss://heavy.example": 5000,
		"wss://light.example": 10,
	}, 1)
	if err != nil || len(ordered) != 1 || ordered[0].URL != "wss://heavy.example" {
		t.Fatalf("weighted order = %#v err=%v", ordered, err)
	}
}

func containsPrepared(items []preparedURL, url string) bool {
	for _, item := range items {
		if item.URL == url {
			return true
		}
	}
	return false
}

func containsSelected(selected []SelectedRelay, url string) bool {
	for _, item := range selected {
		if item.URL == url {
			return true
		}
	}
	return false
}

func stringIn(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func reasonCodeOn(reasons []Reason, code string) bool {
	for _, reason := range reasons {
		if reason.Code == code {
			return true
		}
	}
	return false
}

func urlsOfSelected(result SelectResult) string {
	out := make([]string, 0, len(result.Selected))
	for _, item := range result.Selected {
		out = append(out, item.URL)
	}
	return strings.Join(out, ",")
}
