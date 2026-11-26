package relayintel

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestBlockedAndPaidPinKeepsTheBlock(t *testing.T) {
	pay := true
	relay := ScoreRelay(ScoreInput{
		Role: RolePublicInbox, Pinned: true, Blocked: true, FreeOnly: true, PaymentRequired: &pay,
	})
	if relay.Eligible || relay.IneligibleReason != "blocked_by_account" {
		t.Fatalf("primary = %s eligible=%v", relay.IneligibleReason, relay.Eligible)
	}
	if !hasViolation(relay, "blocked_by_account") || !hasViolation(relay, "payment_required") {
		t.Fatalf("violations = %#v", relay.Violations)
	}
	relay.URL = "wss://blocked.example"
	relay.Pinned = true
	other := scored("wss://other.example", 0.4, "host:other", map[string]float64{"a": 1})
	result := Select(SelectRequest{
		Role: RolePublicInbox, Min: 1, Max: 1,
		Relays: []ScoredRelay{relay, other},
	})
	if containsURL(result, relay.URL) || result.ConstraintsSatisfied {
		t.Fatalf("blocked pin survived: %#v %+v", result.Selected, result.Unsatisfied)
	}
}

func TestConstraintCombinationsNeverRestoreABlock(t *testing.T) {
	for pins := 0; pins < 2; pins++ {
		for blocked := 0; blocked < 2; blocked++ {
			for excluded := 0; excluded < 2; excluded++ {
				relay := scored("wss://mix.example", 0.9, "host:mix", map[string]float64{"a": 1})
				relay.Current = true
				relay.Pinned = pins == 1
				if blocked == 1 {
					relay.Violations = append(relay.Violations, Violation{Code: "blocked_by_account", Hard: true, BlocksSigning: true})
					relay.Eligible = false
					relay.IneligibleReason = "blocked_by_account"
				}
				if excluded == 1 {
					relay.Violations = append(relay.Violations, Violation{Code: "excluded", Hard: true, ManualRetention: true, BlocksSigning: true})
					if relay.IneligibleReason == "" {
						relay.IneligibleReason = "excluded"
					}
					if !relay.Pinned {
						relay.Eligible = false
					}
				}
				result := Select(SelectRequest{
					Role: RolePublicInbox, Min: 1, Max: 1, RetainExisting: true, Hysteresis: 0.08,
					Relays: []ScoredRelay{relay, scored("wss://other.example", 0.2, "host:other", map[string]float64{"a": 1})},
				})
				if blocked == 1 && containsURL(result, relay.URL) {
					t.Fatalf("pins=%d excluded=%d restored a block", pins, excluded)
				}
				if excluded == 1 && !relay.Pinned && containsURL(result, relay.URL) {
					t.Fatalf("excluded relay was kept")
				}
				if (blocked == 1 || (excluded == 1 && !relay.Pinned)) && result.ConstraintsSatisfied {
					t.Fatalf("constraints satisfied with a prohibition pins=%d blocked=%d excluded=%d", pins, blocked, excluded)
				}
			}
		}
	}
}

func TestCanonicalWinnerReplacesAStaleProjection(t *testing.T) {
	heads := []Head{{SourceKind: KindPublicRelayList, EventID: "bbbbbbbb", CreatedAt: 10}}
	updated, notes := preferCanonical(heads, []canonicalWinner{{
		Kind: KindPublicRelayList, EventID: "aaaaaaaa", CreatedAt: 10,
	}})
	if updated[0].EventID != "aaaaaaaa" || !reasonCodeOn(notesReasons(notes), "projection_stale") {
		t.Fatalf("heads=%+v notes=%#v", updated, notes)
	}
	newer, none := preferCanonical([]Head{{SourceKind: KindPublicRelayList, EventID: "aaaaaaaa", CreatedAt: 20}}, []canonicalWinner{{
		Kind: KindPublicRelayList, EventID: "bbbbbbbb", CreatedAt: 10,
	}})
	if newer[0].EventID != "aaaaaaaa" || len(none) != 0 {
		t.Fatalf("canonical older event replaced the projection: %+v %#v", newer, none)
	}
}

func TestSharedOutboxKeepsKeyPackagePreferences(t *testing.T) {
	free := true
	reliability := 0.8
	preferred := []string{"wss://preferred.example"}
	constraints := Constraints{ByRole: map[string]RoleConstraints{
		RolePublicOutbox: {},
		RoleKeyPackage:   {FreeOnly: &free, MinReliability: &reliability, Preferred: &preferred},
	}}
	_, notes := foldSharedRoles([]string{RoleKeyPackage}, AccountConfig{KeyPackageDiscovery: "nip65_outbox"}, &constraints)
	merged := constraints.forRole(RolePublicOutbox)
	if merged.FreeOnly != true || merged.MinReliability != 0.8 || !stringIn(merged.Preferred, "wss://preferred.example") {
		t.Fatalf("merged = %+v notes=%#v", merged, notes)
	}
}

func notesReasons(notes []sourceNote) []Reason {
	out := make([]Reason, 0, len(notes))
	for _, note := range notes {
		out = append(out, note.reason)
	}
	return out
}

func TestFinalAuditRejectsAThresholdTheSelectionKept(t *testing.T) {
	weak := scored("wss://weak.example", 0.9, "host:weak", map[string]float64{"a": 1})
	weak.Components.Reliability = 0.2
	weak.Violations = []Violation{{Code: "min_reliability", Hard: true, ManualRetention: true, BlocksSigning: true}}
	weak.IneligibleReason = "min_reliability"
	strong := scored("wss://other.example", 0.4, "host:other", map[string]float64{"b": 1})
	selection := SelectResult{
		Selected:             []SelectedRelay{{URL: weak.URL}},
		ConstraintsSatisfied: true,
	}
	audited := auditSelection(selection, []ScoredRelay{weak, strong}, nil, 1, 2, 1, Constraints{MinReliability: 0.5})
	if audited.ConstraintsSatisfied || audited.IndependentlySatisfied || !reasonCode(audited.Unsatisfied, "min_reliability") {
		t.Fatalf("audit = %+v", audited)
	}
}

func TestUnrelatedEventsDoNotCrowdOutTheRequestedEvent(t *testing.T) {
	raw := signedEvent(t, KindPublicRelayList, 1_700_000_000, [][]string{{"r", "wss://relay.example.com"}}, "")
	var wanted map[string]any
	if err := json.Unmarshal(raw, &wanted); err != nil {
		t.Fatal(err)
	}
	noise := signedEvent(t, 1, 1_700_000_001, nil, "unrelated")
	var other map[string]any
	if err := json.Unmarshal(noise, &other); err != nil {
		t.Fatal(err)
	}
	server := relayFrames(t, func(sub string, filter map[string]any) []any {
		frames := make([]any, 0, 12)
		for i := 0; i < 10; i++ {
			frames = append(frames, []any{"EVENT", sub, other})
		}
		frames = append(frames, []any{"EVENT", sub, wanted}, []any{"EOSE", sub})
		return frames
	})
	defer server.Close()
	otherServer := relayFrames(t, func(sub string, filter map[string]any) []any {
		return []any{[]any{"EVENT", sub, wanted}, []any{"EOSE", sub}}
	})
	defer otherServer.Close()
	result, err := VerifyEvent(context.Background(), raw, []string{wsURL(server), wsURL(otherServer)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !result.SampleVerified || result.ExactRetrievedCount != 2 || result.LatestAgreementCount != 2 {
		t.Fatalf("noise crowded the match: %+v", result)
	}
}

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

func TestPaidAccessDoesNotImplyPaymentRequired(t *testing.T) {
	relay := scored("wss://paid.example", 0.8, "host:paid", map[string]float64{"a": 1})
	relay.PaidAccess = true
	relay.Eligible = true
	selection := SelectResult{Selected: []SelectedRelay{{URL: relay.URL}}, ConstraintsSatisfied: true, IndependentlySatisfied: true}
	audited := auditSelection(selection, []ScoredRelay{relay}, nil, 1, 1, 1, Constraints{FreeOnly: true})
	if !audited.ConstraintsSatisfied || reasonCode(audited.Unsatisfied, "payment_required") {
		t.Fatalf("declared paid access blocked a free-only list: %+v", audited)
	}
}

func TestDiversityPreferenceDoesNotBlockSigning(t *testing.T) {
	first := scored("wss://one.example", 0.8, "host:same", map[string]float64{"a": 1})
	second := scored("wss://two.example", 0.7, "host:same", map[string]float64{"b": 1})
	other := scored("wss://other.example", 0.4, "host:other", map[string]float64{"c": 1})
	selection := SelectResult{
		Selected:             []SelectedRelay{{URL: first.URL}, {URL: second.URL}},
		ConstraintsSatisfied: true, IndependentlySatisfied: true,
	}
	soft := auditSelection(selection, []ScoredRelay{first, second, other}, nil, 1, 3, 0.5, Constraints{})
	if !soft.ConstraintsSatisfied || !reasonCode(soft.Qualifications, "operator_concentration") {
		t.Fatalf("soft diversity = %+v", soft)
	}
	hard := auditSelection(selection, []ScoredRelay{first, second, other}, nil, 1, 3, 0.5, Constraints{RequireDiversity: true})
	if hard.ConstraintsSatisfied || !reasonCode(hard.Unsatisfied, "operator_concentration") {
		t.Fatalf("mandatory diversity = %+v", hard)
	}
}

func TestMandatoryDiversityWithoutAnAlternativeStaysUnsatisfied(t *testing.T) {
	only := scored("wss://only.example", 0.9, "host:only", map[string]float64{"a": 1})
	selection := SelectResult{
		Selected:             []SelectedRelay{{URL: only.URL}},
		ConstraintsSatisfied: true, IndependentlySatisfied: true,
	}
	got := auditSelection(selection, []ScoredRelay{only}, nil, 1, 1, 1, Constraints{RequireDiversity: true})
	if got.ConstraintsSatisfied || !reasonCode(got.Unsatisfied, "operator_concentration") {
		t.Fatalf("single endpoint = %+v", got)
	}
	for _, reason := range got.Unsatisfied {
		if reason.Code == "operator_concentration" && !strings.Contains(reason.Text, "No eligible relay") {
			t.Fatalf("missing explanation: %s", reason.Text)
		}
	}
	unknown := scored("wss://unknown.example", 0.8, "", map[string]float64{"a": 1})
	unknownSel := SelectResult{Selected: []SelectedRelay{{URL: unknown.URL}}, ConstraintsSatisfied: true, IndependentlySatisfied: true}
	unknownGot := auditSelection(unknownSel, []ScoredRelay{unknown}, nil, 1, 1, 1, Constraints{RequireDiversity: true})
	if unknownGot.ConstraintsSatisfied || !reasonCode(unknownGot.Unsatisfied, "operator_ownership_unknown") {
		t.Fatalf("unknown operator = %+v", unknownGot)
	}
	pinnedA := scored("wss://pin-a.example", 0.8, "host:pinned", map[string]float64{"a": 1})
	pinnedB := scored("wss://pin-b.example", 0.7, "host:pinned", map[string]float64{"b": 1})
	pinnedA.Pinned = true
	pinnedB.Pinned = true
	pinnedSel := SelectResult{
		Selected:             []SelectedRelay{{URL: pinnedA.URL}, {URL: pinnedB.URL}},
		ConstraintsSatisfied: true, IndependentlySatisfied: true,
	}
	pinnedGot := auditSelection(pinnedSel, []ScoredRelay{pinnedA, pinnedB}, nil, 1, 2, 1, Constraints{RequireDiversity: true})
	if pinnedGot.ConstraintsSatisfied || !reasonCode(pinnedGot.Unsatisfied, "operator_concentration") {
		t.Fatalf("pinned concentration = %+v", pinnedGot)
	}
}

func TestVerifiedKeyPackageIsRequiredDuringEligibility(t *testing.T) {
	min := 0.7
	unknown := scored("wss://unknown.example", 0.9, "host:a", nil)
	unknown.Role = RolePublicOutbox
	unknown.Capabilities = map[string]Capability{
		CapWriteRetrieve: {Name: CapWriteRetrieve, Status: StatusVerified, Confidence: 0.95},
		CapKeyPackage:    {Name: CapKeyPackage, Status: StatusUnknown, Confidence: 0.9},
	}
	applyVerifiedKeyPackage(&unknown, RolePublicOutbox, min)
	if unknown.Eligible {
		unknown.IneligibleReason = primaryReason(unknown.Violations)
		unknown.Eligible = false
	}
	if unknown.Eligible || !hasViolation(unknown, "keypackage_unverified") {
		t.Fatalf("unknown keypackage stayed eligible: %+v", unknown.Violations)
	}
	advertised := unknown
	advertised.URL = "wss://advertised.example"
	advertised.Violations = nil
	advertised.Eligible = true
	advertised.Capabilities = map[string]Capability{
		CapKeyPackage: {Name: CapKeyPackage, Status: StatusAdvertised, Confidence: 0.95},
	}
	applyVerifiedKeyPackage(&advertised, RolePublicOutbox, min)
	if !hasViolation(advertised, "keypackage_unverified") {
		t.Fatal("advertised keypackage satisfied a verified requirement")
	}
	verified := scored("wss://verified.example", 0.8, "host:b", nil)
	verified.Capabilities = map[string]Capability{
		CapKeyPackage: {Name: CapKeyPackage, Status: StatusVerified, Confidence: 0.9},
	}
	applyVerifiedKeyPackage(&verified, RoleKeyPackage, min)
	if hasViolation(verified, "keypackage_unverified") {
		t.Fatal("verified keypackage was rejected")
	}
	weak := verified
	weak.URL = "wss://weak.example"
	weak.Capabilities = map[string]Capability{
		CapKeyPackage: {Name: CapKeyPackage, Status: StatusVerified, Confidence: 0.2},
	}
	applyVerifiedKeyPackage(&weak, RoleKeyPackage, min)
	if !hasViolation(weak, "keypackage_unverified") {
		t.Fatal("low verified confidence satisfied the minimum")
	}
}

func TestKeyPackageConfidenceIsNotPublicWriteConfidence(t *testing.T) {
	confidence := 0.9
	maxOutbox := 4
	maxKey := 2
	constraints := Constraints{ByRole: map[string]RoleConstraints{
		RolePublicOutbox: {Max: &maxOutbox},
		RoleKeyPackage:   {Max: &maxKey, MinCapabilityConfidence: &confidence},
	}}
	_, notes := foldSharedRoles([]string{RoleKeyPackage}, AccountConfig{KeyPackageDiscovery: "nip65_outbox"}, &constraints)
	merged := constraints.forRole(RolePublicOutbox)
	if merged.MinCapabilityConfidence != 0 {
		t.Fatalf("keypackage confidence became a public write threshold: %v", merged.MinCapabilityConfidence)
	}
	if merged.Max[RolePublicOutbox] != 2 {
		t.Fatalf("stricter maximum was not kept: %#v", merged.Max)
	}
	if reasonCodeOn(notes, "shared_constraint_conflict") {
		t.Fatalf("a stricter maximum was reported as a conflict: %#v", notes)
	}
	relay := scored("wss://out.example", 0.8, "host:out", nil)
	relay.Capabilities = ResolveCapabilities(Evidence{Observations: []Observation{{
		Capability: CapWriteRetrieve, Status: StatusVerified, Confidence: 0.95, ObservedAt: time.Now(), SampleCount: 1,
	}}})
	selection := SelectResult{Selected: []SelectedRelay{{URL: relay.URL}}, ConstraintsSatisfied: true, IndependentlySatisfied: true}
	audited := auditKeyPackageCapability(selection, []ScoredRelay{relay}, constraints)
	if audited.ConstraintsSatisfied || !reasonCode(audited.Unsatisfied, "keypackage_unverified") {
		t.Fatalf("kind 1 write satisfied keypackage: %+v", audited)
	}
}

func TestAmbiguousRelayMetadataIsRejected(t *testing.T) {
	url := "wss://a.example"
	raw := eventRaw(t, KindPublicRelayList, "", [][]string{
		{"r", url, "read", "hint", "a"},
		{"r", url, "write", "hint", "b"},
	})
	result := Patch(PatchInput{
		Source: raw, InboxOptimized: true,
		Inbox: []string{"wss://other.example"},
	})
	if result.Safe || result.Event != nil || !reasonCodeOn(result.Blocking, "ambiguous_relay_metadata") {
		t.Fatalf("ambiguous metadata was accepted: %+v", result)
	}
}

func TestDroppedRelayMetadataIsNotSafe(t *testing.T) {
	raw := eventRaw(t, KindPublicRelayList, "", [][]string{
		{"r", "wss://a.example", "read", "hint"},
	})
	input := PatchInput{
		Source: raw, InboxOptimized: true, OutboxOptimized: true,
		Inbox: []string{"wss://b.example"}, Outbox: []string{"wss://a.example"},
	}
	result := Patch(input)
	if result.Event == nil {
		t.Fatal(result)
	}
	for i, tag := range result.Event.Tags {
		if len(tag) > 0 && tag[0] == "r" && strings.Contains(tag[1], "a.example") && len(tag) > 3 {
			result.Event.Tags[i] = tag[:3]
		}
	}
	rejected := confirmProposal(result, input)
	if rejected.Safe || !reasonCodeOn(rejected.Blocking, "metadata_lost") {
		t.Fatalf("dropped metadata stayed safe: %+v", rejected)
	}
}

func TestSourceReportShowsAStaleProjection(t *testing.T) {
	before := []Head{{SourceKind: KindPublicRelayList, EventID: "bbbbbbbb", CreatedAt: 10}}
	after := []Head{{SourceKind: KindPublicRelayList, EventID: "aaaaaaaa", CreatedAt: 10}}
	_, notes := preferCanonical(append([]Head(nil), before...), []canonicalWinner{{
		Kind: KindPublicRelayList, EventID: "aaaaaaaa", CreatedAt: 10,
	}})
	resolved := map[string]resolvedSource{
		sourceKey(KindPublicRelayList, ""): {EventID: after[0].EventID, Kind: KindPublicRelayList},
	}
	reports := sourceReports(before, resolved, notes)
	if len(reports) != 1 || reports[0].Status != "stale" || !reports[0].RequiresRefresh || reports[0].ProjectionEventID != "bbbbbbbb" || reports[0].CanonicalEventID != "aaaaaaaa" {
		t.Fatalf("reports = %+v", reports)
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

func urlsOfSelected(result SelectResult) string {
	out := make([]string, 0, len(result.Selected))
	for _, item := range result.Selected {
		out = append(out, item.URL)
	}
	return strings.Join(out, ",")
}
