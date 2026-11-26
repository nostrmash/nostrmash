package relayintel

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/nostrmash/nostrmash/internal/metrics"
	"github.com/nostrmash/nostrmash/internal/relayurl"
)

// Constraints are caller limits. Zero values mean "use the preset".
type Constraints struct {
	Max                     map[string]int
	Min                     map[string]int
	Pinned                  []string
	Excluded                []string
	Preferred               []string
	Custom                  []string
	PaidAccess              []string
	FreeOnly                bool
	AuthRead                string
	AuthWrite               string
	MinReliability          float64
	MinCapabilityConfidence float64
	Diversity               *float64
	RetainExisting          *bool
	Weights                 Weights
	HasWeights              bool
	ByRole                  map[string]RoleConstraints
}

// RoleConstraints overrides the global constraints for one role.
// A nil slice means "inherit the global list". An empty slice means "none".
// Exclusions are always added to the global exclusions.
type RoleConstraints struct {
	Max                     *int
	Min                     *int
	Pinned                  *[]string
	Excluded                *[]string
	Preferred               *[]string
	Custom                  *[]string
	PaidAccess              *[]string
	FreeOnly                *bool
	AuthRead                string
	AuthWrite               string
	MinReliability          *float64
	MinCapabilityConfidence *float64
	Diversity               *float64
	RetainExisting          *bool
	Weights                 *Weights
}

// SuppliedRelay is an endpoint the caller already uses and did not publish.
// It is not written to shared tables.
type SuppliedRelay struct {
	Role string
	URL  string
}

// PlanRequest is one optimization. An empty role list uses the default four.
type PlanRequest struct {
	Pubkey         string
	Preset         string
	Roles          []string
	Constraints    Constraints
	Supplied       []SuppliedRelay
	IncludeRanking bool
	SourceEvents   []json.RawMessage
	AllowCreate    bool
}

// Meta is the version and freshness block on every response.
type Meta struct {
	Algorithm        string     `json:"algorithm"`
	ScoreVersion     string     `json:"score_version"`
	OptimizerVersion string     `json:"optimizer_version"`
	TieBreak         string     `json:"tie_break"`
	TieBreakNote     string     `json:"tie_break_note"`
	GeneratedAt      time.Time  `json:"generated_at"`
	StatsComputedAt  *time.Time `json:"stats_computed_at,omitempty"`
	Fallbacks        []string   `json:"fallbacks,omitempty"`
}

// CohortReport says whether the neighborhood was usable.
type CohortReport struct {
	Size      int    `json:"size"`
	Truncated bool   `json:"truncated"`
	TrustData bool   `json:"trust_data"`
	Used      string `json:"used"`
}

// Proposal is the unsigned preview. Nothing here has a signature.
// ProposalSafe is false when a replacement could destroy configuration the
// server has not fully seen. Recommendations can still be present.
type Proposal struct {
	PublicRelays    *UnsignedEvent  `json:"public_relays,omitempty"`
	Lists           []UnsignedEvent `json:"lists,omitempty"`
	Note            string          `json:"note"`
	Safe            bool            `json:"proposal_safe"`
	SourceComplete  bool            `json:"source_complete"`
	SourceOrigin    string          `json:"source_origin,omitempty"`
	SourceEventID   string          `json:"source_event_id,omitempty"`
	SourceCreatedAt int64           `json:"source_created_at,omitempty"`
	RequiresRefresh bool            `json:"requires_refresh"`
	RequiresReview  bool            `json:"requires_review"`
	Blocking        []Reason        `json:"blocking_reasons,omitempty"`
}

// PlanResponse is a full optimization for one pubkey.
type PlanResponse struct {
	Meta
	Pubkey               string       `json:"pubkey"`
	Preset               string       `json:"preset"`
	PresetVersion        string       `json:"preset_version"`
	Cohort               CohortReport `json:"cohort"`
	Roles                []RolePlan   `json:"roles"`
	Proposal             Proposal     `json:"proposal"`
	ConstraintsSatisfied bool         `json:"constraints_satisfied"`
	Unsatisfied          []Reason     `json:"unsatisfied_constraints,omitempty"`
}

type preparedURL struct {
	URL        string
	Key        string
	PublicSafe bool
}

func (s *Service) plan(ctx context.Context, req PlanRequest) (PlanResponse, error) {
	req.Pubkey = strings.ToLower(strings.TrimSpace(req.Pubkey))
	if !validPubkey(req.Pubkey) {
		return PlanResponse{}, invalidf("pubkey must be 64 hex characters or an npub")
	}
	preset, ok := LookupPreset(req.Preset)
	if !ok {
		return PlanResponse{}, invalidf("unknown preset %q", req.Preset)
	}
	if err := validateConstraints(req.Constraints); err != nil {
		return PlanResponse{}, err
	}
	sources, err := ValidateSourceEvents(req.Pubkey, req.SourceEvents)
	if err != nil {
		return PlanResponse{}, err
	}
	roles := req.Roles
	if len(roles) == 0 {
		roles = DefaultOptimizeRoles()
	}
	for _, role := range roles {
		if !knownRole(role) {
			return PlanResponse{}, invalidf("unknown role %q", role)
		}
	}

	heads, members, err := loadAccount(ctx, s.pool, req.Pubkey)
	if err != nil {
		return PlanResponse{}, err
	}
	account, resolved, sourceNotes, err := s.resolveAccount(ctx, req.Pubkey, heads, members, sources)
	if err != nil {
		return PlanResponse{}, err
	}
	roles, foldNotes := foldSharedRoles(roles, account)
	computedAt, statsReady, err := loadSnapshotTime(ctx, s.pool)
	if err != nil {
		return PlanResponse{}, err
	}
	signals, err := loadCohortSignals(ctx, s.pool, req.Pubkey)
	if err != nil {
		return PlanResponse{}, err
	}
	cohort := BuildCohort(signals, cohortLimit)
	now := time.Now().UTC()

	var (
		pinned, excluded, preferred, custom, paid map[string]preparedURL
		pinNotes, excludeNotes, preferNotes       []Reason
		customNotes, paidNotes                    []Reason
	)
	_, pinNotes, err = s.normalizeMany(req.Constraints.Pinned)
	if err != nil {
		return PlanResponse{}, err
	}
	_, excludeNotes, err = s.normalizeMany(req.Constraints.Excluded)
	if err != nil {
		return PlanResponse{}, err
	}
	_, preferNotes, err = s.normalizeMany(req.Constraints.Preferred)
	if err != nil {
		return PlanResponse{}, err
	}
	_, customNotes, err = s.normalizeMany(req.Constraints.Custom)
	if err != nil {
		return PlanResponse{}, err
	}
	_, paidNotes, err = s.normalizeMany(req.Constraints.PaidAccess)
	if err != nil {
		return PlanResponse{}, err
	}
	privateNotes := make([]Reason, 0)
	privateNotes = append(privateNotes, pinNotes...)
	privateNotes = append(privateNotes, excludeNotes...)
	privateNotes = append(privateNotes, preferNotes...)
	privateNotes = append(privateNotes, customNotes...)
	privateNotes = append(privateNotes, paidNotes...)
	supplied := map[string][]preparedURL{}
	for _, item := range req.Supplied {
		if !knownRole(item.Role) {
			return PlanResponse{}, invalidf("unknown role %q", item.Role)
		}
		prepared, rejected, err := s.normalizeOne(item.URL)
		if err != nil {
			return PlanResponse{}, err
		}
		if rejected {
			privateNotes = append(privateNotes, Reason{Code: "private_rejected", Text: "A supplied relay is on a private or metadata network. This deployment does not accept it."})
			metrics.IncRelayIntelFallback("private_rejected")
			continue
		}
		supplied[item.Role] = append(supplied[item.Role], prepared)
	}

	blocked := map[string]struct{}{}
	if role, ok := account.RoleByName(RoleBlocked); ok {
		for _, relay := range role.Relays {
			blocked[relay.URL] = struct{}{}
		}
	}

	out := PlanResponse{
		Meta:                 newMeta(now),
		Pubkey:               req.Pubkey,
		Preset:               preset.Name,
		PresetVersion:        preset.Version,
		ConstraintsSatisfied: true,
		Cohort: CohortReport{
			Size: cohort.Size, Truncated: cohort.Truncated, TrustData: cohort.Trusted,
			Used: "personalized",
		},
		Proposal: Proposal{Note: "Unsigned preview. This server does not sign or publish. The client sets created_at, id, and sig, then checks propagation with POST /api/v1/relays/verify."},
	}
	if !statsReady {
		out.Fallbacks = append(out.Fallbacks, "stats_not_ready")
		metrics.IncRelayIntelFallback("stats_not_ready")
	} else {
		out.StatsComputedAt = &computedAt
	}
	if cohort.Size == 0 {
		out.Cohort.Used = "global_fallback"
		out.Fallbacks = append(out.Fallbacks, "global_fallback")
		metrics.IncRelayIntelFallback("global_fallback")
	}
	out.Unsatisfied = append(out.Unsatisfied, privateNotes...)
	if len(privateNotes) > 0 {
		out.ConstraintsSatisfied = false
	}

	builtRoles := make([]builtRole, 0, len(roles))
	pubkeys := memberPubkeys(cohort)

	for _, role := range roles {
		current, _ := account.RoleByName(role)
		plan := RolePlan{Role: role, Availability: current.Availability}
		if reason, skip := skipRole(role); skip {
			plan.Selection.ConstraintsSatisfied = false
			plan.Selection.Unsatisfied = []Reason{{Code: reason, Text: skipText(reason)}}
			plan.Fallback = reason
			out.Roles = append(out.Roles, plan)
			out.ConstraintsSatisfied = false
			out.Unsatisfied = append(out.Unsatisfied, plan.Selection.Unsatisfied...)
			continue
		}
		rc := req.Constraints.forRole(role)
		var roleNotes []Reason
		pinned, roleNotes, err = s.normalizeMany(rc.Pinned)
		if err != nil {
			return PlanResponse{}, err
		}
		out.Unsatisfied = append(out.Unsatisfied, roleNotes...)
		excluded, roleNotes, err = s.normalizeMany(rc.Excluded)
		if err != nil {
			return PlanResponse{}, err
		}
		out.Unsatisfied = append(out.Unsatisfied, roleNotes...)
		preferred, roleNotes, err = s.normalizeMany(rc.Preferred)
		if err != nil {
			return PlanResponse{}, err
		}
		out.Unsatisfied = append(out.Unsatisfied, roleNotes...)
		custom, roleNotes, err = s.normalizeMany(rc.Custom)
		if err != nil {
			return PlanResponse{}, err
		}
		out.Unsatisfied = append(out.Unsatisfied, roleNotes...)
		paid, roleNotes, err = s.normalizeMany(rc.PaidAccess)
		if err != nil {
			return PlanResponse{}, err
		}
		out.Unsatisfied = append(out.Unsatisfied, roleNotes...)
		if len(out.Unsatisfied) > 0 {
			out.ConstraintsSatisfied = false
		}
		minCount, maxCount := preset.Counts(role)
		if value, ok := rc.Min[role]; ok {
			minCount = value
		}
		if value, ok := rc.Max[role]; ok {
			maxCount = value
		}
		if minCount > maxCount {
			plan.Selection.ConstraintsSatisfied = false
			plan.Selection.Unsatisfied = []Reason{{Code: "min_exceeds_max", Text: "The minimum list size is larger than the maximum. This role was left unchanged."}}
			plan.Selection.CoverageDefinition = coverageDefinition(role)
			out.Roles = append(out.Roles, plan)
			out.ConstraintsSatisfied = false
			out.Unsatisfied = append(out.Unsatisfied, plan.Selection.Unsatisfied...)
			builtRoles = append(builtRoles, builtRole{role: role, urls: urlsOf(current.Relays)})
			continue
		}

		coverRole := coverageRole(role)
		byURL, declared, truncated, err := loadDeclarations(ctx, s.pool, coverRole, pubkeys)
		if err != nil {
			return PlanResponse{}, err
		}
		if truncated {
			cohort.Truncated = true
			out.Cohort.Truncated = true
		}
		roleMembers := append([]CohortMember(nil), cohort.Members...)
		var knownWeight float64
		for i := range roleMembers {
			_, roleMembers[i].HasDeclaration = declared[roleMembers[i].Pubkey]
			if roleMembers[i].HasDeclaration {
				knownWeight += roleMembers[i].Weight
			}
		}
		if cohort.Size > 0 && knownWeight == 0 {
			plan.Fallback = "sparse_declarations"
		}

		mandatory := map[string]preparedURL{}
		optional := map[string]preparedURL{}
		addClass := func(dst map[string]preparedURL, item preparedURL) {
			if item.URL == "" {
				return
			}
			if _, drop := excluded[item.URL]; drop {
				if _, keep := pinned[item.URL]; !keep {
					return
				}
			}
			dst[item.URL] = item
		}
		for _, relay := range current.Relays {
			mandatory[relay.URL] = preparedURL{URL: relay.URL, Key: relay.URLKey, PublicSafe: relay.PublicSafe}
		}
		for _, item := range []map[string]preparedURL{pinned, preferred, custom, paid} {
			for _, prepared := range item {
				addClass(mandatory, prepared)
			}
		}
		for _, item := range supplied[role] {
			addClass(mandatory, item)
		}
		if statsReady {
			for _, statRole := range statRoles(role) {
				stats, err := loadTopStats(ctx, s.pool, statRole, maxStatsPerRole)
				if err != nil {
					return PlanResponse{}, err
				}
				for _, stat := range stats {
					addClass(optional, preparedURL{URL: stat.URL, Key: stat.URLKey, PublicSafe: true})
				}
			}
		}
		for endpoint := range byURL {
			normalized, key, safe, ok := normalizeNostr(endpoint)
			if !ok {
				continue
			}
			addClass(optional, preparedURL{URL: normalized, Key: key, PublicSafe: safe})
		}
		rank := map[string]int{}
		for endpoint, pubs := range byURL {
			rank[endpoint] = len(pubs)
		}
		ordered, err := prioritizeCandidates(mandatory, optional, rank, maxCandidates)
		if err != nil {
			return PlanResponse{}, err
		}

		keys := make([]string, 0, len(ordered))
		urls := make([]string, 0, len(ordered))
		for _, item := range ordered {
			keys = append(keys, item.Key)
			urls = append(urls, item.URL)
		}
		statIndex := map[string]roleStat{}
		var maxUsers int64
		var maxTrust float64
		for _, statRole := range statRoles(role) {
			stats, err := loadStatsFor(ctx, s.pool, statRole, keys)
			if err != nil {
				return PlanResponse{}, err
			}
			for _, stat := range stats {
				if prev, ok := statIndex[stat.URLKey]; !ok || stat.Users > prev.Users {
					statIndex[stat.URLKey] = stat
				}
				if stat.Users > maxUsers {
					maxUsers = stat.Users
				}
				if stat.Trust > maxTrust {
					maxTrust = stat.Trust
				}
			}
		}
		registry, err := loadRegistry(ctx, s.pool, keys)
		if err != nil {
			return PlanResponse{}, err
		}
		observations, err := loadObservations(ctx, s.pool, keys)
		if err != nil {
			return PlanResponse{}, err
		}
		propagation, err := loadPropagation(ctx, s.pool, urls)
		if err != nil {
			return PlanResponse{}, err
		}
		endpointUsers, err := loadEndpointUsers(ctx, s.pool, keys)
		if err != nil {
			return PlanResponse{}, err
		}

		weights := preset.WeightsFor(role, rc.Weights, rc.HasWeights)
		scored := make([]ScoredRelay, 0, len(ordered))
		scoredByURL := map[string]ScoredRelay{}
		var thin int
		for _, item := range ordered {
			stat := statIndex[item.Key]
			reg := registry[item.Key]
			facts := ParseNIP11Summary(reg.Summary)
			covers := map[string]float64{}
			var covered float64
			for pubkey := range byURL[item.URL] {
				for _, member := range roleMembers {
					if member.Pubkey == pubkey && member.HasDeclaration {
						covers[pubkey] = member.Weight
						covered += member.Weight
					}
				}
			}
			cohortCoverage := 0.0
			if knownWeight > 0 {
				cohortCoverage = covered / knownWeight
			}
			evidence := Evidence{
				Now:          now,
				Probe:        probeFrom(reg),
				NIP11:        facts,
				Observations: observations[item.Key],
				Propagation:  propagation[item.URL],
			}
			_, isPinned := pinned[item.URL]
			_, isPreferred := preferred[item.URL]
			_, isPaid := paid[item.URL]
			_, isCustom := custom[item.URL]
			_, isBlocked := blocked[item.URL]
			currentRelay := false
			for _, relay := range current.Relays {
				if relay.URL == item.URL {
					currentRelay = true
					if !relay.PublicSafe {
						isCustom = true
					}
				}
			}
			for _, extra := range supplied[role] {
				if extra.URL == item.URL {
					isCustom = true
				}
			}
			input := ScoreInput{
				Role: role, Weights: weights,
				CohortCoverage: cohortCoverage, CohortKnown: knownWeight > 0,
				MaxTrust: maxTrust, TrustWeighted: stat.Trust,
				Users: stat.Users, MaxUsers: maxUsers,
				Propagation: evidence.Propagation, Evidence: evidence,
				PaymentRequired: paymentFlag(facts), AuthRequired: authFlag(facts),
				FreeOnly: rc.FreeOnly, PaidAccess: isPaid,
				AuthRead: rc.AuthRead, AuthWrite: rc.AuthWrite,
				Blocked: isBlocked && role != RoleBlocked, Pinned: isPinned,
			}
			relay := ScoreRelay(input)
			relay.URL = item.URL
			relay.URLKey = item.Key
			relay.EndpointType = endpointType(role)
			relay.OperatorKey = operatorKey(item.URL, facts)
			relay.Domain = hostOf(item.URL)
			relay.Users = stat.Users
			relay.TrustWeighted = stat.Trust
			relay.ActiveUsers = stat.Active
			relay.ModernUsers = stat.Modern
			relay.LegacyUsers = stat.Legacy
			relay.NetUserDelta7d = stat.Delta
			relay.EndpointUsers = endpointUsers[item.Key]
			relay.AdmissionState = reg.Admission
			relay.Current = currentRelay
			relay.Pinned = isPinned
			relay.Preferred = isPreferred
			relay.PaidAccess = isPaid
			relay.Custom = isCustom
			relay.Covers = covers
			if rc.MinReliability > 0 && relay.Components.Reliability < rc.MinReliability && !isPinned {
				relay.Eligible = false
				relay.IneligibleReason = "min_reliability"
			}
			if rc.MinCapabilityConfidence > 0 && capabilityConfidence(role, relay.Capabilities) < rc.MinCapabilityConfidence && !isPinned {
				relay.Eligible = false
				if relay.IneligibleReason == "" {
					relay.IneligibleReason = "min_capability_confidence"
				}
			}
			if _, banned := excluded[item.URL]; banned && isPinned {
				out.ConstraintsSatisfied = false
				out.Unsatisfied = append(out.Unsatisfied, Reason{Code: "pinned_excluded", Text: "A relay is both pinned and excluded. The pin was kept."})
			}
			if _, banned := excluded[item.URL]; banned && !isPinned {
				relay.Eligible = false
				if relay.IneligibleReason == "" {
					relay.IneligibleReason = "excluded"
				}
			}
			if relay.Score == nil {
				thin++
			}
			scored = append(scored, relay)
			scoredByURL[relay.URL] = relay
		}
		if len(scored) > 0 && thin == len(scored) {
			plan.Fallback = appendFallback(plan.Fallback, "insufficient_evidence")
			metrics.IncRelayIntelFallback("insufficient_evidence")
		}
		if !statsReady && cohort.Size > 0 {
			plan.Fallback = appendFallback(plan.Fallback, "cohort_only")
		}

		diversity := preset.DiversityFactor
		if rc.Diversity != nil {
			diversity = *rc.Diversity
		}
		retain := preset.RetainExisting
		if rc.RetainExisting != nil {
			retain = *rc.RetainExisting
		}
		selection := Select(SelectRequest{
			Role: role, Preset: preset, Objective: ObjectiveFor(role),
			Min: minCount, Max: maxCount, RetainExisting: retain,
			DiversityFactor: diversity, Hysteresis: preset.Hysteresis, QualityBias: preset.QualityBias,
			Members: roleMembers, Relays: scored,
		})
		down := map[string]bool{}
		for _, relay := range scored {
			reg := registry[relay.URLKey]
			if sustainedDown(reg) {
				down[relay.URL] = true
			}
		}
		preview := BuildRecommendations(role, current.Relays, selection.Selected, scoredByURL, down)
		selection.Selected, _ = keepInvestigations(selection.Selected, preview)
		selection = reconcileSelection(selection, scoredByURL, roleMembers, maxCount)
		recs := BuildRecommendations(role, current.Relays, selection.Selected, scoredByURL, down)
		if role == RoleFavorite || role == RoleRelaySet || role == RoleDMInbox || role == RoleKeyPackage {
			for i := range recs {
				if recs[i].Action == ActionRemove || recs[i].Action == ActionReplace {
					recs[i].Attention = true
					if role == RoleFavorite || role == RoleRelaySet {
						recs[i].Limitations = append(recs[i].Limitations, "This list is a user-owned preference.")
					}
				}
			}
		}
		plan.Selection = selection
		plan.Recommendations = recs
		if req.IncludeRanking {
			plan.Ranking = publicRanking(scored, 10)
		}
		if !selection.ConstraintsSatisfied {
			out.ConstraintsSatisfied = false
			out.Unsatisfied = append(out.Unsatisfied, selection.Unsatisfied...)
		}
		out.Roles = append(out.Roles, plan)
		kind, tag, emit := eventFor(role, account)
		builtRoles = append(builtRoles, builtRole{
			role: role, urls: selectedURLs(selection.Selected), kind: kind, tag: tag, emit: emit,
			source: current.EventID, when: current.CreatedAt,
			truncated: current.Truncated, availability: current.Availability, dTag: current.DTag,
		})
	}

	out.Proposal = buildProposal(account, builtRoles, resolved, req.AllowCreate, sourceNotes, foldNotes, out.Proposal.Note)
	if len(out.Fallbacks) == 0 && out.Cohort.Used == "personalized" {
		out.Cohort.Used = "personalized"
	}
	return out, nil
}

type builtRole struct {
	role         string
	urls         []string
	kind         int
	tag          string
	emit         bool
	source       string
	when         int64
	truncated    bool
	availability string
	dTag         string
}

func buildProposal(account AccountConfig, roles []builtRole, resolved map[string]resolvedSource, allowCreate bool, notes []sourceNote, foldNotes []Reason, note string) Proposal {
	proposal := Proposal{Note: note, Safe: true, SourceComplete: true}
	keys := map[string]struct{}{}
	for _, item := range roles {
		if item.role == RolePublicInbox || item.role == RolePublicOutbox {
			keys[sourceKey(KindPublicRelayList, "")] = struct{}{}
		}
		if item.kind != 0 {
			keys[sourceKey(item.kind, item.dTag)] = struct{}{}
		}
	}
	for _, item := range notes {
		if _, ok := keys[sourceKey(item.kind, item.dTag)]; !ok {
			continue
		}
		proposal.Blocking = append(proposal.Blocking, item.reason)
		if item.reason.Code == "stale_source" {
			proposal.RequiresReview = true
		}
	}
	if len(foldNotes) > 0 {
		proposal.RequiresReview = true
		proposal.Blocking = append(proposal.Blocking, foldNotes...)
	}
	inboxOptimized, outboxOptimized := false, false
	inbox := urlsOf(roleRelays(account, RolePublicInbox))
	outbox := urlsOf(roleRelays(account, RolePublicOutbox))
	publicTruncated := false
	for _, item := range roles {
		switch item.role {
		case RolePublicInbox:
			inbox = item.urls
			inboxOptimized = true
			publicTruncated = publicTruncated || item.truncated
		case RolePublicOutbox:
			outbox = item.urls
			outboxOptimized = true
			publicTruncated = publicTruncated || item.truncated
		case RoleKeyPackage:
			if account.KeyPackageDiscovery != "kind_10051_legacy" && !containsRole(roles, RolePublicOutbox) {
				outbox = item.urls
				outboxOptimized = true
				publicTruncated = publicTruncated || item.truncated
			}
		}
	}
	if inboxOptimized || outboxOptimized {
		src := resolved[sourceKey(KindPublicRelayList, "")]
		if src.Origin == "" {
			src.Origin = "missing"
		}
		if publicTruncated {
			src.Complete = false
		}
		result := Patch(PatchInput{
			Source: src.Raw, Origin: src.Origin, Truncated: !src.Complete && src.Origin != "missing" && src.Origin != "deleted",
			AllowCreate: allowCreate && src.Origin == "missing", ExpectedPubkey: account.Pubkey,
			ExpectedKind: KindPublicRelayList, InboxOptimized: inboxOptimized, OutboxOptimized: outboxOptimized,
			Inbox: inbox, Outbox: outbox,
		})
		applyPatch(&proposal, result)
		if result.Event != nil {
			proposal.PublicRelays = result.Event
		}
	}
	for _, item := range roles {
		if !item.emit || item.kind == 0 || item.kind == KindPublicRelayList {
			continue
		}
		if item.role == RoleKeyPackage && account.KeyPackageDiscovery != "kind_10051_legacy" {
			proposal.RequiresReview = true
			proposal.Blocking = append(proposal.Blocking, Reason{Code: "keypackage_follows_outbox", Text: "KeyPackage discovery follows the public outbox. It was not written as a separate kind 10051 list."})
			continue
		}
		src := resolved[sourceKey(item.kind, item.dTag)]
		if src.Origin == "" {
			src.Origin = "missing"
		}
		if item.truncated {
			src.Complete = false
		}
		result := Patch(PatchInput{
			Source: src.Raw, Origin: src.Origin, Truncated: !src.Complete && src.Origin != "missing" && src.Origin != "deleted",
			AllowCreate: allowCreate && src.Origin == "missing", ExpectedPubkey: account.Pubkey, ExpectedKind: item.kind,
			ListOptimized: true, TagName: item.tag, Desired: item.urls,
		})
		applyPatch(&proposal, result)
		if result.Event != nil {
			proposal.Lists = append(proposal.Lists, *result.Event)
		}
	}
	if !proposal.Safe {
		proposal.Note = "proposal_safe is false. Do not sign or publish a replacement until the client refreshes the source events named by requires_refresh. " + note
		return proposal
	}
	if proposal.PublicRelays == nil && len(proposal.Lists) == 0 {
		proposal.Note = "No replacement event was generated. The proposed configuration matches the source, or no role produced a publishable change. " + note
	}
	return proposal
}

func applyPatch(proposal *Proposal, result PatchResult) {
	if proposal.SourceOrigin == "" {
		proposal.SourceOrigin = result.Origin
	}
	if proposal.SourceEventID == "" {
		proposal.SourceEventID = result.EventID
		proposal.SourceCreatedAt = result.CreatedAt
	}
	if !result.Complete {
		proposal.SourceComplete = false
	}
	if !result.Safe {
		proposal.Safe = false
	}
	if result.RequiresRefresh {
		proposal.RequiresRefresh = true
	}
	if result.RequiresReview {
		proposal.RequiresReview = true
	}
	proposal.Blocking = append(proposal.Blocking, result.Blocking...)
}

func (s *Service) normalizeMany(urls []string) (map[string]preparedURL, []Reason, error) {
	out := map[string]preparedURL{}
	var notes []Reason
	if len(urls) > maxCandidates {
		return nil, nil, invalidf("a relay list is longer than %d", maxCandidates)
	}
	for _, raw := range urls {
		item, rejected, err := s.normalizeOne(raw)
		if err != nil {
			return nil, nil, err
		}
		if rejected {
			notes = append(notes, Reason{Code: "private_rejected", Text: "A constraint relay is on a private or metadata network. This deployment did not use it."})
			metrics.IncRelayIntelFallback("private_rejected")
			continue
		}
		out[item.URL] = item
	}
	return out, notes, nil
}

func (s *Service) normalizeOne(raw string) (preparedURL, bool, error) {
	normalized, key, safe, ok := normalizeNostr(raw)
	if !ok {
		blossom, err := NormalizeBlossomURL(raw)
		if err != nil {
			return preparedURL{}, false, invalidf("relay URL is not a valid websocket or blossom URL")
		}
		parsed, err := url.Parse(blossom)
		if err != nil {
			return preparedURL{}, false, invalidf("relay URL is not a valid websocket or blossom URL")
		}
		if err := HostDialable(parsed.Hostname(), s.allowPrivate); err != nil {
			return preparedURL{}, true, nil
		}
		return preparedURL{URL: blossom, Key: relayurl.CanonicalKey(blossom), PublicSafe: BlossomPublic(blossom)}, false, nil
	}
	parsed, err := url.Parse(normalized)
	if err != nil {
		return preparedURL{}, false, invalidf("relay URL is not valid")
	}
	if err := HostDialable(parsed.Hostname(), s.allowPrivate); err != nil {
		return preparedURL{}, true, nil
	}
	return preparedURL{URL: normalized, Key: key, PublicSafe: safe}, false, nil
}

func newMeta(now time.Time) Meta {
	return Meta{
		Algorithm: AlgorithmVersion, ScoreVersion: ScoreVersion, OptimizerVersion: OptimizerVersion,
		TieBreak: TieBreak, TieBreakNote: TieBreakNote, GeneratedAt: now,
	}
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidInput, fmt.Sprintf(format, args...))
}

func validateConstraints(c Constraints) error {
	if !validAuth(c.AuthRead) || !validAuth(c.AuthWrite) {
		return invalidf("auth_read and auth_write must be allow, prefer_without, require, or reject")
	}
	if c.MinReliability < 0 || c.MinReliability > 1 || c.MinCapabilityConfidence < 0 || c.MinCapabilityConfidence > 1 {
		return invalidf("reliability and confidence minimums must be between 0 and 1")
	}
	if c.Diversity != nil && (*c.Diversity < 0.2 || *c.Diversity > 1) {
		return invalidf("diversity must be between 0.2 and 1")
	}
	for _, set := range []map[string]int{c.Min, c.Max} {
		for role, value := range set {
			if !knownRole(role) {
				return invalidf("unknown role %q", role)
			}
			if value < 0 || value > 32 {
				return invalidf("relay counts must be from 0 to 32")
			}
		}
	}
	for role, value := range c.Max {
		if value == 0 {
			return invalidf("maximum relay count for %s must be at least 1", role)
		}
	}
	for role, item := range c.ByRole {
		if !knownRole(role) {
			return invalidf("unknown role %q", role)
		}
		if !validAuth(item.AuthRead) || !validAuth(item.AuthWrite) {
			return invalidf("auth_read and auth_write must be allow, prefer_without, require, or reject")
		}
		if item.MinReliability != nil && (*item.MinReliability < 0 || *item.MinReliability > 1) {
			return invalidf("reliability and confidence minimums must be between 0 and 1")
		}
		if item.MinCapabilityConfidence != nil && (*item.MinCapabilityConfidence < 0 || *item.MinCapabilityConfidence > 1) {
			return invalidf("reliability and confidence minimums must be between 0 and 1")
		}
		if item.Diversity != nil && (*item.Diversity < 0.2 || *item.Diversity > 1) {
			return invalidf("diversity must be between 0.2 and 1")
		}
		if item.Max != nil && (*item.Max < 1 || *item.Max > 32) {
			return invalidf("maximum relay count for %s must be from 1 to 32", role)
		}
		if item.Min != nil && (*item.Min < 0 || *item.Min > 32) {
			return invalidf("relay counts must be from 0 to 32")
		}
	}
	return nil
}

func (c Constraints) forRole(role string) Constraints {
	out := c
	out.ByRole = nil
	if c.Max != nil {
		out.Max = maps.Clone(c.Max)
	}
	if c.Min != nil {
		out.Min = maps.Clone(c.Min)
	}
	item, ok := c.ByRole[role]
	if !ok {
		return out
	}
	if item.Max != nil {
		if out.Max == nil {
			out.Max = map[string]int{}
		}
		out.Max[role] = *item.Max
	}
	if item.Min != nil {
		if out.Min == nil {
			out.Min = map[string]int{}
		}
		out.Min[role] = *item.Min
	}
	if item.Pinned != nil {
		out.Pinned = append([]string{}, (*item.Pinned)...)
	}
	if item.Excluded != nil {
		out.Excluded = append(append([]string{}, c.Excluded...), (*item.Excluded)...)
	}
	if item.Preferred != nil {
		out.Preferred = append([]string{}, (*item.Preferred)...)
	}
	if item.Custom != nil {
		out.Custom = append([]string{}, (*item.Custom)...)
	}
	if item.PaidAccess != nil {
		out.PaidAccess = append([]string{}, (*item.PaidAccess)...)
	}
	if item.FreeOnly != nil {
		out.FreeOnly = *item.FreeOnly
	}
	if item.AuthRead != "" {
		out.AuthRead = item.AuthRead
	}
	if item.AuthWrite != "" {
		out.AuthWrite = item.AuthWrite
	}
	if item.MinReliability != nil {
		out.MinReliability = *item.MinReliability
	}
	if item.MinCapabilityConfidence != nil {
		out.MinCapabilityConfidence = *item.MinCapabilityConfidence
	}
	if item.Diversity != nil {
		out.Diversity = item.Diversity
	}
	if item.RetainExisting != nil {
		out.RetainExisting = item.RetainExisting
	}
	if item.Weights != nil {
		out.Weights = *item.Weights
		out.HasWeights = true
	}
	return out
}

func prioritizeCandidates(mandatory, optional map[string]preparedURL, rank map[string]int, limit int) ([]preparedURL, error) {
	required := mapValues(mandatory)
	if len(required) > limit {
		return nil, invalidf("pinned, current, and supplied relays exceed the candidate limit of %d", limit)
	}
	extra := mapValues(optional)
	sort.Slice(extra, func(i, j int) bool {
		if rank[extra[i].URL] != rank[extra[j].URL] {
			return rank[extra[i].URL] > rank[extra[j].URL]
		}
		return extra[i].URL < extra[j].URL
	})
	out := make([]preparedURL, 0, limit)
	seen := map[string]struct{}{}
	for _, item := range required {
		if _, ok := seen[item.URL]; ok {
			continue
		}
		seen[item.URL] = struct{}{}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URL < out[j].URL })
	for _, item := range extra {
		if len(out) >= limit {
			break
		}
		if _, ok := seen[item.URL]; ok {
			continue
		}
		seen[item.URL] = struct{}{}
		out = append(out, item)
	}
	return out, nil
}

func mapValues(items map[string]preparedURL) []preparedURL {
	out := make([]preparedURL, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}

func reconcileSelection(selection SelectResult, scored map[string]ScoredRelay, members []CohortMember, maxCount int) SelectResult {
	known := map[string]float64{}
	var knownWeight float64
	for _, member := range members {
		if member.HasDeclaration && member.Weight > 0 {
			known[member.Pubkey] = member.Weight
			knownWeight += member.Weight
		}
	}
	relays := make([]ScoredRelay, 0, len(selection.Selected))
	for _, item := range selection.Selected {
		if relay, ok := scored[item.URL]; ok {
			relays = append(relays, relay)
		}
	}
	selection.Selected = annotate(relays, known, knownWeight)
	selection.DistinctEndpoints = len(selection.Selected)
	selection.ProposedCoverage = round3(coverageOf(relays, known, knownWeight))
	selection.Improvement = round3(selection.ProposedCoverage - selection.CurrentCoverage)
	selection.Redundancy = redundancy(relays, known, knownWeight)
	finishDiversity(&selection, relays)
	if maxCount > 0 && len(selection.Selected) > maxCount {
		selection.ConstraintsSatisfied = false
		selection.Unsatisfied = append(selection.Unsatisfied, Reason{Code: "retained_over_max", Text: "Keeping an investigated relay made the list longer than the maximum."})
	}
	return selection
}

func capabilityConfidence(role string, caps map[string]Capability) float64 {
	if caps == nil {
		return 0
	}
	switch role {
	case RoleSearch:
		return caps[CapSearch].Confidence
	case RoleDMInbox:
		return caps[CapGiftWrap].Confidence
	case RolePublicOutbox, RoleKeyPackage:
		return caps[CapWriteRetrieve].Confidence
	default:
		return caps[CapRead].Confidence
	}
}

func validAuth(value string) bool {
	switch value {
	case "", "allow", "prefer_without", "require", "reject":
		return true
	default:
		return false
	}
}

func knownRole(role string) bool {
	switch role {
	case RolePublicInbox, RolePublicOutbox, RoleDMInbox, RoleSearch, RoleBlocked,
		RoleFavorite, RoleRelaySet, RoleKeyPackage, RoleBlossom, RoleWiki, "private_storage":
		return true
	default:
		return false
	}
}

func skipRole(role string) (string, bool) {
	switch role {
	case RoleBlocked:
		return "blocked_relays_are_not_optimized", true
	case "private_storage":
		return "private_storage_is_client_side", true
	case RoleRelaySet:
		return "relay_sets_are_not_rewritten", true
	default:
		return "", false
	}
}

func skipText(code string) string {
	switch code {
	case "blocked_relays_are_not_optimized":
		return "Blocked relays are not a recommendation and this server will not propose a blocklist."
	case "private_storage_is_client_side":
		return "Kind 10013 is encrypted. The server will not read or optimize it."
	case "relay_sets_are_not_rewritten":
		return "Kind 30002 sets stay under the account that published them. Open a set by its address instead of optimizing it here."
	default:
		return code
	}
}

func coverageRole(role string) string {
	switch role {
	case RolePublicInbox:
		return RolePublicOutbox
	case RolePublicOutbox:
		return RolePublicInbox
	case RoleKeyPackage:
		return RolePublicOutbox
	default:
		return role
	}
}

func statRoles(role string) []string {
	if role == RoleKeyPackage {
		return []string{RoleKeyPackage, RolePublicOutbox}
	}
	return []string{role}
}

func eventFor(role string, account AccountConfig) (kind int, tag string, emit bool) {
	switch role {
	case RoleDMInbox:
		return KindDMRelays, "relay", true
	case RoleSearch:
		return KindSearchRelays, "relay", true
	case RoleFavorite:
		return KindFavoriteRelays, "relay", true
	case RoleWiki:
		return KindWikiRelays, "relay", true
	case RoleBlossom:
		return KindBlossomServers, "server", true
	case RoleKeyPackage:
		if account.KeyPackageDiscovery == "kind_10051_legacy" {
			return KindKeyPackageList, "relay", true
		}
		return 0, "", false
	default:
		return 0, "", false
	}
}

func probeFrom(reg registryRow) ProbeSnapshot {
	if reg.ProbeAt == nil {
		return ProbeSnapshot{}
	}
	return ProbeSnapshot{
		At: reg.ProbeAt, Status: reg.Status,
		ConnectOK: reg.ConnectOK, SubscribeOK: reg.SubscribeOK, EOSEOK: reg.EOSEOK,
		ConnectMs: reg.ConnectMs, EOSEMs: reg.EOSEMs,
		FailRate: reg.FailRate, HasFailRate: true,
	}
}

func sustainedDown(reg registryRow) bool {
	if reg.ProbeAt == nil || reg.FailRate < 0.8 || reg.EOSEOK == nil || *reg.EOSEOK {
		return false
	}
	return reg.Status == "connect_failed" || reg.Status == "eose_timeout"
}

func paymentFlag(facts *NIP11Facts) *bool {
	if facts == nil {
		return nil
	}
	if facts.HasFees {
		value := true
		return &value
	}
	return facts.PaymentRequired
}

func authFlag(facts *NIP11Facts) *bool {
	if facts == nil {
		return nil
	}
	return facts.AuthRequired
}

func operatorKey(relayURL string, facts *NIP11Facts) string {
	if facts != nil && validPubkey(strings.ToLower(strings.TrimSpace(facts.Pubkey))) {
		return "nip11:" + strings.ToLower(strings.TrimSpace(facts.Pubkey))
	}
	return "host:" + hostOf(relayURL)
}

func hostOf(relayURL string) string {
	parsed, err := url.Parse(relayURL)
	if err != nil {
		return "unknown"
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return "unknown"
	}
	return host
}

func endpointType(role string) string {
	if role == RoleBlossom {
		return EndpointBlossom
	}
	return EndpointNostr
}

func memberPubkeys(cohort Cohort) []string {
	out := make([]string, 0, len(cohort.Members))
	for _, member := range cohort.Members {
		out = append(out, member.Pubkey)
	}
	return out
}

func urlsOf(relays []DeclaredRelay) []string {
	out := make([]string, 0, len(relays))
	for _, relay := range relays {
		out = append(out, relay.URL)
	}
	sort.Strings(out)
	return out
}

func roleRelays(account AccountConfig, role string) []DeclaredRelay {
	item, ok := account.RoleByName(role)
	if !ok {
		return nil
	}
	return item.Relays
}

func selectedURLs(selected []SelectedRelay) []string {
	out := make([]string, 0, len(selected))
	for _, relay := range selected {
		out = append(out, relay.URL)
	}
	sort.Strings(out)
	return out
}

func keepInvestigations(selected []SelectedRelay, recs []Recommendation) ([]SelectedRelay, []string) {
	have := map[string]struct{}{}
	for _, relay := range selected {
		have[relay.URL] = struct{}{}
	}
	for _, rec := range recs {
		if rec.Action != ActionInvestigate || rec.Current == "" {
			continue
		}
		if _, ok := have[rec.Current]; ok {
			continue
		}
		selected = append(selected, SelectedRelay{URL: rec.Current, Current: true})
		have[rec.Current] = struct{}{}
	}
	return selected, selectedURLs(selected)
}

func publicRanking(relays []ScoredRelay, limit int) []ScoredRelay {
	out := append([]ScoredRelay(nil), relays...)
	sort.Slice(out, func(i, j int) bool { return relayLess(out[i], out[j]) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	for i := range out {
		out[i].Covers = nil
	}
	return out
}

func appendFallback(current, next string) string {
	if current == "" {
		return next
	}
	if strings.Contains(current, next) {
		return current
	}
	return current + "," + next
}

func containsRole(roles []builtRole, role string) bool {
	for _, item := range roles {
		if item.role == role {
			return true
		}
	}
	return false
}
