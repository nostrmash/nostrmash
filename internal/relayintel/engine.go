package relayintel

import (
	"context"
	"fmt"
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
type Proposal struct {
	PublicRelays *UnsignedEvent  `json:"public_relays,omitempty"`
	Lists        []UnsignedEvent `json:"lists,omitempty"`
	Note         string          `json:"note"`
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
	account := EffectiveConfig(req.Pubkey, heads, members)
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

	pinned, pinNotes, err := s.normalizeMany(req.Constraints.Pinned)
	if err != nil {
		return PlanResponse{}, err
	}
	excluded, excludeNotes, err := s.normalizeMany(req.Constraints.Excluded)
	if err != nil {
		return PlanResponse{}, err
	}
	preferred, preferNotes, err := s.normalizeMany(req.Constraints.Preferred)
	if err != nil {
		return PlanResponse{}, err
	}
	custom, customNotes, err := s.normalizeMany(req.Constraints.Custom)
	if err != nil {
		return PlanResponse{}, err
	}
	paid, paidNotes, err := s.normalizeMany(req.Constraints.PaidAccess)
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
		minCount, maxCount := preset.Counts(role)
		if value, ok := req.Constraints.Min[role]; ok {
			minCount = value
		}
		if value, ok := req.Constraints.Max[role]; ok {
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

		candidates := map[string]preparedURL{}
		addPrepared := func(item preparedURL) {
			if item.URL == "" {
				return
			}
			if _, drop := excluded[item.URL]; drop {
				if _, keep := pinned[item.URL]; !keep {
					return
				}
			}
			candidates[item.URL] = item
		}
		for _, relay := range current.Relays {
			addPrepared(preparedURL{URL: relay.URL, Key: relay.URLKey, PublicSafe: relay.PublicSafe})
		}
		for _, item := range []map[string]preparedURL{pinned, preferred, custom, paid} {
			for _, prepared := range item {
				addPrepared(prepared)
			}
		}
		for _, item := range supplied[role] {
			addPrepared(item)
		}
		if statsReady {
			for _, statRole := range statRoles(role) {
				stats, err := loadTopStats(ctx, s.pool, statRole, maxStatsPerRole)
				if err != nil {
					return PlanResponse{}, err
				}
				for _, stat := range stats {
					addPrepared(preparedURL{URL: stat.URL, Key: stat.URLKey, PublicSafe: true})
				}
			}
		}
		for endpoint := range byURL {
			normalized, key, safe, ok := normalizeNostr(endpoint)
			if !ok {
				continue
			}
			addPrepared(preparedURL{URL: normalized, Key: key, PublicSafe: safe})
		}
		ordered := make([]preparedURL, 0, len(candidates))
		for _, item := range candidates {
			ordered = append(ordered, item)
		}
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].URL < ordered[j].URL })
		if len(ordered) > maxCandidates {
			ordered = ordered[:maxCandidates]
			for _, relay := range current.Relays {
				if _, ok := candidates[relay.URL]; ok {
					found := false
					for _, item := range ordered {
						if item.URL == relay.URL {
							found = true
							break
						}
					}
					if !found {
						ordered = append(ordered, preparedURL{URL: relay.URL, Key: relay.URLKey, PublicSafe: relay.PublicSafe})
					}
				}
			}
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

		weights := preset.WeightsFor(role, req.Constraints.Weights, req.Constraints.HasWeights)
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
			_, inRegistry := registry[item.Key]
			_, inStats := statIndex[item.Key]
			if !inRegistry && !inStats {
				isCustom = true
			}
			input := ScoreInput{
				Role: role, Weights: weights,
				CohortCoverage: cohortCoverage, CohortKnown: cohort.Size > 0,
				MaxTrust: maxTrust, TrustWeighted: stat.Trust,
				Users: stat.Users, MaxUsers: maxUsers,
				Propagation: evidence.Propagation, Evidence: evidence,
				PaymentRequired: paymentFlag(facts), AuthRequired: authFlag(facts),
				FreeOnly: req.Constraints.FreeOnly, PaidAccess: isPaid,
				AuthRead: req.Constraints.AuthRead, AuthWrite: req.Constraints.AuthWrite,
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
			if req.Constraints.MinReliability > 0 && relay.Components.Reliability < req.Constraints.MinReliability && !isPinned {
				relay.Eligible = false
				relay.IneligibleReason = "min_reliability"
			}
			if req.Constraints.MinCapabilityConfidence > 0 && relay.Confidence < req.Constraints.MinCapabilityConfidence && !isPinned {
				relay.Eligible = false
				if relay.IneligibleReason == "" {
					relay.IneligibleReason = "min_capability_confidence"
				}
			}
			if _, banned := excluded[item.URL]; banned && isPinned {
				out.ConstraintsSatisfied = false
				out.Unsatisfied = append(out.Unsatisfied, Reason{Code: "pinned_excluded", Text: "A relay is both pinned and excluded. The pin was kept."})
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
		if req.Constraints.Diversity != nil {
			diversity = *req.Constraints.Diversity
		}
		retain := preset.RetainExisting
		if req.Constraints.RetainExisting != nil {
			retain = *req.Constraints.RetainExisting
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
		recs := BuildRecommendations(role, current.Relays, selection.Selected, scoredByURL, down)
		selection.Selected, _ = keepInvestigations(selection.Selected, recs)
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
		})
	}

	sourceIDs := make([]string, 0, 4)
	if account.ModernEventID != "" {
		sourceIDs = append(sourceIDs, account.ModernEventID)
	}
	for _, item := range builtRoles {
		if item.source != "" {
			sourceIDs = append(sourceIDs, item.source)
		}
	}
	tags, err := loadEventTags(ctx, s.pool, sourceIDs)
	if err != nil {
		return PlanResponse{}, err
	}
	out.Proposal = buildProposal(account, builtRoles, tags, out.Proposal.Note)
	if len(out.Fallbacks) == 0 && out.Cohort.Used == "personalized" {
		out.Cohort.Used = "personalized"
	}
	return out, nil
}

type builtRole struct {
	role   string
	urls   []string
	kind   int
	tag    string
	emit   bool
	source string
	when   int64
}

func buildProposal(account AccountConfig, roles []builtRole, tags map[string][][]string, note string) Proposal {
	proposal := Proposal{Note: note}
	inbox := urlsOf(roleRelays(account, RolePublicInbox))
	outbox := urlsOf(roleRelays(account, RolePublicOutbox))
	touchPublic := false
	for _, item := range roles {
		switch item.role {
		case RolePublicInbox:
			inbox = item.urls
			touchPublic = true
		case RolePublicOutbox:
			outbox = item.urls
			touchPublic = true
		case RoleKeyPackage:
			if account.KeyPackageDiscovery != "kind_10051_legacy" && !containsRole(roles, RolePublicOutbox) {
				outbox = item.urls
				touchPublic = true
			}
		}
	}
	if touchPublic {
		event := NIP65Event(account.Pubkey, tags[account.ModernEventID], account.ModernEventID, account.ModernCreatedAt, inbox, outbox)
		proposal.PublicRelays = &event
	}
	for _, item := range roles {
		if !item.emit || item.kind == 0 || item.kind == KindPublicRelayList {
			continue
		}
		if item.role == RoleKeyPackage && account.KeyPackageDiscovery != "kind_10051_legacy" {
			continue
		}
		event := ListEvent(item.kind, account.Pubkey, item.tag, item.source, item.when, tags[item.source], item.urls)
		proposal.Lists = append(proposal.Lists, event)
	}
	return proposal
}

func (s *Service) normalizeMany(urls []string) (map[string]preparedURL, []Reason, error) {
	out := map[string]preparedURL{}
	var notes []Reason
	if len(urls) > 32 {
		return nil, nil, invalidf("a relay list is longer than 32")
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
	return nil
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
