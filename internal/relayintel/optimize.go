package relayintel

import "sort"

const (
	ObjectiveCoverage = "coverage"
	ObjectiveQuality  = "quality"
	ObjectiveRetain   = "retain"
)

// SelectRequest is one role's optimization problem.
type SelectRequest struct {
	Role            string
	Preset          Preset
	Objective       string
	Min             int
	Max             int
	RetainExisting  bool
	DiversityFactor float64
	Hysteresis      float64
	QualityBias     float64
	Members         []CohortMember
	Relays          []ScoredRelay
}

// SelectedRelay is one endpoint in the proposed set, with the coverage it adds.
type SelectedRelay struct {
	URL              string  `json:"url"`
	MarginalCoverage float64 `json:"marginal_coverage"`
	UniqueCoverage   float64 `json:"unique_coverage"`
	OperatorKey      string  `json:"operator_key,omitempty"`
	Pinned           bool    `json:"pinned,omitempty"`
	Current          bool    `json:"current,omitempty"`
}

// SelectResult is a deterministic set plus the coverage arithmetic.
type SelectResult struct {
	Selected               []SelectedRelay `json:"selected"`
	CurrentCoverage        float64         `json:"current_coverage"`
	ProposedCoverage       float64         `json:"proposed_coverage"`
	Improvement            float64         `json:"improvement"`
	UnknownWeight          float64         `json:"unknown_weight"`
	KnownWeight            float64         `json:"known_weight"`
	DistinctEndpoints      int             `json:"distinct_endpoints"`
	OperatorCount          int             `json:"operator_count"`
	MaxOperatorShare       float64         `json:"max_operator_share"`
	ConcentrationBasis     string          `json:"concentration_basis"`
	Redundancy             float64         `json:"redundancy"`
	ConstraintsSatisfied   bool            `json:"constraints_satisfied"`
	IndependentlySatisfied bool            `json:"independently_satisfied"`
	Qualifications         []Reason        `json:"qualifications,omitempty"`
	Unsatisfied            []Reason        `json:"unsatisfied_constraints,omitempty"`
	CoverageDefinition     string          `json:"coverage_definition"`
}

// Select picks a complementary set. Individually popular relays that cover
// the same people lose to a relay that covers someone new, once the gain
// clears the hysteresis used to avoid needless churn.
func Select(req SelectRequest) SelectResult {
	if req.Max < 1 {
		req.Max = 1
	}
	if req.Min < 0 {
		req.Min = 0
	}
	if req.Min > req.Max {
		req.Min = req.Max
	}
	if req.DiversityFactor <= 0 || req.DiversityFactor > 1 {
		req.DiversityFactor = 1
	}
	if req.Objective == "" {
		req.Objective = ObjectiveCoverage
	}
	known := map[string]float64{}
	var knownWeight, unknownWeight float64
	for _, member := range req.Members {
		if member.Weight <= 0 {
			continue
		}
		if !member.HasDeclaration {
			unknownWeight += member.Weight
			continue
		}
		known[member.Pubkey] = member.Weight
		knownWeight += member.Weight
	}
	relays := append([]ScoredRelay(nil), req.Relays...)
	sort.Slice(relays, func(i, j int) bool {
		return relayLess(relays[i], relays[j])
	})

	result := SelectResult{
		UnknownWeight:          round3(unknownWeight),
		KnownWeight:            round3(knownWeight),
		CoverageDefinition:     coverageDefinition(req.Role),
		ConcentrationBasis:     "host_or_nip11_pubkey",
		ConstraintsSatisfied:   true,
		IndependentlySatisfied: true,
	}

	byURL := map[string]ScoredRelay{}
	for _, relay := range relays {
		if relay.URL == "" {
			continue
		}
		if _, ok := byURL[relay.URL]; !ok {
			byURL[relay.URL] = relay
		}
	}

	selected := make([]ScoredRelay, 0, req.Max)
	seen := map[string]struct{}{}
	add := func(relay ScoredRelay) {
		if _, ok := seen[relay.URL]; ok {
			return
		}
		seen[relay.URL] = struct{}{}
		selected = append(selected, relay)
	}

	var pins []ScoredRelay
	for _, relay := range relays {
		if !relay.Pinned {
			continue
		}
		if absoluteProhibition(relay) {
			result.ConstraintsSatisfied = false
			result.Unsatisfied = append(result.Unsatisfied, Reason{
				Code: "blocked_by_account",
				Text: "A pinned relay is on the account blocklist. The block was not overridden.",
			})
			continue
		}
		pins = append(pins, relay)
		add(relay)
	}
	if len(pins) > req.Max {
		result.ConstraintsSatisfied = false
		result.Unsatisfied = append(result.Unsatisfied, Reason{
			Code: "pinned_exceeds_max",
			Text: "Pinned relays already exceed the maximum list size. None were removed.",
		})
	}
	excludedPins := 0
	for _, relay := range relays {
		if relay.Pinned && !relay.Eligible && !absoluteProhibition(relay) {
			excludedPins++
		}
	}
	if excludedPins > 0 {
		result.ConstraintsSatisfied = false
		result.Unsatisfied = append(result.Unsatisfied, Reason{
			Code: "pinned_ineligible",
			Text: "A pinned relay failed an eligibility check and was kept anyway.",
		})
	}
	preferredMiss := false
	for _, relay := range relays {
		if !relay.Preferred {
			continue
		}
		if relay.Eligible || relay.Pinned {
			add(relay)
			continue
		}
		preferredMiss = true
	}
	if preferredMiss {
		result.ConstraintsSatisfied = false
		result.Unsatisfied = append(result.Unsatisfied, Reason{
			Code: "preferred_ineligible",
			Text: "A preferred relay failed an eligibility check and was not added.",
		})
	}

	if req.Objective == ObjectiveRetain {
		for _, relay := range relays {
			if relay.Current {
				add(relay)
			}
		}
		result.Selected = annotate(selected, known, knownWeight)
		result.CurrentCoverage = coverageOf(currentOnly(relays), known, knownWeight)
		result.ProposedCoverage = result.CurrentCoverage
		finishDiversity(&result, selected)
		result.IndependentlySatisfied = result.ConstraintsSatisfied
		return result
	}

	if req.RetainExisting {
		for _, relay := range relays {
			if !relay.Current {
				continue
			}
			if absoluteProhibition(relay) || (hardIneligible(relay) && !relay.Pinned) {
				result.ConstraintsSatisfied = false
				result.Unsatisfied = append(result.Unsatisfied, Reason{
					Code: relay.IneligibleReason,
					Text: "A current relay was not kept because it is excluded, blocked, or otherwise prohibited.",
				})
				continue
			}
			add(relay)
		}
	}
	currentCoverage := coverageOf(currentOnly(relays), known, knownWeight)
	result.CurrentCoverage = round3(currentCoverage)

	// Drop unpinned current relays when the list is over the cap, worst unique
	// contribution first. Pins stay even when that leaves the list too large.
	for len(selected) > req.Max {
		idx := worstDropIndex(selected, known, knownWeight, req)
		if idx < 0 || selected[idx].Pinned {
			break
		}
		delete(seen, selected[idx].URL)
		selected = append(selected[:idx], selected[idx+1:]...)
	}
	for _, relay := range relays {
		if relay.Preferred && (relay.Eligible || relay.Pinned) {
			if _, ok := seen[relay.URL]; !ok {
				result.ConstraintsSatisfied = false
				result.Unsatisfied = append(result.Unsatisfied, Reason{
					Code: "preferred_not_kept",
					Text: "A preferred relay did not fit the maximum list size. Pinned relays were not removed to make room.",
				})
				break
			}
		}
	}

	for len(selected) < req.Max {
		best, gain, ok := bestAddition(selected, relays, seen, known, knownWeight, req)
		if !ok {
			break
		}
		minMet := len(selected) >= req.Min
		if minMet && gain < req.Hysteresis && req.Objective == ObjectiveCoverage {
			break
		}
		if minMet && req.Objective == ObjectiveQuality && gain < req.Hysteresis {
			break
		}
		add(best)
	}

	if len(selected) < req.Min {
		result.ConstraintsSatisfied = false
		result.Unsatisfied = append(result.Unsatisfied, Reason{
			Code: "minimum_count_unmet",
			Text: "Not enough eligible relays were available to meet the minimum list size.",
		})
	}

	if req.Objective != ObjectiveRetain {
		selected = improveBySwap(selected, relays, seen, known, knownWeight, req)
	}

	result.Selected = annotate(selected, known, knownWeight)
	result.ProposedCoverage = round3(coverageOf(selected, known, knownWeight))
	result.Improvement = round3(result.ProposedCoverage - result.CurrentCoverage)
	result.DistinctEndpoints = len(selected)
	finishDiversity(&result, selected)
	result.Redundancy = redundancy(selected, known, knownWeight)
	result.IndependentlySatisfied = result.ConstraintsSatisfied
	return result
}

func improveBySwap(selected, relays []ScoredRelay, seen map[string]struct{}, known map[string]float64, knownWeight float64, req SelectRequest) []ScoredRelay {
	if len(selected) == 0 {
		return selected
	}
	base := setObjective(selected, known, knownWeight, req)
	for step := 0; step < 4; step++ {
		threshold := swapThreshold(req, len(selected))
		bestGain := threshold
		dropIdx := -1
		var add ScoredRelay
		found := false
		for i, current := range selected {
			if current.Pinned {
				continue
			}
			without := append(append([]ScoredRelay{}, selected[:i]...), selected[i+1:]...)
			for _, alt := range relays {
				if _, ok := seen[alt.URL]; ok {
					continue
				}
				if absoluteProhibition(alt) || (hardIneligible(alt) && !alt.Pinned) {
					continue
				}
				if !alt.Eligible && !alt.Pinned && !alt.Custom {
					continue
				}
				trial := append(append([]ScoredRelay{}, without...), alt)
				gain := setObjective(trial, known, knownWeight, req) - base
				if gain <= threshold {
					continue
				}
				if !found || gain > bestGain+1e-9 || (abs(gain-bestGain) <= 1e-9 && relayLess(alt, add)) {
					bestGain = gain
					dropIdx = i
					add = alt
					found = true
				}
			}
		}
		if !found || dropIdx < 0 {
			break
		}
		delete(seen, selected[dropIdx].URL)
		selected = append(selected[:dropIdx], selected[dropIdx+1:]...)
		selected = append(selected, add)
		seen[add.URL] = struct{}{}
		base = setObjective(selected, known, knownWeight, req)
	}
	return selected
}

func swapThreshold(req SelectRequest, size int) float64 {
	if size < 1 {
		size = 1
	}
	// Hysteresis is defined on a full-list change. A one-relay swap is
	// compared with the same threshold divided across the current slots,
	// so a large quality or coverage gain in one relay can clear it.
	return req.Hysteresis / float64(size)
}

func setObjective(selected []ScoredRelay, known map[string]float64, knownWeight float64, req SelectRequest) float64 {
	coverage := coverageOf(selected, known, knownWeight)
	if len(selected) == 0 {
		return coverage
	}
	bias := req.QualityBias
	if bias < 0 {
		bias = 0
	}
	if bias > 1 {
		bias = 1
	}
	var quality float64
	var counted int
	for _, relay := range selected {
		if relay.Score == nil {
			continue
		}
		quality += *relay.Score
		counted++
	}
	mean := 0.0
	if counted > 0 {
		mean = quality / float64(counted)
	}
	base := (1-bias)*coverage + bias*mean
	if req.Objective == ObjectiveQuality {
		base = (1-bias)*mean + bias*coverage
	}
	return base - diversityPenalty(selected, req.DiversityFactor)
}

func diversityPenalty(selected []ScoredRelay, factor float64) float64 {
	if len(selected) < 2 || factor >= 1 {
		return 0
	}
	if factor < 0 {
		factor = 0
	}
	counts := map[string]int{}
	for _, relay := range selected {
		key := relay.OperatorKey
		if key == "" {
			key = relay.URL
		}
		counts[key]++
	}
	extra := 0
	for _, count := range counts {
		if count > 1 {
			extra += count - 1
		}
	}
	return (1 - factor) * float64(extra) / float64(len(selected))
}

func currentOnly(relays []ScoredRelay) []ScoredRelay {
	out := make([]ScoredRelay, 0)
	for _, relay := range relays {
		if relay.Current {
			out = append(out, relay)
		}
	}
	return out
}

func bestAddition(selected, relays []ScoredRelay, seen map[string]struct{}, known map[string]float64, knownWeight float64, req SelectRequest) (ScoredRelay, float64, bool) {
	var best ScoredRelay
	bestGain := -1.0
	found := false
	for _, relay := range relays {
		if _, ok := seen[relay.URL]; ok {
			continue
		}
		if absoluteProhibition(relay) || (hardIneligible(relay) && !relay.Pinned) {
			continue
		}
		if !relay.Eligible && !relay.Pinned && !relay.Custom && !relay.Current {
			continue
		}
		gain := marginal(selected, relay, known, knownWeight, req)
		if !found || gain > bestGain+1e-9 || (abs(gain-bestGain) <= 1e-9 && relayLess(relay, best)) {
			best = relay
			bestGain = gain
			found = true
		}
	}
	return best, bestGain, found
}

func marginal(selected []ScoredRelay, relay ScoredRelay, known map[string]float64, knownWeight float64, req SelectRequest) float64 {
	trial := append(append([]ScoredRelay{}, selected...), relay)
	return setObjective(trial, known, knownWeight, req) - setObjective(selected, known, knownWeight, req)
}

func covers(selected []ScoredRelay, pubkey string) bool {
	for _, relay := range selected {
		if coversOne(relay, pubkey) {
			return true
		}
	}
	return false
}

func coversOne(relay ScoredRelay, pubkey string) bool {
	_, ok := relay.Covers[pubkey]
	return ok
}

func coverageOf(selected []ScoredRelay, known map[string]float64, knownWeight float64) float64 {
	if knownWeight <= 0 {
		return 0
	}
	var hit float64
	for pubkey, weight := range known {
		if covers(selected, pubkey) {
			hit += weight
		}
	}
	return hit / knownWeight
}

func annotate(selected []ScoredRelay, known map[string]float64, knownWeight float64) []SelectedRelay {
	out := make([]SelectedRelay, 0, len(selected))
	for i, relay := range selected {
		prior := selected[:i]
		marginalGain := 0.0
		unique := 0.0
		if knownWeight > 0 {
			var added, only float64
			for pubkey, weight := range known {
				if !coversOne(relay, pubkey) {
					continue
				}
				if !covers(prior, pubkey) {
					added += weight
				}
				if !covers(without(selected, i), pubkey) {
					only += weight
				}
			}
			marginalGain = added / knownWeight
			unique = only / knownWeight
		}
		out = append(out, SelectedRelay{
			URL:              relay.URL,
			MarginalCoverage: round3(marginalGain),
			UniqueCoverage:   round3(unique),
			OperatorKey:      relay.OperatorKey,
			Pinned:           relay.Pinned,
			Current:          relay.Current,
		})
	}
	return out
}

func without(selected []ScoredRelay, skip int) []ScoredRelay {
	out := make([]ScoredRelay, 0, len(selected)-1)
	for i, relay := range selected {
		if i != skip {
			out = append(out, relay)
		}
	}
	return out
}

func worstDropIndex(selected []ScoredRelay, known map[string]float64, knownWeight float64, req SelectRequest) int {
	base := setObjective(selected, known, knownWeight, req)
	worst := -1
	worstLoss := 0.0
	for i, relay := range selected {
		if relay.Pinned {
			continue
		}
		loss := base - setObjective(without(selected, i), known, knownWeight, req)
		if worst < 0 || loss < worstLoss-1e-9 || (abs(loss-worstLoss) <= 1e-9 && relay.URL > selected[worst].URL) {
			worst = i
			worstLoss = loss
		}
	}
	return worst
}

func finishDiversity(result *SelectResult, selected []ScoredRelay) {
	counts := map[string]int{}
	for _, relay := range selected {
		key := relay.OperatorKey
		if key == "" {
			key = relay.Domain
		}
		if key == "" {
			key = relay.URL
		}
		counts[key]++
	}
	result.OperatorCount = len(counts)
	maxCount := 0
	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
	}
	if len(selected) == 0 {
		result.MaxOperatorShare = 0
		return
	}
	result.MaxOperatorShare = round3(float64(maxCount) / float64(len(selected)))
}

func redundancy(selected []ScoredRelay, known map[string]float64, knownWeight float64) float64 {
	if len(selected) <= 1 || knownWeight <= 0 {
		return 0
	}
	union := coverageOf(selected, known, knownWeight)
	var sum float64
	for _, relay := range selected {
		sum += coverageOf([]ScoredRelay{relay}, known, knownWeight)
	}
	if sum <= 0 {
		return 0
	}
	return round3(clamp01(1 - union/sum))
}

func relayLess(a, b ScoredRelay) bool {
	as, bs := -1.0, -1.0
	if a.Score != nil {
		as = *a.Score
	}
	if b.Score != nil {
		bs = *b.Score
	}
	if as != bs {
		return as > bs
	}
	if a.Confidence != b.Confidence {
		return a.Confidence > b.Confidence
	}
	return a.URL < b.URL
}

func coverageDefinition(role string) string {
	switch role {
	case RolePublicInbox:
		return "Share of neighborhood weight whose declared public outbox includes the relay. Accounts with no stored outbox are excluded from the denominator and reported as unknown_weight. This is declared preference overlap, not a delivery guarantee."
	case RolePublicOutbox:
		return "Share of neighborhood weight whose declared public inbox includes the relay. Accounts with no stored inbox are excluded from the denominator. This estimates where the audience reads mentions, not guaranteed propagation."
	case RoleDMInbox:
		return "Share of neighborhood weight that declares this DM inbox, blended with privacy and reliability. It is not evidence of who messages the account."
	case RoleSearch:
		return "Search selection is driven by verified or advertised search behavior, reliability, and latency. Adoption overlap is secondary and is not a coverage promise."
	default:
		return "Role-specific declared overlap among neighborhood accounts that published a list for this role. Missing lists are unknown, not unreachable."
	}
}

func hardIneligible(relay ScoredRelay) bool {
	for _, violation := range relay.Violations {
		if violation.Hard {
			return true
		}
	}
	switch relay.IneligibleReason {
	case "search_unsupported", "dm_unsupported", "blocked_by_account", "payment_required", "auth_rejected", "excluded", "min_reliability", "min_capability_confidence", "keypackage_unverified":
		return true
	default:
		return false
	}
}

func abs(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}
