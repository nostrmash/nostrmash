package relayintel

import "math"

// Weights are the scoring shares for one role. Diversity is intentionally
// absent: it is applied once, to the selected set.
type Weights struct {
	Personalized  float64 `json:"personalized"`
	Reliability   float64 `json:"reliability"`
	Latency       float64 `json:"latency"`
	Reach         float64 `json:"reach"`
	Privacy       float64 `json:"privacy"`
	Functionality float64 `json:"functionality"`
}

// Normalize rescales non-negative weights onto 1. A zero vector is unchanged.
func (w Weights) Normalize() Weights {
	w.Personalized = nonneg(w.Personalized)
	w.Reliability = nonneg(w.Reliability)
	w.Latency = nonneg(w.Latency)
	w.Reach = nonneg(w.Reach)
	w.Privacy = nonneg(w.Privacy)
	w.Functionality = nonneg(w.Functionality)
	sum := w.Personalized + w.Reliability + w.Latency + w.Reach + w.Privacy + w.Functionality
	if sum == 0 {
		return w
	}
	w.Personalized /= sum
	w.Reliability /= sum
	w.Latency /= sum
	w.Reach /= sum
	w.Privacy /= sum
	w.Functionality /= sum
	return w
}

func nonneg(value float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0
	}
	return value
}

// DefaultWeights are the starting shares from the product brief, with the 5%
// diversity share removed and the rest renormalized. Diversity is a set penalty.
func DefaultWeights(role string) Weights {
	switch role {
	case RolePublicOutbox:
		return Weights{Personalized: 0.25, Reliability: 0.20, Latency: 0.05, Reach: 0.30, Functionality: 0.15}.Normalize()
	case RoleDMInbox:
		return Weights{Personalized: 0.10, Reliability: 0.30, Latency: 0.10, Privacy: 0.30, Functionality: 0.15}.Normalize()
	case RoleSearch:
		return Weights{Personalized: 0.05, Reliability: 0.25, Latency: 0.20, Reach: 0.15, Functionality: 0.30}.Normalize()
	case RoleFavorite, RoleRelaySet:
		return Weights{Personalized: 0.40, Reliability: 0.20, Latency: 0.05, Reach: 0.15, Functionality: 0.15}.Normalize()
	case RoleKeyPackage:
		return Weights{Personalized: 0.15, Reliability: 0.25, Latency: 0.10, Reach: 0.25, Functionality: 0.20}.Normalize()
	case RoleWiki:
		return Weights{Personalized: 0.10, Reliability: 0.25, Latency: 0.10, Reach: 0.20, Functionality: 0.30}.Normalize()
	case RoleBlossom:
		return Weights{Personalized: 0.15, Reliability: 0.45, Latency: 0.15, Functionality: 0.20}.Normalize()
	default:
		return Weights{Personalized: 0.35, Reliability: 0.25, Latency: 0.10, Reach: 0.15, Functionality: 0.10}.Normalize()
	}
}

// Components are each scaled to 0..1. Missing evidence stays near the middle
// and lowers confidence instead of pretending to be a perfect score.
type Components struct {
	Personalized  float64 `json:"personalized"`
	Reliability   float64 `json:"reliability"`
	Latency       float64 `json:"latency"`
	Reach         float64 `json:"reach"`
	Privacy       float64 `json:"privacy"`
	Functionality float64 `json:"functionality"`
}

// Reason is a stable code plus a sentence a client can show.
type Reason struct {
	Code string `json:"code"`
	Text string `json:"text"`
}

// ScoredRelay is one candidate after eligibility and scoring.
type ScoredRelay struct {
	URL              string                `json:"url"`
	URLKey           string                `json:"url_key"`
	Role             string                `json:"role"`
	EndpointType     string                `json:"endpoint_type"`
	Score            *float64              `json:"score"`
	Components       Components            `json:"components"`
	Weights          Weights               `json:"weights"`
	Confidence       float64               `json:"confidence"`
	ConfidenceLabel  string                `json:"confidence_label"`
	Eligible         bool                  `json:"eligible"`
	IneligibleReason string                `json:"ineligible_reason,omitempty"`
	Reasons          []Reason              `json:"reasons"`
	OperatorKey      string                `json:"operator_key,omitempty"`
	Domain           string                `json:"domain,omitempty"`
	Users            int64                 `json:"distinct_users"`
	EndpointUsers    int64                 `json:"endpoint_users,omitempty"`
	TrustWeighted    float64               `json:"trust_weighted"`
	ActiveUsers      int64                 `json:"active_users"`
	ModernUsers      int64                 `json:"modern_users"`
	LegacyUsers      int64                 `json:"legacy_users"`
	NetUserDelta7d   int64                 `json:"net_user_delta_7d"`
	CohortCoverage   float64               `json:"cohort_coverage"`
	AdmissionState   string                `json:"admission_state,omitempty"`
	Capabilities     map[string]Capability `json:"capabilities,omitempty"`
	Current          bool                  `json:"current"`
	Pinned           bool                  `json:"pinned"`
	Preferred        bool                  `json:"preferred,omitempty"`
	PaidAccess       bool                  `json:"paid_access,omitempty"`
	Custom           bool                  `json:"custom,omitempty"`
	Covers           map[string]float64    `json:"-"`
}

// ScoreInput is the per-relay evidence used to fill Components.
type ScoreInput struct {
	Role            string
	Weights         Weights
	CohortCoverage  float64
	CohortKnown     bool
	MaxTrust        float64
	TrustWeighted   float64
	Users           int64
	MaxUsers        int64
	Propagation     Propagation
	Evidence        Evidence
	PaymentRequired *bool
	AuthRequired    *bool
	FreeOnly        bool
	PaidAccess      bool
	AuthRead        string
	AuthWrite       string
	Blocked         bool
	Pinned          bool
}

// ScoreRelay computes one explainable score. A nil Score means the evidence
// is too thin for a precise number. The relay can still be ordered after
// scored relays.
func ScoreRelay(in ScoreInput) ScoredRelay {
	weights := in.Weights.Normalize()
	if weights.Personalized+weights.Reliability+weights.Latency+weights.Reach+weights.Privacy+weights.Functionality == 0 {
		weights = DefaultWeights(in.Role)
	}
	caps := ResolveCapabilities(in.Evidence)
	components := Components{}
	var reasons []Reason
	confidenceParts := make([]float64, 0, 4)

	if in.CohortKnown {
		components.Personalized = clamp01(in.CohortCoverage)
		confidenceParts = append(confidenceParts, 0.7)
		if components.Personalized >= 0.25 {
			reasons = append(reasons, Reason{Code: "cohort_overlap", Text: "People in this account's public neighborhood already use this relay for the matching role."})
		}
	} else if in.MaxTrust > 0 {
		components.Personalized = clamp01(in.TrustWeighted / in.MaxTrust)
		components.Personalized = math.Sqrt(components.Personalized)
		confidenceParts = append(confidenceParts, 0.45)
		reasons = append(reasons, Reason{Code: "global_fallback", Text: "No usable neighborhood was available, so personalized relevance fell back to trust-weighted public adoption."})
	} else {
		components.Personalized = 0.4
		confidenceParts = append(confidenceParts, 0.15)
		reasons = append(reasons, Reason{Code: "global_fallback", Text: "Personalized inputs were missing, so this component is a neutral prior."})
	}

	reliability, relConf, relReasons := reliabilityComponent(in.Evidence)
	components.Reliability = reliability
	confidenceParts = append(confidenceParts, relConf)
	reasons = append(reasons, relReasons...)

	latency, latConf := latencyComponent(in.Evidence)
	components.Latency = latency
	confidenceParts = append(confidenceParts, latConf)

	reach, reachConf := reachComponent(in)
	components.Reach = reach
	confidenceParts = append(confidenceParts, reachConf)
	if in.Users > 0 && in.TrustWeighted == 0 && in.MaxTrust > 0 {
		reasons = append(reasons, Reason{Code: "low_trust_adoption", Text: "Declarations for this relay come from accounts without trust weight, so raw popularity was discounted."})
	}

	privacy, privConf, privReasons := privacyComponent(in.Role, caps)
	components.Privacy = privacy
	if weights.Privacy > 0 {
		confidenceParts = append(confidenceParts, privConf)
		reasons = append(reasons, privReasons...)
	}

	functionality, fnConf, fnReasons, ineligible := functionalityComponent(in.Role, caps)
	components.Functionality = functionality
	confidenceParts = append(confidenceParts, fnConf)
	reasons = append(reasons, fnReasons...)

	confidence := 0.0
	for _, part := range confidenceParts {
		confidence += part
	}
	if len(confidenceParts) > 0 {
		confidence /= float64(len(confidenceParts))
	}
	confidence = round3(clamp01(confidence))

	out := ScoredRelay{
		Role:            in.Role,
		Components:      roundComponents(components),
		Weights:         weights,
		Confidence:      confidence,
		ConfidenceLabel: confidenceLabel(confidence),
		Eligible:        true,
		Reasons:         reasons,
		Capabilities:    caps,
		Users:           in.Users,
		TrustWeighted:   round3(in.TrustWeighted),
		CohortCoverage:  round3(in.CohortCoverage),
	}

	if in.Blocked {
		out.Eligible = false
		out.IneligibleReason = "blocked_by_account"
		out.Reasons = append(out.Reasons, Reason{Code: "blocked_by_account", Text: "This account's public blocked-relay list names this endpoint."})
	}
	if ineligible != "" && !in.Pinned {
		out.Eligible = false
		out.IneligibleReason = ineligible
	}
	if in.FreeOnly && !in.PaidAccess && !in.Pinned && paymentRestricted(in, caps) {
		out.Eligible = false
		out.IneligibleReason = "payment_required"
		out.Reasons = append(out.Reasons, Reason{Code: "payment_required", Text: "The relay advertises a payment requirement and this request asked for free relays."})
	}
	if !in.Pinned && authRejected(in, caps) {
		out.Eligible = false
		out.IneligibleReason = "auth_rejected"
		out.Reasons = append(out.Reasons, Reason{Code: "auth_rejected", Text: "Authentication does not match the requested read or write preference."})
	}

	if confidence < 0.25 && !in.Pinned {
		out.Score = nil
		out.ConfidenceLabel = "insufficient"
		out.Reasons = append(out.Reasons, Reason{Code: "insufficient_evidence", Text: "Evidence is too thin for a precise score."})
		return out
	}
	raw := weights.Personalized*components.Personalized +
		weights.Reliability*components.Reliability +
		weights.Latency*components.Latency +
		weights.Reach*components.Reach +
		weights.Privacy*components.Privacy +
		weights.Functionality*components.Functionality
	// Uncertainty shrinks the score. A fast, popular relay cannot look certain
	// when its role capability was never checked.
	raw *= 0.55 + 0.45*confidence
	if authPreference(in) == "prefer_without" && authRestricted(caps) && !in.PaidAccess {
		raw *= 0.85
		out.Reasons = append(out.Reasons, Reason{Code: "auth_penalty", Text: "Authentication is restricted and the request prefers relays that do not require it."})
	}
	value := round3(clamp01(raw))
	out.Score = &value
	return out
}

func reliabilityComponent(ev Evidence) (float64, float64, []Reason) {
	if ev.Probe.At == nil {
		return 0.45, 0.3, []Reason{{Code: "reliability_unknown", Text: "No read probe is stored for this relay."}}
	}
	value := 0.55
	if ev.Probe.HasFailRate {
		value = clamp01(1 - ev.Probe.FailRate)
	}
	if ev.Probe.EOSEOK != nil && *ev.Probe.EOSEOK {
		value = clamp01(value*0.7 + 0.3)
	}
	conf := 0.75
	var reasons []Reason
	age := ev.Now.Sub(*ev.Probe.At)
	if ev.Now.IsZero() {
		age = 0
	}
	if age > ProbeStaleFor {
		value = 0.5
		conf = 0.35
		reasons = append(reasons, Reason{Code: "stale_probe", Text: "The last read probe is older than seven days."})
	} else if age > ProbeFreshFor {
		conf = 0.55
	}
	if ev.Probe.HasFailRate && ev.Probe.FailRate >= 0.8 {
		reasons = append(reasons, Reason{Code: "connection_failures", Text: "Recent read probes failed often. A short outage is not treated as a reason to remove the relay."})
	}
	return value, conf, reasons
}

func latencyComponent(ev Evidence) (float64, float64) {
	ms := ev.Probe.EOSEMs
	if ms == nil {
		ms = ev.Probe.ConnectMs
	}
	if ms == nil {
		return 0.5, 0.25
	}
	value := 1 - clamp01(*ms/2000)
	conf := 0.7
	if ev.Probe.At != nil && !ev.Now.IsZero() && ev.Now.Sub(*ev.Probe.At) > ProbeStaleFor {
		conf = 0.3
	}
	return value, conf
}

func reachComponent(in ScoreInput) (float64, float64) {
	if in.Role == RoleDMInbox {
		return 0, 0.4
	}
	adoption := 0.0
	if in.MaxUsers > 0 && in.Users > 0 {
		adoption = math.Log1p(float64(in.Users)) / math.Log1p(float64(in.MaxUsers))
	}
	if in.MaxTrust > 0 {
		trusted := clamp01(in.TrustWeighted / in.MaxTrust)
		if in.Users > 0 && in.TrustWeighted == 0 {
			adoption *= 0.25
		} else {
			adoption = 0.45*adoption + 0.55*math.Sqrt(trusted)
		}
	}
	if adoption > 0.85 && in.TrustWeighted == 0 {
		adoption = 0.85
	}
	conf := 0.55
	if in.Propagation.Known && in.Propagation.EventsSeen7d > 0 {
		observed := math.Log1p(float64(in.Propagation.EventsSeen7d)) / math.Log1p(100000)
		adoption = clamp01(0.7*adoption + 0.3*observed)
		conf = 0.7
	}
	if in.Users == 0 && !in.Propagation.Known {
		return 0.35, 0.25
	}
	return clamp01(adoption), conf
}

func privacyComponent(role string, caps map[string]Capability) (float64, float64, []Reason) {
	if role != RoleDMInbox && role != RoleKeyPackage {
		return 0.5, 0.3, nil
	}
	wrap := caps[CapGiftWrap]
	auth := caps[CapAuth]
	switch wrap.Status {
	case StatusVerified:
		return 0.95, 0.85, []Reason{{Code: "giftwrap_verified", Text: "Gift-wrap behavior was checked with synthetic identities."}}
	case StatusUnsupported:
		return 0, 0.8, []Reason{{Code: "giftwrap_unsupported", Text: "The relay rejected gift-wrapped events."}}
	case StatusAdvertised:
		score := 0.4
		if auth.Status == StatusRestricted {
			score = 0.55
		}
		return score, 0.35, []Reason{{Code: "giftwrap_advertised", Text: "The relay advertises NIP-17 or NIP-59. That is not proof of recipient-only storage."}}
	default:
		if auth.Status == StatusRestricted {
			return 0.5, 0.4, []Reason{{Code: "auth_not_disqualifying", Text: "Authentication is required. That is recorded separately from DM privacy and does not by itself make the relay unsuitable."}}
		}
		return 0.35, 0.25, []Reason{{Code: "privacy_unknown", Text: "Recipient-only DM access has not been verified."}}
	}
}

func functionalityComponent(role string, caps map[string]Capability) (float64, float64, []Reason, string) {
	switch role {
	case RoleSearch:
		search := caps[CapSearch]
		if search.Status == StatusUnsupported {
			return 0, 0.8, []Reason{{Code: "search_unsupported", Text: "Search queries were rejected."}}, "search_unsupported"
		}
		if search.Status == StatusVerified {
			return 0.95, 0.85, []Reason{{Code: "search_verified", Text: "A search query returned a matching event."}}, ""
		}
		if search.Status == StatusAdvertised {
			return 0.45, 0.4, []Reason{{Code: "search_advertised", Text: "NIP-50 is advertised and has not been verified by a query."}}, ""
		}
		return 0.3, 0.25, []Reason{{Code: "search_unknown", Text: "Search support is unknown. Connectivity is not treated as search."}}, ""
	case RoleDMInbox:
		wrap := caps[CapGiftWrap]
		if wrap.Status == StatusUnsupported {
			return 0, 0.8, []Reason{{Code: "dm_unsupported", Text: "The relay does not accept the gift-wrap kinds used for DMs."}}, "dm_unsupported"
		}
		if wrap.Status == StatusVerified {
			return 0.9, 0.8, nil, ""
		}
		read := caps[CapRead]
		if read.Status == StatusObserved || read.Status == StatusVerified {
			return 0.45, 0.4, []Reason{{Code: "dm_unverified", Text: "The relay answers ordinary subscriptions. DM acceptance is still unverified."}}, ""
		}
		return 0.35, 0.25, nil, ""
	case RolePublicOutbox, RoleKeyPackage:
		write := caps[CapWriteRetrieve]
		accept := caps[CapWriteAccept]
		if write.Status == StatusVerified {
			return 0.95, 0.85, []Reason{{Code: "write_retrieved", Text: "A synthetic event was accepted and then read back. Acceptance alone is not counted as retrieval."}}, ""
		}
		if accept.Status == StatusVerified && write.Status != StatusVerified {
			return 0.6, 0.55, []Reason{{Code: "write_accepted_not_retrieved", Text: "The relay accepted a synthetic event. A later read did not return it."}}, ""
		}
		if accept.Status == StatusUnsupported || write.Status == StatusUnsupported {
			return 0.15, 0.7, []Reason{{Code: "write_unverified", Text: "Write behavior looked unsuccessful. The relay stays eligible when the caller already uses it, and is a weak candidate otherwise."}}, ""
		}
		read := caps[CapRead]
		if read.Status == StatusObserved || read.Status == StatusVerified {
			return 0.55, 0.45, []Reason{{Code: "read_only_evidence", Text: "Only a read subscription was checked. Publishing was not."}}, ""
		}
		return 0.4, 0.25, nil, ""
	case RoleBlossom:
		return 0.4, 0.3, []Reason{{Code: "blossom_unprobed", Text: "Blossom servers are HTTP media endpoints and are not scored as Nostr relays."}}, ""
	default:
		read := caps[CapRead]
		if read.Status == StatusObserved || read.Status == StatusVerified {
			return 0.75, 0.6, []Reason{{Code: "read_observed", Text: "A read subscription completed. This does not prove every event kind is stored."}}, ""
		}
		if read.Status == StatusUnsupported {
			return 0.2, 0.7, []Reason{{Code: "read_failed", Text: "The last read subscription did not complete."}}, ""
		}
		return 0.4, 0.25, nil, ""
	}
}

func paymentRestricted(in ScoreInput, caps map[string]Capability) bool {
	if in.PaymentRequired != nil && *in.PaymentRequired {
		return true
	}
	return caps[CapPayment].Status == StatusRestricted
}

func authRestricted(caps map[string]Capability) bool {
	return caps[CapAuth].Status == StatusRestricted
}

func authPreference(in ScoreInput) string {
	if in.Role == RolePublicOutbox || in.Role == RoleKeyPackage {
		if in.AuthWrite != "" {
			return in.AuthWrite
		}
	}
	if in.AuthRead != "" {
		return in.AuthRead
	}
	return "allow"
}

func authRejected(in ScoreInput, caps map[string]Capability) bool {
	pref := authPreference(in)
	restricted := authRestricted(caps)
	switch pref {
	case "reject":
		return restricted
	case "require":
		// Advertised and not restricted means the document explicitly said
		// authentication is not required. Unknown stays eligible.
		return caps[CapAuth].Status == StatusAdvertised && !restricted
	default:
		return false
	}
}

func confidenceLabel(value float64) string {
	switch {
	case value >= 0.75:
		return "high"
	case value >= 0.45:
		return "medium"
	case value >= 0.25:
		return "low"
	default:
		return "insufficient"
	}
}

func roundComponents(c Components) Components {
	c.Personalized = round3(c.Personalized)
	c.Reliability = round3(c.Reliability)
	c.Latency = round3(c.Latency)
	c.Reach = round3(c.Reach)
	c.Privacy = round3(c.Privacy)
	c.Functionality = round3(c.Functionality)
	return c
}
