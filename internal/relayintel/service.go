package relayintel

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nostrmash/nostrmash/internal/metrics"
)

// Options configures one process-local service.
type Options struct {
	AllowPrivateNetwork bool
}

// Service answers relay intelligence reads. It does not publish events.
type Service struct {
	pool         *pgxpool.Pool
	allowPrivate bool
	mu           sync.Mutex
	entries      map[string]cacheEntry
	inflight     map[string]*flight
}

type cacheEntry struct {
	expires time.Time
	body    any
}

type flight struct {
	done chan struct{}
	body any
	err  error
}

// RankingsResponse is a global role ranking. It is not a personalized plan.
type RankingsResponse struct {
	Meta
	Role               string        `json:"role"`
	PositiveAdoption   bool          `json:"positive_adoption"`
	CoverageDefinition string        `json:"coverage_definition"`
	Relays             []ScoredRelay `json:"relays"`
	Ranking            string        `json:"ranking"`
	CandidatePool      int           `json:"candidate_pool"`
	Note               string        `json:"note"`
}

// ConfigResponse is one account's effective public relay configuration.
type ConfigResponse struct {
	Meta
	Account AccountConfig  `json:"account"`
	Sources []SourceReport `json:"sources,omitempty"`
	Note    string         `json:"note"`
}

// DiagnosticsResponse is the account-level finding list.
type DiagnosticsResponse struct {
	Meta
	Pubkey   string         `json:"pubkey"`
	Sources  []SourceReport `json:"sources,omitempty"`
	Findings []Finding      `json:"findings"`
}

// CapabilitiesResponse is the evidence record for one endpoint.
type CapabilitiesResponse struct {
	Meta
	RelayURL       string                `json:"relay_url"`
	URLKey         string                `json:"url_key"`
	EndpointType   string                `json:"endpoint_type"`
	AdmissionState string                `json:"admission_state,omitempty"`
	Capabilities   map[string]Capability `json:"capabilities"`
	Privacy        PrivacyReport         `json:"privacy"`
	Propagation    Propagation           `json:"propagation"`
	Note           string                `json:"note"`
}

// PrivacyReport is the effective gift-wrap assessment. Downstream clients use
// these fields instead of parsing observation detail text. A nil boolean is
// unknown. False is a completed check that did not establish the claim.
type PrivacyReport struct {
	Assessment                    string       `json:"assessment"`
	RecipientRetrievalVerified    *bool        `json:"recipient_retrieval_verified"`
	GiftWrapAccepted              *bool        `json:"gift_wrap_accepted"`
	ExposureConfirmed             bool         `json:"exposure_confirmed"`
	ExposureUnresolved            bool         `json:"exposure_unresolved"`
	PrivateObservedAt             *time.Time   `json:"private_observed_at,omitempty"`
	ExposureObservedAt            *time.Time   `json:"exposure_observed_at,omitempty"`
	RecipientAuthVerified         *bool        `json:"recipient_auth_verified"`
	PublicationAuthVerified       *bool        `json:"publication_auth_verified"`
	Stale                         bool         `json:"stale"`
	Confidence                    float64      `json:"confidence,omitempty"`
	Freshness                     string       `json:"freshness,omitempty"`
	EvidenceVersion               int          `json:"evidence_version,omitempty"`
	RecipientExactVerified        *bool        `json:"recipient_exact_verified"`
	RecipientTagVerified          *bool        `json:"recipient_tag_verified"`
	UnauthenticatedExposure       *bool        `json:"unauthenticated_exposure"`
	AuthenticatedObserverExposure *bool        `json:"authenticated_observer_exposure"`
	Comprehensive                 bool         `json:"comprehensive"`
	AccessPaths                   []AccessPath `json:"access_paths,omitempty"`
}

// AccessPath is one gift-wrap subscription outcome. It is stored as capability
// evidence so clients do not parse diagnostic detail strings.
type AccessPath struct {
	Name            string     `json:"name"`
	Status          string     `json:"status"`
	Retrieved       *bool      `json:"retrieved,omitempty"`
	Unauthenticated *bool      `json:"unauthenticated,omitempty"`
	Authenticated   *bool      `json:"authenticated,omitempty"`
	ObservedAt      *time.Time `json:"observed_at,omitempty"`
	Confidence      float64    `json:"confidence,omitempty"`
	Freshness       string     `json:"freshness,omitempty"`
}

// NewService binds the service to a pool. A nil pool reports the schema as not ready.
func NewService(pool *pgxpool.Pool, opts Options) *Service {
	return &Service{
		pool:         pool,
		allowPrivate: opts.AllowPrivateNetwork,
		entries:      map[string]cacheEntry{},
		inflight:     map[string]*flight{},
	}
}

// Roles returns the static catalog.
func (s *Service) Roles() []RoleInfo {
	return RoleCatalog()
}

// Rankings returns the global role list. Blocked relays are counted, not recommended.
func (s *Service) Rankings(ctx context.Context, role string, limit int) (RankingsResponse, error) {
	if !knownRole(role) || role == "private_storage" || role == RoleRelaySet {
		return RankingsResponse{}, invalidf("rankings do not include that role")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > maxRankingLimit {
		limit = maxRankingLimit
	}
	return cached(s, fmt.Sprintf("rank:%s:%d", role, limit), 2*time.Minute, func() (RankingsResponse, error) {
		return s.rankings(ctx, role, limit)
	})
}

// Capabilities reads stored evidence for one URL. It does not dial the relay.
func (s *Service) Capabilities(ctx context.Context, relayURL string) (CapabilitiesResponse, error) {
	prepared, rejected, err := s.normalizeOne(relayURL)
	if err != nil {
		return CapabilitiesResponse{}, err
	}
	if rejected {
		return CapabilitiesResponse{}, invalidf("that relay URL is not dialable from this deployment")
	}
	return cached(s, "cap:"+prepared.Key, 2*time.Minute, func() (CapabilitiesResponse, error) {
		return s.capabilities(ctx, prepared)
	})
}

// Config returns the effective configuration for a public pubkey.
func (s *Service) Config(ctx context.Context, pubkey string) (ConfigResponse, error) {
	pubkey = normalizePubkey(pubkey)
	if !validPubkey(pubkey) {
		return ConfigResponse{}, invalidf("pubkey must be 64 hex characters or an npub")
	}
	return cached(s, "cfg:"+pubkey, 30*time.Second, func() (ConfigResponse, error) {
		account, _, _, reports, err := s.resolveAccount(ctx, pubkey, nil)
		if err != nil {
			return ConfigResponse{}, err
		}
		computed, ready, err := loadSnapshotTime(ctx, s.pool)
		if err != nil {
			return ConfigResponse{}, err
		}
		resp := ConfigResponse{
			Meta:    newMeta(time.Now().UTC()),
			Account: account,
			Sources: reports,
			Note:    "Kind 10013 is not decrypted. A public_safe false URL was published by the account and is excluded from global rankings. An empty modern list is not the same as a missing one, and a kind 3 list is ignored while a kind 10002 exists. sources compares the projection with canonical replaceable state.",
		}
		if ready {
			resp.StatsComputedAt = &computed
		}
		return resp, nil
	})
}

// Diagnostics explains the stored configuration. It does not connect to relays.
func (s *Service) Diagnostics(ctx context.Context, pubkey string) (DiagnosticsResponse, error) {
	pubkey = normalizePubkey(pubkey)
	if !validPubkey(pubkey) {
		return DiagnosticsResponse{}, invalidf("pubkey must be 64 hex characters or an npub")
	}
	return cached(s, "diag:"+pubkey, 30*time.Second, func() (DiagnosticsResponse, error) {
		return s.diagnostics(ctx, pubkey)
	})
}

// Recommendations builds a personalized plan with default constraints.
// Requests that carry caller-supplied relays go through Optimize and are not cached.
func (s *Service) Recommendations(ctx context.Context, pubkey, role, preset string) (PlanResponse, error) {
	if preset == "" {
		preset = PresetBalanced
	}
	var roles []string
	if role != "" {
		roles = []string{role}
	}
	pubkey = normalizePubkey(pubkey)
	return cached(s, "rec:"+pubkey+":"+role+":"+preset, 30*time.Second, func() (PlanResponse, error) {
		return s.plan(ctx, PlanRequest{Pubkey: pubkey, Preset: preset, Roles: roles, IncludeRanking: true})
	})
}

// Optimize applies caller constraints. The result is not cached.
func (s *Service) Optimize(ctx context.Context, req PlanRequest) (PlanResponse, error) {
	if s == nil {
		return PlanResponse{}, ErrNotReady
	}
	req.IncludeRanking = true
	if req.Preset == "" {
		req.Preset = PresetBalanced
	}
	req.Pubkey = strings.ToLower(strings.TrimSpace(req.Pubkey))
	if !validPubkey(req.Pubkey) {
		return PlanResponse{}, invalidf("pubkey must be 64 hex characters or an npub")
	}
	if _, ok := LookupPreset(req.Preset); !ok {
		return PlanResponse{}, invalidf("unknown preset %q", req.Preset)
	}
	if s.pool == nil {
		return PlanResponse{}, ErrNotReady
	}
	return s.plan(ctx, req)
}

// Verify checks a signed event on the relays the client names. It does not publish.
func (s *Service) Verify(ctx context.Context, eventJSON []byte, relays []string) (VerifyResult, error) {
	if s == nil {
		return VerifyResult{}, ErrNotReady
	}
	return VerifyEvent(ctx, eventJSON, relays, s.allowPrivate)
}

func (s *Service) rankings(ctx context.Context, role string, limit int) (RankingsResponse, error) {
	if s.pool == nil {
		return RankingsResponse{}, ErrNotReady
	}
	now := time.Now().UTC()
	computed, ready, err := loadSnapshotTime(ctx, s.pool)
	if err != nil {
		return RankingsResponse{}, err
	}
	resp := RankingsResponse{
		Meta:               newMeta(now),
		Role:               role,
		PositiveAdoption:   role != RoleBlocked,
		CoverageDefinition: coverageDefinition(role),
		Relays:             []ScoredRelay{},
		Note:               "Admission state is independent of this ranking. Declared use is not the same as observed propagation, and propagation is not proof the author published there on purpose.",
	}
	if ready {
		resp.StatsComputedAt = &computed
	} else {
		resp.Fallbacks = []string{"stats_not_ready"}
		metrics.IncRelayIntelFallback("stats_not_ready")
		return resp, nil
	}
	stats, err := loadTopStats(ctx, s.pool, role, maxStatsPerRole)
	if err != nil {
		return RankingsResponse{}, err
	}
	keys := make([]string, 0, len(stats))
	urls := make([]string, 0, len(stats))
	var maxUsers int64
	var maxTrust float64
	for _, stat := range stats {
		keys = append(keys, stat.URLKey)
		urls = append(urls, stat.URL)
		if stat.Users > maxUsers {
			maxUsers = stat.Users
		}
		if stat.Trust > maxTrust {
			maxTrust = stat.Trust
		}
	}
	registry, err := loadRegistry(ctx, s.pool, keys)
	if err != nil {
		return RankingsResponse{}, err
	}
	observations, err := loadObservations(ctx, s.pool, keys)
	if err != nil {
		return RankingsResponse{}, err
	}
	propagation, err := loadPropagation(ctx, s.pool, urls)
	if err != nil {
		return RankingsResponse{}, err
	}
	endpointUsers, err := loadEndpointUsers(ctx, s.pool, keys)
	if err != nil {
		return RankingsResponse{}, err
	}
	weights := DefaultWeights(role)
	for _, stat := range stats {
		reg := registry[stat.URLKey]
		facts := ParseNIP11Summary(reg.Summary)
		relay := ScoreRelay(ScoreInput{
			Role: role, Weights: weights, CohortKnown: false,
			MaxTrust: maxTrust, TrustWeighted: stat.Trust,
			Users: stat.Users, MaxUsers: maxUsers,
			Propagation: propagation[stat.URL],
			Evidence: Evidence{
				Now: now, Probe: probeFrom(reg), NIP11: facts,
				Observations: observations[stat.URLKey], Propagation: propagation[stat.URL],
			},
			PaymentRequired: paymentFlag(facts), AuthRequired: authFlag(facts),
		})
		relay.URL = stat.URL
		relay.URLKey = stat.URLKey
		relay.EndpointType = stat.EndpointType
		relay.OperatorKey = operatorKey(stat.URL, facts)
		relay.Domain = hostOf(stat.URL)
		relay.Users = stat.Users
		relay.ActiveUsers = stat.Active
		relay.ModernUsers = stat.Modern
		relay.LegacyUsers = stat.Legacy
		relay.NetUserDelta7d = stat.Delta
		relay.EndpointUsers = endpointUsers[stat.URLKey]
		relay.AdmissionState = reg.Admission
		if role == RoleBlocked {
			relay.Score = nil
			relay.Eligible = false
			relay.IneligibleReason = "not_a_usage_signal"
			relay.Reasons = append(relay.Reasons, Reason{Code: "not_a_usage_signal", Text: "Accounts listed this relay as blocked. That is not adoption."})
		}
		resp.Relays = append(resp.Relays, relay)
	}
	sort.Slice(resp.Relays, func(i, j int) bool { return relayLess(resp.Relays[i], resp.Relays[j]) })
	resp.CandidatePool = len(resp.Relays)
	if limit > 0 && len(resp.Relays) > limit {
		resp.Relays = resp.Relays[:limit]
	}
	resp.Ranking = "role_score"
	resp.Note += " Ordered by role score after the trust-weighted adoption pool is scored. The pool is capped at 80."
	return resp, nil
}

func (s *Service) capabilities(ctx context.Context, item preparedURL) (CapabilitiesResponse, error) {
	if s.pool == nil {
		return CapabilitiesResponse{}, ErrNotReady
	}
	now := time.Now().UTC()
	registry, err := loadRegistry(ctx, s.pool, []string{item.Key})
	if err != nil {
		return CapabilitiesResponse{}, err
	}
	observations, err := loadObservations(ctx, s.pool, []string{item.Key})
	if err != nil {
		return CapabilitiesResponse{}, err
	}
	propagation, err := loadPropagation(ctx, s.pool, []string{item.URL})
	if err != nil {
		return CapabilitiesResponse{}, err
	}
	reg := registry[item.Key]
	facts := ParseNIP11Summary(reg.Summary)
	endpoint := EndpointNostr
	if strings.HasPrefix(item.URL, "http://") || strings.HasPrefix(item.URL, "https://") {
		endpoint = EndpointBlossom
	}
	resp := CapabilitiesResponse{
		Meta:           newMeta(now),
		RelayURL:       item.URL,
		URLKey:         item.Key,
		EndpointType:   endpoint,
		AdmissionState: reg.Admission,
		Capabilities: ResolveCapabilities(Evidence{
			Now: now, Probe: probeFrom(reg), NIP11: facts,
			Observations: observations[item.Key], Propagation: propagation[item.URL],
		}),
		Propagation: propagation[item.URL],
		Note:        "Advertised NIP support is not verification. EOSE on a read subscription is not write, search, or gift-wrap proof. privacy is the structured assessment; detail strings are diagnostic. No measurement here claims mobile push delivery.",
	}
	resp.Privacy = privacyReport(resp.Capabilities, now)
	computed, ready, err := loadSnapshotTime(ctx, s.pool)
	if err != nil {
		return CapabilitiesResponse{}, err
	}
	if ready {
		resp.StatsComputedAt = &computed
	}
	return resp, nil
}

func (s *Service) diagnostics(ctx context.Context, pubkey string) (DiagnosticsResponse, error) {
	if s.pool == nil {
		return DiagnosticsResponse{}, ErrNotReady
	}
	account, _, _, reports, err := s.resolveAccount(ctx, pubkey, nil)
	if err != nil {
		return DiagnosticsResponse{}, err
	}
	keys := make([]string, 0)
	seen := map[string]struct{}{}
	for _, role := range account.Roles {
		for _, relay := range role.Relays {
			if _, ok := seen[relay.URL]; ok {
				continue
			}
			seen[relay.URL] = struct{}{}
			keys = append(keys, relay.URLKey)
		}
	}
	registry, err := loadRegistry(ctx, s.pool, keys)
	if err != nil {
		return DiagnosticsResponse{}, err
	}
	observations, err := loadObservations(ctx, s.pool, keys)
	if err != nil {
		return DiagnosticsResponse{}, err
	}
	now := time.Now().UTC()
	health := map[string]RelayHealth{}
	for _, role := range account.Roles {
		for _, relay := range role.Relays {
			reg := registry[relay.URLKey]
			facts := ParseNIP11Summary(reg.Summary)
			item := RelayHealth{
				LastProbeAt:  reg.ProbeAt,
				LastStatus:   reg.Status,
				LastEOSEOK:   reg.EOSEOK,
				OperatorKey:  operatorKey(relay.URL, facts),
				Capabilities: ResolveCapabilities(Evidence{Now: now, Probe: probeFrom(reg), NIP11: facts, Observations: observations[relay.URLKey]}),
			}
			if reg.ProbeAt != nil {
				fail := reg.FailRate
				item.FailRate = &fail
			}
			if facts != nil {
				if facts.AuthRequired != nil {
					item.AuthKnown = true
					item.AuthRequired = *facts.AuthRequired
				}
				if facts.PaymentRequired != nil || facts.HasFees {
					item.PaymentKnown = true
					item.PaymentRequired = facts.HasFees || (facts.PaymentRequired != nil && *facts.PaymentRequired)
				}
			}
			health[relay.URL] = item
		}
	}
	resp := DiagnosticsResponse{
		Meta:     newMeta(now),
		Pubkey:   pubkey,
		Sources:  reports,
		Findings: Diagnose(account, health, now),
	}
	computed, ready, err := loadSnapshotTime(ctx, s.pool)
	if err != nil {
		return DiagnosticsResponse{}, err
	}
	if ready {
		resp.StatsComputedAt = &computed
	} else {
		resp.Fallbacks = []string{"stats_not_ready"}
	}
	if resp.Findings == nil {
		resp.Findings = []Finding{}
	}
	return resp, nil
}

func normalizePubkey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func cached[T any](s *Service, key string, ttl time.Duration, fn func() (T, error)) (T, error) {
	var zero T
	if s == nil || s.pool == nil {
		return zero, ErrNotReady
	}
	s.mu.Lock()
	if ent, ok := s.entries[key]; ok && time.Now().Before(ent.expires) {
		s.mu.Unlock()
		metrics.IncRelayIntelCache("hit")
		value, ok := ent.body.(T)
		if !ok {
			return zero, fmt.Errorf("relay intelligence cache entry has the wrong type")
		}
		return value, nil
	}
	if existing, ok := s.inflight[key]; ok {
		s.mu.Unlock()
		<-existing.done
		if existing.err != nil {
			var zero T
			return zero, existing.err
		}
		value, ok := existing.body.(T)
		if !ok {
			return zero, fmt.Errorf("relay intelligence cache entry has the wrong type")
		}
		return value, nil
	}
	job := &flight{done: make(chan struct{})}
	s.inflight[key] = job
	s.mu.Unlock()
	metrics.IncRelayIntelCache("miss")
	value, err := fn()
	s.mu.Lock()
	job.body = value
	job.err = err
	if err == nil {
		s.entries[key] = cacheEntry{expires: time.Now().Add(ttl), body: value}
		for len(s.entries) > 192 {
			for old := range s.entries {
				delete(s.entries, old)
				break
			}
		}
	}
	delete(s.inflight, key)
	close(job.done)
	s.mu.Unlock()
	return value, err
}
