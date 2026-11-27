package relayintel

import (
	"encoding/json"
	"strings"
	"time"
)

const (
	CapConnect                = "connect"
	CapRead                   = "read_subscription"
	CapWriteAccept            = "write_accept"
	CapWriteRetrieve          = "write_retrieve"
	CapSearch                 = "search"
	CapAuth                   = "auth"
	CapPayment                = "payment"
	CapGiftWrap               = "giftwrap"
	CapGiftWrapAccept         = "giftwrap_accept"
	CapGiftWrapPrivate        = "giftwrap_private"
	CapGiftWrapPublic         = "giftwrap_public"
	CapAuthWrite              = "auth_write"
	CapAuthSearch             = "auth_search"
	CapAuthGiftWrap           = "auth_giftwrap"
	CapAuthRecipient          = "auth_recipient"
	CapGiftWrapRecipient      = "giftwrap_recipient_retrieval"
	CapGiftWrapRecipientExact = "giftwrap_recipient_exact"
	CapGiftWrapRecipientTag   = "giftwrap_recipient_tag"
	CapGiftWrapObserverExact  = "giftwrap_observer_exact"
	CapGiftWrapObserverTag    = "giftwrap_observer_tag"
	CapGiftWrapAnonExact      = "giftwrap_anon_exact"
	CapGiftWrapAnonTag        = "giftwrap_anon_tag"
	CapGiftWrapUnauthExposure = "giftwrap_unauth_exposure"
	CapGiftWrapAuthExposure   = "giftwrap_auth_exposure"
	CapKeyPackage             = "keypackage"
	CapRetention              = "retention"
	CapNIP11                  = "nip11"

	StatusAdvertised  = "advertised"
	StatusVerified    = "verified"
	StatusObserved    = "observed"
	StatusUnsupported = "unsupported"
	StatusUnknown     = "unknown"
	StatusStale       = "stale"
	StatusRestricted  = "restricted"

	// ComprehensivePrivacyVersion marks a six-path recipient-only result.
	// Observations stored before that contract do not carry this version.
	ComprehensivePrivacyVersion = 2
)

// Capability is one claim about a relay, with the evidence class attached.
type Capability struct {
	Name               string     `json:"name"`
	Status             string     `json:"status"`
	Confidence         float64    `json:"confidence"`
	Source             string     `json:"source"`
	ObservedAt         *time.Time `json:"observed_at,omitempty"`
	SampleCount        int        `json:"sample_count,omitempty"`
	RecentFailures     int        `json:"recent_failures,omitempty"`
	RecentSuccesses    int        `json:"recent_successes,omitempty"`
	RecentInconclusive int        `json:"recent_inconclusive,omitempty"`
	Detail             string     `json:"detail,omitempty"`
	EvidenceVersion    int        `json:"evidence_version,omitempty"`
	Freshness          string     `json:"freshness,omitempty"`
	Retrieved          *bool      `json:"retrieved,omitempty"`
	Unauthenticated    *bool      `json:"unauthenticated,omitempty"`
	Authenticated      *bool      `json:"authenticated,omitempty"`
}

// Observation is a stored measurement. It is not a user declaration.
type Observation struct {
	URLKey             string
	Capability         string
	Status             string
	Confidence         float64
	Source             string
	ObservedAt         time.Time
	SampleCount        int
	RecentFailures     int
	RecentSuccesses    int
	RecentInconclusive int
	RecentWindow       int
	Detail             string
	EvidenceVersion    int
	Retrieved          *bool
	Unauthenticated    *bool
	Authenticated      *bool
}

// ProbeSnapshot is the existing read-probe rollup. EOSE success is not write,
// search, or DM proof.
type ProbeSnapshot struct {
	At          *time.Time
	Status      string
	ConnectOK   *bool
	SubscribeOK *bool
	EOSEOK      *bool
	ConnectMs   *float64
	EOSEMs      *float64
	FailRate    float64
	HasFailRate bool
}

// NIP11Facts is the whitelisted document the registry already stored.
type NIP11Facts struct {
	OK               bool
	FetchedAt        time.Time
	Pubkey           string
	Software         string
	SupportedNIPs    []int
	AuthRequired     *bool
	PaymentRequired  *bool
	RestrictedWrites *bool
	HasFees          bool
	HasRetention     bool
}

// Evidence is everything the scorer may use for one relay.
type Evidence struct {
	Now          time.Time
	Probe        ProbeSnapshot
	NIP11        *NIP11Facts
	Observations []Observation
	Propagation  Propagation
}

// Propagation is observed public activity on a relay. It does not prove that
// the authors published there on purpose.
type Propagation struct {
	EventsSeen7d int64
	FirstSeen7d  int64
	Known        bool
}

// ParseNIP11Summary reads relay_registry.capability_summary_json.
func ParseNIP11Summary(raw []byte) *NIP11Facts {
	if len(raw) == 0 {
		return nil
	}
	var doc struct {
		OK        bool      `json:"ok"`
		FetchedAt time.Time `json:"fetched_at"`
		Info      *struct {
			Pubkey        string `json:"pubkey"`
			Software      string `json:"software"`
			SupportedNIPs []int  `json:"supported_nips"`
			Limitation    *struct {
				AuthRequired     *bool `json:"auth_required"`
				PaymentRequired  *bool `json:"payment_required"`
				RestrictedWrites *bool `json:"restricted_writes"`
			} `json:"limitation"`
			Fees *struct {
				Admission    []json.RawMessage `json:"admission"`
				Subscription []json.RawMessage `json:"subscription"`
				Publication  []json.RawMessage `json:"publication"`
			} `json:"fees"`
			Retention []json.RawMessage `json:"retention"`
		} `json:"info"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil
	}
	facts := &NIP11Facts{OK: doc.OK, FetchedAt: doc.FetchedAt}
	if doc.Info == nil {
		return facts
	}
	facts.Pubkey = strings.TrimSpace(doc.Info.Pubkey)
	facts.Software = strings.TrimSpace(doc.Info.Software)
	facts.SupportedNIPs = append([]int(nil), doc.Info.SupportedNIPs...)
	if doc.Info.Limitation != nil {
		facts.AuthRequired = doc.Info.Limitation.AuthRequired
		facts.PaymentRequired = doc.Info.Limitation.PaymentRequired
		facts.RestrictedWrites = doc.Info.Limitation.RestrictedWrites
	}
	if doc.Info.Fees != nil && (len(doc.Info.Fees.Admission) > 0 || len(doc.Info.Fees.Subscription) > 0 || len(doc.Info.Fees.Publication) > 0) {
		facts.HasFees = true
	}
	facts.HasRetention = len(doc.Info.Retention) > 0
	return facts
}

func nipSupports(facts *NIP11Facts, nip int) bool {
	if facts == nil {
		return false
	}
	for _, item := range facts.SupportedNIPs {
		if item == nip {
			return true
		}
	}
	return false
}

// ResolveCapabilities merges advertisements, probes, and active observations.
// An advertisement never becomes a verification.
func ResolveCapabilities(ev Evidence) map[string]Capability {
	now := ev.Now
	if now.IsZero() {
		now = time.Now()
	}
	out := map[string]Capability{
		CapConnect:                {Name: CapConnect, Status: StatusUnknown, Confidence: 0.2, Source: "none"},
		CapRead:                   {Name: CapRead, Status: StatusUnknown, Confidence: 0.2, Source: "none"},
		CapWriteAccept:            {Name: CapWriteAccept, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapWriteRetrieve:          {Name: CapWriteRetrieve, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapSearch:                 {Name: CapSearch, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapAuth:                   {Name: CapAuth, Status: StatusUnknown, Confidence: 0.2, Source: "none"},
		CapPayment:                {Name: CapPayment, Status: StatusUnknown, Confidence: 0.2, Source: "none"},
		CapGiftWrap:               {Name: CapGiftWrap, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapAccept:         {Name: CapGiftWrapAccept, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapPrivate:        {Name: CapGiftWrapPrivate, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapPublic:         {Name: CapGiftWrapPublic, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapAuthWrite:              {Name: CapAuthWrite, Status: StatusUnknown, Confidence: 0.2, Source: "none"},
		CapAuthSearch:             {Name: CapAuthSearch, Status: StatusUnknown, Confidence: 0.2, Source: "none"},
		CapAuthGiftWrap:           {Name: CapAuthGiftWrap, Status: StatusUnknown, Confidence: 0.2, Source: "none"},
		CapAuthRecipient:          {Name: CapAuthRecipient, Status: StatusUnknown, Confidence: 0.2, Source: "none"},
		CapGiftWrapRecipient:      {Name: CapGiftWrapRecipient, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapRecipientExact: {Name: CapGiftWrapRecipientExact, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapRecipientTag:   {Name: CapGiftWrapRecipientTag, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapObserverExact:  {Name: CapGiftWrapObserverExact, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapObserverTag:    {Name: CapGiftWrapObserverTag, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapAnonExact:      {Name: CapGiftWrapAnonExact, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapAnonTag:        {Name: CapGiftWrapAnonTag, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapUnauthExposure: {Name: CapGiftWrapUnauthExposure, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapGiftWrapAuthExposure:   {Name: CapGiftWrapAuthExposure, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapKeyPackage:             {Name: CapKeyPackage, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapRetention:              {Name: CapRetention, Status: StatusUnknown, Confidence: 0.15, Source: "none"},
		CapNIP11:                  {Name: CapNIP11, Status: StatusUnknown, Confidence: 0.2, Source: "none"},
	}
	if ev.Probe.At != nil {
		age := now.Sub(*ev.Probe.At)
		stale := age > ProbeStaleFor
		conf := 0.8
		if ev.Probe.HasFailRate {
			conf = clamp01(1 - ev.Probe.FailRate)
		}
		if age > ProbeFreshFor {
			conf *= 0.7
		}
		status := StatusObserved
		if stale {
			status = StatusStale
			conf = min(conf, 0.35)
		}
		if ev.Probe.ConnectOK != nil {
			cap := out[CapConnect]
			cap.ObservedAt = ev.Probe.At
			cap.Source = "read_probe"
			cap.Confidence = round3(conf)
			if *ev.Probe.ConnectOK {
				cap.Status = status
			} else if !stale {
				cap.Status = StatusUnsupported
				cap.Confidence = round3(clamp01(ev.Probe.FailRate))
				if cap.Confidence < 0.5 {
					cap.Confidence = 0.7
				}
			} else {
				cap.Status = StatusStale
			}
			out[CapConnect] = cap
		}
		if ev.Probe.EOSEOK != nil && ev.Probe.SubscribeOK != nil && *ev.Probe.SubscribeOK {
			cap := out[CapRead]
			cap.ObservedAt = ev.Probe.At
			cap.Source = "read_probe"
			cap.Confidence = round3(conf * 0.85)
			cap.Detail = "kind 0 subscription reached EOSE or failed; this is not write, search, or DM proof"
			if *ev.Probe.EOSEOK {
				cap.Status = status
			} else if !stale {
				cap.Status = StatusUnsupported
			} else {
				cap.Status = StatusStale
			}
			out[CapRead] = cap
		}
	}
	if ev.NIP11 != nil {
		facts := ev.NIP11
		cap := out[CapNIP11]
		cap.Source = "nip11"
		if !facts.FetchedAt.IsZero() {
			fetched := facts.FetchedAt
			cap.ObservedAt = &fetched
		}
		if facts.OK {
			cap.Status = StatusAdvertised
			cap.Confidence = 0.7
			if !facts.FetchedAt.IsZero() && now.Sub(facts.FetchedAt) > NIP11StaleFor {
				cap.Status = StatusStale
				cap.Confidence = 0.35
			}
		} else if !facts.FetchedAt.IsZero() {
			cap.Status = StatusUnknown
			cap.Confidence = 0.3
			cap.Detail = "last NIP-11 fetch failed"
		}
		out[CapNIP11] = cap
		if facts.AuthRequired != nil {
			auth := out[CapAuth]
			auth.Source = "nip11"
			auth.Status = StatusAdvertised
			auth.Confidence = 0.55
			auth.Detail = "NIP-11 auth_required is not specific to reading or writing"
			if *facts.AuthRequired {
				auth.Status = StatusRestricted
				auth.Confidence = 0.6
			}
			out[CapAuth] = auth
		}
		if facts.PaymentRequired != nil || facts.HasFees {
			pay := out[CapPayment]
			pay.Source = "nip11"
			pay.Status = StatusAdvertised
			pay.Confidence = 0.55
			if (facts.PaymentRequired != nil && *facts.PaymentRequired) || facts.HasFees {
				pay.Status = StatusRestricted
				pay.Confidence = 0.6
			}
			out[CapPayment] = pay
		}
		if nipSupports(facts, 50) {
			search := out[CapSearch]
			search.Status = StatusAdvertised
			search.Source = "nip11"
			search.Confidence = 0.4
			search.Detail = "NIP-50 is advertised and has not been verified"
			out[CapSearch] = search
		}
		if nipSupports(facts, 17) || nipSupports(facts, 59) {
			wrap := out[CapGiftWrap]
			wrap.Status = StatusAdvertised
			wrap.Source = "nip11"
			wrap.Confidence = 0.3
			wrap.Detail = "advertising NIP-17 or NIP-59 does not establish recipient-only access"
			out[CapGiftWrap] = wrap
		}
		if facts.HasRetention {
			ret := out[CapRetention]
			ret.Status = StatusAdvertised
			ret.Source = "nip11"
			ret.Confidence = 0.45
			out[CapRetention] = ret
		}
		if facts.RestrictedWrites != nil && *facts.RestrictedWrites {
			write := out[CapWriteAccept]
			if write.Status == StatusUnknown {
				write.Status = StatusRestricted
				write.Source = "nip11"
				write.Confidence = 0.45
				write.Detail = "NIP-11 restricted_writes"
				out[CapWriteAccept] = write
			}
		}
	}
	for _, obs := range ev.Observations {
		if obs.Capability == "" {
			continue
		}
		current := out[obs.Capability]
		if current.Name == "" {
			current.Name = obs.Capability
		}
		if !outranks(obs.Status, current.Status) && current.Status != StatusUnknown && current.Status != StatusAdvertised {
			continue
		}
		if obs.Status == StatusVerified || obs.Status == StatusObserved || obs.Status == StatusUnsupported || obs.Status == StatusRestricted {
			at := obs.ObservedAt
			current.ObservedAt = &at
			current.Status = obs.Status
			current.Confidence = round3(clamp01(obs.Confidence))
			current.Source = obs.Source
			current.SampleCount = obs.SampleCount
			current.RecentFailures = obs.RecentFailures
			current.RecentSuccesses = obs.RecentSuccesses
			current.RecentInconclusive = obs.RecentInconclusive
			current.EvidenceVersion = obs.EvidenceVersion
			current.Retrieved = obs.Retrieved
			current.Unauthenticated = obs.Unauthenticated
			current.Authenticated = obs.Authenticated
			current.Freshness = freshnessLabel(obs.ObservedAt, now)
			window := obs.SampleCount
			if obs.RecentWindow > 0 {
				window = obs.RecentWindow
			}
			current.Status, current.Confidence = adjustForRecentFailures(current.Status, current.Confidence, obs.RecentFailures, window)
			if obs.Detail != "" {
				current.Detail = obs.Detail
			}
			if !obs.ObservedAt.IsZero() && now.Sub(obs.ObservedAt) > ProbeStaleFor {
				current.Status = StatusStale
				current.Confidence = min(current.Confidence, 0.35)
				current.Freshness = "stale"
			}
			out[obs.Capability] = current
		}
	}
	return out
}

// adjustForRecentFailures lowers confidence when recent attempts failed.
// The conclusive status stays. A later timeout does not erase a verified result.
func adjustForRecentFailures(status string, confidence float64, failures, samples int) (string, float64) {
	if samples < 2 || failures <= 0 || status != StatusVerified {
		return status, confidence
	}
	if failures*2 >= samples {
		return status, min(confidence, 0.45)
	}
	return status, min(confidence, 0.6)
}

func freshnessLabel(at, now time.Time) string {
	if at.IsZero() || now.IsZero() {
		return "unknown"
	}
	age := now.Sub(at)
	switch {
	case age <= ProbeFreshFor:
		return "fresh"
	case age <= ProbeStaleFor:
		return "aging"
	default:
		return "stale"
	}
}

func outranks(next, current string) bool {
	rank := func(status string) int {
		switch status {
		case StatusVerified:
			return 6
		case StatusObserved:
			return 5
		case StatusUnsupported, StatusRestricted:
			return 4
		case StatusStale:
			return 3
		case StatusAdvertised:
			return 2
		default:
			return 1
		}
	}
	return rank(next) >= rank(current)
}

func clamp01(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func round3(value float64) float64 {
	return float64(int(value*1000+0.5)) / 1000
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
