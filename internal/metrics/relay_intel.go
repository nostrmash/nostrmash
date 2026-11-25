package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	relayIntelRequests     *prometheus.CounterVec
	relayIntelDuration     *prometheus.HistogramVec
	relayIntelCache        *prometheus.CounterVec
	relayIntelProjections  *prometheus.CounterVec
	relayIntelDeclarers    *prometheus.GaugeVec
	relayIntelStatsAge     prometheus.Gauge
	relayIntelBackfill     *prometheus.CounterVec
	relayIntelBackfillDone *prometheus.GaugeVec
	relayIntelCapabilities *prometheus.CounterVec
	relayIntelProbes       *prometheus.CounterVec
	relayIntelFallbacks    *prometheus.CounterVec
)

func registerRelayIntelMetrics() {
	relayIntelRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_intel_requests_total",
			Help: "Relay intelligence API requests by endpoint and outcome.",
		},
		[]string{"endpoint", "outcome"},
	)
	relayIntelDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_relay_intel_request_duration_seconds",
			Help:    "Relay intelligence API latency by endpoint.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"endpoint"},
	)
	relayIntelCache = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_intel_cache_lookups_total",
			Help: "In-process relay intelligence cache lookups.",
		},
		[]string{"result"},
	)
	relayIntelProjections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_intel_projection_events_total",
			Help: "Role projection attempts by bounded kind label and outcome.",
		},
		[]string{"kind", "outcome"},
	)
	relayIntelDeclarers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_relay_intel_declarers",
			Help: "Distinct public-safe pubkeys declaring a fixed relay role.",
		},
		[]string{"role"},
	)
	relayIntelStatsAge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_relay_intel_stats_age_seconds",
			Help: "Age of the latest role-stat snapshot. Zero just after a successful refresh.",
		},
	)
	relayIntelBackfillDone = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_relay_intel_backfill_done",
			Help: "1 when the stored-event backfill for a fixed kind has finished.",
		},
		[]string{"kind"},
	)
	relayIntelBackfill = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_intel_backfill_events_total",
			Help: "Stored events visited by the relay-role backfill.",
		},
		[]string{"outcome"},
	)
	relayIntelCapabilities = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_intel_capability_observations_total",
			Help: "Stored capability observations by capability and status.",
		},
		[]string{"capability", "status"},
	)
	relayIntelProbes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_intel_probe_total",
			Help: "Active capability probes by class and outcome. Disabled unless configured.",
		},
		[]string{"class", "outcome"},
	)
	relayIntelFallbacks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_intel_fallbacks_total",
			Help: "Recommendation requests that used a fallback or had thin evidence.",
		},
		[]string{"reason"},
	)
	registry.MustRegister(
		relayIntelRequests,
		relayIntelDuration,
		relayIntelCache,
		relayIntelProjections,
		relayIntelDeclarers,
		relayIntelStatsAge,
		relayIntelBackfill,
		relayIntelBackfillDone,
		relayIntelCapabilities,
		relayIntelProbes,
		relayIntelFallbacks,
	)
}

func ObserveRelayIntelRequest(endpoint, outcome string, seconds float64) {
	ensureRegistered()
	relayIntelRequests.WithLabelValues(boundRelayIntelEndpoint(endpoint), boundRelayIntelOutcome(outcome)).Inc()
	relayIntelDuration.WithLabelValues(boundRelayIntelEndpoint(endpoint)).Observe(seconds)
}

func IncRelayIntelCache(result string) {
	ensureRegistered()
	if result != "hit" && result != "miss" {
		result = "miss"
	}
	relayIntelCache.WithLabelValues(result).Inc()
}

func IncRelayIntelProjection(kind int, outcome string) {
	ensureRegistered()
	relayIntelProjections.WithLabelValues(boundRelayIntelKind(kind), boundRelayIntelOutcome(outcome)).Inc()
}

func SetRelayIntelDeclarers(role string, count float64) {
	ensureRegistered()
	relayIntelDeclarers.WithLabelValues(boundRelayIntelRole(role)).Set(count)
}

func SetRelayIntelStatsAge(seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	relayIntelStatsAge.Set(seconds)
}

func SetRelayIntelBackfillDone(kind int, done float64) {
	ensureRegistered()
	relayIntelBackfillDone.WithLabelValues(boundRelayIntelKind(kind)).Set(done)
}

func IncRelayIntelBackfill(outcome string) {
	ensureRegistered()
	relayIntelBackfill.WithLabelValues(boundRelayIntelOutcome(outcome)).Inc()
}

func IncRelayIntelCapability(capability, status string) {
	ensureRegistered()
	relayIntelCapabilities.WithLabelValues(boundToken(capability), boundToken(status)).Inc()
}

func IncRelayIntelProbe(class, outcome string) {
	ensureRegistered()
	relayIntelProbes.WithLabelValues(boundToken(class), boundRelayIntelOutcome(outcome)).Inc()
}

func IncRelayIntelFallback(reason string) {
	ensureRegistered()
	relayIntelFallbacks.WithLabelValues(boundToken(reason)).Inc()
}

func boundRelayIntelEndpoint(endpoint string) string {
	switch endpoint {
	case "roles", "rankings", "capabilities", "config", "diagnostics", "recommendations", "optimize", "verify":
		return endpoint
	default:
		return "other"
	}
}

func boundRelayIntelOutcome(outcome string) string {
	switch outcome {
	case "ok", "error", "empty", "fallback", "insufficient_evidence", "skipped", "applied", "malformed", "disabled", "timeout", "unsupported", "rejected":
		return outcome
	default:
		return "other"
	}
}

func boundRelayIntelKind(kind int) string {
	switch kind {
	case 3, 10002, 10006, 10007, 10012, 10013, 10050, 10051, 10063, 10102, 30002:
		return itoa(kind)
	default:
		return "other"
	}
}

func boundRelayIntelRole(role string) string {
	switch role {
	case "public_inbox", "public_outbox", "dm_inbox", "search", "blocked", "favorite",
		"relay_set", "keypackage", "blossom", "wiki":
		return role
	default:
		return "other"
	}
}

func boundToken(value string) string {
	switch value {
	case "hit", "miss", "ok", "error", "timeout", "unsupported", "verified", "observed",
		"advertised", "unknown", "stale", "restricted", "connect", "read_subscription",
		"write_accept", "write_retrieve", "search", "auth", "payment", "giftwrap",
		"retention", "nip11", "write", "global_fallback", "insufficient_evidence",
		"cohort_only", "stats_not_ready", "private_rejected":
		return value
	default:
		return "other"
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var buf [16]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	if negative {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
