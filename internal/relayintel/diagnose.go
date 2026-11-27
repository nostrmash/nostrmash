package relayintel

import (
	"net/url"
	"strings"
	"time"
)

const (
	SeverityCritical   = "critical"
	SeverityWarning    = "warning"
	SeveritySuggestion = "suggestion"
	SeverityInfo       = "info"
)

// Finding is one diagnostic. Codes are stable. Text is for humans.
type Finding struct {
	Code       string     `json:"code"`
	Severity   string     `json:"severity"`
	Role       string     `json:"role,omitempty"`
	RelayURL   string     `json:"relay_url,omitempty"`
	Text       string     `json:"text"`
	ObservedAt *time.Time `json:"observed_at,omitempty"`
}

// RelayHealth is the stored measurement bundle for diagnostics.
type RelayHealth struct {
	FailRate        *float64
	LastProbeAt     *time.Time
	LastStatus      string
	LastEOSEOK      *bool
	AuthKnown       bool
	AuthRequired    bool
	PaymentKnown    bool
	PaymentRequired bool
	OperatorKey     string
	Capabilities    map[string]Capability
}

// Diagnose inspects one effective configuration. A single failed probe does
// not become a removal. Sustained failure is a warning to investigate.
func Diagnose(cfg AccountConfig, health map[string]RelayHealth, now time.Time) []Finding {
	if now.IsZero() {
		now = time.Now()
	}
	var out []Finding
	if cfg.ModernPublic == AvailabilityMissing && cfg.LegacyPublic == AvailabilityPresent {
		out = append(out, Finding{
			Code: "obsolete_legacy_config", Severity: SeverityWarning,
			Text: "This account has a legacy kind 3 relay map and no kind 10002 list. Clients that speak NIP-65 will not see the legacy map.",
		})
	}
	if cfg.ModernPublic == AvailabilityMissing && cfg.LegacyPublic == AvailabilityMissing {
		out = append(out, Finding{
			Code: "missing_modern_config", Severity: SeverityWarning,
			Text: "No kind 10002 relay list is stored. Recommendations will use global data.",
		})
	}
	if cfg.ModernPublic == AvailabilityEmpty {
		out = append(out, Finding{
			Code: "explicit_empty_config", Severity: SeverityInfo,
			Text: "The newest kind 10002 list is explicitly empty. The older kind 3 map is not mixed back in.",
		})
	}
	if cfg.LegacyPublic == AvailabilityPresent && cfg.ModernPublic != AvailabilityMissing {
		out = append(out, Finding{
			Code: "obsolete_legacy_config", Severity: SeveritySuggestion,
			Text: "A legacy kind 3 relay map is still stored beside a kind 10002 list. Optimization will not rewrite kind 3.",
		})
	}
	blocked := map[string]struct{}{}
	if role, ok := cfg.RoleByName(RoleBlocked); ok {
		for _, relay := range role.Relays {
			blocked[relay.URL] = struct{}{}
		}
	}
	for _, role := range cfg.Roles {
		if role.Role == RoleBlocked || role.Role == "private_storage" || role.Role == RoleKeyPackage {
			continue
		}
		out = append(out, diagnoseRole(role, blocked, health, now)...)
	}
	if role, ok := cfg.RoleByName("private_storage"); ok && role.Availability == AvailabilityClientSide {
		out = append(out, Finding{
			Code: "private_config_client_side", Severity: SeverityInfo, Role: "private_storage",
			Text: "A kind 10013 private relay list exists. Its contents are not decrypted or recommended.",
		})
	}
	if role, ok := cfg.RoleByName(RoleKeyPackage); ok {
		if cfg.KeyPackageDiscovery == "kind_10051_legacy" {
			out = append(out, Finding{
				Code: "legacy_keypackage_list", Severity: SeverityInfo, Role: RoleKeyPackage,
				Text: "This account still publishes kind 10051. Current Marmot clients look for KeyPackages on the NIP-65 outbox instead.",
			})
		}
		if role.Availability == AvailabilityPresent {
			out = append(out, Finding{
				Code: "keypackage_migration_required", Severity: SeverityInfo, Role: RoleKeyPackage,
				Text: "Replacing a KeyPackage relay requires the client to publish a fresh KeyPackage before peers will find it. This service does not do that.",
			})
		}
	}
	if role, ok := cfg.RoleByName(RoleDMInbox); ok && role.Availability == AvailabilityPresent {
		out = append(out, Finding{
			Code: "dm_history_not_migrated", Severity: SeverityInfo, Role: RoleDMInbox,
			Text: "Changing DM relays does not move historical messages. Peers learn the new inbox only after they fetch the new kind 10050 event.",
		})
	}
	return dedupeFindings(out)
}

func diagnoseRole(role RoleConfig, blocked map[string]struct{}, health map[string]RelayHealth, now time.Time) []Finding {
	var out []Finding
	if role.Availability == AvailabilityMissing {
		return out
	}
	min, max := DefaultCountRange(role.Role)
	if role.Availability == AvailabilityEmpty {
		out = append(out, Finding{
			Code: "explicit_empty_config", Severity: SeverityInfo, Role: role.Role,
			Text: "This role's newest list is explicitly empty.",
		})
		return out
	}
	if len(role.Relays) > max+2 {
		out = append(out, Finding{
			Code: "excessive_relay_count", Severity: SeverityWarning, Role: role.Role,
			Text: "This list is larger than the usual range. Extra relays add connection cost and are not removed automatically.",
		})
	}
	if len(role.Relays) < min && role.Role != RoleBlossom {
		out = append(out, Finding{
			Code: "insufficient_redundancy", Severity: SeverityWarning, Role: role.Role,
			Text: "This list is below the usual redundancy floor. A single outage would remove the role.",
		})
	}
	seen := map[string]struct{}{}
	operators := map[string]int{}
	for _, relay := range role.Relays {
		if _, ok := seen[relay.URL]; ok {
			out = append(out, Finding{Code: "duplicate_url", Severity: SeverityWarning, Role: role.Role, RelayURL: relay.URL, Text: "The same endpoint appears more than once."})
		}
		seen[relay.URL] = struct{}{}
		if _, blocked := blocked[relay.URL]; blocked && role.Role != RoleBlocked {
			out = append(out, Finding{Code: "blocked_relay_in_active_config", Severity: SeverityCritical, Role: role.Role, RelayURL: relay.URL, Text: "This endpoint is on the account's blocked-relay list and also in an active list."})
		}
		if !relay.PublicSafe {
			out = append(out, Finding{Code: "private_endpoint", Severity: SeverityInfo, Role: role.Role, RelayURL: relay.URL, Text: "This endpoint is not a public address. It is kept because the account declared it, and it is excluded from global rankings."})
		}
		op := operatorOf(relay.URL, health[relay.URL])
		operators[op]++
		out = append(out, diagnoseHealth(role.Role, relay.URL, health[relay.URL], now)...)
	}
	if len(role.Relays) >= 2 {
		maxShare := 0
		for _, count := range operators {
			if count > maxShare {
				maxShare = count
			}
		}
		if float64(maxShare)/float64(len(role.Relays)) > 0.5 {
			out = append(out, Finding{
				Code: "infrastructure_concentration", Severity: SeverityWarning, Role: role.Role,
				Text: "More than half of this list shares a hostname or a NIP-11 operator pubkey. That is a concentration signal, not proof of shared ownership.",
			})
		}
	}
	if role.Role == RoleDMInbox {
		for _, relay := range role.Relays {
			wrap := health[relay.URL].Capabilities[CapGiftWrap]
			if wrap.Status == StatusUnsupported {
				out = append(out, Finding{Code: "dm_privacy_unsuitable", Severity: SeverityWarning, Role: role.Role, RelayURL: relay.URL, Text: "Gift-wrap acceptance looked unsuccessful. Advertising NIP-42 would not, by itself, mean the same thing."})
			} else if wrap.Status != StatusVerified {
				out = append(out, Finding{Code: "dm_privacy_unverified", Severity: SeveritySuggestion, Role: role.Role, RelayURL: relay.URL, Text: "Recipient-only DM storage has not been verified. An authentication requirement is not treated as a failure."})
			}
		}
	}
	return out
}

func diagnoseHealth(role, relayURL string, health RelayHealth, now time.Time) []Finding {
	if health.LastProbeAt == nil {
		return []Finding{{Code: "stale_health", Severity: SeverityInfo, Role: role, RelayURL: relayURL, Text: "No read probe is stored. Absence of a probe is not treated as an outage."}}
	}
	var out []Finding
	age := now.Sub(*health.LastProbeAt)
	if age > ProbeStaleFor {
		out = append(out, Finding{Code: "stale_health", Severity: SeverityInfo, Role: role, RelayURL: relayURL, ObservedAt: health.LastProbeAt, Text: "The last read probe is older than seven days."})
		return out
	}
	fail := 0.0
	if health.FailRate != nil {
		fail = *health.FailRate
	}
	down := health.LastEOSEOK != nil && !*health.LastEOSEOK && (health.LastStatus == "connect_failed" || health.LastStatus == "eose_timeout")
	if fail >= 0.8 && down {
		out = append(out, Finding{Code: "repeated_connection_failures", Severity: SeverityWarning, Role: role, RelayURL: relayURL, ObservedAt: health.LastProbeAt, Text: "Read probes have been failing, and the latest attempt did not finish. This is not yet a removal. A short outage should be watched first."})
	} else if down {
		out = append(out, Finding{Code: "unreachable", Severity: SeverityInfo, Role: role, RelayURL: relayURL, ObservedAt: health.LastProbeAt, Text: "The latest read probe did not finish. One failure does not change the recommendation."})
	}
	return out
}

func operatorOf(relayURL string, health RelayHealth) string {
	if health.OperatorKey != "" {
		return health.OperatorKey
	}
	parsed, err := url.Parse(relayURL)
	if err != nil {
		return relayURL
	}
	return "host:" + strings.ToLower(parsed.Hostname())
}

func dedupeFindings(in []Finding) []Finding {
	seen := map[string]struct{}{}
	out := make([]Finding, 0, len(in))
	for _, finding := range in {
		key := finding.Code + "|" + finding.Role + "|" + finding.RelayURL
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, finding)
	}
	return out
}
