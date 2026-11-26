package relayintel

import "sort"

// DeclaredRelay is one endpoint in the configuration a client should display.
type DeclaredRelay struct {
	URL           string `json:"url"`
	URLKey        string `json:"url_key"`
	EndpointType  string `json:"endpoint_type"`
	Marker        string `json:"marker,omitempty"`
	PublicSafe    bool   `json:"public_safe"`
	SourceKind    int    `json:"source_kind"`
	SourceEventID string `json:"source_event_id"`
	CreatedAt     int64  `json:"created_at"`
	Declaration   string `json:"declaration_source"`
	DTag          string `json:"d_tag,omitempty"`
}

// RoleConfig is the effective list for one role.
type RoleConfig struct {
	Role         string          `json:"role"`
	Availability string          `json:"availability"`
	Source       string          `json:"source,omitempty"`
	EventID      string          `json:"event_id,omitempty"`
	CreatedAt    int64           `json:"created_at,omitempty"`
	DTag         string          `json:"d_tag,omitempty"`
	Title        string          `json:"title,omitempty"`
	Relays       []DeclaredRelay `json:"relays"`
	InvalidCount int             `json:"invalid_endpoint_count,omitempty"`
	Truncated    bool            `json:"truncated,omitempty"`
}

// RelaySetConfig is one addressable kind-30002 set.
type RelaySetConfig struct {
	DTag         string          `json:"d_tag"`
	Title        string          `json:"title,omitempty"`
	Availability string          `json:"availability"`
	EventID      string          `json:"event_id,omitempty"`
	CreatedAt    int64           `json:"created_at,omitempty"`
	Relays       []DeclaredRelay `json:"relays"`
}

// AccountConfig is the effective public configuration for one pubkey.
type AccountConfig struct {
	Pubkey              string           `json:"pubkey"`
	Roles               []RoleConfig     `json:"roles"`
	RelaySets           []RelaySetConfig `json:"relay_sets"`
	ModernPublic        string           `json:"modern_public"`
	LegacyPublic        string           `json:"legacy_public"`
	ModernEventID       string           `json:"modern_event_id,omitempty"`
	ModernCreatedAt     int64            `json:"modern_created_at,omitempty"`
	LegacyEventID       string           `json:"legacy_event_id,omitempty"`
	LegacyCreatedAt     int64            `json:"legacy_created_at,omitempty"`
	KeyPackageDiscovery string           `json:"keypackage_discovery,omitempty"`
}

// EffectiveConfig applies NIP-65 precedence over kind 3 and current Marmot
// KeyPackage discovery over historical kind 10051.
func EffectiveConfig(pubkey string, heads []Head, members []Membership) AccountConfig {
	byAddr := map[addrKey]Head{}
	for _, head := range heads {
		byAddr[addrKey{head.SourceKind, head.DTag}] = head
	}
	modern, hasModern := byAddr[addrKey{KindPublicRelayList, ""}]
	legacy, hasLegacy := byAddr[addrKey{KindLegacyContact, ""}]
	out := AccountConfig{
		Pubkey:       pubkey,
		ModernPublic: availabilityOf(hasModern, modern),
		LegacyPublic: availabilityOf(hasLegacy, legacy),
	}
	if hasModern {
		out.ModernEventID = modern.EventID
		out.ModernCreatedAt = modern.CreatedAt
	}
	if hasLegacy {
		out.LegacyEventID = legacy.EventID
		out.LegacyCreatedAt = legacy.CreatedAt
	}

	publicSource := ""
	var publicHead Head
	if hasModern {
		publicSource = SourceModern
		publicHead = modern
	} else if hasLegacy {
		publicSource = SourceLegacy
		publicHead = legacy
	}
	out.Roles = append(out.Roles,
		roleFrom(RolePublicInbox, publicSource, publicHead, hasModern || hasLegacy, filterRole(members, RolePublicInbox, publicSource)),
		roleFrom(RolePublicOutbox, publicSource, publicHead, hasModern || hasLegacy, filterRole(members, RolePublicOutbox, publicSource)),
	)

	out.Roles = append(out.Roles, standardRole(RoleDMInbox, KindDMRelays, byAddr, members))
	out.Roles = append(out.Roles, standardRole(RoleSearch, KindSearchRelays, byAddr, members))
	out.Roles = append(out.Roles, standardRole(RoleBlocked, KindBlockedRelays, byAddr, members))
	out.Roles = append(out.Roles, standardRole(RoleFavorite, KindFavoriteRelays, byAddr, members))
	out.Roles = append(out.Roles, standardRole(RoleBlossom, KindBlossomServers, byAddr, members))
	out.Roles = append(out.Roles, standardRole(RoleWiki, KindWikiRelays, byAddr, members))

	keyRole, discovery := effectiveKeyPackage(hasModern, modern, byAddr, members)
	out.KeyPackageDiscovery = discovery
	out.Roles = append(out.Roles, keyRole)

	if head, ok := byAddr[addrKey{KindPrivateRelays, ""}]; ok {
		out.Roles = append(out.Roles, RoleConfig{
			Role:         "private_storage",
			Availability: AvailabilityClientSide,
			Source:       SourceModern,
			EventID:      head.EventID,
			CreatedAt:    head.CreatedAt,
			Relays:       []DeclaredRelay{},
		})
	} else {
		out.Roles = append(out.Roles, RoleConfig{Role: "private_storage", Availability: AvailabilityMissing, Relays: []DeclaredRelay{}})
	}

	out.RelaySets = relaySets(byAddr, members)
	sort.Slice(out.Roles, func(i, j int) bool { return out.Roles[i].Role < out.Roles[j].Role })
	return out
}

type addrKey struct {
	kind int
	dTag string
}

func availabilityOf(ok bool, head Head) string {
	if !ok {
		return AvailabilityMissing
	}
	if head.ClientSideOnly {
		return AvailabilityClientSide
	}
	if head.ExplicitlyEmpty {
		return AvailabilityEmpty
	}
	return AvailabilityPresent
}

func filterRole(members []Membership, role, source string) []Membership {
	if source == "" {
		return nil
	}
	out := make([]Membership, 0)
	for _, member := range members {
		if member.Role != role || member.DeclarationSource != source {
			continue
		}
		if source == SourceModern && member.SourceKind == KindLegacyContact {
			continue
		}
		if source == SourceLegacy && member.SourceKind != KindLegacyContact {
			continue
		}
		out = append(out, member)
	}
	return out
}

func roleFrom(role, source string, head Head, ok bool, members []Membership) RoleConfig {
	cfg := RoleConfig{Role: role, Availability: AvailabilityMissing, Relays: []DeclaredRelay{}}
	if !ok {
		return cfg
	}
	cfg.Source = source
	cfg.EventID = head.EventID
	cfg.CreatedAt = head.CreatedAt
	cfg.InvalidCount = head.InvalidEndpoints
	cfg.Truncated = head.Truncated
	cfg.Relays = declared(members)
	if len(cfg.Relays) == 0 {
		cfg.Availability = AvailabilityEmpty
	} else {
		cfg.Availability = AvailabilityPresent
	}
	return cfg
}

func standardRole(role string, kind int, heads map[addrKey]Head, members []Membership) RoleConfig {
	head, ok := heads[addrKey{kind, ""}]
	cfg := RoleConfig{Role: role, Availability: AvailabilityMissing, Relays: []DeclaredRelay{}}
	if !ok {
		return cfg
	}
	if head.ClientSideOnly {
		cfg.Availability = AvailabilityClientSide
		cfg.Source = SourceModern
		cfg.EventID = head.EventID
		cfg.CreatedAt = head.CreatedAt
		return cfg
	}
	selected := make([]Membership, 0)
	for _, member := range members {
		if member.Role == role && member.SourceKind == kind && member.DTag == "" {
			selected = append(selected, member)
		}
	}
	return roleFrom(role, SourceModern, head, true, selected)
}

func effectiveKeyPackage(hasModern bool, modern Head, heads map[addrKey]Head, members []Membership) (RoleConfig, string) {
	if hasModern {
		selected := filterRole(members, RolePublicOutbox, SourceModern)
		cfg := roleFrom(RoleKeyPackage, "nip65_outbox", modern, true, selected)
		cfg.Source = "nip65_outbox"
		return cfg, "nip65_outbox"
	}
	if head, ok := heads[addrKey{KindKeyPackageList, ""}]; ok {
		selected := make([]Membership, 0)
		for _, member := range members {
			if member.Role == RoleKeyPackage && member.SourceKind == KindKeyPackageList {
				selected = append(selected, member)
			}
		}
		cfg := roleFrom(RoleKeyPackage, "kind_10051_legacy", head, true, selected)
		return cfg, "kind_10051_legacy"
	}
	return RoleConfig{Role: RoleKeyPackage, Availability: AvailabilityMissing, Relays: []DeclaredRelay{}}, ""
}

func relaySets(heads map[addrKey]Head, members []Membership) []RelaySetConfig {
	out := make([]RelaySetConfig, 0)
	for key, head := range heads {
		if key.kind != KindRelaySet {
			continue
		}
		selected := make([]Membership, 0)
		for _, member := range members {
			if member.SourceKind == KindRelaySet && member.DTag == key.dTag && member.Role == RoleRelaySet {
				selected = append(selected, member)
			}
		}
		availability := AvailabilityPresent
		relays := declared(selected)
		if len(relays) == 0 {
			availability = AvailabilityEmpty
		}
		out = append(out, RelaySetConfig{
			DTag:         key.dTag,
			Title:        head.Title,
			Availability: availability,
			EventID:      head.EventID,
			CreatedAt:    head.CreatedAt,
			Relays:       relays,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DTag < out[j].DTag })
	return out
}

func declared(members []Membership) []DeclaredRelay {
	out := make([]DeclaredRelay, 0, len(members))
	seen := map[string]struct{}{}
	for _, member := range members {
		if _, ok := seen[member.EndpointURL]; ok {
			continue
		}
		seen[member.EndpointURL] = struct{}{}
		out = append(out, DeclaredRelay{
			URL:           member.EndpointURL,
			URLKey:        member.URLKey,
			EndpointType:  member.EndpointType,
			Marker:        member.Marker,
			PublicSafe:    member.PublicSafe,
			SourceKind:    member.SourceKind,
			SourceEventID: member.SourceEventID,
			CreatedAt:     member.CreatedAt,
			Declaration:   member.DeclarationSource,
			DTag:          member.DTag,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URL < out[j].URL })
	return out
}

// RoleByName finds one effective role.
func (a AccountConfig) RoleByName(role string) (RoleConfig, bool) {
	for _, item := range a.Roles {
		if item.Role == role {
			return item, true
		}
	}
	return RoleConfig{}, false
}
