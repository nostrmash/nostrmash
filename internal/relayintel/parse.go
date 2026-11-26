package relayintel

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nostrmash/nostrmash/internal/relayurl"
)

// Parsed is the public projection of one event. Clear, when true, means this
// winning event carries no declaration and any older projection of the same
// address must be removed. That is an unavailable configuration, not an
// explicit empty list.
type Parsed struct {
	Head    Head
	Members []Membership
	Clear   bool
	// PreservedTags are tags the proposal builder must copy unchanged.
	PreservedTags [][]string
}

// Parse projects one event into role memberships.
// Private and local URLs are kept on the account and marked unsafe for
// global statistics. Kind 10013 never contributes URLs.
func Parse(in EventInput) (Parsed, error) {
	in.Pubkey = strings.TrimSpace(in.Pubkey)
	in.EventID = strings.TrimSpace(in.EventID)
	if in.Pubkey == "" || in.EventID == "" || !HandlesKind(in.Kind) {
		return Parsed{}, nil
	}
	switch in.Kind {
	case KindLegacyContact:
		return parseKind3(in), nil
	case KindPrivateRelays:
		return parsePrivate(in), nil
	case KindPublicRelayList:
		return parseMarkedRelays(in, SourceModern, ""), nil
	case KindBlossomServers:
		return parseBlossom(in), nil
	default:
		return parsePlainList(in)
	}
}

func endpointCap(in EventInput) int {
	if in.EndpointLimit > 0 {
		return in.EndpointLimit
	}
	return MaxEndpointsPerEvent
}

func parsePrivate(in EventInput) Parsed {
	// Kind 10013 is NIP-44 encrypted. Public tags are not a license to index
	// private relay URLs. The client retrieves the original event itself.
	return Parsed{Head: Head{
		Pubkey:            in.Pubkey,
		SourceKind:        in.Kind,
		EventID:           in.EventID,
		CreatedAt:         in.CreatedAt,
		ClientSideOnly:    true,
		DeclarationSource: SourceModern,
		Title:             clip(firstTag(in.Tags, "title"), 160),
	}}
}

func parseKind3(in EventInput) Parsed {
	content := strings.TrimSpace(in.Content)
	if content == "" {
		return Parsed{Clear: true}
	}
	dec := json.NewDecoder(strings.NewReader(content))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return Parsed{Clear: true}
	}
	type spec struct {
		Read  bool `json:"read"`
		Write bool `json:"write"`
	}
	var members []Membership
	invalid := 0
	truncated := false
	for dec.More() {
		keyToken, err := dec.Token()
		if err != nil {
			break
		}
		key, _ := keyToken.(string)
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			break
		}
		var item spec
		if err := json.Unmarshal(raw, &item); err != nil || (!item.Read && !item.Write) {
			if strings.TrimSpace(key) != "" {
				invalid++
			}
			continue
		}
		if len(members) >= endpointCap(in) {
			truncated = true
			continue
		}
		normalized, keyHash, safe, ok := normalizeNostr(key)
		if !ok {
			invalid++
			continue
		}
		if item.Read {
			members = append(members, membership(in, RolePublicInbox, normalized, keyHash, EndpointNostr, SourceLegacy, "", safe))
		}
		if item.Write {
			members = append(members, membership(in, RolePublicOutbox, normalized, keyHash, EndpointNostr, SourceLegacy, "", safe))
		}
	}
	return Parsed{Head: Head{
		Pubkey:            in.Pubkey,
		SourceKind:        KindLegacyContact,
		EventID:           in.EventID,
		CreatedAt:         in.CreatedAt,
		ExplicitlyEmpty:   len(members) == 0,
		DeclarationSource: SourceLegacy,
		InvalidEndpoints:  invalid,
		Truncated:         truncated,
	}, Members: dedupeMembers(members)}
}

func parseMarkedRelays(in EventInput, source, dTag string) Parsed {
	var members []Membership
	var preserved [][]string
	invalid := 0
	truncated := false
	seen := 0
	for _, tag := range in.Tags {
		if len(tag) == 0 {
			continue
		}
		if tag[0] != "r" {
			preserved = append(preserved, append([]string(nil), tag...))
			continue
		}
		if len(tag) < 2 || strings.TrimSpace(tag[1]) == "" {
			invalid++
			continue
		}
		marker := ""
		if len(tag) >= 3 {
			marker = strings.ToLower(strings.TrimSpace(tag[2]))
		}
		roles := markerRoles(marker)
		if roles == nil {
			preserved = append(preserved, append([]string(nil), tag...))
			continue
		}
		normalized, keyHash, safe, ok := normalizeNostr(tag[1])
		if !ok {
			invalid++
			continue
		}
		if seen >= endpointCap(in) {
			truncated = true
			continue
		}
		seen++
		for _, role := range roles {
			members = append(members, membership(in, role, normalized, keyHash, EndpointNostr, source, marker, safe))
		}
	}
	return Parsed{
		Head: Head{
			Pubkey:            in.Pubkey,
			SourceKind:        in.Kind,
			DTag:              dTag,
			EventID:           in.EventID,
			CreatedAt:         in.CreatedAt,
			ExplicitlyEmpty:   len(members) == 0,
			DeclarationSource: source,
			InvalidEndpoints:  invalid,
			Truncated:         truncated,
		},
		Members:       dedupeMembers(members),
		PreservedTags: preserved,
	}
}

func markerRoles(marker string) []string {
	switch marker {
	case "":
		return []string{RolePublicInbox, RolePublicOutbox}
	case "read":
		return []string{RolePublicInbox}
	case "write":
		return []string{RolePublicOutbox}
	default:
		return nil
	}
}

func parsePlainList(in EventInput) (Parsed, error) {
	role := RoleForKind(in.Kind)
	dTag := ""
	title := ""
	tagName := "relay"
	// Favorite lists and relay sets use "relay". Some historical events also
	// carry "r". Both are accepted. "a" tags point at sets, not URLs.
	var members []Membership
	var preserved [][]string
	invalid := 0
	truncated := false
	seen := 0
	for _, tag := range in.Tags {
		if len(tag) == 0 {
			continue
		}
		switch tag[0] {
		case "d":
			// Only addressable relay sets are keyed by the d tag.
			// Replaceable lists ignore it, or two d values would not replace.
			if in.Kind == KindRelaySet && dTag == "" && len(tag) >= 2 {
				dTag = strings.TrimSpace(tag[1])
				if len(dTag) > 1024 {
					return Parsed{}, fmt.Errorf("%w: d tag is too long", ErrInvalidInput)
				}
			}
			preserved = append(preserved, append([]string(nil), tag...))
			continue
		case "title":
			if title == "" && len(tag) >= 2 {
				title = clip(strings.TrimSpace(tag[1]), 160)
			}
			preserved = append(preserved, append([]string(nil), tag...))
			continue
		case "relay", "r":
			if tag[0] != tagName && tag[0] != "r" {
				preserved = append(preserved, append([]string(nil), tag...))
				continue
			}
		default:
			preserved = append(preserved, append([]string(nil), tag...))
			continue
		}
		if len(tag) < 2 || strings.TrimSpace(tag[1]) == "" {
			invalid++
			continue
		}
		normalized, keyHash, safe, ok := normalizeNostr(tag[1])
		if !ok {
			invalid++
			continue
		}
		if seen >= endpointCap(in) {
			truncated = true
			continue
		}
		seen++
		members = append(members, membership(in, role, normalized, keyHash, EndpointNostr, SourceModern, "", safe))
	}
	for i := range members {
		members[i].DTag = dTag
	}
	return Parsed{
		Head: Head{
			Pubkey:            in.Pubkey,
			SourceKind:        in.Kind,
			DTag:              dTag,
			EventID:           in.EventID,
			CreatedAt:         in.CreatedAt,
			ExplicitlyEmpty:   len(members) == 0,
			DeclarationSource: SourceModern,
			InvalidEndpoints:  invalid,
			Truncated:         truncated,
			Title:             title,
		},
		Members:       dedupeMembers(members),
		PreservedTags: preserved,
	}, nil
}

func parseBlossom(in EventInput) Parsed {
	var members []Membership
	var preserved [][]string
	invalid := 0
	truncated := false
	seen := 0
	for _, tag := range in.Tags {
		if len(tag) == 0 {
			continue
		}
		if tag[0] != "server" {
			preserved = append(preserved, append([]string(nil), tag...))
			continue
		}
		if len(tag) < 2 || strings.TrimSpace(tag[1]) == "" {
			invalid++
			continue
		}
		normalized, keyHash, safe, ok := normalizeBlossom(tag[1])
		if !ok {
			invalid++
			continue
		}
		if seen >= endpointCap(in) {
			truncated = true
			continue
		}
		seen++
		members = append(members, membership(in, RoleBlossom, normalized, keyHash, EndpointBlossom, SourceModern, "", safe))
	}
	return Parsed{
		Head: Head{
			Pubkey:            in.Pubkey,
			SourceKind:        in.Kind,
			EventID:           in.EventID,
			CreatedAt:         in.CreatedAt,
			ExplicitlyEmpty:   len(members) == 0,
			DeclarationSource: SourceModern,
			InvalidEndpoints:  invalid,
			Truncated:         truncated,
		},
		Members:       dedupeMembers(members),
		PreservedTags: preserved,
	}
}

func membership(in EventInput, role, endpoint, key, endpointType, source, marker string, safe bool) Membership {
	return Membership{
		Pubkey:            in.Pubkey,
		Role:              role,
		EndpointURL:       endpoint,
		URLKey:            key,
		EndpointType:      endpointType,
		SourceKind:        in.Kind,
		SourceEventID:     in.EventID,
		DTag:              "",
		CreatedAt:         in.CreatedAt,
		DeclarationSource: source,
		Marker:            marker,
		PublicSafe:        safe,
	}
}

func dedupeMembers(in []Membership) []Membership {
	type key struct {
		role, url string
	}
	// One row per role and URL. A repeated tag does not become a second member.
	seen := make(map[key]int, len(in))
	out := make([]Membership, 0, len(in))
	for _, member := range in {
		k := key{member.Role, member.EndpointURL}
		if idx, ok := seen[k]; ok {
			if member.PublicSafe {
				out[idx].PublicSafe = true
			}
			continue
		}
		seen[k] = len(out)
		out = append(out, member)
	}
	return out
}

func normalizeNostr(raw string) (normalized, key string, safe, ok bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > maxTagValueLen {
		return "", "", false, false
	}
	normalized, err := relayurl.Normalize(raw, relayurl.NormalizeOptions{})
	if err != nil {
		return "", "", false, false
	}
	safe = relayurl.Validate(normalized, relayurl.ValidateOptions{}) == nil
	return normalized, relayurl.CanonicalKey(normalized), safe, true
}

func normalizeBlossom(raw string) (normalized, key string, safe, ok bool) {
	normalized, err := NormalizeBlossomURL(raw)
	if err != nil {
		return "", "", false, false
	}
	safe = BlossomPublic(normalized)
	return normalized, relayurl.CanonicalKey(normalized), safe, true
}

func firstTag(tags [][]string, name string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == name {
			return strings.TrimSpace(tag[1])
		}
	}
	return ""
}

func clip(value string, max int) string {
	if max <= 0 || len(value) <= max {
		return value
	}
	return value[:max]
}
