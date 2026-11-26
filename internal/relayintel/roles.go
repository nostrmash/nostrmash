// Package relayintel projects public relay-role declarations and builds
// explainable, role-specific recommendations. It does not admit relays into
// NostrMash's ingest set and it never signs or publishes a user's events.
package relayintel

import (
	"time"

	"github.com/nostrmash/nostrmash/internal/nostr"
)

const (
	// AlgorithmVersion identifies the whole recommendation response shape.
	AlgorithmVersion = "relay-intel-v1"
	// ScoreVersion is the weighted scoring model.
	ScoreVersion = "relay-score-v1"
	// OptimizerVersion is the complementary-set selector.
	OptimizerVersion = "relay-opt-v2"
	// TieBreak is the NIP-01 replaceable-event rule.
	// A newer created_at wins. The same created_at keeps the lower event id.
	TieBreak     = "lower_event_id"
	TieBreakNote = "Timestamp ties keep the lexicographically lower event id, per NIP-01. Events that were deleted before this rule was adopted cannot be reconstructed."

	RolePublicInbox  = "public_inbox"
	RolePublicOutbox = "public_outbox"
	RoleDMInbox      = "dm_inbox"
	RoleSearch       = "search"
	RoleBlocked      = "blocked"
	RoleFavorite     = "favorite"
	RoleRelaySet     = "relay_set"
	RoleKeyPackage   = "keypackage"
	RoleBlossom      = "blossom"
	RoleWiki         = "wiki"

	EndpointNostr   = "nostr_relay"
	EndpointBlossom = "blossom_http"

	SourceModern = "modern"
	SourceLegacy = "legacy"

	AvailabilityMissing    = "missing"
	AvailabilityEmpty      = "empty"
	AvailabilityPresent    = "present"
	AvailabilityClientSide = "client_side"

	KindLegacyContact   = 3
	KindPublicRelayList = 10002
	KindBlockedRelays   = 10006
	KindSearchRelays    = 10007
	KindFavoriteRelays  = 10012
	KindPrivateRelays   = 10013
	KindDMRelays        = 10050
	KindKeyPackageList  = 10051
	KindBlossomServers  = 10063
	KindWikiRelays      = 10102
	KindRelaySet        = 30002

	// MaxEndpointsPerEvent bounds one replaceable list. Larger lists are
	// truncated and the raw event is left unchanged.
	MaxEndpointsPerEvent = 32

	maxTagValueLen = 2048
	maxDTagLen     = 256
)

// PositiveRoles are declared-usage roles. Blocked relays are not usage.
func PositiveRoles() []string {
	return []string{
		RolePublicInbox,
		RolePublicOutbox,
		RoleDMInbox,
		RoleSearch,
		RoleFavorite,
		RoleRelaySet,
		RoleKeyPackage,
		RoleBlossom,
		RoleWiki,
	}
}

// DefaultOptimizeRoles are the categories a one-shot optimize call changes.
// Favorite lists, relay sets, blossom servers, and wiki relays stay put
// unless the caller names them.
func DefaultOptimizeRoles() []string {
	return []string{RolePublicInbox, RolePublicOutbox, RoleDMInbox, RoleSearch}
}

// HandlesKind reports whether projection should read this event.
func HandlesKind(kind int) bool {
	switch kind {
	case KindLegacyContact, KindPublicRelayList, KindBlockedRelays, KindSearchRelays,
		KindFavoriteRelays, KindPrivateRelays, KindDMRelays, KindKeyPackageList,
		KindBlossomServers, KindWikiRelays, KindRelaySet:
		return true
	default:
		return false
	}
}

// RoleForKind is the single role a non-NIP-65 list contributes.
// Kind 10002 and kind 3 return "" because one event fills two roles.
func RoleForKind(kind int) string {
	switch kind {
	case KindBlockedRelays:
		return RoleBlocked
	case KindSearchRelays:
		return RoleSearch
	case KindFavoriteRelays:
		return RoleFavorite
	case KindDMRelays:
		return RoleDMInbox
	case KindKeyPackageList:
		return RoleKeyPackage
	case KindBlossomServers:
		return RoleBlossom
	case KindWikiRelays:
		return RoleWiki
	case KindRelaySet:
		return RoleRelaySet
	default:
		return ""
	}
}

// DefaultCountRange is the conservative list size for a role.
// The low number is the redundancy floor. The high number is the default cap.
// Manually configured accounts are not rejected for being outside the range.
func DefaultCountRange(role string) (min, max int) {
	switch role {
	case RolePublicInbox, RolePublicOutbox:
		return 2, 4
	case RoleDMInbox:
		return 1, 3
	case RoleSearch, RoleFavorite, RoleKeyPackage, RoleWiki:
		return 1, 3
	case RoleBlossom:
		return 1, 2
	case RoleRelaySet:
		return 1, 4
	default:
		return 1, 4
	}
}

// RoleInfo describes one category the API can name.
type RoleInfo struct {
	Role               string `json:"role"`
	Kinds              []int  `json:"kinds"`
	EndpointType       string `json:"endpoint_type"`
	DefaultMin         int    `json:"default_min"`
	DefaultMax         int    `json:"default_max"`
	PositiveAdoption   bool   `json:"positive_adoption"`
	ClientSide         bool   `json:"client_side,omitempty"`
	OptimizedByDefault bool   `json:"optimized_by_default"`
	Notes              string `json:"notes"`
}

// RoleCatalog is the static role index. It does not look at the database.
func RoleCatalog() []RoleInfo {
	inMin, inMax := DefaultCountRange(RolePublicInbox)
	dmMin, dmMax := DefaultCountRange(RoleDMInbox)
	searchMin, searchMax := DefaultCountRange(RoleSearch)
	favMin, favMax := DefaultCountRange(RoleFavorite)
	kpMin, kpMax := DefaultCountRange(RoleKeyPackage)
	setMin, setMax := DefaultCountRange(RoleRelaySet)
	bloomMin, bloomMax := DefaultCountRange(RoleBlossom)
	wikiMin, wikiMax := DefaultCountRange(RoleWiki)
	return []RoleInfo{
		{Role: RolePublicInbox, Kinds: []int{KindPublicRelayList, KindLegacyContact}, EndpointType: EndpointNostr, DefaultMin: inMin, DefaultMax: inMax, PositiveAdoption: true, OptimizedByDefault: true, Notes: "NIP-65 read, plus unmarked relays. An unmarked relay is also an outbox. Kind 3 is used only when no kind 10002 exists. These are relays where the account expects mentions, not the relays a client must download the whole home feed from."},
		{Role: RolePublicOutbox, Kinds: []int{KindPublicRelayList, KindLegacyContact}, EndpointType: EndpointNostr, DefaultMin: inMin, DefaultMax: inMax, PositiveAdoption: true, OptimizedByDefault: true, Notes: "NIP-65 write, plus unmarked relays. Inbox and outbox are one kind 10002 event. Optimization never emits kind 3."},
		{Role: RoleDMInbox, Kinds: []int{KindDMRelays}, EndpointType: EndpointNostr, DefaultMin: dmMin, DefaultMax: dmMax, PositiveAdoption: true, OptimizedByDefault: true, Notes: "Kind 10050. Removing one requires review because historical direct messages are not migrated."},
		{Role: RoleSearch, Kinds: []int{KindSearchRelays}, EndpointType: EndpointNostr, DefaultMin: searchMin, DefaultMax: searchMax, PositiveAdoption: true, OptimizedByDefault: true, Notes: "Kind 10007. A relay is not treated as search-capable because a websocket subscription reached EOSE."},
		{Role: RoleBlocked, Kinds: []int{KindBlockedRelays}, EndpointType: EndpointNostr, DefaultMin: 0, DefaultMax: 0, PositiveAdoption: false, Notes: "Kind 10006. Blocked relays are not adoption and are not turned into a suggested blocklist."},
		{Role: RoleFavorite, Kinds: []int{KindFavoriteRelays}, EndpointType: EndpointNostr, DefaultMin: favMin, DefaultMax: favMax, PositiveAdoption: true, Notes: "Kind 10012. A user-owned preference. The default optimizer leaves it alone."},
		{Role: RoleRelaySet, Kinds: []int{KindRelaySet}, EndpointType: EndpointNostr, DefaultMin: setMin, DefaultMax: setMax, PositiveAdoption: true, Notes: "Kind 30002, one address per d tag. The optimizer does not rewrite these sets."},
		{Role: RoleKeyPackage, Kinds: []int{KindPublicRelayList, KindKeyPackageList}, EndpointType: EndpointNostr, DefaultMin: kpMin, DefaultMax: kpMax, PositiveAdoption: true, Notes: "Current Marmot publishes KeyPackages to NIP-65 write relays. Kind 10051 is used only when the account has no kind 10002. Replacing one needs a new KeyPackage on the new relay."},
		{Role: "private_storage", Kinds: []int{KindPrivateRelays}, EndpointType: EndpointNostr, ClientSide: true, Notes: "Kind 10013 stays encrypted. The server stores that an event exists and does not read its URLs."},
		{Role: RoleBlossom, Kinds: []int{KindBlossomServers}, EndpointType: EndpointBlossom, DefaultMin: bloomMin, DefaultMax: bloomMax, PositiveAdoption: true, Notes: "Kind 10063 servers are HTTP media endpoints. They are not inserted into the Nostr relay registry and are not replaced from popularity."},
		{Role: RoleWiki, Kinds: []int{KindWikiRelays}, EndpointType: EndpointNostr, DefaultMin: wikiMin, DefaultMax: wikiMax, PositiveAdoption: true, Notes: "Kind 10102. Optional. The default optimizer leaves wiki relays alone."},
	}
}

// EventInput is one stored event handed to the projection.
type EventInput struct {
	Pubkey    string
	EventID   string
	Kind      int
	CreatedAt int64
	Content   string
	Tags      [][]string
	// EndpointLimit overrides the analytical projection cap for one parse.
	// Zero uses MaxEndpointsPerEvent. A larger complete event is not truncated
	// to the projection cap. Past the limit the result is marked truncated.
	EndpointLimit int
}

// Head is the winning declaration for one address, including an explicit empty list.
type Head struct {
	Pubkey            string
	SourceKind        int
	DTag              string
	EventID           string
	CreatedAt         int64
	ExplicitlyEmpty   bool
	ClientSideOnly    bool
	DeclarationSource string
	InvalidEndpoints  int
	Truncated         bool
	Title             string
}

// Membership is one normalized endpoint in one role.
type Membership struct {
	Pubkey            string
	Role              string
	EndpointURL       string
	URLKey            string
	EndpointType      string
	SourceKind        int
	SourceEventID     string
	DTag              string
	CreatedAt         int64
	DeclarationSource string
	Marker            string
	PublicSafe        bool
}

// wins reports whether next should replace the stored declaration.
// Equal ids are replays and must refresh memberships.
func wins(newCreated int64, newID string, oldCreated int64, oldID string) bool {
	return nostr.Supersedes(newCreated, newID, oldCreated, oldID)
}

// Freshness is how long a measurement stays current.
const (
	ProbeFreshFor = 48 * time.Hour
	ProbeStaleFor = 7 * 24 * time.Hour
	NIP11StaleFor = 7 * 24 * time.Hour
)
