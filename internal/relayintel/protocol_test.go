package relayintel

import (
	"fmt"
	"strings"
	"testing"
)

func TestParseNIP65Markers(t *testing.T) {
	parsed := mustParse(t, sampleEvent(KindPublicRelayList, "", [][]string{
		{"r", "wss://read.example.com", "read"},
		{"r", "wss://write.example.com/", "write"},
		{"r", "wss://both.example.com"},
		{"r", "wss://both.example.com", "read"},
		{"r", "wss://odd.example.com", "both"},
		{"client", "nostrmash"},
		{"p", "ab", "wss://hint.example.com"},
	}))
	if parsed.Clear || parsed.Head.ExplicitlyEmpty {
		t.Fatalf("expected a declaration, got %+v", parsed.Head)
	}
	got := roleURLs(parsed)
	if !sameURLs(got[RolePublicInbox], "wss://both.example.com", "wss://read.example.com") {
		t.Fatalf("inbox = %v", got[RolePublicInbox])
	}
	if !sameURLs(got[RolePublicOutbox], "wss://both.example.com", "wss://write.example.com") {
		t.Fatalf("outbox = %v", got[RolePublicOutbox])
	}
	if _, ok := got["wss://hint.example.com"]; ok {
		t.Fatal("a p-tag relay hint was treated as a preference")
	}
	for _, member := range parsed.Members {
		if member.DTag != "" {
			t.Fatalf("replaceable list stored a d tag: %+v", member)
		}
		if strings.Contains(member.EndpointURL, "odd.example") || strings.Contains(member.EndpointURL, "hint.example") {
			t.Fatalf("unexpected member %s", member.EndpointURL)
		}
	}
	if len(parsed.PreservedTags) != 3 {
		t.Fatalf("preserved tags = %#v", parsed.PreservedTags)
	}
}

func TestParseKind3PrecedenceInputs(t *testing.T) {
	empty := mustParse(t, sampleEvent(KindLegacyContact, "", nil))
	if !empty.Clear || len(empty.Members) != 0 {
		t.Fatal("blank kind 3 content is unavailable, not an empty declaration")
	}
	garbage := mustParse(t, sampleEvent(KindLegacyContact, "not-json", [][]string{{"p", strings.Repeat("ab", 32), "wss://hint.example.com"}}))
	if !garbage.Clear {
		t.Fatal("non-object kind 3 content must clear rather than invent relays")
	}
	explicit := mustParse(t, sampleEvent(KindLegacyContact, `{}`, [][]string{{"p", strings.Repeat("ab", 32), "wss://hint.example.com"}}))
	if explicit.Clear || !explicit.Head.ExplicitlyEmpty || len(explicit.Members) != 0 {
		t.Fatalf("empty object = %+v members=%d", explicit.Head, len(explicit.Members))
	}
	marked := mustParse(t, sampleEvent(KindLegacyContact, `{"wss://legacy.example.com":{"read":true,"write":false}}`, nil))
	got := roleURLs(marked)
	if !sameURLs(got[RolePublicInbox], "wss://legacy.example.com") || len(got[RolePublicOutbox]) != 0 {
		t.Fatalf("kind 3 roles = %#v", got)
	}
}

func TestParsePrivateBlossomAndSets(t *testing.T) {
	private := mustParse(t, sampleEvent(KindPrivateRelays, "ciphertext", [][]string{{"relay", "wss://secret.example.com"}}))
	if !private.Head.ClientSideOnly || len(private.Members) != 0 {
		t.Fatalf("kind 10013 leaked URLs: %+v", private)
	}
	blossom := mustParse(t, sampleEvent(KindBlossomServers, "", [][]string{{"server", "https://cdn.example/path"}}))
	if len(blossom.Members) != 1 || blossom.Members[0].EndpointType != EndpointBlossom || blossom.Members[0].Role != RoleBlossom {
		t.Fatalf("blossom = %+v", blossom.Members)
	}
	set := mustParse(t, sampleEvent(KindRelaySet, "", [][]string{{"d", "friends"}, {"title", "Friends"}, {"relay", "wss://set.example.com"}}))
	if set.Head.DTag != "friends" || len(set.Members) != 1 || set.Members[0].DTag != "friends" {
		t.Fatalf("relay set = %+v", set)
	}
	local := mustParse(t, sampleEvent(KindPublicRelayList, "", [][]string{{"r", "ws://127.0.0.1:8080"}}))
	if len(local.Members) != 2 || local.Members[0].PublicSafe || local.Members[1].PublicSafe {
		t.Fatalf("private URL = %+v", local.Members)
	}
}

func TestParseCapsAndDropsMalformed(t *testing.T) {
	tags := make([][]string, 0, 34)
	for i := 0; i < 33; i++ {
		tags = append(tags, []string{"r", fmt.Sprintf("wss://r%d.example.com", i)})
	}
	tags = append(tags, []string{"r", "wss://bad.example.com?x=1"})
	parsed := mustParse(t, sampleEvent(KindDMRelays, "", tags))
	if len(parsed.Members) != MaxEndpointsPerEvent || !parsed.Head.Truncated || parsed.Head.InvalidEndpoints == 0 {
		t.Fatalf("members=%d truncated=%v invalid=%d", len(parsed.Members), parsed.Head.Truncated, parsed.Head.InvalidEndpoints)
	}
}

func TestWinsUsesHigherEventID(t *testing.T) {
	if !wins(20, "b", 10, "c") {
		t.Fatal("newer created_at must win")
	}
	if wins(10, "aa", 10, "bb") {
		t.Fatal("lower event id must lose a timestamp tie")
	}
	if !wins(10, "bb", 10, "aa") {
		t.Fatal("higher event id must win a timestamp tie")
	}
	if !wins(10, "aa", 10, "aa") {
		t.Fatal("replaying the same event must refresh memberships")
	}
}

func TestEffectiveConfigDoesNotMergeLegacyIntoModern(t *testing.T) {
	pubkey := strings.Repeat("ab", 32)
	modern := mustParse(t, sampleEvent(KindPublicRelayList, "", [][]string{{"r", "wss://modern.example.com", "read"}}))
	legacy := mustParse(t, sampleEvent(KindLegacyContact, `{"wss://legacy.example.com":{"read":true,"write":true}}`, nil))
	cfg := EffectiveConfig(pubkey, []Head{modern.Head, legacy.Head}, append(modern.Members, legacy.Members...))
	inbox, _ := cfg.RoleByName(RolePublicInbox)
	if inbox.Availability != AvailabilityPresent || len(inbox.Relays) != 1 || inbox.Relays[0].URL != "wss://modern.example.com" {
		t.Fatalf("inbox = %+v", inbox)
	}
	outbox, _ := cfg.RoleByName(RolePublicOutbox)
	if outbox.Availability != AvailabilityEmpty || len(outbox.Relays) != 0 {
		t.Fatalf("empty modern outbox must not inherit kind 3: %+v", outbox)
	}
	if cfg.KeyPackageDiscovery != "nip65_outbox" {
		t.Fatalf("discovery = %s", cfg.KeyPackageDiscovery)
	}

	emptyModern := mustParse(t, sampleEvent(KindPublicRelayList, "", nil))
	emptyCfg := EffectiveConfig(pubkey, []Head{emptyModern.Head, legacy.Head}, legacy.Members)
	if emptyCfg.ModernPublic != AvailabilityEmpty {
		t.Fatalf("modern = %s", emptyCfg.ModernPublic)
	}
	if got, _ := emptyCfg.RoleByName(RolePublicInbox); got.Availability != AvailabilityEmpty || len(got.Relays) != 0 {
		t.Fatalf("explicit empty modern list merged legacy relays: %+v", got)
	}

	legacyOnly := EffectiveConfig(pubkey, []Head{legacy.Head}, legacy.Members)
	legacyOutbox, _ := legacyOnly.RoleByName(RolePublicOutbox)
	if legacyOnly.ModernPublic != AvailabilityMissing || len(legacyOutbox.Relays) != 1 {
		t.Fatalf("legacy fallback = %+v", legacyOutbox)
	}
	if legacyOnly.KeyPackageDiscovery != "kind_10051_legacy" && legacyOnly.KeyPackageDiscovery != "" {
		// No kind 10051 head, so discovery stays empty even though kind 3 exists.
	}
	if legacyOnly.KeyPackageDiscovery != "" {
		t.Fatalf("kind 3 must not become a keypackage list: %s", legacyOnly.KeyPackageDiscovery)
	}
}

func TestEffectiveKeyPackageUsesLegacyListOnlyWithoutNIP65(t *testing.T) {
	pubkey := strings.Repeat("ab", 32)
	legacyList := mustParse(t, sampleEvent(KindKeyPackageList, "", [][]string{{"relay", "wss://kp.example.com"}}))
	cfg := EffectiveConfig(pubkey, []Head{legacyList.Head}, legacyList.Members)
	role, _ := cfg.RoleByName(RoleKeyPackage)
	if cfg.KeyPackageDiscovery != "kind_10051_legacy" || len(role.Relays) != 1 {
		t.Fatalf("discovery=%s role=%+v", cfg.KeyPackageDiscovery, role)
	}
	modern := mustParse(t, sampleEvent(KindPublicRelayList, "", [][]string{{"r", "wss://out.example.com", "write"}}))
	both := EffectiveConfig(pubkey, []Head{modern.Head, legacyList.Head}, append(modern.Members, legacyList.Members...))
	keypackage, _ := both.RoleByName(RoleKeyPackage)
	if both.KeyPackageDiscovery != "nip65_outbox" || len(keypackage.Relays) != 1 || keypackage.Relays[0].URL != "wss://out.example.com" {
		t.Fatalf("modern keypackage = %+v", keypackage)
	}
}

func mustParse(t *testing.T, in EventInput) Parsed {
	t.Helper()
	parsed, err := Parse(in)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func sampleEvent(kind int, content string, tags [][]string) EventInput {
	return EventInput{
		Pubkey:    strings.Repeat("ab", 32),
		EventID:   strings.Repeat("cd", 32),
		Kind:      kind,
		CreatedAt: 1_700_000_000,
		Content:   content,
		Tags:      tags,
	}
}

func roleURLs(parsed Parsed) map[string][]string {
	out := map[string][]string{}
	for _, member := range parsed.Members {
		out[member.Role] = append(out[member.Role], member.EndpointURL)
	}
	return out
}

func sameURLs(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := map[string]int{}
	for _, item := range got {
		seen[item]++
	}
	for _, item := range want {
		seen[item]--
		if seen[item] < 0 {
			return false
		}
	}
	return true
}
