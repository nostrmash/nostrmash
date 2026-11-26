package relayintel

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestAuthoritativeSourcePrefersNewerClientEvent(t *testing.T) {
	pubkey := strings.Repeat("ab", 32)
	indexedRaw := configEvent("bbbbbbbb", pubkey, 10, []string{"wss://a.example", "wss://b.example"})
	clientRaw := configEvent("cccccccc", pubkey, 20, []string{"wss://a.example", "wss://b.example", "wss://c.example"})
	heads := []Head{{
		Pubkey: pubkey, SourceKind: KindPublicRelayList, EventID: "bbbbbbbb", CreatedAt: 10,
		DeclarationSource: SourceModern,
	}}
	members := []Membership{
		{Pubkey: pubkey, Role: RolePublicInbox, EndpointURL: "wss://a.example", SourceKind: KindPublicRelayList, DeclarationSource: SourceModern},
		{Pubkey: pubkey, Role: RolePublicOutbox, EndpointURL: "wss://a.example", SourceKind: KindPublicRelayList, DeclarationSource: SourceModern},
		{Pubkey: pubkey, Role: RolePublicInbox, EndpointURL: "wss://b.example", SourceKind: KindPublicRelayList, DeclarationSource: SourceModern},
		{Pubkey: pubkey, Role: RolePublicOutbox, EndpointURL: "wss://b.example", SourceKind: KindPublicRelayList, DeclarationSource: SourceModern},
	}
	account, resolved, notes := applyAuthoritative(pubkey, heads, members, map[string]json.RawMessage{"bbbbbbbb": indexedRaw}, map[string]SourceEvent{
		sourceKey(KindPublicRelayList, ""): {Raw: clientRaw, ID: "cccccccc", Pubkey: pubkey, Kind: KindPublicRelayList, CreatedAt: 20},
	}, deletionView{})
	if len(notes) != 0 {
		t.Fatalf("notes = %#v", notes)
	}
	src := resolved[sourceKey(KindPublicRelayList, "")]
	if src.Origin != "client" || !src.Complete || src.EventID != "cccccccc" {
		t.Fatalf("source = %+v", src)
	}
	role, _ := account.RoleByName(RolePublicInbox)
	if !hasDeclared(role.Relays, "wss://c.example") {
		t.Fatalf("optimizer source dropped C: %#v", role.Relays)
	}
	result := Patch(PatchInput{
		Source: src.Raw, Origin: src.Origin, ExpectedPubkey: pubkey, ExpectedKind: KindPublicRelayList,
		InboxOptimized: true, OutboxOptimized: true,
		Inbox: []string{"wss://a.example", "wss://b.example"}, Outbox: []string{"wss://a.example", "wss://b.example"},
	})
	if !result.Safe || result.Event == nil {
		t.Fatalf("patch = %+v", result)
	}
	for _, tag := range result.Event.Tags {
		if len(tag) > 1 && tag[1] == "wss://c.example" {
			t.Fatal("replacement kept C after the optimizer removed it")
		}
	}
}

func TestStaleSuppliedEventDoesNotReplaceIndexed(t *testing.T) {
	pubkey := strings.Repeat("cd", 32)
	indexedRaw := configEvent("bbbbbbbb", pubkey, 30, []string{"wss://new.example"})
	clientRaw := configEvent("aaaaaaaa", pubkey, 10, []string{"wss://old.example"})
	heads := []Head{{Pubkey: pubkey, SourceKind: KindPublicRelayList, EventID: "bbbbbbbb", CreatedAt: 30, DeclarationSource: SourceModern}}
	account, resolved, notes := applyAuthoritative(pubkey, heads, nil, map[string]json.RawMessage{"bbbbbbbb": indexedRaw}, map[string]SourceEvent{
		sourceKey(KindPublicRelayList, ""): {Raw: clientRaw, ID: "aaaaaaaa", Kind: KindPublicRelayList, CreatedAt: 10},
	}, deletionView{})
	if resolved[sourceKey(KindPublicRelayList, "")].Origin != "indexed" {
		t.Fatalf("origin = %s", resolved[sourceKey(KindPublicRelayList, "")].Origin)
	}
	if len(notes) != 1 || notes[0].reason.Code != "stale_source" {
		t.Fatalf("notes = %#v", notes)
	}
	role, _ := account.RoleByName(RolePublicInbox)
	if hasDeclared(role.Relays, "wss://old.example") || !hasDeclared(role.Relays, "wss://new.example") {
		t.Fatalf("relays = %#v", role.Relays)
	}
}

func TestEqualTimestampKeepsLowerEventID(t *testing.T) {
	pubkey := strings.Repeat("ef", 32)
	lower := configEvent("aaaaaaaa", pubkey, 10, []string{"wss://lower.example"})
	higher := configEvent("bbbbbbbb", pubkey, 10, []string{"wss://higher.example"})
	heads := []Head{{Pubkey: pubkey, SourceKind: KindPublicRelayList, EventID: "bbbbbbbb", CreatedAt: 10, DeclarationSource: SourceModern}}
	_, resolved, notes := applyAuthoritative(pubkey, heads, nil, map[string]json.RawMessage{"bbbbbbbb": higher}, map[string]SourceEvent{
		sourceKey(KindPublicRelayList, ""): {Raw: lower, ID: "aaaaaaaa", Kind: KindPublicRelayList, CreatedAt: 10},
	}, deletionView{})
	if resolved[sourceKey(KindPublicRelayList, "")].EventID != "aaaaaaaa" || len(notes) != 0 {
		t.Fatalf("winner = %+v notes=%#v", resolved[sourceKey(KindPublicRelayList, "")], notes)
	}
}

func TestDeletedSourceIsNotABlankList(t *testing.T) {
	pubkey := strings.Repeat("11", 32)
	raw := configEvent("aaaaaaaa", pubkey, 10, []string{"wss://gone.example"})
	heads := []Head{{Pubkey: pubkey, SourceKind: KindPublicRelayList, EventID: "aaaaaaaa", CreatedAt: 10, DeclarationSource: SourceModern}}
	members := []Membership{{Pubkey: pubkey, Role: RolePublicInbox, EndpointURL: "wss://gone.example", SourceKind: KindPublicRelayList, DeclarationSource: SourceModern}}
	deleted := deletionView{ByCoord: map[string]int64{sourceKey(KindPublicRelayList, ""): 10}}
	account, resolved, notes := applyAuthoritative(pubkey, heads, members, map[string]json.RawMessage{"aaaaaaaa": raw}, nil, deleted)
	if resolved[sourceKey(KindPublicRelayList, "")].Origin != "deleted" {
		t.Fatalf("origin = %+v", resolved[sourceKey(KindPublicRelayList, "")])
	}
	role, _ := account.RoleByName(RolePublicInbox)
	if role.Availability != AvailabilityMissing || len(notes) != 1 {
		t.Fatalf("availability=%s relays=%#v", role.Availability, role.Relays)
	}
}

func TestNewerEventSurvivesAnOlderDeletion(t *testing.T) {
	pubkey := strings.Repeat("22", 32)
	raw := configEvent("bbbbbbbb", pubkey, 30, []string{"wss://back.example"})
	deleted := deletionView{ByCoord: map[string]int64{sourceKey(KindPublicRelayList, ""): 10}}
	account, resolved, _ := applyAuthoritative(pubkey, nil, nil, nil, map[string]SourceEvent{
		sourceKey(KindPublicRelayList, ""): {Raw: raw, ID: "bbbbbbbb", Kind: KindPublicRelayList, CreatedAt: 30},
	}, deleted)
	if resolved[sourceKey(KindPublicRelayList, "")].Origin != "client" {
		t.Fatalf("origin = %+v", resolved[sourceKey(KindPublicRelayList, "")])
	}
	role, _ := account.RoleByName(RolePublicInbox)
	if !hasDeclared(role.Relays, "wss://back.example") {
		t.Fatalf("newer event was suppressed: %#v", role.Relays)
	}
}

func TestAllowCreateMintsAListOnlyWhenAsked(t *testing.T) {
	missing := Patch(PatchInput{Origin: "missing", ExpectedPubkey: "abc", ExpectedKind: KindDMRelays, ListOptimized: true, TagName: "relay", Desired: []string{"wss://dm.example"}})
	if missing.Safe || missing.Event != nil {
		t.Fatal("absence minted a list")
	}
	created := Patch(PatchInput{Origin: "missing", AllowCreate: true, ExpectedPubkey: "abc", ExpectedKind: KindDMRelays, ListOptimized: true, TagName: "relay", Desired: []string{"wss://dm.example"}})
	if !created.Safe || created.Event == nil || created.Event.Change != "creation" || !created.RequiresReview {
		t.Fatalf("create = %+v", created)
	}
}

func configEvent(id, pubkey string, created int64, relays []string) json.RawMessage {
	tags := make([][]string, 0, len(relays))
	for _, relay := range relays {
		tags = append(tags, []string{"r", relay})
	}
	raw, _ := json.Marshal(map[string]any{
		"id": id, "pubkey": pubkey, "created_at": created, "kind": KindPublicRelayList,
		"tags": tags, "content": "", "sig": "00",
	})
	return raw
}

func hasDeclared(relays []DeclaredRelay, url string) bool {
	for _, relay := range relays {
		if relay.URL == url {
			return true
		}
	}
	return false
}
