package relayintel

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPatchPreservesEncryptedContentAndUnknownTags(t *testing.T) {
	ciphertext := "nip44-ciphertext-not-decrypted"
	raw := eventRaw(t, KindDMRelays, ciphertext, [][]string{
		{"relay", "wss://dm.example.com"},
		{"unknown", "keep-me", "extra"},
		{"r", "wss://also.example.com", "mystery"},
	})
	result := Patch(PatchInput{
		Source: raw, Origin: "client", ExpectedKind: KindDMRelays,
		ListOptimized: true, TagName: "relay",
		Desired: []string{"wss://dm.example.com", "wss://new.example.com"},
	})
	if !result.Safe || result.Event == nil || result.Event.Content != ciphertext {
		t.Fatalf("content was not preserved: %+v", result)
	}
	foundUnknown, foundMarker := false, false
	for _, tag := range result.Event.Tags {
		if len(tag) >= 2 && tag[0] == "unknown" && tag[1] == "keep-me" && tag[2] == "extra" {
			foundUnknown = true
		}
		if len(tag) >= 3 && tag[0] == "r" && tag[2] == "mystery" {
			foundMarker = true
		}
	}
	if !foundUnknown || !foundMarker {
		t.Fatalf("tags = %#v", result.Event.Tags)
	}
}

func TestPatchKeepsOutboxWhenOnlyInboxChanges(t *testing.T) {
	raw := eventRaw(t, KindPublicRelayList, "", [][]string{
		{"r", "wss://in.example.com", "read"},
		{"r", "wss://out.example.com", "write"},
		{"r", "wss://both.example.com"},
		{"client", "nostrmash"},
	})
	result := Patch(PatchInput{
		Source: raw, Origin: "indexed", ExpectedKind: KindPublicRelayList,
		InboxOptimized: true,
		Inbox:          []string{"wss://both.example.com", "wss://fresh.example.com"},
	})
	if !result.Safe || result.Event == nil {
		t.Fatalf("result = %+v", result)
	}
	write, read := map[string]string{}, map[string]string{}
	for _, tag := range result.Event.Tags {
		if len(tag) == 0 || tag[0] != "r" {
			continue
		}
		marker := ""
		if len(tag) >= 3 {
			marker = tag[2]
		}
		switch marker {
		case "write":
			write[tag[1]] = marker
		case "read":
			read[tag[1]] = marker
		default:
			read[tag[1]] = marker
			write[tag[1]] = marker
		}
	}
	if _, ok := write["wss://out.example.com"]; !ok {
		t.Fatalf("outbox was dropped: %#v", result.Event.Tags)
	}
	if _, ok := read["wss://in.example.com"]; ok {
		t.Fatalf("removed inbox relay survived: %#v", result.Event.Tags)
	}
	if _, ok := read["wss://fresh.example.com"]; !ok {
		t.Fatalf("new inbox relay missing: %#v", result.Event.Tags)
	}
	sawClient := false
	for _, tag := range result.Event.Tags {
		if tag[0] == "client" && tag[1] == "nostrmash" {
			sawClient = true
		}
	}
	if !sawClient {
		t.Fatalf("unrelated tag dropped: %#v", result.Event.Tags)
	}
}

func TestPatchDoesNotEmitWhenUnchangedOrUnsafe(t *testing.T) {
	raw := eventRaw(t, KindPublicRelayList, "keep", [][]string{{"r", "wss://both.example.com"}})
	same := Patch(PatchInput{
		Source: raw, Origin: "indexed", InboxOptimized: true, OutboxOptimized: true,
		Inbox: []string{"wss://both.example.com"}, Outbox: []string{"wss://both.example.com"},
	})
	if !same.Safe || !same.Unchanged || same.Event != nil {
		t.Fatalf("unchanged = %+v", same)
	}
	missing := Patch(PatchInput{InboxOptimized: true, Inbox: []string{"wss://new.example.com"}})
	if missing.Safe || missing.Event != nil || !missing.RequiresRefresh {
		t.Fatalf("missing = %+v", missing)
	}
	truncated := Patch(PatchInput{Source: raw, Truncated: true, InboxOptimized: true, Inbox: []string{"wss://other.example.com"}})
	if truncated.Safe || truncated.Event != nil || truncated.Blocking[0].Code != "source_truncated" {
		t.Fatalf("truncated = %+v", truncated)
	}
	kind3 := eventRaw(t, 3, "{}", [][]string{{"r", "wss://old.example.com"}})
	blocked := Patch(PatchInput{Source: kind3, ExpectedKind: 3, ListOptimized: true, Desired: []string{"wss://new.example.com"}})
	if blocked.Event != nil || blocked.Blocking[0].Code != "kind_3_not_emitted" {
		t.Fatalf("kind 3 = %+v", blocked)
	}
}

func TestPatchPreservesBlossomOrder(t *testing.T) {
	raw := eventRaw(t, KindBlossomServers, "", [][]string{
		{"server", "https://a.example"},
		{"server", "https://b.example"},
		{"server", "https://c.example"},
	})
	result := Patch(PatchInput{
		Source: raw, ListOptimized: true, TagName: "server",
		Desired: []string{"https://c.example", "https://a.example", "https://d.example"},
	})
	if !result.Safe || result.Event == nil {
		t.Fatalf("result = %+v", result)
	}
	var urls []string
	for _, tag := range result.Event.Tags {
		if tag[0] == "server" {
			urls = append(urls, tag[1])
		}
	}
	got := strings.Join(urls, ",")
	if got != "https://c.example,https://a.example,https://d.example" && got != "https://a.example,https://c.example,https://d.example" {
		t.Fatalf("order = %s", got)
	}
	if urls[len(urls)-1] != "https://d.example" {
		t.Fatalf("new server was not appended: %s", got)
	}
	if strings.Contains(got, "b.example") {
		t.Fatalf("removed server survived: %s", got)
	}
}

func eventRaw(t *testing.T, kind int, content string, tags [][]string) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"id": strings.Repeat("ab", 32), "pubkey": strings.Repeat("cd", 32),
		"created_at": 100, "kind": kind, "tags": tags, "content": content, "sig": strings.Repeat("ef", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
