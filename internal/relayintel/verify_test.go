package relayintel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/gorilla/websocket"
)

func TestVerifyTwoRelaysCanSucceedAndOneCannotProveIt(t *testing.T) {
	raw := signedEvent(t, KindPublicRelayList, 1_700_000_000, [][]string{{"r", "wss://relay.example.com"}}, "")
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	left := relayServer(t, func(filter map[string]any) map[string]any { return event })
	right := relayServer(t, func(filter map[string]any) map[string]any { return event })
	defer left.Close()
	defer right.Close()
	one, err := VerifyEvent(context.Background(), raw, []string{wsURL(left)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if one.GlobalSuccess || one.Reason != "insufficient_relay_sample" || one.RetrievedCount != 1 {
		t.Fatalf("one relay = %+v", one)
	}
	two, err := VerifyEvent(context.Background(), raw, []string{wsURL(left), wsURL(right)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !two.SignatureValid || !two.GlobalSuccess || two.Reason != "ok" || two.RetrievedCount != 2 {
		t.Fatalf("two relays = %+v", two)
	}
}

func TestVerifyReportsConflictAndDoesNotDialABadSignature(t *testing.T) {
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	raw := signWith(t, priv, KindPublicRelayList, 1_700_000_000, [][]string{{"r", "wss://relay.example.com", "read"}}, "")
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	newerRaw := signWith(t, priv, KindPublicRelayList, 1_700_000_100, [][]string{{"r", "wss://newer.example.com"}}, "")
	var newer map[string]any
	if err := json.Unmarshal(newerRaw, &newer); err != nil {
		t.Fatal(err)
	}
	agree := relayServer(t, func(filter map[string]any) map[string]any {
		if _, ok := filter["ids"]; ok {
			return event
		}
		return event
	})
	conflict := relayServer(t, func(filter map[string]any) map[string]any {
		if _, ok := filter["ids"]; ok {
			return event
		}
		return newer
	})
	defer agree.Close()
	defer conflict.Close()
	result, err := VerifyEvent(context.Background(), raw, []string{wsURL(agree), wsURL(conflict)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if result.GlobalSuccess || result.Reason != "conflict" {
		t.Fatalf("conflict = %+v", result)
	}

	var hits atomic.Int32
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.NotFound(w, r)
	}))
	defer sink.Close()
	bad := append([]byte(nil), raw...)
	bad = []byte(strings.Replace(string(bad), `"sig":"`, `"sig":"00`, 1))
	invalid, err := VerifyEvent(context.Background(), bad, []string{wsURL(sink)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if invalid.SignatureValid || hits.Load() != 0 || invalid.Relays[0].Outcome != "not_queried" {
		t.Fatalf("invalid = %+v hits=%d", invalid, hits.Load())
	}
}

func TestVerifyIgnoresAForgedMatchingID(t *testing.T) {
	raw := signedEvent(t, KindPublicRelayList, 1_700_000_000, [][]string{{"r", "wss://relay.example.com"}}, "")
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	forged := map[string]any{"id": event["id"], "pubkey": event["pubkey"], "kind": float64(KindPublicRelayList), "created_at": float64(1_700_000_000)}
	server := relayServer(t, func(map[string]any) map[string]any { return forged })
	defer server.Close()
	result, err := VerifyEvent(context.Background(), raw, []string{wsURL(server), wsURL(server)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if result.GlobalSuccess || result.RetrievedCount != 0 {
		t.Fatalf("forged id was accepted: %+v", result)
	}
}

func TestVerifyClosedSubscriptionID(t *testing.T) {
	raw := signedEvent(t, KindPublicRelayList, 1_700_000_000, [][]string{{"r", "wss://relay.example.com"}}, "")
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serve := func(match bool) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			for {
				_, payload, err := conn.ReadMessage()
				if err != nil {
					return
				}
				var frame []json.RawMessage
				if json.Unmarshal(payload, &frame) != nil || len(frame) < 2 {
					continue
				}
				var sub string
				_ = json.Unmarshal(frame[1], &sub)
				closedSub := "someone-else"
				if match {
					closedSub = sub
				}
				_ = conn.WriteJSON([]any{"CLOSED", closedSub, "error: restricted"})
				if match {
					return
				}
				_ = conn.WriteJSON([]any{"EVENT", sub, event})
				_ = conn.WriteJSON([]any{"EOSE", sub})
			}
		}))
	}
	ignored := serve(false)
	defer ignored.Close()
	matched := serve(true)
	defer matched.Close()
	ok, err := VerifyEvent(context.Background(), raw, []string{wsURL(ignored)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(ok.Relays) != 1 || !ok.Relays[0].Retrieved || ok.Relays[0].Outcome == "closed" {
		t.Fatalf("unrelated CLOSED failed the subscription: %+v", ok.Relays)
	}
	closed, err := VerifyEvent(context.Background(), raw, []string{wsURL(matched)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(closed.Relays) != 1 || closed.Relays[0].Retrieved || closed.Relays[0].Outcome != "closed" {
		t.Fatalf("matching CLOSED = %+v", closed.Relays)
	}
}

func TestVerifyRejectsInternalTargets(t *testing.T) {
	raw := signedEvent(t, KindPublicRelayList, 1_700_000_000, [][]string{{"r", "wss://relay.example.com"}}, "")
	for _, target := range []string{"wss://169.254.169.254", "wss://metadata.google.internal", "ws://127.0.0.1"} {
		result, err := VerifyEvent(context.Background(), raw, []string{target, "wss://relay.example.com"}, false)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, item := range result.Relays {
			if strings.Contains(item.URL, "169.254") || strings.Contains(item.URL, "metadata") || strings.Contains(item.URL, "127.0.0.1") {
				found = true
				if item.Outcome != "rejected" {
					t.Fatalf("%s outcome = %+v", target, item)
				}
			}
		}
		if !found {
			t.Fatalf("missing rejection for %s in %+v", target, result.Relays)
		}
	}
	if _, err := VerifyEvent(context.Background(), raw, []string{
		"wss://a.example", "wss://b.example", "wss://c.example", "wss://d.example",
		"wss://e.example", "wss://f.example", "wss://g.example", "wss://h.example", "wss://i.example",
	}, false); err == nil {
		t.Fatal("nine relays were accepted")
	}
}

func TestVerifyIncompleteAndAddressableFilter(t *testing.T) {
	raw := signedEvent(t, KindPublicRelayList, 1_700_000_000, [][]string{{"r", "wss://relay.example.com"}}, "")
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	incomplete := relayFrames(t, func(sub string, filter map[string]any) []any {
		frames := []any{[]any{"EVENT", sub, event}}
		for i := 0; i < 20; i++ {
			frames = append(frames, []any{"NOTICE", "pending"})
		}
		return frames
	})
	defer incomplete.Close()
	result, err := VerifyEvent(context.Background(), raw, []string{wsURL(incomplete), wsURL(incomplete)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if result.SampleVerified || result.GlobalSuccess || result.Reason == "ok" {
		t.Fatalf("missing EOSE counted as verified: %+v", result)
	}

	set := signedEvent(t, KindRelaySet, 1_700_000_000, [][]string{{"d", "room:1"}, {"relay", "wss://set.example"}}, "")
	var setEvent map[string]any
	if err := json.Unmarshal(set, &setEvent); err != nil {
		t.Fatal(err)
	}
	var sawD atomic.Bool
	respond := func(filter map[string]any) map[string]any {
		if _, ok := filter["ids"]; !ok {
			if values, ok := filter["#d"].([]any); ok && len(values) == 1 && values[0] == "room:1" {
				sawD.Store(true)
			}
		}
		return setEvent
	}
	left := relayServer(t, respond)
	right := relayServer(t, respond)
	defer left.Close()
	defer right.Close()
	checked, err := VerifyEvent(context.Background(), set, []string{wsURL(left), wsURL(right)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !sawD.Load() {
		t.Fatal("addressable latest query omitted #d")
	}
	if !checked.SampleVerified {
		t.Fatalf("addressable sample = %+v", checked)
	}
}

func signedEvent(t *testing.T, kind int, created int64, tags [][]string, content string) []byte {
	t.Helper()
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	return signWith(t, priv, kind, created, tags, content)
}

func signWith(t *testing.T, priv *btcec.PrivateKey, kind int, created int64, tags [][]string, content string) []byte {
	t.Helper()
	if tags == nil {
		tags = [][]string{}
	}
	pub := hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))
	body, err := json.Marshal([]any{0, pub, created, kind, tags, content})
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	sig, err := schnorr.Sign(priv, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{
		"id": hex.EncodeToString(sum[:]), "pubkey": pub, "created_at": created,
		"kind": kind, "tags": tags, "content": content, "sig": hex.EncodeToString(sig.Serialize()),
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func relayServer(t *testing.T, respond func(map[string]any) map[string]any) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame []json.RawMessage
			if json.Unmarshal(payload, &frame) != nil || len(frame) < 3 {
				continue
			}
			var sub string
			_ = json.Unmarshal(frame[1], &sub)
			var filter map[string]any
			_ = json.Unmarshal(frame[2], &filter)
			event := respond(filter)
			_ = conn.WriteJSON([]any{"EVENT", sub, event})
			_ = conn.WriteJSON([]any{"EOSE", sub})
		}
	}))
}

func relayFrames(t *testing.T, respond func(sub string, filter map[string]any) []any) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var frame []json.RawMessage
		if json.Unmarshal(payload, &frame) != nil || len(frame) < 3 {
			return
		}
		var sub string
		_ = json.Unmarshal(frame[1], &sub)
		var filter map[string]any
		_ = json.Unmarshal(frame[2], &filter)
		for _, item := range respond(sub, filter) {
			_ = conn.WriteJSON(item)
		}
	}))
}

func wsURL(server *httptest.Server) string {
	return "ws" + strings.TrimPrefix(server.URL, "http")
}
