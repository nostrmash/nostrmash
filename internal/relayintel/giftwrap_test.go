package relayintel

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/gorilla/websocket"
)

func TestAuthChallengeMustMatchTheSubmittedEvent(t *testing.T) {
	key := newKey(t)
	pubkey := pubOf(key)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteJSON([]any{"AUTH", "challenge-1"})
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var frame []json.RawMessage
		if json.Unmarshal(payload, &frame) != nil || len(frame) < 2 {
			return
		}
		var signed map[string]any
		if json.Unmarshal(frame[1], &signed) != nil {
			return
		}
		if signed["kind"] != float64(22242) {
			return
		}
		_ = conn.WriteJSON([]any{"OK", "wanted", true, ""})
	}))
	defer server.Close()
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL(server), nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`["EVENT",{}]`))
	ack, authStatus := readWriteAckWithAuth(conn, "wanted", time.Now().Add(2*time.Second), func(challenge string) (map[string]any, error) {
		if challenge != "challenge-1" {
			t.Fatalf("challenge = %s", challenge)
		}
		return signAuth(key, pubkey, wsURL(server), challenge)
	})
	if ack != "accepted" || authStatus != "ok" {
		t.Fatalf("ack=%s auth=%s", ack, authStatus)
	}
}

func TestGiftWrapDecryptsOnlyForTheRecipient(t *testing.T) {
	sender, recipient, observer := newKey(t), newKey(t), newKey(t)
	senderPub := pubOf(sender)
	recipientPub := pubOf(recipient)
	event, err := buildGiftWrap(sender, senderPub, recipientPub)
	if err != nil {
		t.Fatal(err)
	}
	content, err := openGiftWrap(recipient, recipientPub, event)
	if err != nil || content != "nostrmash relay intelligence gift-wrap probe" {
		t.Fatalf("content=%q err=%v", content, err)
	}
	if _, err := openGiftWrap(observer, pubOf(observer), event); err == nil {
		t.Fatal("an unrelated observer decrypted the gift wrap")
	}
	if classifyGiftWrap(giftWrapEvidence{Accept: "accepted", Recipient: "incomplete", Observer: "incomplete"}) != "accepted_only" {
		t.Fatal("incomplete queries were treated as private")
	}
	if classifyGiftWrap(giftWrapEvidence{Accept: "accepted", Recipient: "complete", RecipientHit: true, Observer: "incomplete"}) != "retrieved_privacy_unknown" {
		t.Fatal("an incomplete observer query proved privacy")
	}
	if classifyGiftWrap(giftWrapEvidence{Accept: "accepted", Recipient: "complete", RecipientHit: true, Observer: "complete", ObserverHit: true}) != "public" {
		t.Fatal("a public gift wrap was classified as private")
	}
	if classifyGiftWrap(giftWrapEvidence{Accept: "accepted", Recipient: "complete", RecipientHit: true, Observer: "complete"}) != "private" {
		t.Fatal("recipient-only access was not recognized")
	}
}

func TestGiftWrapProbeRequiresRecipientOnlyEvidence(t *testing.T) {
	sender, recipient, observer := newKey(t), newKey(t), newKey(t)
	recipientPub := pubOf(recipient)
	var published map[string]any
	server := giftWrapServer(t, recipientPub, &published, false)
	defer server.Close()
	state := &probeState{
		key: sender, pubkey: pubOf(sender), allowPrivate: true,
		recipientKey: recipient, recipientPub: recipientPub,
		observerKey: observer, observerPub: pubOf(observer),
		allowlist: []string{probeHost(wsURL(server))},
	}
	evidence := state.exchangeGiftWrap(t.Context(), wsURL(server))
	if evidence.Outcome != "private" || evidence.Accept != "accepted" {
		t.Fatalf("evidence = %+v", evidence)
	}
	publicServer := giftWrapServer(t, recipientPub, &published, true)
	defer publicServer.Close()
	state.allowlist = []string{probeHost(wsURL(publicServer))}
	public := state.exchangeGiftWrap(t.Context(), wsURL(publicServer))
	if public.Outcome != "public" {
		t.Fatalf("public evidence = %+v", public)
	}
}

func TestKind1WriteDoesNotVerifyKeyPackage(t *testing.T) {
	relay := ScoreRelay(ScoreInput{
		Role: RoleKeyPackage,
		Evidence: Evidence{Observations: []Observation{{
			Capability: CapWriteRetrieve, Status: StatusVerified, Confidence: 0.9, ObservedAt: time.Now(),
		}}},
	})
	for _, reason := range relay.Reasons {
		if reason.Code == "write_retrieved" || reason.Code == "keypackage_retrieved" {
			t.Fatalf("kind 1 retrieval counted as KeyPackage support: %#v", relay.Reasons)
		}
	}
	if !reasonCode(relay.Reasons, "keypackage_unknown") {
		t.Fatalf("reasons = %#v", relay.Reasons)
	}
}

func TestAcceptedGiftWrapIsNotPrivate(t *testing.T) {
	now := time.Now()
	_, _, reasons := privacyComponent(RoleDMInbox, ResolveCapabilities(Evidence{
		Now: now,
		Observations: []Observation{{
			Capability: CapGiftWrapAccept, Status: StatusVerified, Confidence: 0.8,
			Source: "probe", ObservedAt: now, SampleCount: 1,
		}},
	}))
	for _, reason := range reasons {
		if reason.Code == "giftwrap_verified" {
			t.Fatal("acceptance was scored as recipient-only access")
		}
	}
}

func newKey(t *testing.T) *btcec.PrivateKey {
	t.Helper()
	key, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func pubOf(key *btcec.PrivateKey) string {
	return hex.EncodeToString(schnorr.SerializePubKey(key.PubKey()))
}

func giftWrapServer(t *testing.T, recipient string, published *map[string]any, public bool) *httptest.Server {
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
			if json.Unmarshal(payload, &frame) != nil || len(frame) == 0 {
				continue
			}
			var typ string
			_ = json.Unmarshal(frame[0], &typ)
			switch typ {
			case "EVENT":
				if len(frame) < 2 {
					continue
				}
				var event map[string]any
				if json.Unmarshal(frame[1], &event) != nil {
					continue
				}
				*published = event
				id, _ := event["id"].(string)
				_ = conn.WriteJSON([]any{"OK", id, true, ""})
			case "REQ":
				if len(frame) < 3 {
					continue
				}
				var sub string
				_ = json.Unmarshal(frame[1], &sub)
				var filter map[string]any
				_ = json.Unmarshal(frame[2], &filter)
				returnEvent := public
				if values, ok := filter["#p"].([]any); ok {
					for _, value := range values {
						if value == recipient {
							returnEvent = true
						}
					}
				}
				if returnEvent && *published != nil {
					_ = conn.WriteJSON([]any{"EVENT", sub, *published})
				}
				_ = conn.WriteJSON([]any{"EOSE", sub})
			}
		}
	}))
}
