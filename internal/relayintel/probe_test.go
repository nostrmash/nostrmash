package relayintel

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWriteAckRequiresTheSubmittedEventID(t *testing.T) {
	if classifyWriteFrame([]any{"OK", "other", true}, "wanted") != "mismatched" {
		t.Fatal("a true OK for a different id was accepted")
	}
	if classifyWriteFrame([]any{"OK", "wanted", false}, "wanted") != "rejected" {
		t.Fatal("a false OK was treated as acceptance")
	}
	if classifyWriteFrame([]any{"OK", "wanted", true}, "wanted") != "accepted" {
		t.Fatal("matching OK was rejected")
	}
	if classifyWriteFrame([]any{"NOTICE", "rate limited"}, "wanted") != "closed" {
		t.Fatal("notice was not distinguished from acceptance")
	}
}

func TestSearchVerificationNeedsANegativeMiss(t *testing.T) {
	if classifySearch(true, false) != "verified" {
		t.Fatal("a matching query with a clean negative query was not verified")
	}
	if classifySearch(true, true) != "ignored" {
		t.Fatal("a relay that returns the event for a non-matching query was verified")
	}
	if classifySearch(false, false) != "inconclusive" {
		t.Fatal("an empty search was treated as proof of no support")
	}
}

func TestReadWriteAckStopsOnAMismatchedOK(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteJSON([]any{"OK", "forged", true, ""})
		_ = conn.WriteJSON([]any{"OK", "wanted", true, ""})
	}))
	defer server.Close()
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(wsURL(server), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`["EVENT",{}]`))
	if got := readWriteAck(conn, "wanted", time.Now().Add(2*time.Second)); got != "mismatched" {
		t.Fatalf("ack = %s", got)
	}
}

func TestStaleGiftWrapIsNotVerifiedPrivacy(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	caps := ResolveCapabilities(Evidence{
		Now: now,
		Observations: []Observation{{
			Capability: CapGiftWrap, Status: StatusVerified, Confidence: 0.9,
			Source: "probe", ObservedAt: now.Add(-8 * 24 * time.Hour), SampleCount: 2,
		}},
	})
	if caps[CapGiftWrap].Status != StatusStale {
		t.Fatalf("status = %s", caps[CapGiftWrap].Status)
	}
	value, _, reasons := privacyComponent(RoleDMInbox, caps)
	if value > 0.5 {
		t.Fatalf("stale gift wrap still scored as private: %f", value)
	}
	for _, reason := range reasons {
		if reason.Code == "giftwrap_verified" {
			t.Fatal("stale observation was described as verified")
		}
	}
}
