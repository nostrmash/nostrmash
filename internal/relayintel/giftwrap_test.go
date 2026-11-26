package relayintel

import (
	"bytes"
	"context"
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
	ack, authStatus := readWriteAckWithAuth(conn, "wanted", nil, time.Now().Add(2*time.Second), func(challenge string) (map[string]any, error) {
		if challenge != "challenge-1" {
			t.Fatalf("challenge = %s", challenge)
		}
		return signAuth(key, pubkey, wsURL(server), challenge)
	})
	if ack != "accepted" || authStatus != "sent" {
		t.Fatalf("ack=%s auth=%s", ack, authStatus)
	}
}

func TestNewerExposureOverridesOlderPrivacy(t *testing.T) {
	now := time.Now()
	older := now.Add(-time.Hour)
	newer := now.Add(time.Minute)
	stale := now.Add(-8 * 24 * time.Hour)
	exposed := map[string]Capability{
		CapGiftWrapPrivate: {Status: StatusVerified, Confidence: 0.8, ObservedAt: &older},
		CapGiftWrap:        {Status: StatusVerified, ObservedAt: &older},
		CapGiftWrapPublic:  {Status: StatusVerified, Confidence: 0.8, ObservedAt: &now},
	}
	if privacyAssessment(exposed) != "publicly_accessible" {
		t.Fatal(privacyAssessment(exposed))
	}
	_, _, reasons := privacyComponent(RoleDMInbox, exposed)
	if !reasonCode(reasons, "giftwrap_public") {
		t.Fatalf("reasons = %#v", reasons)
	}
	exposed[CapGiftWrapPrivate] = Capability{Status: StatusVerified, Confidence: 0.9, ObservedAt: &newer}
	if privacyAssessment(exposed) != "recipient_only_verified" {
		t.Fatal(privacyAssessment(exposed))
	}
	report := privacyReport(exposed, now)
	if !report.RecipientRetrievalVerified || report.ExposureUnresolved || !report.ExposureConfirmed {
		t.Fatalf("privacy report = %+v", report)
	}
	if privacyAssessmentAt(map[string]Capability{
		CapGiftWrapPrivate: {Status: StatusVerified, ObservedAt: &stale},
	}, now) != "stale" {
		t.Fatal("stale private evidence still counted as current")
	}
	if privacyAssessmentAt(map[string]Capability{
		CapGiftWrapPublic: {Status: StatusStale, ObservedAt: &stale},
	}, now) != "previous_exposure_unresolved" {
		t.Fatal("a stale exposure disappeared")
	}
	if unresolved := privacyReport(map[string]Capability{
		CapGiftWrapPublic: {Status: StatusStale, ObservedAt: &stale},
	}, now); unresolved.RecipientRetrievalVerified || !unresolved.ExposureUnresolved {
		t.Fatalf("unresolved exposure = %+v", unresolved)
	}
	if privacyAssessmentAt(map[string]Capability{
		CapGiftWrapPublic:  {Status: StatusVerified, ObservedAt: &older},
		CapGiftWrapPrivate: {Status: StatusVerified, ObservedAt: &now},
	}, now) != "recipient_only_verified" {
		t.Fatal("a newer complete private result did not replace exposure")
	}
	if classifyGiftWrap(giftWrapEvidence{Accept: "accepted", Observer: "incomplete", ObserverHit: true}) != "public" {
		t.Fatal("an observed unauthorized hit was discarded because EOSE did not arrive")
	}
}

func TestAuthAcknowledgementIsNotPublication(t *testing.T) {
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
		_ = conn.WriteJSON([]any{"AUTH", "challenge-2"})
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
		authID, _ := signed["id"].(string)
		_ = conn.WriteJSON([]any{"OK", authID, true, ""})
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
	ack, authStatus := readWriteAckWithAuth(conn, "wanted", nil, time.Now().Add(2*time.Second), func(challenge string) (map[string]any, error) {
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
	if classifyGiftWrap(giftWrapEvidence{
		Accept: "accepted", Checked: true,
		RecipientExact: accessResult{Status: "complete", Hit: true, HitAfterAuth: true, Authenticated: true},
		RecipientTag:   accessResult{Status: "complete", Hit: true, HitAfterAuth: true, Authenticated: true},
		ObserverExact:  accessResult{Status: "complete", Authenticated: true},
		ObserverTag:    accessResult{Status: "complete", Authenticated: true},
		AnonExact:      accessResult{Status: "complete"},
		AnonTag:        accessResult{Status: "complete"},
	}) != "private" {
		t.Fatal("recipient-only access was not recognized")
	}
	denied := giftWrapEvidence{
		Accept: "accepted", Checked: true,
		RecipientExact: accessResult{Status: "complete", Hit: true, HitAfterAuth: true, Authenticated: true},
		RecipientTag:   accessResult{Status: "complete", Hit: true, HitAfterAuth: true, Authenticated: true},
		ObserverExact:  accessResult{Status: "denied", Authenticated: true},
		ObserverTag:    accessResult{Status: "denied", Authenticated: true},
		AnonExact:      accessResult{Status: "auth_required"},
		AnonTag:        accessResult{Status: "auth_required"},
	}
	if classifyGiftWrap(denied) != "private" {
		t.Fatal("an explicit authorization denial was not negative evidence")
	}
	denied.ObserverExact.Status = "payment_required"
	if classifyGiftWrap(denied) == "private" {
		t.Fatal("a payment restriction proved recipient-only access")
	}
	denied.ObserverExact.Status = "rate_limited"
	if classifyGiftWrap(denied) == "private" {
		t.Fatal("a rate limit proved recipient-only access")
	}
	denied.ObserverExact.Status = "denied"
	denied.AnonExact.Status = "incomplete"
	if classifyGiftWrap(denied) == "private" {
		t.Fatal("an incomplete anonymous query proved recipient-only access")
	}
}

func TestGiftWrapProbeRequiresRecipientOnlyEvidence(t *testing.T) {
	sender, recipient, observer := newKey(t), newKey(t), newKey(t)
	recipientPub := pubOf(recipient)
	var published map[string]any
	server := giftWrapServer(t, recipientPub, &published, "private")
	defer server.Close()
	state := &probeState{
		key: sender, pubkey: pubOf(sender), allowPrivate: true,
		recipientKey: recipient, recipientPub: recipientPub,
		observerKey: observer, observerPub: pubOf(observer),
		allowlist: []string{probeHost(wsURL(server))},
	}
	evidence := state.exchangeGiftWrap(t.Context(), nil, wsURL(server))
	if evidence.Outcome != "private" || evidence.Accept != "accepted" {
		t.Fatalf("evidence = %+v", evidence)
	}
	publicServer := giftWrapServer(t, recipientPub, &published, "public")
	defer publicServer.Close()
	state.allowlist = []string{probeHost(wsURL(publicServer))}
	public := state.exchangeGiftWrap(t.Context(), nil, wsURL(publicServer))
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

func TestObserverQueriesTheRecipientTag(t *testing.T) {
	sender, recipient, observer := newKey(t), newKey(t), newKey(t)
	recipientPub := pubOf(recipient)
	var published map[string]any
	server := giftWrapServer(t, recipientPub, &published, "tag_leak")
	defer server.Close()
	state := &probeState{
		key: sender, pubkey: pubOf(sender), allowPrivate: true,
		recipientKey: recipient, recipientPub: recipientPub,
		observerKey: observer, observerPub: pubOf(observer),
		allowlist: []string{probeHost(wsURL(server))},
	}
	evidence := state.exchangeGiftWrap(t.Context(), nil, wsURL(server))
	if evidence.Outcome != "public" || !evidence.ObserverTag.Hit {
		t.Fatalf("tag leak evidence = %+v", evidence)
	}
	if evidence.ObserverExact.Hit {
		t.Fatal("exact-id retrieval was not protected")
	}
}

func TestPublicationIsRetriedAfterAuth(t *testing.T) {
	key := newKey(t)
	pubkey := pubOf(key)
	var sawRetry bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		authed := false
		events := 0
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
				events++
				var event map[string]any
				_ = json.Unmarshal(frame[1], &event)
				id, _ := event["id"].(string)
				if !authed {
					_ = conn.WriteJSON([]any{"OK", id, false, "auth-required: take a challenge"})
					_ = conn.WriteJSON([]any{"AUTH", "retry-challenge"})
					continue
				}
				sawRetry = events >= 2
				_ = conn.WriteJSON([]any{"OK", id, true, ""})
			case "AUTH":
				var event map[string]any
				_ = json.Unmarshal(frame[1], &event)
				if event["kind"] != float64(22242) {
					return
				}
				authID, _ := event["id"].(string)
				authed = true
				_ = conn.WriteJSON([]any{"OK", authID, true, ""})
			}
		}
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
	event, err := signProbeEvent(key, pubkey)
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal([]any{"EVENT", event})
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		t.Fatal(err)
	}
	id, _ := event["id"].(string)
	ack, authStatus := readWriteAckWithAuth(conn, id, payload, time.Now().Add(2*time.Second), func(challenge string) (map[string]any, error) {
		if challenge != "retry-challenge" {
			t.Fatalf("challenge = %s", challenge)
		}
		return signAuth(key, pubkey, wsURL(server), challenge)
	})
	if ack != "accepted" || authStatus != "ok" || !sawRetry {
		t.Fatalf("ack=%s auth=%s retry=%v", ack, authStatus, sawRetry)
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

func TestExposureBeforeAuthSurvivesAuthentication(t *testing.T) {
	sender, recipient, observer := newKey(t), newKey(t), newKey(t)
	recipientPub := pubOf(recipient)
	var published map[string]any
	server := giftWrapServer(t, recipientPub, &published, "expose_then_deny")
	defer server.Close()
	state := &probeState{
		key: sender, pubkey: pubOf(sender), allowPrivate: true,
		recipientKey: recipient, recipientPub: recipientPub,
		observerKey: observer, observerPub: pubOf(observer),
		allowlist: []string{probeHost(wsURL(server))},
	}
	evidence := state.exchangeGiftWrap(t.Context(), nil, wsURL(server))
	if evidence.Outcome != "public" {
		t.Fatalf("pre-auth exposure became %s: %+v", evidence.Outcome, evidence)
	}
	if !evidence.ObserverExact.SeenBeforeAuth || evidence.ObserverExact.HitAfterAuth || !evidence.AnonExact.Hit {
		t.Fatalf("unauthorized retrieval was not retained: %+v", evidence)
	}
	if !evidence.RecipientExact.SeenBeforeAuth || evidence.RecipientExact.HitAfterAuth {
		t.Fatalf("recipient pre-auth retrieval counted as authenticated access: %+v", evidence.RecipientExact)
	}
}

func TestRecipientPreAuthExposureIsNotAuthenticatedRetrieval(t *testing.T) {
	sender, recipient := newKey(t), newKey(t)
	recipientPub := pubOf(recipient)
	var published map[string]any
	server := giftWrapServer(t, recipientPub, &published, "expose_then_empty")
	defer server.Close()
	state := &probeState{
		key: sender, pubkey: pubOf(sender), allowPrivate: true,
		recipientKey: recipient, recipientPub: recipientPub,
		allowlist: []string{probeHost(wsURL(server))},
	}
	event, err := buildGiftWrap(sender, pubOf(sender), recipientPub)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := event["id"].(string)
	published = event
	result := state.readGiftWrap(t.Context(), wsURL(server), id, recipient, recipientPub, recipientPub, false, true)
	if !result.SeenBeforeAuth || result.HitAfterAuth || result.Hit {
		t.Fatalf("authenticated retrieval was manufactured from pre-auth exposure: %+v", result)
	}
	if !result.Authenticated || result.Status != "complete" {
		t.Fatalf("authenticated subscription did not complete: %+v", result)
	}
	evidence := giftWrapEvidence{
		Accept: "accepted", Checked: true,
		RecipientExact: result,
		RecipientTag:   accessResult{Status: "complete", Authenticated: true},
		ObserverExact:  accessResult{Status: "complete", Authenticated: true},
		ObserverTag:    accessResult{Status: "complete", Authenticated: true},
		AnonExact:      accessResult{Status: "complete"},
		AnonTag:        accessResult{Status: "auth_required"},
	}
	if classifyGiftWrap(evidence) == "private" {
		t.Fatal("pre-auth recipient exposure satisfied recipient-only verification")
	}
}

func TestSubscriptionRetriesAfterAuthRequiredClose(t *testing.T) {
	key := newKey(t)
	pubkey := pubOf(key)
	event, err := signProbeEvent(key, pubkey)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := event["id"].(string)
	var opened []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
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
			case "REQ":
				var sub string
				_ = json.Unmarshal(frame[1], &sub)
				opened = append(opened, sub)
				if len(opened) == 1 {
					_ = conn.WriteJSON([]any{"CLOSED", sub, "auth-required: sign in"})
					_ = conn.WriteJSON([]any{"AUTH", "sub-challenge"})
					continue
				}
				_ = conn.WriteJSON([]any{"EOSE", opened[0]})
				_ = conn.WriteJSON([]any{"EOSE", sub})
			case "AUTH":
				var signed map[string]any
				if json.Unmarshal(frame[1], &signed) != nil || signed["kind"] != float64(22242) {
					return
				}
				tags, _ := signed["tags"].([]any)
				var relayOK, challengeOK bool
				for _, raw := range tags {
					tag, _ := raw.([]any)
					if len(tag) < 2 {
						continue
					}
					name, _ := tag[0].(string)
					value, _ := tag[1].(string)
					if name == "relay" && value != "" {
						relayOK = true
					}
					if name == "challenge" && value == "sub-challenge" {
						challengeOK = true
					}
				}
				if !relayOK || !challengeOK {
					return
				}
				authID, _ := signed["id"].(string)
				_ = conn.WriteJSON([]any{"OK", authID, true, ""})
			}
		}
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
	status, ids, _, authenticated := readSubscription(context.Background(), conn, "nm-gift", map[string]any{"ids": []string{id}, "limit": 1}, time.Now().Add(2*time.Second), func(challenge string) (map[string]any, error) {
		if challenge != "sub-challenge" {
			t.Fatalf("challenge = %s", challenge)
		}
		return signAuth(key, pubkey, wsURL(server), challenge)
	})
	if status != "complete" || !authenticated || len(ids) != 0 {
		t.Fatalf("status=%s auth=%v ids=%v opened=%v", status, authenticated, ids, opened)
	}
	if len(opened) < 2 || opened[0] == opened[1] {
		t.Fatalf("subscription was not reopened: %v", opened)
	}
}

func TestOversizedFrameDoesNotCompleteASubscription(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
		frame := append([]byte(`["EOSE","nm-gift","`), bytes.Repeat([]byte("x"), 70*1024)...)
		frame = append(frame, ']', byte('"'), ']')
		_ = conn.WriteMessage(websocket.TextMessage, frame)
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
	status, ids, exposed, _ := readSubscription(context.Background(), conn, "nm-gift", map[string]any{"limit": 1}, time.Now().Add(2*time.Second), nil)
	if status != "incomplete" || len(ids) != 0 || len(exposed) != 0 {
		t.Fatalf("status=%s ids=%v exposed=%v", status, ids, exposed)
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

func giftWrapServer(t *testing.T, recipient string, published *map[string]any, mode string) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		authed := ""
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
				if event["kind"] == float64(22242) {
					authed, _ = event["pubkey"].(string)
					id, _ := event["id"].(string)
					_ = conn.WriteJSON([]any{"OK", id, true, ""})
					continue
				}
				*published = event
				id, _ := event["id"].(string)
				_ = conn.WriteJSON([]any{"OK", id, true, ""})
			case "AUTH":
				if len(frame) < 2 {
					continue
				}
				var event map[string]any
				if json.Unmarshal(frame[1], &event) != nil {
					continue
				}
				authed, _ = event["pubkey"].(string)
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
				if mode == "expose_then_empty" && authed == "" {
					if *published != nil {
						_ = conn.WriteJSON([]any{"EVENT", sub, *published})
					}
					_ = conn.WriteJSON([]any{"AUTH", "gift-challenge"})
					continue
				}
				if mode == "expose_then_empty" {
					_ = conn.WriteJSON([]any{"EOSE", sub})
					continue
				}
				if mode == "expose_then_deny" && authed == "" {
					if *published != nil {
						_ = conn.WriteJSON([]any{"EVENT", sub, *published})
					}
					_ = conn.WriteJSON([]any{"AUTH", "gift-challenge"})
					_ = conn.WriteJSON([]any{"EOSE", sub})
					continue
				}
				if mode == "expose_then_deny" {
					_ = conn.WriteJSON([]any{"CLOSED", sub, "restricted: not authorized"})
					continue
				}
				if mode != "public" && authed == "" {
					_ = conn.WriteJSON([]any{"AUTH", "gift-challenge"})
					_ = conn.WriteJSON([]any{"EOSE", sub})
					continue
				}
				if *published != nil && giftVisible(mode, recipient, authed, filter, published) {
					_ = conn.WriteJSON([]any{"EVENT", sub, *published})
				}
				_ = conn.WriteJSON([]any{"EOSE", sub})
			}
		}
	}))
}

func giftVisible(mode, recipient, authed string, filter map[string]any, published *map[string]any) bool {
	if published == nil || *published == nil {
		return false
	}
	id, _ := (*published)["id"].(string)
	exact := false
	if values, ok := filter["ids"].([]any); ok {
		for _, value := range values {
			if value == id {
				exact = true
			}
		}
	}
	tagged := false
	if values, ok := filter["#p"].([]any); ok {
		for _, value := range values {
			if value == recipient {
				tagged = true
			}
		}
	}
	switch mode {
	case "public":
		return true
	case "tag_leak":
		if exact {
			return authed == recipient
		}
		return tagged
	default:
		return authed == recipient && (exact || tagged)
	}
}
