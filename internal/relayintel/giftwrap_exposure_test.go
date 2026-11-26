package relayintel

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestUnauthenticatedRetrievalIsExposure(t *testing.T) {
	sender, recipient, observer := newKey(t), newKey(t), newKey(t)
	recipientPub := pubOf(recipient)
	event, err := buildGiftWrap(sender, pubOf(sender), recipientPub)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := event["id"].(string)
	state := &probeState{
		key: sender, pubkey: pubOf(sender), allowPrivate: true,
		recipientKey: recipient, recipientPub: recipientPub,
		observerKey: observer, observerPub: pubOf(observer),
	}

	noChallenge := relayScript(t, func(conn *websocket.Conn, sub string) {
		_ = conn.WriteJSON([]any{"EVENT", sub, event})
		_ = conn.WriteJSON([]any{"EOSE", sub})
	})
	defer noChallenge.Close()
	state.allowlist = []string{probeHost(wsURL(noChallenge))}
	withoutAuth := state.readGiftWrap(t.Context(), wsURL(noChallenge), id, recipient, recipientPub, recipientPub, false, true)
	if !withoutAuth.RetrievedUnauthenticated || !withoutAuth.SeenBeforeAuth || withoutAuth.Authenticated || withoutAuth.HitAfterAuth {
		t.Fatalf("unauthenticated recipient retrieval was not retained: %+v", withoutAuth)
	}
	if classifyGiftWrap(giftWrapEvidence{Accept: "accepted", Checked: true, RecipientExact: withoutAuth}) != "public" {
		t.Fatal("a recipient connection that never saw an AUTH challenge was not exposure")
	}

	never := relayScript(t, func(conn *websocket.Conn, sub string) {
		_ = conn.WriteJSON([]any{"EVENT", sub, event})
		_ = conn.WriteJSON([]any{"AUTH", "never-ok"})
		_ = conn.Close()
	})
	defer never.Close()
	failedAuth := state.readGiftWrap(t.Context(), wsURL(never), id, recipient, recipientPub, recipientPub, false, true)
	if !failedAuth.SeenBeforeAuth || failedAuth.Authenticated || failedAuth.HitAfterAuth {
		t.Fatalf("exposure disappeared when authentication never succeeded: %+v", failedAuth)
	}

	returned := relayScript(t, func(conn *websocket.Conn, sub string) {
		authed := false
		_ = conn.WriteJSON([]any{"EVENT", sub, event})
		_ = conn.WriteJSON([]any{"AUTH", "again"})
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
			if typ == "AUTH" && len(frame) > 1 {
				var signed map[string]any
				_ = json.Unmarshal(frame[1], &signed)
				authID, _ := signed["id"].(string)
				authed = true
				_ = conn.WriteJSON([]any{"OK", authID, true, ""})
				continue
			}
			if typ == "REQ" && authed && len(frame) > 1 {
				var next string
				_ = json.Unmarshal(frame[1], &next)
				_ = conn.WriteJSON([]any{"EVENT", next, event})
				_ = conn.WriteJSON([]any{"EOSE", next})
				return
			}
		}
	})
	defer returned.Close()
	both := state.readGiftWrap(t.Context(), wsURL(returned), id, recipient, recipientPub, recipientPub, false, true)
	if !both.SeenBeforeAuth || !both.HitAfterAuth || !both.Authenticated || both.AuthIdentity != recipientPub {
		t.Fatalf("pre-auth exposure was replaced by the later authenticated hit: %+v", both)
	}
	if classifyGiftWrap(giftWrapEvidence{
		Accept: "accepted", Checked: true, RecipientExact: both,
		RecipientTag:  accessResult{Status: "complete", Hit: true, HitAfterAuth: true, Authenticated: true},
		ObserverExact: accessResult{Status: "complete", Authenticated: true},
		ObserverTag:   accessResult{Status: "complete", Authenticated: true},
		AnonExact:     accessResult{Status: "complete"},
		AnonTag:       accessResult{Status: "auth_required"},
	}) != "public" {
		t.Fatal("authenticated retrieval cleared an earlier unauthorized hit")
	}

	anon := state.readGiftWrap(t.Context(), wsURL(noChallenge), id, nil, "", recipientPub, false, false)
	if !anon.Hit || anon.Authenticated || !anon.RetrievedUnauthenticated {
		t.Fatalf("anonymous exposure = %+v", anon)
	}
	observerResult := state.readGiftWrap(t.Context(), wsURL(returned), id, observer, pubOf(observer), recipientPub, false, true)
	if !observerResult.SeenBeforeAuth || !observerResult.HitAfterAuth {
		t.Fatalf("observer exposure before and after auth = %+v", observerResult)
	}
}

func TestValidSixPathStaysPrivate(t *testing.T) {
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
	if evidence.Outcome != "private" {
		t.Fatalf("six-path result = %s %+v", evidence.Outcome, evidence)
	}
	if !evidence.RecipientExact.HitAfterAuth || evidence.RecipientExact.SeenBeforeAuth || !evidence.RecipientExact.Authenticated {
		t.Fatalf("recipient exact = %+v", evidence.RecipientExact)
	}
	if evidence.AnonExact.Hit || evidence.ObserverExact.Hit || evidence.ObserverTag.Hit {
		t.Fatalf("unauthorized path returned the event: %+v", evidence)
	}
	evidence.AnonTag.SeenBeforeAuth = true
	if classifyGiftWrap(evidence) == "private" {
		t.Fatal("one unauthorized hit left the recipient-only result intact")
	}
}

func relayScript(t *testing.T, onReq func(conn *websocket.Conn, sub string)) *httptest.Server {
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
			if json.Unmarshal(payload, &frame) != nil || len(frame) < 2 {
				continue
			}
			var typ, sub string
			_ = json.Unmarshal(frame[0], &typ)
			_ = json.Unmarshal(frame[1], &sub)
			if typ == "REQ" {
				onReq(conn, sub)
				return
			}
		}
	}))
}

func TestIncompleteExposureIsNotVerifiedNegative(t *testing.T) {
	timeout := accessResult{Status: "timeout", AuthAttempted: true}
	completeMiss := accessResult{Status: "complete", Authenticated: true, SubscriptionComplete: true, AuthAttempted: true}
	anonMiss := accessResult{Status: "complete"}
	preAuth := accessResult{Status: "incomplete", SeenBeforeAuth: true, RetrievedUnauthenticated: true, Hit: true, AuthAttempted: true}
	both := accessResult{Status: "complete", Authenticated: true, HitAfterAuth: true, SeenBeforeAuth: true, RetrievedUnauthenticated: true, Hit: true, SubscriptionComplete: true, AuthAttempted: true, AuthIdentity: "recipient"}

	writes := giftWrapPathWrites(giftWrapEvidence{
		Checked: true, Accept: "accepted",
		RecipientExact: completeMiss, RecipientTag: completeMiss,
		ObserverExact: timeout, ObserverTag: completeMiss,
		AnonExact: anonMiss, AnonTag: accessResult{Status: "auth_required"},
	})
	auth := findWrite(t, writes, CapGiftWrapAuthExposure)
	unauth := findWrite(t, writes, CapGiftWrapUnauthExposure)
	if auth.Status != "inconclusive" || auth.Retrieved != nil || unauth.Status != "inconclusive" || unauth.Retrieved != nil {
		t.Fatalf("incomplete observer produced negative exposure: auth=%+v unauth=%+v", auth, unauth)
	}
	exact := findWrite(t, writes, CapGiftWrapRecipientExact)
	if exact.Retrieved == nil || *exact.Retrieved || exact.Status != StatusVerified {
		t.Fatalf("completed authenticated miss = %+v", exact)
	}

	anonTimeout := giftWrapPathWrites(giftWrapEvidence{
		Checked:        true,
		RecipientExact: completeMiss, RecipientTag: completeMiss,
		ObserverExact: completeMiss, ObserverTag: completeMiss,
		AnonExact: accessResult{Status: "timeout"}, AnonTag: anonMiss,
	})
	if findWrite(t, anonTimeout, CapGiftWrapUnauthExposure).Status != "inconclusive" {
		t.Fatal("an incomplete anonymous query verified that nothing was exposed")
	}

	partialRecipient := giftWrapPathWrites(giftWrapEvidence{
		Checked:        true,
		RecipientExact: completeMiss, RecipientTag: timeout,
		ObserverExact: completeMiss, ObserverTag: completeMiss,
		AnonExact: anonMiss, AnonTag: accessResult{Status: "auth_required"},
	})
	if _, ok := writeByCapability(partialRecipient, CapGiftWrapRecipient); ok {
		t.Fatal("one completed recipient path established aggregate retrieval")
	}
	if findWrite(t, partialRecipient, CapGiftWrapRecipientTag).Status != "inconclusive" {
		t.Fatal("an incomplete recipient-tag query was verified")
	}

	hitThenTimeout := giftWrapPathWrites(giftWrapEvidence{
		Checked:        true,
		RecipientExact: preAuth,
		ObserverExact:  timeout,
	})
	exposed := findWrite(t, hitThenTimeout, CapGiftWrapUnauthExposure)
	if exposed.Status != StatusVerified || exposed.Retrieved == nil || !*exposed.Retrieved {
		t.Fatalf("unauthorized hit followed by timeout = %+v", exposed)
	}
	if findWrite(t, hitThenTimeout, CapGiftWrapRecipientExact).Retrieved != nil {
		t.Fatal("pre-authentication retrieval counted as authenticated recipient delivery")
	}

	followed := giftWrapPathWrites(giftWrapEvidence{
		Checked:        true,
		RecipientExact: both, RecipientTag: completeMiss,
		ObserverExact: completeMiss, ObserverTag: completeMiss,
		AnonExact: anonMiss, AnonTag: accessResult{Status: "auth_required"},
	})
	if findWrite(t, followed, CapGiftWrapRecipientExact).Retrieved == nil || !*findWrite(t, followed, CapGiftWrapRecipientExact).Retrieved {
		t.Fatal("authenticated retrieval after exposure was dropped")
	}
	if findWrite(t, followed, CapGiftWrapUnauthExposure).Retrieved == nil || !*findWrite(t, followed, CapGiftWrapUnauthExposure).Retrieved {
		t.Fatal("pre-authentication exposure was cleared by the later authenticated retrieval")
	}

	negative := giftWrapPathWrites(giftWrapEvidence{
		Checked:        true,
		RecipientExact: completeMiss, RecipientTag: completeMiss,
		ObserverExact: completeMiss, ObserverTag: completeMiss,
		AnonExact: anonMiss, AnonTag: accessResult{Status: "auth_required"},
	})
	if got := findWrite(t, negative, CapGiftWrapUnauthExposure); got.Status != StatusVerified || got.Retrieved == nil || *got.Retrieved {
		t.Fatalf("complete negative observer tests = %+v", got)
	}
	if got := findWrite(t, negative, CapGiftWrapAuthExposure); got.Status != StatusVerified || got.Retrieved == nil || *got.Retrieved {
		t.Fatalf("complete negative authenticated observer tests = %+v", got)
	}

	closed := relayScript(t, func(conn *websocket.Conn, sub string) {
		_ = conn.Close()
	})
	defer closed.Close()
	sender, recipient, observer := newKey(t), newKey(t), newKey(t)
	state := &probeState{key: sender, pubkey: pubOf(sender), allowPrivate: true, recipientKey: recipient, recipientPub: pubOf(recipient), observerKey: observer, observerPub: pubOf(observer)}
	live := state.readGiftWrap(t.Context(), wsURL(closed), strings.Repeat("ab", 32), observer, pubOf(observer), pubOf(recipient), false, true)
	if live.Status == "complete" || live.Hit || live.HitAfterAuth {
		t.Fatalf("closed observer query = %+v", live)
	}
	liveWrites := giftWrapPathWrites(giftWrapEvidence{Checked: true, ObserverExact: live, ObserverTag: completeMiss})
	if findWrite(t, liveWrites, CapGiftWrapAuthExposure).Status == StatusVerified {
		t.Fatal("a closed authenticated observer query became verified negative evidence")
	}
}

func findWrite(t *testing.T, writes []observationWrite, capability string) observationWrite {
	t.Helper()
	write, ok := writeByCapability(writes, capability)
	if !ok {
		t.Fatalf("missing %s", capability)
	}
	return write
}

func writeByCapability(writes []observationWrite, capability string) (observationWrite, bool) {
	for _, write := range writes {
		if write.Capability == capability {
			return write, true
		}
	}
	return observationWrite{}, false
}

func TestPrivacyFieldsStayIndependent(t *testing.T) {
	now := time.Now()
	older := now.Add(-time.Hour)
	stale := now.Add(-8 * 24 * time.Hour)
	accepted := privacyReport(map[string]Capability{
		CapGiftWrapAccept: {Status: StatusVerified, Source: "probe", ObservedAt: &now},
	}, now)
	if accepted.Assessment != "privacy_unknown" || accepted.GiftWrapAccepted == nil || !*accepted.GiftWrapAccepted || accepted.RecipientRetrievalVerified != nil {
		t.Fatalf("accepted only = %+v", accepted)
	}
	retrieved := privacyReport(map[string]Capability{
		CapGiftWrapRecipient: {Status: StatusVerified, Source: "probe", ObservedAt: &now, Retrieved: boolPtr(true)},
	}, now)
	if retrieved.Assessment != "privacy_unknown" || retrieved.RecipientRetrievalVerified == nil || !*retrieved.RecipientRetrievalVerified || retrieved.Comprehensive {
		t.Fatalf("retrieval without privacy = %+v", retrieved)
	}
	exposed := privacyReport(map[string]Capability{
		CapGiftWrapRecipient:      {Status: StatusVerified, Source: "probe", ObservedAt: &now, Retrieved: boolPtr(true)},
		CapGiftWrapPublic:         {Status: StatusVerified, Source: "probe", ObservedAt: &now, Confidence: 0.8},
		CapGiftWrapUnauthExposure: {Status: StatusVerified, Source: "probe", ObservedAt: &now, Retrieved: boolPtr(true)},
	}, now)
	if exposed.Assessment != "publicly_accessible" || exposed.RecipientRetrievalVerified == nil || !*exposed.RecipientRetrievalVerified || !exposed.ExposureConfirmed || !exposed.ExposureUnresolved {
		t.Fatalf("retrieval with exposure = %+v", exposed)
	}
	private := privacyReport(map[string]Capability{
		CapGiftWrapPrivate: {Status: StatusVerified, Source: "probe", ObservedAt: &now, Confidence: 0.8, EvidenceVersion: ComprehensivePrivacyVersion, Freshness: "fresh"},
		CapAuthRecipient:   {Status: StatusVerified, Source: "probe"},
		CapAuthGiftWrap:    {Status: StatusVerified, Source: "probe"},
		CapAuthWrite:       {Status: StatusRestricted, Source: "probe"},
	}, now)
	if private.Assessment != "recipient_only_verified" || !private.Comprehensive || private.ExposureUnresolved || private.PublicationAuthVerified == nil || !*private.PublicationAuthVerified || private.RecipientAuthVerified == nil || !*private.RecipientAuthVerified {
		t.Fatalf("verified private = %+v", private)
	}
	if privacyReport(map[string]Capability{
		CapGiftWrapPrivate: {Status: StatusVerified, Source: "probe", ObservedAt: &stale, EvidenceVersion: ComprehensivePrivacyVersion},
	}, now).Assessment != "stale" {
		t.Fatal("stale comprehensive evidence stayed current")
	}
	historical := privacyReport(map[string]Capability{
		CapGiftWrapPublic: {Status: StatusStale, Source: "probe", ObservedAt: &stale},
	}, now)
	if historical.Assessment != "previous_exposure_unresolved" || !historical.ExposureUnresolved || !historical.ExposureConfirmed {
		t.Fatalf("historical exposure = %+v", historical)
	}
	conflict := privacyReport(map[string]Capability{
		CapGiftWrapPublic:  {Status: StatusVerified, Source: "probe", ObservedAt: &now},
		CapGiftWrapPrivate: {Status: StatusVerified, Source: "probe", ObservedAt: &now, EvidenceVersion: ComprehensivePrivacyVersion},
	}, now)
	if conflict.Assessment != "conflicting_evidence" || !conflict.ExposureUnresolved {
		t.Fatalf("conflict = %+v", conflict)
	}
	resolved := privacyReport(map[string]Capability{
		CapGiftWrapPublic:  {Status: StatusVerified, Source: "probe", ObservedAt: &older},
		CapGiftWrapPrivate: {Status: StatusVerified, Source: "probe", ObservedAt: &now, EvidenceVersion: ComprehensivePrivacyVersion, Confidence: 0.9, Freshness: "fresh"},
	}, now)
	if resolved.Assessment != "recipient_only_verified" || resolved.ExposureUnresolved || !resolved.ExposureConfirmed || resolved.EvidenceVersion != ComprehensivePrivacyVersion {
		t.Fatalf("resolved exposure = %+v", resolved)
	}
	publicationOnly := privacyReport(map[string]Capability{
		CapAuthGiftWrap: {Status: StatusVerified, Source: "probe"},
		CapAuthWrite:    {Status: StatusVerified, Source: "probe"},
	}, now)
	if publicationOnly.PublicationAuthVerified == nil || !*publicationOnly.PublicationAuthVerified || publicationOnly.RecipientAuthVerified != nil {
		t.Fatalf("publication auth was confused with recipient or kind 1 auth: %+v", publicationOnly)
	}
	kind1 := privacyReport(map[string]Capability{
		CapAuthWrite: {Status: StatusVerified, Source: "probe"},
	}, now)
	if kind1.PublicationAuthVerified != nil || kind1.RecipientAuthVerified != nil {
		t.Fatalf("kind 1 auth satisfied gift-wrap authentication: %+v", kind1)
	}
	body, err := json.Marshal(exposed)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["assessment"] != "publicly_accessible" || decoded["recipient_retrieval_verified"] != true || decoded["exposure_confirmed"] != true || decoded["exposure_unresolved"] != true {
		t.Fatalf("api privacy = %s", body)
	}
	if _, ok := decoded["publication_auth_verified"]; !ok || decoded["publication_auth_verified"] != nil {
		t.Fatalf("unknown publication auth was not null: %s", body)
	}
}
