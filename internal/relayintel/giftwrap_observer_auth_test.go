package relayintel

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestAuthenticatedObserverDenialRequiresAuthentication(t *testing.T) {
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
	recipientHit := accessResult{Status: "complete", Authenticated: true, Hit: true, HitAfterAuth: true, SubscriptionComplete: true, AuthAttempted: true}
	anonMiss := accessResult{Status: "complete"}
	anonDenied := accessResult{Status: "auth_required"}

	rejectedServer := observerAuthRelay(t, false, nil)
	defer rejectedServer.Close()
	rejected := state.readGiftWrap(t.Context(), wsURL(rejectedServer), id, observer, pubOf(observer), recipientPub, false, true)
	rejectedAgain := state.readGiftWrap(t.Context(), wsURL(rejectedServer), id, observer, pubOf(observer), recipientPub, true, true)
	if rejected.Authenticated || rejected.Status != "unauthorized" || rejectedAgain.Authenticated || rejectedAgain.Status != "unauthorized" {
		t.Fatalf("observer AUTH rejection = %+v %+v", rejected, rejectedAgain)
	}
	rejectedEvidence := giftWrapEvidence{
		Accept: "accepted", Checked: true,
		RecipientExact: recipientHit, RecipientTag: recipientHit,
		ObserverExact: rejected, ObserverTag: rejectedAgain,
		AnonExact: anonMiss, AnonTag: anonDenied,
	}
	rejectedReport, rejectedWrites := assessGiftWrap(rejectedEvidence)
	if got := findWrite(t, rejectedWrites, CapGiftWrapObserverExact); got.Status != "inconclusive" || got.Retrieved != nil {
		t.Fatalf("AUTH rejection became observer evidence: %+v", got)
	}
	if got := findWrite(t, rejectedWrites, CapGiftWrapAuthExposure); got.Status != "inconclusive" || got.Retrieved != nil {
		t.Fatalf("AUTH rejection verified authenticated-observer privacy: %+v", got)
	}
	if rejectedReport.Assessment == "recipient_only_verified" || rejectedReport.AuthenticatedObserverExposure != nil || rejectedReport.Comprehensive {
		t.Fatalf("rejected observer authentication = %+v", rejectedReport)
	}

	incompleteServer := relayScript(t, func(conn *websocket.Conn, sub string) {
		_ = conn.WriteJSON([]any{"AUTH", "incomplete"})
		_, _, _ = conn.ReadMessage()
		_ = conn.Close()
	})
	defer incompleteServer.Close()
	incomplete := state.readGiftWrap(t.Context(), wsURL(incompleteServer), id, observer, pubOf(observer), recipientPub, false, true)
	if incomplete.Authenticated || incomplete.Status == "complete" || incomplete.Status == "denied" || incomplete.Status == "unauthorized" {
		t.Fatalf("incomplete observer authentication = %+v", incomplete)
	}
	incompleteReport, incompleteWrites := assessGiftWrap(giftWrapEvidence{
		Accept: "accepted", Checked: true,
		RecipientExact: recipientHit, RecipientTag: recipientHit,
		ObserverExact: incomplete, ObserverTag: incomplete,
		AnonExact: anonMiss, AnonTag: anonDenied,
	})
	if got := findWrite(t, incompleteWrites, CapGiftWrapAuthExposure); got.Status != "inconclusive" || got.Retrieved != nil || incompleteReport.AuthenticatedObserverExposure != nil {
		t.Fatalf("incomplete observer authentication = field %+v report %+v", got, incompleteReport)
	}

	deniedServer := observerAuthRelay(t, true, func(conn *websocket.Conn, sub string) {
		_ = conn.WriteJSON([]any{"CLOSED", sub, "restricted: not the recipient"})
	})
	defer deniedServer.Close()
	deniedExact := state.readGiftWrap(t.Context(), wsURL(deniedServer), id, observer, pubOf(observer), recipientPub, false, true)
	deniedTag := state.readGiftWrap(t.Context(), wsURL(deniedServer), id, observer, pubOf(observer), recipientPub, true, true)
	if !deniedExact.Authenticated || deniedExact.Status != "denied" || !deniedTag.Authenticated || deniedTag.Status != "denied" {
		t.Fatalf("authenticated subscription denial = %+v %+v", deniedExact, deniedTag)
	}
	deniedReport, deniedWrites := assessGiftWrap(giftWrapEvidence{
		Accept: "accepted", Checked: true,
		RecipientExact: recipientHit, RecipientTag: recipientHit,
		ObserverExact: deniedExact, ObserverTag: deniedTag,
		AnonExact: anonMiss, AnonTag: anonDenied,
	})
	if got := findWrite(t, deniedWrites, CapGiftWrapObserverExact); got.Status != StatusVerified || got.Retrieved == nil || *got.Retrieved {
		t.Fatalf("authenticated denial = %+v", got)
	}
	if deniedReport.Assessment != "recipient_only_verified" || !deniedReport.Comprehensive || deniedReport.EvidenceVersion != ComprehensivePrivacyVersion || deniedReport.AuthenticatedObserverExposure == nil || *deniedReport.AuthenticatedObserverExposure || deniedReport.RecipientRetrievalVerified == nil || !*deniedReport.RecipientRetrievalVerified {
		t.Fatalf("denial after authentication = %+v", deniedReport)
	}

	emptyServer := observerAuthRelay(t, true, func(conn *websocket.Conn, sub string) {
		_ = conn.WriteJSON([]any{"EOSE", sub})
	})
	defer emptyServer.Close()
	emptyExact := state.readGiftWrap(t.Context(), wsURL(emptyServer), id, observer, pubOf(observer), recipientPub, false, true)
	emptyTag := state.readGiftWrap(t.Context(), wsURL(emptyServer), id, observer, pubOf(observer), recipientPub, true, true)
	if !emptyExact.Authenticated || emptyExact.Status != "complete" || emptyExact.HitAfterAuth || !emptyTag.Authenticated || emptyTag.HitAfterAuth {
		t.Fatalf("authenticated empty subscription = %+v %+v", emptyExact, emptyTag)
	}
	emptyReport, emptyWrites := assessGiftWrap(giftWrapEvidence{
		Accept: "accepted", Checked: true,
		RecipientExact: recipientHit, RecipientTag: recipientHit,
		ObserverExact: emptyExact, ObserverTag: emptyTag,
		AnonExact: anonMiss, AnonTag: anonDenied,
	})
	if got := findWrite(t, emptyWrites, CapGiftWrapObserverTag); got.Status != StatusVerified || got.Retrieved == nil || *got.Retrieved {
		t.Fatalf("empty authenticated subscription = %+v", got)
	}
	if emptyReport.Assessment != "recipient_only_verified" || emptyReport.AuthenticatedObserverExposure == nil || *emptyReport.AuthenticatedObserverExposure {
		t.Fatalf("empty authenticated observer = %+v", emptyReport)
	}

	retrievedServer := observerAuthRelay(t, true, func(conn *websocket.Conn, sub string) {
		_ = conn.WriteJSON([]any{"EVENT", sub, event})
		_ = conn.WriteJSON([]any{"EOSE", sub})
	})
	defer retrievedServer.Close()
	retrievedExact := state.readGiftWrap(t.Context(), wsURL(retrievedServer), id, observer, pubOf(observer), recipientPub, false, true)
	if !retrievedExact.Authenticated || !retrievedExact.HitAfterAuth {
		t.Fatalf("authenticated observer retrieval = %+v", retrievedExact)
	}
	retrievedReport, retrievedWrites := assessGiftWrap(giftWrapEvidence{
		Accept: "accepted", Checked: true,
		RecipientExact: recipientHit, RecipientTag: recipientHit,
		ObserverExact: retrievedExact, ObserverTag: emptyTag,
		AnonExact: anonMiss, AnonTag: anonDenied,
	})
	if got := findWrite(t, retrievedWrites, CapGiftWrapObserverExact); got.Status != StatusVerified || got.Retrieved == nil || !*got.Retrieved {
		t.Fatalf("observer retrieval field = %+v", got)
	}
	if retrievedReport.Assessment != "publicly_accessible" || retrievedReport.AuthenticatedObserverExposure == nil || !*retrievedReport.AuthenticatedObserverExposure || !retrievedReport.ExposureConfirmed || retrievedReport.Comprehensive {
		t.Fatalf("observer retrieval assessment = %+v", retrievedReport)
	}

	timeoutServer := relayScript(t, func(conn *websocket.Conn, _ string) {
		time.Sleep(6 * time.Second)
		_ = conn.Close()
	})
	defer timeoutServer.Close()
	timedOut := state.readGiftWrap(t.Context(), wsURL(timeoutServer), id, observer, pubOf(observer), recipientPub, true, true)
	if timedOut.Status == "complete" || timedOut.HitAfterAuth {
		t.Fatalf("observer timeout = %+v", timedOut)
	}
	followedReport, followedWrites := assessGiftWrap(giftWrapEvidence{
		Accept: "accepted", Checked: true,
		RecipientExact: recipientHit, RecipientTag: recipientHit,
		ObserverExact: retrievedExact, ObserverTag: timedOut,
		AnonExact: anonMiss, AnonTag: anonDenied,
	})
	if got := findWrite(t, followedWrites, CapGiftWrapAuthExposure); got.Status != StatusVerified || got.Retrieved == nil || !*got.Retrieved {
		t.Fatalf("retrieval followed by timeout = %+v", got)
	}
	if followedReport.Assessment != "publicly_accessible" || !followedReport.ExposureConfirmed || followedReport.AuthenticatedObserverExposure == nil || !*followedReport.AuthenticatedObserverExposure {
		t.Fatalf("timeout erased observer exposure: %+v", followedReport)
	}

	anonServer := relayScript(t, func(conn *websocket.Conn, sub string) {
		_ = conn.WriteJSON([]any{"CLOSED", sub, "auth-required: sign in"})
	})
	defer anonServer.Close()
	anonClosed := state.readGiftWrap(t.Context(), wsURL(anonServer), id, nil, "", recipientPub, false, false)
	if anonClosed.Authenticated || anonClosed.Status != "auth_required" {
		t.Fatalf("anonymous denial = %+v", anonClosed)
	}
	splitReport, splitWrites := assessGiftWrap(giftWrapEvidence{
		Accept: "accepted", Checked: true,
		RecipientExact: recipientHit, RecipientTag: recipientHit,
		ObserverExact: incomplete, ObserverTag: incomplete,
		AnonExact: anonClosed, AnonTag: anonDenied,
	})
	if got := findWrite(t, splitWrites, CapGiftWrapAnonExact); got.Status != StatusVerified || got.Retrieved == nil || *got.Retrieved {
		t.Fatalf("anonymous denial = %+v", got)
	}
	if got := findWrite(t, splitWrites, CapGiftWrapAuthExposure); got.Status != "inconclusive" || got.Retrieved != nil || splitReport.AuthenticatedObserverExposure != nil || splitReport.Assessment == "recipient_only_verified" {
		t.Fatalf("anonymous denial stood in for the observer: field %+v report %+v", got, splitReport)
	}
}

func observerAuthRelay(t *testing.T, accept bool, after func(conn *websocket.Conn, sub string)) *httptest.Server {
	t.Helper()
	return relayScript(t, func(conn *websocket.Conn, _ string) {
		if err := conn.WriteJSON([]any{"AUTH", "observer-challenge"}); err != nil {
			return
		}
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
			if typ != "AUTH" || len(frame) < 2 {
				continue
			}
			var signed map[string]any
			_ = json.Unmarshal(frame[1], &signed)
			authID, _ := signed["id"].(string)
			if !accept {
				_ = conn.WriteJSON([]any{"OK", authID, false, "rejected"})
				return
			}
			if err := conn.WriteJSON([]any{"OK", authID, true, ""}); err != nil {
				return
			}
			for {
				_, payload, err = conn.ReadMessage()
				if err != nil {
					return
				}
				frame = nil
				if json.Unmarshal(payload, &frame) != nil || len(frame) < 2 {
					continue
				}
				typ = ""
				_ = json.Unmarshal(frame[0], &typ)
				if typ != "REQ" {
					continue
				}
				var sub string
				_ = json.Unmarshal(frame[1], &sub)
				if after != nil {
					after(conn, sub)
				}
				return
			}
		}
	})
}

func assessGiftWrap(evidence giftWrapEvidence) (PrivacyReport, []observationWrite) {
	evidence.Outcome = classifyGiftWrap(evidence)
	writes := giftWrapPathWrites(evidence)
	switch evidence.Outcome {
	case "private":
		writes = append(writes, observationWrite{
			Capability: CapGiftWrapPrivate, Status: StatusVerified, Detail: "recipient_only",
			Confidence: 0.8, EvidenceVersion: ComprehensivePrivacyVersion,
		})
	case "public":
		writes = append(writes, observationWrite{
			Capability: CapGiftWrapPublic, Status: StatusVerified, Detail: "exposed",
			Confidence: 0.8, Retrieved: boolPtr(true),
		})
	default:
		writes = append(writes, observationWrite{
			Capability: CapGiftWrapPrivate, Status: "inconclusive", Detail: evidence.Outcome, Confidence: 0.35,
		})
	}
	now := time.Now()
	caps := make(map[string]Capability, len(writes))
	for _, write := range writes {
		observed := now
		caps[write.Capability] = Capability{
			Status: write.Status, Confidence: write.Confidence, Source: "probe", ObservedAt: &observed,
			Retrieved: write.Retrieved, Unauthenticated: write.Unauthenticated, Authenticated: write.Authenticated,
			EvidenceVersion: write.EvidenceVersion, Freshness: "fresh",
		}
	}
	return privacyReport(caps, now), writes
}
