package relayintel

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nbd-wtf/go-nostr/nip44"
	"github.com/nostrmash/nostrmash/internal/metrics"
	"github.com/nostrmash/nostrmash/internal/nostr"
)

// giftWrapEvidence is what one controlled gift-wrap probe established.
// Private is set only when the recipient retrieved the event and every
// unauthorized path completed without returning it. One unauthorized hit is
// exposure even if a later query does not finish.
type giftWrapEvidence struct {
	Accept         string
	Auth           string
	Recipient      string
	RecipientHit   bool
	Observer       string
	ObserverHit    bool
	Outcome        string
	Checked        bool
	RecipientExact accessResult
	RecipientTag   accessResult
	ObserverExact  accessResult
	ObserverTag    accessResult
	AnonExact      accessResult
	AnonTag        accessResult
}

type accessResult struct {
	Status                   string
	Hit                      bool
	HitAfterAuth             bool
	Authenticated            bool
	AuthAttempted            bool
	SeenBeforeAuth           bool
	RetrievedUnauthenticated bool
	AuthIdentity             string
	SubscriptionComplete     bool
	Denied                   bool
	Unauthorized             bool
}

func (p *probeState) probeOneGiftWrap(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	rows, err := pool.Query(ctx, `
		SELECT s.endpoint_url, s.url_key
		FROM relay_role_stats s
		LEFT JOIN relay_capability_observations o
		  ON o.url_key = s.url_key AND o.capability = 'giftwrap_private'
		WHERE s.role = 'dm_inbox'
		  AND s.endpoint_type = 'nostr_relay'
		  AND NOT EXISTS (
		    SELECT 1 FROM relay_probe_leases l
		    WHERE l.url_key = s.url_key
		      AND l.capability = 'giftwrap_private'
		      AND l.expires_at > now()
		  )
		  AND relay_probe_host(s.endpoint_url) = ANY($1::text[])
		  AND NOT EXISTS (
		    SELECT 1 FROM relay_probe_attempts a
		    WHERE a.url_key = s.url_key
		      AND a.capability = 'giftwrap_private'
		      AND a.started_at > now() - make_interval(secs => $2::int)
		  )
		ORDER BY o.observed_at ASC NULLS FIRST, s.trust_weighted DESC, s.url_key ASC
		LIMIT 20
	`, p.allowlist, int(p.minInterval.Seconds()))
	if err != nil {
		return false, wrapDB(err)
	}
	type candidate struct{ url, key string }
	var picks []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.url, &item.key); err != nil {
			rows.Close()
			return false, err
		}
		picks = append(picks, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	for _, item := range picks {
		if !p.allowed(item.url) {
			continue
		}
		token, ok := p.claim(ctx, pool, item.key, CapGiftWrapPrivate, probeHost(item.url))
		if !ok {
			continue
		}
		evidence := p.exchangeGiftWrap(ctx, pool, item.url)
		if err := p.storeGiftWrap(ctx, pool, item.key, token, evidence); err != nil && !errors.Is(err, errProbeAlreadyCompleted) {
			releaseOwnedAttempt(ctx, pool, item.key, token, "commit_failed")
			return true, err
		}
		return true, nil
	}
	return false, nil
}

func (p *probeState) exchangeGiftWrap(ctx context.Context, pool *pgxpool.Pool, relayURL string) giftWrapEvidence {
	event, err := buildGiftWrap(p.key, p.pubkey, p.recipientPub)
	if err != nil {
		metrics.IncRelayIntelProbe("giftwrap", "error")
		return giftWrapEvidence{Outcome: "error"}
	}
	author, _ := event["pubkey"].(string)
	// Durable registration. Raw events and search documents can outlive a
	// 30-day expiry, and an expired row would let a later replay treat the
	// author as ordinary.
	if err := registerSyntheticIdentity(ctx, pool, author); err != nil {
		metrics.IncRelayIntelProbe("giftwrap", "error")
		return giftWrapEvidence{Outcome: "error"}
	}
	id, _ := event["id"].(string)
	conn, err := dialRelay(ctx, relayURL, p.allowPrivate, 5*time.Second)
	if err != nil {
		metrics.IncRelayIntelProbe("giftwrap", classifyDial(err))
		return giftWrapEvidence{Outcome: classifyDial(err)}
	}
	payload, _ := json.Marshal([]any{"EVENT", event})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		conn.Close()
		return giftWrapEvidence{Outcome: "error"}
	}
	ack, authStatus := readWriteAckWithAuth(conn, id, payload, time.Now().Add(5*time.Second), func(challenge string) (map[string]any, error) {
		return signAuth(p.key, p.pubkey, relayURL, challenge)
	})
	conn.Close()
	evidence := giftWrapEvidence{Accept: ack, Auth: authStatus}
	if ack != "accepted" {
		evidence.Outcome = ack
		return evidence
	}
	evidence.Checked = true
	evidence.RecipientExact = p.readGiftWrap(ctx, relayURL, id, p.recipientKey, p.recipientPub, p.recipientPub, false, true)
	evidence.RecipientTag = p.readGiftWrap(ctx, relayURL, id, p.recipientKey, p.recipientPub, p.recipientPub, true, true)
	evidence.ObserverExact = p.readGiftWrap(ctx, relayURL, id, p.observerKey, p.observerPub, p.recipientPub, false, true)
	evidence.ObserverTag = p.readGiftWrap(ctx, relayURL, id, p.observerKey, p.observerPub, p.recipientPub, true, true)
	evidence.AnonExact = p.readGiftWrap(ctx, relayURL, id, nil, "", p.recipientPub, false, false)
	evidence.AnonTag = p.readGiftWrap(ctx, relayURL, id, nil, "", p.recipientPub, true, false)
	evidence.RecipientHit = evidence.RecipientExact.Hit || evidence.RecipientTag.Hit
	evidence.ObserverHit = evidence.ObserverExact.Hit || evidence.ObserverTag.Hit || evidence.AnonExact.Hit || evidence.AnonTag.Hit
	evidence.Recipient = rollupAccess(evidence.RecipientExact, evidence.RecipientTag)
	evidence.Observer = rollupAccess(evidence.ObserverExact, evidence.ObserverTag, evidence.AnonExact, evidence.AnonTag)
	evidence.Outcome = classifyGiftWrap(evidence)
	return evidence
}

func rollupAccess(results ...accessResult) string {
	hit := false
	complete := true
	for _, result := range results {
		if result.Hit {
			hit = true
		}
		if result.Status != "complete" {
			complete = false
		}
	}
	if hit {
		return "complete"
	}
	if complete {
		return "complete"
	}
	return "incomplete"
}

func (p *probeState) readGiftWrap(ctx context.Context, relayURL, eventID string, key *btcec.PrivateKey, authPub, targetRecipient string, byRecipientTag, authenticate bool) accessResult {
	conn, err := dialRelay(ctx, relayURL, p.allowPrivate, 5*time.Second)
	if err != nil {
		return accessResult{Status: "incomplete"}
	}
	defer conn.Close()
	filter := map[string]any{"ids": []string{eventID}, "limit": 1}
	if byRecipientTag {
		filter = map[string]any{"kinds": []int{1059}, "#p": []string{targetRecipient}, "limit": 1}
	}
	var auth func(string) (map[string]any, error)
	if authenticate && key != nil {
		auth = func(challenge string) (map[string]any, error) {
			return signAuth(key, authPub, relayURL, challenge)
		}
	}
	status, ids, exposed, authenticated := readSubscription(ctx, conn, "nm-gift", filter, time.Now().Add(5*time.Second), auth)
	unauthenticated := containsID(exposed, eventID) || (!authenticated && containsID(ids, eventID))
	after := authenticated && containsID(ids, eventID)
	result := accessResult{
		Status: status, HitAfterAuth: after, SeenBeforeAuth: unauthenticated,
		RetrievedUnauthenticated: unauthenticated, Authenticated: authenticated, AuthAttempted: authenticate,
		SubscriptionComplete: status == "complete", Denied: status == "denied" || status == "unauthorized",
	}
	if authenticated {
		result.AuthIdentity = authPub
		result.Hit = after
	} else {
		result.Hit = unauthenticated
	}
	result.Unauthorized = unauthenticated || (!authenticated && result.Hit)
	return result
}

func (p *probeState) storeGiftWrap(ctx context.Context, pool *pgxpool.Pool, key, token string, evidence giftWrapEvidence) error {
	if pool == nil || token == "" {
		return nil
	}
	var writes []observationWrite
	if evidence.Accept == "accepted" {
		writes = append(writes, observationWrite{Capability: CapGiftWrapAccept, Status: StatusVerified, Detail: "ok_true", Confidence: 0.7})
	} else if evidence.Accept != "" {
		status, detail := publicationEvidence(evidence.Accept)
		writes = append(writes, observationWrite{Capability: CapGiftWrapAccept, Status: status, Detail: detail, Confidence: 0.55})
	}
	switch evidence.Auth {
	case "ok":
		writes = append(writes, observationWrite{Capability: CapAuthGiftWrap, Status: StatusVerified, Detail: "auth_succeeded", Confidence: 0.7})
	case "unauthorized":
		writes = append(writes, observationWrite{Capability: CapAuthGiftWrap, Status: StatusRestricted, Detail: "auth_rejected", Confidence: 0.6})
	}
	attemptStatus := evidence.Outcome
	if attemptStatus == "" {
		attemptStatus = "incomplete"
	}
	writes = append(writes, giftWrapPathWrites(evidence)...)
	switch evidence.Outcome {
	case "private":
		writes = append(writes,
			observationWrite{Capability: CapGiftWrapPrivate, Status: StatusVerified, Detail: "recipient_only " + accessPathSummary(evidence), Confidence: 0.8, EvidenceVersion: ComprehensivePrivacyVersion},
			observationWrite{Capability: CapGiftWrap, Status: StatusVerified, Detail: "recipient_only " + accessPathSummary(evidence), Confidence: 0.8, EvidenceVersion: ComprehensivePrivacyVersion},
		)
		metrics.IncRelayIntelProbe("giftwrap", "ok")
	case "public":
		detail := "observer_retrieved"
		if unauthorizedRetrieval(evidence) {
			detail = "retrieved_while_unauthenticated"
		}
		writes = append(writes,
			observationWrite{Capability: CapGiftWrapPublic, Status: StatusVerified, Detail: detail + " " + accessPathSummary(evidence), Confidence: 0.8, Retrieved: boolPtr(true)},
			observationWrite{Capability: CapGiftWrap, Status: StatusRestricted, Detail: "publicly_accessible " + accessPathSummary(evidence), Confidence: 0.7},
		)
		metrics.IncRelayIntelProbe("giftwrap", "public")
	default:
		if evidence.Accept == "accepted" {
			writes = append(writes, observationWrite{Capability: CapGiftWrapPrivate, Status: "inconclusive", Detail: evidence.Outcome, Confidence: 0.35})
		}
		metrics.IncRelayIntelProbe("giftwrap", "inconclusive")
	}
	return commitProbeResult(ctx, pool, key, token, stampEvidence(probeCompletion{AttemptStatus: attemptStatus, Writes: writes}))
}

func giftWrapPathWrites(evidence giftWrapEvidence) []observationWrite {
	if !evidence.Checked && evidence.Accept != "accepted" {
		return nil
	}
	var writes []observationWrite
	paths := []struct {
		capability string
		result     accessResult
	}{
		{CapGiftWrapRecipientExact, evidence.RecipientExact},
		{CapGiftWrapRecipientTag, evidence.RecipientTag},
		{CapGiftWrapObserverExact, evidence.ObserverExact},
		{CapGiftWrapObserverTag, evidence.ObserverTag},
		{CapGiftWrapAnonExact, evidence.AnonExact},
		{CapGiftWrapAnonTag, evidence.AnonTag},
	}
	for _, path := range paths {
		if write, ok := pathWrite(path.capability, path.result); ok {
			writes = append(writes, write)
		}
	}
	if evidence.RecipientExact.Authenticated || evidence.RecipientTag.Authenticated {
		writes = append(writes, observationWrite{Capability: CapAuthRecipient, Status: StatusVerified, Detail: "recipient_auth_succeeded", Confidence: 0.7, Authenticated: boolPtr(true)})
	} else if evidence.Checked && (evidence.RecipientExact.AuthAttempted || evidence.RecipientTag.AuthAttempted) && (evidence.RecipientExact.Denied || evidence.RecipientTag.Denied || evidence.RecipientExact.Status == "unauthorized" || evidence.RecipientTag.Status == "unauthorized") {
		writes = append(writes, observationWrite{Capability: CapAuthRecipient, Status: StatusRestricted, Detail: "recipient_auth_rejected", Confidence: 0.6, Authenticated: boolPtr(false)})
	}
	exactHit := evidence.RecipientExact.Authenticated && evidence.RecipientExact.HitAfterAuth && evidence.RecipientExact.SubscriptionComplete
	tagHit := evidence.RecipientTag.Authenticated && evidence.RecipientTag.HitAfterAuth && evidence.RecipientTag.SubscriptionComplete
	if exactHit || tagHit {
		writes = append(writes, observationWrite{Capability: CapGiftWrapRecipient, Status: StatusVerified, Detail: "authenticated_recipient_retrieval", Confidence: 0.8, Retrieved: boolPtr(true)})
	} else if evidence.Checked && evidence.RecipientExact.Authenticated && evidence.RecipientTag.Authenticated && evidence.RecipientExact.SubscriptionComplete && evidence.RecipientTag.SubscriptionComplete {
		writes = append(writes, observationWrite{Capability: CapGiftWrapRecipient, Status: StatusVerified, Detail: "authenticated_recipient_miss", Confidence: 0.6, Retrieved: boolPtr(false)})
	}
	if status, retrieved, ok := aggregateUnauthenticatedExposure(evidence); ok {
		writes = append(writes, observationWrite{
			Capability: CapGiftWrapUnauthExposure, Status: status, Detail: "unauthenticated_exposure",
			Confidence: exposureConfidence(status), Retrieved: retrieved, Unauthenticated: retrieved,
		})
	}
	if status, retrieved, ok := aggregateAuthenticatedExposure(evidence); ok {
		writes = append(writes, observationWrite{
			Capability: CapGiftWrapAuthExposure, Status: status, Detail: "authenticated_observer_exposure",
			Confidence: exposureConfidence(status), Retrieved: retrieved, Authenticated: retrieved,
		})
	}
	return writes
}

func exposureConfidence(status string) float64 {
	if status == StatusVerified {
		return 0.8
	}
	return 0.35
}

func aggregateUnauthenticatedExposure(evidence giftWrapEvidence) (string, *bool, bool) {
	paths := []accessResult{
		evidence.RecipientExact, evidence.RecipientTag, evidence.ObserverExact, evidence.ObserverTag, evidence.AnonExact, evidence.AnonTag,
	}
	return aggregateExposure(paths, unauthenticatedHit, negativeUnauthenticated)
}

func aggregateAuthenticatedExposure(evidence giftWrapEvidence) (string, *bool, bool) {
	if authenticatedObserverHit(evidence.AnonExact) || authenticatedObserverHit(evidence.AnonTag) {
		return StatusVerified, boolPtr(true), true
	}
	return aggregateExposure(
		[]accessResult{evidence.ObserverExact, evidence.ObserverTag},
		authenticatedObserverHit, negativeAuthenticatedObserver,
	)
}

func aggregateExposure(paths []accessResult, positive, negative func(accessResult) bool) (string, *bool, bool) {
	seen := false
	for _, path := range paths {
		if path.Status != "" || positive(path) {
			seen = true
		}
		if positive(path) {
			return StatusVerified, boolPtr(true), true
		}
	}
	if !seen {
		return "", nil, false
	}
	for _, path := range paths {
		if !negative(path) {
			return "inconclusive", nil, true
		}
	}
	return StatusVerified, boolPtr(false), true
}

func unauthenticatedHit(result accessResult) bool {
	return result.RetrievedUnauthenticated || result.SeenBeforeAuth || (result.Hit && !result.Authenticated)
}

func authenticatedObserverHit(result accessResult) bool {
	return result.Authenticated && result.HitAfterAuth
}

func negativeUnauthenticated(result accessResult) bool {
	if unauthenticatedHit(result) {
		return false
	}
	switch result.Status {
	case "complete", "denied", "unauthorized", "auth_required":
		return true
	default:
		return false
	}
}

func negativeAuthenticatedObserver(result accessResult) bool {
	if authenticatedObserverHit(result) || result.Hit || result.SeenBeforeAuth || result.RetrievedUnauthenticated {
		return false
	}
	// A rejected or unfinished AUTH event does not show what an authenticated
	// observer would be allowed to read. The subscription itself has to
	// complete, or be refused, after authentication succeeded.
	if !result.Authenticated {
		return false
	}
	return result.Status == "complete" || result.Status == "denied"
}

func pathWrite(capability string, result accessResult) (observationWrite, bool) {
	if result.Status == "" && !result.Hit && !result.HitAfterAuth && !result.SeenBeforeAuth && !result.RetrievedUnauthenticated {
		return observationWrite{}, false
	}
	recipient := capability == CapGiftWrapRecipientExact || capability == CapGiftWrapRecipientTag
	unauth := unauthenticatedHit(result)
	authenticatedRetrieval := result.Authenticated && result.HitAfterAuth && result.Status == "complete"
	write := observationWrite{
		Capability: capability, Detail: result.Status,
		Unauthenticated: boolPtr(unauth),
	}
	if result.AuthAttempted || result.Authenticated {
		write.Authenticated = boolPtr(result.Authenticated)
	}
	if recipient {
		if authenticatedRetrieval {
			write.Status = StatusVerified
			write.Confidence = 0.8
			write.Retrieved = boolPtr(true)
			return write, true
		}
		if !unauth && result.Authenticated && result.Status == "complete" {
			write.Status = StatusVerified
			write.Confidence = 0.6
			write.Retrieved = boolPtr(false)
			return write, true
		}
		write.Status = "inconclusive"
		write.Confidence = 0.35
		return write, true
	}
	if unauth || result.Hit || result.HitAfterAuth {
		write.Status = StatusVerified
		write.Confidence = 0.8
		write.Retrieved = boolPtr(true)
		return write, true
	}
	observer := capability == CapGiftWrapObserverExact || capability == CapGiftWrapObserverTag
	negative := negativeUnauthenticated(result)
	if observer {
		negative = negativeAuthenticatedObserver(result)
	}
	if negative {
		write.Status = StatusVerified
		write.Confidence = 0.6
		write.Retrieved = boolPtr(false)
		return write, true
	}
	write.Status = "inconclusive"
	write.Confidence = 0.35
	return write, true
}

func classifyGiftWrap(evidence giftWrapEvidence) string {
	if evidence.Accept != "" && evidence.Accept != "accepted" {
		return evidence.Accept
	}
	if unauthorizedRetrieval(evidence) {
		return "public"
	}
	if evidence.Checked {
		recipientOK := pathEstablished(evidence.RecipientExact, true) && pathEstablished(evidence.RecipientTag, true)
		observerOK := negativeEvidence(evidence.ObserverExact, true) && negativeEvidence(evidence.ObserverTag, true)
		anonOK := negativeEvidence(evidence.AnonExact, false) && negativeEvidence(evidence.AnonTag, false)
		if recipientOK && observerOK && anonOK {
			return "private"
		}
		if authenticatedRecipientHit(evidence.RecipientExact) || authenticatedRecipientHit(evidence.RecipientTag) {
			return "retrieved_privacy_unknown"
		}
		if evidence.Recipient == "complete" && evidence.RecipientHit {
			return "retrieved_privacy_unknown"
		}
		return "accepted_only"
	}
	if evidence.Recipient == "complete" && evidence.RecipientHit {
		return "retrieved_privacy_unknown"
	}
	return "accepted_only"
}

func unauthorizedRetrieval(evidence giftWrapEvidence) bool {
	if evidence.ObserverHit {
		return true
	}
	paths := []struct {
		result    accessResult
		recipient bool
	}{
		{evidence.RecipientExact, true},
		{evidence.RecipientTag, true},
		{evidence.ObserverExact, false},
		{evidence.ObserverTag, false},
		{evidence.AnonExact, false},
		{evidence.AnonTag, false},
	}
	for _, path := range paths {
		if path.result.RetrievedUnauthenticated || path.result.SeenBeforeAuth || path.result.Unauthorized {
			return true
		}
		if path.recipient && path.result.Hit && !path.result.Authenticated {
			return true
		}
		if !path.recipient && (path.result.Hit || path.result.HitAfterAuth) {
			return true
		}
	}
	return false
}

func authenticatedRecipientHit(result accessResult) bool {
	return result.Status == "complete" && result.Authenticated && result.HitAfterAuth && !result.SeenBeforeAuth && !result.RetrievedUnauthenticated
}

func accessPathSummary(evidence giftWrapEvidence) string {
	return fmt.Sprintf("recipient_exact=%s hit=%t before_auth=%t recipient_tag=%s hit=%t before_auth=%t observer_exact=%s hit=%t before_auth=%t observer_tag=%s hit=%t before_auth=%t anon_exact=%s hit=%t before_auth=%t anon_tag=%s hit=%t before_auth=%t",
		evidence.RecipientExact.Status, evidence.RecipientExact.Hit, evidence.RecipientExact.SeenBeforeAuth,
		evidence.RecipientTag.Status, evidence.RecipientTag.Hit, evidence.RecipientTag.SeenBeforeAuth,
		evidence.ObserverExact.Status, evidence.ObserverExact.Hit, evidence.ObserverExact.SeenBeforeAuth,
		evidence.ObserverTag.Status, evidence.ObserverTag.Hit, evidence.ObserverTag.SeenBeforeAuth,
		evidence.AnonExact.Status, evidence.AnonExact.Hit, evidence.AnonExact.SeenBeforeAuth,
		evidence.AnonTag.Status, evidence.AnonTag.Hit, evidence.AnonTag.SeenBeforeAuth)
}

func negativeEvidence(result accessResult, authenticatedPath bool) bool {
	if result.Hit || result.HitAfterAuth || result.SeenBeforeAuth || result.RetrievedUnauthenticated || result.Unauthorized {
		return false
	}
	if authenticatedPath {
		return result.Authenticated && (result.Status == "complete" || result.Status == "denied")
	}
	return !result.Authenticated && (result.Status == "complete" || result.Status == "auth_required")
}

func pathEstablished(result accessResult, wantHit bool) bool {
	if result.Status != "complete" || !result.Authenticated || result.HitAfterAuth != wantHit {
		return false
	}
	return true
}

func buildGiftWrap(sender *btcec.PrivateKey, senderPub, recipientPub string) (map[string]any, error) {
	rumorAt := time.Now().Add(-time.Duration(randomSeconds(120)) * time.Second).Unix()
	rumor, err := unsignedEvent(senderPub, 14, rumorAt, [][]string{{"p", recipientPub}}, "nostrmash relay intelligence gift-wrap probe")
	if err != nil {
		return nil, err
	}
	rumorJSON, err := json.Marshal(rumor)
	if err != nil {
		return nil, err
	}
	senderSecret := hex.EncodeToString(sender.Serialize())
	conversation, err := nip44.GenerateConversationKey(recipientPub, senderSecret)
	if err != nil {
		return nil, err
	}
	sealedText, err := nip44.Encrypt(string(rumorJSON), conversation)
	if err != nil {
		return nil, err
	}
	sealAt := time.Now().Add(-time.Duration(randomSeconds(300)) * time.Second).Unix()
	seal, err := signNostrEvent(sender, senderPub, 13, sealAt, [][]string{}, sealedText)
	if err != nil {
		return nil, err
	}
	sealJSON, err := json.Marshal(seal)
	if err != nil {
		return nil, err
	}
	ephemeral, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, err
	}
	ephemeralPub := hex.EncodeToString(schnorr.SerializePubKey(ephemeral.PubKey()))
	wrapKey, err := nip44.GenerateConversationKey(recipientPub, hex.EncodeToString(ephemeral.Serialize()))
	if err != nil {
		return nil, err
	}
	wrapped, err := nip44.Encrypt(string(sealJSON), wrapKey)
	if err != nil {
		return nil, err
	}
	wrapAt := time.Now().Add(-time.Duration(randomSeconds(60)) * time.Second).Unix()
	return signNostrEvent(ephemeral, ephemeralPub, 1059, wrapAt, [][]string{{"p", recipientPub}}, wrapped)
}

func openGiftWrap(recipient *btcec.PrivateKey, recipientPub string, event map[string]any) (string, error) {
	raw, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	parsed := nostr.ParseAndValidate(raw, nostr.Options{MaxRawJSONBytes: 64 * 1024, MaxContentBytes: 64 * 1024})
	if !parsed.Valid() {
		return "", fmt.Errorf("gift wrap signature is invalid")
	}
	secret := hex.EncodeToString(recipient.Serialize())
	conversation, err := nip44.GenerateConversationKey(parsed.Event.Pubkey, secret)
	if err != nil {
		return "", err
	}
	sealJSON, err := nip44.Decrypt(parsed.Event.Content, conversation)
	if err != nil {
		return "", err
	}
	sealParsed := nostr.ParseAndValidate([]byte(sealJSON), nostr.Options{MaxRawJSONBytes: 64 * 1024, MaxContentBytes: 64 * 1024})
	if !sealParsed.Valid() {
		return "", fmt.Errorf("seal signature is invalid")
	}
	rumorKey, err := nip44.GenerateConversationKey(sealParsed.Event.Pubkey, secret)
	if err != nil {
		return "", err
	}
	rumorJSON, err := nip44.Decrypt(sealParsed.Event.Content, rumorKey)
	if err != nil {
		return "", err
	}
	var rumor map[string]any
	if err := json.Unmarshal([]byte(rumorJSON), &rumor); err != nil {
		return "", err
	}
	content, _ := rumor["content"].(string)
	return content, nil
}

func unsignedEvent(pubkey string, kind int, created int64, tags [][]string, content string) (map[string]any, error) {
	if tags == nil {
		tags = [][]string{}
	}
	canonical, err := json.Marshal([]any{0, pubkey, created, kind, tags, content})
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	return map[string]any{
		"id": hex.EncodeToString(sum[:]), "pubkey": pubkey, "created_at": created,
		"kind": kind, "tags": tags, "content": content,
	}, nil
}

func signNostrEvent(priv *btcec.PrivateKey, pubkey string, kind int, created int64, tags [][]string, content string) (map[string]any, error) {
	event, err := unsignedEvent(pubkey, kind, created, tags, content)
	if err != nil {
		return nil, err
	}
	id, _ := event["id"].(string)
	raw, err := hex.DecodeString(id)
	if err != nil {
		return nil, err
	}
	sig, err := schnorr.Sign(priv, raw)
	if err != nil {
		return nil, err
	}
	event["sig"] = hex.EncodeToString(sig.Serialize())
	return event, nil
}

func signAuth(priv *btcec.PrivateKey, pubkey, relayURL, challenge string) (map[string]any, error) {
	return signNostrEvent(priv, pubkey, 22242, time.Now().Unix(), [][]string{
		{"relay", relayURL},
		{"challenge", challenge},
	}, "")
}

func randomSeconds(max int) int {
	if max < 1 {
		return 1
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 1
	}
	return int(binary.BigEndian.Uint64(buf[:])%uint64(max)) + 1
}

func (p *probeState) allowed(raw string) bool {
	if p == nil || len(p.allowlist) == 0 {
		return false
	}
	host := probeHost(raw)
	for _, item := range p.allowlist {
		if item == host {
			return true
		}
	}
	return false
}
