package relayintel

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
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
// Private is set only when the recipient retrieved the event and a completed
// observer query did not. An incomplete observer query is not privacy evidence.
type giftWrapEvidence struct {
	Accept       string
	Auth         string
	Recipient    string
	RecipientHit bool
	Observer     string
	ObserverHit  bool
	Outcome      string
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
		  AND lower(split_part(split_part(regexp_replace(s.endpoint_url, '^wss?://', ''), '/', 1), ':', 1)) = ANY($1::text[])
		ORDER BY o.observed_at ASC NULLS FIRST, s.trust_weighted DESC, s.url_key ASC
		LIMIT 20
	`, p.allowlist)
	if err != nil {
		return false, wrapDB(err)
	}
	defer rows.Close()
	for rows.Next() {
		var relayURL, key string
		if err := rows.Scan(&relayURL, &key); err != nil {
			return false, err
		}
		if !p.allowed(relayURL) {
			continue
		}
		token, ok := p.claim(ctx, pool, key, CapGiftWrapPrivate, probeHost(relayURL))
		if !ok {
			continue
		}
		evidence := p.exchangeGiftWrap(ctx, pool, relayURL)
		p.storeGiftWrap(ctx, pool, key, token, evidence)
		p.finish(ctx, pool, token)
		return true, nil
	}
	return false, rows.Err()
}

func (p *probeState) exchangeGiftWrap(ctx context.Context, pool *pgxpool.Pool, relayURL string) giftWrapEvidence {
	event, err := buildGiftWrap(p.key, p.pubkey, p.recipientPub)
	if err != nil {
		metrics.IncRelayIntelProbe("giftwrap", "error")
		return giftWrapEvidence{Outcome: "error"}
	}
	id, _ := event["id"].(string)
	if pub, _ := event["pubkey"].(string); pub != "" && pool != nil {
		_, _ = pool.Exec(ctx, `INSERT INTO relay_probe_identities (pubkey) VALUES ($1) ON CONFLICT (pubkey) DO NOTHING`, pub)
	}
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
	ack, authStatus := readWriteAckWithAuth(conn, id, time.Now().Add(5*time.Second), func(challenge string) (map[string]any, error) {
		return signAuth(p.key, p.pubkey, relayURL, challenge)
	})
	conn.Close()
	evidence := giftWrapEvidence{Accept: ack, Auth: authStatus}
	if ack != "accepted" {
		evidence.Outcome = "accepted_only"
		if ack != "accepted" {
			evidence.Outcome = ack
		}
		return evidence
	}
	recipientID, recipientIDHit := p.readGiftWrap(ctx, relayURL, id, p.recipientKey, p.recipientPub, false)
	recipientTag, recipientTagHit := p.readGiftWrap(ctx, relayURL, id, p.recipientKey, p.recipientPub, true)
	observerID, observerIDHit := p.readGiftWrap(ctx, relayURL, id, p.observerKey, p.observerPub, false)
	observerTag, observerTagHit := p.readGiftWrap(ctx, relayURL, id, p.observerKey, p.observerPub, true)
	evidence.RecipientHit = recipientIDHit || recipientTagHit
	evidence.ObserverHit = observerIDHit || observerTagHit
	if evidence.RecipientHit && (recipientID == "complete" || recipientTag == "complete") {
		evidence.Recipient = "complete"
	} else {
		evidence.Recipient = "incomplete"
	}
	if evidence.ObserverHit {
		evidence.Observer = "complete"
	} else if observerID == "complete" && observerTag == "complete" {
		evidence.Observer = "complete"
	} else {
		evidence.Observer = "incomplete"
	}
	evidence.Outcome = classifyGiftWrap(evidence)
	return evidence
}

func (p *probeState) readGiftWrap(ctx context.Context, relayURL, eventID string, key *btcec.PrivateKey, pubkey string, recipient bool) (string, bool) {
	conn, err := dialRelay(ctx, relayURL, p.allowPrivate, 5*time.Second)
	if err != nil {
		return "incomplete", false
	}
	defer conn.Close()
	filter := map[string]any{"ids": []string{eventID}, "limit": 1}
	if recipient {
		filter = map[string]any{"kinds": []int{1059}, "#p": []string{pubkey}, "limit": 1}
	}
	status, ids := readSubscription(conn, "nm-gift", filter, time.Now().Add(5*time.Second), func(challenge string) (map[string]any, error) {
		return signAuth(key, pubkey, relayURL, challenge)
	})
	return status, containsID(ids, eventID)
}

func (p *probeState) storeGiftWrap(ctx context.Context, pool *pgxpool.Pool, key, token string, evidence giftWrapEvidence) {
	if pool == nil {
		return
	}
	if evidence.Accept == "accepted" {
		_ = storeObservation(ctx, pool, key, CapGiftWrapAccept, StatusVerified, 0.7, "probe", "ok_true", token)
	} else if evidence.Accept == "rejected" {
		_ = storeObservation(ctx, pool, key, CapGiftWrapAccept, StatusUnsupported, 0.7, "probe", "ok_false", token)
	}
	switch evidence.Auth {
	case "ok":
		_ = storeObservation(ctx, pool, key, CapAuthGiftWrap, StatusVerified, 0.7, "probe", "auth_succeeded", token)
	case "unauthorized":
		_ = storeObservation(ctx, pool, key, CapAuthGiftWrap, StatusRestricted, 0.6, "probe", "auth_rejected", token)
	}
	switch evidence.Outcome {
	case "private":
		_ = storeObservation(ctx, pool, key, CapGiftWrapPrivate, StatusVerified, 0.8, "probe", "recipient_only", token)
		_ = storeObservation(ctx, pool, key, CapGiftWrap, StatusVerified, 0.8, "probe", "recipient_only", token)
		metrics.IncRelayIntelProbe("giftwrap", "ok")
	case "public":
		_ = storeObservation(ctx, pool, key, CapGiftWrapPublic, StatusVerified, 0.8, "probe", "observer_retrieved", token)
		_ = storeObservation(ctx, pool, key, CapGiftWrap, StatusRestricted, 0.7, "probe", "publicly_accessible", token)
		metrics.IncRelayIntelProbe("giftwrap", "public")
	default:
		metrics.IncRelayIntelProbe("giftwrap", "inconclusive")
	}
}

func classifyGiftWrap(evidence giftWrapEvidence) string {
	if evidence.Accept != "accepted" {
		return evidence.Accept
	}
	if evidence.ObserverHit {
		return "public"
	}
	if evidence.Recipient == "complete" && evidence.RecipientHit && evidence.Observer == "complete" && !evidence.ObserverHit {
		return "private"
	}
	if evidence.Recipient == "complete" && evidence.RecipientHit {
		return "retrieved_privacy_unknown"
	}
	return "accepted_only"
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
	RememberSynthetic(ephemeralPub)
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
