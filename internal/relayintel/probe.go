package relayintel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/nostrmash/nostrmash/internal/nostr"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nostrmash/nostrmash/internal/metrics"
)

// probeState is the worker-only active probe. It is off unless a synthetic key is configured.
// The hourly budget and the per-relay interval live in Postgres so a restart
// or a second worker does not start the allowance over.
type probeState struct {
	key          *btcec.PrivateKey
	pubkey       string
	worker       string
	allowPrivate bool
	globalLimit  int
	minInterval  time.Duration
	giftwrap     bool
	turn         int
	allowlist    []string
	recipientKey *btcec.PrivateKey
	recipientPub string
	observerKey  *btcec.PrivateKey
	observerPub  string
}

func newProbeState(secretHex string, allowPrivate bool, globalPerHour int, minInterval time.Duration, giftwrap bool, allowlist []string) (*probeState, string, error) {
	raw, err := hex.DecodeString(secretHex)
	if err != nil || len(raw) != 32 {
		return nil, "", fmt.Errorf("RELAY_INTEL_PROBE_SECRET_KEY must be 32-byte hex")
	}
	priv, _ := btcec.PrivKeyFromBytes(raw)
	if globalPerHour <= 0 {
		globalPerHour = 20
	}
	if minInterval <= 0 {
		minInterval = 24 * time.Hour
	}
	pub := hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))
	RememberSynthetic(pub)
	recipient, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, "", err
	}
	observer, err := btcec.NewPrivateKey()
	if err != nil {
		return nil, "", err
	}
	recipientPub := hex.EncodeToString(schnorr.SerializePubKey(recipient.PubKey()))
	observerPub := hex.EncodeToString(schnorr.SerializePubKey(observer.PubKey()))
	RememberSynthetic(recipientPub)
	RememberSynthetic(observerPub)
	host, _ := os.Hostname()
	if host == "" {
		host = "worker"
	}
	return &probeState{
		key: priv, pubkey: pub, worker: fmt.Sprintf("%s:%d", host, os.Getpid()),
		allowPrivate: allowPrivate, globalLimit: globalPerHour, minInterval: minInterval, giftwrap: giftwrap,
		allowlist: allowlist, recipientKey: recipient, recipientPub: recipientPub,
		observerKey: observer, observerPub: observerPub,
	}, pub, nil
}

// Once probes at most one approved relay.
func (p *probeState) Once(ctx context.Context, pool *pgxpool.Pool) error {
	if p == nil || pool == nil {
		return nil
	}
	p.turn++
	if followed, err := p.followOne(ctx, pool); err != nil || followed {
		return err
	}
	if p.giftwrap && p.turn%2 == 0 {
		probed, err := p.probeOneGiftWrap(ctx, pool)
		if err != nil || probed {
			return err
		}
	}
	rows, err := pool.Query(ctx, `
		SELECT s.endpoint_url, s.url_key, s.role
		FROM relay_role_stats s
		LEFT JOIN relay_capability_observations o
		  ON o.url_key = s.url_key
		 AND o.capability = CASE WHEN s.role = 'search' THEN 'search' ELSE 'write_accept' END
		WHERE s.role IN ('public_outbox', 'search')
		  AND s.endpoint_type = 'nostr_relay'
		  AND NOT EXISTS (
		    SELECT 1 FROM relay_probe_leases l
		    WHERE l.url_key = s.url_key
		      AND l.capability = CASE WHEN s.role = 'search' THEN 'search' ELSE 'write_accept' END
		      AND l.expires_at > now()
		  )
		  AND relay_probe_host(s.endpoint_url) = ANY($1::text[])
		  AND NOT EXISTS (
		    SELECT 1 FROM relay_probe_attempts a
		    WHERE a.url_key = s.url_key
		      AND a.capability = CASE WHEN s.role = 'search' THEN 'search' ELSE 'write_accept' END
		      AND a.started_at > now() - make_interval(secs => $2::int)
		  )
		ORDER BY o.observed_at ASC NULLS FIRST, s.trust_weighted DESC, s.url_key ASC
		LIMIT 50
	`, p.allowlist, int(p.minInterval.Seconds()))
	if err != nil {
		return wrapDB(err)
	}
	defer rows.Close()
	type candidate struct{ url, key, role string }
	var picks []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.url, &item.key, &item.role); err != nil {
			return err
		}
		picks = append(picks, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range picks {
		capability := CapWriteAccept
		if item.role == RoleSearch {
			capability = CapSearch
		}
		if !p.allowed(item.url) {
			continue
		}
		token, ok := p.claim(ctx, pool, item.key, capability, probeHost(item.url))
		if !ok {
			continue
		}
		if item.role == RoleSearch {
			p.search(ctx, pool, item.url, item.key, token)
		} else {
			p.write(ctx, pool, item.url, item.key, token)
		}
		p.finish(ctx, pool, token)
		return nil
	}
	if p.giftwrap && p.turn%2 == 1 {
		_, err = p.probeOneGiftWrap(ctx, pool)
		return err
	}
	return nil
}

func (p *probeState) claim(ctx context.Context, pool *pgxpool.Pool, key, capability, host string) (string, bool) {
	seconds := int(p.minInterval.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	var token string
	err := pool.QueryRow(ctx, `
		SELECT claim_relay_probe($1, $2, $3, $4, $5, $6, 120)
	`, key, capability, host, p.worker, p.globalLimit, seconds).Scan(&token)
	if err != nil || token == "" {
		metrics.IncRelayIntelProbe(capability, "skipped")
		return "", false
	}
	return token, true
}

func (p *probeState) scheduleRetention(ctx context.Context, pool *pgxpool.Pool, key, relayURL, eventID string) {
	if pool == nil || eventID == "" || !p.allowed(relayURL) {
		return
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO relay_probe_followups (url_key, endpoint_url, event_id, capability, check_after)
		VALUES ($1, $2, $3, 'retention', now() + interval '1 hour')
	`, key, relayURL, eventID)
}

func (p *probeState) followOne(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	if p == nil || pool == nil {
		return false, nil
	}
	var recent int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM relay_probe_attempts WHERE started_at > now() - interval '1 hour'
	`).Scan(&recent); err != nil {
		if SchemaMissing(err) {
			return false, nil
		}
		return false, err
	}
	if recent >= p.globalLimit {
		return false, nil
	}
	var id int64
	var relayURL, eventID, key string
	err := pool.QueryRow(ctx, `
		UPDATE relay_probe_followups
		SET finished_at = now(), result = 'checking'
		WHERE id = (
			SELECT id FROM relay_probe_followups
			WHERE finished_at IS NULL AND check_after <= now()
			ORDER BY check_after, id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, endpoint_url, event_id, url_key
	`).Scan(&id, &relayURL, &eventID, &key)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || SchemaMissing(err) {
			return false, nil
		}
		return false, err
	}
	if !p.allowed(relayURL) {
		_, _ = pool.Exec(ctx, `UPDATE relay_probe_followups SET result = 'skipped' WHERE id = $1`, id)
		return true, nil
	}
	found, fetchErr := fetchEvents(ctx, relayURL, p.allowPrivate, map[string]any{"ids": []string{eventID}, "limit": 1})
	result := "missing_after_one_interval"
	if fetchErr == nil {
		for _, item := range found {
			if item != nil && item.ID == eventID {
				result = "retrieved_after_one_interval"
				_ = storeObservation(ctx, pool, key, CapRetention, StatusVerified, 0.6, "probe", result, "")
			}
		}
	} else {
		result = "incomplete"
	}
	if result == "missing_after_one_interval" {
		_ = storeObservation(ctx, pool, key, CapRetention, "inconclusive", 0.35, "probe", result, "")
	}
	_, _ = pool.Exec(ctx, `UPDATE relay_probe_followups SET result = $2 WHERE id = $1`, id, result)
	return true, nil
}

func (p *probeState) finish(ctx context.Context, pool *pgxpool.Pool, token string) {
	if token == "" {
		return
	}
	_, _ = pool.Exec(ctx, `
		UPDATE relay_probe_attempts
		SET finished_at = now(), status = 'finished'
		WHERE lease_token = $1 AND finished_at IS NULL
	`, token)
	_, _ = pool.Exec(ctx, `
		DELETE FROM relay_probe_leases WHERE lease_token = $1
	`, token)
}

func leaseAllows(leased, requested string) bool {
	if leased == requested {
		return true
	}
	switch leased {
	case CapWriteAccept:
		return requested == CapWriteRetrieve || requested == CapAuthWrite || requested == CapRetention
	case CapSearch:
		return requested == CapAuthSearch
	case CapGiftWrapPrivate:
		return requested == CapGiftWrap || requested == CapGiftWrapAccept || requested == CapGiftWrapPublic || requested == CapAuthGiftWrap || requested == CapRetention
	default:
		return false
	}
}

func probeHost(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" {
		return raw
	}
	return parsed.Hostname()
}

func (p *probeState) write(ctx context.Context, pool *pgxpool.Pool, relayURL, key, token string) {
	event, err := signProbeEvent(p.key, p.pubkey)
	if err != nil {
		metrics.IncRelayIntelProbe("write", "error")
		return
	}
	conn, err := dialRelay(ctx, relayURL, p.allowPrivate, 5*time.Second)
	if err != nil {
		metrics.IncRelayIntelProbe("write", classifyDial(err))
		return
	}
	defer conn.Close()
	payload, _ := json.Marshal([]any{"EVENT", event})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		metrics.IncRelayIntelProbe("write", "error")
		return
	}
	id, _ := event["id"].(string)
	ack, authStatus := readWriteAckWithAuth(conn, id, payload, time.Now().Add(5*time.Second), func(challenge string) (map[string]any, error) {
		return signAuth(p.key, p.pubkey, relayURL, challenge)
	})
	if pool != nil && authStatus == "ok" {
		_ = storeObservation(ctx, pool, key, CapAuthWrite, StatusVerified, 0.7, "probe", "auth_succeeded", token)
	} else if pool != nil && (authStatus == "unauthorized" || ack == "auth_required") {
		_ = storeObservation(ctx, pool, key, CapAuthWrite, StatusRestricted, 0.6, "probe", authStatus, token)
	}
	if ack != "accepted" {
		if pool != nil {
			status, detail := publicationEvidence(ack)
			_ = storeObservation(ctx, pool, key, CapWriteAccept, status, 0.55, "probe", detail, token)
		}
		metrics.IncRelayIntelProbe("write", ack)
		return
	}
	if pool != nil {
		_ = storeObservation(ctx, pool, key, CapWriteAccept, StatusVerified, 0.8, "probe", "ok_true", token)
		p.scheduleRetention(ctx, pool, key, relayURL, id)
	}
	found, err := fetchEvents(ctx, relayURL, p.allowPrivate, map[string]any{"ids": []string{id}, "limit": 1})
	if err == nil {
		for _, item := range found {
			if item != nil && item.ID == id {
				if pool != nil {
					_ = storeObservation(ctx, pool, key, CapWriteRetrieve, StatusVerified, 0.85, "probe", "retrieved", token)
				}
				metrics.IncRelayIntelProbe("write", "ok")
				return
			}
		}
	}
	metrics.IncRelayIntelProbe("write", "incomplete")
}

func (p *probeState) search(ctx context.Context, pool *pgxpool.Pool, relayURL, key, token string) {
	event, err := signProbeEvent(p.key, p.pubkey)
	if err != nil {
		metrics.IncRelayIntelProbe("search", "error")
		return
	}
	conn, err := dialRelay(ctx, relayURL, p.allowPrivate, 5*time.Second)
	if err != nil {
		metrics.IncRelayIntelProbe("search", classifyDial(err))
		return
	}
	defer conn.Close()
	id, _ := event["id"].(string)
	content, _ := event["content"].(string)
	payload, _ := json.Marshal([]any{"EVENT", event})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		metrics.IncRelayIntelProbe("search", "error")
		return
	}
	ack, authStatus := readWriteAckWithAuth(conn, id, payload, time.Now().Add(5*time.Second), func(challenge string) (map[string]any, error) {
		return signAuth(p.key, p.pubkey, relayURL, challenge)
	})
	if pool != nil && authStatus == "ok" {
		_ = storeObservation(ctx, pool, key, CapAuthSearch, StatusVerified, 0.7, "probe", "auth_succeeded", token)
	} else if pool != nil && (authStatus == "unauthorized" || ack == "auth_required") {
		_ = storeObservation(ctx, pool, key, CapAuthSearch, StatusRestricted, 0.6, "probe", authStatus, token)
	}
	if ack != "accepted" {
		if pool != nil {
			status, detail := publicationEvidence(ack)
			_ = storeObservation(ctx, pool, key, CapSearch, status, 0.45, "probe", detail, token)
		}
		metrics.IncRelayIntelProbe("search", "inconclusive")
		return
	}
	positive, positiveStatus := readSearchIDs(conn, "nm-search-yes", content, p.pubkey)
	negative, negativeStatus := readSearchIDs(conn, "nm-search-no", "nostrmash-relay-intel-negative-"+id, p.pubkey)
	if positiveStatus == "unsupported" || negativeStatus == "unsupported" {
		if pool != nil {
			_ = storeObservation(ctx, pool, key, CapSearch, StatusUnsupported, 0.7, "probe", "closed", token)
		}
		metrics.IncRelayIntelProbe("search", "unsupported")
		return
	}
	outcome := classifyCompletedSearch(positiveStatus, negativeStatus, containsID(positive, id), containsID(negative, id))
	switch outcome {
	case "verified":
		if pool != nil {
			_ = storeObservation(ctx, pool, key, CapSearch, StatusVerified, 0.8, "probe", "search_match", token)
		}
		metrics.IncRelayIntelProbe("search", "ok")
	case "ignored":
		if pool != nil {
			_ = storeObservation(ctx, pool, key, CapSearch, StatusRestricted, 0.6, "probe", "search_ignored", token)
		}
		metrics.IncRelayIntelProbe("search", "inconclusive")
	default:
		if pool != nil {
			_ = storeObservation(ctx, pool, key, CapSearch, "inconclusive", 0.35, "probe", outcome, token)
		}
		metrics.IncRelayIntelProbe("search", "inconclusive")
	}
}

func classifyWriteFrame(frame []any, eventID string) string {
	if len(frame) < 1 {
		return "other"
	}
	typ, _ := frame[0].(string)
	switch typ {
	case "OK":
		if len(frame) < 3 {
			return "other"
		}
		id, _ := frame[1].(string)
		ok, isBool := frame[2].(bool)
		if !isBool || id == "" {
			return "other"
		}
		if id != eventID {
			return "mismatched"
		}
		if !ok {
			return "rejected"
		}
		return "accepted"
	case "NOTICE", "CLOSED":
		return "closed"
	default:
		return "other"
	}
}

func rejectionReason(frame []any) string {
	msg := ""
	if len(frame) >= 4 {
		msg, _ = frame[3].(string)
	}
	return classifyRelayMessage(msg)
}

func classifyRelayMessage(msg string) string {
	msg = strings.ToLower(msg)
	switch {
	case strings.Contains(msg, "auth-required") || strings.Contains(msg, "auth required"):
		return "auth_required"
	case strings.Contains(msg, "rate"):
		return "rate_limited"
	case strings.Contains(msg, "payment") || strings.Contains(msg, "invoice"):
		return "payment_required"
	case strings.Contains(msg, "unsupported") || strings.Contains(msg, "unknown kind"):
		return "unsupported"
	case strings.Contains(msg, "invalid") || strings.Contains(msg, "malformed"):
		return "invalid"
	case msg == "":
		return "rejected"
	default:
		return "rejected"
	}
}

func publicationEvidence(ack string) (string, string) {
	switch ack {
	case "unsupported":
		return StatusUnsupported, ack
	case "auth_required", "unauthorized", "payment_required", "rejected":
		return StatusRestricted, ack
	case "rate_limited", "timeout", "incomplete", "closed":
		return "temporary", ack
	default:
		return "inconclusive", ack
	}
}

func readWriteAck(conn *websocket.Conn, eventID string, deadline time.Time) string {
	ack, _ := readWriteAckWithAuth(conn, eventID, nil, deadline, nil)
	return ack
}

func readWriteAckWithAuth(conn *websocket.Conn, eventID string, resend []byte, deadline time.Time, auth func(challenge string) (map[string]any, error)) (string, string) {
	authState := ""
	authEventID := ""
	challenges := 0
	retries := 0
	messages := 0
	total := 0
	for {
		if messages >= 32 || total >= 256*1024 || time.Now().After(deadline) {
			return "incomplete", authState
		}
		_ = conn.SetReadDeadline(deadline)
		_, message, err := conn.ReadMessage()
		if err != nil {
			return "timeout", authState
		}
		if len(message) > 64*1024 {
			return "incomplete", authState
		}
		messages++
		total += len(message)
		var frame []any
		if json.Unmarshal(message, &frame) != nil || len(frame) == 0 {
			continue
		}
		typ, _ := frame[0].(string)
		if typ == "AUTH" {
			if challenges >= 2 || auth == nil || len(frame) < 2 {
				return "auth_required", "required"
			}
			challenge, _ := frame[1].(string)
			if challenge == "" {
				return "auth_required", "required"
			}
			challenges++
			signed, err := auth(challenge)
			if err != nil {
				return "auth_required", "failed"
			}
			authEventID, _ = signed["id"].(string)
			payload, _ := json.Marshal([]any{"AUTH", signed})
			_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return "auth_required", "failed"
			}
			authState = "sent"
			continue
		}
		if typ == "OK" && len(frame) >= 3 {
			id, _ := frame[1].(string)
			if authEventID != "" && id == authEventID {
				ok, isBool := frame[2].(bool)
				if !isBool {
					continue
				}
				if !ok {
					return "unauthorized", "unauthorized"
				}
				authState = "ok"
				if len(resend) > 0 && retries < 1 {
					retries++
					_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
					if err := conn.WriteMessage(websocket.TextMessage, resend); err != nil {
						return "incomplete", authState
					}
				}
				continue
			}
		}
		switch classifyWriteFrame(frame, eventID) {
		case "accepted":
			return "accepted", authState
		case "rejected":
			reason := rejectionReason(frame)
			if reason == "auth_required" && auth != nil && authState != "ok" {
				continue
			}
			if reason == "auth_required" && authState == "ok" && len(resend) > 0 && retries < 1 {
				retries++
				_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, resend); err != nil {
					return "incomplete", authState
				}
				continue
			}
			return reason, authState
		case "mismatched":
			ok, isBool := frame[2].(bool)
			if isBool && !ok {
				continue
			}
			return "mismatched", authState
		case "closed":
			text := strings.ToLower(fmt.Sprint(frame))
			if strings.Contains(text, "auth-required") && authState == "ok" && len(resend) > 0 && retries < 1 {
				retries++
				_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, resend); err != nil {
					return "incomplete", authState
				}
				continue
			}
			return "closed", authState
		}
	}
}

func readSubscription(conn *websocket.Conn, sub string, filter map[string]any, deadline time.Time, auth func(challenge string) (map[string]any, error)) (string, []string, bool) {
	req, _ := json.Marshal([]any{"REQ", sub, filter})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
		return "incomplete", nil, false
	}
	var ids []string
	authID := ""
	authState := ""
	retries := 0
	messages := 0
	total := 0
	for messages < 32 && total < 256*1024 && time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(deadline)
		_, message, err := conn.ReadMessage()
		if err != nil {
			return "incomplete", ids, authState == "ok"
		}
		if len(message) > 64*1024 {
			return "incomplete", ids, authState == "ok"
		}
		messages++
		total += len(message)
		var frame []json.RawMessage
		if json.Unmarshal(message, &frame) != nil || len(frame) == 0 {
			continue
		}
		var typ string
		if json.Unmarshal(frame[0], &typ) != nil {
			continue
		}
		switch typ {
		case "AUTH":
			if auth == nil || len(frame) < 2 || authState == "ok" || retries >= 2 {
				continue
			}
			var challenge string
			if json.Unmarshal(frame[1], &challenge) != nil || challenge == "" {
				continue
			}
			signed, err := auth(challenge)
			if err != nil {
				return "auth_required", ids, false
			}
			authID, _ = signed["id"].(string)
			payload, _ := json.Marshal([]any{"AUTH", signed})
			_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return "incomplete", ids, false
			}
			authState = "sent"
		case "OK":
			if len(frame) < 3 || authID == "" {
				continue
			}
			var id string
			var ok bool
			if json.Unmarshal(frame[1], &id) != nil || json.Unmarshal(frame[2], &ok) != nil || id != authID {
				continue
			}
			if !ok {
				return "unauthorized", ids, false
			}
			authState = "ok"
			ids = nil
			if retries < 2 {
				retries++
				_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
					return "incomplete", nil, true
				}
			}
		case "EVENT":
			if len(frame) < 3 {
				continue
			}
			var got string
			if json.Unmarshal(frame[1], &got) != nil || got != sub {
				continue
			}
			parsed := nostr.ParseAndValidate(frame[2], nostr.Options{MaxRawJSONBytes: 64 * 1024, MaxContentBytes: 64 * 1024})
			if !parsed.Valid() || parsed.Event == nil || !eventMatchesFilter(parsed.Event, filter) {
				continue
			}
			ids = append(ids, parsed.Event.ID)
			if len(ids) > 8 {
				return "incomplete", ids, authState == "ok"
			}
		case "EOSE":
			if len(frame) < 2 {
				continue
			}
			var got string
			if json.Unmarshal(frame[1], &got) != nil || got != sub {
				continue
			}
			if authState == "sent" {
				continue
			}
			return "complete", ids, authState == "ok"
		case "CLOSED":
			if len(frame) < 2 {
				continue
			}
			var got string
			if json.Unmarshal(frame[1], &got) != nil || got != sub {
				continue
			}
			text := strings.ToLower(string(message))
			switch classifyRelayMessage(text) {
			case "auth_required":
				return "auth_required", ids, authState == "ok"
			case "unsupported":
				return "unsupported", ids, authState == "ok"
			case "rate_limited", "payment_required":
				return classifyRelayMessage(text), ids, authState == "ok"
			default:
				if strings.Contains(text, "restricted") || strings.Contains(text, "blocked") {
					return "denied", ids, authState == "ok"
				}
				return "incomplete", ids, authState == "ok"
			}
		}
	}
	return "incomplete", ids, authState == "ok"
}

func hasTagValue(tags [][]string, name string, wants []string) bool {
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != name {
			continue
		}
		for _, want := range wants {
			if tag[1] == want {
				return true
			}
		}
	}
	return false
}

func classifySearch(positiveHit, negativeHit bool) string {
	switch {
	case positiveHit && !negativeHit:
		return "verified"
	case positiveHit && negativeHit:
		return "ignored"
	default:
		return "inconclusive"
	}
}

func classifyCompletedSearch(positiveStatus, negativeStatus string, positiveHit, negativeHit bool) string {
	if positiveStatus != "complete" || negativeStatus != "complete" {
		return "inconclusive"
	}
	return classifySearch(positiveHit, negativeHit)
}

func readSearchIDs(conn *websocket.Conn, sub, query, author string) ([]string, string) {
	filter := map[string]any{"kinds": []int{1}, "search": query, "limit": 5}
	if author != "" {
		filter["authors"] = []string{author}
	}
	req, _ := json.Marshal([]any{"REQ", sub, filter})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
		return nil, "incomplete"
	}
	deadline := time.Now().Add(5 * time.Second)
	var ids []string
	messages := 0
	total := 0
	for messages < 32 && total < 256*1024 && time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(deadline)
		_, message, err := conn.ReadMessage()
		if err != nil {
			return ids, "incomplete"
		}
		if len(message) > 64*1024 {
			return ids, "incomplete"
		}
		messages++
		total += len(message)
		var frame []json.RawMessage
		if json.Unmarshal(message, &frame) != nil || len(frame) == 0 {
			continue
		}
		var typ string
		if json.Unmarshal(frame[0], &typ) != nil {
			continue
		}
		switch typ {
		case "EVENT":
			if len(frame) < 3 {
				continue
			}
			var got string
			if json.Unmarshal(frame[1], &got) != nil || got != sub {
				continue
			}
			parsed := nostr.ParseAndValidate(frame[2], nostr.Options{MaxRawJSONBytes: 64 * 1024, MaxContentBytes: 32 * 1024})
			if !parsed.Valid() || parsed.Event == nil {
				continue
			}
			ids = append(ids, parsed.Event.ID)
		case "EOSE":
			if len(frame) < 2 {
				continue
			}
			var got string
			if json.Unmarshal(frame[1], &got) != nil || got != sub {
				continue
			}
			return ids, "complete"
		case "CLOSED":
			if len(frame) < 2 {
				continue
			}
			var got string
			if json.Unmarshal(frame[1], &got) != nil || got != sub {
				continue
			}
			text := strings.ToLower(string(message))
			if strings.Contains(text, "unsupported") || strings.Contains(text, "not supported") {
				return ids, "unsupported"
			}
			return ids, "incomplete"
		}
	}
	return ids, "incomplete"
}

func containsID(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func signProbeEvent(priv *btcec.PrivateKey, pubkey string) (map[string]any, error) {
	created := time.Now().Unix()
	nonceSum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", pubkey, created)))
	nonce := hex.EncodeToString(nonceSum[:8])
	tags := [][]string{{"nonce", nonce}}
	content := "nostrmash probe " + nonce
	canonical, err := json.Marshal([]any{0, pubkey, created, 1, tags, content})
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(canonical)
	sig, err := schnorr.Sign(priv, sum[:])
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"id": hex.EncodeToString(sum[:]), "pubkey": pubkey, "created_at": created,
		"kind": 1, "tags": tags, "content": content, "sig": hex.EncodeToString(sig.Serialize()),
	}, nil
}

func storeObservation(ctx context.Context, pool *pgxpool.Pool, key, capability, status string, confidence float64, source, detail, token string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var attemptID *int64
	if token != "" {
		var leasedURL, leasedCap string
		var current bool
		err = tx.QueryRow(ctx, `
			SELECT l.url_key, l.capability, l.expires_at > now(), a.id
			FROM relay_probe_leases l
			JOIN relay_probe_attempts a ON a.lease_token = l.lease_token
			WHERE l.lease_token = $1
			FOR UPDATE OF l
		`, token).Scan(&leasedURL, &leasedCap, &current, &attemptID)
		if err != nil {
			return fmt.Errorf("probe lease %s cannot publish evidence", token)
		}
		if !current || leasedURL != key || !leaseAllows(leasedCap, capability) {
			return fmt.Errorf("probe lease %s cannot publish evidence", token)
		}
	}
	inserted := false
	if attemptID != nil {
		var sampleID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO relay_capability_samples (url_key, capability, status, confidence, source, detail, attempt_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (attempt_id, capability, status, detail) WHERE attempt_id IS NOT NULL DO NOTHING
			RETURNING id
		`, key, capability, status, confidence, source, detail, *attemptID).Scan(&sampleID)
		if err != nil {
			if SchemaMissing(err) {
				return nil
			}
			if errors.Is(err, pgx.ErrNoRows) {
				return tx.Commit(ctx)
			}
			return err
		}
		inserted = true
	} else {
		if _, err = tx.Exec(ctx, `
			INSERT INTO relay_capability_samples (url_key, capability, status, confidence, source, detail)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, key, capability, status, confidence, source, detail); err != nil {
			if SchemaMissing(err) {
				return nil
			}
			return err
		}
		inserted = true
	}
	if !inserted {
		return tx.Commit(ctx)
	}
	_, _ = tx.Exec(ctx, `
		DELETE FROM relay_capability_samples
		WHERE id IN (
			SELECT id FROM relay_capability_samples
			WHERE url_key = $1 AND capability = $2
			ORDER BY observed_at DESC, id DESC
			OFFSET 20
		)
	`, key, capability)
	recentFailures, recentWindow := 0, 0
	_ = tx.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE status IN ('unsupported', 'restricted', 'temporary'))
		FROM (
			SELECT status FROM relay_capability_samples
			WHERE url_key = $1 AND capability = $2
			ORDER BY observed_at DESC, id DESC
			LIMIT 20
		) recent
	`, key, capability).Scan(&recentWindow, &recentFailures)
	provisional := status == "inconclusive" || status == "temporary"
	if _, err = tx.Exec(ctx, `
		INSERT INTO relay_capability_observations (
			url_key, capability, status, confidence, source, detail, sample_count, observed_at
		) VALUES (
			$1, $2, $3, $4, $5, jsonb_build_object('text', $6::text, 'recent_failures', $7::int, 'recent_window', $8::int), 1, now()
		)
		ON CONFLICT (url_key, capability) DO UPDATE
		SET status = CASE
		        WHEN $9::bool AND relay_capability_observations.status IN ('verified', 'unsupported', 'restricted')
		        THEN relay_capability_observations.status
		        ELSE EXCLUDED.status
		    END,
		    confidence = EXCLUDED.confidence,
		    source = EXCLUDED.source,
		    detail = EXCLUDED.detail,
		    sample_count = relay_capability_observations.sample_count + 1,
		    observed_at = CASE WHEN $9::bool THEN relay_capability_observations.observed_at ELSE now() END
	`, key, capability, status, confidence, source, detail, recentFailures, recentWindow, provisional); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	metrics.IncRelayIntelCapability(capability, status)
	return nil
}
