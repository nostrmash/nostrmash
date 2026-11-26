package relayintel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/nostrmash/nostrmash/internal/nostr"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/gorilla/websocket"
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
	giftwrapLog  bool
}

func newProbeState(secretHex string, allowPrivate bool, globalPerHour int, minInterval time.Duration, giftwrap bool) (*probeState, string, error) {
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
	host, _ := os.Hostname()
	if host == "" {
		host = "worker"
	}
	return &probeState{
		key: priv, pubkey: pub, worker: fmt.Sprintf("%s:%d", host, os.Getpid()),
		allowPrivate: allowPrivate, globalLimit: globalPerHour, minInterval: minInterval, giftwrap: giftwrap,
	}, pub, nil
}

// Once probes at most one relay. Gift-wrap probes are not implemented.
func (p *probeState) Once(ctx context.Context, pool *pgxpool.Pool) error {
	if p == nil || pool == nil {
		return nil
	}
	if p.giftwrap && !p.giftwrapLog {
		p.giftwrapLog = true
		metrics.IncRelayIntelProbe("giftwrap", "disabled")
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
		ORDER BY o.observed_at ASC NULLS FIRST, s.trust_weighted DESC, s.url_key ASC
		LIMIT 50
	`)
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
		token, ok := p.claim(ctx, pool, item.key, capability, probeHost(item.url))
		if !ok {
			continue
		}
		if item.role == RoleSearch {
			p.search(ctx, pool, item.url, item.key)
		} else {
			p.write(ctx, pool, item.url, item.key)
		}
		p.finish(ctx, pool, token)
		return nil
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

func probeHost(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" {
		return raw
	}
	return parsed.Hostname()
}

func (p *probeState) write(ctx context.Context, pool *pgxpool.Pool, relayURL, key string) {
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
	ack := readWriteAck(conn, id, time.Now().Add(5*time.Second))
	if ack != "accepted" {
		if pool != nil && ack == "rejected" {
			_ = storeObservation(ctx, pool, key, CapWriteAccept, StatusRestricted, 0.55, "probe", "ok_false")
		}
		metrics.IncRelayIntelProbe("write", ack)
		return
	}
	if pool != nil {
		_ = storeObservation(ctx, pool, key, CapWriteAccept, StatusVerified, 0.8, "probe", "ok_true")
	}
	found, err := fetchEvents(ctx, relayURL, p.allowPrivate, map[string]any{"ids": []string{id}, "limit": 1})
	if err == nil {
		for _, item := range found {
			if item != nil && item.ID == id {
				if pool != nil {
					_ = storeObservation(ctx, pool, key, CapWriteRetrieve, StatusVerified, 0.85, "probe", "retrieved")
				}
				metrics.IncRelayIntelProbe("write", "ok")
				return
			}
		}
	}
	metrics.IncRelayIntelProbe("write", "incomplete")
}

func (p *probeState) search(ctx context.Context, pool *pgxpool.Pool, relayURL, key string) {
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
	payload, _ := json.Marshal([]any{"EVENT", event})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		metrics.IncRelayIntelProbe("search", "error")
		return
	}
	if readWriteAck(conn, id, time.Now().Add(5*time.Second)) != "accepted" {
		metrics.IncRelayIntelProbe("search", "inconclusive")
		return
	}
	positive, positiveStatus := readSearchIDs(conn, "nm-search-yes", "nostrmash relay intelligence probe")
	negative, negativeStatus := readSearchIDs(conn, "nm-search-no", "nostrmash-relay-intel-negative")
	if positiveStatus == "unsupported" || negativeStatus == "unsupported" {
		if pool != nil {
			_ = storeObservation(ctx, pool, key, CapSearch, StatusUnsupported, 0.7, "probe", "closed")
		}
		metrics.IncRelayIntelProbe("search", "unsupported")
		return
	}
	outcome := classifyCompletedSearch(positiveStatus, negativeStatus, containsID(positive, id), containsID(negative, id))
	switch outcome {
	case "verified":
		if pool != nil {
			_ = storeObservation(ctx, pool, key, CapSearch, StatusVerified, 0.8, "probe", "search_match")
		}
		metrics.IncRelayIntelProbe("search", "ok")
	case "ignored":
		if pool != nil {
			_ = storeObservation(ctx, pool, key, CapSearch, StatusRestricted, 0.6, "probe", "search_ignored")
		}
		metrics.IncRelayIntelProbe("search", "inconclusive")
	default:
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
		if !isBool || !ok {
			return "rejected"
		}
		if id != eventID {
			return "mismatched"
		}
		return "accepted"
	case "NOTICE", "CLOSED":
		return "closed"
	default:
		return "other"
	}
}

func readWriteAck(conn *websocket.Conn, eventID string, deadline time.Time) string {
	for {
		_ = conn.SetReadDeadline(deadline)
		_, message, err := conn.ReadMessage()
		if err != nil {
			return "timeout"
		}
		var frame []any
		if json.Unmarshal(message, &frame) != nil {
			continue
		}
		switch classifyWriteFrame(frame, eventID) {
		case "accepted":
			return "accepted"
		case "rejected", "mismatched", "closed":
			return classifyWriteFrame(frame, eventID)
		}
	}
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

func readSearchIDs(conn *websocket.Conn, sub, query string) ([]string, string) {
	req, _ := json.Marshal([]any{"REQ", sub, map[string]any{"kinds": []int{1}, "search": query, "limit": 5}})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
		return nil, "incomplete"
	}
	deadline := time.Now().Add(5 * time.Second)
	var ids []string
	for {
		_ = conn.SetReadDeadline(deadline)
		_, message, err := conn.ReadMessage()
		if err != nil {
			return ids, "incomplete"
		}
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
	content := "nostrmash relay intelligence probe"
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

func storeObservation(ctx context.Context, pool *pgxpool.Pool, key, capability, status string, confidence float64, source, detail string) error {
	recentFailures := 0
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_capability_samples (url_key, capability, status, confidence, source, detail)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, key, capability, status, confidence, source, detail); err != nil && !SchemaMissing(err) {
		return err
	} else if err == nil {
		_, _ = pool.Exec(ctx, `
			DELETE FROM relay_capability_samples
			WHERE id IN (
				SELECT id FROM relay_capability_samples
				WHERE url_key = $1 AND capability = $2
				ORDER BY observed_at DESC, id DESC
				OFFSET 20
			)
		`, key, capability)
		_ = pool.QueryRow(ctx, `
			SELECT count(*) FILTER (WHERE status IN ('unsupported', 'restricted', 'inconclusive', 'temporary'))
			FROM (
				SELECT status FROM relay_capability_samples
				WHERE url_key = $1 AND capability = $2
				ORDER BY observed_at DESC, id DESC
				LIMIT 20
			) recent
		`, key, capability).Scan(&recentFailures)
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO relay_capability_observations (
			url_key, capability, status, confidence, source, detail, sample_count, observed_at
		) VALUES (
			$1, $2, $3, $4, $5, jsonb_build_object('text', $6::text, 'recent_failures', $7::int), 1, now()
		)
		ON CONFLICT (url_key, capability) DO UPDATE
		SET status = EXCLUDED.status,
		    confidence = EXCLUDED.confidence,
		    source = EXCLUDED.source,
		    detail = EXCLUDED.detail,
		    sample_count = relay_capability_observations.sample_count + 1,
		    observed_at = now()
	`, key, capability, status, confidence, source, detail, recentFailures)
	if err != nil {
		return err
	}
	metrics.IncRelayIntelCapability(capability, status)
	return nil
}
