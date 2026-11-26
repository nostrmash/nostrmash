package relayintel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nostrmash/nostrmash/internal/metrics"
)

// probeState is the worker-only active probe. It is off unless a synthetic key is configured.
type probeState struct {
	key          *btcec.PrivateKey
	pubkey       string
	allowPrivate bool
	globalLimit  int
	minInterval  time.Duration
	giftwrap     bool
	mu           sync.Mutex
	hour         time.Time
	used         int
	last         map[string]time.Time
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
	return &probeState{
		key: priv, pubkey: pub, allowPrivate: allowPrivate,
		globalLimit: globalPerHour, minInterval: minInterval, giftwrap: giftwrap,
		last: map[string]time.Time{},
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
	if !p.budgetLeft() {
		metrics.IncRelayIntelProbe("write", "skipped")
		return nil
	}
	rows, err := pool.Query(ctx, `
		SELECT endpoint_url, url_key, role
		FROM relay_role_stats
		WHERE role IN ('public_outbox', 'search')
		  AND endpoint_type = 'nostr_relay'
		ORDER BY trust_weighted DESC, url_key ASC
		LIMIT 20
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
		if !p.due(item.key) || !p.spend() {
			continue
		}
		if item.role == RoleSearch {
			p.search(ctx, pool, item.url, item.key)
		} else {
			p.write(ctx, pool, item.url, item.key)
		}
		return nil
	}
	return nil
}

func (p *probeState) budgetLeft() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollHourLocked()
	return p.used < p.globalLimit
}

func (p *probeState) spend() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollHourLocked()
	if p.used >= p.globalLimit {
		return false
	}
	p.used++
	return true
}

func (p *probeState) rollHourLocked() {
	now := time.Now()
	if p.hour.IsZero() || now.Sub(p.hour) >= time.Hour {
		p.hour = now
		p.used = 0
	}
}

func (p *probeState) due(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	last, ok := p.last[key]
	if ok && time.Since(last) < p.minInterval {
		return false
	}
	p.last[key] = time.Now()
	return true
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
	accepted := false
	deadline := time.Now().Add(5 * time.Second)
	for {
		_ = conn.SetReadDeadline(deadline)
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var frame []any
		if json.Unmarshal(message, &frame) != nil || len(frame) < 3 {
			continue
		}
		typ, _ := frame[0].(string)
		if typ == "OK" {
			if ok, _ := frame[2].(bool); ok {
				accepted = true
			}
			break
		}
		if typ == "NOTICE" || typ == "CLOSED" {
			break
		}
	}
	if !accepted {
		_ = storeObservation(ctx, pool, key, CapWriteAccept, StatusUnsupported, 0.7, "probe", "ok_false")
		metrics.IncRelayIntelProbe("write", "unsupported")
		return
	}
	_ = storeObservation(ctx, pool, key, CapWriteAccept, StatusVerified, 0.8, "probe", "ok_true")
	id, _ := event["id"].(string)
	found, err := fetchEvents(ctx, relayURL, p.allowPrivate, map[string]any{"ids": []string{id}, "limit": 1})
	if err == nil {
		for _, item := range found {
			if item != nil && item.ID == id {
				_ = storeObservation(ctx, pool, key, CapWriteRetrieve, StatusVerified, 0.85, "probe", "retrieved")
				metrics.IncRelayIntelProbe("write", "ok")
				return
			}
		}
	}
	metrics.IncRelayIntelProbe("write", "ok")
}

func (p *probeState) search(ctx context.Context, pool *pgxpool.Pool, relayURL, key string) {
	conn, err := dialRelay(ctx, relayURL, p.allowPrivate, 5*time.Second)
	if err != nil {
		metrics.IncRelayIntelProbe("search", classifyDial(err))
		return
	}
	defer conn.Close()
	req, _ := json.Marshal([]any{"REQ", "nm-search", map[string]any{"kinds": []int{1}, "search": "nostrmash-relay-intel-probe", "limit": 1}})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
		metrics.IncRelayIntelProbe("search", "error")
		return
	}
	deadline := time.Now().Add(5 * time.Second)
	matched := false
	rejected := false
	for {
		_ = conn.SetReadDeadline(deadline)
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var frame []any
		if json.Unmarshal(message, &frame) != nil || len(frame) == 0 {
			continue
		}
		typ, _ := frame[0].(string)
		switch typ {
		case "EVENT":
			if len(frame) >= 3 {
				if body, ok := frame[2].(map[string]any); ok {
					content, _ := body["content"].(string)
					if strings.Contains(content, "nostrmash relay intelligence probe") {
						matched = true
					}
				}
			}
		case "EOSE":
			if matched {
				_ = storeObservation(ctx, pool, key, CapSearch, StatusVerified, 0.8, "probe", "search_match")
				metrics.IncRelayIntelProbe("search", "ok")
			} else {
				metrics.IncRelayIntelProbe("search", "inconclusive")
			}
			return
		case "CLOSED", "NOTICE":
			text := strings.ToLower(fmt.Sprint(frame))
			if strings.Contains(text, "unsupported") || strings.Contains(text, "not supported") {
				rejected = true
			} else {
				metrics.IncRelayIntelProbe("search", "restricted")
				return
			}
		}
		if rejected {
			break
		}
	}
	if rejected {
		_ = storeObservation(ctx, pool, key, CapSearch, StatusUnsupported, 0.7, "probe", "closed")
		metrics.IncRelayIntelProbe("search", "unsupported")
		return
	}
	metrics.IncRelayIntelProbe("search", "timeout")
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
	_, err := pool.Exec(ctx, `
		INSERT INTO relay_capability_observations (
			url_key, capability, status, confidence, source, detail, sample_count, observed_at
		) VALUES (
			$1, $2, $3, $4, $5, jsonb_build_object('text', $6::text), 1, now()
		)
		ON CONFLICT (url_key, capability) DO UPDATE
		SET status = EXCLUDED.status,
		    confidence = EXCLUDED.confidence,
		    source = EXCLUDED.source,
		    detail = EXCLUDED.detail,
		    sample_count = relay_capability_observations.sample_count + 1,
		    observed_at = now()
	`, key, capability, status, confidence, source, detail)
	if err != nil {
		return err
	}
	metrics.IncRelayIntelCapability(capability, status)
	return nil
}
