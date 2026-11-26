package relayintel

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/nostrmash/nostrmash/internal/nostr"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// Once probes at most one approved relay. Retention follow-ups take one turn
// in four so they cannot occupy every cycle while publication work is waiting.
func (p *probeState) Once(ctx context.Context, pool *pgxpool.Pool) error {
	if p == nil || pool == nil {
		return nil
	}
	p.turn++
	followFirst := p.turn%4 == 0
	if followFirst {
		if followed, err := p.followOne(ctx, pool); err != nil || followed {
			return err
		}
	}
	if ran, err := p.probePrimary(ctx, pool); err != nil || ran {
		return err
	}
	if !followFirst {
		_, err := p.followOne(ctx, pool)
		return err
	}
	return nil
}

func (p *probeState) probePrimary(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	if p.giftwrap && p.turn%2 == 0 {
		probed, err := p.probeOneGiftWrap(ctx, pool)
		if err != nil || probed {
			return probed, err
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
		return false, wrapDB(err)
	}
	defer rows.Close()
	type candidate struct{ url, key, role string }
	var picks []candidate
	for rows.Next() {
		var item candidate
		if err := rows.Scan(&item.url, &item.key, &item.role); err != nil {
			return false, err
		}
		picks = append(picks, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	rows.Close()
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
		var probeErr error
		if item.role == RoleSearch {
			probeErr = p.search(ctx, pool, item.url, item.key, token)
		} else {
			probeErr = p.write(ctx, pool, item.url, item.key, token)
		}
		return true, probeErr
	}
	if p.giftwrap && p.turn%2 == 1 {
		return p.probeOneGiftWrap(ctx, pool)
	}
	return false, nil
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

func (p *probeState) scheduleRetention(ctx context.Context, pool *pgxpool.Pool, key, relayURL, eventID string, published time.Time) {
	if pool == nil || eventID == "" || !p.allowed(relayURL) {
		return
	}
	if published.IsZero() {
		published = time.Now()
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO relay_probe_followups (
			url_key, endpoint_url, event_id, capability, check_after, published_at, status
		) VALUES ($1, $2, $3, 'retention', $4, $5, 'pending')
		ON CONFLICT (url_key, event_id, capability) DO NOTHING
	`, key, relayURL, eventID, published.Add(time.Hour), published.UTC())
}

func (p *probeState) cleanupFollowups(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return nil
	}
	statements := []string{
		`DELETE FROM relay_probe_followups
		 WHERE id IN (
		     SELECT id FROM relay_probe_followups
		     WHERE finished_at IS NOT NULL AND finished_at < clock_timestamp() - interval '30 days'
		     ORDER BY finished_at
		     LIMIT 200
		 )`,
		`DELETE FROM relay_probe_identities
		 WHERE pubkey IN (
		     SELECT pubkey FROM relay_probe_identities
		     WHERE expires_at IS NOT NULL AND expires_at < clock_timestamp()
		     LIMIT 200
		 )`,
		`UPDATE relay_probe_followups
		 SET status = 'failed', result = 'retry_exhausted', finished_at = clock_timestamp()
		 WHERE id IN (
		     SELECT f.id FROM relay_probe_followups f
		     WHERE f.finished_at IS NULL
		       AND f.attempts >= 8
		       AND NOT EXISTS (
		           SELECT 1 FROM relay_probe_leases l
		           WHERE l.lease_token = f.lease_token
		             AND f.lease_token <> ''
		             AND l.expires_at > clock_timestamp()
		       )
		     LIMIT 200
		 )`,
		`UPDATE relay_probe_followups
		 SET status = 'failed', result = 'observation_expired', finished_at = clock_timestamp()
		 WHERE id IN (
		     SELECT f.id FROM relay_probe_followups f
		     WHERE f.finished_at IS NULL
		       AND f.published_at IS NOT NULL
		       AND f.published_at < clock_timestamp() - interval '7 days'
		       AND NOT EXISTS (
		           SELECT 1 FROM relay_probe_leases l
		           WHERE l.lease_token = f.lease_token
		             AND f.lease_token <> ''
		             AND l.expires_at > clock_timestamp()
		       )
		     LIMIT 200
		 )`,
	}
	for _, statement := range statements {
		if _, err := pool.Exec(ctx, statement); err != nil && !SchemaMissing(err) {
			return err
		}
	}
	return nil
}

func (p *probeState) followOne(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	if p == nil || pool == nil {
		return false, nil
	}
	if err := p.cleanupFollowups(ctx, pool); err != nil {
		return false, err
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id int64
	var relayURL, eventID, key, previousToken, followCapability string
	var publishedAt, checkAfter *time.Time
	var attempts int
	err = tx.QueryRow(ctx, `
		WITH picked AS (
			SELECT id, lease_token AS previous_token
			FROM relay_probe_followups
			WHERE finished_at IS NULL
			  AND attempts < 8
			  AND (published_at IS NULL OR published_at > clock_timestamp() - interval '7 days')
			  AND (
			    status IN ('pending', 'retryable') AND check_after <= clock_timestamp()
			    OR status IN ('claimed', 'running') AND (
			        claimed_at IS NULL OR claimed_at < clock_timestamp() - interval '2 minutes'
			    ) AND (
			        lease_token = ''
			        OR NOT EXISTS (
			            SELECT 1 FROM relay_probe_leases l
			            WHERE l.lease_token = relay_probe_followups.lease_token
			              AND l.expires_at > clock_timestamp()
			        )
			    )
			  )
			ORDER BY check_after, id
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE relay_probe_followups f
		SET status = 'claimed', claimed_at = clock_timestamp(), lease_token = ''
		FROM picked
		WHERE f.id = picked.id
		RETURNING f.id, f.endpoint_url, f.event_id, f.url_key, picked.previous_token, f.published_at, f.check_after, f.attempts, f.capability
	`).Scan(&id, &relayURL, &eventID, &key, &previousToken, &publishedAt, &checkAfter, &attempts, &followCapability)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || SchemaMissing(err) {
			return false, nil
		}
		return false, err
	}
	if !p.allowed(relayURL) {
		if _, err = tx.Exec(ctx, `
			UPDATE relay_probe_followups
			SET status = 'skipped', result = 'allowlist_removed', finished_at = clock_timestamp()
			WHERE id = $1 AND status = 'claimed'
		`, id); err != nil {
			return false, err
		}
		if err = tx.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}
	if previousToken != "" {
		if _, err = tx.Exec(ctx, `
			UPDATE relay_probe_attempts
			SET finished_at = clock_timestamp(), status = 'expired'
			WHERE lease_token = $1 AND url_key = $2 AND finished_at IS NULL
		`, previousToken, key); err != nil {
			return false, err
		}
	}
	seconds := int(p.minInterval.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	var token string
	if err = tx.QueryRow(ctx, `
		SELECT claim_relay_probe($1, $2, $3, $4, $5, $6, 120)
	`, key, CapRetention, probeHost(relayURL), p.worker, p.globalLimit, seconds).Scan(&token); err != nil {
		return false, err
	}
	if token == "" {
		if _, err = tx.Exec(ctx, `
			UPDATE relay_probe_followups
			SET status = 'pending', lease_token = '', deferrals = deferrals + 1,
			    check_after = clock_timestamp() + interval '30 seconds'
			WHERE id = $1 AND status = 'claimed' AND finished_at IS NULL
		`, id); err != nil {
			return false, err
		}
		if err = tx.Commit(ctx); err != nil {
			return false, err
		}
		metrics.IncRelayIntelProbe(CapRetention, "skipped")
		return false, nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE relay_probe_followups
		SET status = 'running', lease_token = $2, attempts = attempts + 1
		WHERE id = $1 AND status = 'claimed' AND finished_at IS NULL
		  AND url_key = $3 AND endpoint_url = $4 AND capability = $5
	`, id, token, key, relayURL, followCapability)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("follow-up %d lost its claim", id)
	}
	attempts++
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	published := time.Now().Add(-time.Hour)
	if publishedAt != nil {
		published = *publishedAt
	}
	conn, dialErr := dialRelay(ctx, relayURL, p.allowPrivate, 5*time.Second)
	var status string
	var ids []string
	if dialErr != nil {
		status = classifyDial(dialErr)
	} else {
		status, ids, _, _ = readSubscription(ctx, conn, "nm-retain", map[string]any{"ids": []string{eventID}, "limit": 1}, time.Now().Add(5*time.Second), func(challenge string) (map[string]any, error) {
			return signAuth(p.key, p.pubkey, relayURL, challenge)
		})
		conn.Close()
	}
	elapsed := int(time.Since(published).Seconds())
	if elapsed < 0 {
		elapsed = 0
	}
	retrieved := containsID(ids, eventID)
	result := fmt.Sprintf("missing_after_%ds", elapsed)
	obsStatus := "inconclusive"
	confidence := 0.35
	followStatus := "completed"
	followResult := result
	if status != "complete" && !retrieved {
		result = status
		if result == "" {
			result = "incomplete"
		}
		obsStatus = "temporary"
		followStatus = "retryable"
		followResult = result
	}
	if retrieved {
		result = fmt.Sprintf("retrieved_after_%ds", elapsed)
		obsStatus = StatusVerified
		confidence = 0.6
		followStatus = "completed"
		followResult = result
	}
	ageExceeded := !published.IsZero() && published.Before(time.Now().Add(-7*24*time.Hour))
	if followStatus == "retryable" && (attempts >= 8 || ageExceeded) {
		followStatus = "failed"
		if ageExceeded {
			followResult = "observation_expired"
		} else {
			followResult = "retry_exhausted"
		}
	}
	err = commitProbeResult(ctx, pool, key, token, probeCompletion{
		AttemptStatus: result, FollowupID: id, FollowStatus: followStatus, FollowResult: followResult,
		Elapsed: elapsed, Endpoint: relayURL, FollowCapability: followCapability,
		Writes: []observationWrite{{
			Capability: CapRetention, Status: obsStatus, Detail: result, Confidence: confidence,
		}},
	})
	if errors.Is(err, errProbeAlreadyCompleted) {
		return true, nil
	}
	if err != nil {
		releaseOwnedAttempt(ctx, pool, key, token, "commit_failed")
		nextStatus := "retryable"
		followResult = "commit_failed"
		if attempts >= 8 {
			nextStatus = "failed"
			followResult = "retry_exhausted"
		}
		_, _ = pool.Exec(ctx, `
			UPDATE relay_probe_followups
			SET status = $3,
			    result = $4,
			    lease_token = '',
			    check_after = clock_timestamp() + interval '5 minutes',
			    finished_at = CASE WHEN $3 = 'failed' THEN clock_timestamp() ELSE NULL END
			WHERE id = $1 AND lease_token = $2 AND url_key = $5 AND endpoint_url = $6
			  AND capability = $7 AND status IN ('running', 'claimed') AND finished_at IS NULL
		`, id, token, nextStatus, followResult, key, relayURL, followCapability)
		return true, err
	}
	return true, nil
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
		switch requested {
		case CapGiftWrap, CapGiftWrapAccept, CapGiftWrapPublic, CapAuthGiftWrap, CapAuthRecipient, CapRetention,
			CapGiftWrapRecipient, CapGiftWrapRecipientExact, CapGiftWrapRecipientTag,
			CapGiftWrapObserverExact, CapGiftWrapObserverTag, CapGiftWrapAnonExact, CapGiftWrapAnonTag,
			CapGiftWrapUnauthExposure, CapGiftWrapAuthExposure:
			return true
		default:
			return false
		}
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

func (p *probeState) write(ctx context.Context, pool *pgxpool.Pool, relayURL, key, token string) error {
	completion := p.collectWrite(ctx, relayURL)
	return p.publishProbe(ctx, pool, "write", key, token, completion)
}

func (p *probeState) collectWrite(ctx context.Context, relayURL string) probeCompletion {
	if ctx.Err() != nil {
		return probeCompletion{AttemptStatus: "canceled", Metric: "canceled"}
	}
	event, err := signProbeEvent(p.key, p.pubkey)
	if err != nil {
		return probeCompletion{AttemptStatus: "signing_failed", Metric: "error"}
	}
	conn, err := dialRelay(ctx, relayURL, p.allowPrivate, 5*time.Second)
	if err != nil {
		status := classifyDial(err)
		return probeCompletion{AttemptStatus: status, Metric: status}
	}
	defer conn.Close()
	payload, _ := json.Marshal([]any{"EVENT", event})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return probeCompletion{AttemptStatus: "write_failed", Metric: "error"}
	}
	id, _ := event["id"].(string)
	ack, authStatus := readWriteAckWithAuth(conn, id, payload, time.Now().Add(5*time.Second), func(challenge string) (map[string]any, error) {
		return signAuth(p.key, p.pubkey, relayURL, challenge)
	})
	var writes []observationWrite
	if authStatus == "ok" {
		writes = append(writes, observationWrite{Capability: CapAuthWrite, Status: StatusVerified, Detail: "auth_succeeded", Confidence: 0.7})
	} else if authStatus == "unauthorized" || ack == "auth_required" || authStatus == "failed" || authStatus == "required" {
		detail := authStatus
		if detail == "" {
			detail = ack
		}
		writes = append(writes, observationWrite{Capability: CapAuthWrite, Status: StatusRestricted, Detail: detail, Confidence: 0.6})
	}
	if ack != "accepted" {
		status, detail := publicationEvidence(ack)
		if ack == "" {
			ack = "write_failed"
		}
		writes = append(writes, observationWrite{Capability: CapWriteAccept, Status: status, Detail: detail, Confidence: 0.55})
		return probeCompletion{AttemptStatus: ack, Metric: ack, Writes: writes}
	}
	writes = append(writes, observationWrite{Capability: CapWriteAccept, Status: StatusVerified, Detail: "ok_true", Confidence: 0.8})
	created, _ := event["created_at"].(int64)
	retention := &scheduledRetention{RelayURL: relayURL, EventID: id, Published: time.Unix(created, 0)}
	if ctx.Err() != nil {
		return probeCompletion{AttemptStatus: "canceled", Metric: "canceled", Writes: writes, Retention: retention}
	}
	found, err := fetchEvents(ctx, relayURL, p.allowPrivate, map[string]any{"ids": []string{id}, "limit": 1})
	if err == nil {
		for _, item := range found {
			if item != nil && item.ID == id {
				writes = append(writes, observationWrite{Capability: CapWriteRetrieve, Status: StatusVerified, Detail: "retrieved", Confidence: 0.85})
				return probeCompletion{AttemptStatus: "retrieved", Metric: "ok", Writes: writes, Retention: retention}
			}
		}
		return probeCompletion{AttemptStatus: "accepted_not_retrieved", Metric: "incomplete", Writes: writes, Retention: retention}
	}
	return probeCompletion{AttemptStatus: "incomplete", Metric: "incomplete", Writes: writes, Retention: retention}
}

func (p *probeState) search(ctx context.Context, pool *pgxpool.Pool, relayURL, key, token string) error {
	completion := p.collectSearch(ctx, relayURL)
	return p.publishProbe(ctx, pool, "search", key, token, completion)
}

func (p *probeState) publishProbe(ctx context.Context, pool *pgxpool.Pool, metric, key, token string, completion probeCompletion) error {
	err := commitProbeResult(ctx, pool, key, token, completion)
	if errors.Is(err, errProbeAlreadyCompleted) {
		metrics.IncRelayIntelProbe(metric, completion.Metric)
		return nil
	}
	if err != nil {
		releaseOwnedAttempt(ctx, pool, key, token, "commit_failed")
		metrics.IncRelayIntelProbe(metric, "error")
		return err
	}
	metrics.IncRelayIntelProbe(metric, completion.Metric)
	return nil
}

func (p *probeState) collectSearch(ctx context.Context, relayURL string) probeCompletion {
	if ctx.Err() != nil {
		return probeCompletion{AttemptStatus: "canceled", Metric: "canceled"}
	}
	event, err := signProbeEvent(p.key, p.pubkey)
	if err != nil {
		return probeCompletion{AttemptStatus: "signing_failed", Metric: "error"}
	}
	conn, err := dialRelay(ctx, relayURL, p.allowPrivate, 5*time.Second)
	if err != nil {
		status := classifyDial(err)
		return probeCompletion{AttemptStatus: status, Metric: status}
	}
	defer conn.Close()
	id, _ := event["id"].(string)
	content, _ := event["content"].(string)
	payload, _ := json.Marshal([]any{"EVENT", event})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return probeCompletion{AttemptStatus: "write_failed", Metric: "error"}
	}
	auth := func(challenge string) (map[string]any, error) {
		return signAuth(p.key, p.pubkey, relayURL, challenge)
	}
	ack, authStatus := readWriteAckWithAuth(conn, id, payload, time.Now().Add(5*time.Second), auth)
	var writes []observationWrite
	if authStatus == "ok" {
		writes = append(writes, observationWrite{Capability: CapAuthSearch, Status: StatusVerified, Detail: "auth_succeeded", Confidence: 0.7})
	} else if authStatus == "unauthorized" || ack == "auth_required" || authStatus == "failed" || authStatus == "required" {
		detail := authStatus
		if detail == "" {
			detail = ack
		}
		writes = append(writes, observationWrite{Capability: CapAuthSearch, Status: StatusRestricted, Detail: detail, Confidence: 0.6})
	}
	if ack != "accepted" {
		status, detail := publicationEvidence(ack)
		if ack == "" {
			ack = "write_failed"
		}
		writes = append(writes, observationWrite{Capability: CapSearch, Status: status, Detail: detail, Confidence: 0.45})
		return probeCompletion{AttemptStatus: ack, Metric: "inconclusive", Writes: writes}
	}
	presentStatus, presentIDs, presentExposed, _ := readSubscription(ctx, conn, "nm-present", map[string]any{"ids": []string{id}, "limit": 1}, time.Now().Add(5*time.Second), auth)
	if !containsID(presentIDs, id) && !containsID(presentExposed, id) {
		writes = append(writes, observationWrite{Capability: CapSearch, Status: "inconclusive", Detail: "not_present:" + presentStatus, Confidence: 0.35})
		attempt := presentStatus
		if attempt == "" || attempt == "complete" {
			attempt = "not_present"
		}
		return probeCompletion{AttemptStatus: attempt, Metric: "inconclusive", Writes: writes}
	}
	positive, positiveStatus := readSearchIDs(ctx, conn, "nm-search-yes", content, p.pubkey, auth)
	negative, negativeStatus := readSearchIDs(ctx, conn, "nm-search-no", "nostrmash-relay-intel-negative-"+id, p.pubkey, auth)
	if positiveStatus == "unsupported" || negativeStatus == "unsupported" {
		writes = append(writes, observationWrite{Capability: CapSearch, Status: StatusUnsupported, Detail: "closed", Confidence: 0.7})
		return probeCompletion{AttemptStatus: "unsupported", Metric: "unsupported", Writes: writes}
	}
	outcome := classifyCompletedSearch(positiveStatus, negativeStatus, containsID(positive, id), containsID(negative, id))
	switch outcome {
	case "verified":
		writes = append(writes, observationWrite{Capability: CapSearch, Status: StatusVerified, Detail: "search_match", Confidence: 0.8})
		return probeCompletion{AttemptStatus: "verified", Metric: "ok", Writes: writes}
	case "ignored":
		writes = append(writes, observationWrite{Capability: CapSearch, Status: StatusRestricted, Detail: "search_ignored", Confidence: 0.6})
		return probeCompletion{AttemptStatus: "search_ignored", Metric: "inconclusive", Writes: writes}
	default:
		writes = append(writes, observationWrite{Capability: CapSearch, Status: "inconclusive", Detail: outcome, Confidence: 0.35})
		return probeCompletion{AttemptStatus: outcome, Metric: "inconclusive", Writes: writes}
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
		if len(message) > 64*1024 || total+len(message) > 256*1024 {
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

func readSubscription(ctx context.Context, conn *websocket.Conn, sub string, filter map[string]any, deadline time.Time, auth func(challenge string) (map[string]any, error)) (string, []string, []string, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	active := sub
	req, _ := json.Marshal([]any{"REQ", active, filter})
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
		return "incomplete", nil, nil, false
	}
	var ids []string
	var exposed []string
	authID := ""
	authState := ""
	retries := 0
	messages := 0
	total := 0
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stop:
		}
	}()
	_ = conn.SetReadDeadline(deadline)
	for messages < 32 && total < 256*1024 {
		if ctx.Err() != nil {
			return "incomplete", ids, exposed, authState == "ok"
		}
		_, message, err := conn.ReadMessage()
		if err != nil {
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				_ = conn.Close()
				return "timeout", ids, exposed, authState == "ok"
			}
			if ctx.Err() != nil {
				return "incomplete", ids, exposed, authState == "ok"
			}
			return "incomplete", ids, exposed, authState == "ok"
		}
		if len(message) > 64*1024 || total+len(message) > 256*1024 {
			return "incomplete", ids, exposed, authState == "ok"
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
				return "auth_required", ids, exposed, false
			}
			authID, _ = signed["id"].(string)
			payload, _ := json.Marshal([]any{"AUTH", signed})
			_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return "incomplete", ids, exposed, false
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
				return "unauthorized", ids, exposed, false
			}
			authState = "ok"
			for _, id := range ids {
				exposed = appendID(exposed, id)
			}
			ids = nil
			if retries < 2 {
				retries++
				active = fmt.Sprintf("%s-a%d", sub, retries)
				req, _ = json.Marshal([]any{"REQ", active, filter})
				_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
				if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
					return "incomplete", ids, exposed, true
				}
			}
		case "EVENT":
			if len(frame) < 3 {
				continue
			}
			var got string
			if json.Unmarshal(frame[1], &got) != nil || got != active {
				continue
			}
			parsed := nostr.ParseAndValidate(frame[2], nostr.Options{MaxRawJSONBytes: 64 * 1024, MaxContentBytes: 64 * 1024})
			if !parsed.Valid() || parsed.Event == nil || !eventMatchesFilter(parsed.Event, filter) {
				continue
			}
			if authState != "ok" {
				exposed = appendID(exposed, parsed.Event.ID)
			}
			ids = append(ids, parsed.Event.ID)
			if len(ids) > 8 {
				return "incomplete", ids, exposed, authState == "ok"
			}
		case "EOSE":
			if len(frame) < 2 {
				continue
			}
			var got string
			if json.Unmarshal(frame[1], &got) != nil || got != active {
				continue
			}
			if authState == "sent" {
				continue
			}
			return "complete", ids, exposed, authState == "ok"
		case "CLOSED":
			if len(frame) < 2 {
				continue
			}
			var got string
			if json.Unmarshal(frame[1], &got) != nil || got != active {
				continue
			}
			text := strings.ToLower(string(message))
			switch classifyRelayMessage(text) {
			case "auth_required":
				if auth != nil && authState != "ok" {
					continue
				}
				return "auth_required", ids, exposed, authState == "ok"
			case "unsupported":
				return "unsupported", ids, exposed, authState == "ok"
			case "rate_limited", "payment_required":
				return classifyRelayMessage(text), ids, exposed, authState == "ok"
			default:
				if strings.Contains(text, "restricted") || strings.Contains(text, "blocked") {
					return "denied", ids, exposed, authState == "ok"
				}
				return "incomplete", ids, exposed, authState == "ok"
			}
		}
	}
	if time.Now().After(deadline) {
		return "timeout", ids, exposed, authState == "ok"
	}
	return "incomplete", ids, exposed, authState == "ok"
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

func readSearchIDs(ctx context.Context, conn *websocket.Conn, sub, query, author string, auth func(string) (map[string]any, error)) ([]string, string) {
	filter := map[string]any{"kinds": []int{1}, "search": query, "limit": 5}
	if author != "" {
		filter["authors"] = []string{author}
	}
	status, ids, exposed, _ := readSubscription(ctx, conn, sub, filter, time.Now().Add(5*time.Second), auth)
	seen := append([]string{}, ids...)
	for _, id := range exposed {
		seen = appendID(seen, id)
	}
	return seen, status
}

func appendID(ids []string, id string) []string {
	if id == "" || containsID(ids, id) || len(ids) >= 16 {
		return ids
	}
	return append(ids, id)
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
	var nonceBuf [8]byte
	if _, err := rand.Read(nonceBuf[:]); err != nil {
		return nil, err
	}
	nonce := hex.EncodeToString(nonceBuf[:])
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
			SELECT l.url_key, l.capability, l.expires_at > clock_timestamp(), a.id
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
	var recordedAttempt int64
	if attemptID != nil {
		recordedAttempt = *attemptID
	}
	inserted, err := applySample(ctx, tx, key, recordedAttempt, observationWrite{
		Capability: capability, Status: status, Detail: detail, Confidence: confidence,
	})
	if err != nil {
		if SchemaMissing(err) {
			return nil
		}
		return err
	}
	if !inserted {
		return tx.Commit(ctx)
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	metrics.IncRelayIntelCapability(capability, status)
	return nil
}

type observationWrite struct {
	Capability      string
	Status          string
	Detail          string
	Confidence      float64
	EvidenceVersion int
	Retrieved       *bool
	Unauthenticated *bool
	Authenticated   *bool
}

type scheduledRetention struct {
	RelayURL  string
	EventID   string
	Published time.Time
}

type probeCompletion struct {
	AttemptStatus    string
	FollowupID       int64
	FollowStatus     string
	FollowResult     string
	Elapsed          int
	Endpoint         string
	FollowCapability string
	Writes           []observationWrite
	Retention        *scheduledRetention
	Metric           string
}

var errProbeAlreadyCompleted = errors.New("probe result already recorded")

func applySample(ctx context.Context, tx pgx.Tx, key string, attemptID int64, item observationWrite) (bool, error) {
	inserted := false
	if attemptID != 0 {
		var sampleID int64
		err := tx.QueryRow(ctx, `
			INSERT INTO relay_capability_samples (url_key, capability, status, confidence, source, detail, attempt_id)
			VALUES ($1, $2, $3, $4, 'probe', $5, $6)
			ON CONFLICT (attempt_id, capability, status, detail) WHERE attempt_id IS NOT NULL DO NOTHING
			RETURNING id
		`, key, item.Capability, item.Status, item.Confidence, item.Detail, attemptID).Scan(&sampleID)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		inserted = true
	} else {
		if _, err := tx.Exec(ctx, `
			INSERT INTO relay_capability_samples (url_key, capability, status, confidence, source, detail)
			VALUES ($1, $2, $3, $4, 'probe', $5)
		`, key, item.Capability, item.Status, item.Confidence, item.Detail); err != nil {
			return false, err
		}
		inserted = true
	}
	if !inserted {
		return false, nil
	}
	_, _ = tx.Exec(ctx, `
		DELETE FROM relay_capability_samples
		WHERE id IN (
			SELECT id FROM relay_capability_samples
			WHERE url_key = $1 AND capability = $2
			ORDER BY observed_at DESC, id DESC
			OFFSET 20
		)
	`, key, item.Capability)
	recentFailures, recentWindow, recentSuccesses, recentInconclusive := 0, 0, 0, 0
	_ = tx.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE status IN ('unsupported', 'restricted', 'temporary')),
		       count(*) FILTER (WHERE status = 'verified'),
		       count(*) FILTER (WHERE status IN ('inconclusive', 'temporary'))
		FROM (
			SELECT status FROM relay_capability_samples
			WHERE url_key = $1 AND capability = $2
			ORDER BY observed_at DESC, id DESC
			LIMIT 20
		) recent
	`, key, item.Capability).Scan(&recentWindow, &recentFailures, &recentSuccesses, &recentInconclusive)
	provisional := item.Status == "inconclusive" || item.Status == "temporary"
	conclusive := item.Status == StatusVerified || item.Status == StatusUnsupported || item.Status == StatusRestricted
	_, err := tx.Exec(ctx, `
		INSERT INTO relay_capability_observations (
			url_key, capability, status, confidence, source, detail, sample_count, observed_at
		) VALUES (
			$1, $2, $3, $4, 'probe', jsonb_strip_nulls(jsonb_build_object(
				'text', $5::text,
				'evidence_version', NULLIF($9::int, 0),
				'recent_failures', $6::int,
				'recent_window', $7::int,
				'recent_successes', $10::int,
				'recent_inconclusive', $11::int,
				'retrieved', $13::boolean,
				'unauthenticated', $14::boolean,
				'authenticated', $15::boolean,
				'last_attempt_at', clock_timestamp(),
				'last_conclusive_at', CASE WHEN $12::bool THEN clock_timestamp() ELSE NULL END,
				'last_success_at', CASE WHEN $3 = 'verified' THEN clock_timestamp() ELSE NULL END,
				'last_negative_at', CASE WHEN $3 IN ('unsupported', 'restricted') THEN clock_timestamp() ELSE NULL END,
				'conclusive_status', CASE WHEN $12::bool THEN $3 ELSE NULL END,
				'conclusive_confidence', CASE WHEN $12::bool THEN $4::float8 ELSE NULL END
			)), 1, clock_timestamp()
		)
		ON CONFLICT (url_key, capability) DO UPDATE
		SET status = CASE
		        WHEN (
		            $8::bool AND relay_capability_observations.status IN ('verified', 'unsupported', 'restricted')
		        ) OR (
		            $9::int < COALESCE((relay_capability_observations.detail->>'evidence_version')::int, 0)
		            AND relay_capability_observations.status = 'verified'
		            AND $3 = 'verified'
		        )
		        THEN relay_capability_observations.status
		        ELSE EXCLUDED.status
		    END,
		    confidence = CASE
		        WHEN (
		            $8::bool AND relay_capability_observations.status IN ('verified', 'unsupported', 'restricted')
		        ) OR (
		            $9::int < COALESCE((relay_capability_observations.detail->>'evidence_version')::int, 0)
		            AND relay_capability_observations.status = 'verified'
		            AND $3 = 'verified'
		        )
		        THEN relay_capability_observations.confidence
		        ELSE EXCLUDED.confidence
		    END,
		    source = CASE
		        WHEN (
		            $8::bool AND relay_capability_observations.status IN ('verified', 'unsupported', 'restricted')
		        ) OR (
		            $9::int < COALESCE((relay_capability_observations.detail->>'evidence_version')::int, 0)
		            AND relay_capability_observations.status = 'verified'
		            AND $3 = 'verified'
		        )
		        THEN relay_capability_observations.source
		        ELSE EXCLUDED.source
		    END,
		    detail = CASE
		        WHEN (
		            $8::bool AND relay_capability_observations.status IN ('verified', 'unsupported', 'restricted')
		        ) OR (
		            $9::int < COALESCE((relay_capability_observations.detail->>'evidence_version')::int, 0)
		            AND relay_capability_observations.status = 'verified'
		            AND $3 = 'verified'
		        )
		        THEN relay_capability_observations.detail
		            || jsonb_build_object(
		                'recent_failures', $6::int,
		                'recent_window', $7::int,
		                'recent_successes', $10::int,
		                'recent_inconclusive', $11::int,
		                'last_attempt_at', clock_timestamp()
		            )
		        ELSE EXCLUDED.detail
		    END,
		    sample_count = relay_capability_observations.sample_count + 1,
		    observed_at = CASE
		        WHEN (
		            $8::bool AND relay_capability_observations.status IN ('verified', 'unsupported', 'restricted')
		        ) OR (
		            $9::int < COALESCE((relay_capability_observations.detail->>'evidence_version')::int, 0)
		            AND relay_capability_observations.status = 'verified'
		            AND $3 = 'verified'
		        )
		        THEN relay_capability_observations.observed_at
		        ELSE clock_timestamp()
		    END
	`, key, item.Capability, item.Status, item.Confidence, item.Detail, recentFailures, recentWindow, provisional, item.EvidenceVersion, recentSuccesses, recentInconclusive, conclusive, item.Retrieved, item.Unauthenticated, item.Authenticated)
	return err == nil, err
}

func commitProbeResult(ctx context.Context, pool *pgxpool.Pool, key, token string, completion probeCompletion) error {
	if completion.AttemptStatus == "" {
		completion.AttemptStatus = "incomplete"
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var leasedURL, leasedCap, attemptURL, attemptCap, storedStatus string
	var current, attemptOpen bool
	var attemptID int64
	err = tx.QueryRow(ctx, `
		SELECT l.url_key, l.capability, l.expires_at > clock_timestamp(),
		       a.id, a.url_key, a.capability, a.status, a.finished_at IS NULL
		FROM relay_probe_leases l
		JOIN relay_probe_attempts a ON a.lease_token = l.lease_token
		WHERE l.lease_token = $1
		FOR UPDATE OF l
	`, token).Scan(&leasedURL, &leasedCap, &current, &attemptID, &attemptURL, &attemptCap, &storedStatus, &attemptOpen)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return recognizeFinishedAttempt(ctx, tx, key, token, completion.AttemptStatus)
		}
		return err
	}
	var stillCurrent bool
	if err = tx.QueryRow(ctx, `
		SELECT expires_at > clock_timestamp()
		FROM relay_probe_leases
		WHERE lease_token = $1
	`, token).Scan(&stillCurrent); err != nil {
		return err
	}
	if !current || !stillCurrent || leasedURL != key || attemptURL != key || attemptCap != leasedCap {
		return fmt.Errorf("probe lease %s cannot publish evidence", token)
	}
	if !attemptOpen {
		if storedStatus == completion.AttemptStatus {
			return errProbeAlreadyCompleted
		}
		return fmt.Errorf("conflicting probe result for %s: stored %s", token, storedStatus)
	}
	for _, item := range completion.Writes {
		if !leaseAllows(leasedCap, item.Capability) {
			return fmt.Errorf("probe lease %s cannot publish %s", token, item.Capability)
		}
		if _, err = applySample(ctx, tx, key, attemptID, item); err != nil {
			return err
		}
	}
	if completion.Retention != nil && completion.Retention.EventID != "" {
		published := completion.Retention.Published
		if published.IsZero() {
			published = time.Now().UTC()
		}
		if _, err = tx.Exec(ctx, `
			INSERT INTO relay_probe_followups (
				url_key, endpoint_url, event_id, capability, check_after, published_at, status
			) VALUES ($1, $2, $3, 'retention', $4, $5, 'pending')
			ON CONFLICT (url_key, event_id, capability) DO NOTHING
		`, key, completion.Retention.RelayURL, completion.Retention.EventID, published.Add(time.Hour), published.UTC()); err != nil {
			return err
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE relay_probe_attempts
		SET finished_at = clock_timestamp(), status = $2
		WHERE id = $1 AND lease_token = $3 AND url_key = $4 AND capability = $5 AND finished_at IS NULL
	`, attemptID, completion.AttemptStatus, token, key, leasedCap)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("probe attempt %d was not updated", attemptID)
	}
	if completion.FollowupID > 0 {
		endpoint := completion.Endpoint
		capability := completion.FollowCapability
		if capability == "" {
			capability = CapRetention
		}
		var followTag pgconn.CommandTag
		if completion.FollowStatus == "completed" || completion.FollowStatus == "skipped" || completion.FollowStatus == "failed" {
			followTag, err = tx.Exec(ctx, `
				UPDATE relay_probe_followups
				SET status = $2, result = $3, elapsed_seconds = $4,
				    verified_at = clock_timestamp(), finished_at = clock_timestamp()
				WHERE id = $1 AND lease_token = $5 AND url_key = $6 AND endpoint_url = $7
				  AND capability = $8 AND status IN ('running', 'claimed') AND finished_at IS NULL
			`, completion.FollowupID, completion.FollowStatus, completion.FollowResult, completion.Elapsed, token, key, endpoint, capability)
		} else {
			followTag, err = tx.Exec(ctx, `
				UPDATE relay_probe_followups
				SET status = $2, result = $3, elapsed_seconds = $4,
				    verified_at = clock_timestamp(), lease_token = '',
				    check_after = clock_timestamp() + interval '5 minutes'
				WHERE id = $1 AND lease_token = $5 AND url_key = $6 AND endpoint_url = $7
				  AND capability = $8 AND status IN ('running', 'claimed') AND finished_at IS NULL
			`, completion.FollowupID, completion.FollowStatus, completion.FollowResult, completion.Elapsed, token, key, endpoint, capability)
		}
		if err != nil {
			return err
		}
		if followTag.RowsAffected() != 1 {
			return fmt.Errorf("follow-up %d is not owned by lease %s", completion.FollowupID, token)
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM relay_probe_leases WHERE lease_token = $1 AND url_key = $2`, token, key); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func releaseOwnedAttempt(ctx context.Context, pool *pgxpool.Pool, key, token, status string) {
	if pool == nil || token == "" || key == "" {
		return
	}
	if status == "" {
		status = "commit_failed"
	}
	tag, err := pool.Exec(ctx, `
		UPDATE relay_probe_attempts
		SET finished_at = clock_timestamp(), status = $2
		WHERE lease_token = $1 AND url_key = $3 AND finished_at IS NULL
	`, token, status, key)
	if err != nil || tag.RowsAffected() != 1 {
		return
	}
	_, _ = pool.Exec(ctx, `DELETE FROM relay_probe_leases WHERE lease_token = $1 AND url_key = $2`, token, key)
}

func recognizeFinishedAttempt(ctx context.Context, tx pgx.Tx, key, token, attemptStatus string) error {
	var storedKey, storedStatus string
	var finished bool
	err := tx.QueryRow(ctx, `
		SELECT url_key, status, finished_at IS NOT NULL
		FROM relay_probe_attempts
		WHERE lease_token = $1
		ORDER BY id DESC
		LIMIT 1
	`, token).Scan(&storedKey, &storedStatus, &finished)
	if err != nil {
		return fmt.Errorf("probe lease %s cannot publish evidence", token)
	}
	if storedKey != key || !finished {
		return fmt.Errorf("probe lease %s cannot publish evidence", token)
	}
	if storedStatus == attemptStatus {
		return errProbeAlreadyCompleted
	}
	return fmt.Errorf("conflicting probe result for %s: stored %s", token, storedStatus)
}
