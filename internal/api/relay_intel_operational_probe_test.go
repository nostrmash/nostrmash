//go:build relayintelbench

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/relayintel"
	"github.com/nostrmash/nostrmash/internal/relayurl"
)

func measureAtomicCompletion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, out map[string]any) {
	t.Helper()
	resetProbeWindow(t, ctx, pool)
	out["completion_digest"] = "The integrated implementation has no completion digest. There is no digest overhead to measure."
	out["completion_path_note"] = "public_write and search use publishProbe. Gift-wrap, retention, replay, conflict, expiry, and concurrent completion use commitProbeResult, which publishProbe and gift-wrap storage both call. Neither path dials a relay."

	writeKey := "wss://write-finish.bench.example"
	writeToken := claim(t, ctx, pool, writeKey, relayintel.CapWriteAccept, "write-finish.bench.example", "bench", 60, 1)
	write := relayintel.BenchCompletion{
		AttemptStatus: "finished",
		Metric:        "ok",
		Writes: []relayintel.BenchObservation{
			{Capability: relayintel.CapWriteAccept, Status: relayintel.StatusVerified, Detail: "ok_true", Confidence: 0.8},
			{Capability: relayintel.CapWriteRetrieve, Status: relayintel.StatusVerified, Detail: "retrieved", Confidence: 0.85},
		},
	}
	started := time.Now()
	err := relayintel.BenchPublishProbe(ctx, pool, "write", writeKey, writeToken, write)
	var attemptStatus string
	var finished bool
	var leases int
	_ = pool.QueryRow(ctx, `SELECT status, finished_at IS NOT NULL FROM relay_probe_attempts WHERE lease_token = $1`, writeToken).Scan(&attemptStatus, &finished)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_leases WHERE lease_token = $1`, writeToken).Scan(&leases)
	out["public_write_completion"] = map[string]any{
		"path": "publishProbe", "wall_ms": ms(time.Since(started)), "error": errorText(err),
		"attempt_status": attemptStatus, "finished": finished, "lease_rows": leases,
		"accepted": err == nil && finished && attemptStatus == "finished" && leases == 0,
	}

	searchKey := "wss://search-finish.bench.example"
	searchToken := claim(t, ctx, pool, searchKey, relayintel.CapSearch, "search-finish.bench.example", "bench", 60, 1)
	started = time.Now()
	err = relayintel.BenchPublishProbe(ctx, pool, "search", searchKey, searchToken, relayintel.BenchCompletion{
		AttemptStatus: "finished",
		Metric:        "ok",
		Writes: []relayintel.BenchObservation{
			{Capability: relayintel.CapSearch, Status: relayintel.StatusVerified, Detail: "search_match", Confidence: 0.8},
		},
	})
	var searchSamples int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_capability_samples WHERE url_key = $1 AND capability = 'search'`, searchKey).Scan(&searchSamples)
	out["search_completion"] = map[string]any{
		"path": "publishProbe", "wall_ms": ms(time.Since(started)), "error": errorText(err),
		"sample_rows": searchSamples, "accepted": err == nil && searchSamples == 1,
	}

	giftURL := "wss://gift.bench.example"
	normalized, normErr := relayurl.NormalizePublic(giftURL)
	giftKey := giftURL
	if normErr == nil {
		giftKey = relayurl.CanonicalKey(normalized)
	}
	giftToken := claim(t, ctx, pool, giftKey, relayintel.CapGiftWrapPrivate, "gift.bench.example", "bench", 60, 1)
	retrieved := true
	notRetrieved := false
	started = time.Now()
	giftOutcome, giftErr := relayintel.BenchCommitProbe(ctx, pool, giftKey, giftToken, relayintel.BenchCompletion{
		AttemptStatus: "private",
		Writes: []relayintel.BenchObservation{
			{Capability: relayintel.CapGiftWrapAccept, Status: relayintel.StatusVerified, Detail: "ok_true", Confidence: 0.7},
			{Capability: relayintel.CapGiftWrapRecipientExact, Status: relayintel.StatusVerified, Detail: "authenticated_hit", Confidence: 0.7, Retrieved: &retrieved, Authenticated: &retrieved},
			{Capability: relayintel.CapGiftWrapRecipient, Status: relayintel.StatusVerified, Detail: "authenticated_recipient_retrieval", Confidence: 0.8, Retrieved: &retrieved},
			{Capability: relayintel.CapGiftWrapUnauthExposure, Status: relayintel.StatusVerified, Detail: "unauthenticated_exposure", Confidence: 0.8, Retrieved: &notRetrieved, Unauthenticated: &notRetrieved},
			{Capability: relayintel.CapGiftWrapAuthExposure, Status: relayintel.StatusVerified, Detail: "authenticated_observer_exposure", Confidence: 0.8, Retrieved: &notRetrieved, Authenticated: &notRetrieved},
			{Capability: relayintel.CapGiftWrapPrivate, Status: relayintel.StatusVerified, Detail: "recipient_only", Confidence: 0.8, EvidenceVersion: relayintel.ComprehensivePrivacyVersion},
			{Capability: relayintel.CapGiftWrap, Status: relayintel.StatusVerified, Detail: "recipient_only", Confidence: 0.8, EvidenceVersion: relayintel.ComprehensivePrivacyVersion},
		},
	})
	var giftSamples int
	var evidenceVersion int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_capability_samples WHERE url_key = $1`, giftKey).Scan(&giftSamples)
	_ = pool.QueryRow(ctx, `SELECT COALESCE((detail->>'evidence_version')::int, 0) FROM relay_capability_observations WHERE url_key = $1 AND capability = $2`, giftKey, relayintel.CapGiftWrapPrivate).Scan(&evidenceVersion)
	privacy := map[string]any{"error": errorText(normErr)}
	if normErr == nil {
		readStart := time.Now()
		resp, readErr := relayintel.NewService(pool, relayintel.Options{}).Capabilities(ctx, giftURL)
		raw, _ := json.Marshal(resp.Privacy)
		_ = json.Unmarshal(raw, &privacy)
		privacy["read_wall_ms"] = ms(time.Since(readStart))
		privacy["read_error"] = errorText(readErr)
		privacy["assessment_is_recipient_only"] = resp.Privacy.Assessment == "recipient_only_verified"
	}
	out["giftwrap_completion"] = map[string]any{
		"path": "commitProbeResult", "wall_ms": ms(time.Since(started)), "outcome": giftOutcome, "error": errorText(giftErr),
		"sample_rows": giftSamples, "private_evidence_version": evidenceVersion,
		"accepted": giftErr == nil && giftOutcome == "committed" && giftSamples == 7 && evidenceVersion == relayintel.ComprehensivePrivacyVersion,
	}
	out["structured_privacy_read"] = privacy

	scheduleKey := "wss://schedule.bench.example"
	scheduleToken := claim(t, ctx, pool, scheduleKey, relayintel.CapWriteAccept, "schedule.bench.example", "bench", 60, 1)
	eventID := fmt.Sprintf("%064x", time.Now().UnixNano())
	started = time.Now()
	scheduleOutcome, scheduleErr := relayintel.BenchCommitProbe(ctx, pool, scheduleKey, scheduleToken, relayintel.BenchCompletion{
		AttemptStatus:    "finished",
		RetentionURL:     scheduleKey,
		RetentionEventID: eventID,
		Writes: []relayintel.BenchObservation{
			{Capability: relayintel.CapWriteAccept, Status: relayintel.StatusVerified, Detail: "ok_true", Confidence: 0.8},
		},
	})
	var scheduled int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_followups WHERE event_id = $1 AND capability = 'retention' AND status = 'pending'`, eventID).Scan(&scheduled)
	out["retention_followup_scheduling"] = map[string]any{
		"path": "commitProbeResult", "wall_ms": ms(time.Since(started)), "outcome": scheduleOutcome, "error": errorText(scheduleErr),
		"pending_rows": scheduled, "accepted": scheduleErr == nil && scheduled == 1,
	}

	var followID int64
	var followKey, followURL string
	err = pool.QueryRow(ctx, `
		SELECT id, url_key, endpoint_url FROM relay_probe_followups
		WHERE finished_at IS NULL AND status = 'pending' AND check_after <= now()
		ORDER BY id LIMIT 1
	`).Scan(&followID, &followKey, &followURL)
	if err != nil {
		out["retention_followup_claim"] = map[string]any{"error": err.Error()}
	} else {
		claimStart := time.Now()
		followToken := claim(t, ctx, pool, followKey, relayintel.CapRetention, "follow.bench.example", "bench", 60, 1)
		if followToken == "" {
			out["retention_followup_claim"] = map[string]any{"wall_ms": ms(time.Since(claimStart)), "obtained": false}
		} else {
			if _, err := pool.Exec(ctx, `
				UPDATE relay_probe_followups
				SET status = 'running', lease_token = $2, attempts = attempts + 1, claimed_at = now()
				WHERE id = $1
			`, followID, followToken); err != nil {
				t.Fatal(err)
			}
			done := time.Now()
			outcome, finishErr := relayintel.BenchCommitProbe(ctx, pool, followKey, followToken, relayintel.BenchCompletion{
				AttemptStatus: "finished", FollowupID: followID, FollowStatus: "completed",
				FollowResult: "retrieved_after_120s", Elapsed: 120, Endpoint: followURL, FollowCapability: relayintel.CapRetention,
				Writes: []relayintel.BenchObservation{
					{Capability: relayintel.CapRetention, Status: relayintel.StatusVerified, Detail: "retrieved_after_120s", Confidence: 0.6},
				},
			})
			var followStatus string
			var followFinished bool
			_ = pool.QueryRow(ctx, `SELECT status, finished_at IS NOT NULL FROM relay_probe_followups WHERE id = $1`, followID).Scan(&followStatus, &followFinished)
			out["retention_followup_claim"] = map[string]any{"wall_ms": ms(time.Since(claimStart)), "obtained": true, "url": followURL}
			out["retention_followup_completion"] = map[string]any{
				"path": "commitProbeResult", "wall_ms": ms(time.Since(done)), "outcome": outcome, "error": errorText(finishErr),
				"status": followStatus, "finished": followFinished,
				"accepted": finishErr == nil && outcome == "committed" && followStatus == "completed" && followFinished,
			}
		}
	}

	replayKey := "wss://replay.bench.example"
	replayToken := claim(t, ctx, pool, replayKey, relayintel.CapWriteAccept, "replay.bench.example", "bench", 60, 1)
	replay := relayintel.BenchCompletion{
		AttemptStatus: "finished",
		Writes: []relayintel.BenchObservation{
			{Capability: relayintel.CapWriteAccept, Status: relayintel.StatusVerified, Detail: "ok_true", Confidence: 0.8},
		},
	}
	firstOutcome, firstErr := relayintel.BenchCommitProbe(ctx, pool, replayKey, replayToken, replay)
	started = time.Now()
	replayOutcome, replayErr := relayintel.BenchCommitProbe(ctx, pool, replayKey, replayToken, replay)
	var replaySamples int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_capability_samples WHERE url_key = $1 AND detail = 'ok_true'`, replayKey).Scan(&replaySamples)
	out["identical_completion_replay"] = map[string]any{
		"path": "commitProbeResult", "first_outcome": firstOutcome, "first_error": errorText(firstErr),
		"wall_ms": ms(time.Since(started)), "outcome": replayOutcome, "error": errorText(replayErr),
		"sample_rows": replaySamples, "accepted": firstOutcome == "committed" && replayOutcome == "already_completed" && replaySamples == 1,
	}
	started = time.Now()
	conflictOutcome, conflictErr := relayintel.BenchCommitProbe(ctx, pool, replayKey, replayToken, relayintel.BenchCompletion{AttemptStatus: "failed"})
	out["conflicting_completion"] = map[string]any{
		"path": "commitProbeResult", "wall_ms": ms(time.Since(started)), "outcome": conflictOutcome, "error": errorText(conflictErr),
		"accepted": conflictOutcome == "error" && conflictErr != nil,
	}

	expireKey := "wss://expire-complete.bench.example"
	expireToken := claim(t, ctx, pool, expireKey, relayintel.CapWriteAccept, "expire-complete.bench.example", "bench", 60, 1)
	if _, err := pool.Exec(ctx, `UPDATE relay_probe_leases SET expires_at = now() - interval '1 minute' WHERE lease_token = $1`, expireToken); err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	expiredOutcome, expiredErr := relayintel.BenchCommitProbe(ctx, pool, expireKey, expireToken, relayintel.BenchCompletion{
		AttemptStatus: "finished",
		Writes: []relayintel.BenchObservation{
			{Capability: relayintel.CapWriteAccept, Status: relayintel.StatusVerified, Detail: "late", Confidence: 0.8},
		},
	})
	var expiredSamples int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_capability_samples WHERE url_key = $1`, expireKey).Scan(&expiredSamples)
	out["expired_lease_completion"] = map[string]any{
		"path": "commitProbeResult", "wall_ms": ms(time.Since(started)), "outcome": expiredOutcome, "error": errorText(expiredErr),
		"sample_rows": expiredSamples, "accepted": expiredOutcome == "error" && expiredSamples == 0,
	}

	raceKey := "wss://race-complete.bench.example"
	raceToken := claim(t, ctx, pool, raceKey, relayintel.CapWriteAccept, "race-complete.bench.example", "bench", 60, 1)
	var wg sync.WaitGroup
	outcomes := make([]string, 8)
	errs := make([]string, 8)
	lats := make([]time.Duration, 8)
	raceStart := time.Now()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			one := time.Now()
			outcome, raceErr := relayintel.BenchCommitProbe(ctx, pool, raceKey, raceToken, relayintel.BenchCompletion{
				AttemptStatus: "finished",
				Writes: []relayintel.BenchObservation{
					{Capability: relayintel.CapWriteAccept, Status: relayintel.StatusVerified, Detail: "race", Confidence: 0.8},
				},
			})
			lats[n] = time.Since(one)
			outcomes[n] = outcome
			errs[n] = errorText(raceErr)
		}(i)
	}
	wg.Wait()
	committed := 0
	for _, outcome := range outcomes {
		if outcome == "committed" {
			committed++
		}
	}
	var raceSamples int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM relay_capability_samples WHERE url_key = $1 AND detail = 'race'`, raceKey).Scan(&raceSamples)
	out["concurrent_completion"] = map[string]any{
		"path": "commitProbeResult", "workers": 8, "wall_ms": ms(time.Since(raceStart)),
		"outcomes": outcomes, "errors": nonempty(errs), "latencies_ms": durationsMS(lats),
		"committed": committed, "sample_rows": raceSamples, "accepted": committed == 1 && raceSamples == 1,
	}

	var window, successes int
	_ = pool.QueryRow(ctx, `
		SELECT COALESCE((detail->>'recent_window')::int, 0), COALESCE((detail->>'recent_successes')::int, 0)
		FROM relay_capability_observations
		WHERE url_key = $1 AND capability = $2
	`, writeKey, relayintel.CapWriteAccept).Scan(&window, &successes)
	out["evidence_history"] = map[string]any{
		"url_key": writeKey, "capability": relayintel.CapWriteAccept,
		"recent_window": window, "recent_successes": successes, "aggregated": window > 0,
	}
}
