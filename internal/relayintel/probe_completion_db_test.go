package relayintel

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func TestBudgetDeferralDoesNotConsumeRetries(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "probe budget deferral"), "probebudget")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	state := &probeState{worker: "worker-a", allowPrivate: true, globalLimit: 1, minInterval: time.Second, allowlist: []string{"budget.example"}}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_attempts (url_key, capability, host, worker_id, lease_token, started_at)
		VALUES ('wss://spent.example', 'retention', 'spent.example', 'worker-a', 'spent', now())
	`); err != nil {
		t.Fatal(err)
	}
	eventID := strings.Repeat("aa", 32)
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_followups (url_key, endpoint_url, event_id, capability, check_after, published_at, status, attempts)
		VALUES ('wss://budget.example', 'wss://budget.example', $1, 'retention', now() - interval '1 minute', now() - interval '1 hour', 'pending', 0)
	`, eventID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := pool.Exec(ctx, `UPDATE relay_probe_followups SET check_after = now() - interval '1 second' WHERE event_id = $1`, eventID); err != nil {
			t.Fatal(err)
		}
		ok, err := state.followOne(ctx, pool)
		if err != nil || ok {
			t.Fatalf("deferral %d ok=%v err=%v", i, ok, err)
		}
	}
	var attempts, deferrals int
	var status string
	if err := pool.QueryRow(ctx, `SELECT attempts, deferrals, status FROM relay_probe_followups WHERE event_id = $1`, eventID).Scan(&attempts, &deferrals, &status); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || deferrals != 3 || status != "pending" {
		t.Fatalf("attempts=%d deferrals=%d status=%s", attempts, deferrals, status)
	}
}

func TestActiveFollowupSurvivesCleanup(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "probe active follow-up"), "probeactive")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_leases (url_key, capability, host, worker_id, lease_token, expires_at)
		VALUES ('wss://live.example', 'retention', 'live.example', 'worker-a', 'live-token', now() + interval '2 minutes')
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_attempts (url_key, capability, host, worker_id, lease_token)
		VALUES ('wss://live.example', 'retention', 'live.example', 'worker-a', 'live-token')
	`); err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO relay_probe_followups (
			url_key, endpoint_url, event_id, capability, check_after, published_at, status, attempts, lease_token, claimed_at
		) VALUES (
			'wss://live.example', 'wss://live.example', $1, 'retention', now() - interval '1 minute', now() - interval '8 days',
			'running', 8, 'live-token', now()
		) RETURNING id
	`, strings.Repeat("bb", 32)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	state := &probeState{worker: "worker-b", allowPrivate: true, globalLimit: 20, minInterval: time.Second, allowlist: []string{"live.example"}}
	if _, err := state.followOne(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var status, token string
	var finished bool
	if err := pool.QueryRow(ctx, `SELECT status, lease_token, finished_at IS NOT NULL FROM relay_probe_followups WHERE id = $1`, id).Scan(&status, &token, &finished); err != nil {
		t.Fatal(err)
	}
	if status != "running" || token != "live-token" || finished {
		t.Fatalf("active work was terminated: status=%s token=%s finished=%v", status, token, finished)
	}
}

func TestExpiredOwnershipFinalizesThePreviousAttempt(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "probe reclaim"), "probereclaim")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_leases (url_key, capability, host, worker_id, lease_token, expires_at)
		VALUES ('wss://127.0.0.1:1', 'retention', '127.0.0.1', 'worker-old', 'old-token', now() - interval '1 minute')
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_attempts (url_key, capability, host, worker_id, lease_token, started_at)
		VALUES ('wss://127.0.0.1:1', 'retention', '127.0.0.1', 'worker-old', 'old-token', now() - interval '2 minutes')
	`); err != nil {
		t.Fatal(err)
	}
	eventID := strings.Repeat("cc", 32)
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_followups (
			url_key, endpoint_url, event_id, capability, check_after, published_at, status, attempts, lease_token, claimed_at
		) VALUES (
			'wss://127.0.0.1:1', 'wss://127.0.0.1:1', $1, 'retention', now() - interval '1 minute', now() - interval '1 hour',
			'running', 1, 'old-token', now() - interval '5 minutes'
		)
	`, eventID); err != nil {
		t.Fatal(err)
	}
	state := &probeState{
		key: newKey(t), worker: "worker-new", allowPrivate: true, globalLimit: 20, minInterval: time.Second,
		allowlist: []string{"127.0.0.1"}, pubkey: strings.Repeat("ab", 32),
	}
	ok, err := state.followOne(ctx, pool)
	if err != nil || !ok {
		t.Fatalf("reclaim ok=%v err=%v", ok, err)
	}
	var oldStatus string
	var oldFinished bool
	if err := pool.QueryRow(ctx, `SELECT status, finished_at IS NOT NULL FROM relay_probe_attempts WHERE lease_token = 'old-token'`).Scan(&oldStatus, &oldFinished); err != nil {
		t.Fatal(err)
	}
	if oldStatus != "expired" || !oldFinished {
		t.Fatalf("previous attempt status=%s finished=%v", oldStatus, oldFinished)
	}
	var followStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_probe_followups WHERE event_id = $1`, eventID).Scan(&followStatus); err != nil {
		t.Fatal(err)
	}
	if followStatus != "retryable" && followStatus != "completed" && followStatus != "failed" {
		t.Fatalf("reclaimed follow-up status=%s", followStatus)
	}
}

func TestCompletionRejectsStaleAndDuplicateResults(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "probe completion"), "probecomplete")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	token := claimToken(t, ctx, pool, "wss://complete.example", CapWriteAccept, "complete.example", "worker-a", 20, 1)
	first := probeCompletion{
		AttemptStatus: "retrieved",
		Writes:        []observationWrite{{Capability: CapWriteAccept, Status: StatusVerified, Detail: "ok", Confidence: 0.8}},
		Retention:     &scheduledRetention{RelayURL: "wss://complete.example", EventID: strings.Repeat("d1", 32), Published: time.Now().Add(-time.Hour)},
	}
	if err := commitProbeResult(ctx, pool, "wss://complete.example", token, first); err != nil {
		t.Fatal(err)
	}
	var samples, followups int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_capability_samples WHERE url_key = 'wss://complete.example'`).Scan(&samples); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_followups WHERE event_id = $1`, strings.Repeat("d1", 32)).Scan(&followups); err != nil {
		t.Fatal(err)
	}
	if samples != 1 || followups != 1 {
		t.Fatalf("samples=%d followups=%d", samples, followups)
	}
	if err := commitProbeResult(ctx, pool, "wss://complete.example", token, first); err != errProbeAlreadyCompleted {
		t.Fatalf("identical retry err=%v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_capability_samples WHERE url_key = 'wss://complete.example'`).Scan(&samples); err != nil {
		t.Fatal(err)
	}
	if samples != 1 {
		t.Fatalf("duplicate completion stored %d samples", samples)
	}
	conflict := first
	conflict.AttemptStatus = "timeout"
	if err := commitProbeResult(ctx, pool, "wss://complete.example", token, conflict); err == nil || err == errProbeAlreadyCompleted {
		t.Fatalf("conflicting retry err=%v", err)
	}
	var followupID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO relay_probe_followups (url_key, endpoint_url, event_id, capability, check_after, status, lease_token)
		VALUES ('wss://other.example', 'wss://other.example', $1, 'retention', now(), 'running', 'other-token')
		RETURNING id
	`, strings.Repeat("d2", 32)).Scan(&followupID); err != nil {
		t.Fatal(err)
	}
	other := claimToken(t, ctx, pool, "wss://complete.example", CapSearch, "complete.example", "worker-a", 20, 1)
	err := commitProbeResult(ctx, pool, "wss://complete.example", other, probeCompletion{
		AttemptStatus: "verified", FollowupID: followupID, FollowStatus: "completed", FollowResult: "wrong",
		Endpoint: "wss://other.example", FollowCapability: CapRetention,
	})
	if err == nil {
		t.Fatal("a probe token completed a different follow-up")
	}
	var owner string
	if err := pool.QueryRow(ctx, `SELECT lease_token FROM relay_probe_followups WHERE id = $1`, followupID).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != "other-token" {
		t.Fatalf("follow-up owner changed to %s", owner)
	}
}

func TestRetryAndAgeBoundsLeaveOwnedWorkAlone(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "probe bounds"), "probebounds")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_followups (url_key, endpoint_url, event_id, capability, check_after, published_at, status, attempts)
		VALUES
			('wss://retry.example', 'wss://retry.example', $1, 'retention', now() - interval '1 minute', now() - interval '1 hour', 'retryable', 8),
			('wss://old.example', 'wss://old.example', $2, 'retention', now() - interval '1 minute', now() - interval '8 days', 'pending', 1)
	`, strings.Repeat("e1", 32), strings.Repeat("e2", 32)); err != nil {
		t.Fatal(err)
	}
	state := &probeState{worker: "worker-a", allowPrivate: true, globalLimit: 20, minInterval: time.Second, allowlist: []string{"retry.example", "old.example"}}
	if err := state.cleanupFollowups(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var retryStatus, oldStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_probe_followups WHERE event_id = $1`, strings.Repeat("e1", 32)).Scan(&retryStatus); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_probe_followups WHERE event_id = $1`, strings.Repeat("e2", 32)).Scan(&oldStatus); err != nil {
		t.Fatal(err)
	}
	if retryStatus != "failed" || oldStatus != "failed" {
		t.Fatalf("retry=%s age=%s", retryStatus, oldStatus)
	}
}

func TestSameEventOnTwoRelaysStaysTwoFollowups(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "probe two relays"), "probetwo")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	state := &probeState{allowPrivate: true, allowlist: []string{"one.example", "two.example"}}
	eventID := strings.Repeat("f1", 32)
	state.scheduleRetention(ctx, pool, "wss://one.example", "wss://one.example", eventID, time.Now().Add(-2*time.Hour))
	state.scheduleRetention(ctx, pool, "wss://two.example", "wss://two.example", eventID, time.Now().Add(-2*time.Hour))
	var scheduled int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_followups WHERE event_id = $1`, eventID).Scan(&scheduled); err != nil {
		t.Fatal(err)
	}
	if scheduled != 2 {
		t.Fatalf("scheduled %d", scheduled)
	}
}

func TestSmallPoolCanSelectAndComplete(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "probe small pool")
	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "probesmall")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 1
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	small, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(small.Close)
	if _, err := small.Exec(ctx, `
		INSERT INTO relay_role_stats (
			url_key, role, endpoint_url, endpoint_type, distinct_users, trust_weighted,
			active_users, modern_users, legacy_users, computed_at
		) VALUES ('wss://127.0.0.1:9', 'public_outbox', 'wss://127.0.0.1:9', 'nostr_relay', 1, 1, 1, 1, 0, now())
	`); err != nil {
		t.Fatal(err)
	}
	state := &probeState{
		key: newKey(t), pubkey: strings.Repeat("ab", 32), worker: "worker-a", allowPrivate: true,
		globalLimit: 20, minInterval: time.Second, allowlist: []string{"127.0.0.1"},
	}
	done := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := state.probePrimary(ctx, small)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("a one-connection pool stalled while selecting a probe")
	}
	wg.Wait()
	var attempts int
	if err := small.QueryRow(ctx, `SELECT count(*) FROM relay_probe_attempts WHERE url_key = 'wss://127.0.0.1:9'`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("attempts=%d", attempts)
	}
	var status string
	if err := small.QueryRow(ctx, `SELECT status FROM relay_probe_attempts WHERE url_key = 'wss://127.0.0.1:9'`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status == "started" || status == "finished" {
		t.Fatalf("attempt status=%s", status)
	}
}

func TestReleaseOwnedAttemptDoesNotFinishAnotherLease(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "probe release"), "proberelease")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	token := claimToken(t, ctx, pool, "wss://release.example", CapWriteAccept, "release.example", "worker-a", 20, 1)
	other := claimToken(t, ctx, pool, "wss://other-release.example", CapSearch, "other-release.example", "worker-a", 20, 1)
	releaseOwnedAttempt(ctx, pool, "wss://release.example", other, "commit_failed")
	var open int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_attempts WHERE finished_at IS NULL`).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open != 2 {
		t.Fatalf("a foreign token finished %d attempts", 2-open)
	}
	releaseOwnedAttempt(ctx, pool, "wss://release.example", token, "commit_failed")
	var status string
	var leases int
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_probe_attempts WHERE lease_token = $1`, token).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_leases WHERE lease_token = $1`, token).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if status != "commit_failed" || leases != 0 {
		t.Fatalf("status=%s leases=%d", status, leases)
	}
	releaseOwnedAttempt(ctx, pool, "wss://release.example", token, "timeout")
	if err := pool.QueryRow(ctx, `SELECT status FROM relay_probe_attempts WHERE lease_token = $1`, token).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "commit_failed" {
		t.Fatalf("second release rewrote the attempt as %s", status)
	}
}

func TestConcurrentCleanupLeavesExecutingWork(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "probe concurrent cleanup"), "probeclean")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_leases (url_key, capability, host, worker_id, lease_token, expires_at)
		VALUES ('wss://running.example', 'retention', 'running.example', 'worker-a', 'running-token', now() + interval '2 minutes')
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_followups (
			url_key, endpoint_url, event_id, capability, check_after, published_at, status, attempts, lease_token, claimed_at
		) VALUES (
			'wss://running.example', 'wss://running.example', $1, 'retention', now() - interval '1 minute', now() - interval '8 days',
			'running', 8, 'running-token', now()
		)
	`, strings.Repeat("91", 32)); err != nil {
		t.Fatal(err)
	}
	state := &probeState{worker: "worker-b"}
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- state.cleanupFollowups(ctx, pool)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var status, token string
	if err := pool.QueryRow(ctx, `SELECT status, lease_token FROM relay_probe_followups WHERE event_id = $1`, strings.Repeat("91", 32)).Scan(&status, &token); err != nil {
		t.Fatal(err)
	}
	if status != "running" || token != "running-token" {
		t.Fatalf("concurrent cleanup terminated active work: status=%s token=%s", status, token)
	}
}
