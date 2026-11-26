package relayintel

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func TestProbeClaimsSurviveRestartAndHonorBudgets(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "relay probe leases")
	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "probelease")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	first := claimToken(t, ctx, pool, "wss://a.example", "search", "a.example", "worker-a", 20, 60)
	if first == "" {
		t.Fatal("first claim was refused")
	}
	if again := claimToken(t, ctx, pool, "wss://a.example", "search", "a.example", "worker-b", 20, 60); again != "" {
		t.Fatal("a second worker claimed a live lease")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM relay_probe_leases WHERE lease_token = $1`, "stale-token"); err != nil {
		t.Fatal(err)
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_leases WHERE lease_token = $1`, first).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatal("a stale token deleted the current lease")
	}
	for i, url := range []string{"wss://h1.example", "wss://h2.example", "wss://h3.example", "wss://h4.example"} {
		if token := claimToken(t, ctx, pool, url, "write_accept", "shared.example", "worker-a", 20, 1); token == "" {
			t.Fatalf("host claim %d was refused", i)
		}
	}
	if token := claimToken(t, ctx, pool, "wss://h5.example", "write_accept", "shared.example", "worker-a", 20, 1); token != "" {
		t.Fatal("the per-host budget was exceeded")
	}
}

func TestStaleLeaseCannotPublishEvidence(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay probe evidence"), "probeevidence")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	token := claimToken(t, ctx, pool, "wss://evidence.example", "write_accept", "evidence.example", "worker-a", 20, 60)
	if err := storeObservation(ctx, pool, "wss://evidence.example", CapWriteAccept, StatusVerified, 0.8, "probe", "ok", "stale-token"); err == nil {
		t.Fatal("a stale token published evidence")
	}
	if err := storeObservation(ctx, pool, "wss://other.example", CapWriteAccept, StatusVerified, 0.8, "probe", "ok", token); err == nil {
		t.Fatal("a token for one endpoint published another")
	}
	if err := storeObservation(ctx, pool, "wss://evidence.example", CapWriteAccept, StatusVerified, 0.8, "probe", "ok", token); err != nil {
		t.Fatal(err)
	}
	if err := storeObservation(ctx, pool, "wss://evidence.example", CapWriteAccept, StatusVerified, 0.8, "probe", "ok", token); err != nil {
		t.Fatal(err)
	}
	var samples int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM relay_capability_samples
		WHERE url_key = $1 AND capability = $2 AND status = $3 AND detail = 'ok'
	`, "wss://evidence.example", CapWriteAccept, StatusVerified).Scan(&samples); err != nil {
		t.Fatal(err)
	}
	if samples != 1 {
		t.Fatalf("duplicate completion stored %d samples", samples)
	}
	if err := storeObservation(ctx, pool, "wss://evidence.example", CapWriteAccept, "inconclusive", 0.2, "probe", "timeout", token); err != nil {
		t.Fatal(err)
	}
	var status, text string
	var confidence float64
	var failures, window int
	if err := pool.QueryRow(ctx, `
		SELECT status, confidence, COALESCE(detail->>'text', ''),
		       COALESCE((detail->>'recent_failures')::int, 0), COALESCE((detail->>'recent_window')::int, 0)
		FROM relay_capability_observations
		WHERE url_key = $1 AND capability = $2
	`, "wss://evidence.example", CapWriteAccept).Scan(&status, &confidence, &text, &failures, &window); err != nil {
		t.Fatal(err)
	}
	if status != StatusVerified || confidence < 0.79 || text != "ok" || window < 2 {
		t.Fatalf("status=%s confidence=%v text=%s failures=%d window=%d", status, confidence, text, failures, window)
	}
}

func TestStaleWorkerLosesTheLeaseDuringCommit(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay probe fence"), "probefence")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	token := claimToken(t, ctx, pool, "wss://fence.example", "write_accept", "fence.example", "worker-a", 20, 60)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var locked string
	if err := tx.QueryRow(ctx, `
		SELECT lease_token FROM relay_probe_leases WHERE lease_token = $1 FOR UPDATE
	`, token).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- storeObservation(ctx, pool, "wss://fence.example", CapWriteAccept, StatusVerified, 0.8, "probe", "late", token)
	}()
	time.Sleep(200 * time.Millisecond)
	if _, err := tx.Exec(ctx, `UPDATE relay_probe_leases SET expires_at = now() - interval '1 minute' WHERE lease_token = $1`, token); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("an expired lease published evidence")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("evidence write did not finish after the lease expired")
	}
	var samples int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_capability_samples WHERE detail = 'late'`).Scan(&samples); err != nil {
		t.Fatal(err)
	}
	if samples != 0 {
		t.Fatal("rolled-back evidence remained")
	}
}

func claimToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, url, capability, host, worker string, global, minSeconds int) string {
	t.Helper()
	var token string
	if err := pool.QueryRow(ctx, `SELECT claim_relay_probe($1, $2, $3, $4, $5, $6, 120)`, url, capability, host, worker, global, minSeconds).Scan(&token); err != nil {
		t.Fatal(err)
	}
	return token
}
