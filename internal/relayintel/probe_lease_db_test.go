package relayintel

import (
	"context"
	"testing"

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
	var status string
	var failures, window int
	if err := pool.QueryRow(ctx, `
		SELECT status, COALESCE((detail->>'recent_failures')::int, 0), COALESCE((detail->>'recent_window')::int, 0)
		FROM relay_capability_observations
		WHERE url_key = $1 AND capability = $2
	`, "wss://evidence.example", CapWriteAccept).Scan(&status, &failures, &window); err != nil {
		t.Fatal(err)
	}
	if status != StatusVerified || window < 2 {
		t.Fatalf("status=%s failures=%d window=%d", status, failures, window)
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
