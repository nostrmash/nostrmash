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

func claimToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, url, capability, host, worker string, global, minSeconds int) string {
	t.Helper()
	var token string
	if err := pool.QueryRow(ctx, `SELECT claim_relay_probe($1, $2, $3, $4, $5, $6, 120)`, url, capability, host, worker, global, minSeconds).Scan(&token); err != nil {
		t.Fatal(err)
	}
	return token
}
