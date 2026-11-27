package relayintel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nostrmash/nostrmash/internal/relayintel"
)

func TestStatsRefreshSkipsARecentSharedSnapshot(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	if err := relayintel.RefreshStats(ctx, pool, "", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := relayintel.RefreshStats(ctx, pool, "", time.Hour); !errors.Is(err, relayintel.ErrStatsFresh) {
		t.Fatalf("second replica error = %v", err)
	}
	if err := relayintel.RecordStatsFailure(ctx, pool, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	if err := relayintel.RefreshStats(ctx, pool, "", time.Hour); err != nil {
		t.Fatalf("failed refresh was not retried: %v", err)
	}
	if err := relayintel.RefreshStats(ctx, pool, "", time.Hour); !errors.Is(err, relayintel.ErrStatsFresh) {
		t.Fatalf("restarted worker error = %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE relay_intel_snapshot SET computed_at = now() - interval '2 hours'`); err != nil {
		t.Fatal(err)
	}
	if err := relayintel.RefreshStats(ctx, pool, "", time.Hour); err != nil {
		t.Fatalf("elapsed interval did not refresh: %v", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('nostrmash-relay-intel-stats'))`).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("test could not hold the statistics lock")
	}
	busy := make(chan error, 1)
	go func() {
		busy <- relayintel.RefreshStats(ctx, pool, "", 0)
	}()
	select {
	case err := <-busy:
		if !errors.Is(err, relayintel.ErrStatsBusy) {
			t.Fatalf("overlapping refresh error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("overlapping refresh blocked instead of skipping")
	}
}
