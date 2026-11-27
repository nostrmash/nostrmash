package relayintel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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

func TestStatsRefreshScopesSettingsToItsTransaction(t *testing.T) {
	ctx := context.Background()
	pool := repairPool(t, ctx)
	if _, err := pool.Exec(ctx, `
		CREATE FUNCTION assert_relay_stats_settings() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF (SELECT setting FROM pg_settings WHERE name = 'statement_timeout') <> '120000' THEN
				RAISE EXCEPTION 'unexpected statement timeout for relay statistics';
			END IF;
			IF current_setting('enable_nestloop') <> 'off' THEN
				RAISE EXCEPTION 'unexpected nested-loop setting for relay statistics';
			END IF;
			RETURN NULL;
		END;
		$$;
		CREATE TRIGGER assert_relay_stats_settings BEFORE INSERT ON relay_role_stats
		FOR EACH STATEMENT EXECUTE FUNCTION assert_relay_stats_settings();
	`); err != nil {
		t.Fatal(err)
	}
	cfg := pool.Config()
	cfg.MaxConns = 1
	cfg.MinConns = 0
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "15s"
	cfg.ConnConfig.RuntimeParams["enable_nestloop"] = "on"
	refreshPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer refreshPool.Close()
	if err := relayintel.RefreshStats(ctx, refreshPool, "", 0); err != nil {
		t.Fatalf("refresh with production session settings: %v", err)
	}
	var sessionTimeout, sessionNestloop string
	if err := refreshPool.QueryRow(ctx, `
		SELECT (SELECT setting FROM pg_settings WHERE name = 'statement_timeout'),
		       current_setting('enable_nestloop')
	`).Scan(&sessionTimeout, &sessionNestloop); err != nil {
		t.Fatal(err)
	}
	if sessionTimeout != "15000" {
		t.Fatalf("timeout after refresh = %q, want 15000", sessionTimeout)
	}
	if sessionNestloop != "on" {
		t.Fatalf("nested-loop setting after refresh = %q, want on", sessionNestloop)
	}
}
