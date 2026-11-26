package relayintel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nostrmash/nostrmash/internal/metrics"
)

// ErrStatsBusy means another worker already holds the statistics refresh lock.
var ErrStatsBusy = errors.New("relay intelligence statistics refresh is already running")

// RefreshStats rebuilds role aggregates from relay_role_memberships.
// excludePubkey drops the synthetic probe identity so probes do not become adoption.
func RefreshStats(ctx context.Context, pool *pgxpool.Pool, excludePubkey string) error {
	if pool == nil {
		return fmt.Errorf("pool is required")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('nostrmash-relay-intel-stats'))`).Scan(&locked); err != nil {
		return err
	}
	if !locked {
		return ErrStatsBusy
	}
	started := time.Now()

	if _, err := tx.Exec(ctx, `DELETE FROM relay_role_stats`); err != nil {
		return fmt.Errorf("clear relay role stats: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO relay_role_stats (
			url_key, role, endpoint_url, endpoint_type,
			distinct_users, trust_weighted, active_users,
			modern_users, legacy_users, net_user_delta_7d, computed_at
		)
		SELECT
			base.url_key,
			base.role,
			min(base.endpoint_url),
			min(base.endpoint_type),
			count(*),
			COALESCE(sum(LEAST(GREATEST(ts.score, 0), 1)), 0),
			count(*) FILTER (WHERE EXISTS (
				SELECT 1 FROM author_activity_daily activity
				WHERE activity.pubkey = base.pubkey
				  AND activity.activity_date >= CURRENT_DATE - 30
				  AND activity.post_count > 0
			)),
			count(*) FILTER (WHERE base.modern),
			count(*) FILTER (WHERE base.legacy),
			0,
			now()
		FROM (
			SELECT url_key, role, min(endpoint_url) AS endpoint_url, min(endpoint_type) AS endpoint_type, pubkey,
			       bool_or(declaration_source = 'modern') AS modern,
			       bool_or(declaration_source = 'legacy') AS legacy
			FROM relay_effective_memberships
			WHERE public_safe AND endpoint_url <> ''
			  AND ($1 = '' OR pubkey <> $1)
			GROUP BY url_key, role, pubkey
		) base
		LEFT JOIN trust_scores_global ts ON ts.pubkey = base.pubkey
		GROUP BY base.url_key, base.role
	`, excludePubkey); err != nil {
		return fmt.Errorf("insert relay role stats: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM relay_endpoint_adoption`); err != nil {
		return fmt.Errorf("clear endpoint adoption: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO relay_endpoint_adoption (url_key, endpoint_url, endpoint_type, distinct_users, computed_at)
		SELECT url_key, min(endpoint_url), min(endpoint_type), count(DISTINCT pubkey), now()
		FROM relay_effective_memberships
		WHERE public_safe AND endpoint_url <> ''
		  AND role <> $2
		  AND ($1 = '' OR pubkey <> $1)
		GROUP BY url_key
	`, excludePubkey, RoleBlocked); err != nil {
		return fmt.Errorf("insert endpoint adoption: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO relay_role_adoption_daily (day, url_key, role, distinct_users)
		SELECT CURRENT_DATE, url_key, role, distinct_users
		FROM (
			SELECT url_key, role, distinct_users,
			       row_number() OVER (PARTITION BY role ORDER BY distinct_users DESC, url_key) AS n
			FROM relay_role_stats
		) ranked
		WHERE n <= 200
		ON CONFLICT (day, url_key, role) DO UPDATE
		SET distinct_users = EXCLUDED.distinct_users
	`); err != nil {
		return fmt.Errorf("upsert adoption daily: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE relay_role_stats stats
		SET net_user_delta_7d = stats.distinct_users - COALESCE(past.distinct_users, stats.distinct_users)
		FROM (
			SELECT url_key, role, distinct_users
			FROM relay_role_adoption_daily
			WHERE day = CURRENT_DATE - 7
		) past
		WHERE past.url_key = stats.url_key AND past.role = stats.role
	`); err != nil {
		return fmt.Errorf("update adoption delta: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM relay_role_adoption_daily WHERE day < CURRENT_DATE - 30
	`); err != nil {
		return fmt.Errorf("prune adoption daily: %w", err)
	}

	kindCounts := map[string]int64{}
	rows, err := tx.Query(ctx, `
		SELECT source_kind::text, count(*)
		FROM relay_config_heads
		GROUP BY source_kind
	`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var kind string
		var count int64
		if err := rows.Scan(&kind, &count); err != nil {
			rows.Close()
			return err
		}
		kindCounts[kind] = count
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	roleCounts := map[string]int64{}
	rows, err = tx.Query(ctx, `
		SELECT role, count(DISTINCT pubkey)
		FROM relay_effective_memberships
		WHERE public_safe AND endpoint_url <> '' AND ($1 = '' OR pubkey <> $1)
		GROUP BY role
	`, excludePubkey)
	if err != nil {
		return err
	}
	for rows.Next() {
		var role string
		var count int64
		if err := rows.Scan(&role, &count); err != nil {
			rows.Close()
			return err
		}
		roleCounts[role] = count
		metrics.SetRelayIntelDeclarers(role, float64(count))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	kindJSON, _ := json.Marshal(kindCounts)
	roleJSON, _ := json.Marshal(roleCounts)
	if _, err := tx.Exec(ctx, `
		INSERT INTO relay_intel_snapshot (id, computed_at, kind_counts, role_declarers, last_duration_ms, last_error)
		VALUES (true, now(), $1::jsonb, $2::jsonb, $3, NULL)
		ON CONFLICT (id) DO UPDATE
		SET computed_at = EXCLUDED.computed_at,
		    kind_counts = EXCLUDED.kind_counts,
		    role_declarers = EXCLUDED.role_declarers,
		    last_duration_ms = EXCLUDED.last_duration_ms,
		    last_error = NULL
	`, kindJSON, roleJSON, time.Since(started).Milliseconds()); err != nil {
		return fmt.Errorf("upsert relay intel snapshot: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	metrics.SetRelayIntelStatsAge(0)
	return nil
}

// StatsAge returns how long ago the snapshot was computed.
func StatsAge(ctx context.Context, pool *pgxpool.Pool, now time.Time) (time.Duration, bool, error) {
	var computed time.Time
	err := pool.QueryRow(ctx, `SELECT computed_at FROM relay_intel_snapshot WHERE id`).Scan(&computed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, wrapDB(err)
	}
	if now.IsZero() {
		now = time.Now()
	}
	return now.Sub(computed), true, nil
}

// RecordStatsFailure stores the latest statistics error without moving the
// last successful computed_at timestamp.
func RecordStatsFailure(ctx context.Context, pool *pgxpool.Pool, cause error) error {
	if pool == nil || cause == nil {
		return nil
	}
	text := cause.Error()
	if len(text) > 500 {
		text = text[:500]
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO relay_intel_snapshot (id, computed_at, last_error, last_failure_at)
		VALUES (true, now(), $1, now())
		ON CONFLICT (id) DO UPDATE
		SET last_error = EXCLUDED.last_error,
		    last_failure_at = EXCLUDED.last_failure_at
	`, text)
	return wrapDB(err)
}
