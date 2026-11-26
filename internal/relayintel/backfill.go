package relayintel

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nostrmash/nostrmash/internal/metrics"
)

// BackfillKinds are the stored kinds the resumable projection walks.
func BackfillKinds() []int {
	return []int{
		KindLegacyContact, KindPublicRelayList, KindBlockedRelays, KindSearchRelays,
		KindFavoriteRelays, KindPrivateRelays, KindDMRelays, KindKeyPackageList,
		KindBlossomServers, KindWikiRelays, KindRelaySet,
	}
}

// BackfillBatch projects one page of stored relay events for the next
// unfinished kind, then reconciles relay heads that are missing, stale, or
// explicitly marked for replay. It does not dial relays and it does not scan
// unrelated replaceable kinds. Historical tie repair is RunCanonicalRepair.
func BackfillBatch(ctx context.Context, pool *pgxpool.Pool, batch int) error {
	if pool == nil {
		return ErrNotReady
	}
	if batch <= 0 || batch > 1000 {
		batch = 200
	}
	if err := reconcileAuthoritative(ctx, pool, batch); err != nil {
		return err
	}
	for _, kind := range BackfillKinds() {
		done, err := backfillKind(ctx, pool, kind, batch)
		if err != nil {
			return err
		}
		if !done {
			return nil
		}
	}
	return nil
}

// RepairCanonicalBatch applies one page of NIP-01 tie repair and one page of
// address-deletion backfill. Both functions return immediately once their
// cursors are idle. Projection flags do not gate this work.
func RepairCanonicalBatch(ctx context.Context, pool *pgxpool.Pool, batch int) error {
	if pool == nil {
		return ErrNotReady
	}
	if batch <= 0 || batch > 200 {
		batch = 200
	}
	var repaired int
	if err := pool.QueryRow(ctx, `SELECT repair_replaceable_tie($1)`, batch).Scan(&repaired); err != nil {
		if SchemaMissing(err) || undefinedFunction(err) {
			return nil
		}
		return wrapDB(err)
	}
	var inserted int
	if err := pool.QueryRow(ctx, `SELECT backfill_deletion_addresses($1)`, batch).Scan(&inserted); err != nil {
		if SchemaMissing(err) || undefinedFunction(err) {
			return nil
		}
		return wrapDB(err)
	}
	return nil
}

// RunCanonicalRepair keeps canonical replaceable winners and deletion
// tombstones moving even when relay projection, statistics, and probes are off.
func RunCanonicalRepair(ctx context.Context, log Logger, pool *pgxpool.Pool, batch int) {
	if pool == nil || log == nil {
		return
	}
	log.Info("relay_intel_canonical_repair_enabled", "batch", batch)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		if err := RepairCanonicalBatch(ctx, pool, batch); err != nil {
			log.Error("relay_intel_canonical_repair", "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func releaseSessionLock(conn *pgxpool.Conn, held bool, query string, args ...any) {
	if conn == nil {
		return
	}
	if held {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var released bool
		err := conn.QueryRow(unlockCtx, query, args...).Scan(&released)
		if err != nil || !released {
			_ = conn.Conn().Close(context.Background())
		}
	}
	conn.Release()
}

// ResetProjectionReconcile makes the next backfill tick walk relay heads again.
// Process start uses this so projection that was off can catch up without
// waiting out the idle interval.
func ResetProjectionReconcile(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return ErrNotReady
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO relay_intel_work_cursor (work_name, idle, next_run_at)
		VALUES ('projection_reconcile', false, now())
		ON CONFLICT (work_name) DO UPDATE
		SET idle = false,
		    next_run_at = now(),
		    updated_at = now()
	`)
	return wrapDB(err)
}

func backfillKind(ctx context.Context, pool *pgxpool.Pool, kind, batch int) (bool, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	locked := false
	defer func() {
		releaseSessionLock(conn, locked, `SELECT pg_advisory_unlock(hashtext('nostrmash-relay-intel-backfill'), $1::int)`, int32(kind))
	}()
	var held bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('nostrmash-relay-intel-backfill'), $1::int)`, int32(kind)).Scan(&held); err != nil {
		return false, wrapDB(err)
	}
	locked = held
	if !locked {
		return true, nil
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO relay_intel_backfill_cursor (source_kind)
		VALUES ($1)
		ON CONFLICT (source_kind) DO NOTHING
	`, kind); err != nil {
		return false, wrapDB(err)
	}
	var created int64
	var eventID string
	var done bool
	err = conn.QueryRow(ctx, `
		SELECT last_created_at, last_event_id, done
		FROM relay_intel_backfill_cursor
		WHERE source_kind = $1
	`, kind).Scan(&created, &eventID, &done)
	if err != nil {
		return false, wrapDB(err)
	}
	if done {
		metrics.SetRelayIntelBackfillDone(kind, 1)
		return true, nil
	}
	metrics.SetRelayIntelBackfillDone(kind, 0)
	rows, err := conn.Query(ctx, `
		SELECT e.id, e.pubkey, e.created_at, e.content, e.raw_json::text
		FROM replaceable_state rs
		JOIN events e ON e.id = rs.event_id
		WHERE rs.kind = $1
		  AND (e.created_at, e.id) > ($2, $3)
		ORDER BY e.created_at ASC, e.id ASC
		LIMIT $4
	`, kind, created, eventID, batch)
	if err != nil {
		return false, wrapDB(err)
	}
	defer rows.Close()
	type row struct {
		id, pubkey, content, raw string
		created                  int64
	}
	page := make([]row, 0, batch)
	for rows.Next() {
		var item row
		if err := rows.Scan(&item.id, &item.pubkey, &item.created, &item.content, &item.raw); err != nil {
			return false, err
		}
		page = append(page, item)
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	added := int64(0)
	for _, item := range page {
		var payload struct {
			Tags [][]string `json:"tags"`
		}
		if err := json.Unmarshal([]byte(item.raw), &payload); err != nil {
			metrics.IncRelayIntelBackfill("malformed")
		} else {
			tx, err := conn.Begin(ctx)
			if err != nil {
				return false, err
			}
			err = Project(ctx, tx, EventInput{
				Pubkey: item.pubkey, EventID: item.id, Kind: kind,
				CreatedAt: item.created, Content: item.content, Tags: payload.Tags,
			})
			if err != nil {
				_ = tx.Rollback(ctx)
				if SchemaMissing(err) {
					return false, ErrNotReady
				}
				if skippable(err) {
					metrics.IncRelayIntelBackfill("error")
				} else {
					return false, err
				}
			} else if err := tx.Commit(ctx); err != nil {
				return false, err
			} else {
				metrics.IncRelayIntelBackfill("applied")
			}
		}
		created = item.created
		eventID = item.id
		added++
	}
	finished := len(page) < batch
	if _, err := conn.Exec(ctx, `
		UPDATE relay_intel_backfill_cursor
		SET last_created_at = $2,
		    last_event_id = $3,
		    done = $4,
		    processed = processed + $5,
		    updated_at = now()
		WHERE source_kind = $1
		  AND ($2, $3) >= (last_created_at, last_event_id)
	`, kind, created, eventID, finished, added); err != nil {
		return false, wrapDB(err)
	}
	if finished {
		metrics.SetRelayIntelBackfillDone(kind, 1)
	}
	return finished, nil
}

func reconcileAuthoritative(ctx context.Context, pool *pgxpool.Pool, batch int) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	locked := false
	defer func() {
		releaseSessionLock(conn, locked, `SELECT pg_advisory_unlock(hashtext('nostrmash-relay-intel-reconcile'))`)
	}()
	var held bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('nostrmash-relay-intel-reconcile'))`).Scan(&held); err != nil {
		return wrapDB(err)
	}
	locked = held
	if !locked {
		return nil
	}
	if _, err := conn.Exec(ctx, `
		INSERT INTO relay_intel_work_cursor (work_name)
		VALUES ('projection_reconcile')
		ON CONFLICT (work_name) DO NOTHING
	`); err != nil {
		if SchemaMissing(err) {
			return nil
		}
		return wrapDB(err)
	}
	var cursorKind int
	var cursorPubkey, cursorDTag string
	var idle bool
	var nextRun time.Time
	if err := conn.QueryRow(ctx, `
		SELECT cursor_kind, cursor_pubkey, cursor_d_tag, idle, next_run_at
		FROM relay_intel_work_cursor
		WHERE work_name = 'projection_reconcile'
	`).Scan(&cursorKind, &cursorPubkey, &cursorDTag, &idle, &nextRun); err != nil {
		return wrapDB(err)
	}
	if idle && nextRun.After(time.Now()) {
		return nil
	}
	kinds := make([]int32, len(BackfillKinds()))
	for i, kind := range BackfillKinds() {
		kinds[i] = int32(kind)
	}
	rows, err := conn.Query(ctx, `
		SELECT rs.pubkey, rs.kind, rs.d_tag, rs.event_id, rs.created_at
		FROM replaceable_state rs
		LEFT JOIN relay_config_heads h
		  ON h.pubkey = rs.pubkey AND h.source_kind = rs.kind AND h.d_tag = rs.d_tag
		WHERE rs.kind = ANY($1::int[])
		  AND (rs.kind, rs.pubkey, rs.d_tag) > ($2, $3, $4)
		  AND (h.event_id IS NULL OR h.event_id <> rs.event_id OR h.needs_replay)
		  AND NOT EXISTS (
		      SELECT 1 FROM deletion_events de
		      WHERE de.target_event_id = rs.event_id AND de.deleter_pubkey = rs.pubkey
		  )
		  AND NOT EXISTS (
		      SELECT 1 FROM deletion_addresses da
		      WHERE da.pubkey = rs.pubkey
		        AND da.kind = rs.kind
		        AND da.created_at >= rs.created_at
		        AND (
		            (rs.kind >= 30000 AND rs.kind < 40000 AND da.d_tag = rs.d_tag)
		            OR rs.kind < 30000
		            OR rs.kind >= 40000
		        )
		  )
		ORDER BY rs.kind, rs.pubkey, rs.d_tag
		LIMIT $5
	`, kinds, cursorKind, cursorPubkey, cursorDTag, batch)
	if err != nil {
		if SchemaMissing(err) {
			return nil
		}
		return wrapDB(err)
	}
	defer rows.Close()
	type item struct {
		pubkey, dTag, id, content, raw string
		kind                           int
		created                        int64
	}
	page := make([]item, 0, batch)
	for rows.Next() {
		var row item
		if err := rows.Scan(&row.pubkey, &row.kind, &row.dTag, &row.id, &row.created); err != nil {
			return err
		}
		page = append(page, row)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for i := range page {
		if err := conn.QueryRow(ctx, `
			SELECT content, raw_json::text FROM events WHERE id = $1
		`, page[i].id).Scan(&page[i].content, &page[i].raw); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			return err
		}
	}
	for _, row := range page {
		if !HandlesKind(row.kind) || row.raw == "" {
			continue
		}
		var payload struct {
			Tags [][]string `json:"tags"`
		}
		if json.Unmarshal([]byte(row.raw), &payload) != nil {
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return err
		}
		err = Project(ctx, tx, EventInput{
			Pubkey: row.pubkey, EventID: row.id, Kind: row.kind, CreatedAt: row.created, Content: row.content, Tags: payload.Tags,
		})
		if err != nil {
			_ = tx.Rollback(ctx)
			if SchemaMissing(err) || skippable(err) {
				continue
			}
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	if len(page) < batch {
		_, err = conn.Exec(ctx, `
			UPDATE relay_intel_work_cursor
			SET cursor_kind = 0,
			    cursor_pubkey = '',
			    cursor_d_tag = '',
			    idle = true,
			    next_run_at = now() + interval '15 minutes',
			    last_completed_at = now(),
			    last_rows = $1,
			    updated_at = now()
			WHERE work_name = 'projection_reconcile'
		`, len(page))
	} else {
		last := page[len(page)-1]
		_, err = conn.Exec(ctx, `
			UPDATE relay_intel_work_cursor
			SET cursor_kind = $1,
			    cursor_pubkey = $2,
			    cursor_d_tag = $3,
			    idle = false,
			    last_rows = $4,
			    updated_at = now()
			WHERE work_name = 'projection_reconcile'
		`, last.kind, last.pubkey, last.dTag, len(page))
	}
	if err != nil {
		return wrapDB(err)
	}
	return nil
}

func undefinedFunction(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42883"
}

func skippable(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23503", "22P02", "22001":
			return true
		}
	}
	return errors.Is(err, pgx.ErrNoRows)
}
