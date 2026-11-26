package relayintel

import (
	"context"
	"encoding/json"
	"errors"

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

// BackfillBatch projects one page of stored events for the next unfinished kind.
// It does not dial relays. A missing table returns ErrNotReady without moving the cursor.
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

func backfillKind(ctx context.Context, pool *pgxpool.Pool, kind, batch int) (bool, error) {
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	var locked bool
	if err := lockTx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtext('nostrmash-relay-intel-backfill'), $1)`, kind).Scan(&locked); err != nil {
		return false, wrapDB(err)
	}
	if !locked {
		return true, nil
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_intel_backfill_cursor (source_kind)
		VALUES ($1)
		ON CONFLICT (source_kind) DO NOTHING
	`, kind); err != nil {
		return false, wrapDB(err)
	}
	var created int64
	var eventID string
	var done bool
	err = pool.QueryRow(ctx, `
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
	rows, err := pool.Query(ctx, `
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
			tx, err := pool.Begin(ctx)
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
	if _, err := pool.Exec(ctx, `
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
	if _, err := pool.Exec(ctx, `SELECT repair_replaceable_tie($1)`, batch); err != nil {
		if SchemaMissing(err) || undefinedFunction(err) {
			return nil
		}
		return wrapDB(err)
	}
	rows, err := pool.Query(ctx, `
		SELECT e.id, e.pubkey, e.kind, e.created_at, e.content, e.raw_json::text
		FROM replaceable_state rs
		JOIN events e ON e.id = rs.event_id
		LEFT JOIN relay_config_heads h
		  ON h.pubkey = rs.pubkey AND h.source_kind = rs.kind AND h.d_tag = rs.d_tag
		WHERE (h.event_id IS NULL OR h.event_id <> rs.event_id OR h.truncated)
		ORDER BY e.created_at ASC, e.id ASC
		LIMIT $1
	`, batch)
	if err != nil {
		if SchemaMissing(err) {
			return nil
		}
		return wrapDB(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, pubkey, content, raw string
		var kind int
		var created int64
		if err := rows.Scan(&id, &pubkey, &kind, &created, &content, &raw); err != nil {
			return err
		}
		if !HandlesKind(kind) {
			continue
		}
		var payload struct {
			Tags [][]string `json:"tags"`
		}
		if json.Unmarshal([]byte(raw), &payload) != nil {
			continue
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		err = Project(ctx, tx, EventInput{
			Pubkey: pubkey, EventID: id, Kind: kind, CreatedAt: created, Content: content, Tags: payload.Tags,
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
	return rows.Err()
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
