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
	var processed int64
	err := pool.QueryRow(ctx, `
		SELECT last_created_at, last_event_id, done, processed
		FROM relay_intel_backfill_cursor
		WHERE source_kind = $1
	`, kind).Scan(&created, &eventID, &done, &processed)
	if err != nil {
		return false, wrapDB(err)
	}
	if done {
		metrics.SetRelayIntelBackfillDone(kind, 1)
		return true, nil
	}
	metrics.SetRelayIntelBackfillDone(kind, 0)
	rows, err := pool.Query(ctx, `
		SELECT id, pubkey, created_at, content, raw_json::text
		FROM events
		WHERE kind = $1
		  AND (created_at, id) > ($2, $3)
		ORDER BY created_at ASC, id ASC
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
		processed++
	}
	finished := len(page) < batch
	if _, err := pool.Exec(ctx, `
		UPDATE relay_intel_backfill_cursor
		SET last_created_at = $2,
		    last_event_id = $3,
		    done = $4,
		    processed = $5,
		    updated_at = now()
		WHERE source_kind = $1
	`, kind, created, eventID, finished, processed); err != nil {
		return false, wrapDB(err)
	}
	if finished {
		metrics.SetRelayIntelBackfillDone(kind, 1)
	}
	return finished, nil
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
