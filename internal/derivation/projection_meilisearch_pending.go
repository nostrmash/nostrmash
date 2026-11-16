package derivation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/nostrmash/nostrmash/internal/nostr"
)

// reinsertClaimedMeilisearchSyncsTimeout bounds the best-effort
// re-queue of a claimed batch when SyncEventsBatch failed because the
// caller's context was canceled or expired. It uses
// context.Background() so the re-queue still completes during graceful
// shutdown or after a per-batch deadline trip; the timeout protects
// against an unrelated DB stall wedging the sweeper goroutine.
const reinsertClaimedMeilisearchSyncsTimeout = 5 * time.Second

// MarkMeilisearchDirty replaces the in-bundle SyncMeilisearch call with a
// cheap upsert into pending_meilisearch_syncs for events that need search
// indexing.
//
// The actual HTTP sync (which can take up to ~30s when Meilisearch is
// slow, single-handedly capping live-pool throughput at
// live_concurrency * 2/min) runs out-of-band in
// DrainPendingMeilisearchSyncBatch, scheduled by the worker's
// meilisearch sweeper loop. Marking covers kind-0 profiles and every
// kind in the notes index. Duplicates collapse via the PRIMARY KEY,
// and the sweeper drains them in batches so search index lag stays
// small even during heavy ingest.
//
// Skips events whose source row no longer exists (e.g., deleted between
// enqueue and dispatch) and events that are not indexed; the bundle
// should not dead-letter on this.
func (h *Handlers) MarkMeilisearchDirty(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	if h.meili == nil || !h.meili.Enabled() {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var kind int
	if err := h.pool.QueryRow(ctx, `
		SELECT kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load event for meilisearch dirty marking: %w", err)
	}
	if kind != 0 && !nostr.IsIndexedNoteKind(kind) {
		return nil
	}

	// Bump marked_at on conflict so a re-mark during an in-flight FullSync
	// survives the post-sync prune (which only deletes marked_at <= sync start).
	if _, err := h.pool.Exec(ctx, `
		INSERT INTO pending_meilisearch_syncs (event_id)
		VALUES ($1)
		ON CONFLICT (event_id) DO UPDATE
		SET marked_at = now()
	`, eventID); err != nil {
		return fmt.Errorf("mark meilisearch dirty: %w", err)
	}
	return nil
}

// meiliClaimTarget selects which pending rows a drain claims. A mixed
// batch makes Meilisearch commit every index: the notes commit costs the
// same ~30s for a few dozen documents as it would for a full batch, and
// running it beside every profile batch left the profiles index idle for
// that whole commit. Profiles are almost the entire backlog, so they drain
// on their own. Notes drain only once a full batch is waiting, or the
// oldest note has waited out the sweeper interval.
type meiliClaimTarget int

const (
	meiliClaimOldest meiliClaimTarget = iota
	meiliClaimProfiles
	meiliClaimNotes
)

// meiliClaim is one homogeneous batch already removed from
// pending_meilisearch_syncs. SyncIDs is what Meilisearch should upsert
// (one event per profile pubkey; every note). Picked is how many rows the
// LIMIT locked before profile duplicates were collapsed, and is the signal
// that more of this kind is still waiting.
type meiliClaim struct {
	syncIDs []string
	picked  int
}

// DrainPendingMeilisearchSyncBatch claims one kind — whichever pending row
// is oldest — and syncs it. Profile rows for the same author collapse to a
// single upsert.
func (h *Handlers) DrainPendingMeilisearchSyncBatch(ctx context.Context, limit int) (int, error) {
	synced, _, err := h.drainPendingMeilisearchSyncBatch(ctx, limit, meiliClaimOldest)
	return synced, err
}

// DrainPendingProfileMeilisearchSyncBatch drains kind-0 rows only. Picked
// counts rows locked before same-author duplicates were collapsed, so a
// full claim still reports picked == limit when many authors repeat.
func (h *Handlers) DrainPendingProfileMeilisearchSyncBatch(ctx context.Context, limit int) (synced, picked int, err error) {
	return h.drainPendingMeilisearchSyncBatch(ctx, limit, meiliClaimProfiles)
}

// DrainPendingNoteMeilisearchSyncBatch drains indexed note kinds only.
func (h *Handlers) DrainPendingNoteMeilisearchSyncBatch(ctx context.Context, limit int) (synced, picked int, err error) {
	return h.drainPendingMeilisearchSyncBatch(ctx, limit, meiliClaimNotes)
}

// IndexedNoteSyncDue reports whether a note commit is worth its fixed cost.
// A full batch always is. A short batch is, once the oldest note has waited
// maxWait, so a trickle still lands.
func (h *Handlers) IndexedNoteSyncDue(ctx context.Context, batchSize int, maxWait time.Duration) (bool, error) {
	if h == nil || h.pool == nil {
		return false, fmt.Errorf("handlers are not initialized")
	}
	var pending int64
	var oldestSeconds float64
	if err := h.pool.QueryRow(ctx, `
		SELECT COUNT(*),
		       COALESCE(EXTRACT(EPOCH FROM (now() - MIN(p.marked_at))), 0)
		FROM pending_meilisearch_syncs p
		JOIN events e ON e.id = p.event_id
		WHERE e.kind IN (`+nostr.IndexedNoteKindsSQL+`)
	`).Scan(&pending, &oldestSeconds); err != nil {
		return false, fmt.Errorf("read pending note sync stats: %w", err)
	}
	if oldestSeconds < 0 {
		oldestSeconds = 0
	}
	return NoteSyncDue(int(pending), time.Duration(oldestSeconds*float64(time.Second)), batchSize, maxWait), nil
}

// NoteSyncDue is the decision IndexedNoteSyncDue applies to a note-queue
// snapshot. A full batch is always due. A shorter one waits out maxWait so
// the notes commit is not spent on a handful of fresh documents.
func NoteSyncDue(pending int, oldest time.Duration, batchSize int, maxWait time.Duration) bool {
	if pending <= 0 || batchSize <= 0 {
		return false
	}
	if pending >= batchSize {
		return true
	}
	return maxWait > 0 && oldest >= maxWait
}

func (h *Handlers) drainPendingMeilisearchSyncBatch(ctx context.Context, limit int, target meiliClaimTarget) (int, int, error) {
	if h == nil || h.pool == nil {
		return 0, 0, fmt.Errorf("handlers are not initialized")
	}
	if h.meili == nil || !h.meili.Enabled() {
		return 0, 0, nil
	}
	if limit <= 0 {
		return 0, 0, nil
	}

	claimed, err := h.claimPendingMeilisearchSyncs(ctx, limit, target)
	if err != nil {
		return 0, 0, err
	}
	eventIDs := claimed.syncIDs
	if len(eventIDs) == 0 {
		return 0, claimed.picked, nil
	}

	// The claim is one kind, so SyncEventsBatch opens the notes index or
	// the profiles index, plus search documents for that kind, and does
	// not commit the other one.
	batchErr := h.meili.SyncEventsBatch(ctx, h.pool, eventIDs)
	if batchErr == nil {
		return len(eventIDs), claimed.picked, nil
	}

	// Distinguish "Meili saturated / context expired" from "data
	// poisoning". The per-event fallback below is designed to isolate
	// a single bad document so it does not poison an otherwise healthy
	// batch — but when Meilisearch itself is overloaded (its
	// per-index task queue is the bottleneck) every per-event call
	// will hit the same saturation and time out. With concurrency=N
	// goroutines that turns into a hard livelock: each cycle deletes
	// 500 rows during claim, then per-event fails for ~all of them and
	// re-inserts ~all of them, so net drain ≈ 0 while production keeps
	// adding work.
	//
	// On context.DeadlineExceeded / context.Canceled, re-insert the
	// sync ids and let the next cycle retry them as a batch when Meili
	// recovers. Same-author profile duplicates stay deleted: one pending
	// row per pubkey is enough, because the sync loads the latest
	// profile. This is correctness-safe because (a) MarkMeilisearchDirty
	// is idempotent on event_id and (b) Meilisearch upserts are
	// idempotent, so a duplicate sync from a race is wasteful but not
	// corrupting.
	if errors.Is(batchErr, context.DeadlineExceeded) || errors.Is(batchErr, context.Canceled) {
		if reErr := h.reinsertClaimedMeilisearchSyncs(eventIDs); reErr != nil {
			slog.Error(
				"meilisearch_sweeper_reinsert_failed",
				"reason", "context_expired",
				"claimed", len(eventIDs),
				"reinsert_error", reErr,
				"batch_error", batchErr,
			)
			return 0, 0, fmt.Errorf("reinsert claimed events after meilisearch timeout (%w): %w", batchErr, reErr)
		}
		slog.Warn(
			"meilisearch_batch_timeout_reinsert",
			"claimed", len(eventIDs),
			"batch_error", batchErr,
		)
		return 0, 0, fmt.Errorf("meilisearch batch sync timed out, re-queued %d events: %w", len(eventIDs), batchErr)
	}

	// Non-timeout error: fall back to per-event sync so good events
	// still drain and bad events get isolated (re-marked individually
	// for the next cycle's retry rather than poisoning the entire
	// batch).
	processed := 0
	var firstErr error
	for _, eventID := range eventIDs {
		if err := h.meili.SyncEvent(ctx, h.pool, eventID); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("meilisearch sync for %s: %w", eventID, err)
			}
			if _, reinsertErr := h.pool.Exec(ctx, `
				INSERT INTO pending_meilisearch_syncs (event_id)
				VALUES ($1)
				ON CONFLICT (event_id) DO NOTHING
			`, eventID); reinsertErr != nil && firstErr == nil {
				firstErr = fmt.Errorf("re-mark failed event %s: %w", eventID, reinsertErr)
			}
			continue
		}
		processed++
	}
	if firstErr == nil && processed > 0 {
		// Whole batch recovered via per-event fallback; surface the
		// original batch error so logs/metrics still record that
		// the fast path failed.
		return processed, claimed.picked, fmt.Errorf("meilisearch batch sync fell back to per-event: %w", batchErr)
	}
	return processed, claimed.picked, firstErr
}

// reinsertClaimedMeilisearchSyncs re-queues a previously-claimed
// (DELETEd) batch using a fresh background context so it succeeds even
// when the caller's context is canceled (graceful shutdown) or already
// past its deadline (per-batch timeout). Idempotent via the PRIMARY KEY
// on event_id.
func (h *Handlers) reinsertClaimedMeilisearchSyncs(eventIDs []string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	if len(eventIDs) == 0 {
		return nil
	}
	bgCtx, cancel := context.WithTimeout(context.Background(), reinsertClaimedMeilisearchSyncsTimeout)
	defer cancel()
	if _, err := h.pool.Exec(bgCtx, `
		INSERT INTO pending_meilisearch_syncs (event_id)
		SELECT unnest($1::text[])
		ON CONFLICT (event_id) DO NOTHING
	`, eventIDs); err != nil {
		return fmt.Errorf("reinsert claimed meilisearch syncs: %w", err)
	}
	return nil
}

// claimPendingMeilisearchSyncs atomically removes one kind of pending row.
// A crash between the DELETE-commit and the sync commit drops that sync;
// the next event from the same author re-marks the row, and FullSync can
// reconcile, so eventual consistency is preserved.
//
// Kind 0 keeps a single sync id per pubkey and deletes every other pending
// metadata event for that author. Those duplicates would each reindex the
// same latest profile.
func (h *Handlers) claimPendingMeilisearchSyncs(ctx context.Context, limit int, target meiliClaimTarget) (meiliClaim, error) {
	if target == meiliClaimOldest {
		kind, ok, err := h.peekOldestPendingMeiliKind(ctx)
		if err != nil || !ok {
			return meiliClaim{}, err
		}
		if kind == 0 {
			target = meiliClaimProfiles
		} else if nostr.IsIndexedNoteKind(kind) {
			target = meiliClaimNotes
		} else {
			return meiliClaim{}, nil
		}
	}
	switch target {
	case meiliClaimProfiles:
		return h.claimPendingProfileSyncs(ctx, limit)
	case meiliClaimNotes:
		return h.claimPendingNoteSyncs(ctx, limit)
	default:
		return meiliClaim{}, fmt.Errorf("unknown meilisearch claim target %d", target)
	}
}

func (h *Handlers) peekOldestPendingMeiliKind(ctx context.Context) (int, bool, error) {
	var kind int
	err := h.pool.QueryRow(ctx, `
		SELECT e.kind
		FROM pending_meilisearch_syncs p
		JOIN events e ON e.id = p.event_id
		ORDER BY p.marked_at ASC, p.event_id ASC
		LIMIT 1
	`).Scan(&kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("peek oldest pending meilisearch kind: %w", err)
	}
	return kind, true, nil
}

func (h *Handlers) claimPendingNoteSyncs(ctx context.Context, limit int) (meiliClaim, error) {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return meiliClaim{}, fmt.Errorf("begin pending note sync claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		WITH claimed AS (
			SELECT p.event_id
			FROM pending_meilisearch_syncs p
			JOIN events e ON e.id = p.event_id
			WHERE e.kind IN (`+nostr.IndexedNoteKindsSQL+`)
			ORDER BY p.marked_at ASC, p.event_id ASC
			FOR UPDATE OF p SKIP LOCKED
			LIMIT $1
		)
		DELETE FROM pending_meilisearch_syncs p
		USING claimed c
		WHERE p.event_id = c.event_id
		RETURNING p.event_id
	`, limit)
	if err != nil {
		return meiliClaim{}, fmt.Errorf("claim pending note syncs: %w", err)
	}
	defer rows.Close()

	eventIDs := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return meiliClaim{}, fmt.Errorf("scan claimed note id: %w", err)
		}
		eventIDs = append(eventIDs, id)
	}
	if err := rows.Err(); err != nil {
		return meiliClaim{}, fmt.Errorf("read claimed note ids: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return meiliClaim{}, fmt.Errorf("commit pending note sync claim: %w", err)
	}
	return meiliClaim{syncIDs: eventIDs, picked: len(eventIDs)}, nil
}

func (h *Handlers) claimPendingProfileSyncs(ctx context.Context, limit int) (meiliClaim, error) {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return meiliClaim{}, fmt.Errorf("begin pending profile sync claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT p.event_id, e.pubkey
		FROM pending_meilisearch_syncs p
		JOIN events e ON e.id = p.event_id
		WHERE e.kind = 0
		ORDER BY p.marked_at ASC, p.event_id ASC
		FOR UPDATE OF p SKIP LOCKED
		LIMIT $1
	`, limit)
	if err != nil {
		return meiliClaim{}, fmt.Errorf("pick pending profile syncs: %w", err)
	}

	syncIDs := make([]string, 0, limit)
	pubkeys := make([]string, 0, limit)
	seen := make(map[string]struct{}, limit)
	picked := 0
	for rows.Next() {
		var eventID, pubkey string
		if err := rows.Scan(&eventID, &pubkey); err != nil {
			rows.Close()
			return meiliClaim{}, fmt.Errorf("scan pending profile sync: %w", err)
		}
		picked++
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		syncIDs = append(syncIDs, eventID)
		pubkeys = append(pubkeys, pubkey)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return meiliClaim{}, fmt.Errorf("read pending profile syncs: %w", err)
	}
	rows.Close()
	if picked == 0 {
		if err := tx.Commit(ctx); err != nil {
			return meiliClaim{}, fmt.Errorf("commit empty profile sync claim: %w", err)
		}
		return meiliClaim{}, nil
	}

	// Drop every pending metadata event for the authors we just picked,
	// including rows newer than the LIMIT window. The sync publishes the
	// latest profile, so the extras would repeat that work.
	if _, err := tx.Exec(ctx, `
		DELETE FROM pending_meilisearch_syncs p
		WHERE p.event_id IN (
			SELECT p2.event_id
			FROM pending_meilisearch_syncs p2
			JOIN events e ON e.id = p2.event_id
			WHERE e.kind = 0
			  AND e.pubkey = ANY($1::text[])
			FOR UPDATE OF p2 SKIP LOCKED
		)
	`, pubkeys); err != nil {
		return meiliClaim{}, fmt.Errorf("collapse pending profile syncs: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return meiliClaim{}, fmt.Errorf("commit pending profile sync claim: %w", err)
	}
	return meiliClaim{syncIDs: syncIDs, picked: picked}, nil
}

// PendingMeilisearchSyncBacklog returns the current depth of the dirty
// queue. Exposed for metrics / admin observability.
func (h *Handlers) PendingMeilisearchSyncBacklog(ctx context.Context) (int64, error) {
	if h == nil || h.pool == nil {
		return 0, fmt.Errorf("handlers are not initialized")
	}
	var n int64
	if err := h.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM pending_meilisearch_syncs
	`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count pending meilisearch syncs: %w", err)
	}
	return n, nil
}

// PendingMeilisearchSyncStats returns both the queue depth and the age of the
// oldest pending entry. The oldest-age signal is the search-index-lag SLO:
// unlike a raw backlog count it stays flat under steady high throughput and
// only grows when the sweeper cannot keep up. Returns a zero age when the
// queue is empty.
func (h *Handlers) PendingMeilisearchSyncStats(ctx context.Context) (backlog int64, oldestAge time.Duration, err error) {
	if h == nil || h.pool == nil {
		return 0, 0, fmt.Errorf("handlers are not initialized")
	}
	var oldestAgeSeconds float64
	if err := h.pool.QueryRow(ctx, `
		SELECT
			COUNT(*),
			COALESCE(EXTRACT(EPOCH FROM (now() - MIN(marked_at))), 0)
		FROM pending_meilisearch_syncs
	`).Scan(&backlog, &oldestAgeSeconds); err != nil {
		return 0, 0, fmt.Errorf("read pending meilisearch sync stats: %w", err)
	}
	if oldestAgeSeconds < 0 {
		oldestAgeSeconds = 0
	}
	return backlog, time.Duration(oldestAgeSeconds * float64(time.Second)), nil
}
