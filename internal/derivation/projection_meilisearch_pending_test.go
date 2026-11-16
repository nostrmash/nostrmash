package derivation_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nostrmash/nostrmash/internal/derivation"
	"github.com/nostrmash/nostrmash/internal/nostr"
	"github.com/nostrmash/nostrmash/internal/testutil/derivationbootstrap"
)

// fakeMeiliSyncer is a minimal MeilisearchSyncer used by sweeper tests
// to drive specific failure modes (timeout, data error, success).
type fakeMeiliSyncer struct {
	mu sync.Mutex

	enabled bool

	batchErr      error
	batchCalls    int
	batchEventIDs [][]string

	perEventErr   func(eventID string) error
	perEventCalls []string
}

func (f *fakeMeiliSyncer) Enabled() bool { return f.enabled }

func (f *fakeMeiliSyncer) SyncEvent(_ context.Context, _ *pgxpool.Pool, eventID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.perEventCalls = append(f.perEventCalls, eventID)
	if f.perEventErr != nil {
		return f.perEventErr(eventID)
	}
	return nil
}

func (f *fakeMeiliSyncer) SyncEventsBatch(_ context.Context, _ *pgxpool.Pool, eventIDs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchCalls++
	cp := make([]string, len(eventIDs))
	copy(cp, eventIDs)
	f.batchEventIDs = append(f.batchEventIDs, cp)
	return f.batchErr
}

func setupMeiliSweeperTest(t *testing.T) (*pgxpool.Pool, *fakeMeiliSyncer, *derivation.Handlers, context.Context) {
	t.Helper()
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-meili-sweeper")

	meili := &fakeMeiliSyncer{enabled: true}
	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		MeiliClient: meili,
	})
	return pool, meili, handlers, ctx
}

const meiliSweeperTestPubkey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func seedPendingMeilisearchSyncs(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventIDs []string) {
	t.Helper()
	seedPendingMeilisearchSyncsKind(t, ctx, pool, eventIDs, 1, meiliSweeperTestPubkey, 0)
}

func seedPendingMeilisearchSyncsKind(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventIDs []string, kind int, pubkey string, age time.Duration) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		SELECT id, $2, $3, $4, 'sig', '', '{}'::jsonb
		FROM unnest($1::text[]) AS id
		ON CONFLICT (id) DO NOTHING
	`, eventIDs, pubkey, time.Now().Unix(), kind); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO pending_meilisearch_syncs (event_id, marked_at, kind)
		SELECT id, now() - make_interval(secs => $2), $3
		FROM unnest($1::text[]) AS id
		ON CONFLICT (event_id) DO UPDATE
		SET marked_at = EXCLUDED.marked_at,
		    kind = EXCLUDED.kind
	`, eventIDs, age.Seconds(), kind); err != nil {
		t.Fatalf("seed pending_meilisearch_syncs: %v", err)
	}
}

func countPendingMeilisearchSyncs(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM pending_meilisearch_syncs`).Scan(&n); err != nil {
		t.Fatalf("count pending_meilisearch_syncs: %v", err)
	}
	return n
}

// TestDrainPendingMeilisearchSyncBatch_TimeoutReinsertsBatch verifies
// the regression fix for the per-event fallback livelock: when
// SyncEventsBatch fails with context.DeadlineExceeded, the sweeper
// must re-queue the entire claimed batch in a single statement and
// MUST NOT call SyncEvent per-event (because every per-event call
// would hit the same saturated Meilisearch and itself time out,
// burning ~30s × batch_size of goroutine time per cycle while
// re-inserting the same events anyway → backlog grows at exactly the
// production rate).
func TestDrainPendingMeilisearchSyncBatch_TimeoutReinsertsBatch(t *testing.T) {
	pool, meili, handlers, ctx := setupMeiliSweeperTest(t)
	meili.batchErr = context.DeadlineExceeded

	eventIDs := []string{"evt_a", "evt_b", "evt_c", "evt_d", "evt_e"}
	seedPendingMeilisearchSyncs(t, ctx, pool, eventIDs)
	if got, want := countPendingMeilisearchSyncs(t, ctx, pool), len(eventIDs); got != want {
		t.Fatalf("seed sanity: got %d pending rows want %d", got, want)
	}

	processed, err := handlers.DrainPendingMeilisearchSyncBatch(ctx, len(eventIDs))
	if processed != 0 {
		t.Fatalf("expected processed=0 on timeout, got %d", processed)
	}
	if err == nil {
		t.Fatalf("expected error from DrainPendingMeilisearchSyncBatch on timeout, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected wrapped context.DeadlineExceeded, got %v", err)
	}

	if meili.batchCalls != 1 {
		t.Fatalf("expected exactly 1 SyncEventsBatch call, got %d", meili.batchCalls)
	}
	if len(meili.perEventCalls) != 0 {
		t.Fatalf("expected 0 SyncEvent (per-event) calls on timeout, got %d: %v",
			len(meili.perEventCalls), meili.perEventCalls)
	}

	if got, want := countPendingMeilisearchSyncs(t, ctx, pool), len(eventIDs); got != want {
		t.Fatalf("expected all %d events re-queued after timeout, got %d", want, got)
	}
}

// TestDrainPendingMeilisearchSyncBatch_TimeoutSurvivesParentCancel
// asserts that re-insertion succeeds even when the caller's context is
// already canceled (the worker shutdown / per-batch-deadline path).
// The helper uses context.Background() with its own short timeout
// precisely to avoid losing claimed events on cancellation.
func TestDrainPendingMeilisearchSyncBatch_TimeoutSurvivesParentCancel(t *testing.T) {
	pool, meili, handlers, _ := setupMeiliSweeperTest(t)
	meili.batchErr = context.DeadlineExceeded

	eventIDs := []string{"evt_x", "evt_y", "evt_z"}
	seedPendingMeilisearchSyncs(t, context.Background(), pool, eventIDs)

	// Caller's context is already canceled; the claim must still
	// succeed (it ran before the cancel in real life — we simulate
	// the worst case: cancel happens between claim and reinsert by
	// cancelling immediately).
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	processed, err := handlers.DrainPendingMeilisearchSyncBatch(cancelCtx, len(eventIDs))
	if processed != 0 {
		t.Fatalf("expected processed=0 on canceled ctx, got %d", processed)
	}
	if err == nil {
		t.Fatalf("expected error on canceled ctx, got nil")
	}
	// Either the claim phase fails (events stay in the table, fine) or
	// the batch fails after a successful claim and we must re-insert
	// all of them. In both cases the queue must end up at len(eventIDs).
	if got, want := countPendingMeilisearchSyncs(t, context.Background(), pool), len(eventIDs); got != want {
		t.Fatalf("expected %d events still pending after cancel-during-drain, got %d", want, got)
	}
	if len(meili.perEventCalls) != 0 {
		t.Fatalf("per-event fallback must not run on context cancel, got %d calls", len(meili.perEventCalls))
	}
}

// TestDrainPendingMeilisearchSyncBatch_DataErrorFallsBackPerEvent
// verifies the original poisoned-document isolation behavior is
// preserved for non-timeout errors: when SyncEventsBatch fails with a
// generic data error, the sweeper falls back to per-event SyncEvent so
// good events still drain and a single bad event is isolated for
// individual retry.
func TestDrainPendingMeilisearchSyncBatch_DataErrorFallsBackPerEvent(t *testing.T) {
	pool, meili, handlers, ctx := setupMeiliSweeperTest(t)
	meili.batchErr = errors.New("malformed document at position 2: invalid utf8")
	badEvent := "evt_bad"
	meili.perEventErr = func(eventID string) error {
		if eventID == badEvent {
			return errors.New("per-event poison")
		}
		return nil
	}

	eventIDs := []string{"evt_ok1", "evt_ok2", badEvent, "evt_ok3"}
	seedPendingMeilisearchSyncs(t, ctx, pool, eventIDs)

	processed, err := handlers.DrainPendingMeilisearchSyncBatch(ctx, len(eventIDs))
	if processed != 0 && processed != 3 {
		// SyncEvent only counts the events for which it was called and returned nil.
		// We expect 3 OK + 1 failure = processed=3, firstErr=nil from the fallback;
		// the wrapper returns "fell back to per-event" which still yields a non-nil err
		// and processed=3.
		t.Fatalf("expected processed=3 (3 ok + 1 isolated), got %d", processed)
	}
	if err == nil {
		t.Fatalf("expected error wrapper from per-event fallback, got nil")
	}
	if meili.batchCalls != 1 {
		t.Fatalf("expected exactly 1 SyncEventsBatch call, got %d", meili.batchCalls)
	}
	if len(meili.perEventCalls) != len(eventIDs) {
		t.Fatalf("expected per-event SyncEvent for all %d events on data error, got %d: %v",
			len(eventIDs), len(meili.perEventCalls), meili.perEventCalls)
	}
	// Only the bad event should be re-queued (poisoned-document isolation).
	if got, want := countPendingMeilisearchSyncs(t, ctx, pool), 1; got != want {
		t.Fatalf("expected only the bad event re-queued, got %d pending rows", got)
	}
	var pendingID string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM pending_meilisearch_syncs LIMIT 1`).Scan(&pendingID); err != nil {
		t.Fatalf("scan remaining pending row: %v", err)
	}
	if pendingID != badEvent {
		t.Fatalf("expected remaining pending row to be the bad event %q, got %q", badEvent, pendingID)
	}
}

// TestDrainPendingMeilisearchSyncBatch_HappyPath sanity-checks that
// SyncEventsBatch success returns processed=len(claimed) and leaves
// pending_meilisearch_syncs empty.
func TestDrainPendingMeilisearchSyncBatch_HappyPath(t *testing.T) {
	pool, meili, handlers, ctx := setupMeiliSweeperTest(t)

	eventIDs := []string{"evt_h1", "evt_h2", "evt_h3"}
	seedPendingMeilisearchSyncs(t, ctx, pool, eventIDs)

	processed, err := handlers.DrainPendingMeilisearchSyncBatch(ctx, len(eventIDs))
	if err != nil {
		t.Fatalf("expected no error on happy path, got %v", err)
	}
	if processed != len(eventIDs) {
		t.Fatalf("expected processed=%d, got %d", len(eventIDs), processed)
	}
	if len(meili.perEventCalls) != 0 {
		t.Fatalf("happy path must not call per-event SyncEvent, got %d", len(meili.perEventCalls))
	}
	if got := countPendingMeilisearchSyncs(t, ctx, pool); got != 0 {
		t.Fatalf("expected pending_meilisearch_syncs empty after happy path drain, got %d", got)
	}
}

// TestDrainPendingMeilisearchSyncBatch_TimeoutDrainRateMatchesProduction
// is a regression scenario test: simulate sustained production into the
// queue while every batch call times out, and assert that the queue's
// claimed-then-reinserted rows do not drift to zero ("lost") and do
// not double-count.
func TestDrainPendingMeilisearchSyncBatch_TimeoutCorrectness(t *testing.T) {
	pool, meili, handlers, ctx := setupMeiliSweeperTest(t)
	meili.batchErr = context.DeadlineExceeded

	eventIDs := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		eventIDs = append(eventIDs, "evt_"+time.Now().Format("150405.000")+"_"+itoa(i))
	}
	seedPendingMeilisearchSyncs(t, ctx, pool, eventIDs)
	initial := countPendingMeilisearchSyncs(t, ctx, pool)
	if initial != len(eventIDs) {
		t.Fatalf("seed sanity: got %d want %d", initial, len(eventIDs))
	}

	for cycle := 0; cycle < 5; cycle++ {
		processed, err := handlers.DrainPendingMeilisearchSyncBatch(ctx, 25)
		if processed != 0 {
			t.Fatalf("cycle %d: expected processed=0, got %d", cycle, processed)
		}
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cycle %d: expected wrapped DeadlineExceeded, got %v", cycle, err)
		}
		got := countPendingMeilisearchSyncs(t, ctx, pool)
		if got != initial {
			t.Fatalf("cycle %d: queue size drifted; got %d want %d (no loss / no double-count)", cycle, got, initial)
		}
	}
	if len(meili.perEventCalls) != 0 {
		t.Fatalf("per-event fallback must never run across timeout cycles, got %d calls",
			len(meili.perEventCalls))
	}
}

func TestDrainPendingProfileSyncBatch_CollapsesSameAuthor(t *testing.T) {
	pool, meili, handlers, ctx := setupMeiliSweeperTest(t)
	authorA := meiliSweeperTestPubkey
	authorB := "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	seedPendingMeilisearchSyncsKind(t, ctx, pool, []string{"a_old", "a_mid"}, 0, authorA, 2*time.Minute)
	seedPendingMeilisearchSyncsKind(t, ctx, pool, []string{"a_new"}, 0, authorA, time.Minute)
	seedPendingMeilisearchSyncsKind(t, ctx, pool, []string{"b_only"}, 0, authorB, 30*time.Second)

	synced, picked, err := handlers.DrainPendingProfileMeilisearchSyncBatch(ctx, 10)
	if err != nil {
		t.Fatalf("drain profiles: %v", err)
	}
	if picked != 4 {
		t.Fatalf("picked = %d, want 4", picked)
	}
	if synced != 2 {
		t.Fatalf("synced = %d, want one event per author", synced)
	}
	if meili.batchCalls != 1 {
		t.Fatalf("batch calls = %d, want 1", meili.batchCalls)
	}
	if len(meili.batchEventIDs) != 1 || len(meili.batchEventIDs[0]) != 2 {
		t.Fatalf("sync ids = %#v, want 2", meili.batchEventIDs)
	}
	if got := countPendingMeilisearchSyncs(t, ctx, pool); got != 0 {
		t.Fatalf("pending = %d, want the duplicate author rows removed", got)
	}
}

func TestDrainPendingMeilisearchSyncBatch_KeepsKindsApart(t *testing.T) {
	pool, meili, handlers, ctx := setupMeiliSweeperTest(t)
	seedPendingMeilisearchSyncsKind(t, ctx, pool, []string{"old_note"}, 1, meiliSweeperTestPubkey, 5*time.Minute)
	seedPendingMeilisearchSyncsKind(t, ctx, pool, []string{"profile_a", "profile_b"}, 0, meiliSweeperTestPubkey, time.Minute)

	synced, err := handlers.DrainPendingMeilisearchSyncBatch(ctx, 10)
	if err != nil {
		t.Fatalf("drain oldest: %v", err)
	}
	if synced != 1 {
		t.Fatalf("synced = %d, want only the oldest note", synced)
	}
	if len(meili.batchEventIDs) != 1 || len(meili.batchEventIDs[0]) != 1 || meili.batchEventIDs[0][0] != "old_note" {
		t.Fatalf("sync ids = %#v, want [old_note]", meili.batchEventIDs)
	}
	if got := countPendingMeilisearchSyncs(t, ctx, pool); got != 2 {
		t.Fatalf("pending = %d, want the two profile rows still queued", got)
	}

	synced, picked, err := handlers.DrainPendingProfileMeilisearchSyncBatch(ctx, 10)
	if err != nil {
		t.Fatalf("drain profiles: %v", err)
	}
	if synced != 1 || picked != 2 {
		t.Fatalf("profile drain synced=%d picked=%d, want 1 and 2", synced, picked)
	}
	if got := countPendingMeilisearchSyncs(t, ctx, pool); got != 0 {
		t.Fatalf("pending = %d, want empty", got)
	}
}

func TestDrainPendingNoteSyncBatch_SkipsOlderProfiles(t *testing.T) {
	pool, meili, handlers, ctx := setupMeiliSweeperTest(t)
	profiles := make([]string, 0, 40)
	for i := 0; i < 40; i++ {
		profiles = append(profiles, fmt.Sprintf("old_profile_%02d", i))
	}
	seedPendingMeilisearchSyncsKind(t, ctx, pool, profiles, 0, meiliSweeperTestPubkey, 2*time.Hour)
	seedPendingMeilisearchSyncsKind(t, ctx, pool, []string{"newer_note"}, 1, meiliSweeperTestPubkey, time.Minute)

	synced, picked, err := handlers.DrainPendingNoteMeilisearchSyncBatch(ctx, 10)
	if err != nil {
		t.Fatalf("drain notes: %v", err)
	}
	if synced != 1 || picked != 1 {
		t.Fatalf("synced=%d picked=%d, want the note behind the older profiles", synced, picked)
	}
	if len(meili.batchEventIDs) != 1 || len(meili.batchEventIDs[0]) != 1 || meili.batchEventIDs[0][0] != "newer_note" {
		t.Fatalf("sync ids = %#v, want [newer_note]", meili.batchEventIDs)
	}
	if got := countPendingMeilisearchSyncs(t, ctx, pool); got != len(profiles) {
		t.Fatalf("pending = %d, want the %d profiles still queued", got, len(profiles))
	}
}

func TestNoteClaimPlanUsesPartialIndex(t *testing.T) {
	pool, _, _, ctx := setupMeiliSweeperTest(t)
	if _, err := pool.Exec(ctx, `
		INSERT INTO pending_meilisearch_syncs (event_id, marked_at, kind)
		SELECT 'old_profile_' || g, now() - interval '3 hours', 0
		FROM generate_series(1, 4000) AS g
	`); err != nil {
		t.Fatalf("seed profiles: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO pending_meilisearch_syncs (event_id, marked_at, kind)
		SELECT 'note_' || g, now() - interval '1 minute', 1
		FROM generate_series(1, 40) AS g
	`); err != nil {
		t.Fatalf("seed notes: %v", err)
	}
	if _, err := pool.Exec(ctx, `ANALYZE pending_meilisearch_syncs`); err != nil {
		t.Fatalf("analyze: %v", err)
	}

	rows, err := pool.Query(ctx, `
		EXPLAIN
		SELECT p.event_id
		FROM pending_meilisearch_syncs p
		WHERE p.kind IN (`+nostr.IndexedNoteKindsSQL+`)
		ORDER BY p.marked_at ASC, p.event_id ASC
		FOR UPDATE OF p SKIP LOCKED
		LIMIT 10
	`)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan explain: %v", err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read explain: %v", err)
	}
	if !strings.Contains(plan.String(), "idx_pending_meilisearch_syncs_note_marked_at") {
		t.Fatalf("note claim plan did not use the partial index:\n%s", plan.String())
	}
}

func TestNoteSyncDue(t *testing.T) {
	if derivation.NoteSyncDue(0, time.Hour, 1000, 5*time.Minute) {
		t.Fatal("empty queue is not due")
	}
	if derivation.NoteSyncDue(40, time.Second, 1000, 5*time.Minute) {
		t.Fatal("a short fresh batch is not due")
	}
	if !derivation.NoteSyncDue(40, 5*time.Minute, 1000, 5*time.Minute) {
		t.Fatal("a short batch is due once it has waited out the interval")
	}
	if !derivation.NoteSyncDue(1000, time.Second, 1000, 5*time.Minute) {
		t.Fatal("a full batch is due immediately")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
