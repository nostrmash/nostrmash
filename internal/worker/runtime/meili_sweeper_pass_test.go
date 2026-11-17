package runtime

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/config"
	"github.com/nostrmash/nostrmash/internal/derivation"
	"github.com/nostrmash/nostrmash/internal/store"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

// failingFirstBatchMeili fails the first SyncEventsBatch and accepts the rest.
// The sweeper calls notes first, so the failure is the note batch.
type failingFirstBatchMeili struct {
	mu    sync.Mutex
	calls [][]string
}

func (f *failingFirstBatchMeili) Enabled() bool { return true }

func (f *failingFirstBatchMeili) SyncEvent(context.Context, *pgxpool.Pool, string) error {
	return nil
}

func (f *failingFirstBatchMeili) SyncEventsBatch(_ context.Context, _ *pgxpool.Pool, eventIDs []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := append([]string(nil), eventIDs...)
	f.calls = append(f.calls, cp)
	if len(f.calls) == 1 {
		return context.DeadlineExceeded
	}
	return nil
}

func TestSweepMeilisearchBacklog_NoteFailureStillDrainsProfiles(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "meili sweeper pass"), "meili_pass")
	if err := store.Migrate(ctx, pool, "meili-sweeper-pass"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	const pubkey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES
			('stuck_note', $1, $2, 1, 'sig', '', '{}'::jsonb),
			('profile_a', $1, $2, 0, 'sig', '', '{}'::jsonb),
			('profile_b', $1, $2, 0, 'sig', '', '{}'::jsonb)
	`, pubkey, time.Now().Unix()); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO pending_meilisearch_syncs (event_id, marked_at, kind) VALUES
			('stuck_note', now() - interval '10 minutes', 1),
			('profile_a', now() - interval '2 hours', 0),
			('profile_b', now() - interval '1 hour', 0)
	`); err != nil {
		t.Fatalf("seed pending: %v", err)
	}

	meili := &failingFirstBatchMeili{}
	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{MeiliClient: meili})
	log := &recordingLogger{}
	sweepMeilisearchBacklog(ctx, log, handlers, config.WorkerMeilisearchSweeperConfig{
		Enabled:   true,
		Interval:  5 * time.Minute,
		BatchSize: 10,
	}, 1)

	if !log.sawError("meilisearch_sweeper_batch_failed") {
		t.Fatal("expected the note batch failure to be logged")
	}
	meili.mu.Lock()
	calls := meili.calls
	meili.mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("batch calls = %d, want notes then profiles", len(calls))
	}
	if len(calls[0]) != 1 || calls[0][0] != "stuck_note" {
		t.Fatalf("first batch = %#v, want the note", calls[0])
	}
	if len(calls[1]) != 1 || calls[1][0] != "profile_a" {
		t.Fatalf("second batch = %#v, want the older profile (same author collapses)", calls[1])
	}

	var pending int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM pending_meilisearch_syncs`).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 1 {
		t.Fatalf("pending = %d, want the re-queued note only", pending)
	}
	var survivor string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM pending_meilisearch_syncs`).Scan(&survivor); err != nil {
		t.Fatalf("load survivor: %v", err)
	}
	if survivor != "stuck_note" {
		t.Fatalf("survivor = %q, want stuck_note", survivor)
	}
}
