package derivation

import (
	"context"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5"
)

// projectionReadBarrier runs after a projector has captured a canonical
// winner and before it writes that snapshot. Tests use it to interleave
// another worker. Production leaves it unset.
var (
	projectionBarrierMu sync.Mutex
	projectionBarrier   func(winnerEventID string)
)

// SetProjectionReadBarrier installs the test hook described above.
// A nil function removes it.
func SetProjectionReadBarrier(fn func(winnerEventID string)) {
	projectionBarrierMu.Lock()
	projectionBarrier = fn
	projectionBarrierMu.Unlock()
}

func runProjectionReadBarrier(winnerEventID string) {
	projectionBarrierMu.Lock()
	fn := projectionBarrier
	projectionBarrierMu.Unlock()
	if fn != nil {
		fn(winnerEventID)
	}
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// loadAuthoritativeReplaceable returns the event a projection may write.
// A canonical row wins, including an older replacement. A deleted canonical
// row is not a fallback source. When no canonical row exists, the newest
// non-deleted event wins, with the lower id breaking a timestamp tie.
func loadAuthoritativeReplaceable(ctx context.Context, q queryRower, pubkey string, kind int, dTag string) (id string, created int64, content string, found bool, err error) {
	var stateID string
	err = q.QueryRow(ctx, `
		SELECT event_id
		FROM replaceable_state
		WHERE pubkey = $1 AND kind = $2 AND d_tag = $3
	`, pubkey, kind, dTag).Scan(&stateID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", 0, "", false, err
	}
	if err == nil {
		err = q.QueryRow(ctx, `
			SELECT e.id, e.created_at, e.content
			FROM events e
			WHERE e.id = $1
			  AND NOT relay_intel_deleted($2, $3, $4, e.id, e.created_at)
		`, stateID, pubkey, kind, dTag).Scan(&id, &created, &content)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", 0, "", false, nil
		}
		if err != nil {
			return "", 0, "", false, err
		}
		return id, created, content, true, nil
	}
	err = q.QueryRow(ctx, `
		SELECT e.id, e.created_at, e.content
		FROM events e
		WHERE e.pubkey = $1
		  AND e.kind = $2
		  AND (
		      NOT relay_intel_addressable($2)
		      OR relay_intel_event_d_tag(e.id) = $3
		  )
		  AND NOT relay_intel_deleted($1, $2, $3, e.id, e.created_at)
		ORDER BY e.created_at DESC, e.id ASC
		LIMIT 1
	`, pubkey, kind, dTag).Scan(&id, &created, &content)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, "", false, nil
	}
	if err != nil {
		return "", 0, "", false, err
	}
	return id, created, content, true, nil
}
