package derivation

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nostrmash/nostrmash/internal/metrics"
	"github.com/nostrmash/nostrmash/internal/relayintel"
)

// SyncMeilisearch keeps Meilisearch indexes warm after event derivations.
// Sync failures are intentionally non-fatal so projection jobs remain durable
// when Meilisearch is unavailable.
func (h *Handlers) SyncMeilisearch(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil || h.meili == nil || !h.meili.Enabled() {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	var pubkey string
	if err := h.pool.QueryRow(ctx, `SELECT pubkey FROM events WHERE id = $1`, eventID).Scan(&pubkey); err == nil && relayintel.SyntheticPubkey(pubkey) {
		return nil
	}
	started := time.Now()
	if err := h.meili.SyncEvent(ctx, h.pool, eventID); err != nil {
		metrics.ObserveMeiliSync("derivation", "error", time.Since(started))
		slog.Warn("meilisearch_sync_failed", "source", "derivation", "event_id", eventID, "error", err)
		// Best-effort behavior: do not fail the derivation bundle if sync is down.
		return nil
	}
	metrics.ObserveMeiliSync("derivation", "success", time.Since(started))
	return nil
}
