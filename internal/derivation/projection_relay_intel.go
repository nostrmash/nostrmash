package derivation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/nostrmash/nostrmash/internal/metrics"
	"github.com/nostrmash/nostrmash/internal/relayintel"
)

// projectRelayIntelEvent updates role memberships for one stored event.
// It is safe to call for every kind: unrelated kinds return immediately.
// The write uses its own transaction so a retry after a committed sibling
// projection is idempotent.
func (h *Handlers) projectRelayIntelEvent(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil || eventID == "" {
		return nil
	}
	var in relayintel.EventInput
	var raw string
	err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind, created_at, content, raw_json::text
		FROM events
		WHERE id = $1
	`, eventID).Scan(&in.Pubkey, &in.Kind, &in.CreatedAt, &in.Content, &raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load event for relay intel: %w", err)
	}
	if !relayintel.HandlesKind(in.Kind) {
		return nil
	}
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		metrics.IncRelayIntelProjection(in.Kind, "malformed")
		return nil
	}
	in.EventID = eventID
	in.Tags = payload.Tags
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := relayintel.Project(ctx, tx, in); err != nil {
		if relayintel.SchemaMissing(err) {
			metrics.IncRelayIntelProjection(in.Kind, "disabled")
			return nil
		}
		metrics.IncRelayIntelProjection(in.Kind, "error")
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	metrics.IncRelayIntelProjection(in.Kind, "applied")
	return nil
}

// clearRelayIntelDeletion removes role rows the author deleted.
// An e tag clears that version. An a tag clears the whole address.
// Another author's coordinate is ignored.
func (h *Handlers) clearRelayIntelDeletion(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil || eventID == "" {
		return nil
	}
	var pubkey string
	var kind int
	var raw string
	err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind, raw_json::text FROM events WHERE id = $1
	`, eventID).Scan(&pubkey, &kind, &raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if kind != 5 {
		return nil
	}
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return nil
	}
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	ids := make([]string, 0, 4)
	type coord struct {
		kind int
		dTag string
	}
	coords := make([]coord, 0, 2)
	for _, tag := range payload.Tags {
		if len(tag) < 2 {
			continue
		}
		switch tag[0] {
		case "e":
			id := strings.TrimSpace(tag[1])
			if len(id) == 64 {
				ids = append(ids, strings.ToLower(id))
			}
		case "a":
			kindN, owner, dTag, ok := splitRelayCoordinate(tag[1])
			if ok && owner == pubkey && relayintel.HandlesKind(kindN) {
				coords = append(coords, coord{kind: kindN, dTag: dTag})
			}
		}
		if len(ids) >= 64 && len(coords) >= 32 {
			break
		}
	}
	if len(ids) > 64 {
		ids = ids[:64]
	}
	if len(coords) > 32 {
		coords = coords[:32]
	}
	if len(ids) == 0 && len(coords) == 0 {
		return nil
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if len(ids) > 0 {
		if _, err := tx.Exec(ctx, `
			DELETE FROM relay_role_memberships
			WHERE pubkey = $1 AND source_event_id = ANY($2::text[])
		`, pubkey, ids); err != nil {
			if relayintel.SchemaMissing(err) {
				return nil
			}
			return err
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM relay_config_heads
			WHERE pubkey = $1 AND event_id = ANY($2::text[])
		`, pubkey, ids); err != nil {
			if relayintel.SchemaMissing(err) {
				return nil
			}
			return err
		}
	}
	for _, item := range coords {
		if _, err := tx.Exec(ctx, `
			DELETE FROM relay_role_memberships
			WHERE pubkey = $1 AND source_kind = $2 AND d_tag = $3
		`, pubkey, item.kind, item.dTag); err != nil {
			if relayintel.SchemaMissing(err) {
				return nil
			}
			return err
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM relay_config_heads
			WHERE pubkey = $1 AND source_kind = $2 AND d_tag = $3
		`, pubkey, item.kind, item.dTag); err != nil {
			if relayintel.SchemaMissing(err) {
				return nil
			}
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

func splitRelayCoordinate(value string) (kind int, pubkey, dTag string, ok bool) {
	parts := strings.Split(strings.TrimSpace(value), ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, "", "", false
	}
	kind, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, "", "", false
	}
	pubkey = strings.ToLower(strings.TrimSpace(parts[1]))
	if len(pubkey) != 64 {
		return 0, "", "", false
	}
	if len(parts) == 3 {
		dTag = parts[2]
		if len(dTag) > 256 {
			dTag = dTag[:256]
		}
	}
	return kind, pubkey, dTag, true
}
