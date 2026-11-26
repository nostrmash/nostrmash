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
	if !relayintel.ProjectionEnabled() {
		return nil
	}
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
		if errors.Is(err, relayintel.ErrInvalidInput) {
			metrics.IncRelayIntelProjection(in.Kind, "malformed")
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
	var createdAt int64
	var raw string
	err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind, created_at, raw_json::text FROM events WHERE id = $1
	`, eventID).Scan(&pubkey, &kind, &createdAt, &raw)
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
		if err := clearDeletedListProjections(ctx, tx, pubkey, ids, 0, 0); err != nil {
			return err
		}
	}
	for _, item := range coords {
		if err := relayintel.LockCoordinate(ctx, tx, pubkey, item.kind, item.dTag); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM relay_role_memberships
			WHERE pubkey = $1 AND source_kind = $2 AND d_tag = $3 AND created_at <= $4
		`, pubkey, item.kind, item.dTag, createdAt); err != nil {
			if relayintel.SchemaMissing(err) {
				return nil
			}
			return err
		}
		if _, err := tx.Exec(ctx, `
			DELETE FROM relay_config_heads
			WHERE pubkey = $1 AND source_kind = $2 AND d_tag = $3 AND created_at <= $4
		`, pubkey, item.kind, item.dTag, createdAt); err != nil {
			if relayintel.SchemaMissing(err) {
				return nil
			}
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO deletion_addresses (event_id, pubkey, kind, d_tag, created_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (event_id, kind, d_tag) DO NOTHING
		`, eventID, pubkey, item.kind, item.dTag, createdAt); err != nil && !relayintel.SchemaMissing(err) {
			return err
		}
		if err := clearDeletedListProjections(ctx, tx, pubkey, nil, item.kind, createdAt); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func splitRelayCoordinate(value string) (kind int, pubkey, dTag string, ok bool) {
	value = strings.TrimLeft(value, " \t")
	kindEnd := strings.IndexByte(value, ':')
	if kindEnd <= 0 || kindEnd >= len(value)-1 {
		return 0, "", "", false
	}
	kind, err := strconv.Atoi(value[:kindEnd])
	if err != nil {
		return 0, "", "", false
	}
	rest := value[kindEnd+1:]
	pubEnd := strings.IndexByte(rest, ':')
	if pubEnd == -1 {
		if len(rest) != 64 {
			return 0, "", "", false
		}
		return kind, strings.ToLower(rest), "", true
	}
	if pubEnd != 64 {
		return 0, "", "", false
	}
	pubkey = strings.ToLower(rest[:pubEnd])
	dTag = rest[pubEnd+1:]
	if len(dTag) > 1024 {
		return 0, "", "", false
	}
	if !relayintel.AddressableKind(kind) {
		dTag = ""
	}
	return kind, pubkey, dTag, true
}

// clearDeletedListProjections removes list rows a deletion already covered so
// a later replay cannot be the only thing standing between a tombstone and
// a restored configuration. eventIDs clears specific versions. kind plus
// createdAt clears an address up to the deletion timestamp.
func clearDeletedListProjections(ctx context.Context, tx pgx.Tx, pubkey string, eventIDs []string, kind int, createdAt int64) error {
	if len(eventIDs) > 0 {
		statements := []string{
			`DELETE FROM relay_lists_latest WHERE pubkey = $1 AND event_id = ANY($2::text[])`,
			`DELETE FROM dm_relay_lists_latest WHERE pubkey = $1 AND event_id = ANY($2::text[])`,
			`DELETE FROM profiles_latest WHERE pubkey = $1 AND metadata_event_id = ANY($2::text[])`,
			`SELECT relay_intel_remove_contact_projection($1, event_id)
			 FROM contact_lists_latest
			 WHERE pubkey = $1 AND event_id = ANY($2::text[])`,
		}
		for _, statement := range statements {
			if _, err := tx.Exec(ctx, statement, pubkey, eventIDs); err != nil && !relayintel.SchemaMissing(err) {
				return err
			}
		}
		return nil
	}
	var statement string
	switch kind {
	case 3:
		statement = `SELECT relay_intel_remove_contact_projection($1, event_id)
			FROM contact_lists_latest
			WHERE pubkey = $1 AND created_at <= $2`
	case 10002:
		statement = `DELETE FROM relay_lists_latest WHERE pubkey = $1 AND created_at <= $2`
	case 10050:
		statement = `DELETE FROM dm_relay_lists_latest WHERE pubkey = $1 AND created_at <= $2`
	default:
		return nil
	}
	if _, err := tx.Exec(ctx, statement, pubkey, createdAt); err != nil && !relayintel.SchemaMissing(err) {
		return err
	}
	return nil
}
