package relayintel

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Project writes the winning role projection for one event.
// A losing event is a no-op. Replaying the winner replaces its memberships.
func Project(ctx context.Context, tx pgx.Tx, in EventInput) error {
	if tx == nil || !HandlesKind(in.Kind) {
		return nil
	}
	parsed, err := Parse(in)
	if err != nil {
		return err
	}
	if parsed.Clear {
		return clearIfWinner(ctx, tx, in)
	}
	if parsed.Head.EventID == "" {
		return nil
	}
	head := parsed.Head
	var locked string
	err = tx.QueryRow(ctx, `
		SELECT event_id FROM relay_config_heads
		WHERE pubkey = $1 AND source_kind = $2 AND d_tag = $3
		FOR UPDATE
	`, head.Pubkey, head.SourceKind, head.DTag).Scan(&locked)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) && !SchemaMissing(err) {
		return fmt.Errorf("lock relay config head: %w", err)
	}
	blocked, err := deletionBlocks(ctx, tx, head.Pubkey, head.SourceKind, head.DTag, head.EventID, head.CreatedAt)
	if err != nil {
		return err
	}
	if blocked {
		return nil
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO relay_config_heads (
			pubkey, source_kind, d_tag, event_id, created_at,
			explicitly_empty, client_side_only, declaration_source,
			invalid_endpoint_count, truncated, title
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8,
			$9, $10, $11
		)
		ON CONFLICT (pubkey, source_kind, d_tag) DO UPDATE
		SET event_id = EXCLUDED.event_id,
		    created_at = EXCLUDED.created_at,
		    explicitly_empty = EXCLUDED.explicitly_empty,
		    client_side_only = EXCLUDED.client_side_only,
		    declaration_source = EXCLUDED.declaration_source,
		    invalid_endpoint_count = EXCLUDED.invalid_endpoint_count,
		    truncated = EXCLUDED.truncated,
		    title = EXCLUDED.title,
		    updated_at = now()
		WHERE EXCLUDED.created_at > relay_config_heads.created_at
		   OR (
		       EXCLUDED.created_at = relay_config_heads.created_at
		       AND EXCLUDED.event_id < relay_config_heads.event_id
		   )
		   OR EXCLUDED.event_id = relay_config_heads.event_id
	`, head.Pubkey, head.SourceKind, head.DTag, head.EventID, head.CreatedAt,
		head.ExplicitlyEmpty, head.ClientSideOnly, head.DeclarationSource,
		head.InvalidEndpoints, head.Truncated, head.Title)
	if err != nil {
		return fmt.Errorf("upsert relay config head: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	return replaceMembers(ctx, tx, head, parsed.Members)
}

// ErrNotReady means the role tables are not in this database yet.
var ErrNotReady = errors.New("relay intelligence schema is not ready")

// ErrInvalidInput is a caller error. The message is safe to return.
var ErrInvalidInput = errors.New("invalid relay intelligence input")

// SchemaMissing reports a query that failed because a role table does not exist.
func SchemaMissing(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

func deletionBlocks(ctx context.Context, tx pgx.Tx, pubkey string, kind int, dTag, eventID string, createdAt int64) (bool, error) {
	var one int
	err := tx.QueryRow(ctx, `
		SELECT 1 FROM deletion_events
		WHERE target_event_id = $1 AND deleter_pubkey = $2
		LIMIT 1
	`, eventID, pubkey).Scan(&one)
	if err == nil {
		return true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		if SchemaMissing(err) {
			return false, nil
		}
		return false, fmt.Errorf("check event deletion: %w", err)
	}
	err = tx.QueryRow(ctx, `
		SELECT 1 FROM deletion_addresses
		WHERE pubkey = $1 AND kind = $2 AND d_tag = $3 AND created_at >= $4
		LIMIT 1
	`, pubkey, kind, dTag, createdAt).Scan(&one)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) || SchemaMissing(err) {
		return false, nil
	}
	return false, fmt.Errorf("check address deletion: %w", err)
}

func wrapDB(err error) error {
	if err == nil || errors.Is(err, pgx.ErrNoRows) || errors.Is(err, ErrNotReady) {
		return err
	}
	if SchemaMissing(err) {
		return fmt.Errorf("%w", ErrNotReady)
	}
	return err
}

func clearIfWinner(ctx context.Context, tx pgx.Tx, in EventInput) error {
	var existingID string
	var existingCreated int64
	err := tx.QueryRow(ctx, `
		SELECT event_id, created_at
		FROM relay_config_heads
		WHERE pubkey = $1 AND source_kind = $2 AND d_tag = ''
	`, in.Pubkey, in.Kind).Scan(&existingID, &existingCreated)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return fmt.Errorf("load relay config head: %w", err)
	}
	if !wins(in.CreatedAt, in.EventID, existingCreated, existingID) {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM relay_role_memberships
		WHERE pubkey = $1 AND source_kind = $2 AND d_tag = ''
	`, in.Pubkey, in.Kind); err != nil {
		return fmt.Errorf("delete cleared memberships: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM relay_config_heads
		WHERE pubkey = $1 AND source_kind = $2 AND d_tag = ''
	`, in.Pubkey, in.Kind); err != nil {
		return fmt.Errorf("delete cleared head: %w", err)
	}
	return nil
}

func replaceMembers(ctx context.Context, tx pgx.Tx, head Head, members []Membership) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM relay_role_memberships
		WHERE pubkey = $1 AND source_kind = $2 AND d_tag = $3
	`, head.Pubkey, head.SourceKind, head.DTag); err != nil {
		return fmt.Errorf("delete relay role memberships: %w", err)
	}
	for _, member := range members {
		if member.DTag == "" {
			member.DTag = head.DTag
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO relay_role_memberships (
				pubkey, role, url_key, source_kind, d_tag,
				endpoint_url, endpoint_type, source_event_id, created_at,
				declaration_source, marker, public_safe
			) VALUES (
				$1, $2, $3, $4, $5,
				$6, $7, $8, $9,
				$10, $11, $12
			)
			ON CONFLICT (pubkey, role, url_key, source_kind, d_tag) DO UPDATE
			SET endpoint_url = EXCLUDED.endpoint_url,
			    endpoint_type = EXCLUDED.endpoint_type,
			    source_event_id = EXCLUDED.source_event_id,
			    created_at = EXCLUDED.created_at,
			    declaration_source = EXCLUDED.declaration_source,
			    marker = EXCLUDED.marker,
			    public_safe = EXCLUDED.public_safe,
			    updated_at = now()
		`, member.Pubkey, member.Role, member.URLKey, member.SourceKind, member.DTag,
			member.EndpointURL, member.EndpointType, member.SourceEventID, member.CreatedAt,
			member.DeclarationSource, member.Marker, member.PublicSafe); err != nil {
			return fmt.Errorf("insert relay role membership: %w", err)
		}
	}
	return nil
}
