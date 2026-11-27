package derivation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// The profile write below is conditional on the captured winner still being
// canonical. A slower worker must not publish the snapshot it read after a
// newer winner has been projected.

func (h *Handlers) ProjectProfilesLatest(ctx context.Context, eventID string) error {
	return h.projectProfilesLatestWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectProfilesLatestWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var kind int
	err := h.pool.QueryRow(ctx, `
		SELECT pubkey, kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &kind)
	if err != nil {
		return fmt.Errorf("load event for profile projection: %w", err)
	}
	if kind != 0 {
		return nil
	}

	winnerID, winnerCreatedAt, winnerContent, found, err := loadAuthoritativeReplaceable(ctx, h.pool, pubkey, 0, "")
	if err != nil {
		return fmt.Errorf("load latest metadata event: %w", err)
	}
	if !found {
		return nil
	}
	runProjectionReadBarrier(winnerID)

	profileJSON := json.RawMessage(`{}`)
	var profileName string
	var profileDisplayName string
	var profileAbout string
	var profileNIP05 string
	if strings.TrimSpace(winnerContent) != "" {
		var profile map[string]any
		if err := json.Unmarshal([]byte(winnerContent), &profile); err == nil {
			encoded, marshalErr := json.Marshal(profile)
			if marshalErr == nil {
				profileJSON = encoded
			}
			profileName = profileStringField(profile, "name")
			profileDisplayName = profileStringField(profile, "display_name")
			profileAbout = profileStringField(profile, "about")
			profileNIP05 = profileStringField(profile, "nip05")
		}
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationProfilesLatest,
		ProfilesLatestVersion,
		"Project latest effective replaceable metadata (kind 0) per pubkey",
		versionOverride,
	)
	if err != nil {
		return err
	}

	if _, err = tx.Exec(ctx, `SELECT relay_intel_lock_coordinate($1, 0, '')`, pubkey); err != nil {
		return fmt.Errorf("lock profile coordinate: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO profiles_latest (
			pubkey, metadata_event_id, metadata_created_at, profile_json,
			name, display_name, about, nip05, derivation_version
		)
		SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9
		WHERE relay_intel_may_project($1, 0, '', $2, $3, NULL::text, NULL::bigint)
		ON CONFLICT (pubkey) DO UPDATE
		SET metadata_event_id = EXCLUDED.metadata_event_id,
		    metadata_created_at = EXCLUDED.metadata_created_at,
		    profile_json = EXCLUDED.profile_json,
		    name = EXCLUDED.name,
		    display_name = EXCLUDED.display_name,
		    about = EXCLUDED.about,
		    nip05 = EXCLUDED.nip05,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
		WHERE relay_intel_may_project(
		    EXCLUDED.pubkey, 0, '', EXCLUDED.metadata_event_id, EXCLUDED.metadata_created_at,
		    profiles_latest.metadata_event_id, profiles_latest.metadata_created_at
		)
	`,
		pubkey,
		winnerID,
		winnerCreatedAt,
		profileJSON,
		profileName,
		profileDisplayName,
		profileAbout,
		profileNIP05,
		writeVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert profiles_latest: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit profile projection tx: %w", err)
	}
	return nil
}

func profileStringField(profile map[string]any, key string) string {
	if profile == nil {
		return ""
	}
	raw, ok := profile[key]
	if !ok {
		return ""
	}
	value, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}
