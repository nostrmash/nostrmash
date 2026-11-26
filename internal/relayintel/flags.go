package relayintel

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var syntheticPubkeys sync.Map

var syntheticMisses sync.Map

var syntheticMissCount atomic.Int64

type syntheticMiss struct {
	Expires time.Time
	Epoch   int64
}

// projectionEnabled gates role-row writes. Canonical replaceable state and
// deletion tombstones stay on. The default is on so existing deployments
// keep projecting until an operator turns the feature off.
var projectionEnabled atomic.Bool

func init() {
	projectionEnabled.Store(true)
}

// SetProjectionEnabled stops role membership projection without stopping
// protocol-correct replaceable-state updates.
func SetProjectionEnabled(enabled bool) {
	projectionEnabled.Store(enabled)
}

// ProjectionEnabled reports whether role rows should be written.
func ProjectionEnabled() bool {
	return projectionEnabled.Load()
}

// RememberSynthetic marks a probe identity so later analytics can ignore it.
func RememberSynthetic(pubkey string) {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if len(pubkey) == 64 {
		syntheticPubkeys.Store(pubkey, struct{}{})
	}
}

// SyntheticPubkey reports a probe identity recorded in this process.
func SyntheticPubkey(pubkey string) bool {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	_, ok := syntheticPubkeys.Load(pubkey)
	return ok
}

// SyntheticAuthor reports a probe identity stored by any worker.
// A lookup error is not an ordinary author. Callers that publish analytics
// or search documents use LookupSynthetic and retry when it returns an error.
func SyntheticAuthor(ctx context.Context, pool *pgxpool.Pool, pubkey string) bool {
	synthetic, err := LookupSynthetic(ctx, pool, pubkey)
	return err == nil && synthetic
}

// LookupSynthetic reports whether pubkey is a probe identity.
// The positive cache is authoritative for this process. A negative cache hit
// is used only while the shared identity epoch is unchanged, so a registration
// committed by another worker is visible on the next lookup. Database errors
// are returned and are not stored as absence.
func LookupSynthetic(ctx context.Context, pool *pgxpool.Pool, pubkey string) (bool, error) {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if SyntheticPubkey(pubkey) {
		return true, nil
	}
	if len(pubkey) != 64 {
		return false, nil
	}
	if pool == nil {
		return false, fmt.Errorf("synthetic identity registry is unavailable")
	}
	var epoch int64
	if err := pool.QueryRow(ctx, `SELECT epoch FROM relay_probe_identity_epoch WHERE id`).Scan(&epoch); err != nil {
		return false, fmt.Errorf("synthetic identity epoch: %w", err)
	}
	if miss, ok := freshMiss(pubkey); ok && miss.Epoch == epoch {
		return false, nil
	}
	var one int
	err := pool.QueryRow(ctx, `
		SELECT 1 FROM relay_probe_identities
		WHERE pubkey = $1
		  AND (expires_at IS NULL OR expires_at > clock_timestamp())
	`, pubkey).Scan(&one)
	if err == nil {
		RememberSynthetic(pubkey)
		dropSyntheticMiss(pubkey)
		return true, nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		storeSyntheticMiss(pubkey, epoch)
		return false, nil
	}
	return false, fmt.Errorf("synthetic identity lookup: %w", err)
}

func freshMiss(pubkey string) (syntheticMiss, bool) {
	value, ok := syntheticMisses.Load(pubkey)
	if !ok {
		return syntheticMiss{}, false
	}
	entry, ok := value.(syntheticMiss)
	if ok && time.Now().Before(entry.Expires) {
		return entry, true
	}
	dropSyntheticMiss(pubkey)
	return syntheticMiss{}, false
}

func storeSyntheticMiss(pubkey string, epoch int64) {
	if syntheticMissCount.Load() >= 20000 {
		pruneExpiredMisses()
	}
	if syntheticMissCount.Load() >= 20000 {
		return
	}
	entry := syntheticMiss{Expires: time.Now().Add(15 * time.Second), Epoch: epoch}
	if _, loaded := syntheticMisses.LoadOrStore(pubkey, entry); !loaded {
		syntheticMissCount.Add(1)
	}
}

func pruneExpiredMisses() {
	now := time.Now()
	syntheticMisses.Range(func(key, value any) bool {
		entry, ok := value.(syntheticMiss)
		if ok && now.Before(entry.Expires) {
			return true
		}
		pubkey, ok := key.(string)
		if ok {
			dropSyntheticMiss(pubkey)
		}
		return true
	})
}

func dropSyntheticMiss(pubkey string) {
	if _, loaded := syntheticMisses.LoadAndDelete(pubkey); loaded {
		syntheticMissCount.Add(-1)
	}
}

// registerSyntheticIdentity records a durable probe pubkey before publication.
// It stores no private key. A failed insert refuses publication.
func registerSyntheticIdentity(ctx context.Context, pool *pgxpool.Pool, pubkey string) error {
	return registerSyntheticIdentityUntil(ctx, pool, pubkey, time.Time{})
}

// registerSyntheticIdentityUntil records a probe pubkey. A zero expiry is durable.
// A positive expiry is for an ephemeral gift-wrap author and is deleted later.
func registerSyntheticIdentityUntil(ctx context.Context, pool *pgxpool.Pool, pubkey string, expires time.Time) error {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if len(pubkey) != 64 {
		return fmt.Errorf("synthetic identity is not a pubkey")
	}
	RememberSynthetic(pubkey)
	dropSyntheticMiss(pubkey)
	if pool == nil {
		return nil
	}
	var expiry any
	if !expires.IsZero() {
		expiry = expires.UTC()
	}
	if _, err := pool.Exec(ctx, `
		WITH upsert AS (
			INSERT INTO relay_probe_identities (pubkey, expires_at)
			VALUES ($1, $2)
			ON CONFLICT (pubkey) DO UPDATE
			SET expires_at = CASE
				WHEN relay_probe_identities.expires_at IS NULL OR EXCLUDED.expires_at IS NULL THEN NULL
				ELSE GREATEST(relay_probe_identities.expires_at, EXCLUDED.expires_at)
			END
			RETURNING pubkey
		)
		UPDATE relay_probe_identity_epoch
		SET epoch = epoch + 1
		WHERE id AND EXISTS (SELECT 1 FROM upsert)
	`, pubkey, expiry); err != nil {
		return err
	}
	return nil
}

// LoadSyntheticIdentities restores probe pubkeys stored by an earlier worker.
// A missing table leaves the in-memory set unchanged.
func LoadSyntheticIdentities(ctx context.Context, pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	rows, err := pool.Query(ctx, `
		SELECT pubkey FROM relay_probe_identities
		WHERE expires_at IS NULL OR expires_at > clock_timestamp()
	`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var pubkey string
		if rows.Scan(&pubkey) == nil {
			RememberSynthetic(pubkey)
		}
	}
}
