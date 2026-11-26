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
// A cache miss checks relay_probe_identities, so a new ephemeral author is
// excluded without restarting ingestion.
func SyntheticAuthor(ctx context.Context, pool *pgxpool.Pool, pubkey string) bool {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if SyntheticPubkey(pubkey) {
		return true
	}
	if missFresh(pubkey) {
		return false
	}
	if pool == nil || len(pubkey) != 64 {
		return false
	}
	var one int
	err := pool.QueryRow(ctx, `SELECT 1 FROM relay_probe_identities WHERE pubkey = $1`, pubkey).Scan(&one)
	if err == nil {
		RememberSynthetic(pubkey)
		dropSyntheticMiss(pubkey)
		return true
	}
	if errors.Is(err, pgx.ErrNoRows) {
		storeSyntheticMiss(pubkey)
		return false
	}
	return false
}

func missFresh(pubkey string) bool {
	value, ok := syntheticMisses.Load(pubkey)
	if !ok {
		return false
	}
	expires, ok := value.(time.Time)
	if ok && time.Now().Before(expires) {
		return true
	}
	dropSyntheticMiss(pubkey)
	return false
}

func storeSyntheticMiss(pubkey string) {
	if syntheticMissCount.Load() >= 20000 {
		return
	}
	if _, loaded := syntheticMisses.LoadOrStore(pubkey, time.Now().Add(15*time.Second)); !loaded {
		syntheticMissCount.Add(1)
	}
}

func dropSyntheticMiss(pubkey string) {
	if _, loaded := syntheticMisses.LoadAndDelete(pubkey); loaded {
		syntheticMissCount.Add(-1)
	}
}

// registerSyntheticIdentity records a probe pubkey before a test event is sent.
// It stores no private key. A failed insert refuses publication.
func registerSyntheticIdentity(ctx context.Context, pool *pgxpool.Pool, pubkey string) error {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if len(pubkey) != 64 {
		return fmt.Errorf("synthetic identity is not a pubkey")
	}
	RememberSynthetic(pubkey)
	dropSyntheticMiss(pubkey)
	if pool == nil {
		return nil
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_identities (pubkey) VALUES ($1)
		ON CONFLICT (pubkey) DO NOTHING
	`, pubkey); err != nil {
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
	rows, err := pool.Query(ctx, `SELECT pubkey FROM relay_probe_identities`)
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
