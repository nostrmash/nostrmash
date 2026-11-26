package relayintel

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

var syntheticPubkeys sync.Map

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
