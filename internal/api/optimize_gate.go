package api

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/nostrmash/nostrmash/internal/metrics"
)

// errOptimizeOverloaded is a deliberate refusal. The request did not run
// and it is not the per-client rate limit.
var errOptimizeOverloaded = errors.New("relay optimization is busy")

// optimizeGate bounds expensive optimization inside one API process.
// It does not queue waiters. A full gate rejects immediately.
type optimizeGate struct {
	slots    chan struct{}
	inFlight atomic.Int64
}

func newOptimizeGate(limit int) *optimizeGate {
	if limit < 1 {
		limit = 4
	}
	return &optimizeGate{slots: make(chan struct{}, limit)}
}

func (g *optimizeGate) acquire(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		metrics.IncRelayIntelOptimizeAdmission("cancelled")
		return err
	}
	select {
	case g.slots <- struct{}{}:
		metrics.SetRelayIntelOptimizeInFlight(float64(g.inFlight.Add(1)))
		metrics.IncRelayIntelOptimizeAdmission("accepted")
		return nil
	case <-ctx.Done():
		metrics.IncRelayIntelOptimizeAdmission("cancelled")
		return ctx.Err()
	default:
		metrics.IncRelayIntelOptimizeAdmission("rejected")
		return errOptimizeOverloaded
	}
}

func (g *optimizeGate) release() {
	if g == nil {
		return
	}
	select {
	case <-g.slots:
		metrics.SetRelayIntelOptimizeInFlight(float64(g.inFlight.Add(-1)))
	default:
	}
}
