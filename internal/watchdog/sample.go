package watchdog

import (
	"fmt"
	"time"
)

const defaultStallAfter = 5 * time.Minute

// WorkerObservation is one look at the claim loop.
// Idle is true only after a successful claim that returned no jobs and left nothing in flight.
// An empty in-flight count by itself is not that observation.
type WorkerObservation struct {
	ProcessStarted time.Time
	Completed      int64
	Inflight       int64
	ClaimFailed    bool
	LastLoop       time.Time
	ClaimObserved  bool
	LastClaimOK    bool
	LastClaimCount int
	InflightSince  time.Time
	Now            time.Time
	StallAfter     time.Duration
}

// WorkerSample reports claim-loop health.
// The returned false means the loop has not iterated yet, so no heartbeat should be sent.
// loopAt is the last claim-loop iteration, not the time the heartbeat is sent.
// Progress is "<process start unix>:<successful completions>". A restart changes the token
// and opens a new progress window instead of looking like a stall.
func WorkerSample(obs WorkerObservation) (Sample, bool) {
	if obs.ProcessStarted.IsZero() || obs.LastLoop.IsZero() {
		return Sample{}, false
	}
	stall := obs.StallAfter
	if stall <= 0 {
		stall = defaultStallAfter
	}
	stalled := obs.Inflight > 0 && !obs.InflightSince.IsZero() && !obs.Now.Before(obs.InflightSince.Add(stall))
	idle := obs.ClaimObserved && obs.LastClaimOK && !obs.ClaimFailed && obs.Inflight == 0 && obs.LastClaimCount == 0 && !stalled
	return Sample{
		Progress: fmt.Sprintf("%d:%d", obs.ProcessStarted.Unix(), obs.Completed),
		Idle:     idle,
		LoopAt:   obs.LastLoop,
	}, true
}

// IngestObservation is one look at relay connectivity and accepted events.
type IngestObservation struct {
	ProcessStarted time.Time
	Valid          uint64
	Tracked        int
	Healthy        int
	HealthKnown    bool
	ObservedAt     time.Time
}

// IngestorSample reports relay-manager state.
// A connected relay may be quiet, so idle is true when at least one tracked relay is healthy.
// Zero tracked relays is not idle: the manager may be unset, empty, or disconnected from every relay.
// loopAt is when that state was read. It does not prove an event arrived.
// HealthKnown false means the relay manager has not published a snapshot yet, so no sample is sent.
func IngestorSample(obs IngestObservation) (Sample, bool) {
	if !obs.HealthKnown || obs.ProcessStarted.IsZero() || obs.ObservedAt.IsZero() {
		return Sample{}, false
	}
	if obs.Tracked < 0 {
		obs.Tracked = 0
	}
	if obs.Healthy < 0 {
		obs.Healthy = 0
	}
	return Sample{
		Progress: fmt.Sprintf("%d:%d", obs.ProcessStarted.Unix(), obs.Valid),
		Idle:     obs.Tracked > 0 && obs.Healthy > 0,
		LoopAt:   obs.ObservedAt,
	}, true
}
