//go:build relayintelbench

package relayintel

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ClearSyntheticCaches drops process-local synthetic identity caches so a
// benchmark can measure a cold lookup. It does not change the database.
func ClearSyntheticCaches() {
	syntheticPubkeys.Clear()
	syntheticMisses.Clear()
	syntheticMissCount.Store(0)
}

// RegisterSyntheticIdentity records a probe pubkey through the production
// registry path, including the shared epoch bump.
func RegisterSyntheticIdentity(ctx context.Context, pool *pgxpool.Pool, pubkey string) error {
	return registerSyntheticIdentity(ctx, pool, pubkey)
}

// BenchObservation is one capability sample on the worker completion path.
type BenchObservation struct {
	Capability      string
	Status          string
	Detail          string
	Confidence      float64
	EvidenceVersion int
	Retrieved       *bool
	Unauthenticated *bool
	Authenticated   *bool
	ObservedAt      time.Time
}

// BenchCompletion is the worker completion record. It is passed to the same
// functions the probe workers call after a local collection. It does not dial.
type BenchCompletion struct {
	AttemptStatus    string
	FollowupID       int64
	FollowStatus     string
	FollowResult     string
	Elapsed          int
	Endpoint         string
	FollowCapability string
	Metric           string
	ObservedAt       time.Time
	Writes           []BenchObservation
	RetentionURL     string
	RetentionEventID string
}

func benchCompletion(in BenchCompletion) probeCompletion {
	writes := make([]observationWrite, len(in.Writes))
	for i, write := range in.Writes {
		writes[i] = observationWrite{
			Capability:      write.Capability,
			Status:          write.Status,
			Detail:          write.Detail,
			Confidence:      write.Confidence,
			EvidenceVersion: write.EvidenceVersion,
			Retrieved:       write.Retrieved,
			Unauthenticated: write.Unauthenticated,
			Authenticated:   write.Authenticated,
			ObservedAt:      write.ObservedAt,
		}
	}
	var retention *scheduledRetention
	if in.RetentionEventID != "" {
		retention = &scheduledRetention{
			RelayURL:  in.RetentionURL,
			EventID:   in.RetentionEventID,
			Published: time.Now().UTC(),
		}
	}
	return probeCompletion{
		AttemptStatus:    in.AttemptStatus,
		FollowupID:       in.FollowupID,
		FollowStatus:     in.FollowStatus,
		FollowResult:     in.FollowResult,
		Elapsed:          in.Elapsed,
		Endpoint:         in.Endpoint,
		FollowCapability: in.FollowCapability,
		Writes:           writes,
		Retention:        retention,
		Metric:           in.Metric,
		ObservedAt:       in.ObservedAt,
	}
}

// BenchPublishProbe runs publishProbe, the service path for public-write and
// search completion. An identical replay returns nil. A failed commit releases
// the owned attempt. This does not dial a relay.
func BenchPublishProbe(ctx context.Context, pool *pgxpool.Pool, metric, key, token string, completion BenchCompletion) error {
	state := &probeState{worker: "bench"}
	return state.publishProbe(ctx, pool, metric, key, token, benchCompletion(completion))
}

// BenchCommitProbe runs commitProbeResult, the atomic completion used by
// gift-wrap storage and by publishProbe. outcome is committed,
// already_completed, or error. This does not dial a relay and does not
// release the attempt on failure; the caller can observe the rejection.
func BenchCommitProbe(ctx context.Context, pool *pgxpool.Pool, key, token string, completion BenchCompletion) (string, error) {
	err := commitProbeResult(ctx, pool, key, token, benchCompletion(completion))
	if errors.Is(err, errProbeAlreadyCompleted) {
		return "already_completed", nil
	}
	if err != nil {
		return "error", err
	}
	return "committed", nil
}

// BenchDigestCost times resultDigest, the hash commitProbeResult stores on
// the attempt. It does not write a row and it does not dial a relay.
func BenchDigestCost(attemptID int64, urlKey, capability string, completion BenchCompletion, iterations int) (string, time.Duration) {
	if iterations < 1 {
		iterations = 1
	}
	mapped := benchCompletion(completion)
	started := time.Now()
	digest := ""
	for i := 0; i < iterations; i++ {
		digest = resultDigest(attemptID, urlKey, capability, mapped)
	}
	return digest, time.Since(started)
}
