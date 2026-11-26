//go:build relayintelbench

package api

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func measureOptimizeConcurrency(t *testing.T, ctx context.Context, pool *pgxpool.Pool, counter *queryCounter, people benchPeople) map[string]any {
	t.Helper()
	t.Log("concurrent optimization")
	base := newBenchHandlers(t, pool)
	scenarios := map[string]map[string]any{
		"dense_cohort": {
			"pubkey": people.Dense.pub, "preset": "balanced",
			"roles": []string{"public_inbox", "public_outbox"},
		},
		"max_lists": {
			"pubkey": people.Dense.pub, "preset": "decentralization",
			"roles": []string{"public_inbox", "public_outbox", "dm_inbox", "search", "keypackage"},
			"constraints": map[string]any{
				"max": map[string]int{"public_inbox": 8, "public_outbox": 8, "dm_inbox": 4, "search": 4, "keypackage": 4},
			},
		},
	}
	out := map[string]any{}
	for name, body := range scenarios {
		levels := map[string]any{}
		for _, workers := range []int{1, 4, 8} {
			levels[strconv.Itoa(workers)] = runOptimizeLevel(t, pool, counter, base, body, workers, 4)
		}
		out[name] = levels
	}
	out["note"] = "Each level runs four waves. A wave waits for every worker to finish one optimize request. Query counts and pool acquires are for the whole level."
	_ = ctx
	return out
}

func runOptimizeLevel(t *testing.T, pool *pgxpool.Pool, counter *queryCounter, base Handlers, body map[string]any, workers, waves int) map[string]any {
	t.Helper()
	stat0 := pool.Stat()
	q0 := counter.snapshot()
	cpu0 := cpuNow()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	samples := make([]time.Duration, 0, workers*waves)
	var mu sync.Mutex
	errors := 0
	started := time.Now()
	for wave := 0; wave < waves; wave++ {
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				one := time.Now()
				got := callPOST(base.PostRelayOptimize, body)
				elapsed := time.Since(one)
				ok := got.Status == 200 && planMeaningful(got.Body, true)
				mu.Lock()
				if ok {
					samples = append(samples, elapsed)
				} else {
					errors++
				}
				mu.Unlock()
			}()
		}
		wg.Wait()
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	stat1 := pool.Stat()
	summary := summarizeLatency("optimize concurrency", "not cached", pgCacheNote, samples, errors, len(samples), nil, time.Since(started), 0, cpuNow()-cpu0, "", nil)
	alloc := uint64(0)
	if len(samples) > 0 && after.TotalAlloc > before.TotalAlloc {
		alloc = (after.TotalAlloc - before.TotalAlloc) / uint64(len(samples))
	}
	return map[string]any{
		"workers": workers, "waves": waves, "meaningful": len(samples), "errors": errors,
		"wall_ms":   time.Since(started).Milliseconds(),
		"median_ms": summary.MedianMS, "p95_ms": summary.P95MS, "p99_ms": summary.P99MS, "max_ms": summary.MaxMS,
		"alloc_bytes_per_request": alloc,
		"cpu_ms":                  (cpuNow() - cpu0).Milliseconds(),
		"queries":                 counter.snapshot() - q0,
		"pool_acquires":           stat1.AcquireCount() - stat0.AcquireCount(),
		"pool_empty_acquires":     stat1.EmptyAcquireCount() - stat0.EmptyAcquireCount(),
	}
}
