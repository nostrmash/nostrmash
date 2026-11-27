//go:build relayintelbench

package api

import (
	"context"
	"net/http"
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
	limit := 0
	if base.optimizeGate != nil {
		limit = cap(base.optimizeGate.slots)
	}
	if limit != 4 {
		t.Fatalf("optimize gate limit = %d, want 4 from NewHandlersWithOptions", limit)
	}
	out := map[string]any{"gate_limit": limit, "constructor": "NewHandlersWithOptions"}
	for name, body := range scenarios {
		levels := map[string]any{}
		for _, workers := range []int{1, 4, 8, 16} {
			waves := 4
			if workers == 16 {
				waves = 2
			}
			levels[strconv.Itoa(workers)] = runOptimizeLevel(t, pool, counter, base, body, workers, waves)
		}
		out[name] = levels
	}
	out["overload"] = measureOptimizeOverload(t, ctx, pool, counter, base, scenarios["max_lists"])
	out["note"] = "The handler gate allows 4 optimizations. Levels 1, 4, and 8 run four waves. Level 16 runs two waves. HTTP 503 with error code overloaded is deliberate rejection and is not completed throughput. HTTP 429 rate_limited is the ordinary client limiter. Rankings and configuration run during the 16-request overload."
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
	statuses := map[string]int{}
	var mu sync.Mutex
	completed, overloaded, rateLimited, nonmeaningful, unexpected := 0, 0, 0, 0, 0
	started := time.Now()
	for wave := 0; wave < waves; wave++ {
		var wg sync.WaitGroup
		start := make(chan struct{})
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				one := time.Now()
				got := callPOST(base.PostRelayOptimize, body)
				elapsed := time.Since(one)
				class := classifyOptimize(got)
				mu.Lock()
				statuses[strconv.Itoa(got.Status)]++
				switch class {
				case "completed":
					samples = append(samples, elapsed)
					completed++
				case "overloaded":
					overloaded++
				case "rate_limited":
					rateLimited++
				case "nonmeaningful":
					nonmeaningful++
				default:
					unexpected++
				}
				mu.Unlock()
			}()
		}
		close(start)
		wg.Wait()
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	stat1 := pool.Stat()
	summary := summarizeLatency("optimize concurrency", "not cached", pgCacheNote, samples, unexpected+nonmeaningful, completed, nil, time.Since(started), 0, cpuNow()-cpu0, "", nil)
	alloc := uint64(0)
	if completed > 0 && after.TotalAlloc > before.TotalAlloc {
		alloc = (after.TotalAlloc - before.TotalAlloc) / uint64(completed)
	}
	return map[string]any{
		"workers": workers, "waves": waves,
		"completed": completed, "overloaded": overloaded, "rate_limited": rateLimited,
		"nonmeaningful": nonmeaningful, "unexpected_errors": unexpected,
		"in_flight_after": base.optimizeGate.inFlight.Load(),
		"status_codes":    statuses,
		"wall_ms":         time.Since(started).Milliseconds(),
		"median_ms":       summary.MedianMS, "p95_ms": summary.P95MS, "p99_ms": summary.P99MS, "max_ms": summary.MaxMS,
		"alloc_bytes_per_request": alloc,
		"cpu_ms":                  (cpuNow() - cpu0).Milliseconds(),
		"queries":                 counter.snapshot() - q0,
		"pool_acquires":           stat1.AcquireCount() - stat0.AcquireCount(),
		"pool_empty_acquires":     stat1.EmptyAcquireCount() - stat0.EmptyAcquireCount(),
		"pool_acquired_after":     stat1.AcquiredConns(),
	}
}

func classifyOptimize(got httpResult) string {
	code := optimizeErrorCode(got.Body)
	switch {
	case got.Status == http.StatusServiceUnavailable && code == "overloaded":
		return "overloaded"
	case got.Status == http.StatusTooManyRequests && code == "rate_limited":
		return "rate_limited"
	case got.Status == http.StatusOK && planMeaningful(got.Body, true):
		return "completed"
	case got.Status == http.StatusOK:
		return "nonmeaningful"
	default:
		return "unexpected"
	}
}

func classifyCanceledOptimize(got httpResult, ctxErr error) string {
	class := classifyOptimize(got)
	if ctxErr != nil && class != "completed" && class != "overloaded" && class != "rate_limited" {
		return "canceled"
	}
	return class
}

func optimizeErrorCode(body map[string]any) string {
	if body == nil {
		return ""
	}
	errBody, _ := body["error"].(map[string]any)
	code, _ := errBody["code"].(string)
	return code
}

func measureOptimizeOverload(t *testing.T, ctx context.Context, pool *pgxpool.Pool, counter *queryCounter, base Handlers, body map[string]any) map[string]any {
	t.Helper()
	stop := make(chan struct{})
	var sideMu sync.Mutex
	var rankingSamples, configSamples []time.Duration
	rankingStatus := map[string]int{}
	configStatus := map[string]int{}
	var sideWG sync.WaitGroup
	sideWG.Add(1)
	go func() {
		defer sideWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			one := time.Now()
			got := callGET(base.GetRelayRankings, "/api/v1/relays/rankings?role=public_inbox&limit=20", "")
			sideMu.Lock()
			rankingStatus[strconv.Itoa(got.Status)]++
			if got.Status == http.StatusOK {
				rankingSamples = append(rankingSamples, time.Since(one))
			}
			sideMu.Unlock()
			one = time.Now()
			got = callGET(base.GetProfileRelayConfig, "/api/v1/profiles/"+bodyPubkey(body)+"/relay-config", bodyPubkey(body))
			sideMu.Lock()
			configStatus[strconv.Itoa(got.Status)]++
			if got.Status == http.StatusOK {
				configSamples = append(configSamples, time.Since(one))
			}
			sideMu.Unlock()
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	level := runOptimizeLevel(t, pool, counter, base, body, 16, 2)
	close(stop)
	sideWG.Wait()
	sideMu.Lock()
	ranking := summarizeLatency("rankings during overload", "cached", pgCacheNote, rankingSamples, 0, len(rankingSamples), nil, 0, 0, 0, "", nil)
	config := summarizeLatency("config during overload", "cached", pgCacheNote, configSamples, 0, len(configSamples), nil, 0, 0, 0, "", nil)
	sideMu.Unlock()

	acquiredBefore := pool.Stat().AcquiredConns()
	cancelCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	cancelStart := time.Now()
	canceled := callPOSTContext(cancelCtx, base.PostRelayOptimize, body)
	cancelWall := time.Since(cancelStart)
	cancel()
	follow := callPOST(base.PostRelayOptimize, body)
	rankingAfter := callGET(base.GetRelayRankings, "/api/v1/relays/rankings?role=public_inbox&limit=20", "")
	invalid := callPOST(base.PostRelayOptimize, map[string]any{"pubkey": "not-a-pubkey", "preset": "balanced"})
	afterInvalid := callPOST(base.PostRelayOptimize, body)
	overloaded, _ := level["overloaded"].(int)
	note := "Completed requests are HTTP 200 responses with a plan. HTTP 503 overloaded is deliberate gate rejection. HTTP 429 rate_limited is a different limiter."
	if overloaded == 0 {
		note = "No HTTP 503 overloaded response was observed at 16 concurrent requests."
	}
	return map[string]any{
		"workers": 16, "waves": 2, "scenario": "max_lists",
		"requests": level, "rankings_during": map[string]any{
			"samples": len(rankingSamples), "status_codes": rankingStatus,
			"median_ms": ranking.MedianMS, "p95_ms": ranking.P95MS,
		},
		"config_during": map[string]any{
			"samples": len(configSamples), "status_codes": configStatus,
			"median_ms": config.MedianMS, "p95_ms": config.P95MS,
		},
		"canceled_request": map[string]any{
			"status": canceled.Status, "error_code": optimizeErrorCode(canceled.Body),
			"wall_ms": cancelWall.Milliseconds(),
			"class":   classifyCanceledOptimize(canceled, cancelCtx.Err()),
			"note":    "The request context is canceled after 30 ms. A completed plan means the handler finished despite cancellation. overloaded means the gate rejected it.",
		},
		"after_cancel": map[string]any{
			"optimize_status": follow.Status, "optimize_class": classifyOptimize(follow),
			"rankings_status":             rankingAfter.Status,
			"pool_acquired_before_cancel": acquiredBefore,
			"pool_acquired_after":         pool.Stat().AcquiredConns(),
			"in_flight_after":             base.optimizeGate.inFlight.Load(),
		},
		"after_invalid": map[string]any{
			"invalid_status": invalid.Status, "invalid_code": optimizeErrorCode(invalid.Body),
			"following_class": classifyOptimize(afterInvalid),
			"in_flight_after": base.optimizeGate.inFlight.Load(),
		},
		"note": note,
	}
}

func bodyPubkey(body map[string]any) string {
	pubkey, _ := body["pubkey"].(string)
	return pubkey
}
