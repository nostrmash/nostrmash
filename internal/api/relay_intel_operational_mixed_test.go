//go:build relayintelbench

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/derivation"
	"github.com/nostrmash/nostrmash/internal/jobs"
	"github.com/nostrmash/nostrmash/internal/model"
	"github.com/nostrmash/nostrmash/internal/relayintel"
	"github.com/nostrmash/nostrmash/internal/store"
	"github.com/nostrmash/nostrmash/internal/store/retention"
)

func measureMixed(t *testing.T, ctx context.Context, pool *pgxpool.Pool, counter *queryCounter, people benchPeople, report *opReport) {
	t.Helper()
	drained := drainJobs(t, ctx, pool, "bench-mixed-prep", 5000)
	report.note(fmt.Sprintf("mixed workload prep drained %d queued jobs in %s so the comparison starts from an empty claimable queue", drained.Processed, drained.Elapsed.Round(time.Millisecond)))
	events := operationalIntEnv("RELAY_INTEL_OPERATIONAL_MIXED_EVENTS", 200)
	repeats := operationalIntEnv("RELAY_INTEL_OPERATIONAL_MIXED_REPEATS", 3)
	windowMS := operationalIntEnv("RELAY_INTEL_OPERATIONAL_MIXED_WINDOW_MS", 30000)
	report.note(fmt.Sprintf("sustained mixed workload offers %d events paced across %d ms, repeated %d times for baseline, relay intelligence without a full statistics refresh, and relay intelligence with one forced refresh", events, windowMS, repeats))
	modes := []string{"baseline", "relay_intel_on", "stats_refresh"}
	runs := make([]any, 0, repeats*len(modes))
	for pass := 1; pass <= repeats; pass++ {
		for _, mode := range modes {
			runs = append(runs, runMixedPass(t, ctx, pool, counter, people, events, windowMS, mode, pass))
		}
	}
	burstRepeats := operationalIntEnv("RELAY_INTEL_OPERATIONAL_BURST_REPEATS", 2)
	if burstRepeats > 0 {
		report.note(fmt.Sprintf("burst mixed workload offers the same %d events with no pacing, repeated %d times for baseline and relay intelligence. It is a capacity comparison. It is not a maximum sustainable throughput.", events, burstRepeats))
		for pass := 1; pass <= burstRepeats; pass++ {
			runs = append(runs, runMixedPass(t, ctx, pool, counter, people, events, 0, "burst_baseline", pass))
			runs = append(runs, runMixedPass(t, ctx, pool, counter, people, events, 0, "burst_relay_intel_on", pass))
		}
	}
	report.mu.Lock()
	report.Mixed = runs
	report.mu.Unlock()
	report.save()
}

func runMixedPass(t *testing.T, ctx context.Context, pool *pgxpool.Pool, counter *queryCounter, people benchPeople, events, windowMS int, mode string, pass int) map[string]any {
	t.Helper()
	t.Logf("mixed %s pass %d events %d window %dms", mode, pass, events, windowMS)
	relayOn := mode != "baseline" && mode != "burst_baseline"
	paced := windowMS > 0
	relayintel.SetProjectionEnabled(relayOn)
	defer relayintel.SetProjectionEnabled(true)
	handlers := newBenchHandlers(t, pool)
	queue := jobs.NewQueue(pool)
	deriveHandlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	keys := make([]testKey, 16)
	for i := range keys {
		keys[i] = newKey(t)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ingestOK, ingestErr, deriveOK, deriveErr atomicInt
	var apiErr, riErr atomicInt
	var phase atomic.Int32
	type phaseSamples struct {
		api []time.Duration
		ri  []time.Duration
	}
	var phases [3]phaseSamples
	var sampleMu sync.Mutex
	record := func(kind string, elapsed time.Duration, ok bool) {
		sampleMu.Lock()
		defer sampleMu.Unlock()
		slot := int(phase.Load())
		if slot < 0 || slot > 2 {
			slot = 0
		}
		if kind == "api" {
			if ok {
				phases[slot].api = append(phases[slot].api, elapsed)
			} else {
				apiErr.add()
			}
			return
		}
		if ok {
			phases[slot].ri = append(phases[slot].ri, elapsed)
		} else {
			riErr.add()
		}
	}
	wal0 := walLSN(ctx, pool)
	q0 := counter.snapshot()
	stat0 := pool.Stat()
	db0 := dbCounters(ctx, pool)
	cpu0 := cpuNow()
	mem0 := runtimeMem()
	started := time.Now()
	deadline := started.Add(time.Duration(windowMS) * time.Millisecond)

	var maintenanceCalls atomicInt
	var lockWaitSamples atomicInt
	var maxLockWaiters atomicInt
	var refreshSkipped string
	var pendingMax atomicInt
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = relayintel.RepairCanonicalBatch(ctx, pool, 50)
			if relayOn {
				_ = relayintel.BackfillBatch(ctx, pool, 200)
				if mode != "stats_refresh" {
					err := relayintel.RefreshStats(ctx, pool, "", 10*time.Minute)
					if err != nil && refreshSkipped == "" {
						refreshSkipped = err.Error()
					}
				}
			}
			_, _ = retention.New(pool).PurgeSupersededReplaceableEvents(ctx, time.Now().Add(-time.Hour), time.Now().Add(-time.Hour), 25)
			var waiting, pending int
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&waiting)
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = 'pending'`).Scan(&pending)
			if waiting > 0 {
				lockWaitSamples.add()
			}
			maxLockWaiters.max(waiting)
			pendingMax.max(pending)
			maintenanceCalls.add()
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()

	var refreshWall time.Duration
	var refreshErr string
	var computedBefore, computedAfter time.Time
	var refreshDuration int64
	var refreshConfirmed bool
	if mode == "stats_refresh" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-time.After(2 * time.Second):
			case <-stop:
				return
			}
			_ = pool.QueryRow(ctx, `SELECT computed_at FROM relay_intel_snapshot WHERE id`).Scan(&computedBefore)
			phase.Store(1)
			one := time.Now()
			err := relayintel.RefreshStats(ctx, pool, "", 0)
			refreshWall = time.Since(one)
			if err != nil {
				refreshErr = err.Error()
			}
			phase.Store(2)
			_ = pool.QueryRow(ctx, `SELECT computed_at, last_duration_ms FROM relay_intel_snapshot WHERE id`).Scan(&computedAfter, &refreshDuration)
			refreshConfirmed = err == nil && computedAfter.After(computedBefore) && refreshDuration > 0
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			one := time.Now()
			got := callGET(handlers.GetProfileByPubkey, "/api/v1/profiles/"+people.Dense.pub, people.Dense.pub)
			record("api", time.Since(one), got.Status == http.StatusOK)
		}
	}()
	if relayOn {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				one := time.Now()
				got := callGET(handlers.GetProfileRelayRecommendations, "/api/v1/profiles/"+people.Dense.pub+"/relay-recommendations?preset=balanced", people.Dense.pub)
				record("ri", time.Since(one), got.Status == http.StatusOK && planMeaningful(got.Body, true))
				one = time.Now()
				got = callPOST(handlers.PostRelayOptimize, map[string]any{
					"pubkey": people.Sparse.pub, "preset": "balanced", "roles": []string{"public_inbox", "public_outbox"},
				})
				record("ri", time.Since(one), got.Status == http.StatusOK && planMeaningful(got.Body, true))
			}
		}()
	}

	perWorker := events / 4
	if perWorker < 1 {
		perWorker = 1
	}
	var gap time.Duration
	if paced {
		gap = time.Duration(windowMS) * time.Millisecond / time.Duration(perWorker)
	}
	ingestDone := make(chan struct{})
	var ingestWG sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		ingestWG.Add(1)
		go func(n int) {
			defer ingestWG.Done()
			for i := 0; i < perWorker; i++ {
				if paced && time.Now().After(deadline) {
					return
				}
				key := keys[(n+i)%len(keys)]
				kind := 1
				content := fmt.Sprintf("mixed %s %d %d %d", mode, pass, n, i)
				tags := [][]string{{"t", "bench"}}
				if i%8 == 0 {
					kind = relayintel.KindPublicRelayList
					content = ""
					tags = [][]string{{"r", "wss://r1.op1.example", "read"}, {"r", "wss://r2.op2.example", "write"}}
				}
				createdAt := time.Now().Unix() + int64(pass*1_000_000+n*1000+i)
				raw := sign(t, key.priv, kind, createdAt, tags, content)
				var body struct {
					ID string `json:"id"`
				}
				if err := json.Unmarshal(raw, &body); err != nil {
					ingestErr.add()
					continue
				}
				event := model.Event{
					ID: body.ID, Pubkey: key.pub, CreatedAt: createdAt, Kind: kind,
					Sig: strings.Repeat("ab", 64), Content: content, RawJSON: raw,
				}
				if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://r1.op1.example", time.Now().UTC()); err != nil {
					ingestErr.add()
					continue
				}
				ingestOK.add()
				if paced && i+1 < perWorker {
					select {
					case <-time.After(gap):
					case <-stop:
						return
					}
				}
			}
		}(worker)
	}
	var ingestWall time.Duration
	go func() {
		ingestWG.Wait()
		ingestWall = time.Since(started)
		close(ingestDone)
	}()

	deriveStop := make(chan struct{})
	var deriveWG sync.WaitGroup
	for worker := 0; worker < 2; worker++ {
		deriveWG.Add(1)
		go func(n int) {
			defer deriveWG.Done()
			id := fmt.Sprintf("bench-mixed-%s-%d-%d", mode, pass, n)
			for {
				select {
				case <-deriveStop:
					return
				default:
				}
				claimed, err := queue.ClaimAvailable(ctx, id, 10)
				if err != nil {
					deriveErr.add()
					return
				}
				if len(claimed) == 0 {
					select {
					case <-ingestDone:
						claimed, err = queue.ClaimAvailable(ctx, id, 10)
						if err != nil || len(claimed) == 0 {
							return
						}
					case <-deriveStop:
						return
					case <-time.After(20 * time.Millisecond):
						continue
					}
				}
				for _, job := range claimed {
					err := derivation.ProcessJob(ctx, deriveHandlers, derivation.Job{JobType: job.JobType, Payload: job.Payload})
					if err != nil {
						deriveErr.add()
						_, _ = queue.FailJob(ctx, job.ID, id, err.Error(), time.Second)
						continue
					}
					if err := queue.CompleteJob(ctx, job.ID, id); err != nil {
						deriveErr.add()
						continue
					}
					deriveOK.add()
				}
			}
		}(worker)
	}
	<-ingestDone
	drainStarted := time.Now()
	drained := make(chan struct{})
	go func() {
		deriveWG.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(3 * time.Minute):
		t.Logf("mixed %s pass %d derivation drain exceeded 3 minutes", mode, pass)
	}
	drainWall := time.Since(drainStarted)
	close(deriveStop)
	deriveWG.Wait()
	close(stop)
	wg.Wait()
	elapsed := time.Since(started)
	var pending int64
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status = 'pending'`).Scan(&pending)
	sampleMu.Lock()
	copied := phases
	sampleMu.Unlock()
	stat1 := pool.Stat()
	result := map[string]any{
		"scenario": mode, "pass": pass, "events_attempted": perWorker * 4,
		"paced": paced, "offered_window_ms": windowMS,
		"ingest_ok": ingestOK.value(), "ingest_errors": ingestErr.value(),
		"derive_ok": deriveOK.value(), "derive_errors": deriveErr.value(),
		"wall_ms": elapsed.Milliseconds(), "ingest_wall_ms": ingestWall.Milliseconds(),
		"ingest_per_s":       float64(ingestOK.value()) / ingestWall.Seconds(),
		"derive_per_s":       float64(deriveOK.value()) / elapsed.Seconds(),
		"queue_drain_ms":     drainWall.Milliseconds(),
		"pending_jobs_after": pending, "max_pending_jobs": pendingMax.value(),
		"maintenance_cycles":  maintenanceCalls.value(),
		"lock_wait_samples":   lockWaitSamples.value(),
		"max_lock_waiters":    maxLockWaiters.value(),
		"wal_bytes":           walBytes(ctx, pool, wal0, walLSN(ctx, pool)),
		"queries":             counter.snapshot() - q0,
		"pool_empty_acquires": stat1.EmptyAcquireCount() - stat0.EmptyAcquireCount(),
		"pool_acquires":       stat1.AcquireCount() - stat0.AcquireCount(),
		"pool_max_conns":      stat1.MaxConns(),
		"database":            dbDelta(db0, dbCounters(ctx, pool)),
		"process_cpu_ms":      (cpuNow() - cpu0).Milliseconds(),
		"heap_alloc_bytes":    runtimeMem() - mem0,
		"projection_enabled":  relayOn, "canonical_repair_enabled": true,
		"refresh_interval_result": refreshSkipped,
	}
	labels := []string{"before", "during", "after"}
	for i, label := range labels {
		if len(copied[i].api) == 0 && len(copied[i].ri) == 0 && mode != "stats_refresh" && label != "before" {
			continue
		}
		result["existing_api_"+label] = summarizeLatency("profile "+label, "handler", pgCacheNote, copied[i].api, 0, len(copied[i].api), nil, elapsed, 0, 0, "", nil)
		if relayOn {
			result["relay_intel_api_"+label] = summarizeLatency("relay intel "+label, "warm-or-miss", pgCacheNote, copied[i].ri, 0, len(copied[i].ri), nil, elapsed, 0, 0, "", nil)
		}
	}
	result["existing_api_errors"] = apiErr.value()
	result["relay_intel_api_errors"] = riErr.value()
	if mode == "stats_refresh" {
		result["statistics_refresh"] = map[string]any{
			"wall_ms": refreshWall.Milliseconds(), "last_duration_ms": refreshDuration,
			"computed_at_before": computedBefore.UTC().Format(time.RFC3339Nano),
			"computed_at_after":  computedAfter.UTC().Format(time.RFC3339Nano),
			"confirmed":          refreshConfirmed, "error": refreshErr,
			"note": "Interval 0 forces RefreshStats. confirmed requires a newer snapshot timestamp and a positive last_duration_ms. Latency is split into before, during, and after that call.",
		}
	}
	return result
}

func runtimeMem() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.Alloc
}
