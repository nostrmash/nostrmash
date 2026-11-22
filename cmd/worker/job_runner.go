package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nostrmash/nostrmash/internal/jobs"
	"github.com/nostrmash/nostrmash/internal/metrics"
	"github.com/nostrmash/nostrmash/internal/store/failure"
	"github.com/nostrmash/nostrmash/internal/traceutil"
	"github.com/nostrmash/nostrmash/internal/watchdog"
)

type workerQueue interface {
	ClaimAvailableForPool(ctx context.Context, workerID, workerPool string, limit int) ([]jobs.Job, error)
	CompleteJob(ctx context.Context, jobID int64, workerID string) error
	FailJob(ctx context.Context, jobID int64, workerID string, errMsg string, retryDelay time.Duration) (jobs.FailureResult, error)
	RecoverStaleRunningJobs(ctx context.Context, workerPool string, olderThan time.Time, limit int) (jobs.RecoveryResult, error)
	PurgeTerminalJobs(ctx context.Context, succeededBefore, deadBefore time.Time, limit int) (int64, error)
	WaitForWork(ctx context.Context, max time.Duration)
}

func runClaimLoop(
	ctx context.Context,
	log interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
	},
	queue workerQueue,
	workerID string,
	workerPool string,
	batchSize int,
	concurrency int,
	pollInterval time.Duration,
	retryDelay time.Duration,
	processJob func(jobCtx context.Context, job jobs.Job) error,
) {
	if concurrency <= 0 {
		concurrency = 1
	}
	var completed atomic.Int64
	var inflight atomic.Int64
	var inflightSince atomic.Int64
	var claimFailed atomic.Bool
	var claimObserved atomic.Bool
	var lastClaimOK atomic.Bool
	var lastClaimCount atomic.Int64
	var lastLoop atomic.Int64
	processStarted := time.Now()
	if beat := watchdog.FromEnv(); beat != nil {
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					loopAt := lastLoop.Load()
					if loopAt == 0 {
						continue
					}
					since := inflightSince.Load()
					var inflightAt time.Time
					if since != 0 {
						inflightAt = time.UnixMilli(since)
					}
					sample, send := watchdog.WorkerSample(watchdog.WorkerObservation{
						ProcessStarted: processStarted,
						Completed:      completed.Load(),
						Inflight:       inflight.Load(),
						ClaimFailed:    claimFailed.Load(),
						LastLoop:       time.UnixMilli(loopAt),
						ClaimObserved:  claimObserved.Load(),
						LastClaimOK:    lastClaimOK.Load(),
						LastClaimCount: int(lastClaimCount.Load()),
						InflightSince:  inflightAt,
						Now:            time.Now(),
					})
					if send {
						beat.Notify(ctx, sample)
					}
				}
			}
		}()
	}
	jobQueue := make(chan jobs.Job, batchSize*concurrency)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobQueue {
				func() {
					defer doneInflight(&inflight, &inflightSince)
					// Drain in-flight jobs even after shutdown signal.
					workerCtx := context.WithoutCancel(ctx)
					spanCtx, span := traceutil.StartSpan(workerCtx, "worker.job.execute",
						traceutil.KV("job.type", job.JobType),
					)
					started := time.Now()
					err := runJobWithRecovery(spanCtx, job, processJob)
					if err == nil {
						if completeErr := queue.CompleteJob(spanCtx, job.ID, workerID); completeErr != nil {
							class := failure.ClassifyError(completeErr)
							metrics.ObserveWorkerJobExecution(job.JobType, "complete_error", time.Since(started))
							span.End(completeErr)
							log.Error("job_complete_failed", "job_id", job.ID, "failure_class", class.Class, "failure_reason", class.Reason, "error", completeErr)
							return
						}
						metrics.IncWorkerJob(job.JobType, "succeeded")
						metrics.ObserveWorkerJobExecution(job.JobType, "succeeded", time.Since(started))
						completed.Add(1)
						span.End(nil)
						log.Info("job_completed", "job_id", job.ID, "job_type", job.JobType)
						return
					}

					// Scale the retry delay with how often this job has already
					// failed, so persistent failures back off instead of
					// hammering the queue in lockstep every fixed interval.
					backoff := jobs.RetryDelay(job.Attempts, retryDelay, jobs.DefaultMaxRetryDelay)
					failState, failErr := queue.FailJob(spanCtx, job.ID, workerID, err.Error(), backoff)
					if failErr != nil {
						class := failure.ClassifyError(failErr)
						metrics.ObserveWorkerJobExecution(job.JobType, "fail_mark_error", time.Since(started))
						span.End(failErr)
						log.Error("job_fail_mark_failed", "job_id", job.ID, "failure_class", class.Class, "failure_reason", class.Reason, "error", failErr)
						return
					}
					errClass := failure.ClassifyError(err)
					if failState.Status == jobs.StatusDead {
						metrics.IncWorkerJob(job.JobType, "dead")
						metrics.ObserveWorkerJobExecution(job.JobType, "dead", time.Since(started))
						span.End(err)
						log.Error(
							"job_dead_lettered",
							"job_id", job.ID,
							"job_type", job.JobType,
							"failure_class", errClass.Class,
							"failure_reason", errClass.Reason,
							"attempts", failState.Attempts,
							"max_attempts", failState.MaxAttempts,
							"error", err,
						)
						return
					}
					metrics.IncWorkerJob(job.JobType, "retry")
					metrics.ObserveWorkerJobExecution(job.JobType, "retry", time.Since(started))
					span.End(err)
					log.Error(
						"job_failed_retry_scheduled",
						"job_id", job.ID,
						"job_type", job.JobType,
						"failure_class", errClass.Class,
						"failure_reason", errClass.Reason,
						"attempts", failState.Attempts,
						"max_attempts", failState.MaxAttempts,
						"retry_after", backoff.String(),
						"error", err,
					)
				}()
			}
		}()
	}
	defer func() {
		close(jobQueue)
		wg.Wait()
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		lastLoop.Store(time.Now().UnixMilli())
		claimCtx, claimSpan := traceutil.StartSpan(ctx, "worker.queue.claim_available")
		claimed, err := queue.ClaimAvailableForPool(claimCtx, workerID, workerPool, batchSize)
		claimSpan.End(err)
		if err != nil {
			claimObserved.Store(true)
			claimFailed.Store(true)
			lastClaimOK.Store(false)
			log.Error("job_claim_failed", "error", err)
			queue.WaitForWork(ctx, pollInterval)
			continue
		}
		claimObserved.Store(true)
		claimFailed.Store(false)
		lastClaimOK.Store(true)
		lastClaimCount.Store(int64(len(claimed)))
		if len(claimed) == 0 {
			// Wake on LISTEN/NOTIFY (or the poll-interval safety net) instead
			// of always sleeping a full second after an empty claim.
			queue.WaitForWork(ctx, pollInterval)
			continue
		}

		for _, job := range claimed {
			select {
			case <-ctx.Done():
				return
			case jobQueue <- job:
				addInflight(&inflight, &inflightSince)
			}
		}
	}
}

func addInflight(inflight, since *atomic.Int64) {
	if inflight.Add(1) == 1 {
		since.CompareAndSwap(0, time.Now().UnixMilli())
	}
}

func doneInflight(inflight, since *atomic.Int64) {
	if inflight.Add(-1) == 0 {
		since.Store(0)
	}
}

func runJobWithRecovery(ctx context.Context, job jobs.Job, processJob func(jobCtx context.Context, job jobs.Job) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = failure.FromPanic(recovered)
		}
	}()
	return processJob(ctx, job)
}
