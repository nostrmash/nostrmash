package watchdog

import (
	"testing"
	"time"
)

func TestWorkerSample(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	loop := started.Add(time.Second)
	base := WorkerObservation{
		ProcessStarted: started,
		LastLoop:       loop,
		Now:            loop,
		ClaimObserved:  true,
		LastClaimOK:    true,
	}
	idle, ok := WorkerSample(base)
	if !ok || !idle.Idle || idle.Progress != "1700000000:0" || !idle.LoopAt.Equal(loop) {
		t.Fatalf("empty claim = %#v ok=%v", idle, ok)
	}
	backlog := base
	backlog.LastClaimCount = 3
	backlog.Inflight = 0
	busy, _ := WorkerSample(backlog)
	if busy.Idle {
		t.Fatal("a claim that returned jobs is not idle after the workers drain")
	}
	failed := base
	failed.ClaimFailed = true
	failed.LastClaimOK = false
	claimDown, _ := WorkerSample(failed)
	if claimDown.Idle {
		t.Fatal("claim failure must not be reported idle")
	}
	unobserved := base
	unobserved.ClaimObserved = false
	unobserved.LastClaimOK = false
	notYet, _ := WorkerSample(unobserved)
	if notYet.Idle {
		t.Fatal("no successful claim yet")
	}
	stalled := base
	stalled.Inflight = 1
	stalled.LastClaimCount = 1
	stalled.InflightSince = loop.Add(-6 * time.Minute)
	stalled.Now = loop
	stalledSample, _ := WorkerSample(stalled)
	if stalledSample.Idle {
		t.Fatal("stalled in-flight work must not be idle")
	}
	restart := base
	restart.ProcessStarted = started.Add(time.Hour)
	restart.Completed = 0
	restarted, _ := WorkerSample(restart)
	if restarted.Progress == idle.Progress {
		t.Fatal("restart must change the progress token")
	}
	if restarted.Idle != true {
		t.Fatal("restart onto an empty queue stays idle")
	}
	if _, sent := WorkerSample(WorkerObservation{}); sent {
		t.Fatal("a loop that has not iterated must not send")
	}
}

func TestIngestorSample(t *testing.T) {
	started := time.Unix(1_700_000_000, 0)
	observed := started.Add(30 * time.Second)
	quiet, ok := IngestorSample(IngestObservation{
		ProcessStarted: started,
		Valid:          4,
		Tracked:        2,
		Healthy:        1,
		HealthKnown:    true,
		ObservedAt:     observed,
	})
	if !ok || !quiet.Idle || quiet.Progress != "1700000000:4" || !quiet.LoopAt.Equal(observed) {
		t.Fatalf("quiet healthy = %#v ok=%v", quiet, ok)
	}
	down, _ := IngestorSample(IngestObservation{
		ProcessStarted: started,
		Tracked:        2,
		Healthy:        0,
		HealthKnown:    true,
		ObservedAt:     observed,
	})
	if down.Idle {
		t.Fatal("every configured relay disconnected")
	}
	none, _ := IngestorSample(IngestObservation{
		ProcessStarted: started,
		Tracked:        0,
		Healthy:        0,
		HealthKnown:    true,
		ObservedAt:     observed,
	})
	if none.Idle {
		t.Fatal("zero tracked relays is not a healthy idle ingestor")
	}
	if _, sent := IngestorSample(IngestObservation{ProcessStarted: started, ObservedAt: observed}); sent {
		t.Fatal("health is not known yet")
	}
}
