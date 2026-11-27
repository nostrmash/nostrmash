package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/relayintel"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func TestOptimizeGateRejectsWithoutQueueing(t *testing.T) {
	gate := newOptimizeGate(2)
	ctx := context.Background()
	if err := gate.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := gate.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := gate.acquire(ctx); err != errOptimizeOverloaded {
		t.Fatalf("third acquire = %v", err)
	}
	gate.release()
	if err := gate.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := gate.acquire(cancelled); err != context.Canceled {
		t.Fatalf("cancelled acquire = %v", err)
	}
}

func TestOptimizeAdmissionReleasesOnCancel(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay intel optimize gate"), "relaygate")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { relayintel.SetOptimizeHook(nil) })
	started := make(chan struct{})
	var once sync.Once
	relayintel.SetOptimizeHook(func(ctx context.Context) {
		once.Do(func() { close(started) })
		<-ctx.Done()
	})
	handlers := &Handlers{
		relayIntel:   relayintel.NewService(pool, relayintel.Options{}),
		optimizeGate: newOptimizeGate(1),
	}
	pubkey := strings.Repeat("ab", 32)
	body := []byte(`{"pubkey":"` + pubkey + `","preset":"balanced"}`)
	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstDone := make(chan int, 1)
	go func() {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/relays/optimize", bytes.NewReader(body)).WithContext(reqCtx)
		handlers.PostRelayOptimize(rec, req)
		firstDone <- rec.Code
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("optimization did not start")
	}
	second := httptest.NewRecorder()
	secondReq := httptest.NewRequest(http.MethodPost, "/api/v1/relays/optimize", bytes.NewReader(body))
	handlers.PostRelayOptimize(second, secondReq)
	if second.Code != http.StatusServiceUnavailable || !strings.Contains(second.Body.String(), `"overloaded"`) {
		t.Fatalf("overload = %d %s", second.Code, second.Body.String())
	}
	if second.Header().Get("Retry-After") != "1" {
		t.Fatalf("retry-after = %q", second.Header().Get("Retry-After"))
	}
	config := httptest.NewRecorder()
	configReq := httptest.NewRequest(http.MethodGet, "/api/v1/profiles/"+pubkey+"/relay-config", nil)
	configReq.SetPathValue("pubkey", pubkey)
	handlers.GetProfileRelayConfig(config, configReq)
	if config.Code != http.StatusOK {
		t.Fatalf("config while optimize is saturated = %d %s", config.Code, config.Body.String())
	}
	cancel()
	select {
	case code := <-firstDone:
		if code == http.StatusServiceUnavailable {
			t.Fatal("the admitted request was reported as overload")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled optimization did not release")
	}
	relayintel.SetOptimizeHook(nil)
	third := httptest.NewRecorder()
	thirdReq := httptest.NewRequest(http.MethodPost, "/api/v1/relays/optimize", bytes.NewReader(body))
	handlers.PostRelayOptimize(third, thirdReq)
	if third.Code != http.StatusOK {
		t.Fatalf("after release = %d %s", third.Code, third.Body.String())
	}
}
