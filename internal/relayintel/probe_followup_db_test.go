package relayintel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nostrmash/nostrmash/internal/dbmigrate"
	"github.com/nostrmash/nostrmash/internal/testutil/dbtest"
)

func TestRetentionFollowupIsBudgetedAndRecoverable(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay follow-up"), "probefollow")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	key := newKey(t)
	state := &probeState{
		key: key, pubkey: pubOf(key), worker: "worker-a", allowPrivate: true,
		globalLimit: 20, minInterval: time.Second, allowlist: []string{"follow.example"},
	}
	eventID := strings.Repeat("ab", 32)
	published := time.Now().Add(-2 * time.Hour)
	state.scheduleRetention(ctx, pool, "wss://follow.example", "wss://follow.example", eventID, published)
	state.scheduleRetention(ctx, pool, "wss://follow.example", "wss://follow.example", eventID, published)
	var scheduled int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_followups WHERE event_id = $1`, eventID).Scan(&scheduled); err != nil {
		t.Fatal(err)
	}
	if scheduled != 1 {
		t.Fatalf("scheduled %d follow-ups", scheduled)
	}

	state.allowlist = nil
	var ran atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := state.followOne(ctx, pool)
			if err != nil {
				t.Error(err)
				return
			}
			if ok {
				ran.Add(1)
			}
		}()
	}
	wg.Wait()
	if ran.Load() != 1 {
		t.Fatalf("claims = %d", ran.Load())
	}
	var status, result string
	if err := pool.QueryRow(ctx, `SELECT status, result FROM relay_probe_followups WHERE event_id = $1`, eventID).Scan(&status, &result); err != nil {
		t.Fatal(err)
	}
	if status != "skipped" || result != "allowlist_removed" {
		t.Fatalf("status=%s result=%s", status, result)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_followups (url_key, endpoint_url, event_id, capability, check_after, published_at, status)
		VALUES ('wss://127.0.0.1:1', 'wss://127.0.0.1:1', $1, 'retention', now() - interval '1 minute', now() - interval '2 hours', 'claimed')
	`, strings.Repeat("cd", 32)); err != nil {
		t.Fatal(err)
	}
	state.allowlist = []string{"127.0.0.1"}
	ok, err := state.followOne(ctx, pool)
	if err != nil || !ok {
		t.Fatalf("recovery ok=%v err=%v", ok, err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT status, finished_at IS NULL FROM relay_probe_followups WHERE event_id = $1
	`, strings.Repeat("cd", 32)).Scan(&status, &ok); err != nil {
		t.Fatal(err)
	}
	if status != "retryable" || !ok {
		t.Fatalf("crashed follow-up status=%s open=%v", status, ok)
	}

	state.globalLimit = 1
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_attempts (url_key, capability, host, worker_id, lease_token, started_at)
		VALUES ('wss://budget.example', 'retention', 'budget.example', 'worker-a', 'budget-token', now())
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO relay_probe_followups (url_key, endpoint_url, event_id, capability, check_after, published_at, status)
		VALUES ('wss://budget.example', 'wss://budget.example', $1, 'retention', now() - interval '1 minute', now() - interval '2 hours', 'pending')
	`, strings.Repeat("ef", 32)); err != nil {
		t.Fatal(err)
	}
	state.allowlist = []string{"budget.example"}
	ok, err = state.followOne(ctx, pool)
	if err != nil || ok {
		t.Fatalf("budget exhaustion ok=%v err=%v", ok, err)
	}
	var delayed bool
	if err := pool.QueryRow(ctx, `
		SELECT status = 'pending' AND check_after > now() FROM relay_probe_followups WHERE event_id = $1
	`, strings.Repeat("ef", 32)).Scan(&delayed); err != nil || !delayed {
		t.Fatalf("delayed=%v err=%v", delayed, err)
	}
}

func TestRetentionFollowupRecordsTheActualInterval(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay follow-up interval"), "probefollow2")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	key := newKey(t)
	event, err := signProbeEvent(key, pubOf(key))
	if err != nil {
		t.Fatal(err)
	}
	eventID, _ := event["id"].(string)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var frame []json.RawMessage
			if json.Unmarshal(payload, &frame) != nil || len(frame) < 2 {
				continue
			}
			var typ, sub string
			_ = json.Unmarshal(frame[0], &typ)
			_ = json.Unmarshal(frame[1], &sub)
			if typ != "REQ" {
				continue
			}
			_ = conn.WriteJSON([]any{"EVENT", sub, event})
			_ = conn.WriteJSON([]any{"EOSE", sub})
		}
	}))
	defer server.Close()
	relayURL := wsURL(server)
	state := &probeState{
		key: key, pubkey: pubOf(key), worker: "worker-a", allowPrivate: true,
		globalLimit: 20, minInterval: time.Second, allowlist: []string{probeHost(relayURL)},
	}
	state.scheduleRetention(ctx, pool, relayURL, relayURL, eventID, time.Now().Add(-2*time.Hour))
	ok, err := state.followOne(ctx, pool)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	var status, result string
	var elapsed int
	var finished bool
	if err := pool.QueryRow(ctx, `
		SELECT status, result, elapsed_seconds, finished_at IS NOT NULL
		FROM relay_probe_followups WHERE event_id = $1
	`, eventID).Scan(&status, &result, &elapsed, &finished); err != nil {
		t.Fatal(err)
	}
	if status != "completed" || !finished || elapsed < 7000 || !strings.HasPrefix(result, "retrieved_after_") {
		t.Fatalf("status=%s result=%s elapsed=%d finished=%v", status, result, elapsed, finished)
	}
	if result == "retrieved_after_one_interval" {
		t.Fatal("a late check was described as one hour")
	}
	var leases int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM relay_probe_leases WHERE url_key = $1`, relayURL).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if leases != 0 {
		t.Fatal("the follow-up lease was left behind")
	}
}

func TestSyntheticAuthorIsVisibleFromTheDatabase(t *testing.T) {
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "relay synthetic identity"), "probeidentity")
	if err := dbmigrate.Migrate(ctx, pool, "test"); err != nil {
		t.Fatal(err)
	}
	pubkey := strings.Repeat("12", 32)
	if err := registerSyntheticIdentity(ctx, pool, pubkey); err != nil {
		t.Fatal(err)
	}
	syntheticPubkeys.Delete(pubkey)
	if SyntheticPubkey(pubkey) {
		t.Fatal("clearing the local cache left the identity loaded")
	}
	if !SyntheticAuthor(ctx, pool, pubkey) {
		t.Fatal("another worker could not see the registered identity")
	}
}
