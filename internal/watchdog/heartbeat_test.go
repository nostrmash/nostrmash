package watchdog

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFromEnvDisabledAndInvalid(t *testing.T) {
	t.Setenv(urlEnv, "")
	t.Setenv(secretEnv, "")
	if FromEnv() != nil {
		t.Fatal("expected a disabled emitter")
	}
	t.Setenv(urlEnv, "http://watchdog.example/v1/heartbeats/nostrmash-worker")
	t.Setenv(secretEnv, strings.Repeat("a", 32))
	if FromEnv() != nil {
		t.Fatal("expected http to be rejected")
	}
	t.Setenv(urlEnv, "https://user:pw@watchdog.example/v1/heartbeats/nostrmash-worker")
	if FromEnv() != nil {
		t.Fatal("expected userinfo to be rejected")
	}
	t.Setenv(urlEnv, "https://watchdog.example/v1/heartbeats/nostrmash-worker")
	t.Setenv(secretEnv, "short")
	if FromEnv() != nil {
		t.Fatal("expected a short secret to be rejected")
	}
}

func TestNotifySignsTheCanonicalRequest(t *testing.T) {
	secret := strings.Repeat("k", 32)
	var (
		mu       sync.Mutex
		got      *http.Request
		gotBody  []byte
		attempts int
	)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		attempts++
		if attempts == 1 {
			mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		got = r.Clone(r.Context())
		gotBody = body
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	target := server.URL + "/v1/heartbeats/nostrmash-worker"
	t.Setenv(urlEnv, strings.Replace(target, "https://", "https://", 1))
	t.Setenv(secretEnv, secret)
	emitter := FromEnv()
	if emitter == nil {
		t.Fatal("expected an emitter")
	}
	emitter.client = server.Client()
	emitter.client.Timeout = 3 * time.Second

	emitter.Notify(t.Context(), Sample{
		Progress: "4",
		Idle:     false,
		LoopAt:   time.UnixMilli(1_700_000_000_000),
	})
	deadline := time.Now().Add(3 * time.Second)
	for {
		mu.Lock()
		done := got != nil
		mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if got == nil {
		t.Fatalf("expected a signed request after retry, attempts=%d", attempts)
	}
	if got.URL.Path != "/v1/heartbeats/nostrmash-worker" {
		t.Fatalf("path %s", got.URL.Path)
	}
	var payload map[string]any
	if err := json.Unmarshal(gotBody, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["progress"] != "4" || payload["idle"] != false {
		t.Fatalf("payload %#v", payload)
	}
	sum := sha256.Sum256(gotBody)
	canonical := got.Header.Get("x-watchdog-timestamp") + "\n" +
		got.Header.Get("x-watchdog-nonce") + "\nPOST\n" +
		got.URL.Path + "\n" + hex.EncodeToString(sum[:])
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(canonical))
	if !hmac.Equal([]byte(got.Header.Get("x-watchdog-signature")), []byte(hex.EncodeToString(mac.Sum(nil)))) {
		t.Fatal("signature did not match the canonical heartbeat")
	}
}
