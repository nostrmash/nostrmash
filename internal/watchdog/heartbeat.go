// Package watchdog sends an optional signed heartbeat to Nostr Watchdog.
// The integration does nothing when WATCHDOG_URL or WATCHDOG_HMAC_SECRET is unset.
package watchdog

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	urlEnv = "WATCHDOG_URL"
	// Environment variable name. This is not a credential value.
	secretEnv = "WATCHDOG_HMAC_SECRET" //nolint:gosec // G101: env var name, not a credential
)

// Sample is the non-sensitive progress report for one monitor.
type Sample struct {
	Progress string
	Idle     bool
	LoopAt   time.Time
}

// Emitter posts signed heartbeats. A nil Emitter is a no-op.
type Emitter struct {
	target *url.URL
	secret string
	client *http.Client

	mu       sync.Mutex
	inFlight bool
}

// FromEnv returns nil when the heartbeat is not configured.
// The URL must be the full https endpoint, including /v1/heartbeats/<monitor-id>.
func FromEnv() *Emitter {
	rawURL := strings.TrimSpace(getenv(urlEnv))
	secret := strings.TrimSpace(getenv(secretEnv))
	if rawURL == "" || secret == "" {
		return nil
	}
	target, err := url.Parse(rawURL)
	if err != nil || !validTarget(target) || len(secret) < 32 {
		return nil
	}
	return &Emitter{
		target: target,
		secret: secret,
		client: &http.Client{
			Timeout: 3 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return fmt.Errorf("redirect refused")
			},
		},
	}
}

// Notify sends one sample without blocking the caller.
// A send already in flight is dropped. Failures are not returned.
func (e *Emitter) Notify(ctx context.Context, sample Sample) {
	if e == nil || sample.LoopAt.IsZero() {
		return
	}
	e.mu.Lock()
	if e.inFlight {
		e.mu.Unlock()
		return
	}
	e.inFlight = true
	e.mu.Unlock()
	go func() {
		defer func() {
			e.mu.Lock()
			e.inFlight = false
			e.mu.Unlock()
		}()
		sendCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 7*time.Second)
		defer cancel()
		if err := e.post(sendCtx, sample); err != nil {
			_ = e.post(sendCtx, sample)
		}
	}()
}

func (e *Emitter) post(ctx context.Context, sample Sample) error {
	body, canonical, timestamp, nonce, err := sign(e.target.Path, e.secret, sample, time.Now())
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.target.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("content-type", "application/json")
	request.Header.Set("x-watchdog-timestamp", timestamp)
	request.Header.Set("x-watchdog-nonce", nonce)
	request.Header.Set("x-watchdog-signature", canonical)
	response, err := e.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("status %d", response.StatusCode)
	}
	return nil
}

func sign(path, secret string, sample Sample, now time.Time) (body []byte, signature, timestamp, nonce string, err error) {
	payload := map[string]any{}
	if sample.Progress != "" {
		payload["progress"] = sample.Progress
	}
	payload["idle"] = sample.Idle
	payload["loopAt"] = sample.LoopAt.UnixMilli()
	body, err = json.Marshal(payload)
	if err != nil {
		return nil, "", "", "", err
	}
	sum := sha256.Sum256(body)
	timestamp = fmt.Sprintf("%d", now.Unix())
	var nonceBytes [16]byte
	if _, err = rand.Read(nonceBytes[:]); err != nil {
		return nil, "", "", "", err
	}
	nonce = hex.EncodeToString(nonceBytes[:])
	canonical := timestamp + "\n" + nonce + "\nPOST\n" + path + "\n" + hex.EncodeToString(sum[:])
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(canonical))
	return body, hex.EncodeToString(mac.Sum(nil)), timestamp, nonce, nil
}

func validTarget(target *url.URL) bool {
	return target != nil &&
		target.Scheme == "https" &&
		target.Host != "" &&
		target.User == nil &&
		target.RawQuery == "" &&
		target.Fragment == ""
}

// getenv is replaced in tests.
var getenv = osGetenv

func osGetenv(key string) string {
	return strings.TrimSpace(readEnv(key))
}
