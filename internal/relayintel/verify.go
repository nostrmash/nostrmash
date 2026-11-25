package relayintel

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nostrmash/nostrmash/internal/nostr"
)

// VerifyResult is a read-only propagation check of a sample of relays.
// SampleVerified means the submitted event was retrieved and, for replaceable
// events, matched the latest valid version on at least two relays.
// GlobalSuccess is the deprecated name for that sample. It is not network-wide
// propagation.
type VerifyResult struct {
	SignatureValid  bool         `json:"signature_valid"`
	SignatureError  string       `json:"signature_error,omitempty"`
	EventID         string       `json:"event_id,omitempty"`
	Pubkey          string       `json:"pubkey,omitempty"`
	Kind            int          `json:"kind,omitempty"`
	CreatedAt       int64        `json:"created_at,omitempty"`
	Relays          []RelayCheck `json:"relays"`
	RetrievedCount  int          `json:"retrieved_count"`
	Agreement       bool         `json:"latest_agreement"`
	Partial         bool         `json:"partial"`
	SampleVerified  bool         `json:"sample_verified"`
	SampleAgreement bool         `json:"sample_agreement"`
	GlobalSuccess   bool         `json:"global_success"`
	Reason          string       `json:"reason,omitempty"`
	Note            string       `json:"note"`
}

// RelayCheck is one relay's retrieval result. Error is a short code.
type RelayCheck struct {
	URL             string `json:"relay_url"`
	Outcome         string `json:"outcome"`
	RetrievedID     string `json:"retrieved_id,omitempty"`
	LatestID        string `json:"latest_id,omitempty"`
	LatestCreatedAt int64  `json:"latest_created_at,omitempty"`
	Retrieved       bool   `json:"retrieved"`
	LatestVerified  bool   `json:"latest_verified"`
	LatestAgrees    bool   `json:"latest_agrees"`
	Error           string `json:"error,omitempty"`
}

// VerifyEvent validates a signed event and asks each relay for it.
// The event is not published.
func VerifyEvent(ctx context.Context, eventJSON []byte, relays []string, allowPrivate bool) (VerifyResult, error) {
	if len(relays) == 0 {
		return VerifyResult{}, invalidf("at least one relay is required")
	}
	if len(relays) > 8 {
		return VerifyResult{}, invalidf("at most 8 relays can be checked")
	}
	result := VerifyResult{
		Relays: []RelayCheck{},
		Note:   "This server did not publish the event. sample_verified means the checked relays agreed. It is not network-wide propagation. global_success is the deprecated name for that sample. Authentication challenges are reported and not bypassed.",
	}
	parsed := nostr.ParseAndValidate(eventJSON, nostr.Options{MaxRawJSONBytes: 64 * 1024, MaxContentBytes: 32 * 1024})
	if !parsed.Valid() {
		result.SignatureValid = false
		if parsed.Err != nil {
			result.SignatureError = string(parsed.Err.Code)
		}
		result.Reason = "signature_invalid"
		for _, raw := range relays {
			result.Relays = append(result.Relays, RelayCheck{URL: strings.TrimSpace(raw), Outcome: "not_queried", Error: "signature_invalid"})
		}
		return result, nil
	}
	event := parsed.Event
	if !HandlesKind(event.Kind) {
		result.SignatureValid = true
		result.EventID = event.ID
		result.Pubkey = event.Pubkey
		result.Kind = event.Kind
		result.CreatedAt = event.CreatedAt
		result.Reason = "unsupported_kind"
		for _, raw := range relays {
			result.Relays = append(result.Relays, RelayCheck{URL: strings.TrimSpace(raw), Outcome: "not_queried", Error: "unsupported_kind"})
		}
		return result, nil
	}
	result.SignatureValid = true
	result.EventID = event.ID
	result.Pubkey = event.Pubkey
	result.Kind = event.Kind
	result.CreatedAt = event.CreatedAt

	seen := map[string]struct{}{}
	targets := make([]string, 0, len(relays))
	for _, raw := range relays {
		prepared, rejected, err := (&Service{allowPrivate: allowPrivate}).normalizeOne(raw)
		if err != nil || rejected {
			outcome := "rejected"
			if err != nil {
				outcome = "rejected"
			}
			result.Relays = append(result.Relays, RelayCheck{URL: strings.TrimSpace(raw), Outcome: outcome, Error: "rejected"})
			continue
		}
		if _, ok := seen[prepared.URL]; ok {
			continue
		}
		seen[prepared.URL] = struct{}{}
		targets = append(targets, prepared.URL)
	}
	checks := make([]RelayCheck, len(targets))
	parent, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, relayURL := range targets {
		wg.Add(1)
		go func(i int, relayURL string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			checks[i] = checkRelay(parent, relayURL, event, allowPrivate)
		}(i, relayURL)
	}
	wg.Wait()
	result.Relays = append(result.Relays, checks...)

	timeouts, conflicts, unavailable, missing := 0, 0, 0, 0
	for _, item := range result.Relays {
		switch item.Outcome {
		case "retrieved":
			result.RetrievedCount++
		case "timeout":
			timeouts++
		case "conflict":
			conflicts++
		case "not_found":
			missing++
		default:
			unavailable++
		}
	}
	result.Agreement = conflicts == 0 && result.RetrievedCount > 0
	result.Partial = result.RetrievedCount > 0 && (missing > 0 || timeouts > 0 || unavailable > 0 || conflicts > 0)
	switch {
	case conflicts > 0:
		result.Reason = "conflict"
	case len(targets) < 2 || result.RetrievedCount < 2:
		result.Reason = "insufficient_relay_sample"
	case timeouts > 0:
		result.Reason = "timeout"
	case missing > 0 || unavailable > 0:
		result.Reason = "partial_propagation"
	default:
		result.Reason = "ok"
		result.SampleVerified = true
		result.SampleAgreement = true
		result.GlobalSuccess = true
		result.Partial = false
	}
	return result, nil
}

var (
	verifyGlobal = make(chan struct{}, 16)
	verifyHostMu sync.Mutex
	verifyHosts  = map[string]chan struct{}{}
)

func checkRelay(ctx context.Context, relayURL string, event *nostr.Event, allowPrivate bool) RelayCheck {
	out := RelayCheck{URL: relayURL, Outcome: "unavailable", Error: "unavailable"}
	select {
	case verifyGlobal <- struct{}{}:
		defer func() { <-verifyGlobal }()
	case <-ctx.Done():
		out.Outcome = "timeout"
		out.Error = "timeout"
		return out
	}
	host := relayHost(relayURL)
	hostSlot := verifyHostSlot(host)
	select {
	case hostSlot <- struct{}{}:
		defer func() { <-hostSlot }()
	case <-ctx.Done():
		out.Outcome = "timeout"
		out.Error = "timeout"
		return out
	}
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	byID, err := fetchEvents(queryCtx, relayURL, allowPrivate, map[string]any{"ids": []string{event.ID}, "limit": 1})
	if err != nil && len(byID) == 0 {
		out.Error = classifyDial(err)
		out.Outcome = outcomeFor(out.Error)
		return out
	}
	var exact *nostr.Event
	for _, item := range byID {
		if item.ID == event.ID && strings.EqualFold(item.Pubkey, event.Pubkey) && item.Kind == event.Kind {
			exact = item
			break
		}
	}
	if !replaceableKind(event.Kind) {
		if exact == nil {
			out.Outcome = "not_found"
			out.Error = ""
			return out
		}
		out.Outcome = "retrieved"
		out.Retrieved = true
		out.RetrievedID = event.ID
		out.Error = ""
		return out
	}
	filter := map[string]any{"authors": []string{event.Pubkey}, "kinds": []int{event.Kind}, "limit": 8}
	latest, latestErr := fetchEvents(queryCtx, relayURL, allowPrivate, filter)
	winner := newestValid(latest, event.Pubkey, event.Kind, firstD(event.Tags), event.Kind >= 30000 && event.Kind < 40000)
	if latestErr != nil || winner == nil {
		if exact != nil {
			out.Outcome = "retrieved_latest_unknown"
			out.Retrieved = true
			out.RetrievedID = event.ID
			out.LatestVerified = false
			if latestErr != nil {
				out.Error = classifyDial(latestErr)
			} else {
				out.Error = "latest_unverified"
			}
			return out
		}
		if latestErr != nil {
			out.Error = classifyDial(latestErr)
			out.Outcome = outcomeFor(out.Error)
			return out
		}
		out.Outcome = "not_found"
		out.Error = ""
		return out
	}
	out.LatestID = winner.ID
	out.LatestCreatedAt = winner.CreatedAt
	out.LatestVerified = true
	if winner.ID != event.ID && nostr.Supersedes(winner.CreatedAt, winner.ID, event.CreatedAt, event.ID) {
		out.Outcome = "conflict"
		out.Error = "newer_event"
		out.LatestAgrees = false
		if exact != nil {
			out.Retrieved = true
			out.RetrievedID = event.ID
		}
		return out
	}
	if exact == nil {
		out.Outcome = "not_found"
		out.Error = ""
		return out
	}
	out.Outcome = "retrieved"
	out.Retrieved = true
	out.RetrievedID = event.ID
	out.LatestAgrees = winner.ID == event.ID
	out.Error = ""
	return out
}

func newestValid(events []*nostr.Event, pubkey string, kind int, dTag string, addressable bool) *nostr.Event {
	var winner *nostr.Event
	for _, item := range events {
		if item == nil || !strings.EqualFold(item.Pubkey, pubkey) || item.Kind != kind {
			continue
		}
		if addressable && firstD(item.Tags) != dTag {
			continue
		}
		if winner == nil || nostr.Supersedes(item.CreatedAt, item.ID, winner.CreatedAt, winner.ID) {
			winner = item
		}
	}
	return winner
}

func verifyHostSlot(host string) chan struct{} {
	verifyHostMu.Lock()
	defer verifyHostMu.Unlock()
	if slot, ok := verifyHosts[host]; ok {
		return slot
	}
	if len(verifyHosts) > 1024 {
		verifyHosts = map[string]chan struct{}{}
	}
	slot := make(chan struct{}, 2)
	verifyHosts[host] = slot
	return slot
}

func relayHost(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return strings.ToLower(parsed.Hostname())
}

func fetchEvents(ctx context.Context, relayURL string, allowPrivate bool, filter map[string]any) ([]*nostr.Event, error) {
	conn, err := dialRelay(ctx, relayURL, allowPrivate, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetReadLimit(64 * 1024)
	sub := "nm-verify"
	req, err := json.Marshal([]any{"REQ", sub, filter})
	if err != nil {
		return nil, err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
		return nil, err
	}
	var events []*nostr.Event
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	total := 0
	for messages := 0; messages < 12 && len(events) < 8; messages++ {
		_ = conn.SetReadDeadline(deadline)
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return events, err
		}
		total += len(payload)
		if total > 256*1024 {
			return events, errString("oversized")
		}
		var frame []json.RawMessage
		if err := json.Unmarshal(payload, &frame); err != nil || len(frame) == 0 {
			continue
		}
		var typ string
		if err := json.Unmarshal(frame[0], &typ); err != nil {
			continue
		}
		switch typ {
		case "EVENT":
			if len(frame) < 3 {
				continue
			}
			var subID string
			if json.Unmarshal(frame[1], &subID) != nil || subID != sub {
				continue
			}
			parsed := nostr.ParseAndValidate(frame[2], nostr.Options{MaxRawJSONBytes: 64 * 1024, MaxContentBytes: 32 * 1024})
			if !parsed.Valid() || parsed.Event == nil {
				continue
			}
			events = append(events, parsed.Event)
		case "EOSE":
			var subID string
			if len(frame) > 1 && json.Unmarshal(frame[1], &subID) == nil && subID != sub {
				continue
			}
			return events, nil
		case "CLOSED":
			return events, errClosed
		case "AUTH":
			return events, errAuth
		case "NOTICE":
			continue
		}
	}
	return events, nil
}

func dialRelay(ctx context.Context, relayURL string, allowPrivate bool, timeout time.Duration) (*websocket.Conn, error) {
	dialer := websocket.Dialer{
		Proxy:            func(*http.Request) (*url.URL, error) { return nil, nil },
		HandshakeTimeout: timeout,
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return DialPinned(ctx, network, addr, allowPrivate, timeout)
		},
	}
	conn, resp, err := dialer.DialContext(ctx, relayURL, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	return conn, err
}

var (
	errClosed = errString("closed")
	errAuth   = errString("auth_required")
)

type errString string

func (e errString) Error() string { return string(e) }

func classifyDial(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	switch {
	case err == errAuth || strings.Contains(text, "auth_required"):
		return "auth_required"
	case err == errClosed:
		return "closed"
	case strings.Contains(text, "metadata") || strings.Contains(text, "private") || strings.Contains(text, "link-local") || strings.Contains(text, "localhost"):
		return "rejected"
	case strings.Contains(text, "timeout") || strings.Contains(text, "deadline") || strings.Contains(text, "context"):
		return "timeout"
	default:
		return "unavailable"
	}
}

func outcomeFor(code string) string {
	switch code {
	case "timeout":
		return "timeout"
	case "rejected", "auth_required":
		return "rejected"
	default:
		return "unavailable"
	}
}

func replaceableKind(kind int) bool {
	return kind == 0 || kind == 3 || (kind >= 10000 && kind < 20000) || (kind >= 30000 && kind < 40000)
}

func firstD(tags [][]string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == "d" {
			return tag[1]
		}
	}
	return ""
}
