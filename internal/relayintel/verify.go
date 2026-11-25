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

// VerifyResult is a read-only propagation check. GlobalSuccess is never true
// from a single relay.
type VerifyResult struct {
	SignatureValid bool         `json:"signature_valid"`
	SignatureError string       `json:"signature_error,omitempty"`
	EventID        string       `json:"event_id,omitempty"`
	Pubkey         string       `json:"pubkey,omitempty"`
	Kind           int          `json:"kind,omitempty"`
	CreatedAt      int64        `json:"created_at,omitempty"`
	Relays         []RelayCheck `json:"relays"`
	RetrievedCount int          `json:"retrieved_count"`
	Agreement      bool         `json:"latest_agreement"`
	Partial        bool         `json:"partial"`
	GlobalSuccess  bool         `json:"global_success"`
	Reason         string       `json:"reason,omitempty"`
	Note           string       `json:"note"`
}

// RelayCheck is one relay's retrieval result. Error is a short code.
type RelayCheck struct {
	URL             string `json:"relay_url"`
	Outcome         string `json:"outcome"`
	RetrievedID     string `json:"retrieved_id,omitempty"`
	LatestID        string `json:"latest_id,omitempty"`
	LatestCreatedAt int64  `json:"latest_created_at,omitempty"`
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
		Note:   "This server did not publish the event. A single relay acknowledgement is not global propagation. Authentication challenges are reported as unavailable rather than bypassed.",
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
		result.GlobalSuccess = true
		result.Partial = false
	}
	return result, nil
}

func checkRelay(ctx context.Context, relayURL string, event *nostr.Event, allowPrivate bool) RelayCheck {
	out := RelayCheck{URL: relayURL, Outcome: "unavailable", Error: "unavailable"}
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	byID, err := fetchEvents(queryCtx, relayURL, allowPrivate, map[string]any{"ids": []string{event.ID}, "limit": 1})
	if err != nil && len(byID) == 0 {
		out.Error = classifyDial(err)
		out.Outcome = outcomeFor(out.Error)
		return out
	}
	var got map[string]any
	for _, item := range byID {
		if id, _ := item["id"].(string); id == event.ID {
			got = item
			break
		}
	}
	if !replaceableKind(event.Kind) {
		if got == nil {
			out.Outcome = "not_found"
			out.Error = ""
			return out
		}
		out.Outcome = "retrieved"
		out.RetrievedID = event.ID
		out.Error = ""
		return out
	}
	filter := map[string]any{"authors": []string{event.Pubkey}, "kinds": []int{event.Kind}, "limit": 1}
	if event.Kind >= 30000 && event.Kind < 40000 {
		if d := firstD(event.Tags); d != "" {
			filter["#d"] = []string{d}
		}
	}
	latest, err := fetchEvents(queryCtx, relayURL, allowPrivate, filter)
	if err != nil {
		if got != nil {
			out.Outcome = "retrieved"
			out.RetrievedID = event.ID
			out.Error = ""
			return out
		}
		out.Error = classifyDial(err)
		out.Outcome = outcomeFor(out.Error)
		return out
	}
	if len(latest) > 0 {
		if id, _ := latest[0]["id"].(string); id != "" {
			out.LatestID = id
		}
		switch created := latest[0]["created_at"].(type) {
		case float64:
			out.LatestCreatedAt = int64(created)
		case int64:
			out.LatestCreatedAt = created
		}
	}
	if got == nil {
		out.Outcome = "not_found"
		out.Error = ""
		if out.LatestID != "" && out.LatestID != event.ID && out.LatestCreatedAt >= event.CreatedAt {
			out.Outcome = "conflict"
			out.Error = "newer_event"
		}
		return out
	}
	out.RetrievedID = event.ID
	out.Error = ""
	if out.LatestID != "" && out.LatestID != event.ID && out.LatestCreatedAt > event.CreatedAt {
		out.Outcome = "conflict"
		out.Error = "newer_event"
		return out
	}
	out.Outcome = "retrieved"
	return out
}

func fetchEvents(ctx context.Context, relayURL string, allowPrivate bool, filter map[string]any) ([]map[string]any, error) {
	conn, err := dialRelay(ctx, relayURL, allowPrivate, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	sub := "nm-verify"
	req, err := json.Marshal([]any{"REQ", sub, filter})
	if err != nil {
		return nil, err
	}
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, req); err != nil {
		return nil, err
	}
	var events []map[string]any
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	for len(events) < 5 {
		_ = conn.SetReadDeadline(deadline)
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return events, err
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
			var event map[string]any
			if err := json.Unmarshal(frame[2], &event); err != nil {
				continue
			}
			events = append(events, event)
		case "EOSE":
			return events, nil
		case "CLOSED":
			return events, errClosed
		case "AUTH":
			return events, errAuth
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
