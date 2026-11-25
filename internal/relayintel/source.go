package relayintel

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nostrmash/nostrmash/internal/nostr"
)

// SourceEvent is a caller-supplied signed configuration event.
// It is used to build a proposal and is not stored.
type SourceEvent struct {
	Raw       json.RawMessage
	ID        string
	Pubkey    string
	Kind      int
	CreatedAt int64
	DTag      string
}

// ValidateSourceEvents checks signature, author, kind, and coordinate.
// Two events for the same coordinate are rejected. The events are not persisted.
func ValidateSourceEvents(author string, raws []json.RawMessage) (map[string]SourceEvent, error) {
	out := map[string]SourceEvent{}
	if len(raws) > 8 {
		return nil, invalidf("at most 8 source events can be supplied")
	}
	author = strings.ToLower(strings.TrimSpace(author))
	for _, raw := range raws {
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		if len(raw) > 64*1024 {
			return nil, invalidf("a source event is larger than 64KB")
		}
		parsed := nostr.ParseAndValidate(raw, nostr.Options{MaxRawJSONBytes: 64 * 1024, MaxContentBytes: 32 * 1024})
		if !parsed.Valid() || parsed.Event == nil {
			return nil, invalidf("source event signature is invalid")
		}
		ev := parsed.Event
		if !strings.EqualFold(ev.Pubkey, author) {
			return nil, invalidf("source event pubkey does not match the requested account")
		}
		if ev.Kind == 3 {
			return nil, invalidf("kind 3 cannot be supplied as a replacement source")
		}
		if !HandlesKind(ev.Kind) {
			return nil, invalidf("source event kind %d is not a supported configuration list", ev.Kind)
		}
		if ev.CreatedAt <= 0 || ev.CreatedAt > time.Now().Add(2*time.Hour).Unix() {
			return nil, invalidf("source event created_at is not usable")
		}
		dTag := ""
		if ev.Kind == KindRelaySet {
			dTag = firstD(ev.Tags)
		}
		key := sourceKey(ev.Kind, dTag)
		if _, exists := out[key]; exists {
			return nil, invalidf("two source events were supplied for the same list")
		}
		out[key] = SourceEvent{
			Raw: append(json.RawMessage(nil), raw...), ID: ev.ID, Pubkey: strings.ToLower(ev.Pubkey),
			Kind: ev.Kind, CreatedAt: ev.CreatedAt, DTag: dTag,
		}
	}
	return out, nil
}

func sourceKey(kind int, dTag string) string {
	return fmt.Sprintf("%d\x00%s", kind, dTag)
}
