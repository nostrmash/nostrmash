package relayintel

import (
	"encoding/json"
	"strings"
)

// PatchInput is one lossless edit of a signed configuration event.
// Analytical URL lists may be normalized. The source event is not.
type PatchInput struct {
	Source          json.RawMessage
	Origin          string
	Truncated       bool
	ExpectedPubkey  string
	ExpectedKind    int
	InboxOptimized  bool
	OutboxOptimized bool
	Inbox           []string
	Outbox          []string
	ListOptimized   bool
	TagName         string
	Desired         []string
}

// PatchResult is safe to hand to a client only when Safe is true and Event is set.
// A missing or truncated source never produces a replacement.
type PatchResult struct {
	Safe            bool
	Complete        bool
	Unchanged       bool
	Origin          string
	EventID         string
	CreatedAt       int64
	RequiresRefresh bool
	RequiresReview  bool
	Blocking        []Reason
	Event           *UnsignedEvent
}

// Patch edits relay tags on a complete source event.
// Content, unknown tags, unknown markers, and untouched roles are copied.
// Kind 3 is never emitted. An unchanged configuration emits no event.
func Patch(in PatchInput) PatchResult {
	out := PatchResult{Origin: in.Origin}
	if in.Origin == "" {
		out.Origin = "missing"
	}
	if len(in.Source) == 0 {
		out.Blocking = []Reason{{Code: "source_missing", Text: "No complete source event is available. Local absence is not a confirmed empty list."}}
		out.RequiresRefresh = true
		return out
	}
	var ev struct {
		ID        string     `json:"id"`
		Pubkey    string     `json:"pubkey"`
		CreatedAt int64      `json:"created_at"`
		Kind      int        `json:"kind"`
		Tags      [][]string `json:"tags"`
		Content   string     `json:"content"`
	}
	if err := json.Unmarshal(in.Source, &ev); err != nil || ev.Kind == 0 || ev.Pubkey == "" {
		out.Blocking = []Reason{{Code: "source_malformed", Text: "The source event could not be read."}}
		out.RequiresRefresh = true
		return out
	}
	out.EventID = ev.ID
	out.CreatedAt = ev.CreatedAt
	if in.ExpectedPubkey != "" && !strings.EqualFold(ev.Pubkey, in.ExpectedPubkey) {
		out.Blocking = []Reason{{Code: "source_author_mismatch", Text: "The source event was signed by a different pubkey."}}
		out.RequiresRefresh = true
		return out
	}
	if in.ExpectedKind != 0 && ev.Kind != in.ExpectedKind {
		out.Blocking = []Reason{{Code: "source_kind_mismatch", Text: "The source event kind does not match the list being edited."}}
		out.RequiresRefresh = true
		return out
	}
	if ev.Kind == 3 {
		out.Blocking = []Reason{{Code: "kind_3_not_emitted", Text: "Kind 3 is not replaced. Publish a kind 10002 instead."}}
		out.RequiresReview = true
		return out
	}
	if in.Truncated {
		out.Blocking = []Reason{{Code: "source_truncated", Text: "The indexed copy of this list was truncated. Refresh the signed event before accepting a replacement."}}
		out.RequiresRefresh = true
		return out
	}
	var tags [][]string
	var changed bool
	switch {
	case in.InboxOptimized || in.OutboxOptimized:
		tags, changed = patchNIP65(ev.Tags, in)
	case in.ListOptimized:
		tags, changed = patchList(ev.Tags, in.TagName, in.Desired)
	default:
		out.Complete = true
		out.Safe = true
		out.Unchanged = true
		return out
	}
	if !changed {
		out.Complete = true
		out.Safe = true
		out.Unchanged = true
		return out
	}
	note := "Unsigned replacement. Content and unrelated tags were copied from the source event. The client must re-read the latest source, then set created_at, id, and sig. This server does not sign or publish."
	out.Event = &UnsignedEvent{
		Kind: ev.Kind, Pubkey: strings.ToLower(ev.Pubkey), Content: ev.Content, Tags: tags,
		BasedOnEventID: ev.ID, BasedOnCreatedAt: ev.CreatedAt, Note: note,
	}
	out.Complete = true
	out.Safe = true
	return out
}

type relayMark struct {
	read  bool
	write bool
	url   string
}

func patchNIP65(source [][]string, in PatchInput) ([][]string, bool) {
	existing := map[string]relayMark{}
	for _, tag := range source {
		if len(tag) < 2 || tag[0] != "r" {
			continue
		}
		marker, known := knownMarker(tag)
		if !known {
			continue
		}
		norm, ok := canonicalRelay(tag[1])
		if !ok {
			continue
		}
		item := existing[norm]
		if item.url == "" {
			item.url = tag[1]
		}
		if marker == "" || marker == "read" {
			item.read = true
		}
		if marker == "" || marker == "write" {
			item.write = true
		}
		existing[norm] = item
	}
	finalRead := map[string]string{}
	finalWrite := map[string]string{}
	for norm, item := range existing {
		if item.read {
			finalRead[norm] = item.url
		}
		if item.write {
			finalWrite[norm] = item.url
		}
	}
	if in.InboxOptimized {
		finalRead = desiredSet(in.Inbox, existing)
	}
	if in.OutboxOptimized {
		finalWrite = desiredSet(in.Outbox, existing)
	}
	if sameSet(keysOfMap(finalRead), readNorms(existing)) && sameSet(keysOfMap(finalWrite), writeNorms(existing)) {
		return nil, false
	}
	emitted := map[string]struct{}{}
	tags := make([][]string, 0, len(source))
	for _, tag := range source {
		if len(tag) < 2 || tag[0] != "r" {
			tags = append(tags, append([]string(nil), tag...))
			continue
		}
		marker, known := knownMarker(tag)
		if !known {
			tags = append(tags, append([]string(nil), tag...))
			continue
		}
		norm, ok := canonicalRelay(tag[1])
		if !ok {
			tags = append(tags, append([]string(nil), tag...))
			continue
		}
		if _, done := emitted[norm]; done {
			continue
		}
		read, write := finalRead[norm] != "", finalWrite[norm] != ""
		if !read && !write {
			continue
		}
		want := markerFor(read, write)
		if want == marker {
			tags = append(tags, append([]string(nil), tag...))
		} else {
			next := append([]string(nil), tag...)
			next = setMarker(next, want)
			tags = append(tags, next)
		}
		emitted[norm] = struct{}{}
	}
	for _, raw := range append(append([]string{}, in.Inbox...), in.Outbox...) {
		norm, ok := canonicalRelay(raw)
		if !ok {
			continue
		}
		if _, done := emitted[norm]; done {
			continue
		}
		read, write := finalRead[norm] != "", finalWrite[norm] != ""
		if !read && !write {
			continue
		}
		url := raw
		if item, ok := existing[norm]; ok && item.url != "" {
			url = item.url
		}
		tags = append(tags, markerTag(url, markerFor(read, write)))
		emitted[norm] = struct{}{}
	}
	return tags, true
}

func patchList(source [][]string, tagName string, desired []string) ([][]string, bool) {
	if tagName == "" {
		tagName = "relay"
	}
	existing := map[string]string{}
	for _, tag := range source {
		if len(tag) < 2 || !managedTag(tag[0], tagName) {
			continue
		}
		norm, ok := canonicalEndpoint(tag[1], tag[0] == "server")
		if !ok {
			continue
		}
		if _, seen := existing[norm]; !seen {
			existing[norm] = tag[1]
		}
	}
	final := map[string]string{}
	for _, raw := range desired {
		norm, ok := canonicalEndpoint(raw, tagName == "server")
		if !ok {
			continue
		}
		if original, seen := existing[norm]; seen {
			final[norm] = original
		} else {
			final[norm] = raw
		}
	}
	if sameSet(keysOfMap(final), keysOfString(existing)) {
		return nil, false
	}
	emitted := map[string]struct{}{}
	tags := make([][]string, 0, len(source)+len(desired))
	for _, tag := range source {
		if len(tag) >= 2 && tag[0] == "r" {
			if _, known := knownMarker(tag); !known {
				tags = append(tags, append([]string(nil), tag...))
				continue
			}
		}
		if len(tag) < 2 || !managedTag(tag[0], tagName) {
			tags = append(tags, append([]string(nil), tag...))
			continue
		}
		norm, ok := canonicalEndpoint(tag[1], tag[0] == "server")
		if !ok {
			tags = append(tags, append([]string(nil), tag...))
			continue
		}
		if _, keep := final[norm]; !keep {
			continue
		}
		if _, done := emitted[norm]; done {
			continue
		}
		tags = append(tags, append([]string(nil), tag...))
		emitted[norm] = struct{}{}
	}
	for _, raw := range desired {
		norm, ok := canonicalEndpoint(raw, tagName == "server")
		if !ok {
			continue
		}
		if _, done := emitted[norm]; done {
			continue
		}
		if _, keep := final[norm]; !keep {
			continue
		}
		tags = append(tags, []string{tagName, final[norm]})
		emitted[norm] = struct{}{}
	}
	return tags, true
}

func knownMarker(tag []string) (string, bool) {
	if len(tag) < 3 || strings.TrimSpace(tag[2]) == "" {
		return "", true
	}
	marker := strings.ToLower(strings.TrimSpace(tag[2]))
	if marker == "read" || marker == "write" {
		return marker, true
	}
	return marker, false
}

func markerFor(read, write bool) string {
	switch {
	case read && write:
		return ""
	case read:
		return "read"
	default:
		return "write"
	}
}

func setMarker(tag []string, marker string) []string {
	if marker == "" {
		if len(tag) >= 3 && (tag[2] == "read" || tag[2] == "write") {
			return append(append([]string{}, tag[:2]...), tag[3:]...)
		}
		return tag
	}
	if len(tag) < 3 {
		return append(tag, marker)
	}
	tag[2] = marker
	return tag
}

func markerTag(url, marker string) []string {
	if marker == "" {
		return []string{"r", url}
	}
	return []string{"r", url, marker}
}

func managedTag(name, tagName string) bool {
	return name == tagName || name == "r" || name == "server" || name == "relay"
}

func canonicalRelay(raw string) (string, bool) {
	normalized, _, _, ok := normalizeNostr(raw)
	return normalized, ok
}

func canonicalEndpoint(raw string, blossom bool) (string, bool) {
	if blossom {
		normalized, err := NormalizeBlossomURL(raw)
		return normalized, err == nil
	}
	if normalized, ok := canonicalRelay(raw); ok {
		return normalized, true
	}
	normalized, err := NormalizeBlossomURL(raw)
	return normalized, err == nil
}

func desiredSet(urls []string, existing map[string]relayMark) map[string]string {
	out := map[string]string{}
	for _, raw := range urls {
		norm, ok := canonicalRelay(raw)
		if !ok {
			continue
		}
		if item, seen := existing[norm]; seen && item.url != "" {
			out[norm] = item.url
		} else {
			out[norm] = raw
		}
	}
	return out
}

func readNorms(existing map[string]relayMark) []string {
	out := make([]string, 0, len(existing))
	for norm, item := range existing {
		if item.read {
			out = append(out, norm)
		}
	}
	return out
}

func writeNorms(existing map[string]relayMark) []string {
	out := make([]string, 0, len(existing))
	for norm, item := range existing {
		if item.write {
			out = append(out, norm)
		}
	}
	return out
}

func keysOfMap(values map[string]string) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	return out
}

func keysOfString(values map[string]string) []string {
	return keysOfMap(values)
}

func sameSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := map[string]struct{}{}
	for _, item := range left {
		seen[item] = struct{}{}
	}
	for _, item := range right {
		if _, ok := seen[item]; !ok {
			return false
		}
	}
	return true
}
