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
	AllowCreate     bool
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
	if in.Origin == "deleted" && !in.AllowCreate {
		out.Blocking = []Reason{{Code: "source_deleted", Text: "A deletion covers this list. A replacement was not generated from the deleted event. Recreating it requires allow_recreate."}}
		out.RequiresRefresh = true
		return out
	}
	if len(in.Source) == 0 {
		if in.AllowCreate && in.ExpectedKind != 0 && in.ExpectedKind != 3 && in.ExpectedPubkey != "" {
			return createReplacement(in)
		}
		out.Blocking = []Reason{{Code: "source_missing", Text: "No complete source event is available. Local absence is not a confirmed empty list. Creating a list requires allow_create."}}
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
	var problem string
	switch {
	case in.InboxOptimized || in.OutboxOptimized:
		tags, changed, problem = patchNIP65(ev.Tags, in)
	case in.ListOptimized:
		tags, changed, problem = patchList(ev.Tags, in.TagName, in.Desired)
	default:
		out.Complete = true
		out.Safe = true
		out.Unchanged = true
		return out
	}
	if problem != "" {
		out.Blocking = []Reason{{Code: problem, Text: "Managed relay tags for one endpoint carry different extra metadata. The replacement was not returned for signing."}}
		out.RequiresReview = true
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
		ProposalSafe: true, SourceOrigin: out.Origin, Change: "replacement",
	}
	out.Complete = true
	out.Safe = true
	return out
}

func createReplacement(in PatchInput) PatchResult {
	var tags [][]string
	var problem string
	switch {
	case in.InboxOptimized || in.OutboxOptimized:
		tags, _, problem = patchNIP65(nil, in)
	case in.ListOptimized:
		tags, _, problem = patchList(nil, in.TagName, in.Desired)
	default:
		return PatchResult{
			Origin: "missing", RequiresRefresh: true,
			Blocking: []Reason{{Code: "source_missing", Text: "No complete source event is available. Local absence is not a confirmed empty list."}},
		}
	}
	if problem != "" {
		return PatchResult{
			RequiresReview: true,
			Blocking:       []Reason{{Code: problem, Text: "Managed relay tags for one endpoint carry different extra metadata. The replacement was not returned for signing."}},
		}
	}
	if tags == nil {
		tags = [][]string{}
	}
	origin, change := "create", "creation"
	note := "Unsigned new list. allow_create was set. Absence from this index is not proof the list is absent from the network. The client must check for an existing event before signing. This server does not sign or publish."
	if in.Origin == "deleted" {
		origin, change = "recreate", "recreation"
		note = "Unsigned recreation of a deleted list. allow_recreate was set. This server does not sign or publish."
	}
	return PatchResult{
		Safe: true, Complete: true, Origin: origin, RequiresReview: true,
		Event: &UnsignedEvent{
			Kind: in.ExpectedKind, Pubkey: strings.ToLower(in.ExpectedPubkey), Content: "", Tags: tags,
			ProposalSafe: true, SourceOrigin: origin, Change: change, Note: note,
		},
	}
}

// confirmProposal parses the replacement with the same model used for the
// current configuration. A patch that does not produce the selected relays
// is not returned for signing.
func confirmProposal(result PatchResult, in PatchInput) PatchResult {
	if !result.Safe || result.Event == nil {
		return result
	}
	eventID := result.EventID
	if eventID == "" {
		eventID = "proposal"
	}
	parsed, err := Parse(EventInput{
		Pubkey: result.Event.Pubkey, EventID: eventID, Kind: result.Event.Kind, Content: result.Event.Content,
		Tags: result.Event.Tags, EndpointLimit: maxAuthoritativeEndpoints,
	})
	if err != nil || parsed.Head.Truncated {
		return rejectProposal(result)
	}
	var source struct {
		Content string     `json:"content"`
		Tags    [][]string `json:"tags"`
	}
	if len(in.Source) > 0 && json.Unmarshal(in.Source, &source) != nil {
		return rejectProposal(result)
	}
	if result.Event.Content != source.Content {
		return rejectProposal(result)
	}
	if in.ExpectedKind == KindRelaySet && firstD(result.Event.Tags) != firstD(source.Tags) {
		return rejectProposal(result)
	}
	if !sameUnmanagedTags(source.Tags, result.Event.Tags, in) || !managedMetadataKept(source.Tags, result.Event.Tags, in) {
		rejected := rejectProposal(result)
		rejected.Blocking = []Reason{{Code: "metadata_lost", Text: "The replacement dropped metadata on a relay tag that was not part of the selection change."}}
		return rejected
	}
	blossom := in.TagName == "server" || in.ExpectedKind == KindBlossomServers
	switch {
	case in.InboxOptimized || in.OutboxOptimized:
		sourceParsed, sourceErr := Parse(EventInput{
			Pubkey: result.Event.Pubkey, EventID: eventID, Kind: result.Event.Kind, Content: source.Content,
			Tags: source.Tags, EndpointLimit: maxAuthoritativeEndpoints,
		})
		if sourceErr != nil {
			return rejectProposal(result)
		}
		if in.InboxOptimized && !sameEndpoints(urlsForRole(parsed, RolePublicInbox), in.Inbox, false) {
			return rejectProposal(result)
		}
		if !in.InboxOptimized && !sameEndpoints(urlsForRole(parsed, RolePublicInbox), urlsForRole(sourceParsed, RolePublicInbox), false) {
			return rejectProposal(result)
		}
		if in.OutboxOptimized && !sameEndpoints(urlsForRole(parsed, RolePublicOutbox), in.Outbox, false) {
			return rejectProposal(result)
		}
		if !in.OutboxOptimized && !sameEndpoints(urlsForRole(parsed, RolePublicOutbox), urlsForRole(sourceParsed, RolePublicOutbox), false) {
			return rejectProposal(result)
		}
	case in.ListOptimized && blossom:
		if !sameOrderedEndpoints(urlsForRole(parsed, ""), in.Desired, true) {
			return rejectProposal(result)
		}
	case in.ListOptimized:
		if !sameEndpoints(urlsForRole(parsed, ""), in.Desired, false) {
			return rejectProposal(result)
		}
	}
	return result
}

func sameUnmanagedTags(source, next [][]string, in PatchInput) bool {
	left := unmanagedTags(source, in)
	right := unmanagedTags(next, in)
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if len(left[i]) != len(right[i]) {
			return false
		}
		for j := range left[i] {
			if left[i][j] != right[i][j] {
				return false
			}
		}
	}
	return true
}

func unmanagedTags(tags [][]string, in PatchInput) [][]string {
	out := make([][]string, 0)
	for _, tag := range tags {
		if len(tag) == 0 {
			out = append(out, tag)
			continue
		}
		if in.InboxOptimized || in.OutboxOptimized {
			if tag[0] == "r" {
				if _, known := knownMarker(tag); known {
					continue
				}
			}
		} else if in.ListOptimized && managedTag(tag[0], in.TagName) {
			if _, known := knownMarker(tag); known || tag[0] != "r" {
				continue
			}
		}
		out = append(out, tag)
	}
	return out
}

func rejectProposal(result PatchResult) PatchResult {
	result.Safe = false
	result.Complete = false
	result.RequiresReview = true
	result.Event = nil
	result.Blocking = append(result.Blocking, Reason{Code: "proposal_mismatch", Text: "The replacement event does not contain the selected relays. It was not returned for signing."})
	return result
}

func urlsForRole(parsed Parsed, role string) []string {
	out := make([]string, 0, len(parsed.Members))
	for _, member := range parsed.Members {
		if role != "" && member.Role != role {
			continue
		}
		out = append(out, member.EndpointURL)
	}
	return out
}

func sameEndpoints(got, want []string, blossom bool) bool {
	left, right := map[string]int{}, map[string]int{}
	for _, raw := range got {
		left[endpointKey(raw, blossom)]++
	}
	for _, raw := range want {
		right[endpointKey(raw, blossom)]++
	}
	if len(left) != len(right) {
		return false
	}
	for item, count := range left {
		if right[item] != count {
			return false
		}
	}
	return true
}

func sameOrderedEndpoints(got, want []string, blossom bool) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if endpointKey(got[i], blossom) != endpointKey(want[i], blossom) {
			return false
		}
	}
	return true
}

func endpointKey(raw string, blossom bool) string {
	if blossom {
		if normalized, err := NormalizeBlossomURL(raw); err == nil {
			return normalized
		}
		return raw
	}
	if normalized, ok := canonicalRelay(raw); ok {
		return normalized
	}
	return raw
}

type relayMark struct {
	read     bool
	write    bool
	url      string
	tail     []string
	conflict bool
}

func patchNIP65(source [][]string, in PatchInput) ([][]string, bool, string) {
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
		if tail := tagTail(tag); len(tail) > 0 {
			if len(item.tail) > 0 && !sameStrings(item.tail, tail) {
				item.conflict = true
			} else if len(item.tail) == 0 {
				item.tail = append([]string(nil), tail...)
			}
		}
		if marker == "" || marker == "read" {
			item.read = true
		}
		if marker == "" || marker == "write" {
			item.write = true
		}
		existing[norm] = item
	}
	for _, item := range existing {
		if item.conflict {
			return nil, false, "ambiguous_relay_metadata"
		}
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
		return nil, false, ""
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
		if _, done := emitted[norm]; done && len(tag) <= 3 {
			continue
		}
		read, write := finalRead[norm] != "", finalWrite[norm] != ""
		if !read && !write {
			continue
		}
		want := markerFor(read, write)
		next := append([]string(nil), tag...)
		if want != marker {
			next = setMarker(next, want)
		}
		if extra := existing[norm].tail; len(extra) > 0 && len(tagTail(next)) == 0 {
			next = append(next, extra...)
		}
		tags = append(tags, next)
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
		next := markerTag(url, markerFor(read, write))
		if extra := existing[norm].tail; len(extra) > 0 {
			next = append(next, extra...)
		}
		tags = append(tags, next)
		emitted[norm] = struct{}{}
	}
	return tags, true, ""
}

func patchList(source [][]string, tagName string, desired []string) ([][]string, bool, string) {
	if tagName == "" {
		tagName = "relay"
	}
	type listed struct {
		url      string
		tail     []string
		conflict bool
	}
	existing := map[string]listed{}
	for _, tag := range source {
		if len(tag) < 2 || !managedTag(tag[0], tagName) {
			continue
		}
		norm, ok := canonicalEndpoint(tag[1], tag[0] == "server")
		if !ok {
			continue
		}
		item := existing[norm]
		if item.url == "" {
			item.url = tag[1]
		}
		if tail := listTail(tag); len(tail) > 0 {
			if len(item.tail) > 0 && !sameStrings(item.tail, tail) {
				item.conflict = true
			} else if len(item.tail) == 0 {
				item.tail = append([]string(nil), tail...)
			}
		}
		existing[norm] = item
	}
	for _, item := range existing {
		if item.conflict {
			return nil, false, "ambiguous_relay_metadata"
		}
	}
	final := map[string]string{}
	for _, raw := range desired {
		norm, ok := canonicalEndpoint(raw, tagName == "server")
		if !ok {
			continue
		}
		if item, seen := existing[norm]; seen {
			final[norm] = item.url
		} else {
			final[norm] = raw
		}
	}
	current := map[string]string{}
	for norm, item := range existing {
		current[norm] = item.url
	}
	if sameSet(keysOfMap(final), keysOfString(current)) {
		return nil, false, ""
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
		next := append([]string(nil), tag...)
		if extra := existing[norm].tail; len(extra) > 0 && len(listTail(next)) == 0 {
			next = append(next, extra...)
		}
		tags = append(tags, next)
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
		next := []string{tagName, final[norm]}
		if extra := existing[norm].tail; len(extra) > 0 {
			next = append(next, extra...)
		}
		tags = append(tags, next)
		emitted[norm] = struct{}{}
	}
	return tags, true, ""
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
	next := append([]string(nil), tag...)
	if marker == "" {
		if len(next) == 3 && (next[2] == "read" || next[2] == "write" || strings.TrimSpace(next[2]) == "") {
			return next[:2]
		}
		if len(next) >= 3 && (next[2] == "read" || next[2] == "write") {
			next[2] = ""
		}
		return next
	}
	if len(next) < 3 {
		return append(next, marker)
	}
	if next[2] == "read" || next[2] == "write" || strings.TrimSpace(next[2]) == "" {
		next[2] = marker
		return next
	}
	return append([]string{next[0], next[1], marker}, next[2:]...)
}

func markerTag(url, marker string) []string {
	if marker == "" {
		return []string{"r", url}
	}
	return []string{"r", url, marker}
}

func managedTag(name, tagName string) bool {
	if name == tagName {
		return true
	}
	// Relay-list kinds accept a historical r tag as well as relay.
	// Blossom stays on server, so an r tag there is left untouched.
	return tagName == "relay" && name == "r"
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

func tagTail(tag []string) []string {
	if len(tag) <= 3 {
		return nil
	}
	return tag[3:]
}

func listTail(tag []string) []string {
	if len(tag) <= 2 {
		return nil
	}
	return tag[2:]
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func managedMetadataKept(source, next [][]string, in PatchInput) bool {
	nip65 := in.InboxOptimized || in.OutboxOptimized
	want := map[string]string{}
	for _, tag := range source {
		norm, tail, ok := managedIdentity(tag, nip65, in.TagName)
		if !ok || tail == "" {
			continue
		}
		if prev, seen := want[norm]; seen && prev != tail {
			return false
		}
		want[norm] = tail
	}
	got := map[string]string{}
	for _, tag := range next {
		norm, tail, ok := managedIdentity(tag, nip65, in.TagName)
		if !ok {
			continue
		}
		got[norm] = tail
	}
	for norm, tail := range want {
		if _, present := got[norm]; !present {
			continue
		}
		if got[norm] != tail {
			return false
		}
	}
	return true
}

func managedIdentity(tag []string, nip65 bool, tagName string) (string, string, bool) {
	if len(tag) < 2 {
		return "", "", false
	}
	if nip65 {
		if tag[0] != "r" {
			return "", "", false
		}
		if _, known := knownMarker(tag); !known {
			return "", "", false
		}
		norm, ok := canonicalRelay(tag[1])
		if !ok {
			return "", "", false
		}
		return norm, strings.Join(tagTail(tag), "\x00"), true
	}
	if !managedTag(tag[0], tagName) {
		return "", "", false
	}
	norm, ok := canonicalEndpoint(tag[1], tag[0] == "server" || tagName == "server")
	if !ok {
		return "", "", false
	}
	return norm, strings.Join(listTail(tag), "\x00"), true
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
