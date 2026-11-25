package relayintel

import (
	"sort"
	"strings"
)

const (
	ActionAdd         = "add"
	ActionRemove      = "remove"
	ActionReplace     = "replace"
	ActionRetain      = "retain"
	ActionInvestigate = "investigate"
)

// Recommendation is one actionable change. The client signs anything it publishes.
type Recommendation struct {
	Action          string   `json:"action"`
	Role            string   `json:"role"`
	Current         string   `json:"current,omitempty"`
	Suggested       string   `json:"suggested,omitempty"`
	ReasonCodes     []string `json:"reason_codes"`
	Text            string   `json:"text"`
	Benefit         float64  `json:"estimated_benefit,omitempty"`
	Confidence      float64  `json:"confidence"`
	ConfidenceLabel string   `json:"confidence_label"`
	Attention       bool     `json:"attention_required"`
	Limitations     []string `json:"limitations,omitempty"`
}

// UnsignedEvent is a replacement the client may sign. This service never signs it.
type UnsignedEvent struct {
	Kind             int        `json:"kind"`
	Pubkey           string     `json:"pubkey"`
	Content          string     `json:"content"`
	Tags             [][]string `json:"tags"`
	BasedOnEventID   string     `json:"based_on_event_id,omitempty"`
	BasedOnCreatedAt int64      `json:"based_on_created_at,omitempty"`
	Note             string     `json:"note"`
}

// RolePlan is the optimizer output for one role.
type RolePlan struct {
	Role            string           `json:"role"`
	Availability    string           `json:"availability"`
	Fallback        string           `json:"fallback,omitempty"`
	Selection       SelectResult     `json:"selection"`
	Recommendations []Recommendation `json:"recommendations"`
	Ranking         []ScoredRelay    `json:"ranking,omitempty"`
}

// BuildRecommendations turns a current list and a selected set into actions.
// Health problems become "investigate" until failure is sustained and the
// relay is also redundant.
func BuildRecommendations(role string, current []DeclaredRelay, selected []SelectedRelay, scored map[string]ScoredRelay, sustainedDown map[string]bool) []Recommendation {
	currentSet := map[string]DeclaredRelay{}
	for _, relay := range current {
		currentSet[relay.URL] = relay
	}
	proposed := map[string]SelectedRelay{}
	for _, relay := range selected {
		proposed[relay.URL] = relay
	}
	var removed, added []string
	for url := range currentSet {
		if _, ok := proposed[url]; !ok {
			removed = append(removed, url)
		}
	}
	for url := range proposed {
		if _, ok := currentSet[url]; !ok {
			added = append(added, url)
		}
	}
	sort.Strings(removed)
	sort.Strings(added)
	usedAdd := map[string]struct{}{}
	var out []Recommendation
	for _, url := range removed {
		if sustainedDown[url] {
			if partner := bestPartner(url, added, usedAdd, scored); partner != "" {
				usedAdd[partner] = struct{}{}
				out = append(out, action(ActionReplace, role, url, partner, scored, "Sustained read failures plus overlapping coverage support a replacement."))
				continue
			}
		}
		if !sustainedDown[url] && needsLook(scored[url]) {
			item := action(ActionInvestigate, role, url, "", scored, "Health looks uncertain. The relay stays until the failure is sustained.")
			out = append(out, item)
			continue
		}
		item := action(ActionRemove, role, url, "", scored, "The proposed set no longer needs this relay.")
		if role == RoleDMInbox || role == RoleKeyPackage {
			item.Attention = true
			item.Limitations = append(item.Limitations, attentionLimit(role))
		}
		out = append(out, item)
	}
	for _, url := range added {
		if _, ok := usedAdd[url]; ok {
			continue
		}
		item := action(ActionAdd, role, "", url, scored, "This relay adds coverage or capability the current set is missing.")
		if role == RoleBlossom {
			item.Action = ActionInvestigate
			item.Attention = true
			item.Text = "Blossom popularity alone is not a reason to move stored media."
			item.ReasonCodes = append(item.ReasonCodes, "blossom_popularity_not_sufficient")
		}
		out = append(out, item)
	}
	var retained []string
	for url := range currentSet {
		if _, ok := proposed[url]; ok {
			retained = append(retained, url)
		}
	}
	sort.Strings(retained)
	for _, url := range retained {
		if sustainedDown[url] {
			out = append(out, action(ActionInvestigate, role, url, "", scored, "This relay is still in the set, and its read probes have been failing. It was not removed automatically."))
			continue
		}
		out = append(out, action(ActionRetain, role, url, url, scored, "This relay stays."))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Action != out[j].Action {
			return out[i].Action < out[j].Action
		}
		if out[i].Current != out[j].Current {
			return out[i].Current < out[j].Current
		}
		return out[i].Suggested < out[j].Suggested
	})
	return out
}

func needsLook(relay ScoredRelay) bool {
	for _, reason := range relay.Reasons {
		switch reason.Code {
		case "connection_failures", "stale_probe":
			return true
		}
	}
	return false
}

func bestPartner(removed string, added []string, used map[string]struct{}, scored map[string]ScoredRelay) string {
	removedCovers := scored[removed].Covers
	best := ""
	bestOverlap := 0.0
	for _, url := range added {
		if _, ok := used[url]; ok {
			continue
		}
		overlap := 0.0
		for pubkey, weight := range removedCovers {
			if _, ok := scored[url].Covers[pubkey]; ok {
				overlap += weight
			}
		}
		if overlap > bestOverlap || (overlap == bestOverlap && overlap > 0 && (best == "" || url < best)) {
			best = url
			bestOverlap = overlap
		}
	}
	if bestOverlap <= 0 {
		return ""
	}
	return best
}

func action(kind, role, current, suggested string, scored map[string]ScoredRelay, text string) Recommendation {
	ref := suggested
	if ref == "" {
		ref = current
	}
	relay := scored[ref]
	codes := make([]string, 0, len(relay.Reasons))
	for _, reason := range relay.Reasons {
		codes = append(codes, reason.Code)
	}
	if len(codes) == 0 {
		codes = append(codes, kind)
	}
	conf := relay.Confidence
	label := relay.ConfidenceLabel
	if label == "" {
		label = confidenceLabel(conf)
	}
	benefit := 0.0
	if relay.Score != nil {
		benefit = *relay.Score
	}
	return Recommendation{
		Action: kind, Role: role, Current: current, Suggested: suggested,
		ReasonCodes: codes, Text: text, Benefit: round3(benefit),
		Confidence: conf, ConfidenceLabel: label,
	}
}

func attentionLimit(role string) string {
	switch role {
	case RoleDMInbox:
		return "Historical direct messages are not migrated."
	case RoleKeyPackage:
		return "Peers will not find a new KeyPackage until the client publishes one on the replacement relay."
	default:
		return ""
	}
}

// NIP65Event merges inbox and outbox proposals into one kind 10002 event.
// Unknown tags from the source event are copied. Kind 3 is never emitted.
func NIP65Event(pubkey string, sourceTags [][]string, sourceID string, sourceCreated int64, inbox, outbox []string) UnsignedEvent {
	read := map[string]struct{}{}
	write := map[string]struct{}{}
	for _, item := range inbox {
		if item = strings.TrimSpace(item); item != "" {
			read[item] = struct{}{}
		}
	}
	for _, item := range outbox {
		if item = strings.TrimSpace(item); item != "" {
			write[item] = struct{}{}
		}
	}
	urls := map[string]struct{}{}
	for url := range read {
		urls[url] = struct{}{}
	}
	for url := range write {
		urls[url] = struct{}{}
	}
	ordered := make([]string, 0, len(urls))
	for url := range urls {
		ordered = append(ordered, url)
	}
	sort.Strings(ordered)
	tags := make([][]string, 0, len(sourceTags)+len(ordered))
	for _, tag := range sourceTags {
		if len(tag) == 0 || tag[0] == "r" {
			continue
		}
		tags = append(tags, append([]string(nil), tag...))
	}
	for _, url := range ordered {
		_, isRead := read[url]
		_, isWrite := write[url]
		switch {
		case isRead && isWrite:
			tags = append(tags, []string{"r", url})
		case isRead:
			tags = append(tags, []string{"r", url, "read"})
		default:
			tags = append(tags, []string{"r", url, "write"})
		}
	}
	return UnsignedEvent{
		Kind: KindPublicRelayList, Pubkey: pubkey, Content: "", Tags: tags,
		BasedOnEventID: sourceID, BasedOnCreatedAt: sourceCreated,
		Note: "Unsigned kind 10002. Inbox and outbox are one event. The client must set created_at, id, and sig. Unknown non-r tags from the source event were copied.",
	}
}

// ListEvent builds an unsigned replaceable list for a single-role kind.
func ListEvent(kind int, pubkey, tagName, sourceID string, sourceCreated int64, sourceTags [][]string, relays []string) UnsignedEvent {
	tags := make([][]string, 0)
	for _, tag := range sourceTags {
		if len(tag) == 0 || tag[0] == tagName || tag[0] == "r" || tag[0] == "server" {
			continue
		}
		tags = append(tags, append([]string(nil), tag...))
	}
	ordered := append([]string(nil), relays...)
	sort.Strings(ordered)
	seen := map[string]struct{}{}
	for _, relay := range ordered {
		relay = strings.TrimSpace(relay)
		if relay == "" {
			continue
		}
		if _, ok := seen[relay]; ok {
			continue
		}
		seen[relay] = struct{}{}
		tags = append(tags, []string{tagName, relay})
	}
	return UnsignedEvent{
		Kind: kind, Pubkey: pubkey, Content: "", Tags: tags,
		BasedOnEventID: sourceID, BasedOnCreatedAt: sourceCreated,
		Note: "Unsigned replacement. The client must sign and publish it. This service will not.",
	}
}
