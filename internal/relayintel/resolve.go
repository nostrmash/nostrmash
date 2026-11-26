package relayintel

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nostrmash/nostrmash/internal/nostr"
)

// maxAuthoritativeEndpoints is the largest list this request will optimize.
// The stored analytical projection stays at MaxEndpointsPerEvent. A complete
// source between those two sizes is used in full. A larger source is reported
// as truncated and is not patched.
const maxAuthoritativeEndpoints = 80

// resolvedSource is the one event the optimizer and the patch both use.
type resolvedSource struct {
	Raw       json.RawMessage
	Origin    string
	EventID   string
	CreatedAt int64
	Complete  bool
	Kind      int
	DTag      string
}

type sourceNote struct {
	kind   int
	dTag   string
	reason Reason
}

type deletionView struct {
	ByEvent map[string]bool
	ByCoord map[string]int64
}

func (s *Service) resolveAccount(ctx context.Context, pubkey string, heads []Head, members []Membership, client map[string]SourceEvent) (AccountConfig, map[string]resolvedSource, []sourceNote, error) {
	ids := make([]string, 0, len(heads)+len(client))
	for _, head := range heads {
		if head.EventID != "" {
			ids = append(ids, head.EventID)
		}
	}
	for _, item := range client {
		if item.ID != "" {
			ids = append(ids, item.ID)
		}
	}
	indexed, err := loadEventRaw(ctx, s.pool, ids)
	if err != nil {
		return AccountConfig{}, nil, nil, err
	}
	deleted, err := loadDeletions(ctx, s.pool, pubkey, ids)
	if err != nil {
		return AccountConfig{}, nil, nil, err
	}
	account, resolved, notes := applyAuthoritative(pubkey, heads, members, indexed, client, deleted)
	return account, resolved, notes, nil
}

func loadDeletions(ctx context.Context, pool *pgxpool.Pool, pubkey string, ids []string) (deletionView, error) {
	view := deletionView{ByEvent: map[string]bool{}, ByCoord: map[string]int64{}}
	rows, err := pool.Query(ctx, `
		SELECT kind, d_tag, max(created_at)
		FROM deletion_addresses
		WHERE pubkey = $1
		GROUP BY kind, d_tag
	`, pubkey)
	if err != nil {
		if SchemaMissing(err) {
			return view, nil
		}
		return view, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind int
		var dTag string
		var created int64
		if err := rows.Scan(&kind, &dTag, &created); err != nil {
			return view, err
		}
		view.ByCoord[sourceKey(kind, dTag)] = created
	}
	if err := rows.Err(); err != nil {
		return view, err
	}
	rows.Close()
	if len(ids) == 0 {
		return view, nil
	}
	idRows, err := pool.Query(ctx, `
		SELECT target_event_id
		FROM deletion_events
		WHERE deleter_pubkey = $1 AND target_event_id = ANY($2::text[])
	`, pubkey, ids)
	if err != nil {
		if SchemaMissing(err) {
			return view, nil
		}
		return view, err
	}
	defer idRows.Close()
	for idRows.Next() {
		var id string
		if err := idRows.Scan(&id); err != nil {
			return view, err
		}
		view.ByEvent[id] = true
	}
	return view, idRows.Err()
}

func applyAuthoritative(pubkey string, heads []Head, members []Membership, indexed map[string]json.RawMessage, client map[string]SourceEvent, deleted deletionView) (AccountConfig, map[string]resolvedSource, []sourceNote) {
	if indexed == nil {
		indexed = map[string]json.RawMessage{}
	}
	if deleted.ByEvent == nil {
		deleted.ByEvent = map[string]bool{}
	}
	if deleted.ByCoord == nil {
		deleted.ByCoord = map[string]int64{}
	}
	type coord struct {
		kind int
		dTag string
	}
	order := make([]coord, 0, len(heads)+len(client))
	seen := map[string]struct{}{}
	add := func(kind int, dTag string) {
		key := sourceKey(kind, dTag)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		order = append(order, coord{kind: kind, dTag: dTag})
	}
	for _, head := range heads {
		add(head.SourceKind, head.DTag)
	}
	for _, item := range client {
		add(item.Kind, item.DTag)
	}

	resolved := map[string]resolvedSource{}
	var notes []sourceNote
	for _, item := range order {
		key := sourceKey(item.kind, item.dTag)
		var head Head
		has := false
		for _, candidate := range heads {
			if candidate.SourceKind == item.kind && candidate.DTag == item.dTag {
				head = candidate
				has = true
				break
			}
		}
		supplied, hasClient := client[key]
		indexedGone := has && eventSuppressed(head.EventID, head.CreatedAt, item.kind, item.dTag, deleted)
		clientGone := hasClient && eventSuppressed(supplied.ID, supplied.CreatedAt, supplied.Kind, supplied.DTag, deleted)
		useClient := false
		switch {
		case hasClient && !clientGone && (!has || indexedGone || nostr.Supersedes(supplied.CreatedAt, supplied.ID, head.CreatedAt, head.EventID)):
			useClient = true
		case has && !indexedGone:
			useClient = false
		default:
			heads, members = dropCoordinate(heads, members, item.kind, item.dTag)
			resolved[key] = resolvedSource{Origin: "deleted", Kind: item.kind, DTag: item.dTag}
			notes = append(notes, sourceNote{kind: item.kind, dTag: item.dTag, reason: Reason{Code: "source_deleted", Text: "A deletion covers this list. The deleted event was not used as the current configuration."}})
			continue
		}
		if hasClient && !useClient && !clientGone && supplied.ID != head.EventID {
			notes = append(notes, sourceNote{kind: item.kind, dTag: item.dTag, reason: Reason{Code: "stale_source", Text: "A supplied event is older than the indexed event under NIP-01 ordering. The newer indexed event was used."}})
		}

		raw := indexed[head.EventID]
		origin := "indexed"
		eventID := head.EventID
		created := head.CreatedAt
		if useClient {
			raw = supplied.Raw
			origin = "client"
			eventID = supplied.ID
			created = supplied.CreatedAt
		}
		itemResolved := resolvedSource{
			Raw: raw, Origin: origin, EventID: eventID, CreatedAt: created,
			Kind: item.kind, DTag: item.dTag, Complete: len(raw) > 0 && has && !head.Truncated,
		}
		if len(raw) > 0 {
			input, err := eventInputFromRaw(raw, maxAuthoritativeEndpoints)
			var parsed Parsed
			var parseErr error
			if err == nil {
				parsed, parseErr = Parse(input)
			}
			if err != nil || parseErr != nil {
				itemResolved.Complete = false
				notes = append(notes, sourceNote{kind: item.kind, dTag: item.dTag, reason: Reason{Code: "source_unreadable", Text: "The authoritative event could not be parsed. No replacement was generated from it."}})
			} else {
				heads, members = replaceCoordinate(heads, members, parsed)
				itemResolved.Complete = !parsed.Head.Truncated
				itemResolved.EventID = parsed.Head.EventID
				itemResolved.CreatedAt = parsed.Head.CreatedAt
				if parsed.Head.Truncated {
					notes = append(notes, sourceNote{kind: item.kind, dTag: item.dTag, reason: Reason{Code: "source_too_large", Text: "This list is larger than the optimizer can represent. It was not silently truncated into a replacement."}})
				}
			}
		} else if has && head.Truncated {
			itemResolved.Complete = false
		} else if !has {
			itemResolved.Origin = "missing"
			itemResolved.Complete = false
		}
		resolved[key] = itemResolved
	}
	return EffectiveConfig(pubkey, heads, members), resolved, notes
}

func eventSuppressed(eventID string, created int64, kind int, dTag string, deleted deletionView) bool {
	if eventID != "" && deleted.ByEvent[eventID] {
		return true
	}
	if ts, ok := deleted.ByCoord[sourceKey(kind, dTag)]; ok && created <= ts {
		return true
	}
	return false
}

func eventInputFromRaw(raw json.RawMessage, limit int) (EventInput, error) {
	var ev struct {
		ID        string     `json:"id"`
		Pubkey    string     `json:"pubkey"`
		CreatedAt int64      `json:"created_at"`
		Kind      int        `json:"kind"`
		Content   string     `json:"content"`
		Tags      [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return EventInput{}, err
	}
	return EventInput{
		Pubkey: strings.ToLower(ev.Pubkey), EventID: ev.ID, Kind: ev.Kind,
		CreatedAt: ev.CreatedAt, Content: ev.Content, Tags: ev.Tags, EndpointLimit: limit,
	}, nil
}

func replaceCoordinate(heads []Head, members []Membership, parsed Parsed) ([]Head, []Membership) {
	heads, members = dropCoordinate(heads, members, parsed.Head.SourceKind, parsed.Head.DTag)
	heads = append(heads, parsed.Head)
	members = append(members, parsed.Members...)
	return heads, members
}

func dropCoordinate(heads []Head, members []Membership, kind int, dTag string) ([]Head, []Membership) {
	nextHeads := make([]Head, 0, len(heads))
	for _, head := range heads {
		if head.SourceKind == kind && head.DTag == dTag {
			continue
		}
		nextHeads = append(nextHeads, head)
	}
	nextMembers := make([]Membership, 0, len(members))
	for _, member := range members {
		if member.SourceKind == kind && member.DTag == dTag {
			continue
		}
		nextMembers = append(nextMembers, member)
	}
	return nextHeads, nextMembers
}

func foldSharedRoles(roles []string, account AccountConfig) ([]string, []Reason) {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(roles))
	hasKeyPackage := false
	for _, role := range roles {
		if role == RoleKeyPackage {
			hasKeyPackage = true
		}
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
		out = append(out, role)
	}
	if !hasKeyPackage || account.KeyPackageDiscovery != "nip65_outbox" {
		return out, nil
	}
	next := make([]string, 0, len(out)+1)
	for _, role := range out {
		if role == RoleKeyPackage {
			continue
		}
		next = append(next, role)
	}
	if _, ok := seen[RolePublicOutbox]; !ok {
		next = append(next, RolePublicOutbox)
	}
	return next, []Reason{{
		Code: "keypackage_follows_outbox",
		Text: "KeyPackage discovery uses the public outbox. This request optimized that shared kind 10002 list instead of emitting a separate kind 10051 list. A messaging client must publish a new KeyPackage after the outbox changes.",
	}}
}
