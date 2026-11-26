package relayintel

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/nostrmash/nostrmash/internal/nostr"
)

// resolveBarrier runs after a request has read canonical state and before it
// builds the response. Tests use it to commit a concurrent canonical change.
// The read snapshot must not observe that change. Production leaves it unset.
var (
	resolveBarrierMu sync.Mutex
	resolveBarrier   func()
)

// SetResolveBarrier installs the test hook described above. A nil function removes it.
func SetResolveBarrier(fn func()) {
	resolveBarrierMu.Lock()
	resolveBarrier = fn
	resolveBarrierMu.Unlock()
}

func runResolveBarrier() {
	resolveBarrierMu.Lock()
	fn := resolveBarrier
	resolveBarrierMu.Unlock()
	if fn != nil {
		fn()
	}
}

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

// SourceReport is the locally known relationship between a projection head
// and the canonical replaceable winner.
type SourceReport struct {
	Kind              int    `json:"kind"`
	DTag              string `json:"d_tag,omitempty"`
	ProjectionEventID string `json:"projection_event_id,omitempty"`
	CanonicalEventID  string `json:"canonical_event_id,omitempty"`
	Status            string `json:"status"`
	RequiresRefresh   bool   `json:"requires_refresh"`
	Note              string `json:"note,omitempty"`
}

func sourceReports(before []Head, resolved map[string]resolvedSource, notes []sourceNote) []SourceReport {
	projection := map[string]string{}
	for _, head := range before {
		projection[sourceKey(head.SourceKind, head.DTag)] = head.EventID
	}
	noteBy := map[string]Reason{}
	rank := map[string]int{
		"projection_missing":      1,
		"projection_stale":        1,
		"projection_inconsistent": 2,
		"stale_source":            2,
		"source_too_large":        2,
		"source_unreadable":       3,
		"source_deleted":          4,
	}
	for _, note := range notes {
		key := sourceKey(note.kind, note.dTag)
		if previous, ok := noteBy[key]; ok && rank[previous.Code] > rank[note.reason.Code] {
			continue
		}
		noteBy[key] = note.reason
	}
	keys := map[string]struct{}{}
	for key := range projection {
		keys[key] = struct{}{}
	}
	for key := range resolved {
		keys[key] = struct{}{}
	}
	out := make([]SourceReport, 0, len(keys))
	for key := range keys {
		kind, dTag, ok := splitSourceKey(key)
		if !ok {
			continue
		}
		item := resolved[key]
		status, refresh := "current", false
		if reason, found := noteBy[key]; found {
			switch reason.Code {
			case "projection_stale":
				status, refresh = "stale", true
			case "projection_inconsistent":
				status, refresh = "inconsistent", true
			case "projection_missing":
				status, refresh = "missing", true
			case "source_deleted":
				status, refresh = "deleted", true
			case "source_too_large":
				status, refresh = "too_large", true
			case "source_unreadable":
				status, refresh = "unreadable", true
			case "stale_source":
				status, refresh = "stale_client", true
			}
		}
		canonical := item.EventID
		if projection[key] != "" && canonical != "" && projection[key] != canonical && status == "current" {
			status, refresh = "stale", true
		}
		if projection[key] == "" && canonical != "" && status == "current" {
			status, refresh = "missing", true
		}
		text := ""
		if reason, found := noteBy[key]; found {
			text = reason.Text
		}
		out = append(out, SourceReport{
			Kind: kind, DTag: dTag, ProjectionEventID: projection[key], CanonicalEventID: canonical,
			Status: status, RequiresRefresh: refresh, Note: text,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind == out[j].Kind {
			return out[i].DTag < out[j].DTag
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

type deletionView struct {
	ByEvent map[string]bool
	ByCoord map[string]int64
}

func (s *Service) resolveAccount(ctx context.Context, pubkey string, client map[string]SourceEvent) (AccountConfig, map[string]resolvedSource, []sourceNote, []SourceReport, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return AccountConfig{}, nil, nil, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	heads, members, err := loadAccount(ctx, tx, pubkey)
	if err != nil {
		return AccountConfig{}, nil, nil, nil, err
	}
	winners, err := loadCanonical(ctx, tx, pubkey)
	if err != nil {
		return AccountConfig{}, nil, nil, nil, err
	}
	original := append([]Head(nil), heads...)
	ids := make([]string, 0, len(heads)+len(winners)+len(client))
	for _, head := range heads {
		if head.EventID != "" {
			ids = append(ids, head.EventID)
		}
	}
	for _, winner := range winners {
		if winner.EventID != "" {
			ids = append(ids, winner.EventID)
		}
	}
	for _, item := range client {
		if item.ID != "" {
			ids = append(ids, item.ID)
		}
	}
	deleted, err := loadDeletions(ctx, tx, pubkey, ids)
	if err != nil {
		return AccountConfig{}, nil, nil, nil, err
	}
	heads, canonNotes := preferCanonical(heads, winners, deleted)
	chosen := make([]string, 0, len(heads)+len(client))
	for _, head := range heads {
		if head.EventID != "" {
			chosen = append(chosen, head.EventID)
		}
	}
	for _, item := range client {
		if item.ID != "" {
			chosen = append(chosen, item.ID)
		}
	}
	indexed, err := loadEventRaw(ctx, tx, chosen)
	if err != nil {
		return AccountConfig{}, nil, nil, nil, err
	}
	runResolveBarrier()
	if err := tx.Commit(ctx); err != nil {
		return AccountConfig{}, nil, nil, nil, err
	}
	account, resolved, notes := applyAuthoritative(pubkey, heads, members, indexed, client, deleted)
	notes = append(notes, canonNotes...)
	// A deleted canonical winner is not installed. When no live projection
	// remains, the coordinate is still reported as deleted so inspection
	// does not treat the list as merely absent.
	for _, winner := range winners {
		if winner.EventID == "" || !HandlesKind(winner.Kind) || !eventSuppressed(winner.EventID, winner.CreatedAt, winner.Kind, winner.DTag, deleted) {
			continue
		}
		key := sourceKey(winner.Kind, winner.DTag)
		if _, ok := resolved[key]; ok {
			continue
		}
		live := false
		for _, head := range heads {
			if head.SourceKind == winner.Kind && head.DTag == winner.DTag && !eventSuppressed(head.EventID, head.CreatedAt, head.SourceKind, head.DTag, deleted) {
				live = true
				break
			}
		}
		if live {
			continue
		}
		resolved[key] = resolvedSource{Origin: "deleted", Kind: winner.Kind, DTag: winner.DTag, EventID: winner.EventID, CreatedAt: winner.CreatedAt}
		notes = append(notes, sourceNote{kind: winner.Kind, dTag: winner.DTag, reason: Reason{Code: "source_deleted", Text: "A deletion covers this list. The deleted event was not used as the current configuration."}})
	}
	return account, resolved, notes, sourceReports(original, resolved, notes), nil
}

type canonicalWinner struct {
	Kind      int
	DTag      string
	EventID   string
	CreatedAt int64
}

func loadCanonical(ctx context.Context, pool dbQuery, pubkey string) ([]canonicalWinner, error) {
	rows, err := pool.Query(ctx, `
		SELECT kind, d_tag, event_id, created_at
		FROM replaceable_state
		WHERE pubkey = $1
	`, pubkey)
	if err != nil {
		if SchemaMissing(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()
	var out []canonicalWinner
	for rows.Next() {
		var row canonicalWinner
		if err := rows.Scan(&row.Kind, &row.DTag, &row.EventID, &row.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// preferCanonical uses canonical replaceable state when it is the NIP-01
// winner, and also when the stored projection is a deleted event. A live
// newer projection is left alone. A deleted canonical row is not installed.
func preferCanonical(heads []Head, winners []canonicalWinner, deleted deletionView) ([]Head, []sourceNote) {
	index := map[string]int{}
	for i, head := range heads {
		index[sourceKey(head.SourceKind, head.DTag)] = i
	}
	var notes []sourceNote
	for _, winner := range winners {
		if winner.EventID == "" || !HandlesKind(winner.Kind) {
			continue
		}
		if eventSuppressed(winner.EventID, winner.CreatedAt, winner.Kind, winner.DTag, deleted) {
			continue
		}
		key := sourceKey(winner.Kind, winner.DTag)
		position, ok := index[key]
		if !ok {
			heads = append(heads, Head{
				SourceKind: winner.Kind, DTag: winner.DTag, EventID: winner.EventID, CreatedAt: winner.CreatedAt,
				DeclarationSource: SourceModern,
			})
			index[key] = len(heads) - 1
			notes = append(notes, sourceNote{kind: winner.Kind, dTag: winner.DTag, reason: Reason{Code: "projection_missing", Text: "Canonical replaceable state has a winner that the relay projection has not stored yet."}})
			continue
		}
		head := heads[position]
		if head.EventID == winner.EventID {
			continue
		}
		if eventSuppressed(head.EventID, head.CreatedAt, head.SourceKind, head.DTag, deleted) {
			heads[position] = Head{
				Pubkey: head.Pubkey, SourceKind: winner.Kind, DTag: winner.DTag,
				EventID: winner.EventID, CreatedAt: winner.CreatedAt,
				DeclarationSource: head.DeclarationSource,
			}
			notes = append(notes, sourceNote{kind: winner.Kind, dTag: winner.DTag, reason: Reason{Code: "projection_inconsistent", Text: "The stored projection is covered by a deletion. The canonical event remains authoritative and was used."}})
			continue
		}
		if nostr.Supersedes(winner.CreatedAt, winner.EventID, head.CreatedAt, head.EventID) {
			heads[position].EventID = winner.EventID
			heads[position].CreatedAt = winner.CreatedAt
			heads[position].ExplicitlyEmpty = false
			heads[position].ClientSideOnly = false
			heads[position].Truncated = false
			heads[position].InvalidEndpoints = 0
			notes = append(notes, sourceNote{kind: winner.Kind, dTag: winner.DTag, reason: Reason{Code: "projection_stale", Text: "The relay projection is older than the canonical replaceable winner. The canonical event was used."}})
		}
	}
	return heads, notes
}

func loadDeletions(ctx context.Context, pool dbQuery, pubkey string, ids []string) (deletionView, error) {
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
				heads, members = dropCoordinate(heads, members, item.kind, item.dTag)
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
		} else {
			heads, members = dropCoordinate(heads, members, item.kind, item.dTag)
			itemResolved.Complete = false
			itemResolved.Origin = "indexed"
			notes = append(notes, sourceNote{kind: item.kind, dTag: item.dTag, reason: Reason{Code: "source_unreadable", Text: "The authoritative event could not be loaded. No replacement was generated from the stored projection."}})
		}
		resolved[key] = itemResolved
	}
	for key, created := range deleted.ByCoord {
		if _, ok := resolved[key]; ok {
			continue
		}
		kind, dTag, ok := splitSourceKey(key)
		if !ok || created < 0 {
			continue
		}
		resolved[key] = resolvedSource{Origin: "deleted", Kind: kind, DTag: dTag}
		notes = append(notes, sourceNote{kind: kind, dTag: dTag, reason: Reason{Code: "source_deleted", Text: "A deletion covers this list. The deleted event was not used as the current configuration."}})
	}
	return EffectiveConfig(pubkey, heads, members), resolved, notes
}

func splitSourceKey(key string) (int, string, bool) {
	kindText, dTag, ok := strings.Cut(key, "\x00")
	if !ok {
		return 0, "", false
	}
	kind, err := strconv.Atoi(kindText)
	if err != nil {
		return 0, "", false
	}
	return kind, dTag, true
}

func eventSuppressed(eventID string, created int64, kind int, dTag string, deleted deletionView) bool {
	if eventID != "" && deleted.ByEvent[eventID] {
		return true
	}
	if AddressableKind(kind) {
		if ts, ok := deleted.ByCoord[sourceKey(kind, dTag)]; ok && created <= ts {
			return true
		}
		return false
	}
	for key, ts := range deleted.ByCoord {
		coordKind, _, ok := splitSourceKey(key)
		if ok && coordKind == kind && created <= ts {
			return true
		}
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

func foldSharedRoles(roles []string, account AccountConfig, constraints *Constraints) ([]string, []Reason) {
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
	notes := []Reason{{
		Code: "keypackage_follows_outbox",
		Text: "KeyPackage discovery uses the public outbox. This request optimized that shared kind 10002 list instead of emitting a separate kind 10051 list. A messaging client must publish a new KeyPackage after the outbox changes.",
	}}
	notes = append(notes, mergeKeyPackageConstraints(constraints)...)
	return next, notes
}

// mergeKeyPackageConstraints copies KeyPackage pins, exclusions, and counts
// onto the public outbox, which is the list a modern client actually publishes.
func mergeKeyPackageConstraints(c *Constraints) []Reason {
	if c == nil || len(c.ByRole) == 0 {
		return nil
	}
	kp, ok := c.ByRole[RoleKeyPackage]
	if !ok {
		return nil
	}
	ob := c.ByRole[RolePublicOutbox]
	var notes []Reason
	if pins := unionStrings(c.Pinned, derefStrings(ob.Pinned), derefStrings(kp.Pinned)); ob.Pinned != nil || kp.Pinned != nil {
		ob.Pinned = &pins
	}
	if excluded := unionStrings(derefStrings(ob.Excluded), derefStrings(kp.Excluded)); ob.Excluded != nil || kp.Excluded != nil {
		ob.Excluded = &excluded
	}
	switch {
	case kp.Max != nil && ob.Max != nil && *kp.Max != *ob.Max:
		smaller := *kp.Max
		if *ob.Max < smaller {
			smaller = *ob.Max
		}
		ob.Max = &smaller
	case kp.Max != nil && ob.Max == nil:
		value := *kp.Max
		ob.Max = &value
	}
	if kp.Min != nil && ob.Max != nil && *kp.Min > *ob.Max {
		notes = append(notes, Reason{Code: "shared_constraint_conflict", Text: "The KeyPackage minimum is larger than the shared outbox maximum. Both requirements cannot be met."})
	} else if kp.Min != nil && (ob.Min == nil || *kp.Min > *ob.Min) {
		value := *kp.Min
		ob.Min = &value
	}
	if kp.MinReliability != nil && (ob.MinReliability == nil || *kp.MinReliability > *ob.MinReliability) {
		value := *kp.MinReliability
		ob.MinReliability = &value
	}
	if preferred := unionStrings(derefStrings(ob.Preferred), derefStrings(kp.Preferred)); ob.Preferred != nil || kp.Preferred != nil {
		ob.Preferred = &preferred
	}
	if custom := unionStrings(derefStrings(ob.Custom), derefStrings(kp.Custom)); ob.Custom != nil || kp.Custom != nil {
		ob.Custom = &custom
	}
	if paid := unionStrings(derefStrings(ob.PaidAccess), derefStrings(kp.PaidAccess)); ob.PaidAccess != nil || kp.PaidAccess != nil {
		ob.PaidAccess = &paid
	}
	if kp.FreeOnly != nil && (ob.FreeOnly == nil || *kp.FreeOnly) {
		value := *kp.FreeOnly
		ob.FreeOnly = &value
	}
	if kp.Diversity != nil && (ob.Diversity == nil || *kp.Diversity < *ob.Diversity) {
		value := *kp.Diversity
		ob.Diversity = &value
	}
	if kp.RetainExisting != nil && (ob.RetainExisting == nil || *kp.RetainExisting) {
		value := *kp.RetainExisting
		ob.RetainExisting = &value
	}
	if ob.AuthRead == "" {
		ob.AuthRead = kp.AuthRead
	} else if kp.AuthRead != "" && kp.AuthRead != ob.AuthRead {
		notes = append(notes, Reason{Code: "shared_constraint_conflict", Text: "The public outbox and KeyPackage roles disagree about read authentication."})
	}
	if ob.AuthWrite == "" {
		ob.AuthWrite = kp.AuthWrite
	} else if kp.AuthWrite != "" && kp.AuthWrite != ob.AuthWrite {
		notes = append(notes, Reason{Code: "shared_constraint_conflict", Text: "The public outbox and KeyPackage roles disagree about write authentication."})
	}
	if c.ByRole == nil {
		c.ByRole = map[string]RoleConstraints{}
	}
	c.ByRole[RolePublicOutbox] = ob
	return notes
}

func derefStrings(value *[]string) []string {
	if value == nil {
		return nil
	}
	return *value
}

func unionStrings(groups ...[]string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, group := range groups {
		for _, item := range group {
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			out = append(out, item)
		}
	}
	return out
}
