package relayintel

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	maxFollows      = 400
	maxFollowers    = 200
	maxInteractions = 100
	maxSecondDegree = 500
	maxCoverageRows = 20000
	maxStatsPerRole = 80
	maxCandidates   = 80
	maxRankingLimit = 50
)

type roleStat struct {
	URLKey       string
	Role         string
	URL          string
	EndpointType string
	Users        int64
	Trust        float64
	Active       int64
	Modern       int64
	Legacy       int64
	Delta        int64
}

type registryRow struct {
	URLKey      string
	URL         string
	Admission   string
	ProbeAt     *time.Time
	Status      string
	ConnectOK   *bool
	SubscribeOK *bool
	EOSEOK      *bool
	ConnectMs   *float64
	EOSEMs      *float64
	FailRate    float64
	Summary     []byte
}

// dbQuery is the read surface shared by a pool and a transaction.
type dbQuery interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func loadAccount(ctx context.Context, pool dbQuery, pubkey string) ([]Head, []Membership, error) {
	heads := make([]Head, 0)
	rows, err := pool.Query(ctx, `
		SELECT pubkey, source_kind, d_tag, event_id, created_at,
		       explicitly_empty, client_side_only, declaration_source,
		       invalid_endpoint_count, truncated, title
		FROM relay_config_heads
		WHERE pubkey = $1
	`, pubkey)
	if err != nil {
		return nil, nil, wrapDB(err)
	}
	for rows.Next() {
		var head Head
		if err := rows.Scan(
			&head.Pubkey, &head.SourceKind, &head.DTag, &head.EventID, &head.CreatedAt,
			&head.ExplicitlyEmpty, &head.ClientSideOnly, &head.DeclarationSource,
			&head.InvalidEndpoints, &head.Truncated, &head.Title,
		); err != nil {
			rows.Close()
			return nil, nil, err
		}
		heads = append(heads, head)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, wrapDB(err)
	}

	members := make([]Membership, 0)
	rows, err = pool.Query(ctx, `
		SELECT pubkey, role, url_key, endpoint_url, endpoint_type,
		       source_kind, source_event_id, d_tag, created_at,
		       declaration_source, marker, public_safe
		FROM relay_role_memberships
		WHERE pubkey = $1
	`, pubkey)
	if err != nil {
		return nil, nil, wrapDB(err)
	}
	defer rows.Close()
	for rows.Next() {
		var member Membership
		if err := rows.Scan(
			&member.Pubkey, &member.Role, &member.URLKey, &member.EndpointURL, &member.EndpointType,
			&member.SourceKind, &member.SourceEventID, &member.DTag, &member.CreatedAt,
			&member.DeclarationSource, &member.Marker, &member.PublicSafe,
		); err != nil {
			return nil, nil, err
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, wrapDB(err)
	}
	return heads, members, nil
}

func loadSnapshotTime(ctx context.Context, pool *pgxpool.Pool) (time.Time, bool, error) {
	var computed time.Time
	err := pool.QueryRow(ctx, `SELECT computed_at FROM relay_intel_snapshot WHERE id`).Scan(&computed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, wrapDB(err)
	}
	return computed, true, nil
}

func loadTopStats(ctx context.Context, pool *pgxpool.Pool, role string, limit int) ([]roleStat, error) {
	if limit <= 0 || limit > maxStatsPerRole {
		limit = maxStatsPerRole
	}
	rows, err := pool.Query(ctx, `
		SELECT url_key, role, endpoint_url, endpoint_type,
		       distinct_users, trust_weighted, active_users,
		       modern_users, legacy_users, net_user_delta_7d
		FROM relay_role_stats
		WHERE role = $1
		ORDER BY trust_weighted DESC, distinct_users DESC, url_key ASC
		LIMIT $2
	`, role, limit)
	if err != nil {
		return nil, wrapDB(err)
	}
	defer rows.Close()
	return scanStats(rows)
}

func loadStatsFor(ctx context.Context, pool *pgxpool.Pool, role string, keys []string) ([]roleStat, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT url_key, role, endpoint_url, endpoint_type,
		       distinct_users, trust_weighted, active_users,
		       modern_users, legacy_users, net_user_delta_7d
		FROM relay_role_stats
		WHERE role = $1 AND url_key = ANY($2::text[])
	`, role, keys)
	if err != nil {
		return nil, wrapDB(err)
	}
	defer rows.Close()
	return scanStats(rows)
}

type statScanner interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanStats(rows statScanner) ([]roleStat, error) {
	out := make([]roleStat, 0)
	for rows.Next() {
		var item roleStat
		if err := rows.Scan(
			&item.URLKey, &item.Role, &item.URL, &item.EndpointType,
			&item.Users, &item.Trust, &item.Active, &item.Modern, &item.Legacy, &item.Delta,
		); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func loadRegistry(ctx context.Context, pool *pgxpool.Pool, keys []string) (map[string]registryRow, error) {
	out := map[string]registryRow{}
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT url_key, normalized_url, admission_state,
		       last_probe_at, COALESCE(last_probe_status, ''),
		       last_connect_ok, last_subscribe_ok, last_eose_ok,
		       avg_connect_latency_ms, avg_eose_latency_ms, probe_fail_rate,
		       capability_summary_json
		FROM relay_registry
		WHERE url_key = ANY($1::text[])
	`, keys)
	if err != nil {
		return nil, wrapDB(err)
	}
	defer rows.Close()
	for rows.Next() {
		var item registryRow
		if err := rows.Scan(
			&item.URLKey, &item.URL, &item.Admission,
			&item.ProbeAt, &item.Status,
			&item.ConnectOK, &item.SubscribeOK, &item.EOSEOK,
			&item.ConnectMs, &item.EOSEMs, &item.FailRate,
			&item.Summary,
		); err != nil {
			return nil, err
		}
		out[item.URLKey] = item
	}
	return out, rows.Err()
}

func loadObservations(ctx context.Context, pool *pgxpool.Pool, keys []string) (map[string][]Observation, error) {
	out := map[string][]Observation{}
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT url_key, capability, status, confidence, source,
		       COALESCE(detail->>'text', ''), sample_count, observed_at
		FROM relay_capability_observations
		WHERE url_key = ANY($1::text[])
	`, keys)
	if err != nil {
		return nil, wrapDB(err)
	}
	defer rows.Close()
	for rows.Next() {
		var item Observation
		if err := rows.Scan(
			&item.URLKey, &item.Capability, &item.Status, &item.Confidence, &item.Source,
			&item.Detail, &item.SampleCount, &item.ObservedAt,
		); err != nil {
			return nil, err
		}
		out[item.URLKey] = append(out[item.URLKey], item)
	}
	return out, rows.Err()
}

func loadPropagation(ctx context.Context, pool *pgxpool.Pool, urls []string) (map[string]Propagation, error) {
	out := map[string]Propagation{}
	if len(urls) == 0 {
		return out, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT relay_url, COALESCE(SUM(events_seen), 0), COALESCE(SUM(first_seen_count), 0)
		FROM relay_propagation_daily
		WHERE day >= CURRENT_DATE - 7 AND relay_url = ANY($1::text[])
		GROUP BY relay_url
	`, urls)
	if err != nil {
		return nil, wrapDB(err)
	}
	defer rows.Close()
	for rows.Next() {
		var relayURL string
		var item Propagation
		if err := rows.Scan(&relayURL, &item.EventsSeen7d, &item.FirstSeen7d); err != nil {
			return nil, err
		}
		item.Known = true
		out[relayURL] = item
	}
	return out, rows.Err()
}

func loadEndpointUsers(ctx context.Context, pool *pgxpool.Pool, keys []string) (map[string]int64, error) {
	out := map[string]int64{}
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT url_key, distinct_users
		FROM relay_endpoint_adoption
		WHERE url_key = ANY($1::text[])
	`, keys)
	if err != nil {
		return nil, wrapDB(err)
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var users int64
		if err := rows.Scan(&key, &users); err != nil {
			return nil, err
		}
		out[key] = users
	}
	return out, rows.Err()
}

func loadEventRaw(ctx context.Context, pool dbQuery, ids []string) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	if len(ids) == 0 || pool == nil {
		return out, nil
	}
	rows, err := pool.Query(ctx, `SELECT id, raw_json::text FROM events WHERE id = ANY($1::text[])`, ids)
	if err != nil {
		return nil, wrapDB(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		out[id] = json.RawMessage(raw)
	}
	return out, rows.Err()
}

func loadCohortSignals(ctx context.Context, pool *pgxpool.Pool, pubkey string) (CohortSignals, error) {
	signals := CohortSignals{Viewer: pubkey, Interaction: map[string]float64{}, Trust: map[string]float64{}, Active: map[string]bool{}}
	follows, truncated, err := loadTextColumn(ctx, pool, `
		SELECT followed_pubkey FROM follower_edges
		WHERE follower_pubkey = $1
		ORDER BY followed_pubkey ASC
		LIMIT $2
	`, pubkey, maxFollows+1)
	if err != nil {
		return signals, err
	}
	if len(follows) > maxFollows {
		truncated = true
		follows = follows[:maxFollows]
	}
	signals.Follows = follows
	signals.FollowsTruncated = truncated

	followers, followerTruncated, err := loadTextColumn(ctx, pool, `
		SELECT follower_pubkey FROM follower_edges
		WHERE followed_pubkey = $1
		ORDER BY follower_pubkey ASC
		LIMIT $2
	`, pubkey, maxFollowers+1)
	if err != nil {
		return signals, err
	}
	if len(followers) > maxFollowers {
		followerTruncated = true
		followers = followers[:maxFollowers]
	}
	signals.Followers = followers
	signals.FollowersTruncated = followerTruncated

	if err := loadWeights(ctx, pool, `
		SELECT dst_pubkey, weight FROM trust_interaction_edge_weights
		WHERE src_pubkey = $1
		ORDER BY weight DESC, dst_pubkey ASC
		LIMIT $2
	`, pubkey, signals.Interaction); err != nil {
		return signals, err
	}
	if err := loadWeights(ctx, pool, `
		SELECT src_pubkey, weight FROM trust_interaction_edge_weights
		WHERE dst_pubkey = $1
		ORDER BY weight DESC, src_pubkey ASC
		LIMIT $2
	`, pubkey, signals.Interaction); err != nil {
		return signals, err
	}

	people := cleanPubkeys(append(append(append([]string{}, signals.Follows...), signals.Followers...), keysOf(signals.Interaction)...), 1500)
	if len(people) == 0 {
		return signals, nil
	}
	if err := loadTrust(ctx, pool, people, signals.Trust); err != nil {
		return signals, err
	}
	seeds := topFollows(signals.Follows, signals.Trust, 20)
	if len(seeds) > 0 {
		second, _, err := loadTextColumn(ctx, pool, `
				SELECT followed_pubkey FROM follower_edges
				WHERE follower_pubkey = ANY($1::text[])
				ORDER BY followed_pubkey ASC
				LIMIT $2
			`, seeds, maxSecondDegree)
		if err != nil {
			return signals, err
		}
		direct := map[string]struct{}{}
		for _, item := range signals.Follows {
			direct[item] = struct{}{}
		}
		for _, item := range second {
			if item == pubkey {
				continue
			}
			if _, ok := direct[item]; ok {
				continue
			}
			signals.SecondDegree = append(signals.SecondDegree, item)
		}
	}
	people = cleanPubkeys(append(people, signals.SecondDegree...), 1500)
	if err := loadTrust(ctx, pool, signals.SecondDegree, signals.Trust); err != nil {
		return signals, err
	}
	if len(signals.Trust) > 0 {
		signals.TrustAvailable = true
	}

	activeRows, err := pool.Query(ctx, `
		SELECT DISTINCT pubkey FROM author_activity_daily
		WHERE pubkey = ANY($1::text[])
		  AND activity_date >= CURRENT_DATE - 30
		  AND post_count > 0
	`, people)
	if err != nil {
		return signals, wrapDB(err)
	}
	defer activeRows.Close()
	active := map[string]struct{}{}
	for activeRows.Next() {
		var item string
		if err := activeRows.Scan(&item); err != nil {
			return signals, err
		}
		active[item] = struct{}{}
	}
	if err := activeRows.Err(); err != nil {
		return signals, err
	}
	for _, item := range people {
		_, signals.Active[item] = active[item]
	}
	return signals, nil
}

func loadDeclarations(ctx context.Context, pool *pgxpool.Pool, role string, pubkeys []string) (map[string]map[string]struct{}, map[string]struct{}, bool, error) {
	byURL := map[string]map[string]struct{}{}
	declared := map[string]struct{}{}
	if len(pubkeys) == 0 {
		return byURL, declared, false, nil
	}
	rows, err := pool.Query(ctx, `
		SELECT pubkey, endpoint_url
		FROM relay_effective_memberships
		WHERE public_safe AND role = $1 AND pubkey = ANY($2::text[]) AND endpoint_url <> ''
		ORDER BY pubkey, endpoint_url
		LIMIT $3
	`, role, pubkeys, maxCoverageRows)
	if err != nil {
		return nil, nil, false, wrapDB(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var pubkey, endpoint string
		if err := rows.Scan(&pubkey, &endpoint); err != nil {
			return nil, nil, false, err
		}
		count++
		declared[pubkey] = struct{}{}
		set := byURL[endpoint]
		if set == nil {
			set = map[string]struct{}{}
			byURL[endpoint] = set
		}
		set[pubkey] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, false, err
	}
	known, err := pool.Query(ctx, `
		SELECT pubkey FROM relay_effective_known
		WHERE role = $1 AND pubkey = ANY($2::text[])
		ORDER BY pubkey
	`, role, pubkeys)
	if err != nil {
		return nil, nil, false, wrapDB(err)
	}
	defer known.Close()
	for known.Next() {
		var pubkey string
		if err := known.Scan(&pubkey); err != nil {
			return nil, nil, false, err
		}
		declared[pubkey] = struct{}{}
	}
	return byURL, declared, count >= maxCoverageRows, known.Err()
}

func loadTrust(ctx context.Context, pool *pgxpool.Pool, people []string, dest map[string]float64) error {
	if len(people) == 0 {
		return nil
	}
	rows, err := pool.Query(ctx, `SELECT pubkey, score FROM trust_scores_global WHERE pubkey = ANY($1::text[])`, people)
	if err != nil {
		return wrapDB(err)
	}
	defer rows.Close()
	for rows.Next() {
		var item string
		var score float64
		if err := rows.Scan(&item, &score); err != nil {
			return err
		}
		dest[item] = score
	}
	return rows.Err()
}

func loadTextColumn(ctx context.Context, pool *pgxpool.Pool, sql string, arg any, limit int) ([]string, bool, error) {
	rows, err := pool.Query(ctx, sql, arg, limit)
	if err != nil {
		return nil, false, wrapDB(err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, false, err
		}
		value = strings.ToLower(strings.TrimSpace(value))
		if validPubkey(value) {
			out = append(out, value)
		}
	}
	return out, false, rows.Err()
}

func loadWeights(ctx context.Context, pool *pgxpool.Pool, sql, pubkey string, into map[string]float64) error {
	rows, err := pool.Query(ctx, sql, pubkey, maxInteractions)
	if err != nil {
		return wrapDB(err)
	}
	defer rows.Close()
	for rows.Next() {
		var item string
		var weight float64
		if err := rows.Scan(&item, &weight); err != nil {
			return err
		}
		item = strings.ToLower(strings.TrimSpace(item))
		if !validPubkey(item) || weight <= 0 {
			continue
		}
		into[item] += weight
	}
	return rows.Err()
}

func topFollows(follows []string, trust map[string]float64, limit int) []string {
	ordered := append([]string(nil), follows...)
	if len(trust) > 0 {
		for i := 0; i < len(ordered); i++ {
			for j := i + 1; j < len(ordered); j++ {
				if trust[ordered[j]] > trust[ordered[i]] || (trust[ordered[j]] == trust[ordered[i]] && ordered[j] < ordered[i]) {
					ordered[i], ordered[j] = ordered[j], ordered[i]
				}
			}
		}
	}
	if len(ordered) > limit {
		ordered = ordered[:limit]
	}
	return ordered
}

func keysOf(values map[string]float64) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	return out
}

func cleanPubkeys(in []string, limit int) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, item := range in {
		item = strings.ToLower(strings.TrimSpace(item))
		if !validPubkey(item) {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func validPubkey(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
