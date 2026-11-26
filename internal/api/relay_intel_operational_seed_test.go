package api

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nostrmash/nostrmash/internal/relayurl"
)

type seededFixture struct {
	Accounts     int
	PopularRelay string
	Counts       map[string]any
	SeedMS       int64
}

func seedOperationalFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool, accounts int) seededFixture {
	t.Helper()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SET statement_timeout = '30min'`); err != nil {
		t.Fatal(err)
	}
	// The seed aggregation is allowed a larger work_mem. Measured queries use
	// the server default on the other pool connections.
	if _, err := conn.Exec(ctx, `SET work_mem = '64MB'`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	execLabeled(t, ctx, conn, "relays", `
		CREATE TABLE bench_relays (
			relay_id integer PRIMARY KEY,
			url text NOT NULL,
			url_key text NOT NULL,
			host text NOT NULL,
			operator_id integer NOT NULL
		)`)
	execLabeled(t, ctx, conn, "relay rows", `
		INSERT INTO bench_relays
		SELECT i,
		       format('wss://r%s.op%s.example', i, (i % 40)),
		       encode(substring(sha256(convert_to(format('wss://r%s.op%s.example', i, (i % 40)), 'UTF8')) FROM 1 FOR 16), 'hex'),
		       format('r%s.op%s.example', i, (i % 40)),
		       (i % 40)
		FROM generate_series(0, 239) AS i`)
	var url, key string
	if err := conn.QueryRow(ctx, `SELECT url, url_key FROM bench_relays WHERE relay_id = 0`).Scan(&url, &key); err != nil {
		t.Fatal(err)
	}
	if got := relayurl.CanonicalKey(url); got != key {
		t.Fatalf("url key mismatch for %s: sql %s go %s", url, key, got)
	}
	execLabeled(t, ctx, conn, "accounts", `
		CREATE TABLE bench_accounts (
			i integer PRIMARY KEY,
			pubkey text NOT NULL UNIQUE,
			bucket integer NOT NULL
		)`)
	execLabeled(t, ctx, conn, "account rows", `
		INSERT INTO bench_accounts
		SELECT i, lpad(to_hex(100000 + i), 64, '0'), (i % 100)
		FROM generate_series(1, $1) AS i`, accounts)
	execLabeled(t, ctx, conn, "slot function", `
		CREATE FUNCTION bench_relay_slot(p_account integer, p_slot integer) RETURNS integer
		LANGUAGE sql IMMUTABLE AS $$
			SELECT CASE
				WHEN p_slot <= 0 THEN p_account % 8
				WHEN p_slot = 1 THEN 8 + (p_account % 16)
				WHEN p_slot = 2 THEN 24 + (p_account % 36)
				ELSE 60 + ((p_account + p_slot * 7) % 180)
			END
		$$`)
	execLabeled(t, ctx, conn, "decl", `
		CREATE TABLE bench_decl (
			account_i integer NOT NULL,
			pubkey text NOT NULL,
			kind integer NOT NULL,
			d_tag text NOT NULL DEFAULT '',
			role text NOT NULL DEFAULT '',
			marker text NOT NULL DEFAULT '',
			relay_id integer,
			event_suffix integer NOT NULL,
			created_at bigint NOT NULL,
			truncated boolean NOT NULL DEFAULT false,
			explicitly_empty boolean NOT NULL DEFAULT false,
			client_side boolean NOT NULL DEFAULT false,
			declaration_source text NOT NULL,
			is_head boolean NOT NULL DEFAULT true
		)`)
	execLabeled(t, ctx, conn, "modern lists", `
		INSERT INTO bench_decl (
			account_i, pubkey, kind, d_tag, role, marker, relay_id, event_suffix, created_at,
			truncated, explicitly_empty, client_side, declaration_source, is_head
		)
		SELECT a.i, a.pubkey, 10002, '', s.role, s.marker, bench_relay_slot(a.i, s.slot), 1, 1700000000,
		       false, false, false, 'modern', true
		FROM bench_accounts a
		CROSS JOIN (VALUES
			(0, 'public_inbox', 'read'),
			(1, 'public_inbox', 'read'),
			(2, 'public_inbox', 'read'),
			(3, 'public_outbox', 'write'),
			(4, 'public_outbox', 'write'),
			(5, 'public_inbox', ''),
			(5, 'public_outbox', '')
		) AS s(slot, role, marker)
		WHERE a.bucket BETWEEN 26 AND 96`)
	execLabeled(t, ctx, conn, "truncated lists", `
		INSERT INTO bench_decl (
			account_i, pubkey, kind, d_tag, role, marker, relay_id, event_suffix, created_at,
			truncated, explicitly_empty, client_side, declaration_source, is_head
		)
		SELECT a.i, a.pubkey, 10002, '', 'public_inbox', 'read', bench_relay_slot(a.i, s.slot), 1, 1700000000,
		       true, false, false, 'modern', true
		FROM bench_accounts a
		CROSS JOIN generate_series(0, 35) AS s(slot)
		WHERE a.bucket BETWEEN 21 AND 25`)
	execLabeled(t, ctx, conn, "legacy lists", `
		INSERT INTO bench_decl (
			account_i, pubkey, kind, d_tag, role, marker, relay_id, event_suffix, created_at,
			truncated, explicitly_empty, client_side, declaration_source, is_head
		)
		SELECT a.i, a.pubkey, 3, '', s.role, '', bench_relay_slot(a.i, s.slot), 3, 1690000000,
		       false, false, false, 'legacy', true
		FROM bench_accounts a
		CROSS JOIN (VALUES
			(0, 'public_inbox'),
			(0, 'public_outbox'),
			(1, 'public_inbox'),
			(1, 'public_outbox')
		) AS s(slot, role)
		WHERE a.bucket BETWEEN 13 AND 20`)
	execLabeled(t, ctx, conn, "empty lists", `
		INSERT INTO bench_decl (
			account_i, pubkey, kind, d_tag, role, marker, relay_id, event_suffix, created_at,
			truncated, explicitly_empty, client_side, declaration_source, is_head
		)
		SELECT a.i, a.pubkey, 10002, '', '', '', NULL, 1, 1700000000,
		       false, true, false, 'modern', true
		FROM bench_accounts a
		WHERE a.bucket BETWEEN 8 AND 12`)
	execLabeled(t, ctx, conn, "keypackage lists", `
		INSERT INTO bench_decl (
			account_i, pubkey, kind, d_tag, role, marker, relay_id, event_suffix, created_at,
			truncated, explicitly_empty, client_side, declaration_source, is_head
		)
		SELECT a.i, a.pubkey, 10051, '', 'keypackage', '', bench_relay_slot(a.i, s.slot), 9, 1700000000,
		       false, false, false, 'modern', true
		FROM bench_accounts a
		CROSS JOIN generate_series(0, 1) AS s(slot)
		WHERE a.bucket BETWEEN 97 AND 99`)
	execLabeled(t, ctx, conn, "rich extras", `
		INSERT INTO bench_decl (
			account_i, pubkey, kind, d_tag, role, marker, relay_id, event_suffix, created_at,
			truncated, explicitly_empty, client_side, declaration_source, is_head
		)
		SELECT a.i, a.pubkey, s.kind, s.d_tag, s.role, '', bench_relay_slot(a.i, s.slot), s.suffix, 1700001000,
		       false, false, false, 'modern', true
		FROM bench_accounts a
		CROSS JOIN (VALUES
			(10050, '', 'dm_inbox', 4, 0),
			(10050, '', 'dm_inbox', 4, 1),
			(10007, '', 'search', 5, 2),
			(10012, '', 'favorite', 6, 3),
			(10012, '', 'favorite', 6, 4),
			(10006, '', 'blocked', 7, 6),
			(30002, 'reading', 'relay_set', 8, 0),
			(30002, 'reading', 'relay_set', 8, 1)
		) AS s(kind, d_tag, role, suffix, slot)
		WHERE a.bucket BETWEEN 75 AND 96`)
	execLabeled(t, ctx, conn, "private lists", `
		INSERT INTO bench_decl (
			account_i, pubkey, kind, d_tag, role, marker, relay_id, event_suffix, created_at,
			truncated, explicitly_empty, client_side, declaration_source, is_head
		)
		SELECT a.i, a.pubkey, 10013, '', '', '', NULL, 12, 1700002000,
		       false, false, true, 'modern', true
		FROM bench_accounts a
		WHERE a.bucket BETWEEN 75 AND 80`)
	execLabeled(t, ctx, conn, "superseded residue", `
		INSERT INTO bench_decl (
			account_i, pubkey, kind, d_tag, role, marker, relay_id, event_suffix, created_at,
			truncated, explicitly_empty, client_side, declaration_source, is_head
		)
		SELECT a.i, a.pubkey, 10002, '', 'public_inbox', 'read', bench_relay_slot(a.i, 30 + s.slot), 2, 1600000000,
		       false, false, false, 'modern', false
		FROM bench_accounts a
		CROSS JOIN generate_series(0, 1) AS s(slot)
		WHERE a.bucket = 40`)
	execLabeled(t, ctx, conn, "event tags", `
		CREATE TABLE bench_event_tags AS
		SELECT account_i, event_suffix, d_tag, kind,
		       COALESCE(jsonb_agg(DISTINCT tag), '[]'::jsonb) AS tags
		FROM (
			SELECT d.account_i, d.event_suffix, d.d_tag, d.kind,
			       CASE
			           WHEN d.kind IN (3, 10013) OR d.relay_id IS NULL THEN NULL
			           WHEN d.kind = 30002 THEN jsonb_build_array('relay', r.url)
			           WHEN d.kind IN (10050, 10007, 10012, 10006, 10051) THEN jsonb_build_array('relay', r.url)
			           WHEN d.marker = '' THEN jsonb_build_array('r', r.url)
			           ELSE jsonb_build_array('r', r.url, d.marker)
			       END AS tag
			FROM bench_decl d
			LEFT JOIN bench_relays r ON r.relay_id = d.relay_id
		) tagged
		WHERE tag IS NOT NULL
		GROUP BY account_i, event_suffix, d_tag, kind`)
	execLabeled(t, ctx, conn, "legacy content", `
		CREATE TABLE bench_legacy_content AS
		SELECT d.account_i,
		       jsonb_object_agg(DISTINCT r.url, jsonb_build_object('read', true, 'write', true))::text AS content
		FROM bench_decl d
		JOIN bench_relays r ON r.relay_id = d.relay_id
		WHERE d.kind = 3 AND d.role = 'public_inbox'
		GROUP BY d.account_i`)
	execLabeled(t, ctx, conn, "decl events", `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		SELECT lpad(to_hex((100000 + d.account_i) * 1000 + d.event_suffix), 64, '0'),
		       d.pubkey,
		       min(d.created_at),
		       d.kind,
		       repeat('ab', 64),
		       CASE
		           WHEN d.kind = 3 THEN COALESCE(max(legacy.content), '{}')
		           WHEN d.kind = 10013 THEN 'nip44-ciphertext'
		           ELSE ''
		       END,
		       jsonb_build_object(
		           'id', lpad(to_hex((100000 + d.account_i) * 1000 + d.event_suffix), 64, '0'),
		           'pubkey', d.pubkey,
		           'created_at', min(d.created_at),
		           'kind', d.kind,
		           'tags', CASE
		               WHEN d.kind = 30002 THEN jsonb_build_array(jsonb_build_array('d', d.d_tag)) || COALESCE(max(tags.tags::text)::jsonb, '[]'::jsonb)
		               ELSE COALESCE(max(tags.tags::text)::jsonb, '[]'::jsonb)
		           END,
		           'content', CASE
		               WHEN d.kind = 3 THEN COALESCE(max(legacy.content), '{}')
		               WHEN d.kind = 10013 THEN 'nip44-ciphertext'
		               ELSE ''
		           END,
		           'sig', repeat('ab', 64)
		       )
		FROM bench_decl d
		LEFT JOIN bench_event_tags tags
		  ON tags.account_i = d.account_i AND tags.event_suffix = d.event_suffix AND tags.kind = d.kind AND tags.d_tag = d.d_tag
		LEFT JOIN bench_legacy_content legacy ON legacy.account_i = d.account_i AND d.kind = 3
		GROUP BY d.account_i, d.pubkey, d.kind, d.event_suffix, d.d_tag`)
	execLabeled(t, ctx, conn, "kind 0 events", `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		SELECT lpad(to_hex((100000 + i) * 1000 + 10), 64, '0'),
		       pubkey, 1695000000, 0, repeat('cd', 64),
		       jsonb_build_object('name', 'user-' || i, 'display_name', 'User ' || i)::text,
		       jsonb_build_object(
		           'id', lpad(to_hex((100000 + i) * 1000 + 10), 64, '0'),
		           'pubkey', pubkey,
		           'created_at', 1695000000,
		           'kind', 0,
		           'tags', '[]'::jsonb,
		           'content', jsonb_build_object('name', 'user-' || i, 'display_name', 'User ' || i)::text,
		           'sig', repeat('cd', 64)
		       )
		FROM bench_accounts`)
	execLabeled(t, ctx, conn, "replaceable heads", `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		SELECT pubkey, kind, d_tag,
		       lpad(to_hex((100000 + account_i) * 1000 + event_suffix), 64, '0'),
		       min(created_at), 1
		FROM bench_decl
		WHERE is_head
		GROUP BY pubkey, kind, d_tag, account_i, event_suffix`)
	execLabeled(t, ctx, conn, "kind 0 state", `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		SELECT pubkey, 0, '', lpad(to_hex((100000 + i) * 1000 + 10), 64, '0'), 1695000000, 1
		FROM bench_accounts`)
	execLabeled(t, ctx, conn, "heads", `
		INSERT INTO relay_config_heads (
			pubkey, source_kind, d_tag, event_id, created_at, explicitly_empty, client_side_only,
			declaration_source, invalid_endpoint_count, truncated, title
		)
		SELECT pubkey, kind, d_tag,
		       lpad(to_hex((100000 + account_i) * 1000 + event_suffix), 64, '0'),
		       min(created_at), bool_or(explicitly_empty), bool_or(client_side),
		       min(declaration_source), 0, bool_or(truncated),
		       CASE WHEN kind = 30002 THEN 'Reading' ELSE '' END
		FROM bench_decl
		WHERE is_head
		GROUP BY pubkey, kind, d_tag, account_i, event_suffix`)
	execLabeled(t, ctx, conn, "memberships", `
		INSERT INTO relay_role_memberships (
			pubkey, role, url_key, source_kind, d_tag, endpoint_url, endpoint_type,
			source_event_id, created_at, declaration_source, marker, public_safe
		)
		SELECT DISTINCT ON (d.pubkey, d.role, r.url_key, d.kind, d.d_tag)
		       d.pubkey, d.role, r.url_key, d.kind, d.d_tag, r.url, 'nostr_relay',
		       lpad(to_hex((100000 + d.account_i) * 1000 + d.event_suffix), 64, '0'),
		       d.created_at, d.declaration_source, d.marker, true
		FROM bench_decl d
		JOIN bench_relays r ON r.relay_id = d.relay_id
		WHERE d.role <> ''
		ORDER BY d.pubkey, d.role, r.url_key, d.kind, d.d_tag, d.is_head DESC, d.created_at DESC`)
	execLabeled(t, ctx, conn, "purge queue", `
		INSERT INTO replaceable_event_purge (event_id, created_at)
		SELECT DISTINCT lpad(to_hex((100000 + account_i) * 1000 + event_suffix), 64, '0'), created_at
		FROM bench_decl
		WHERE NOT is_head
		ON CONFLICT (event_id) DO NOTHING`)
	execLabeled(t, ctx, conn, "ties", `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		SELECT lpad(to_hex(id_num), 64, '0'),
		       lpad(to_hex(i), 64, '0'),
		       1700000000, 10002, repeat('ab', 64), '',
		       jsonb_build_object(
		           'id', lpad(to_hex(id_num), 64, '0'),
		           'pubkey', lpad(to_hex(i), 64, '0'),
		           'created_at', 1700000000,
		           'kind', 10002,
		           'tags', jsonb_build_array(jsonb_build_array('r', 'wss://tie.op0.example', 'read')),
		           'content', '',
		           'sig', repeat('ab', 64)
		       )
		FROM generate_series(1, 200) AS i
		CROSS JOIN (VALUES (700000000000), (800000000000)) AS ids(base)
		CROSS JOIN LATERAL (SELECT base + i AS id_num) n`)
	execLabeled(t, ctx, conn, "tie state", `
		INSERT INTO replaceable_state (pubkey, kind, d_tag, event_id, created_at, derivation_version)
		SELECT lpad(to_hex(i), 64, '0'), 10002, '', lpad(to_hex(800000000000 + i), 64, '0'), 1700000000, 1
		FROM generate_series(1, 200) AS i`)
	execLabeled(t, ctx, conn, "tie heads", `
		INSERT INTO relay_config_heads (
			pubkey, source_kind, d_tag, event_id, created_at, declaration_source, truncated
		)
		SELECT lpad(to_hex(i), 64, '0'), 10002, '', lpad(to_hex(800000000000 + i), 64, '0'), 1700000000, 'modern', false
		FROM generate_series(1, 200) AS i`)
	execLabeled(t, ctx, conn, "tie memberships", `
		INSERT INTO relay_role_memberships (
			pubkey, role, url_key, source_kind, d_tag, endpoint_url, endpoint_type,
			source_event_id, created_at, declaration_source, marker, public_safe
		)
		SELECT lpad(to_hex(i), 64, '0'), 'public_inbox',
		       encode(substring(sha256(convert_to('wss://tie.op0.example', 'UTF8')) FROM 1 FOR 16), 'hex'),
		       10002, '', 'wss://tie.op0.example', 'nostr_relay',
		       lpad(to_hex(800000000000 + i), 64, '0'), 1700000000, 'modern', 'read', true
		FROM generate_series(1, 200) AS i`)
	execLabeled(t, ctx, conn, "deletions", `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		SELECT lpad(to_hex(600000000000 + i), 64, '0'),
		       a.pubkey, 1500000000, 5, repeat('ef', 64), '',
		       jsonb_build_object(
		           'id', lpad(to_hex(600000000000 + i), 64, '0'),
		           'pubkey', a.pubkey,
		           'created_at', 1500000000,
		           'kind', 5,
		           'tags', jsonb_build_array(jsonb_build_array('a', '10002:' || a.pubkey)),
		           'content', '',
		           'sig', repeat('ef', 64)
		       )
		FROM (
			SELECT i, pubkey FROM bench_accounts WHERE bucket = 50 ORDER BY i LIMIT 200
		) a`)
	execLabeled(t, ctx, conn, "deletion tags", `
		INSERT INTO event_tags (event_id, tag_name, tag_index, value_index, value)
		SELECT lpad(to_hex(600000000000 + i), 64, '0'), 'a', 0, 0, '10002:' || pubkey
		FROM (
			SELECT i, pubkey FROM bench_accounts WHERE bucket = 50 ORDER BY i LIMIT 200
		) a`)
	execLabeled(t, ctx, conn, "deletion tombstones", `
		INSERT INTO deletion_events (event_id, deleter_pubkey, target_event_id, created_at, derivation_version)
		SELECT lpad(to_hex(600000000000 + i), 64, '0'),
		       pubkey,
		       lpad(to_hex((100000 + i) * 1000 + 2), 64, '0'),
		       1500000000, 1
		FROM (
			SELECT i, pubkey FROM bench_accounts WHERE bucket = 40 ORDER BY i LIMIT 40
		) a
		WHERE EXISTS (
			SELECT 1 FROM events e WHERE e.id = lpad(to_hex((100000 + a.i) * 1000 + 2), 64, '0')
		)`)
	execLabeled(t, ctx, conn, "follows", `
		INSERT INTO follower_edges (
			followed_pubkey, follower_pubkey, source_event_id, contact_list_created_at, derivation_version
		)
		SELECT lpad(to_hex(100000 + 2000 + ((a.i - 2000 + s.n) % 800)), 64, '0'),
		       a.pubkey,
		       lpad(to_hex((100000 + a.i) * 1000 + 1), 64, '0'),
		       1700000000, 1
		FROM bench_accounts a
		CROSS JOIN generate_series(1, 15) AS s(n)
		WHERE a.i BETWEEN 2000 AND 2799
		  AND a.bucket BETWEEN 21 AND 96
		ON CONFLICT DO NOTHING`)
	execLabeled(t, ctx, conn, "sparse follows", `
		INSERT INTO follower_edges (
			followed_pubkey, follower_pubkey, source_event_id, contact_list_created_at, derivation_version
		)
		SELECT lpad(to_hex(100000 + 1000 + (a.i % 20)), 64, '0'),
		       a.pubkey,
		       lpad(to_hex((100000 + a.i) * 1000 + CASE WHEN a.bucket BETWEEN 13 AND 20 THEN 3 ELSE 1 END), 64, '0'),
		       1700000000, 1
		FROM bench_accounts a
		WHERE a.i % 10 < 3
		  AND a.bucket BETWEEN 8 AND 96
		  AND a.i NOT BETWEEN 2000 AND 2799
		ON CONFLICT DO NOTHING`)
	var runID int64
	if err := conn.QueryRow(ctx, `
		INSERT INTO trust_runs (derivation_name, target_version, status, finished_at)
		VALUES ('relay_intel_bench', 1, 'succeeded', now())
		RETURNING id
	`).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	execLabeled(t, ctx, conn, "trust", `
		INSERT INTO trust_scores_global (pubkey, score, rank, run_id, derivation_name, target_version)
		SELECT pubkey, 1.0 / (1 + (i % 80)), row_number() OVER (ORDER BY i), $1, 'relay_intel_bench', 1
		FROM bench_accounts
		WHERE i % 5 = 0 OR i BETWEEN 2000 AND 2450`, runID)
	execLabeled(t, ctx, conn, "activity", `
		INSERT INTO author_activity_daily (pubkey, activity_date, post_count, note_count, derivation_version)
		SELECT pubkey, CURRENT_DATE, 1 + (i % 12), 1 + (i % 12), 1
		FROM bench_accounts
		WHERE i % 3 = 0 OR i BETWEEN 2000 AND 2799
		ON CONFLICT DO NOTHING`)
	execLabeled(t, ctx, conn, "interactions", `
		INSERT INTO trust_interaction_edge_weights (src_pubkey, dst_pubkey, weight)
		SELECT a.pubkey, lpad(to_hex(100000 + 2000 + (a.i % 400)), 64, '0'), 0.2 + ((a.i % 10)::float8 / 10.0)
		FROM bench_accounts a
		WHERE a.i BETWEEN 2000 AND 2600
		  AND a.pubkey <> lpad(to_hex(100000 + 2000 + (a.i % 400)), 64, '0')
		ON CONFLICT DO NOTHING`)
	execLabeled(t, ctx, conn, "registry", `
		INSERT INTO relay_registry (
			url_key, normalized_url, source_user_list, admission_state,
			last_probe_at, last_probe_status, last_connect_ok, last_subscribe_ok, last_eose_ok,
			avg_connect_latency_ms, avg_eose_latency_ms, probe_fail_rate, capability_summary_json
		)
		SELECT url_key, url, true,
		       CASE WHEN relay_id % 17 = 0 THEN 'probation' ELSE 'active' END,
		       now() - make_interval(hours => (relay_id % 10)),
		       CASE WHEN relay_id % 19 = 0 THEN 'connect_failed' WHEN relay_id % 11 = 0 THEN 'eose_timeout' ELSE 'ok' END,
		       relay_id % 19 <> 0,
		       relay_id % 11 <> 0,
		       relay_id % 11 <> 0 AND relay_id % 19 <> 0,
		       20 + (relay_id % 80),
		       40 + (relay_id % 200),
		       CASE WHEN relay_id % 19 = 0 THEN 0.9 WHEN relay_id % 11 = 0 THEN 0.4 ELSE 0.05 END,
		       jsonb_build_object(
		           'ok', true,
		           'fetched_at', now(),
		           'info', jsonb_build_object(
		               'pubkey', lpad(to_hex(900000 + operator_id), 64, '0'),
		               'software', CASE WHEN operator_id % 3 = 0 THEN 'strfry' WHEN operator_id % 3 = 1 THEN 'nostream' ELSE 'khatru' END,
		               'supported_nips', jsonb_build_array(1, 11, 42, CASE WHEN operator_id % 2 = 0 THEN 50 ELSE 65 END),
		               'limitation', jsonb_build_object(
		                   'auth_required', (operator_id % 5 = 0),
		                   'payment_required', (operator_id % 11 = 0),
		                   'restricted_writes', false
		               )
		           )
		       )
		FROM bench_relays`)
	execLabeled(t, ctx, conn, "capabilities", `
		INSERT INTO relay_capability_observations (
			url_key, capability, status, confidence, source, detail, sample_count, observed_at
		)
		SELECT url_key, cap, status, confidence, 'probe', jsonb_build_object('text', 'seeded history'), 4,
		       now() - interval '2 hours'
		FROM bench_relays
		CROSS JOIN (VALUES
			('write_accept', 'verified', 0.82),
			('write_retrieve', 'verified', 0.74),
			('search', 'verified', 0.7),
			('giftwrap_private', 'verified', 0.66),
			('auth_write', 'advertised', 0.5),
			('retention', 'observed', 0.45)
		) AS c(cap, status, confidence)
		WHERE relay_id < 120`)
	execLabeled(t, ctx, conn, "probe history", `
		INSERT INTO relay_probe_attempts (
			url_key, capability, host, worker_id, lease_token, started_at, finished_at, status
		)
		SELECT url_key, cap, host, 'bench-history', gen_random_uuid()::text,
		       now() - interval '3 hours', now() - interval '3 hours' + interval '2 seconds', 'finished'
		FROM bench_relays
		CROSS JOIN (VALUES ('write_accept'), ('search')) AS c(cap)
		WHERE relay_id < 80`)
	execLabeled(t, ctx, conn, "followups", `
		INSERT INTO relay_probe_followups (
			url_key, endpoint_url, event_id, capability, check_after, published_at, status, attempts
		)
		SELECT r.url_key, r.url, e.id, 'retention', now() - interval '1 minute', now() - interval '2 hours', 'pending', 0
		FROM (
			SELECT id, row_number() OVER (ORDER BY id) AS n
			FROM events
			WHERE kind = 10002
			ORDER BY id
			LIMIT 180
		) e
		JOIN bench_relays r ON r.relay_id = ((e.n - 1) % 240)`)
	execLabeled(t, ctx, conn, "provenance", `
		INSERT INTO event_relays (event_id, relay_url, seen_at, pubkey)
		SELECT lpad(to_hex((100000 + a.i) * 1000 + 1), 64, '0'), r.url, now() - interval '1 day', a.pubkey
		FROM bench_accounts a
		JOIN bench_relays r ON r.relay_id = bench_relay_slot(a.i, 0)
		WHERE a.bucket BETWEEN 26 AND 96
		  AND EXISTS (
		      SELECT 1 FROM events e WHERE e.id = lpad(to_hex((100000 + a.i) * 1000 + 1), 64, '0')
		  )`)
	execLabeled(t, ctx, conn, "analyze", `
		ANALYZE events, replaceable_state, relay_config_heads, relay_role_memberships,
		        follower_edges, trust_scores_global, author_activity_daily, relay_registry,
		        relay_capability_observations, relay_probe_attempts, relay_probe_followups,
		        event_relays, event_tags, deletion_events, replaceable_event_purge`)
	counts := map[string]any{}
	for _, item := range []struct {
		name string
		sql  string
	}{
		{"events", `SELECT count(*) FROM events`},
		{"replaceable_state", `SELECT count(*) FROM replaceable_state`},
		{"relay_config_heads", `SELECT count(*) FROM relay_config_heads`},
		{"relay_role_memberships", `SELECT count(*) FROM relay_role_memberships`},
		{"effective_memberships", `SELECT count(*) FROM relay_effective_memberships`},
		{"explicitly_empty_heads", `SELECT count(*) FROM relay_config_heads WHERE explicitly_empty`},
		{"truncated_heads", `SELECT count(*) FROM relay_config_heads WHERE truncated`},
		{"client_side_heads", `SELECT count(*) FROM relay_config_heads WHERE client_side_only`},
		{"legacy_heads", `SELECT count(*) FROM relay_config_heads WHERE source_kind = 3`},
		{"follower_edges", `SELECT count(*) FROM follower_edges`},
		{"trust_scores", `SELECT count(*) FROM trust_scores_global`},
		{"author_activity", `SELECT count(*) FROM author_activity_daily`},
		{"relay_registry", `SELECT count(*) FROM relay_registry`},
		{"capability_observations", `SELECT count(*) FROM relay_capability_observations`},
		{"probe_attempts", `SELECT count(*) FROM relay_probe_attempts`},
		{"probe_followups", `SELECT count(*) FROM relay_probe_followups`},
		{"event_relays", `SELECT count(*) FROM event_relays`},
		{"deletion_events", `SELECT count(*) FROM deletion_events`},
		{"purge_queue", `SELECT count(*) FROM replaceable_event_purge`},
		{"accounts_without_relay_head", `
			SELECT count(*) FROM bench_accounts a
			WHERE NOT EXISTS (
				SELECT 1 FROM relay_config_heads h WHERE h.pubkey = a.pubkey
			)`},
	} {
		var n int64
		if err := conn.QueryRow(ctx, item.sql).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", item.name, err)
		}
		counts[item.name] = n
	}
	roleRows, err := conn.Query(ctx, `SELECT role, count(*) FROM relay_role_memberships GROUP BY role ORDER BY role`)
	if err != nil {
		t.Fatal(err)
	}
	roles := map[string]int64{}
	for roleRows.Next() {
		var role string
		var n int64
		if err := roleRows.Scan(&role, &n); err != nil {
			t.Fatal(err)
		}
		roles[role] = n
	}
	roleRows.Close()
	counts["memberships_by_role"] = roles
	var top []map[string]any
	topRows, err := conn.Query(ctx, `
		SELECT endpoint_url, count(DISTINCT pubkey) AS users
		FROM relay_role_memberships
		WHERE role = 'public_inbox' AND public_safe
		GROUP BY endpoint_url
		ORDER BY users DESC, endpoint_url
		LIMIT 8
	`)
	if err != nil {
		t.Fatal(err)
	}
	for topRows.Next() {
		var endpoint string
		var users int64
		if err := topRows.Scan(&endpoint, &users); err != nil {
			t.Fatal(err)
		}
		top = append(top, map[string]any{"url": endpoint, "inbox_users": users})
	}
	topRows.Close()
	counts["top_inbox_relays"] = top
	execLabeled(t, ctx, conn, "drop helpers", `
		DROP TABLE IF EXISTS bench_event_tags, bench_legacy_content, bench_decl, bench_accounts, bench_relays;
		DROP FUNCTION IF EXISTS bench_relay_slot(integer, integer)`)
	t.Logf("seeded %d accounts in %s", accounts, time.Since(started).Round(time.Millisecond))
	return seededFixture{
		Accounts:     accounts,
		PopularRelay: url,
		Counts:       counts,
		SeedMS:       time.Since(started).Milliseconds(),
	}
}

func execLabeled(t *testing.T, ctx context.Context, conn *pgxpool.Conn, label, sql string, args ...any) {
	t.Helper()
	started := time.Now()
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	t.Logf("seed %s %s", label, time.Since(started).Round(time.Millisecond))
}
