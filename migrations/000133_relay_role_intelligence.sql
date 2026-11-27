-- Role-aware relay declarations. relay_lists_latest and dm_relay_lists_latest
-- stay as they are. This projection keeps the read/write marker and the
-- other public list kinds without putting blossom servers in relay_registry.

CREATE TABLE IF NOT EXISTS relay_config_heads (
    pubkey TEXT NOT NULL,
    source_kind INTEGER NOT NULL,
    d_tag TEXT NOT NULL DEFAULT '',
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    explicitly_empty BOOLEAN NOT NULL DEFAULT false,
    client_side_only BOOLEAN NOT NULL DEFAULT false,
    declaration_source TEXT NOT NULL,
    invalid_endpoint_count INTEGER NOT NULL DEFAULT 0,
    truncated BOOLEAN NOT NULL DEFAULT false,
    title TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, source_kind, d_tag)
);

CREATE INDEX IF NOT EXISTS idx_relay_config_heads_kind
    ON relay_config_heads (source_kind);

CREATE TABLE IF NOT EXISTS relay_role_memberships (
    pubkey TEXT NOT NULL,
    role TEXT NOT NULL,
    url_key TEXT NOT NULL,
    source_kind INTEGER NOT NULL,
    d_tag TEXT NOT NULL DEFAULT '',
    endpoint_url TEXT NOT NULL,
    endpoint_type TEXT NOT NULL,
    source_event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    created_at BIGINT NOT NULL,
    declaration_source TEXT NOT NULL,
    marker TEXT NOT NULL DEFAULT '',
    public_safe BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, role, url_key, source_kind, d_tag)
);

CREATE INDEX IF NOT EXISTS idx_relay_role_memberships_role_url
    ON relay_role_memberships (role, url_key)
    WHERE public_safe;

CREATE INDEX IF NOT EXISTS idx_relay_role_memberships_pubkey_role
    ON relay_role_memberships (pubkey, role);

CREATE TABLE IF NOT EXISTS relay_role_stats (
    url_key TEXT NOT NULL,
    role TEXT NOT NULL,
    endpoint_url TEXT NOT NULL,
    endpoint_type TEXT NOT NULL,
    distinct_users BIGINT NOT NULL,
    trust_weighted DOUBLE PRECISION NOT NULL,
    active_users BIGINT NOT NULL,
    modern_users BIGINT NOT NULL,
    legacy_users BIGINT NOT NULL,
    net_user_delta_7d BIGINT NOT NULL DEFAULT 0,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (url_key, role)
);

CREATE INDEX IF NOT EXISTS idx_relay_role_stats_role_users
    ON relay_role_stats (role, trust_weighted DESC, distinct_users DESC);

CREATE TABLE IF NOT EXISTS relay_endpoint_adoption (
    url_key TEXT PRIMARY KEY,
    endpoint_url TEXT NOT NULL,
    endpoint_type TEXT NOT NULL,
    distinct_users BIGINT NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS relay_role_adoption_daily (
    day DATE NOT NULL,
    url_key TEXT NOT NULL,
    role TEXT NOT NULL,
    distinct_users BIGINT NOT NULL,
    PRIMARY KEY (day, url_key, role)
);

CREATE TABLE IF NOT EXISTS relay_capability_observations (
    url_key TEXT NOT NULL,
    capability TEXT NOT NULL,
    status TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    source TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    sample_count INTEGER NOT NULL DEFAULT 1,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (url_key, capability)
);

CREATE TABLE IF NOT EXISTS relay_intel_snapshot (
    id BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    computed_at TIMESTAMPTZ NOT NULL,
    kind_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    role_declarers JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS relay_intel_backfill_cursor (
    source_kind INTEGER PRIMARY KEY,
    last_created_at BIGINT NOT NULL DEFAULT 0,
    last_event_id TEXT NOT NULL DEFAULT '',
    done BOOLEAN NOT NULL DEFAULT false,
    processed BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO untrusted_purge_kind_cursor (kind)
VALUES (10013), (10051), (10102)
ON CONFLICT (kind) DO NOTHING;

CREATE OR REPLACE FUNCTION enqueue_superseded_replaceable(
    p_pubkey TEXT,
    p_kind INTEGER,
    p_d_tag TEXT
) RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    winner_id TEXT;
    winner_created BIGINT;
BEGIN
    IF p_kind NOT IN (
        0, 3, 10000, 10001, 10002, 10003, 10006, 10007, 10009, 10012, 10013, 10015, 10019, 10030, 10050, 10051, 10063, 10102,
        30000, 30002, 30004, 30005, 30008, 30009, 30015, 30023, 30311, 30315, 34235, 34236,
        39000, 39001, 39002, 39003
    ) THEN
        RETURN;
    END IF;

    SELECT event_id, created_at
      INTO winner_id, winner_created
      FROM replaceable_state
     WHERE pubkey = p_pubkey
       AND kind = p_kind
       AND d_tag = p_d_tag;
    IF winner_id IS NULL THEN
        RETURN;
    END IF;

    DELETE FROM replaceable_event_purge WHERE event_id = winner_id;

    INSERT INTO replaceable_event_purge (event_id, created_at)
    SELECT e.id, e.created_at
    FROM events e
    WHERE e.pubkey = p_pubkey
      AND e.kind = p_kind
      AND e.id <> winner_id
      AND (
          e.created_at < winner_created
          OR (e.created_at = winner_created AND e.id > winner_id)
      )
      AND (
          p_d_tag = ''
          OR COALESCE((
              SELECT et.value
              FROM event_tags et
              WHERE et.event_id = e.id
                AND et.tag_name = 'd'
                AND et.value_index = 0
              ORDER BY et.tag_index ASC
              LIMIT 1
          ), '') = p_d_tag
      )
    ON CONFLICT (event_id) DO NOTHING;
END;
$$;
