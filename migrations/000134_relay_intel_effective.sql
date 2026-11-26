-- NIP-01 timestamp ties keep the lower event id. This replaces the function
-- from 000133. Events already deleted under the previous higher-id rule are
-- not recovered. repair_replaceable_tie can promote a lower id that is still
-- stored. It does not rewrite every projection; replay that event to refresh
-- derived rows. It is not run automatically.

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

CREATE OR REPLACE FUNCTION repair_replaceable_tie(p_limit INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    repaired INTEGER := 0;
BEGIN
    IF p_limit IS NULL OR p_limit < 1 THEN
        p_limit := 100;
    END IF;
    IF p_limit > 200 THEN
        p_limit := 200;
    END IF;
    FOR rec IN
        SELECT rs.pubkey, rs.kind, rs.d_tag, rs.event_id, rs.created_at, min(e.id) AS better_id
        FROM replaceable_state rs
        JOIN events e
          ON e.pubkey = rs.pubkey
         AND e.kind = rs.kind
         AND e.created_at = rs.created_at
         AND e.id < rs.event_id
        WHERE rs.d_tag = ''
           OR COALESCE((
                SELECT et.value
                FROM event_tags et
                WHERE et.event_id = e.id
                  AND et.tag_name = 'd'
                  AND et.value_index = 0
                ORDER BY et.tag_index ASC
                LIMIT 1
           ), '') = rs.d_tag
        GROUP BY rs.pubkey, rs.kind, rs.d_tag, rs.event_id, rs.created_at
        LIMIT p_limit
    LOOP
        UPDATE replaceable_state
           SET event_id = rec.better_id
         WHERE pubkey = rec.pubkey
           AND kind = rec.kind
           AND d_tag = rec.d_tag
           AND event_id = rec.event_id;
        INSERT INTO replaceable_event_purge (event_id, created_at)
        VALUES (rec.event_id, rec.created_at)
        ON CONFLICT (event_id) DO NOTHING;
        DELETE FROM replaceable_event_purge WHERE event_id = rec.better_id;
        repaired := repaired + 1;
    END LOOP;
    RETURN repaired;
END;
$$;

CREATE TABLE IF NOT EXISTS deletion_addresses (
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    d_tag TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    PRIMARY KEY (event_id, kind, d_tag)
);

CREATE INDEX IF NOT EXISTS idx_deletion_addresses_coord
    ON deletion_addresses (pubkey, kind, d_tag, created_at DESC);

CREATE OR REPLACE VIEW relay_effective_memberships AS
SELECT
    m.url_key,
    m.role,
    m.endpoint_url,
    m.endpoint_type,
    m.pubkey,
    m.declaration_source,
    m.public_safe,
    m.source_kind,
    m.d_tag,
    m.source_event_id,
    m.created_at
FROM relay_role_memberships m
JOIN relay_config_heads h
  ON h.pubkey = m.pubkey
 AND h.source_kind = m.source_kind
 AND h.d_tag = m.d_tag
 AND h.event_id = m.source_event_id
WHERE NOT h.client_side_only
  AND NOT (
      m.source_kind = 3
      AND m.role IN ('public_inbox', 'public_outbox')
      AND EXISTS (
          SELECT 1 FROM relay_config_heads modern
          WHERE modern.pubkey = m.pubkey
            AND modern.source_kind = 10002
            AND modern.d_tag = ''
      )
  )
  AND NOT (
      m.source_kind = 10051
      AND EXISTS (
          SELECT 1 FROM relay_config_heads modern
          WHERE modern.pubkey = m.pubkey
            AND modern.source_kind = 10002
            AND modern.d_tag = ''
      )
  )
  AND NOT EXISTS (
      SELECT 1 FROM deletion_addresses d
      WHERE d.pubkey = h.pubkey
        AND d.kind = h.source_kind
        AND d.d_tag = h.d_tag
        AND d.created_at >= h.created_at
  )
  AND NOT EXISTS (
      SELECT 1 FROM deletion_events de
      WHERE de.target_event_id = h.event_id
        AND de.deleter_pubkey = h.pubkey
  )
UNION ALL
SELECT
    m.url_key,
    'keypackage' AS role,
    m.endpoint_url,
    m.endpoint_type,
    m.pubkey,
    'modern' AS declaration_source,
    m.public_safe,
    10002 AS source_kind,
    '' AS d_tag,
    m.source_event_id,
    m.created_at
FROM relay_role_memberships m
JOIN relay_config_heads h
  ON h.pubkey = m.pubkey
 AND h.source_kind = 10002
 AND h.d_tag = ''
 AND h.event_id = m.source_event_id
WHERE m.role = 'public_outbox'
  AND m.source_kind = 10002
  AND NOT h.client_side_only
  AND NOT EXISTS (
      SELECT 1 FROM deletion_addresses d
      WHERE d.pubkey = h.pubkey
        AND d.kind = 10002
        AND d.d_tag = ''
        AND d.created_at >= h.created_at
  )
  AND NOT EXISTS (
      SELECT 1 FROM deletion_events de
      WHERE de.target_event_id = h.event_id
        AND de.deleter_pubkey = h.pubkey
  );

CREATE OR REPLACE VIEW relay_effective_known AS
SELECT DISTINCT pubkey, role
FROM relay_effective_memberships
UNION
SELECT h.pubkey, 'public_inbox'
FROM relay_config_heads h
WHERE h.source_kind = 10002 AND h.d_tag = '' AND h.explicitly_empty
  AND NOT EXISTS (
      SELECT 1 FROM deletion_addresses d
      WHERE d.pubkey = h.pubkey AND d.kind = 10002 AND d.d_tag = '' AND d.created_at >= h.created_at
  )
UNION
SELECT h.pubkey, 'public_outbox'
FROM relay_config_heads h
WHERE h.source_kind = 10002 AND h.d_tag = '' AND h.explicitly_empty
  AND NOT EXISTS (
      SELECT 1 FROM deletion_addresses d
      WHERE d.pubkey = h.pubkey AND d.kind = 10002 AND d.d_tag = '' AND d.created_at >= h.created_at
  )
UNION
SELECT h.pubkey, 'public_inbox'
FROM relay_config_heads h
WHERE h.source_kind = 3 AND h.d_tag = '' AND h.explicitly_empty
  AND NOT EXISTS (
      SELECT 1 FROM relay_config_heads modern
      WHERE modern.pubkey = h.pubkey AND modern.source_kind = 10002 AND modern.d_tag = ''
  )
UNION
SELECT h.pubkey, 'public_outbox'
FROM relay_config_heads h
WHERE h.source_kind = 3 AND h.d_tag = '' AND h.explicitly_empty
  AND NOT EXISTS (
      SELECT 1 FROM relay_config_heads modern
      WHERE modern.pubkey = h.pubkey AND modern.source_kind = 10002 AND modern.d_tag = ''
  );
