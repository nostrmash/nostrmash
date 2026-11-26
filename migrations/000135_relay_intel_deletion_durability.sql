-- Address tombstones must outlive the raw kind-5 event. 000134 referenced
-- events(id) ON DELETE CASCADE, so purging a processed deletion also deleted
-- the tombstone and let the old configuration return. This follows 000050.
-- Kind-5 events that were already purged cannot be reconstructed from this
-- database. backfill_deletion_addresses only reads kind-5 events that are
-- still stored.

ALTER TABLE deletion_addresses
    DROP CONSTRAINT IF EXISTS deletion_addresses_event_id_fkey;

-- An empty d tag is one addressable coordinate. It is not a wildcard for
-- every addressable event of that author and kind.

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
      AND COALESCE((
          SELECT et.value
          FROM event_tags et
          WHERE et.event_id = e.id
            AND et.tag_name = 'd'
            AND et.value_index = 0
          ORDER BY et.tag_index ASC
          LIMIT 1
      ), '') = p_d_tag
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
        WHERE COALESCE((
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

-- Replay still-stored kind-5 address deletions into the durable ledger.
-- Returns the number of tombstones inserted. Call it in batches. It does not
-- recover deletions whose raw events have already been purged.
CREATE OR REPLACE FUNCTION backfill_deletion_addresses(p_limit INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    inserted INTEGER := 0;
    kind_text TEXT;
    pubkey TEXT;
    d_tag TEXT;
    kind_value INTEGER;
BEGIN
    IF p_limit IS NULL OR p_limit < 1 THEN
        p_limit := 100;
    END IF;
    IF p_limit > 500 THEN
        p_limit := 500;
    END IF;
    FOR rec IN
        SELECT e.id, e.pubkey, e.created_at, t.value
        FROM events e
        JOIN event_tags t ON t.event_id = e.id AND t.tag_name = 'a'
        WHERE e.kind = 5
          AND split_part(t.value, ':', 1) ~ '^[0-9]+$'
          AND split_part(t.value, ':', 1)::integer IN (
              3, 10002, 10006, 10007, 10013, 10015, 10050, 10051, 10063, 10102, 30002
          )
          AND length(split_part(t.value, ':', 2)) = 64
          AND lower(split_part(t.value, ':', 2)) = lower(e.pubkey)
          AND length(t.value) <= length(split_part(t.value, ':', 1)) + 67 + 1024
          AND NOT EXISTS (
              SELECT 1 FROM deletion_addresses d
              WHERE d.event_id = e.id
                AND d.pubkey = lower(e.pubkey)
                AND d.kind = split_part(t.value, ':', 1)::integer
                AND d.d_tag = CASE
                    WHEN length(t.value) = length(split_part(t.value, ':', 1)) + 65 THEN ''
                    ELSE substr(t.value, length(split_part(t.value, ':', 1)) + 67)
                END
          )
        ORDER BY e.created_at, e.id, t.value
        LIMIT p_limit
    LOOP
        kind_text := split_part(rec.value, ':', 1);
        pubkey := split_part(rec.value, ':', 2);
        kind_value := kind_text::integer;
        IF length(rec.value) = length(kind_text) + 65 THEN
            d_tag := '';
        ELSE
            d_tag := substr(rec.value, length(kind_text) + 67);
        END IF;
        INSERT INTO deletion_addresses (event_id, pubkey, kind, d_tag, created_at)
        VALUES (rec.id, lower(pubkey), kind_value, d_tag, rec.created_at)
        ON CONFLICT (event_id, kind, d_tag) DO NOTHING;
        IF FOUND THEN
            inserted := inserted + 1;
        END IF;
    END LOOP;
    RETURN inserted;
END;
$$;

-- A kind 10002 head makes both public roles known, including a role with no
-- relays. A write-only list is an empty read list, not an unknown one.
CREATE OR REPLACE VIEW relay_effective_known AS
SELECT DISTINCT pubkey, role
FROM relay_effective_memberships
UNION
SELECT h.pubkey, 'public_inbox'
FROM relay_config_heads h
WHERE h.source_kind = 10002 AND h.d_tag = ''
  AND NOT EXISTS (
      SELECT 1 FROM deletion_addresses d
      WHERE d.pubkey = h.pubkey AND d.kind = 10002 AND d.d_tag = '' AND d.created_at >= h.created_at
  )
  AND NOT EXISTS (
      SELECT 1 FROM deletion_events de
      WHERE de.target_event_id = h.event_id AND de.deleter_pubkey = h.pubkey
  )
UNION
SELECT h.pubkey, 'public_outbox'
FROM relay_config_heads h
WHERE h.source_kind = 10002 AND h.d_tag = ''
  AND NOT EXISTS (
      SELECT 1 FROM deletion_addresses d
      WHERE d.pubkey = h.pubkey AND d.kind = 10002 AND d.d_tag = '' AND d.created_at >= h.created_at
  )
  AND NOT EXISTS (
      SELECT 1 FROM deletion_events de
      WHERE de.target_event_id = h.event_id AND de.deleter_pubkey = h.pubkey
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
