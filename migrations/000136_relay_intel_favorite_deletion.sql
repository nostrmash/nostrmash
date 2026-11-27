-- Kind 10012 is a favorite relay list. The deletion backfill omitted it, so a
-- stored kind-5 event for that list could be skipped. Previously purged
-- kind-5 events still cannot be reconstructed.
--
-- repair_replaceable_tie now points a matching relay projection head at the
-- lower event id. Memberships are not rewritten here. Replay the promoted
-- event before treating role statistics for that coordinate as current.
-- This function is still not run automatically during migration.

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
        UPDATE relay_config_heads
           SET event_id = rec.better_id,
               updated_at = now()
         WHERE pubkey = rec.pubkey
           AND source_kind = rec.kind
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
              3, 10002, 10006, 10007, 10012, 10013, 10015, 10050, 10051, 10063, 10102, 30002
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
