-- Repairing a replaceable tie must not leave role memberships pointing at
-- the event that lost. The head is marked truncated until the winning event
-- is projected again. Do not treat that coordinate's role statistics as
-- current until the replay finishes. This function is not run by the migration.

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
        DELETE FROM relay_role_memberships
         WHERE pubkey = rec.pubkey
           AND source_kind = rec.kind
           AND d_tag = rec.d_tag
           AND source_event_id = rec.event_id;
        UPDATE relay_config_heads
           SET event_id = rec.better_id,
               truncated = true,
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
