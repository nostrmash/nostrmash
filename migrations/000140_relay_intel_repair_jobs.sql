-- Durable historical tie repair. The migration does not scan the database.
-- The worker calls repair_replaceable_tie in bounded batches. A lower-id
-- winner that is still stored is removed from the purge queue and replayed
-- through the existing derive_event_bundle job. Events already deleted
-- cannot be reconstructed.

CREATE TABLE IF NOT EXISTS relay_intel_repairs (
    winner_event_id TEXT PRIMARY KEY,
    loser_event_id TEXT NOT NULL,
    pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    d_tag TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (status IN ('pending', 'done'))
);

CREATE INDEX IF NOT EXISTS idx_relay_intel_repairs_pending
    ON relay_intel_repairs (created_at)
    WHERE status = 'pending';

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
        DELETE FROM replaceable_event_purge WHERE event_id = rec.better_id;
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
        INSERT INTO relay_intel_repairs (winner_event_id, loser_event_id, pubkey, kind, d_tag, status)
        VALUES (rec.better_id, rec.event_id, rec.pubkey, rec.kind, rec.d_tag, 'pending')
        ON CONFLICT (winner_event_id) DO UPDATE
           SET loser_event_id = EXCLUDED.loser_event_id,
               status = 'pending',
               updated_at = now();
        INSERT INTO jobs (job_type, worker_pool, payload, idempotency_key, max_attempts, run_after)
        VALUES (
            'derive_event_bundle',
            'default',
            jsonb_build_object('event_id', rec.better_id),
            'relay-intel-repair:' || rec.better_id,
            5,
            now()
        )
        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING;
        repaired := repaired + 1;
    END LOOP;

    FOR rec IN
        SELECT r.winner_event_id
        FROM relay_intel_repairs r
        WHERE r.status = 'pending'
          AND NOT EXISTS (
            SELECT 1 FROM jobs j
            WHERE j.idempotency_key = 'relay-intel-repair:' || r.winner_event_id
              AND j.status IN ('pending', 'running')
          )
        ORDER BY r.created_at
        LIMIT p_limit
    LOOP
        INSERT INTO jobs (job_type, worker_pool, payload, idempotency_key, max_attempts, run_after)
        VALUES (
            'derive_event_bundle',
            'default',
            jsonb_build_object('event_id', rec.winner_event_id),
            'relay-intel-repair:' || rec.winner_event_id || ':' || floor(extract(epoch FROM now()))::text,
            5,
            now()
        )
        ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING;
    END LOOP;

    UPDATE relay_intel_repairs r
       SET status = 'done',
           updated_at = now()
      FROM relay_config_heads h
     WHERE h.pubkey = r.pubkey
       AND h.source_kind = r.kind
       AND h.d_tag = r.d_tag
       AND h.event_id = r.winner_event_id
       AND h.truncated = false
       AND r.status = 'pending';
    RETURN repaired;
END;
$$;
