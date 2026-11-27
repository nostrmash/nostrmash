-- A deleted replaceable winner is replaced only by a remaining event that
-- NIP-09 still allows. The repair stays open until that event's projections
-- agree with canonical state. An event-id deletion can leave an older event
-- authoritative. An address deletion cannot resurrect a covered event.

CREATE OR REPLACE FUNCTION repair_replaceable_tie(p_limit INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    repaired INTEGER := 0;
    v_idle BOOLEAN;
    v_next TIMESTAMPTZ;
    v_pubkey TEXT;
    v_kind INTEGER;
    v_d_tag TEXT;
    v_better TEXT;
    v_job BIGINT;
    v_updated INTEGER;
    v_rows INTEGER := 0;
    v_started TIMESTAMPTZ := clock_timestamp();
    v_state TEXT;
    v_job_status TEXT;
    v_job_attempts INTEGER;
    v_job_max INTEGER;
    v_job_error TEXT;
    v_replacement TEXT;
    v_replacement_created BIGINT;
    v_ready BOOLEAN;
BEGIN
    IF p_limit IS NULL OR p_limit < 1 THEN
        p_limit := 100;
    END IF;
    IF p_limit > 200 THEN
        p_limit := 200;
    END IF;

    INSERT INTO relay_intel_work_cursor (work_name)
    VALUES ('tie_repair')
    ON CONFLICT (work_name) DO NOTHING;

    SELECT idle, next_run_at, cursor_pubkey, cursor_kind, cursor_d_tag
      INTO v_idle, v_next, v_pubkey, v_kind, v_d_tag
      FROM relay_intel_work_cursor
     WHERE work_name = 'tie_repair'
     FOR UPDATE;

    IF NOT v_idle OR v_next <= clock_timestamp() THEN
        FOR rec IN
            SELECT pubkey, kind, d_tag, event_id, created_at
            FROM replaceable_state
            WHERE (pubkey, kind, d_tag) > (v_pubkey, v_kind, v_d_tag)
            ORDER BY pubkey, kind, d_tag
            LIMIT p_limit
            FOR UPDATE SKIP LOCKED
        LOOP
            v_rows := v_rows + 1;
            v_pubkey := rec.pubkey;
            v_kind := rec.kind;
            v_d_tag := rec.d_tag;

            SELECT e.id
              INTO v_better
              FROM events e
             WHERE e.pubkey = rec.pubkey
               AND e.kind = rec.kind
               AND e.created_at = rec.created_at
               AND e.id < rec.event_id
               AND (
                    NOT relay_intel_addressable(rec.kind)
                    OR relay_intel_event_d_tag(e.id) = rec.d_tag
               )
               AND NOT relay_intel_deleted(rec.pubkey, rec.kind, rec.d_tag, e.id, e.created_at)
             ORDER BY e.id ASC
             LIMIT 1;
            IF v_better IS NULL THEN
                CONTINUE;
            END IF;

            v_updated := 0;
            BEGIN
                UPDATE replaceable_state
                   SET event_id = v_better,
                       updated_at = now()
                 WHERE pubkey = rec.pubkey
                   AND kind = rec.kind
                   AND d_tag = rec.d_tag
                   AND event_id = rec.event_id
                   AND created_at = rec.created_at;
                GET DIAGNOSTICS v_updated = ROW_COUNT;
            EXCEPTION
                WHEN unique_violation THEN
                    v_updated := 0;
            END;
            IF v_updated = 0 THEN
                CONTINUE;
            END IF;

            DELETE FROM replaceable_event_purge WHERE event_id = v_better;
            INSERT INTO replaceable_event_purge (event_id, created_at)
            VALUES (rec.event_id, rec.created_at)
            ON CONFLICT (event_id) DO NOTHING;

            IF rec.kind IN (3, 10002, 10006, 10007, 10012, 10013, 10050, 10051, 10063, 10102, 30002) THEN
                DELETE FROM relay_role_memberships
                 WHERE pubkey = rec.pubkey
                   AND source_kind = rec.kind
                   AND d_tag = rec.d_tag
                   AND source_event_id = rec.event_id;
                UPDATE relay_config_heads
                   SET event_id = v_better,
                       needs_replay = true,
                       updated_at = now()
                 WHERE pubkey = rec.pubkey
                   AND source_kind = rec.kind
                   AND d_tag = rec.d_tag
                   AND event_id IN (rec.event_id, v_better);
            END IF;

            v_job := NULL;
            v_job_status := NULL;
            v_job_attempts := NULL;
            v_job_max := NULL;
            SELECT id, status, attempts, max_attempts
              INTO v_job, v_job_status, v_job_attempts, v_job_max
              FROM jobs
             WHERE idempotency_key = 'derive_event_bundle:' || v_better
             FOR UPDATE;
            IF v_job IS NULL THEN
                INSERT INTO jobs (job_type, worker_pool, payload, idempotency_key, max_attempts, run_after)
                VALUES (
                    'derive_event_bundle',
                    'default',
                    jsonb_build_object('event_id', v_better),
                    'derive_event_bundle:' || v_better,
                    5,
                    now()
                )
                ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
                RETURNING id, status, attempts, max_attempts
                INTO v_job, v_job_status, v_job_attempts, v_job_max;
            END IF;
            IF v_job IS NULL THEN
                SELECT id, status, attempts, max_attempts
                  INTO v_job, v_job_status, v_job_attempts, v_job_max
                  FROM jobs
                 WHERE idempotency_key = 'derive_event_bundle:' || v_better
                 FOR UPDATE;
            END IF;

            IF v_job_status = 'running' THEN
                INSERT INTO relay_intel_repairs (
                    winner_event_id, loser_event_id, pubkey, kind, d_tag, status, job_id, attempts, last_error, finished_at
                ) VALUES (
                    v_better, rec.event_id, rec.pubkey, rec.kind, rec.d_tag,
                    'running', v_job, 0, 'derivation already running', NULL
                )
                ON CONFLICT (winner_event_id) DO UPDATE
                    SET loser_event_id = EXCLUDED.loser_event_id,
                        status = 'running',
                        job_id = EXCLUDED.job_id,
                        last_error = EXCLUDED.last_error,
                        finished_at = NULL,
                        updated_at = now();
            ELSIF v_job_status = 'dead' OR COALESCE(v_job_attempts, 0) >= COALESCE(v_job_max, 5) THEN
                INSERT INTO relay_intel_repairs (
                    winner_event_id, loser_event_id, pubkey, kind, d_tag, status, job_id, attempts, last_error
                ) VALUES (
                    v_better, rec.event_id, rec.pubkey, rec.kind, rec.d_tag,
                    'failed', v_job, 5, 'derivation retries exhausted'
                )
                ON CONFLICT (winner_event_id) DO UPDATE
                    SET loser_event_id = EXCLUDED.loser_event_id,
                        status = 'failed',
                        job_id = EXCLUDED.job_id,
                        last_error = EXCLUDED.last_error,
                        updated_at = now();
            ELSIF v_job_status = 'succeeded' THEN
                UPDATE jobs
                   SET status = 'pending',
                       locked_at = NULL,
                       locked_by = NULL,
                       finished_at = NULL,
                       run_after = now(),
                       updated_at = now()
                 WHERE id = v_job
                   AND status = 'succeeded'
                   AND attempts < max_attempts;
                INSERT INTO relay_intel_repairs (
                    winner_event_id, loser_event_id, pubkey, kind, d_tag, status, job_id, attempts, last_error, finished_at
                ) VALUES (
                    v_better, rec.event_id, rec.pubkey, rec.kind, rec.d_tag, 'queued', v_job, 0, NULL, NULL
                )
                ON CONFLICT (winner_event_id) DO UPDATE
                    SET loser_event_id = EXCLUDED.loser_event_id,
                        pubkey = EXCLUDED.pubkey,
                        kind = EXCLUDED.kind,
                        d_tag = EXCLUDED.d_tag,
                        status = 'queued',
                        job_id = EXCLUDED.job_id,
                        last_error = NULL,
                        finished_at = NULL,
                        updated_at = now();
            ELSE
                INSERT INTO relay_intel_repairs (
                    winner_event_id, loser_event_id, pubkey, kind, d_tag, status, job_id, attempts, last_error, finished_at
                ) VALUES (
                    v_better, rec.event_id, rec.pubkey, rec.kind, rec.d_tag, 'queued', v_job, 0, NULL, NULL
                )
                ON CONFLICT (winner_event_id) DO UPDATE
                    SET loser_event_id = EXCLUDED.loser_event_id,
                        pubkey = EXCLUDED.pubkey,
                        kind = EXCLUDED.kind,
                        d_tag = EXCLUDED.d_tag,
                        status = 'queued',
                        job_id = EXCLUDED.job_id,
                        last_error = NULL,
                        finished_at = NULL,
                        updated_at = now();
            END IF;
            repaired := repaired + 1;
        END LOOP;

        IF v_rows < p_limit THEN
            UPDATE relay_intel_work_cursor
               SET cursor_pubkey = '',
                   cursor_kind = 0,
                   cursor_d_tag = '',
                   idle = true,
                   next_run_at = clock_timestamp() + interval '1 hour',
                   last_completed_at = clock_timestamp(),
                   last_duration_ms = (extract(epoch FROM clock_timestamp() - v_started) * 1000)::bigint,
                   last_rows = v_rows,
                   last_error = NULL,
                   updated_at = now()
             WHERE work_name = 'tie_repair';
        ELSE
            UPDATE relay_intel_work_cursor
               SET cursor_pubkey = v_pubkey,
                   cursor_kind = v_kind,
                   cursor_d_tag = v_d_tag,
                   idle = false,
                   last_rows = v_rows,
                   last_error = NULL,
                   updated_at = now()
             WHERE work_name = 'tie_repair';
        END IF;
    END IF;

    FOR rec IN
        SELECT q.pubkey, q.kind, q.d_tag, q.event_id
        FROM (
            SELECT rs.pubkey, rs.kind, rs.d_tag, rs.event_id
            FROM deletion_events de
            JOIN replaceable_state rs
              ON rs.event_id = de.target_event_id
             AND rs.pubkey = de.deleter_pubkey
            UNION
            SELECT rs.pubkey, rs.kind, rs.d_tag, rs.event_id
            FROM deletion_addresses da
            JOIN replaceable_state rs
              ON rs.pubkey = da.pubkey
             AND rs.kind = da.kind
             AND rs.created_at <= da.created_at
             AND (
                 (rs.kind >= 30000 AND rs.kind < 40000 AND rs.d_tag = da.d_tag)
                 OR rs.kind < 30000
                 OR rs.kind >= 40000
             )
        ) q
        LIMIT p_limit
    LOOP
        INSERT INTO relay_intel_repairs (
            winner_event_id, loser_event_id, pubkey, kind, d_tag, status, attempts
        ) VALUES (
            rec.event_id, rec.event_id, rec.pubkey, rec.kind, rec.d_tag, 'queued', 0
        )
        ON CONFLICT (winner_event_id) DO UPDATE
            SET status = 'queued',
                finished_at = NULL,
                last_error = 'winner deleted or missing',
                updated_at = now()
          WHERE relay_intel_repairs.status IN ('completed', 'superseded')
             OR (
                 relay_intel_repairs.status = 'deleted'
                 AND EXISTS (
                     SELECT 1 FROM replaceable_state rs
                     WHERE rs.event_id = relay_intel_repairs.winner_event_id
                       AND rs.pubkey = relay_intel_repairs.pubkey
                       AND rs.kind = relay_intel_repairs.kind
                       AND rs.d_tag = relay_intel_repairs.d_tag
                 )
             );
    END LOOP;

    FOR rec IN
        SELECT r.winner_event_id, r.loser_event_id, r.pubkey, r.kind, r.d_tag, r.attempts, r.job_id, r.status
        FROM relay_intel_repairs r
        WHERE r.status IN ('pending', 'queued', 'running')
        ORDER BY r.updated_at
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    LOOP
        v_job_status := NULL;
        v_job_attempts := NULL;
        v_job_max := NULL;
        v_job_error := NULL;
        IF rec.job_id IS NOT NULL THEN
            SELECT status, attempts, max_attempts, last_error
              INTO v_job_status, v_job_attempts, v_job_max, v_job_error
              FROM jobs
             WHERE id = rec.job_id;
        END IF;
        IF v_job_status IS NULL THEN
            SELECT id, status, attempts, max_attempts, last_error
              INTO v_job, v_job_status, v_job_attempts, v_job_max, v_job_error
              FROM jobs
             WHERE idempotency_key = 'derive_event_bundle:' || rec.winner_event_id;
            IF v_job IS NOT NULL THEN
                UPDATE relay_intel_repairs SET job_id = v_job WHERE winner_event_id = rec.winner_event_id;
            END IF;
        END IF;

        IF relay_intel_deleted(rec.pubkey, rec.kind, rec.d_tag, rec.winner_event_id, COALESCE((
            SELECT created_at FROM events WHERE id = rec.winner_event_id
        ), 0)) OR NOT EXISTS (SELECT 1 FROM events WHERE id = rec.winner_event_id) THEN
            v_replacement := NULL;
            v_replacement_created := NULL;
            SELECT e.id, e.created_at
              INTO v_replacement, v_replacement_created
              FROM events e
             WHERE e.pubkey = rec.pubkey
               AND e.kind = rec.kind
               AND e.id <> rec.winner_event_id
               AND (
                    NOT relay_intel_addressable(rec.kind)
                    OR relay_intel_event_d_tag(e.id) = rec.d_tag
               )
               AND NOT relay_intel_deleted(rec.pubkey, rec.kind, rec.d_tag, e.id, e.created_at)
             ORDER BY e.created_at DESC, e.id ASC
             LIMIT 1;
            IF v_replacement IS NULL THEN
                DELETE FROM replaceable_state
                 WHERE pubkey = rec.pubkey
                   AND kind = rec.kind
                   AND d_tag = rec.d_tag
                   AND event_id = rec.winner_event_id;
                DELETE FROM relay_role_memberships
                 WHERE pubkey = rec.pubkey
                   AND source_kind = rec.kind
                   AND d_tag = rec.d_tag
                   AND source_event_id = rec.winner_event_id;
                DELETE FROM relay_config_heads
                 WHERE pubkey = rec.pubkey
                   AND source_kind = rec.kind
                   AND d_tag = rec.d_tag
                   AND event_id = rec.winner_event_id;
                IF rec.kind = 0 THEN
                    DELETE FROM profiles_latest
                     WHERE pubkey = rec.pubkey
                       AND metadata_event_id = rec.winner_event_id;
                ELSIF rec.kind = 3 THEN
                    DELETE FROM contact_lists_latest
                     WHERE pubkey = rec.pubkey
                       AND event_id = rec.winner_event_id;
                ELSIF rec.kind = 10002 THEN
                    DELETE FROM relay_lists_latest
                     WHERE pubkey = rec.pubkey
                       AND event_id = rec.winner_event_id;
                END IF;
                INSERT INTO replaceable_event_purge (event_id, created_at)
                SELECT rec.winner_event_id, COALESCE(created_at, 0)
                  FROM events
                 WHERE id = rec.winner_event_id
                ON CONFLICT (event_id) DO NOTHING;
                UPDATE relay_intel_repairs
                   SET status = 'deleted',
                       finished_at = now(),
                       last_error = 'winner deleted or missing',
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
                CONTINUE;
            END IF;

            UPDATE replaceable_state
               SET event_id = v_replacement,
                   created_at = v_replacement_created,
                   updated_at = now()
             WHERE pubkey = rec.pubkey
               AND kind = rec.kind
               AND d_tag = rec.d_tag
               AND event_id = rec.winner_event_id;
            SELECT event_id INTO v_state
              FROM replaceable_state
             WHERE pubkey = rec.pubkey
               AND kind = rec.kind
               AND d_tag = rec.d_tag;
            IF v_state IS NULL THEN
                UPDATE relay_intel_repairs
                   SET status = 'deleted',
                       finished_at = now(),
                       last_error = 'winner deleted or missing',
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
                CONTINUE;
            END IF;
            IF v_state <> v_replacement THEN
                UPDATE relay_intel_repairs
                   SET status = 'superseded',
                       finished_at = now(),
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
                CONTINUE;
            END IF;
            DELETE FROM replaceable_event_purge WHERE event_id = v_replacement;
            INSERT INTO replaceable_event_purge (event_id, created_at)
            SELECT rec.winner_event_id, COALESCE(created_at, 0)
              FROM events
             WHERE id = rec.winner_event_id
            ON CONFLICT (event_id) DO NOTHING;
            UPDATE relay_config_heads
               SET event_id = v_replacement,
                   created_at = v_replacement_created,
                   needs_replay = true,
                   updated_at = now()
             WHERE pubkey = rec.pubkey
               AND source_kind = rec.kind
               AND d_tag = rec.d_tag
               AND event_id = rec.winner_event_id;
            DELETE FROM relay_role_memberships
             WHERE pubkey = rec.pubkey
               AND source_kind = rec.kind
               AND d_tag = rec.d_tag
               AND source_event_id = rec.winner_event_id;
            INSERT INTO jobs (job_type, worker_pool, payload, idempotency_key, max_attempts, run_after)
            VALUES (
                'derive_event_bundle', 'default',
                jsonb_build_object('event_id', v_replacement),
                'derive_event_bundle:' || v_replacement,
                5, now()
            )
            ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
            DO NOTHING;
            SELECT id, status, attempts, max_attempts, last_error
              INTO v_job, v_job_status, v_job_attempts, v_job_max, v_job_error
              FROM jobs
             WHERE idempotency_key = 'derive_event_bundle:' || v_replacement;
            v_ready := false;
            IF v_job_status = 'succeeded' AND v_state = v_replacement THEN
                v_ready := true;
                IF rec.kind IN (3, 10002, 10006, 10007, 10012, 10013, 10050, 10051, 10063, 10102, 30002) THEN
                    v_ready := NOT EXISTS (
                        SELECT 1 FROM relay_config_heads h
                         WHERE h.pubkey = rec.pubkey
                           AND h.source_kind = rec.kind
                           AND h.d_tag = rec.d_tag
                           AND h.event_id <> v_replacement
                    ) AND NOT EXISTS (
                        SELECT 1 FROM relay_role_memberships m
                         WHERE m.pubkey = rec.pubkey
                           AND m.source_kind = rec.kind
                           AND m.d_tag = rec.d_tag
                           AND m.source_event_id IS DISTINCT FROM v_replacement
                    );
                END IF;
                IF rec.kind = 0 THEN
                    v_ready := v_ready AND NOT EXISTS (
                        SELECT 1 FROM profiles_latest p
                         WHERE p.pubkey = rec.pubkey
                           AND p.metadata_event_id IS DISTINCT FROM v_replacement
                    );
                ELSIF rec.kind = 3 THEN
                    v_ready := v_ready AND NOT EXISTS (
                        SELECT 1 FROM contact_lists_latest c
                         WHERE c.pubkey = rec.pubkey
                           AND c.event_id IS DISTINCT FROM v_replacement
                    );
                ELSIF rec.kind = 10002 THEN
                    v_ready := v_ready AND NOT EXISTS (
                        SELECT 1 FROM relay_lists_latest r
                         WHERE r.pubkey = rec.pubkey
                           AND r.event_id IS DISTINCT FROM v_replacement
                    );
                END IF;
            END IF;
            IF v_ready THEN
                UPDATE relay_intel_repairs
                   SET status = 'deleted',
                       finished_at = now(),
                       last_error = 'winner deleted; replacement reconciled',
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
            ELSIF v_job_status = 'running' THEN
                UPDATE relay_intel_repairs
                   SET status = 'running',
                       last_error = 'replacement pending',
                       finished_at = NULL,
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
            ELSIF v_job_status = 'dead' OR (v_job_max IS NOT NULL AND v_job_attempts >= v_job_max) THEN
                UPDATE relay_intel_repairs
                   SET status = 'failed',
                       last_error = 'replacement retries exhausted',
                       finished_at = now(),
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
            ELSIF rec.attempts >= 4 THEN
                UPDATE relay_intel_repairs
                   SET status = 'failed',
                       last_error = 'projection inconsistent',
                       finished_at = now(),
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
            ELSE
                UPDATE jobs
                   SET status = 'pending',
                       locked_at = NULL,
                       locked_by = NULL,
                       finished_at = NULL,
                       run_after = now(),
                       updated_at = now()
                 WHERE idempotency_key = 'derive_event_bundle:' || v_replacement
                   AND status = 'succeeded';
                UPDATE relay_intel_repairs
                   SET status = 'queued',
                       attempts = attempts + 1,
                       last_error = 'replacement pending',
                       finished_at = NULL,
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
            END IF;
            CONTINUE;
        END IF;

        SELECT event_id INTO v_state
          FROM replaceable_state
         WHERE pubkey = rec.pubkey
           AND kind = rec.kind
           AND d_tag = rec.d_tag;
        IF v_state IS NOT NULL AND v_state <> rec.winner_event_id AND v_state <> rec.loser_event_id THEN
            UPDATE relay_intel_repairs
               SET status = 'superseded',
                   finished_at = now(),
                   updated_at = now()
             WHERE winner_event_id = rec.winner_event_id;
            CONTINUE;
        END IF;

        IF v_job_status IS NULL THEN
            UPDATE relay_intel_repairs
               SET status = 'failed',
                   last_error = 'derivation job missing',
                   finished_at = now(),
                   updated_at = now()
             WHERE winner_event_id = rec.winner_event_id;
        ELSIF v_job_status = 'pending' THEN
            UPDATE relay_intel_repairs
               SET status = 'queued', updated_at = now()
             WHERE winner_event_id = rec.winner_event_id;
        ELSIF v_job_status = 'running' THEN
            UPDATE relay_intel_repairs
               SET status = 'running', updated_at = now()
             WHERE winner_event_id = rec.winner_event_id;
        ELSIF v_job_status = 'dead' OR v_job_attempts >= v_job_max THEN
            UPDATE relay_intel_repairs
               SET status = 'failed',
                   last_error = COALESCE(v_job_error, 'retries exhausted'),
                   finished_at = now(),
                   updated_at = now()
             WHERE winner_event_id = rec.winner_event_id;
        ELSIF v_job_status = 'succeeded' AND v_state = rec.winner_event_id THEN
            v_ready := true;
            IF rec.kind IN (3, 10002, 10006, 10007, 10012, 10013, 10050, 10051, 10063, 10102, 30002) THEN
                v_ready := EXISTS (
                    SELECT 1 FROM relay_config_heads h
                     WHERE h.pubkey = rec.pubkey
                       AND h.source_kind = rec.kind
                       AND h.d_tag = rec.d_tag
                       AND h.event_id = rec.winner_event_id
                       AND h.needs_replay = false
                ) AND NOT EXISTS (
                    SELECT 1 FROM relay_role_memberships m
                     WHERE m.pubkey = rec.pubkey
                       AND m.source_kind = rec.kind
                       AND m.d_tag = rec.d_tag
                       AND m.source_event_id IS DISTINCT FROM rec.winner_event_id
                );
            END IF;
            IF rec.kind = 0 THEN
                v_ready := v_ready AND NOT EXISTS (
                    SELECT 1 FROM profiles_latest p
                     WHERE p.pubkey = rec.pubkey
                       AND p.metadata_event_id IS DISTINCT FROM rec.winner_event_id
                );
            ELSIF rec.kind = 3 THEN
                v_ready := v_ready AND NOT EXISTS (
                    SELECT 1 FROM contact_lists_latest c
                     WHERE c.pubkey = rec.pubkey
                       AND c.event_id IS DISTINCT FROM rec.winner_event_id
                );
            ELSIF rec.kind = 10002 THEN
                v_ready := v_ready AND NOT EXISTS (
                    SELECT 1 FROM relay_lists_latest r
                     WHERE r.pubkey = rec.pubkey
                       AND r.event_id IS DISTINCT FROM rec.winner_event_id
                );
            END IF;
            IF v_ready THEN
                UPDATE relay_intel_repairs
                   SET status = 'completed',
                       finished_at = now(),
                       last_error = NULL,
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
            ELSIF rec.attempts >= 4 THEN
                UPDATE relay_intel_repairs
                   SET status = 'failed',
                       last_error = 'projection inconsistent',
                       finished_at = now(),
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
            ELSE
                UPDATE jobs
                   SET status = 'pending',
                       locked_at = NULL,
                       locked_by = NULL,
                       finished_at = NULL,
                       run_after = now(),
                       updated_at = now()
                 WHERE id = COALESCE(rec.job_id, v_job)
                   AND status = 'succeeded';
                UPDATE relay_intel_repairs
                   SET status = 'queued',
                       attempts = attempts + 1,
                       last_error = 'projection pending',
                       updated_at = now()
                 WHERE winner_event_id = rec.winner_event_id;
            END IF;
        ELSIF rec.attempts >= 4 THEN
            UPDATE relay_intel_repairs
               SET status = 'failed',
                   last_error = 'derivation finished without the canonical winner',
                   finished_at = now(),
                   updated_at = now()
             WHERE winner_event_id = rec.winner_event_id;
        ELSE
            UPDATE jobs
               SET status = 'pending',
                   locked_at = NULL,
                   locked_by = NULL,
                   finished_at = NULL,
                   run_after = now(),
                   updated_at = now()
             WHERE id = rec.job_id
               AND status = 'succeeded';
            UPDATE relay_intel_repairs
               SET status = 'queued',
                   attempts = attempts + 1,
                   updated_at = now()
             WHERE winner_event_id = rec.winner_event_id;
        END IF;
    END LOOP;

    RETURN repaired;
END;
$$;

