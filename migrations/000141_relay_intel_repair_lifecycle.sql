-- Historical tie repair is a bounded operational task. A full pass records
-- an idle cursor so maintenance does not scan replaceable_state every tick.
-- Completion follows the derive job and canonical state. A truncated but
-- valid relay projection is not an unfinished repair.
--
-- Ordinary replaceable kinds (below 30000) ignore stray d tags. Addressable
-- kinds keep an empty d tag as its own coordinate. Oversized kind numbers in
-- deletion a-tags are skipped instead of casting past integer range.

ALTER TABLE relay_intel_repairs
    ADD COLUMN IF NOT EXISTS job_id BIGINT,
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT,
    ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;

ALTER TABLE relay_intel_repairs DROP CONSTRAINT IF EXISTS relay_intel_repairs_status_check;

UPDATE relay_intel_repairs
   SET status = 'completed',
       finished_at = COALESCE(finished_at, updated_at)
 WHERE status = 'done';

ALTER TABLE relay_intel_repairs
    ADD CONSTRAINT relay_intel_repairs_status_check
    CHECK (status IN ('pending', 'queued', 'running', 'completed', 'superseded', 'deleted', 'failed'));

DROP INDEX IF EXISTS idx_relay_intel_repairs_pending;

CREATE INDEX IF NOT EXISTS idx_relay_intel_repairs_active
    ON relay_intel_repairs (updated_at)
    WHERE status IN ('pending', 'queued', 'running');

ALTER TABLE relay_config_heads
    ADD COLUMN IF NOT EXISTS needs_replay BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE relay_intel_snapshot
    ADD COLUMN IF NOT EXISTS last_duration_ms BIGINT,
    ADD COLUMN IF NOT EXISTS last_error TEXT,
    ADD COLUMN IF NOT EXISTS last_failure_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS relay_intel_work_cursor (
    work_name TEXT PRIMARY KEY,
    cursor_pubkey TEXT NOT NULL DEFAULT '',
    cursor_kind INTEGER NOT NULL DEFAULT 0,
    cursor_d_tag TEXT NOT NULL DEFAULT '',
    cursor_created_at BIGINT NOT NULL DEFAULT 0,
    cursor_event_id TEXT NOT NULL DEFAULT '',
    cursor_tag TEXT NOT NULL DEFAULT '',
    idle BOOLEAN NOT NULL DEFAULT false,
    next_run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_completed_at TIMESTAMPTZ,
    last_duration_ms BIGINT,
    last_error TEXT,
    last_rows BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_replaceable_state_kind_pubkey
    ON replaceable_state (kind, pubkey, d_tag);

CREATE OR REPLACE FUNCTION relay_intel_addressable(p_kind INTEGER)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT p_kind >= 30000 AND p_kind < 40000
$$;

CREATE OR REPLACE FUNCTION relay_intel_event_d_tag(p_event_id TEXT)
RETURNS TEXT
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE((
        SELECT et.value
        FROM event_tags et
        WHERE et.event_id = p_event_id
          AND et.tag_name = 'd'
          AND et.value_index = 0
        ORDER BY et.tag_index ASC
        LIMIT 1
    ), '')
$$;

CREATE OR REPLACE FUNCTION relay_intel_deleted(p_pubkey TEXT, p_kind INTEGER, p_d_tag TEXT, p_event_id TEXT, p_created BIGINT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM deletion_events de
        WHERE de.target_event_id = p_event_id
          AND de.deleter_pubkey = p_pubkey
    ) OR EXISTS (
        SELECT 1
        FROM deletion_addresses da
        WHERE da.pubkey = p_pubkey
          AND da.kind = p_kind
          AND da.created_at >= p_created
          AND (
              (relay_intel_addressable(p_kind) AND da.d_tag = p_d_tag)
              OR NOT relay_intel_addressable(p_kind)
          )
    )
$$;

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
            WITH upserted AS (
                INSERT INTO jobs (job_type, worker_pool, payload, idempotency_key, max_attempts, run_after)
                VALUES (
                    'derive_event_bundle',
                    'default',
                    jsonb_build_object('event_id', v_better),
                    'derive_event_bundle:' || v_better,
                    5,
                    now()
                )
                ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
                DO UPDATE
                    SET status = 'pending',
                        locked_at = NULL,
                        locked_by = NULL,
                        finished_at = NULL,
                        run_after = now(),
                        updated_at = now()
                  WHERE jobs.attempts < jobs.max_attempts
                RETURNING id
            )
            SELECT id INTO v_job FROM upserted;
            IF v_job IS NULL THEN
                SELECT id, status, attempts, max_attempts
                  INTO v_job, v_job_status, v_job_attempts, v_job_max
                  FROM jobs
                 WHERE idempotency_key = 'derive_event_bundle:' || v_better;
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
                repaired := repaired + 1;
                CONTINUE;
            END IF;

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
            UPDATE replaceable_state rs
               SET event_id = rec.loser_event_id,
                   updated_at = now()
             WHERE rs.pubkey = rec.pubkey
               AND rs.kind = rec.kind
               AND rs.d_tag = rec.d_tag
               AND rs.event_id = rec.winner_event_id
               AND EXISTS (SELECT 1 FROM events e WHERE e.id = rec.loser_event_id)
               AND NOT relay_intel_deleted(rec.pubkey, rec.kind, rec.d_tag, rec.loser_event_id, COALESCE((
                    SELECT created_at FROM events WHERE id = rec.loser_event_id
               ), 0));
            UPDATE relay_intel_repairs
               SET status = 'deleted',
                   finished_at = now(),
                   last_error = 'winner deleted or missing',
                   updated_at = now()
             WHERE winner_event_id = rec.winner_event_id;
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
            UPDATE relay_intel_repairs
               SET status = 'completed',
                   finished_at = now(),
                   last_error = NULL,
                   updated_at = now()
             WHERE winner_event_id = rec.winner_event_id;
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

CREATE OR REPLACE FUNCTION backfill_deletion_addresses(p_limit INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    inserted INTEGER := 0;
    v_idle BOOLEAN;
    v_next TIMESTAMPTZ;
    v_created BIGINT;
    v_event TEXT;
    v_tag TEXT;
    v_rows INTEGER := 0;
    v_kind_text TEXT;
    v_owner TEXT;
    v_coord TEXT;
    v_kind_value INTEGER;
BEGIN
    IF p_limit IS NULL OR p_limit < 1 THEN
        p_limit := 100;
    END IF;
    IF p_limit > 500 THEN
        p_limit := 500;
    END IF;

    INSERT INTO relay_intel_work_cursor (work_name)
    VALUES ('deletion_address_backfill')
    ON CONFLICT (work_name) DO NOTHING;

    SELECT idle, next_run_at, cursor_created_at, cursor_event_id, cursor_tag
      INTO v_idle, v_next, v_created, v_event, v_tag
      FROM relay_intel_work_cursor
     WHERE work_name = 'deletion_address_backfill'
     FOR UPDATE;

    IF v_idle AND v_next > clock_timestamp() THEN
        RETURN 0;
    END IF;

    FOR rec IN
        SELECT e.id, e.pubkey, e.created_at, t.value
        FROM events e
        JOIN event_tags t ON t.event_id = e.id AND t.tag_name = 'a'
        WHERE e.kind = 5
          AND (e.created_at, e.id, t.value) > (v_created, v_event, v_tag)
          AND split_part(t.value, ':', 1) IN (
              '3', '10002', '10006', '10007', '10012', '10013', '10015', '10050', '10051', '10063', '10102', '30002'
          )
          AND length(split_part(t.value, ':', 2)) = 64
          AND lower(split_part(t.value, ':', 2)) = lower(e.pubkey)
          AND length(t.value) <= 2048
        ORDER BY e.created_at, e.id, t.value
        LIMIT p_limit
    LOOP
        v_rows := v_rows + 1;
        v_created := rec.created_at;
        v_event := rec.id;
        v_tag := rec.value;
        v_kind_text := split_part(rec.value, ':', 1);
        IF v_kind_text !~ '^[0-9]{1,9}$' THEN
            CONTINUE;
        END IF;
        v_owner := split_part(rec.value, ':', 2);
        v_kind_value := v_kind_text::integer;
        IF relay_intel_addressable(v_kind_value) THEN
            IF length(rec.value) = length(v_kind_text) + 65 THEN
                v_coord := '';
            ELSE
                v_coord := substr(rec.value, length(v_kind_text) + 67);
            END IF;
        ELSE
            v_coord := '';
        END IF;
        IF length(v_coord) > 1024 THEN
            CONTINUE;
        END IF;
        INSERT INTO deletion_addresses (event_id, pubkey, kind, d_tag, created_at)
        VALUES (rec.id, lower(v_owner), v_kind_value, v_coord, rec.created_at)
        ON CONFLICT (event_id, kind, d_tag) DO NOTHING;
        IF FOUND THEN
            inserted := inserted + 1;
        END IF;
    END LOOP;

    IF v_rows < p_limit THEN
        UPDATE relay_intel_work_cursor
           SET cursor_created_at = 0,
               cursor_event_id = '',
               cursor_tag = '',
               idle = true,
               next_run_at = clock_timestamp() + interval '1 hour',
               last_completed_at = clock_timestamp(),
               last_rows = v_rows,
               updated_at = now()
         WHERE work_name = 'deletion_address_backfill';
    ELSE
        UPDATE relay_intel_work_cursor
           SET cursor_created_at = v_created,
               cursor_event_id = v_event,
               cursor_tag = v_tag,
               idle = false,
               last_rows = v_rows,
               updated_at = now()
         WHERE work_name = 'deletion_address_backfill';
    END IF;
    RETURN inserted;
END;
$$;
