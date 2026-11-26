-- Capability samples from one attempt are idempotent, and candidate selection
-- can exclude a relay that is still inside its cooldown without a fragile
-- host split.

ALTER TABLE relay_capability_samples
    ADD COLUMN attempt_id BIGINT;

CREATE UNIQUE INDEX relay_capability_samples_attempt_uidx
    ON relay_capability_samples (attempt_id, capability, status, detail)
    WHERE attempt_id IS NOT NULL;

CREATE INDEX relay_probe_attempts_url_capability_started_idx
    ON relay_probe_attempts (url_key, capability, started_at DESC);

CREATE TABLE relay_probe_followups (
    id BIGSERIAL PRIMARY KEY,
    url_key TEXT NOT NULL,
    endpoint_url TEXT NOT NULL,
    event_id TEXT NOT NULL,
    capability TEXT NOT NULL,
    check_after TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    result TEXT NOT NULL DEFAULT ''
);

CREATE INDEX relay_probe_followups_due_idx
    ON relay_probe_followups (check_after, id)
    WHERE finished_at IS NULL;

CREATE OR REPLACE FUNCTION relay_probe_host(raw TEXT) RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT lower(
        CASE
            WHEN raw ~* '^wss?://\[[0-9a-f:]+\]'
                THEN substring(raw FROM '^wss?://\[([0-9a-f:]+)\]')
            ELSE split_part(split_part(regexp_replace(raw, '^wss?://', '', 'i'), '/', 1), ':', 1)
        END
    );
$$;
