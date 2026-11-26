-- Active probe attempts are shared by every worker. A process restart does not
-- reset the hourly budget, and two workers cannot claim the same relay and
-- capability while a lease is open. The function is not run during migration.
-- Active probes stay off unless the operator sets RELAY_INTEL_ACTIVE_PROBES.

CREATE TABLE IF NOT EXISTS relay_probe_leases (
    url_key TEXT NOT NULL,
    capability TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    PRIMARY KEY (url_key, capability)
);

CREATE INDEX IF NOT EXISTS idx_relay_probe_leases_started
    ON relay_probe_leases (started_at);

CREATE OR REPLACE FUNCTION claim_relay_probe(
    p_url_key TEXT,
    p_capability TEXT,
    p_worker_id TEXT,
    p_global_limit INTEGER,
    p_min_seconds INTEGER,
    p_lease_seconds INTEGER
) RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    recent INTEGER;
    existing relay_probe_leases%ROWTYPE;
BEGIN
    IF p_url_key IS NULL OR p_url_key = '' OR p_capability IS NULL OR p_capability = '' THEN
        RETURN false;
    END IF;
    IF p_global_limit IS NULL OR p_global_limit < 1 THEN
        p_global_limit := 20;
    END IF;
    IF p_global_limit > 60 THEN
        p_global_limit := 60;
    END IF;
    IF p_min_seconds IS NULL OR p_min_seconds < 1 THEN
        p_min_seconds := 86400;
    END IF;
    IF p_lease_seconds IS NULL OR p_lease_seconds < 1 THEN
        p_lease_seconds := 120;
    END IF;

    PERFORM pg_advisory_xact_lock(hashtext('nostrmash-relay-probe'));

    SELECT * INTO existing
      FROM relay_probe_leases
     WHERE url_key = p_url_key
       AND capability = p_capability;
    IF FOUND THEN
        IF existing.finished_at IS NULL AND existing.started_at > now() - make_interval(secs => p_lease_seconds) THEN
            RETURN false;
        END IF;
        IF existing.finished_at IS NOT NULL AND existing.finished_at > now() - make_interval(secs => p_min_seconds) THEN
            RETURN false;
        END IF;
    END IF;

    SELECT count(*) INTO recent
      FROM relay_probe_leases
     WHERE started_at > now() - interval '1 hour';
    IF recent >= p_global_limit THEN
        RETURN false;
    END IF;

    INSERT INTO relay_probe_leases (url_key, capability, worker_id, started_at, finished_at)
    VALUES (p_url_key, p_capability, p_worker_id, now(), NULL)
    ON CONFLICT (url_key, capability) DO UPDATE
        SET worker_id = EXCLUDED.worker_id,
            started_at = EXCLUDED.started_at,
            finished_at = NULL;
    RETURN true;
END;
$$;
