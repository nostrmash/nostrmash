-- Active probe budgets count attempts, not the current lease row. Reclaiming
-- an expired lease inserts another attempt and a new token. A worker that
-- still holds an older token cannot finish the newer lease.

CREATE TABLE relay_probe_attempts (
    id BIGSERIAL PRIMARY KEY,
    url_key TEXT NOT NULL,
    capability TEXT NOT NULL,
    host TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'started'
);

CREATE INDEX relay_probe_attempts_started_idx
    ON relay_probe_attempts (started_at);

CREATE INDEX relay_probe_attempts_host_started_idx
    ON relay_probe_attempts (host, started_at);

CREATE TABLE relay_probe_leases (
    url_key TEXT NOT NULL,
    capability TEXT NOT NULL,
    host TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (url_key, capability)
);

CREATE OR REPLACE FUNCTION claim_relay_probe(
    p_url_key TEXT,
    p_capability TEXT,
    p_host TEXT,
    p_worker TEXT,
    p_global_limit INTEGER,
    p_min_seconds INTEGER,
    p_lease_seconds INTEGER
) RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    recent INTEGER;
    token TEXT;
BEGIN
    IF p_global_limit IS NULL OR p_global_limit < 1 THEN
        p_global_limit := 20;
    END IF;
    IF p_global_limit > 60 THEN
        p_global_limit := 60;
    END IF;
    IF p_min_seconds IS NULL OR p_min_seconds < 1 THEN
        p_min_seconds := 1;
    END IF;
    IF p_lease_seconds IS NULL OR p_lease_seconds < 30 THEN
        p_lease_seconds := 120;
    END IF;
    PERFORM pg_advisory_xact_lock(hashtext('nostrmash-relay-probe'));
    DELETE FROM relay_probe_attempts
    WHERE id IN (
        SELECT id FROM relay_probe_attempts
        WHERE started_at < now() - interval '7 days'
        LIMIT 200
    );
    IF EXISTS (
        SELECT 1 FROM relay_probe_leases
        WHERE url_key = p_url_key
          AND capability = p_capability
          AND expires_at > now()
    ) THEN
        RETURN '';
    END IF;
    IF EXISTS (
        SELECT 1 FROM relay_probe_attempts
        WHERE url_key = p_url_key
          AND capability = p_capability
          AND started_at > now() - make_interval(secs => p_min_seconds)
    ) THEN
        RETURN '';
    END IF;
    SELECT count(*) INTO recent
    FROM relay_probe_attempts
    WHERE started_at > now() - interval '1 hour';
    IF recent >= p_global_limit THEN
        RETURN '';
    END IF;
    SELECT count(*) INTO recent
    FROM relay_probe_attempts
    WHERE host = p_host
      AND started_at > now() - interval '1 hour';
    IF recent >= 4 THEN
        RETURN '';
    END IF;
    token := gen_random_uuid()::text;
    INSERT INTO relay_probe_attempts (url_key, capability, host, worker_id, lease_token)
    VALUES (p_url_key, p_capability, p_host, p_worker, token);
    INSERT INTO relay_probe_leases (url_key, capability, host, worker_id, lease_token, expires_at)
    VALUES (p_url_key, p_capability, p_host, p_worker, token, now() + make_interval(secs => p_lease_seconds))
    ON CONFLICT (url_key, capability) DO UPDATE
        SET host = EXCLUDED.host,
            worker_id = EXCLUDED.worker_id,
            lease_token = EXCLUDED.lease_token,
            started_at = now(),
            expires_at = EXCLUDED.expires_at;
    RETURN token;
END;
$$;
