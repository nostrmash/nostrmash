-- Retention follow-ups use the same attempt and lease path as other probes.
-- One observation is one row, and it stays unfinished until the retrieval runs.

ALTER TABLE relay_probe_followups
    ADD COLUMN status TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN lease_token TEXT NOT NULL DEFAULT '',
    ADD COLUMN published_at TIMESTAMPTZ,
    ADD COLUMN verified_at TIMESTAMPTZ,
    ADD COLUMN elapsed_seconds INTEGER,
    ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX relay_probe_followups_event_capability_uidx
    ON relay_probe_followups (event_id, capability);

CREATE INDEX relay_probe_followups_status_idx
    ON relay_probe_followups (status, check_after, id)
    WHERE finished_at IS NULL;
