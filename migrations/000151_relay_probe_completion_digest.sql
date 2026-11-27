-- A repeated probe completion is identical only when the semantic result matches.
-- Gift-wrap probe authors stay excluded for as long as their events can be replayed.

ALTER TABLE relay_probe_attempts
    ADD COLUMN IF NOT EXISTS result_digest TEXT NOT NULL DEFAULT '';

UPDATE relay_probe_identities
SET expires_at = NULL
WHERE expires_at IS NOT NULL;
