-- A follow-up claim and its probe lease commit together.
-- The same event on two relays is two retention checks.

ALTER TABLE relay_probe_followups
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;

DROP INDEX IF EXISTS relay_probe_followups_event_capability_uidx;

CREATE UNIQUE INDEX IF NOT EXISTS relay_probe_followups_url_event_capability_uidx
    ON relay_probe_followups (url_key, event_id, capability);
