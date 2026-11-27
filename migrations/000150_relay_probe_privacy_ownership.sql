-- Probe follow-ups record scheduling deferrals separately from network attempts.
-- Ephemeral synthetic authors expire; the identity epoch invalidates negative caches.

ALTER TABLE relay_probe_followups
    ADD COLUMN IF NOT EXISTS deferrals INTEGER NOT NULL DEFAULT 0;

ALTER TABLE relay_probe_identities
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS relay_probe_identities_expires_idx
    ON relay_probe_identities (expires_at)
    WHERE expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS relay_probe_identity_epoch (
    id BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    epoch BIGINT NOT NULL DEFAULT 1
);

INSERT INTO relay_probe_identity_epoch (id, epoch)
VALUES (true, 1)
ON CONFLICT (id) DO NOTHING;
