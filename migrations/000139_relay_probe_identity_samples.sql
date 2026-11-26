-- Recent capability samples stay after the summary row is updated, so one
-- success cannot hide earlier failures. Synthetic probe pubkeys are recorded
-- so rankings and author stats can ignore them. Attempts older than the
-- summary are limited to the latest 20 rows per relay and capability by the
-- worker, not by this migration.

CREATE TABLE relay_capability_samples (
    id BIGSERIAL PRIMARY KEY,
    url_key TEXT NOT NULL,
    capability TEXT NOT NULL,
    status TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    source TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX relay_capability_samples_lookup_idx
    ON relay_capability_samples (url_key, capability, observed_at DESC, id DESC);

CREATE TABLE relay_probe_identities (
    pubkey TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
