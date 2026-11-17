-- Claim notes and profiles from their own index.
--
-- The note claim ordered pending_meilisearch_syncs by marked_at and then
-- kept rows whose events.kind was a note. The oldest rows are profiles, so
-- the scan walked that prefix, locked it with FOR UPDATE, and hit
-- statement_timeout. The sweeper then returned before the profile batch.
--
-- kind is copied from events when a row is enqueued. A nostr event's kind
-- does not change. Rows with no event, or a kind the sweeper does not
-- index, are removed: they can never be claimed.
--
-- statement_timeout is disabled because the backfill and the new index
-- take a lock on the whole queue and can outlast the cluster default.

SET LOCAL statement_timeout = 0;

ALTER TABLE pending_meilisearch_syncs
    ADD COLUMN kind INTEGER;

UPDATE pending_meilisearch_syncs p
SET kind = e.kind
FROM events e
WHERE e.id = p.event_id
  AND p.kind IS NULL;

DELETE FROM pending_meilisearch_syncs
WHERE kind IS NULL
   OR kind NOT IN (0, 1, 20, 21, 22, 30023, 30311, 34235, 34236);

ALTER TABLE pending_meilisearch_syncs
    ALTER COLUMN kind SET NOT NULL;

CREATE INDEX idx_pending_meilisearch_syncs_kind_marked_at
    ON pending_meilisearch_syncs (kind, marked_at ASC, event_id ASC);
