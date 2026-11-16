-- Serve the ordered note claim from note rows only.
--
-- idx_pending_meilisearch_syncs_kind_marked_at orders by kind first.
-- The note claim filters several kinds and then orders by marked_at, so
-- the planner kept the marked_at index and skipped the older profile
-- prefix under FOR UPDATE. That prefix is most of the queue and the
-- scan hits statement_timeout.
--
-- This partial index has the same order as the claim and contains only
-- the indexed note kinds. Profile claims stay on the kind index.
--
-- statement_timeout is disabled because the build locks the queue and
-- can outlast the cluster default.

SET LOCAL statement_timeout = 0;

CREATE INDEX idx_pending_meilisearch_syncs_note_marked_at
    ON pending_meilisearch_syncs (marked_at ASC, event_id ASC)
    WHERE kind IN (1, 20, 21, 22, 30023, 30311, 34235, 34236);
