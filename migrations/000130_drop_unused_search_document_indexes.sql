-- Drop the search_documents indexes added in 000129.
--
-- Meilisearch serves search. After those indexes had been in production,
-- pg_stat_user_indexes showed zero scans on all three, while every
-- search_documents update maintained a multi-gigabyte GIN and two prefix
-- btrees. That write cost was most of the table's daily growth.
--
-- The Postgres fallback in internal/store/read/search_documents.go still
-- runs these predicates. Without the indexes it scans the table, which is
-- the accepted cost of Meilisearch being down.
--
-- statement_timeout is disabled because dropping the GIN takes an
-- ACCESS EXCLUSIVE lock and can outlast the cluster default.

SET LOCAL statement_timeout = 0;

DROP INDEX IF EXISTS idx_search_documents_search_tsv;
DROP INDEX IF EXISTS idx_search_documents_entity_id_lower;
DROP INDEX IF EXISTS idx_search_documents_title_lower;
