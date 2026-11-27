-- The profile search-document DELETE trigger looks up other profiles with
-- the same normalized NIP-05 name. Give it an equality lookup so canonical
-- repair can remove a deleted profile without scanning unrelated profiles.
-- Prebuild this index concurrently on a populated database before migrating.
CREATE INDEX IF NOT EXISTS idx_profiles_latest_nip05_normalized
    ON profiles_latest (lower(btrim(nip05)));
