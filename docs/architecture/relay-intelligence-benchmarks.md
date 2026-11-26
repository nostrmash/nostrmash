# Relay intelligence measurements

Operational results below were executed on 26 November 2026 against commit `16d258aea7582cca13a1a1d2fa053af5ad6f4e61` (`feat/relay-capability-probes`, "Expose gift-wrap privacy as structured capability fields."). `origin/feat/relay-capability-probes` was fetched immediately before the run and was still that commit. Agents A and B had not published a later implementation change, so these numbers describe that commit.

Raw results and plans:

- `benchmarks/relay-intelligence/operational/results-small.json`
- `benchmarks/relay-intelligence/operational/results-medium.json`
- `benchmarks/relay-intelligence/operational/plans-small.txt`
- `benchmarks/relay-intelligence/operational/plans-medium.txt`

A 500,000-account run was not executed.

## How to reproduce

Postgres 18 from the development compose file, bound to `127.0.0.1:5432`. The test creates schema `bench_ri_<unix nano>`, migrates it, and drops it. It does not migrate `public`, probe public relays, or write production data.

```sh
docker compose up -d postgres
scripts/relay_intel_operational_bench.sh
```

The script defaults are both scales, 200 mixed events, and 2 mixed repeats. One scale:

```sh
export TEST_DATABASE_URL=postgres://nostrmash:nostrmash@127.0.0.1:5432/nostrmash?sslmode=disable
RELAY_INTEL_OPERATIONAL_BENCH=1 RELAY_INTEL_OPERATIONAL_SCALE=small \
  go test ./internal/api/ -run TestOperationalRelayIntelligence -v -count=1 -timeout 180m
```

`RELAY_INTEL_OPERATIONAL_SCALE` is `small` (10,000 accounts), `medium` (100,000), or `both`. The test is skipped unless `RELAY_INTEL_OPERATIONAL_BENCH=1`, so CI does not run it.

This run, both scales, took 9 minutes 18 seconds of test time (89s small, 468s medium). Almost all of the medium time is the seed, and 5 minutes 3 seconds of that is one `event_relays` insert. See storage.

## Environment

Recorded by the test at startup, then confirmed with `docker info` after a Docker Desktop restart. The VM had stopped answering Postgres and Redis handshakes before the run. Docker Desktop was restarted and `nostrmash-postgres-1` was started again. Measurements start after `pg_isready` succeeded.

| Item | Value |
|---|---|
| Commit | `16d258aea7582cca13a1a1d2fa053af5ad6f4e61` |
| Host | Apple M4 Pro, 14 cores, 24 GB (`hw.memsize` 25769803776), macOS 27.0.1 |
| Go | 1.26.11, `GOMAXPROCS` 14 |
| PostgreSQL | 18.6, image `postgres:18-alpine`, Docker Engine 28.5.1 |
| Docker VM | 14 CPUs, 8092 MiB. `docker stats` reports 7.653 GiB. The compose service has no container CPU or memory limit |
| `shared_buffers` | 128MB |
| `work_mem` | 4MB on measured queries. The seed connection sets 64MB for its own aggregations |
| `maintenance_work_mem` | 64MB |
| `effective_cache_size` | 4GB |
| `max_connections` | 100 |
| `synchronous_commit` | on |
| `wal_compression` | off |
| `max_wal_size` | 1GB |
| Pool | max 20, min 2 |
| Other DB sessions at start | 2 |
| Other containers | `smartflowsocial-postgres-1` and `smartflowsocial-redis-1` were up and idle. NostrMash Redis and Meilisearch were not running |
| Cache | PostgreSQL shared buffers were not flushed. Plans report `shared hit` versus `read`. An application-cache miss is not labeled a cold-database measurement |

Meilisearch was not listening on `127.0.0.1:7700`, so `SyncMeilisearch` HTTP indexing was not measured. Derivation still ran `ApplyIncrementalAuthorStats`, which calls the synthetic-author check.

## Datasets

The generator is `seedOperationalFixture` in `internal/api/relay_intel_operational_seed_test.go`. Bulk SQL builds the population. Ten accounts per scale are real schnorr-signed events inserted with `InsertCanonicalEvent` and `DeriveEventBundle` (dense, sparse, creator, privacy, multi-role, empty, legacy, truncated, shared outbox, and a missing account with no events). Relay URL keys are checked against `relayurl.CanonicalKey` before the load continues. `ANALYZE` runs after the seed. Helper tables are dropped before the storage snapshot.

Popularity is stepped, not a smooth Zipf: slot 0 spreads across 8 relays, slot 1 across 16, slot 2 across 36, and later slots across the remaining 180. Accounts are not copies of one list. Buckets cover missing lists, explicit empty kind 10002, legacy kind 3, truncated kind 10002 (36 read tags, over the 32-tag cap), modern inbox/outbox, DM, search, favorite, blocked, a relay set, client-side kind 10013, keypackage-only accounts, superseded residue, 200 historical ties, deletion tombstones, dense and sparse follows, trust scores, capability observations, probe history from 3 hours earlier, and 180 pending retention follow-ups.

| | 10,000 accounts | 100,000 accounts |
|---|---:|---:|
| Seed | 6.8 s | 5 min 48 s |
| `event_relays` insert inside the seed | 3.0 s (7,100 rows) | 5 min 3 s (71,000 rows) |
| Events | 31,400 | 309,600 |
| Relay heads | 21,000 | 208,200 |
| Role memberships | 89,500 | 893,200 |
| Effective memberships | 110,600 | 1,104,200 |
| Explicitly empty heads | 500 | 5,000 |
| Truncated heads | 500 | 5,000 |
| Legacy kind 3 heads | 800 | 8,000 |
| Client-side heads | 600 | 6,000 |
| Accounts with no relay head | 800 | 8,000 |
| Follower edges | 11,604 | 35,904 |
| Trust scores | 2,360 | 20,360 |
| Author activity rows | 3,866 | 33,866 |
| Deletion tombstones | 40 | 40 |
| Superseded events queued for purge | 100 | 1,000 |
| Capability observations | 720 | 720 |
| Probe attempts at seed | 160 | 160 |
| Pending follow-ups | 180 | 180 |

Effective memberships are higher than stored memberships because `relay_effective_memberships` adds a keypackage row for each public-outbox membership on a kind 10002 head. Membership role counts at 100,000 accounts: public inbox 482,200, public outbox 229,000, DM inbox 44,000, favorite 44,000, relay set 44,000, blocked 22,000, search 22,000, keypackage 6,000.

The target was about 100,000 and 1,000,000 stored memberships. The load landed at 89,500 and 893,200. Those are the counts the timings use.

## API

Handlers are the real `GET` and `POST` functions. Cold means a new `relayintel.Service` per request. Warm means the same service. Optimize is not cached. Every optimize sample below returned HTTP 200, a personalized cohort, and at least one selected relay. Empty and missing configs are listed separately and are not optimization timings. p99 is reported only when the sample count is at least 100.

Latencies barely move from 89,500 memberships to 893,200. Personalized optimize is dominated by the cohort, not by the global membership table. A dense cohort of 400 follows is about 80–100 ms. A sparse cohort of 3 is about 12 ms. Query counts are the same at both sizes (32 to 48 statements per cold optimize).

Medium (100,000 accounts), which matches small within a few milliseconds:

| Call | n | Median | p95 | Max | DB queries, median |
|---|---:|---:|---:|---:|---:|
| `GET /api/v1/relays/roles` cold | 40 | 0.053 ms | 0.080 ms | 0.164 ms | 0 |
| `GET /api/v1/relays/roles` warm | 100 | 0.053 ms | 0.076 ms | 0.104 ms | 0 |
| `GET /api/v1/relays/rankings` cold | 40 | 3.97 ms | 5.02 ms | 7.64 ms | 6 |
| `GET /api/v1/relays/rankings` warm | 100 | 0.68 ms | 1.08 ms | 3.63 ms | 0 |
| `GET /api/v1/relays/capabilities` cold | 40 | 0.96 ms | 1.02 ms | 1.39 ms | 4 |
| `GET /api/v1/relays/capabilities` warm | 100 | 0.031 ms | 0.040 ms | 0.95 ms | 0 |
| Profile relay config, dense, cold | 20 | 2.42 ms | 3.94 ms | 4.39 ms | 9 |
| Profile relay config, dense, warm | 40 | 0.051 ms | 0.062 ms | 2.43 ms | 0 |
| Diagnostics, dense, cold | 20 | 3.01 ms | 3.11 ms | 4.11 ms | 11 |
| Diagnostics, dense, warm | 40 | 0.019 ms | 0.032 ms | 4.39 ms | 0 |
| Recommendations, dense cohort, cold | 20 | 101 ms | 170 ms | | 48 |
| Recommendations, dense cohort, warm | 40 | 1.12 ms | 1.16 ms | | 0 |
| Recommendations, sparse cohort, cold | 20 | 17.4 ms | 19.7 ms | | 48 |
| Existing profile `GET` | 40 | 0.44 ms | 0.49 ms | 1.39 ms | 2 |
| Existing event `GET` | 40 | 2.91 ms | 3.77 ms | 6.61 ms | 7 |

Rankings warm still takes about 0.7 ms with zero SQL statements. That is response encoding of the cached payload. Roles never touch the database. Capabilities, config, and diagnostics warm are memory hits.

### Optimization

All ten requests were personalized and selected relays. Cohort size is the personalized follow count actually used. "Queries" is the median statement count. Allocations are Go heap bytes per call from `runtime.MemStats`, so they include the test process, not only the optimizer.

| Scenario | n | Median, 10k | p95, 10k | Median, 100k | p95, 100k | Cohort | Selected | Queries |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Balanced | 30 | 94.4 ms | 98.5 ms | 93.8 ms | 101 ms | 400 | 10 | 48 |
| Creator | 30 | 12.1 ms | 15.2 ms | 12.2 ms | 13.8 ms | 3 | 6 | 32 |
| Privacy | 30 | 15.5 ms | 18.3 ms | 14.1 ms | 16.7 ms | 3 | 6 | 40 |
| Multi-role | 30 | 24.9 ms | 28.9 ms | 25.0 ms | 27.6 ms | 40 | 10 | 48 |
| Signed source events | 20 | 80.9 ms | 85.2 ms | 81.8 ms | 85.1 ms | 400 | 8 | 32 |
| Global and per-role constraints | 20 | 92.9 ms | 99.0 ms | 88.5 ms | 103 ms | 400 | 10 | 48 |
| Shared outbox and keypackage | 20 | 12.4 ms | 14.8 ms | 11.8 ms | 14.2 ms | 3 | 4 | 32 |
| Sparse evidence | 20 | 11.7 ms | 12.9 ms | 11.6 ms | 15.0 ms | 3 | 4 | 32 |
| Dense evidence | 20 | 81.1 ms | 83.8 ms | 82.0 ms | 84.1 ms | 400 | 7 | 32 |
| Max lists (decentralization preset) | 15 | 149 ms | 152 ms | 151 ms | 155 ms | 400 | 15 | 48 |

The signed-source response was a kind 10002 proposal with `based_on_event_id` set and 6 sources. Constraints reported `constraints_satisfied: true`. The shared keypackage request asked for `keypackage` and got a kind 10002 proposal, which is the fold onto the public outbox when a kind 10002 list exists. The max-list request allocated about 26 MB per call and selected 15 relays. Error rate was 0 in every scenario.

Sparse recommendations reported `proposal_safe: false` while still selecting 6 relays. That is a product flag on a successful plan, not a failed request. The latency above includes those responses.

## SQL and maintenance

Single-account reads stay index lookups. The plans are in `plans-medium.txt`. One effective-membership lookup for a persona executed in 0.040 ms server-side, all shared hits. A canonical head lookup is about 0.2 ms from the client at both sizes (100 samples).

| Operation | 10k accounts | 100k accounts |
|---|---|---|
| Effective membership, one pubkey, median | 0.32 ms (n=50) | 0.34 ms (n=50) |
| Effective membership, cohort up to 400, median / p95 | 9.4 / 10.2 ms (n=15) | 8.8 / 47.7 ms (n=15) |
| Effective known, median / p95 | 11.2 / 14.3 ms (n=20) | 10.9 / 47.2 ms (n=20) |
| Global role count, median | 0.25 ms | 0.29 ms |
| Candidate shortlist, median | 0.24 ms | 0.25 ms |
| Full statistics refresh | 1.69 s, `last_duration_ms` 1692, WAL 869 KB | 17.5 s, `last_duration_ms` 17522, WAL 1.26 MB |
| Refresh again inside 10 minutes | 1 ms, `ErrStatsFresh` | 1 ms, `ErrStatsFresh` |
| Tie repair, 200 fixes | 44 ms, WAL 838 KB | 45 ms, WAL 1.49 MB |
| Next tie page, 0 fixes | 11 ms | 13 ms |
| Idle tie check (scan skipped, deletion pass still runs) | 9 ms | 9 ms |
| Derivation of the 200 repaired events | 5.3 s | 5.6 s |
| One backfill batch after 100 heads marked for replay | 592 ms, WAL 927 KB | 1.22 s, WAL 3.86 MB |
| Reconcile after 40 heads deleted and the cursor forced active | 592 ms | 1.20 s |
| Deletion-address insert, then the following call | 100 rows in 2 ms, then 0 rows in 0 ms | 200 rows in 4 ms, then 0 rows in 0 ms |
| Due follow-up count | 0 ms | 0 ms |
| Replaceable purge, limit 100, after aging `first_seen_at` | 100 deleted in 659 ms, WAL 47 KB | 100 deleted in 7.8 s, WAL 1.03 MB |

The empty reconcile plan at 208,226 relay-kind replaceable rows (`plans-medium.txt`, "reconcile candidates"):

```text
Nested Loop Left Join  (actual time=532.162..532.162 rows=0)
  Filter removed 208226 rows
  -> Index Scan idx_replaceable_state_kind_pubkey
       actual time=0.027..96.817 rows=208226
       Buffers: shared hit=202903 read=8272
  -> Index Scan relay_config_heads_pkey
       actual time=0.002..0.002 rows=1 loops=208226
       Buffers: shared hit=829382 read=3522
Execution Time: 532.176 ms
```

`reconcileAuthoritative` in `internal/relayintel/backfill.go` skips that walk when the `projection_reconcile` cursor is idle and `next_run_at` is in the future. A short page sets the cursor idle for 15 minutes. The 532 ms figure is the forced walk, not the idle tick.

Tie repair of a real 200-row page stays at 45 ms when the table grows from 31k to 309k events. The idle check stays at 9 ms. Those are separate calls. The 5.3–5.6 s that follows is `derive_event_bundle` for the repaired events, not the SQL repair itself.

Statistics refresh grew about 10× (1.69 s to 17.5 s) as effective memberships grew about 10× (111k to 1.10M). The stats tables stayed about 410 KB and 1,095 rows at both sizes, because a refresh aggregates per relay and role. There are 240 relays in the fixture. The cost is the scan, not the stored aggregate. WAL for the refresh was about 1 MB even at the larger size.

The replaceable purge deleted the same 100 queued events in 0.66 s at the small size and 7.8 s at the medium size. The plan for that `DELETE` was not captured. The code path is `PurgeSupersededReplaceableEvents` in `internal/store/retention/events_retention_replaceable.go`.

Cohort and effective-known p95 rose to about 48 ms at 100,000 accounts while the median stayed near 9–11 ms. The sample counts are 15 and 20, so that tail is a handful of slower calls, not a stable p99.

## Mixed workload

Each pass ingests 200 distinct signed events through `InsertCanonicalEvent` (4 workers, 16 authors, every 8th event a kind 10002 list) and derives them with `ClaimAvailable` plus `ProcessJob` (2 workers) until the queue is empty. A maintenance loop runs immediately and then every 5 seconds: canonical repair and a 25-row replaceable purge in both scenarios; scenario B also runs `BackfillBatch(200)` and `RefreshStats` with a 10 minute minimum, which skipped because a refresh had just completed. Existing profile reads run on their own loop. Scenario B adds a second loop of dense recommendations and sparse optimize. Both scenarios keep canonical repair on. Projection is off in the baseline and on when Relay Intelligence is enabled.

The queue was drained before the first pass (50 leftover jobs from persona setup). Every pass then derived 200 of 200 events, with 0 pending jobs and 0 derive errors. Pool empty-acquire count was 0. Deadlocks were 0. `pg_stat_activity` lock waits sampled each maintenance cycle were 0.

| Scale | Scenario | Pass | Ingest / s | Derive / s | Profile median / p95 | RI API median / p95 (n) | WAL | Block reads |
|---|---|---:|---:|---:|---|---|---:|---:|
| 10k | baseline | 1 | 936 | 27.9 | 0.54 / 1.51 ms | | 4.2 MB | 37 |
| 10k | relay intel | 1 | 872 | 23.1 | 0.61 / 1.70 ms | 14.2 / 29.2 ms (652) | 5.9 MB | 25 |
| 10k | baseline | 2 | 874 | 27.2 | 0.56 / 1.51 ms | | 4.1 MB | 0 |
| 10k | relay intel | 2 | 822 | 22.7 | 0.62 / 1.71 ms | 14.6 / 29.1 ms (694) | 8.2 MB | 168 |
| 100k | baseline | 1 | 658 | 20.8 | 0.56 / 1.71 ms | | 5.4 MB | 1.24 M |
| 100k | relay intel | 1 | 776 | 17.0 | 0.64 / 1.94 ms | 15.4 / 25.6 ms (810) | 15.4 MB | 1.62 M |
| 100k | baseline | 2 | 825 | 20.2 | 0.72 / 1.87 ms | | 4.6 MB | 1.36 M |
| 100k | relay intel | 2 | 648 | 15.9 | 0.84 / 2.29 ms | 15.8 / 31.1 ms (826) | 30.7 MB | 1.74 M |

Derivation throughput was lower with projection on in all four paired passes: about 23–28 jobs/s versus 28 at 10k accounts, and 16–17 versus 20–21 at 100k accounts. That is roughly 17% and 20%. Ingest throughput did not move in a stable direction. At 100k accounts the first Relay Intelligence pass was faster than its baseline and the second was slower. Profile median stayed under 1 ms and p95 under 2.3 ms in every pass.

The mixed Relay Intelligence samples blend a dense recommendation and a sparse optimize, so 15 ms is not the isolated optimize number. Those requests still returned personalized plans (error count 0).

At 100k accounts both scenarios read well over a million blocks. `shared_buffers` is 128MB and the events heap alone is about 560MB, so this is the cache, not a Relay Intelligence scan. Relay Intelligence still read more blocks and wrote more WAL. The second 100k pass wrote 31 MB. Two repeats are not enough to treat 31 MB as the steady rate. The first pass, 15 MB versus 5.4 MB, is the cleaner comparison. No pass saturated the 20-connection pool or waited on a lock.

## Probe scheduling

`claim_relay_probe` stores its first argument as an opaque key. These claims use distinct `wss://…bench.example` strings. Production passes `CanonicalKey`. The function does not parse the key. Seeded history is 3 hours old, outside the 1 hour budget window. Each scenario deletes attempts from the last 2 hours and all leases first, so a 60-claim success sample cannot make the next scenario fail for the wrong reason. That delete is a simulated new hour, and it is labeled as such in the JSON.

Medium-scale numbers, 120 successful claims across two hours. The small run matched (median 0.32 ms, 120 of 120 claims).

| Scenario | Result |
|---|---|
| Successful claim, distinct host each time | 120 / 120. Median 0.32 ms, p95 0.43 ms, p99 0.48 ms, max 1.37 ms. WAL 91 KB then 61 KB |
| 61st claim in that hour | Empty token. 60 attempts already in the hour, 0 for that host. 0.22 ms |
| Active lease | Second claim empty. One active lease, one attempt. 0.23 ms |
| Cooldown | Lease deleted, attempt kept, `min_seconds` 3600. Second claim empty. 0.22 ms |
| Per host | 4 claims on one host succeeded, the 5th was empty. Hour total was 6, so the global cap was not the reason. 0.22 ms |
| Expired lease still inside cooldown | Empty token |
| Expired lease after the attempt was aged 30 s | New token, 0.32 ms |
| 8 workers, one key | 1 success |
| 8 workers, 8 keys | 8 successes in 4 ms |
| Single capability completion | 3.8 ms, no error |
| Duplicate completion | 1.2 ms, no error, 1 sample row |
| Three capabilities, one lease | 4.0 ms, no error |
| Follow-up claim and completion | Claim 3.3 ms, finish 2.2 ms, no dial |

The cooldown check in `claim_relay_probe` (`migrations/000138_relay_probe_attempts.sql`) runs before an expired lease can be replaced. A worker whose lease expired cannot take the same URL and capability again until `min_seconds` has passed. Reproduction: claim with `min_seconds` 1, set `expires_at` in the past, claim again immediately and get an empty token; set that attempt's `started_at` to 30 seconds ago and the next claim returns a new token. Leaving this as-is avoids a retry storm against a relay. Changing it would let a crashed worker retry inside the cooldown and spend the host budget of 4 faster. That choice belongs with the implementation owners. The default 24 hour minimum interval makes the interaction matter in production: lease expiry does not by itself allow another probe.

The SQL function clamps the global limit to 60 per hour. The 61st distinct host was rejected with 60 attempts already counted. Database time is not what caps probes. A claim is a third of a millisecond. The default configured budget of 20 per hour is a politeness limit. These runs did not contact relays, so they are not evidence for raising that budget.

## Synthetic author checks

`SyntheticAuthor` is one primary-key lookup on a cache miss. The plan in `plans-medium.txt` is an index-only scan, execution 0.018 ms, 2 buffer hits.

| Case | 10k run | 100k run |
|---|---|---|
| 1,000 distinct ordinary authors, cold | 1,000 queries, 0 hits, 184 µs/lookup | 1,000 queries, 190 µs/lookup |
| Same 1,000 again | 0 queries | 0 queries |
| Positive identity, cold / cached | 1 query / 0 queries | 1 / 0 |
| Negative, cold / cached | 1 / 0 | 1 / 0 |
| Negative after 16 s | 1 query | 1 query |
| 16 goroutines × 50 distinct keys | 800 queries | 800 queries |
| 25 ordinary derivations | 976 ms, 0 errors | 1.10 s, 0 errors |
| 25 synthetic derivations, including the identity insert | 737 ms, 0 errors | 1.33 s, 0 errors |

A burst of distinct authors is one query each. It is not an N+1 fan-out beyond that. The negative cache removes the query until the 15 second TTL. Repeating authors add nothing. Registry growth of 2,000 identities did not change the per-lookup query count. The synthetic derivation path also inserts `relay_probe_identities`, so its wall time is not a pure lookup comparison. Search indexing HTTP was not measured because Meilisearch was down.

## Storage and WAL

Relation totals after `ANALYZE`, from `pg_total_relation_size`. The medium events heap is the large object. Relay Intelligence's per-user data is the membership and head tables. The aggregate stats table does not grow with accounts.

| Relation | 10k total | 100k total |
|---|---:|---:|
| `events` | 60 MB | 589 MB |
| `relay_role_memberships` | 44 MB | 434 MB |
| `replaceable_state` | 19 MB | 190 MB |
| `relay_config_heads` | 8.1 MB | 78 MB |
| `event_relays` | 5.6 MB | 54 MB |
| `follower_edges` | 14 MB | 49 MB |
| `relay_role_stats` | 410 KB | 410 KB |
| `relay_probe_identities` (about 2,000 rows) | 475 KB | 475 KB |
| `relay_capability_observations` | 229 KB | 229 KB |
| `relay_probe_attempts` | 205 KB | 213 KB |
| `relay_probe_followups` | 180 KB | 180 KB |

About 485 bytes of membership storage per stored membership at 100k accounts (434 MB / 893,200), and about 375 bytes per head. A million stored memberships is on the order of half a gigabyte for that table plus its indexes, which is what 893,200 rows already occupy. That is a measured size, not an extrapolation past this fixture. Several million memberships were not loaded.

Refresh WAL stayed near 1 MB because the aggregate is small. Probe-claim WAL was about 60–90 KB per 60 successful claims. Role projection of the mixed workload is the extra WAL in the mixed table above.

The 5 minute `event_relays` insert is the per-row `event_relays_before_insert_is_first_seen` trigger from `migrations/000070_retention_query_cost_hardening.sql`. Seven thousand rows took 3.0 s. Seventy-one thousand rows took 5 minutes 3 seconds, with the backend at about 100% of one core and little buffer turnover. A single ingested event pays one trigger call. A bulk `INSERT … SELECT` into `event_relays` pays it per row. This seed is the measurement. It is not a Relay Intelligence projection cost.

Tables that keep growing with users and therefore need a retention decision:

- `relay_role_memberships` and `relay_config_heads`, tied to live declarations. They shrink when lists are replaced and retention deletes the old events. They are not an append-only log.
- `relay_probe_attempts`. `claim_relay_probe` already deletes up to 200 attempts older than 7 days on each claim.
- `relay_capability_samples`. The duplicate-completion check left a single row for a repeated detail. Volume follows probe traffic, which the 60/hour clamp bounds.
- `relay_probe_followups`. Due selection is an index scan (0.021 ms for 20 of 180). Finished rows are deleted by the follow-up worker after 30 days. This benchmark did not run that delete, because `followOne` dials the relay.
- `relay_probe_identities`. One row per synthetic pubkey. Two thousand rows were 475 KB. The registry is bounded by how many probe keys exist, not by author traffic.

`relay_role_stats` does not need a retention policy at this shape. It is one row per relay and role.

## Recommendations

These are the settings the measurements support.

| Setting | Recommendation |
|---|---|
| Projection | Leave it on. It did not stall ingest, did not fill the pool, and did not create lock waits. Derivation throughput was about 17–20% lower while it was on. That is the cost. |
| Statistics interval | Keep 10 minutes. 17.5 s at 1.10 million effective memberships still fits that interval. Incremental aggregation is not justified at this size. Do not treat 17.5 s as the cost at several million memberships; the refresh scaled about linearly from 111k to 1.10M, which would put a multi-million refresh near or above a minute. Remeasure before that. Alert on `relay_intel_snapshot.last_duration_ms` above 30 s. |
| Backfill batch and interval | Batch 200 and a 5 second interval are fine while the reconcile cursor is idle. A forced reconcile of 208k heads cost 0.5–1.2 s and about a million buffer hits. Keep the 15 minute idle after a short page. Do not call `ResetProjectionReconcile` on a timer. |
| Historical repair | Keep the cursor and the hour idle. A page that fixes 200 ties is 45 ms at both sizes. An idle check is 9 ms. The expensive part is deriving the repaired events (about 5.5 s for 200), which is the normal derivation path. |
| API cache | Keep it. Warm rankings, capabilities, config, diagnostics, and recommendations issued no SQL. Cold dense recommendations are about 100 ms and 48 queries; the 30 second cache is what makes that cheap to repeat. Optimize stays uncached and is the expensive call. A dense cohort is about 90 ms and 10–26 MB of allocations. |
| Pool | 20 connections absorbed 4 ingest workers, 2 derive workers, maintenance, profile traffic, and optimize traffic with zero empty acquires. Do not start from a smaller pool if optimize (about 48 queries) runs next to ingest. |
| Probe global budget | The database accepted 60 distinct claims in well under a second. The SQL clamp of 60/hour, and the configured default of 20/hour, are politeness limits. This run is not a reason to raise them. |
| Per-host budget | 4/hour behaved as written. Keep it. |
| Cooldown | Keep a long minimum interval. Know that an expired lease is still rejected until that interval passes. |
| Retention follow-ups | Selection is cheap. Completion was measured locally without a dial. Keep the retry cap that already fails a follow-up after 8 attempts. |
| Evidence retention | Attempts already age out at 7 days inside the claim function. Samples are bounded by the probe budget. Follow-up deletion still depends on the worker path that dials relays. |
| Monitoring | `last_duration_ms`, reconcile cursor `idle` and `next_run_at`, probe claim rejections by reason (the SQL function returns only an empty token, so the caller has to know which guard it expected), derivation job lag, and WAL rate while projection is on. |

## Bottlenecks, in order

1. Empty projection reconcile when the cursor is not idle. `reconcileAuthoritative` nested-loops every relay-kind `replaceable_state` row. At 208,226 rows that was 532 ms of server time and 1.2 s of `BackfillBatch`, for zero candidates. The 15 minute idle already avoids repeating it. Resetting that cursor on the 5 second tick would put this scan on the hot path.

2. Full statistics refresh. 17.5 s at 1.10 million effective memberships, about 10× the 1.69 s at 111k. Acceptable at 10 minutes for this size. The output table stayed 410 KB. The next size up needs its own measurement before anyone calls the full refresh safe there, and before anyone builds incremental aggregation.

3. Replaceable purge of 100 queued events. 659 ms at the small fixture, 7.8 s at the medium fixture, same limit. Location: `PurgeSupersededReplaceableEvents`. The plan was not captured, so the cause is not isolated here. Do not tighten the purge interval until that statement has an `EXPLAIN (ANALYZE, BUFFERS)`.

4. Bulk `event_relays` insert. The first-seen trigger turned 71,000 rows into a 5 minute, one-core load. Ordinary single-event ingest in the mixed run was still hundreds of events per second. The pain is bulk loading, not the request path.

5. Dense personalized optimize. About 90–150 ms and 48 queries, independent of membership-table size between these two fixtures. Caching recommendations absorbs the repeat. Optimize itself is uncached. The cohort cap of 400 is what this latency corresponds to.

Derivation with projection on was slower by about a fifth and wrote more WAL. It still finished 200 events, left no pending jobs, and did not lock or exhaust the pool. That is not a reason to disable projection.

## What was not measured

- 500,000 accounts and several million memberships.
- A flushed PostgreSQL cache. Plans show shared hits for the point queries. The medium mixed run did read 1.2–1.7 million blocks because the working set is larger than `shared_buffers`.
- Meilisearch HTTP indexing.
- Live relay dials. Follow-up completion used `FinishProbeLease` on a local row.
- More than two mixed repeats, or a mixed run longer than a few hundred events. Ingest differences at 100k accounts were inside that noise. The derivation drop was not.
- `EXPLAIN` for the replaceable purge and for `claim_relay_probe` writes. Claim latency and WAL were measured. The purge plan was not.
- A second hardware or PostgreSQL configuration. These numbers are one M4 Pro, Postgres 18.6, 128MB `shared_buffers`.

## Earlier simplified fixture

`TestPopulatedRelayIntelCost` previously measured two public memberships per account and no mixed workload. On the same class of machine and the same Postgres settings it reported repair medians of 11.1 ms and 13.8 ms, head lookups of 185 µs and 229 µs, and statistics refreshes of 263 ms and 3.4 s at 20,000 and 200,000 memberships. Those runs did not measure WAL, locks, probes that actually obtain a lease, or ingest. Use the tables above for the populated fixture. The older test is still in `internal/relayintel/bench_populated_test.go`.
