# Relay intelligence measurements

These numbers come from `TestPopulatedRelayIntelCost` on 26 November 2026. They are not an empty-schema benchmark. A 500,000-account run was not executed.

## How to reproduce

Postgres 18 from `docker compose up -d postgres`, then:

```sh
export TEST_DATABASE_URL=postgres://nostrmash:nostrmash@localhost:5432/nostrmash?sslmode=disable
RELAY_INTEL_POPULATED_BENCH=1 RELAY_INTEL_POPULATED_ACCOUNTS=10000 \
  go test ./internal/relayintel/ -run TestPopulatedRelayIntelCost -v -count=1
```

`RELAY_INTEL_POPULATED_ACCOUNTS` defaults to 10000. The test creates its own schema and drops it. It does not run in CI.

The fixture is synthetic. Each account has one kind 10002 event, one kind 0 event, a relay head (1% marked truncated), and inbox plus outbox memberships spread across 80 hosts. The first 200 kind 10002 rows are same-timestamp ties whose canonical pointer starts on the higher event id. It is not a copy of production traffic.

## Machine

- Apple M4 Pro, 14 cores, 24 GB RAM
- PostgreSQL 18.6 in Docker Desktop, `shared_buffers=128MB`, `work_mem=4MB`
- The database had no competing application load besides this test

## 10,000 accounts

About 10,000 kind 10002 heads, 10,000 kind 0 rows, and 20,000 memberships. Seed time 687 ms.

| Operation | Result |
|---|---|
| `repair_replaceable_tie(200)`, 20 calls | median 11.1 ms, p95 11.8 ms, max 37.4 ms. The first call fixed 200 ties. |
| Canonical head read, 100 calls | median 185 µs, p95 233 µs |
| Statistics refresh | 263 ms (`last_duration_ms=260`) |

## 100,000 accounts

About 100,000 kind 10002 heads, 100,000 kind 0 rows, and 200,000 memberships. Seed time 6.9 s. Measured after reconcile stopped joining `events` for rows that do not need a rewrite.

| Operation | Result |
|---|---|
| `repair_replaceable_tie(200)`, 20 calls | median 13.8 ms, p95 37.6 ms, max 51.7 ms. The first call fixed 200 ties. Later calls are keyset pages, not a repeat of the same 200. |
| One `BackfillBatch` after 100 heads were marked `needs_replay` | 746 ms. This includes the kind backfill walk, not only the stale page. |
| Empty reconcile candidate query | 175 ms, 100,000 relay rows, 0 candidates. Plan below. |
| Canonical head read, 100 calls | median 229 µs, p95 286 µs, max 595 µs |
| Statistics refresh | 3.4 s (`last_duration_ms=3381`) |

Empty reconcile plan, after the event join was removed:

```text
Limit  (actual time=174.518..174.518 rows=0)
  -> Nested Loop Left Join
       Filter: head missing, different event id, or needs_replay
       Rows Removed by Filter: 100000
       -> Index Scan using idx_replaceable_state_kind_pubkey
            Index Cond: kind = ANY (relay list kinds)
            actual time=0.013..7.982 rows=100000
       -> Index Scan using relay_config_heads_pkey
            actual time=0.002..0.002 rows=1 loops=100000
Execution Time: 174.537 ms
```

A single head lookup is an index scan of `relay_config_heads_pkey`, execution about 0.02 ms inside the server. The 229 µs client figure includes the round trip.

## What this says about production defaults

Tie repair stays on a cursor. A 200-row page is about 14 ms here even with 200,000 replaceable rows in the table, because the page does not scan the whole table. After a full pass the cursor sleeps for an hour. That is the right default. Do not call it on the 5 second projection interval.

An empty reconcile pass still touches every relay-kind head. At 100,000 heads that was 175 ms once, not every 5 seconds. The worker marks the cursor idle for 15 minutes after a short page. Keep that. Do not join `events` until a row is actually going to be projected.

A full statistics rebuild of 200,000 memberships took 3.4 seconds and held one advisory lock. That is acceptable on a 10 minute interval for this size. Incremental aggregation is not justified by this measurement. Revisit it if a refresh on the real membership table exceeds a few tens of seconds or overlaps ingestion badly. Two workers must keep using the advisory lock so they do not start that rebuild together.

500,000 accounts and several million memberships were not measured. Do not treat these results as that size. WAL, lock waits, and a mixed ingest workload were not measured either.
