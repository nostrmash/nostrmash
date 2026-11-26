# Relay intelligence

NostrMash can inspect a public account's relay configuration, rank relays by role, and propose a complementary set. The API returns unsigned events. It does not sign them, publish them, or ask for a private key.

This is separate from relay admission. A relay can be a poor fit for NostrMash's own index and still be a good recommendation for DMs, search, or a user's neighborhood. Admission is not changed by popularity in a specialized role.

## Roles

| Role | Kind | What it means |
|---|---|---|
| `public_inbox` | 10002 `r` with `read`, and unmarked `r` | Where the account expects mentions. Not the home-feed download list. |
| `public_outbox` | 10002 `r` with `write`, and unmarked `r` | Where the account publishes. An unmarked relay is both inbox and outbox. |
| `dm_inbox` | 10050 | Public DM inbox relays. |
| `search` | 10007 | Search relays. |
| `blocked` | 10006 | Relays the account does not want used. Not an adoption signal. |
| `favorite` | 10012 | Favorite relay feeds. The optimizer does not rewrite them by default. |
| `relay_set` | 30002 | One addressable set per `d` tag. Not rewritten by the optimizer. |
| `keypackage` | 10002 write relays, or historical 10051 | See Marmot below. |
| `private_storage` | 10013 | Client-side only. URLs are not read. |
| `blossom` | 10063 | HTTP media servers. Not inserted into `relay_registry`. |
| `wiki` | 10102 | Wiki relays. Parsed and ranked. Not in the default optimization. |

Kind 3 is a read-only fallback. It is used for inbox and outbox only when no kind 10002 head exists. An explicit empty kind 10002 stays empty. A non-object or blank kind 3 content is unavailable, not an empty modern list. `p`-tag relay hints are not preferences. Optimization never emits kind 3.

There is no invented kind for broadcast relays, proxy relays, or client-local settings.

## Public, private, and client-side

Public replaceable lists are projected and can appear in role statistics when the URL is a public address. A private or loopback URL in a public event is stored on that account with `public_safe = false`. It is returned by the per-account config endpoint, because the account already published it, and it is excluded from rankings.

Kind 10013 is stored as a head with `client_side_only` and zero URLs. The ciphertext is not decrypted. Encrypted portions of other NIP-51 events are not decrypted. There is no NIP-44 implementation in this repository.

`POST /api/v1/relays/optimize` accepts caller-supplied URLs for the duration of that request. Those URLs are not written to shared tables, not put in the process cache, and not written to access logs. Optimize responses are not cached. Private and metadata hosts in a request are rejected with `private_rejected` unless `RELAY_INTEL_ALLOW_PRIVATE_NETWORK` is set. Link-local addresses and cloud metadata names are rejected even then.

## Projections

`relay_lists_latest` and `dm_relay_lists_latest` are unchanged. They still store URL arrays without read/write markers. Role rows live in:

- `relay_config_heads`, one winning event per `(pubkey, source_kind, d_tag)`
- `relay_role_memberships`, one row per role and normalized URL

The winner is the newer `created_at`. A timestamp tie keeps the lexicographically lower event id, which is the NIP-01 rule. Replaying that same id refreshes memberships. `enqueue_superseded_replaceable` deletes the higher id on a tie. Responses include `tie_break: lower_event_id`. Events that were deleted under the previous higher-id rule cannot be reconstructed. `repair_replaceable_tie(limit)` promotes a lower id that is still stored, removes it from the purge queue, and enqueues `derive_event_bundle` with idempotency key `relay-intel-repair:<id>`. The worker calls it in batches of at most 200 while backfill is enabled. It does not run during migration. Retention also refuses to delete an event that would win the same-timestamp tie, and a pending `relay_intel_repairs` row.

A winning event replaces the previous memberships for that address in one transaction. An empty modern list deletes the memberships and keeps an explicitly empty head. A kind 5 `e` tag deletes memberships for that event id when the deleter is the author. A kind 5 `a` tag deletes the current address. Deleting a superseded loser does not remove the head, because the head points at the winner. Foreign keys cascade when the winning event row itself is deleted.

URL normalization uses the existing `relayurl` rules: `ws` or `wss`, no query, userinfo, or fragment, and a non-root path is preserved. Lists longer than 32 endpoints are truncated in the projection. The raw event is not rewritten.

Derivation versions were not bumped. New events are projected as they arrive. Events already stored are projected by the worker backfill in `relay_intel_backfill_cursor`, 200 rows every 5 seconds by default, ordered by `created_at, id`. Kinds 10013, 10051, and 10102 were added to the live ingest filter and the untrusted-author purge list. Historical rows of those kinds are empty until an event is stored. The backfill does not crawl relays.

If migration `000133` is missing, projection logs a disabled outcome and returns success so derivation jobs are not retried forever. The API returns 503 `schema_not_ready`. Apply the migration before expecting role rows.

## Discovery and statistics

`GET /api/v1/relays/popular` is unchanged. It still counts distinct users across `relay_lists_latest` and `dm_relay_lists_latest` together.

Role statistics are rebuilt by the worker into `relay_role_stats`. A pubkey is counted once per role. `relay_endpoint_adoption` counts a pubkey once across positive roles, so a relay used as both inbox and outbox is not two users. Blocked-relay references are excluded. The synthetic probe pubkey is excluded. Trust weight is the stored global trust score clamped to 0..1. Active users posted at least once in 30 days. Modern and legacy counts are separate. `net_user_delta_7d` compares the current count with the daily snapshot from seven days earlier, for the top 200 relays per role, retained 30 days.

Declared usage and observed propagation stay separate. `relay_propagation_daily` says events were seen on a relay. It does not prove the author published there on purpose.

## Capabilities

Evidence classes are `advertised`, `observed`, `verified`, `unsupported`, `unknown`, `stale`, and `restricted`.

The existing read probe is `read_subscription` only. EOSE does not verify search, write, gift wrap, or retention. NIP-11 support for NIP-17, NIP-42, or NIP-50 stays `advertised` at low confidence. `auth_required` is `restricted`, and it is not specific to reading or writing. An authentication requirement does not make a relay unsuitable for DMs. Advertising NIP-17 does not make it privacy-preserving.

`GET /api/v1/relays/capabilities` reads stored rows. It does not dial.

Active probes are off unless `RELAY_INTEL_ACTIVE_PROBES=true`. The worker then requires `RELAY_INTEL_PROBE_SECRET_KEY`, a 32-byte hex secp256k1 secret that the API process does not load. Probes use that synthetic identity, never a user key. The default budget is 20 attempts per hour across all workers, at most four attempts per host per hour, and one attempt per relay and capability per 24 hours. `claim_relay_probe` records every attempt and returns a lease token. Finishing requires that token, so a worker whose lease was replaced cannot overwrite the newer one. A restart does not reset the hourly count. Candidates with an unexpired lease are skipped before the shortlist limit. Each stored observation also keeps the latest 20 samples. A later success does not erase earlier failures. Recent failure ratios use the latest 20 samples as the denominator, not the lifetime count. An inconclusive sample does not replace a verified, unsupported, or restricted status. A verified status is downgraded when failures are at least half of that recent window. A newer public gift-wrap retrieval overrides an older recipient-only result. Observation writes require the current lease token. The probe pubkey is stored in `relay_probe_identities` and omitted from role statistics, author-activity counters, and Meilisearch sync in this process. A relay that only advertises NIP-17 is not a private-message recommendation. `GET /api/v1/relays/capabilities` reports each capability's status, confidence, sample count, and recent failure count. Nostr Watchdog can treat `giftwrap` status `verified` together with a separate recipient-access observation as a candidate, and should ignore `advertised`. `RELAY_INTEL_GIFTWRAP_PROBES` publishes one synthetic kind 1059 gift wrap, using NIP-44 from `github.com/nbd-wtf/go-nostr/nip44`, and only to hosts in `RELAY_INTEL_PROBE_ALLOWLIST`. The sender, recipient, and observer are separate synthetic keys. `giftwrap` becomes verified only when the recipient retrieves the event and a completed observer query does not. An incomplete observer query stays inconclusive. Kind 1 write success does not verify Marmot KeyPackage transport. No maintained Go fixture for a synthetic identity's kind 30443 KeyPackage is bundled, so that capability stays unknown until one is supplied. Candidates with no stored observation are ordered ahead of relays that were probed recently. A kind 1 note with content `nostrmash relay intelligence probe` is published and then read back. Acceptance and retrieval are stored separately. A search probe publishes the synthetic event, requires an `OK` for that event id, then sends a matching query and a query that should not return it. An empty result stays inconclusive. A relay that returns the event for the non-matching query is not marked verified. A negative `OK` or a `NOTICE` is not stored as permanent incompatibility. A `CLOSED` frame that says the feature is unsupported is. Connect failures are left to the existing read prober. Gift-wrap probes run on alternate maintenance turns so they do not occupy every probe slot. Recipient and observer checks use both an event-id filter and a kind 1059 `#p` filter. An unauthorized hit counts even when EOSE does not follow. An authentication `OK` is not treated as publication of the original event.

No claim is made about mobile push delivery.

## Scoring

`relay-score-v1` starts from the brief's weights with the 5% diversity share removed and the rest renormalized. Diversity is applied once, to the set.

| Component | Inbox | Outbox | DM | Search |
|---|---:|---:|---:|---:|
| Personalized | 0.368 | 0.263 | 0.105 | 0.053 |
| Reliability | 0.263 | 0.211 | 0.316 | 0.263 |
| Latency | 0.105 | 0.053 | 0.105 | 0.211 |
| Reach | 0.158 | 0.316 | 0 | 0.158 |
| Privacy | 0 | 0 | 0.316 | 0 |
| Functionality | 0.105 | 0.158 | 0.158 | 0.316 |

A missing component is a neutral value with low confidence. The score is multiplied by `0.55 + 0.45 * confidence`. Below 0.25 confidence the numeric score is omitted, unless the relay is pinned. Search that was observed as unsupported, and DM gift-wrap that was observed as unsupported, are ineligible. A pin overrides eligibility and is reported as `pinned_ineligible`. `free_only` rejects a relay that advertises payment unless the caller marked it `paid_access` or pinned it. `auth_read` and `auth_write` accept `allow`, `prefer_without`, `require`, and `reject`. Unknown authentication stays eligible. `require` rejects a document that explicitly says authentication is not required.

Inbox coverage looks at the cohort's outbox declarations. Outbox coverage looks at the cohort's inbox declarations. The denominator is members who have a stored declaration for that role. Everyone else is `unknown_weight`. Missing data is not treated as unreachable. Coverage is declared-preference overlap, not delivery.

## Cohort

The neighborhood uses stored follows (weight 1), followers (0.55), mutual follows (+0.45), log-scaled public interactions, second-degree follows of the top trusted follows (0.15), and the global trust score. Active accounts in the last 30 days are scaled by 1.15, others by 0.75 when activity data exists. If trust data exists and an account's score is 0, its weight is multiplied by 0.35. After normalization, a cohort of 13 or more members is capped so one account holds at most 8%. Smaller cohorts keep relative weights, because an 8% cap cannot sum to 1. The cohort is limited to 500 members. Query limits are 400 follows, 200 followers, 100 interactions each direction, and 500 second-degree rows. Encrypted events are not used to infer DM relationships.

If the neighborhood is empty, scoring falls back to trust-weighted global adoption and the response says `global_fallback`.

## Optimizer

`relay-opt-v3` uses one set objective for the initial selection, a one-relay swap, and removal. The objective blends coverage and quality with the preset bias, then subtracts an operator-concentration penalty. A swap replaces one removable relay when that set objective improves by more than the preset hysteresis divided by the current list size. A trivial quality change stays put. A pin is not swapped out.

Pins are kept even when they are ineligible or they exceed the maximum, and the conflict stays visible. An account blocklist is not overridden by a pin. Preferred relays are added when they fit and are eligible. Excluded relays are dropped unless pinned, which yields `pinned_excluded`. Uncertain health does not put an excluded relay back into the list. A minimum larger than the maximum leaves that role unchanged with `min_exceeds_max`. Unmet minimums, ineligible preferred relays, and preferred relays that do not fit are returned in `unsatisfied_constraints`. They are not silently dropped.

For inbox and outbox the objective is unique weighted coverage, blended with a small quality term from the preset. DM, search, and keypackage use a quality objective. Blossom's objective is `retain`: current servers stay, and a popularity add is returned as `investigate` with `blossom_popularity_not_sufficient`. Blocklists and relay sets are not optimized. Kind 10013 is never rewritten.

`retain_existing` keeps current relays that are not hard-ineligible, then fills remaining slots. Excluded, blocked, unsupported, and payment-rejected relays are not kept. After the minimum size is met, an additional relay is skipped when its marginal gain is below the preset hysteresis. A full list can still replace one relay when a swap clears that hysteresis. A single failed probe does not remove a relay. Uncertain health becomes `investigate`, and that relay stays in the unsigned event.

Diversity uses the NIP-11 pubkey when one is stored, otherwise `host:<hostname>`. It scales marginal gain when the operator key is already selected. It is not an ownership claim and it is not an ASN lookup. No ownership is invented.

Default sizes, which callers can override:

| Role | Min | Max |
|---|---:|---:|
| Public inbox | 2 | 4 |
| Public outbox | 2 | 4 |
| DM inbox | 1 | 3 |
| Search | 1 | 3 |

Presets are `balanced` (default), `creator`, `privacy`, `minimal`, `decentralization`, and `custom`. Privacy does not claim anonymity. It tightens counts and raises the quality bias toward reliability and privacy components. Minimal sets the maximum equal to the minimum. Custom uses caller weights when they are present. An unknown preset name is HTTP 400.

Publishable proposals are patches of a complete signed source event. Content, including encrypted NIP-51 entries, is copied and never decrypted. Unknown tags and unknown `r` markers are kept. Optimizing only the inbox leaves the outbox tags in place, and the reverse. Blossom server order is kept for servers that stay. Kind 3 is never emitted. An unchanged list produces no event.

`proposal_safe` is false when the source used for that request is missing, deleted, truncated, or not authoritative. After a patch, the replacement is parsed again with the same rules. If its relays do not match the selection, the event is omitted and `proposal_mismatch` is returned. `ready_to_sign` is true only when the patch is safe, the source is complete, constraints are satisfied, and the result is neither a creation nor a review hold. A warning, including a KeyPackage republication note, does not by itself make the patch unsafe. Each unsigned event also has its own `proposal_safe`, `source_origin`, and `change` (`replacement`, `creation`, or `recreation`). One safe event can sit next to a list that still needs a refresh. The response can still contain recommendations when the replacement is omitted.

Optional candidates are ordered by the cohort weight that declares them, not by an unweighted head count. Current endpoints, pins, and supplied relays are always kept. Up to a quarter of the remaining shortlist, and at most 20 slots, stays available for relays that appear in the global role statistics, so a large cohort does not crowd out every well-known relay. The role response sets `candidates_truncated` when the shortlist limit of 80 drops something. Scoring still happens only on that shortlist.

The optimizer and the patch use the same event. Indexed and caller-supplied events for one coordinate are ordered by NIP-01: newer `created_at` wins, and an equal timestamp keeps the lower event id. A valid signature does not make an older event win. A supplied event that loses is reported as `stale_source` and is not written over the newer indexed event. `source_events` are not stored, cached, or logged. Optimize and verify responses send `Cache-Control: no-store`.

Local absence is not a confirmed empty list. `allow_create: true` mints a first list only when this index has no source and no deletion tombstone. A known deletion stays deleted unless `allow_recreate: true`, which returns a recreation that still requires review. The client must still look for an existing event before signing. Heads, memberships, raw events, and deletion rows for one optimization are read in one repeatable-read transaction, which ends before scoring.

The client must read the latest source events again immediately before signing. A validated local event is not proof it is the newest on the network.

`based_on_event_id` and `based_on_created_at` identify the source that was patched. DM removal and keypackage replacement set `attention_required`. Historical DMs are not migrated. Modern KeyPackage discovery follows the NIP-65 write relays, so a public-outbox change can affect it. A kind 10051 replacement is emitted only when that legacy list is still the effective source.

The complementary-coverage case is covered by `TestSelectComplementarySetBeatsTopIndividuals`. Two high-scoring relays that cover the same people lose a slot to a lower-scoring relay that covers someone else.

## API

All routes are under `/api/v1` and are off when `RELAY_INTEL_ENABLED=false`, which returns 404 `feature_disabled`.

| Method | Path | Notes |
|---|---|---|
| GET | `/api/v1/relays/roles` | Static catalog |
| GET | `/api/v1/relays/rankings?role=&limit=` | Precomputed public rankings |
| GET | `/api/v1/relays/capabilities?relay_url=` | Stored evidence, no dial |
| GET | `/api/v1/profiles/{pubkey}/relay-config` | Effective public configuration |
| GET | `/api/v1/profiles/{pubkey}/relay-diagnostics` | Findings |
| GET | `/api/v1/profiles/{pubkey}/relay-recommendations` | Optional `role` and `preset` |
| POST | `/api/v1/relays/optimize` | Constraints and unsigned preview |
| POST | `/api/v1/relays/verify` | Read-back of a signed event |

Rankings, roles, and capabilities use the public-stats rate limit. The account and POST routes use the personalized limit. POST bodies are limited to 64KB and reject unknown JSON fields.

`POST /api/v1/relays/verify` does not publish. `sample_verified` is true only when the signature is valid, at least two relays returned the event, each of those relays finished with a matching EOSE, and for a replaceable event that same event is the latest valid version under NIP-01. Exact retrieval without a completed latest-version query is not agreement. A missing or malformed EOSE, a wrong subscription id, or a message budget exhausted before EOSE is incomplete. `global_success` is the old name for that sample and is not network-wide propagation. One relay is `insufficient_relay_sample`. A different latest event is `conflict`. Invalid signatures return 200 with `signature_valid: false` and do not dial. Addressable requests include the `#d` filter, including an empty `d`. Dialing uses the IP that passed the safety check, ignores `HTTP_PROXY`, and rejects redirects by not following a second host. Metadata and link-local targets are rejected.

Public GET responses are cached in process for 30 seconds (rankings and capabilities for 2 minutes), with a 192-entry cap and singleflight. Optimize is never cached.

## Security

Relay URLs, NIP-11 documents, event bodies, and API parameters are untrusted. Dial targets go through `HostDialable` and `DialPinned`. Private networks require an explicit flag. The public deployment is not an open proxy. Recommendation requests do not fan out to relays. Probe and verify budgets are capped. Cohort queries are limited. Cache keys for public GETs do not include caller-supplied private configuration, because those requests are not cached.

## Frontend flow

The future `/relays/manager` screen can use this API now. It should not offer one-click signing until `ready_to_sign` is true for that event, and it must still reload the newest source before asking for a signature. Gift-wrap, search, and write probes are a separate workstream. Until those observations exist, DM and search rows should say the capability is unverified.

1. Connect with NIP-07 or NIP-46 in the browser. Send the pubkey only. Do not send a private key.
2. `GET /api/v1/profiles/{pubkey}/relay-config` shows the indexed lists. `relay-diagnostics` shows health findings. Neither is a network-wide proof that the account has no other list.
3. `GET /api/v1/profiles/{pubkey}/relay-recommendations` or `POST /api/v1/relays/optimize` with pins, exclusions, paid relays, and auth preferences. Pass the newest signed events the client already has in `source_events`.
4. Show the current list and the proposed list. `shared_configuration` means the public outbox and KeyPackage discovery are one kind 10002 event. A warning there means a messaging client must publish a new KeyPackage after the outbox changes. Do not treat every kind 10002 as proof the account uses Marmot.
5. `allow_create` asks for a first list when this index has no source. `allow_recreate` is the only switch that rebuilds a list with a deletion tombstone. Both stay in review. `ready_to_sign` is the only flag that means this response's replacement matched the selection and the constraints. A warning does not clear it, and a creation never sets it.
6. If `requires_refresh` is true, or `based_on_event_id` does not match the event the client just fetched, stop. Fetch the latest event from relays and call optimize again.
7. Ask the signer to sign. Publish the signed event from the client. This server does not sign or publish.
8. `POST /api/v1/relays/verify` checks a sample. `sample_verified` is the success field. `global_success` is not network-wide propagation. A partial publish stays partial until the missing relays agree or the user stops.
9. Restoration is another signed event from the user. There is no server-side rollback of a published list.

## Operations

Environment variables are generated into [configuration.md](../configuration.md) under `RELAY_INTEL_*`. Defaults: enabled, private networks off, stats every 10 minutes, backfill 200 events every 5 seconds, active probes off, gift-wrap probes off.

Migration `000133_relay_role_intelligence.sql` is additive. `000135_relay_intel_deletion_durability.sql` drops the foreign key from `deletion_addresses` to `events` so a purged kind-5 event does not remove the tombstone, fixes empty `d` so it is one coordinate rather than a wildcard, and records both NIP-65 roles as known when a kind 10002 head exists. `repair_replaceable_tie(limit)` promotes a still-stored lower event id. It is resumable, capped at 200 rows per call, and is not run during deployment. Replay each promoted event to refresh projections. Events already deleted under the old higher-id rule cannot be rebuilt from SQL. `backfill_deletion_addresses(limit)` copies address tags from kind-5 events that are still stored. It cannot recover deletions that were purged before the ledger existed. Rollback of the feature is dropping the new tables and removing kinds 10013, 10051, and 10102 from the purge cursor and `enqueue_superseded_replaceable` if those kinds must leave the retention function. Do not drop `relay_lists_latest`. The old popular API does not read the new tables. Purge of superseded replaceable events skips an id that is the current `replaceable_state` winner and removes that stale queue row.

`000136_relay_intel_favorite_deletion.sql` adds kind 10012 to `backfill_deletion_addresses`. `000137_relay_intel_repair_membership.sql` drops memberships that still point at the losing event. `000140_relay_intel_repair_jobs.sql` records each repair. `000141_relay_intel_repair_lifecycle.sql` gives those rows the states `pending`, `queued`, `running`, `completed`, `superseded`, `deleted`, and `failed`. A repair completes when `replaceable_state` points at the lower-id winner and that winner's `derive_event_bundle` job has succeeded. It does not wait for an intentionally truncated projection to become complete. Retries reuse the job `derive_event_bundle:<event id>` and stop after the job's attempt limit. A newer canonical event marks the repair `superseded`. A deletion tombstone marks it `deleted` and does not leave the deleted event authoritative. Ordinary replaceable kinds ignore stray `d` tags. Addressable kinds, including an empty `d` tag, stay separate coordinates. Kind numbers that do not fit in a 32-bit integer are skipped by the deletion backfill instead of aborting the batch.

The worker calls `repair_replaceable_tie` and `backfill_deletion_addresses` about every 30 seconds even when relay projection, statistics, or the whole relay-intelligence API is off. Each function keeps a cursor in `relay_intel_work_cursor`. After a full pass it sleeps for an hour instead of scanning `replaceable_state` on every tick. To run a pass immediately: `UPDATE relay_intel_work_cursor SET idle = false, next_run_at = now() WHERE work_name IN ('tie_repair', 'deletion_address_backfill');` then `SELECT repair_replaceable_tie(200);` until it returns 0 and `SELECT count(*) FROM relay_intel_repairs WHERE status IN ('pending', 'queued', 'running');` is 0. Events purged before the corrected tie-break cannot be reconstructed. Retention will not delete an event that is the current winner, an unreconciled lower-id winner, or the winner of an active repair whose coordinate still points at that winner or the event it replaced. Completed, superseded, deleted, and failed repairs do not pin old events.

Projection reconcile is separate. It reads only relay-list kinds and only heads that are missing, point at a different event, or have `needs_replay`. A truncated head whose event id already matches canonical state is left alone. Disabling backfill does not disable tie repair. Re-enabling projection clears the reconcile idle flag at process start so missing heads are filled in. Statistics refreshes take one advisory lock, so two workers do not rebuild `relay_role_stats` together. `relay_intel_snapshot.last_duration_ms`, `last_error`, and `last_failure_at` record the last attempt. `computed_at` stays on the last success. Measured costs on synthetic data are in [relay-intelligence-benchmarks.md](relay-intelligence-benchmarks.md).

Configuration inspection, diagnostics, recommendations, and optimize all read canonical replaceable state in one repeatable-read snapshot. `sources[].status` is `current`, `stale`, `missing`, or `deleted`. A caller's `paid_access` list means the user can use that relay. It does not mean the relay requires payment. `diversity` is a soft preference: concentration is a warning when another eligible operator exists, and it does not block signing. `require_operator_diversity` is mandatory. The selected list must contain more than one known operator (`operator_key` from NIP-11, not the domain). One operator, a single endpoint, unknown operator ownership, or a pin that forces one operator leaves the requirement unsatisfied and `ready_to_sign` false, including when no eligible alternative exists. The response says so. A KeyPackage confidence minimum is checked while candidates are chosen and again on the final list. It is satisfied only by verified `keypackage` evidence at that confidence. Public kind 1 writes, advertisements, unsupported results, and unknown evidence do not meet it. When KeyPackage discovery follows the public outbox, that requirement stays on the shared kind 10002 selection and is not converted into a public-write threshold.

`RELAY_INTEL_ENABLED=false` stops the API, role-row projection, statistics, and projection backfill. `RELAY_INTEL_PROJECTION_ENABLED=false` stops role-row projection while leaving the API on. `RELAY_INTEL_BACKFILL_ENABLED=false` stops projection backfill only. None of these stop canonical replaceable-state updates, historical tie repair, or deletion-address backfill. Rows already written remain until deleted. Backfill and reconcile read current `replaceable_state` winners rather than every historical event of the kind. An event that is not the stored winner is not projected by that walk.

Metrics, all with bounded labels:

- `nostrmash_relay_intel_requests_total{endpoint,outcome}`
- `nostrmash_relay_intel_request_duration_seconds{endpoint}`
- `nostrmash_relay_intel_cache_lookups_total{result}`
- `nostrmash_relay_intel_projection_events_total{kind,outcome}`
- `nostrmash_relay_intel_declarers{role}`
- `nostrmash_relay_intel_stats_age_seconds`
- `nostrmash_relay_intel_backfill_events_total{outcome}`
- `nostrmash_relay_intel_backfill_done{kind}`
- `nostrmash_relay_intel_capability_observations_total{capability,status}`
- `nostrmash_relay_intel_probe_total{class,outcome}`
- `nostrmash_relay_intel_fallbacks_total{reason}`

## What this does not establish

- Production query latency and table growth were not measured against a production database. The in-memory optimizer benchmark on an Apple M4 Pro, 500 cohort members and 40 candidate relays, is about 3.1 ms/op. On the same machine, Postgres 18 in Docker with an empty migrated schema, `BenchmarkRepairAndClaimOnEmptySchema` measured `repair_replaceable_tie(200)` plus one `claim_relay_probe` at about 0.28 ms/op. That is empty-schema overhead, not a populated workload, and it does not include ingestion, statistics refresh, or API latency.
- Gift-wrap acceptance is not private-message proof. `giftwrap` is verified only after the synthetic recipient retrieves the event and completed unauthorized queries do not. An unauthorized hit counts even without EOSE. An incomplete query with no event stays inconclusive. The probe is off unless `RELAY_INTEL_GIFTWRAP_PROBES=true`, active probes are on, and the relay host is in `RELAY_INTEL_PROBE_ALLOWLIST`. NIP-44 comes from `github.com/nbd-wtf/go-nostr/nip44` (MIT). This does not claim anonymity, guaranteed delivery, or mobile push support. Kind 30443 KeyPackage transport stays `unknown`: this repo does not bundle an MLS KeyPackage for the synthetic identity, and a kind 1 write is not a substitute. `verified` is an observation. `advertised` is only a NIP-11 statement. A status older than seven days is `stale`.
- A kind 10002 that contains only write relays is a known empty read list. Cohort scoring treats a neighborhood with no relevant declarations as unknown, not as a precise personalized score.
- Kind 10051 is historical. Current Marmot clients look for KeyPackages on NIP-65 write relays when a kind 10002 exists, including an empty one.
- Coverage is overlap of stored declarations, not a delivery guarantee.
- Propagation counts are not proof of intentional publication.
- A temporary outage does not produce a removal.
- Active probes publish synthetic events only to hostnames in `RELAY_INTEL_PROBE_ALLOWLIST`. They are off by default. The synthetic sender, recipient, and observer pubkeys are excluded from role stats, author counters, and Meilisearch sync in this process.
- Kinds 10013, 10051, and 10102 have no historical rows until events arrive or the backfill sees stored events. They were not previously ingested.
- Wiki relays are parsed and can be ranked. They are not in the default optimization set.
- Favorite lists and custom relay sets are visible and are not rewritten.
- Blocklists are not inferred from NostrMash policy.
- Blossom servers are not replaced from popularity.
- NostrMash does not admit a relay into its ingest set because a recommendation likes it.
