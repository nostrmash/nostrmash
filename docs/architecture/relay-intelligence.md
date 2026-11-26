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

The winner is the newer `created_at`. A timestamp tie keeps the lexicographically lower event id, which is the NIP-01 rule. Replaying that same id refreshes memberships. `enqueue_superseded_replaceable` deletes the higher id on a tie. Responses include `tie_break: lower_event_id`. Events that were deleted under the previous higher-id rule cannot be reconstructed. `repair_replaceable_tie(limit)` can promote a lower id that is still stored; it does not run during migration and does not by itself refresh every projection. Replay the promoted event to refresh derived rows.

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

Active probes are off unless `RELAY_INTEL_ACTIVE_PROBES=true`. The worker then requires `RELAY_INTEL_PROBE_SECRET_KEY`, a 32-byte hex secp256k1 secret that the API process does not load. Probes use that synthetic identity, never a user key. The default budget is 20 relays per hour globally and one attempt per relay per 24 hours. A kind 1 note with content `nostrmash relay intelligence probe` is published and then read back. Acceptance and retrieval are stored separately. A search probe that only reaches EOSE is not verification. A `CLOSED` or `NOTICE` response is `unsupported`. Connect failures are left to the existing read prober. Gift-wrap probes are not implemented: the flag increments `nostrmash_relay_intel_probe_total{class="giftwrap",outcome="disabled"}` and logs an error. This repository cannot build a NIP-44 gift wrap.

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

1. `GET /api/v1/profiles/{pubkey}/relay-config` and `relay-diagnostics`.
2. `GET /api/v1/profiles/{pubkey}/relay-recommendations` or `POST /api/v1/relays/optimize` with the user's pins, excludes, paid relays, and auth preferences.
3. Show the unsigned event, the diff, reason codes, coverage, unknown weight, and `attention_required` items. Do not hide partial constraint failures.
4. If `based_on_event_id` no longer matches the client's latest event, stop and reload.
5. Ask the signer (NIP-07 or NIP-46) for a signature. NostrMash never sees the secret.
6. The client publishes the signed event to the relays it chooses.
7. `POST /api/v1/relays/verify` with the signed event and at least two relays. Treat `partial`, `conflict`, and `insufficient_relay_sample` as incomplete.

## Operations

Environment variables are generated into [configuration.md](../configuration.md) under `RELAY_INTEL_*`. Defaults: enabled, private networks off, stats every 10 minutes, backfill 200 events every 5 seconds, active probes off, gift-wrap probes off.

Migration `000133_relay_role_intelligence.sql` is additive. `000135_relay_intel_deletion_durability.sql` drops the foreign key from `deletion_addresses` to `events` so a purged kind-5 event does not remove the tombstone, fixes empty `d` so it is one coordinate rather than a wildcard, and records both NIP-65 roles as known when a kind 10002 head exists. `repair_replaceable_tie(limit)` promotes a still-stored lower event id. It is resumable, capped at 200 rows per call, and is not run during deployment. Replay each promoted event to refresh projections. Events already deleted under the old higher-id rule cannot be rebuilt from SQL. `backfill_deletion_addresses(limit)` copies address tags from kind-5 events that are still stored. It cannot recover deletions that were purged before the ledger existed. Rollback of the feature is dropping the new tables and removing kinds 10013, 10051, and 10102 from the purge cursor and `enqueue_superseded_replaceable` if those kinds must leave the retention function. Do not drop `relay_lists_latest`. The old popular API does not read the new tables. Purge of superseded replaceable events skips an id that is the current `replaceable_state` winner and removes that stale queue row.

`000136_relay_intel_favorite_deletion.sql` adds kind 10012 to `backfill_deletion_addresses` and points a matching projection head at the repaired replaceable winner. It does not rewrite memberships and it does not run the repair during deployment. Replay each promoted event before using role statistics for that coordinate.

`RELAY_INTEL_ENABLED=false` stops the API and the maintenance loop. It does not stop canonical replaceable-state updates, and kinds already in the ingest filter are still projected. Rows already written remain until deleted.

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

- Production query latency and table growth were not measured against a production database. The in-memory optimizer benchmark on an Apple M4 Pro, 500 cohort members and 40 candidate relays, is about 3.1 ms/op. SQL, cache, and pool time are extra.
- Gift-wrap privacy cannot be verified here. `RELAY_INTEL_GIFTWRAP_PROBES` still fails startup when set. Advertisement is not proof. Active probe budgets are still process-local and are not a cross-worker lease.
- A kind 10002 that contains only write relays is a known empty read list. Cohort scoring treats a neighborhood with no relevant declarations as unknown, not as a precise personalized score.
- Kind 10051 is historical. Current Marmot clients look for KeyPackages on NIP-65 write relays when a kind 10002 exists, including an empty one.
- Coverage is overlap of stored declarations, not a delivery guarantee.
- Propagation counts are not proof of intentional publication.
- A temporary outage does not produce a removal.
- Active probes can publish a synthetic kind 1 to a relay when the flag is on. They are off by default. The synthetic pubkey is excluded from role stats. Kind 1 from an untrusted author is not indexed by the trust gate.
- Kinds 10013, 10051, and 10102 have no historical rows until events arrive or the backfill sees stored events. They were not previously ingested.
- Wiki relays are parsed and can be ranked. They are not in the default optimization set.
- Favorite lists and custom relay sets are visible and are not rewritten.
- Blocklists are not inferred from NostrMash policy.
- Blossom servers are not replaced from popularity.
- NostrMash does not admit a relay into its ingest set because a recommendation likes it.
