# WS + API load-harness baseline

`main-latest/summary.json` is the seed for the continuous WS+API load-harness
trend line. It is **machine-generated** — do not edit by hand.

- The `loadtest-baseline` job in [../../.github/workflows/perf.yml](../../.github/workflows/perf.yml)
  seeds Postgres via `INGESTOR_MODE=replay`, runs
  [the harness](../harness), and uploads a `loadtest-baseline` artifact on every
  push to `main`. The artifact holds the new `summary.json` and the appended
  `history.ndjson`. The job does not push to `main`.
- The `loadtest-compare` job runs the identical harness on pull requests and
  compares against the latest artifact, falling back to this committed file
  when no artifact exists yet, via [`scripts/loadtest_compare.sh`](../../scripts/loadtest_compare.sh).

Gating is **error-rate only** (5xx + transport regressions): a channel fails the
PR when its error rate is both above an absolute floor (1%) and more than doubles
versus the baseline. Latency p95/p99 deltas are reported as advisory only, to
avoid runner-variance flakes.

Until the first push to `main` publishes the artifact, compare uses this
committed file. If that file is missing too, the compare job stays advisory.
