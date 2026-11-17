#!/usr/bin/env bash

# Advisory gate-recalibration reducer. Reads the append-only NDJSON histories
# produced by scripts/perf_history_append.sh (accumulated by the perf.yml
# baseline jobs) and prints suggested gate thresholds versus the currently
# configured ones. It NEVER edits any config — a human reads the recommendation
# and commits a new threshold if they agree.
#
# What it recommends:
#   - Load harness: an error-rate floor and factor derived from the observed
#     per-channel error-rate distribution, plus advisory p95/p99 run-to-run
#     volatility so operators can see whether the (deliberately advisory)
#     latency signal is stable enough to ever gate.
#   - Protected benchmarks: how often the history-band gate
#     (PROTECTED_HISTORY_FACTOR times the median of the previous
#     PROTECTED_HISTORY_WINDOW runs) would have failed a later run in this
#     file. The per-benchmark run-to-run percent is informational. The
#     pull-request job does not fail on PROTECTED_THRESHOLD_PCT.
#
# Load-harness statistics use run-to-run deltas because that gate fires on
# movement. The protected-benchmark gate fires on absolute level versus the
# recent median. mean + 3σ targets a ~0.1% false-positive rate on a normal-ish
# distribution; the script prints the sample size so a reader can discount
# recommendations built on too few runs.
#
# Usage:
#   scripts/gate_recalibrate.sh [loadtest_history.ndjson] [benchmark_history.ndjson]
#
# Env knobs (current gates, for the "vs configured" column):
#   LOADTEST_ERROR_RATE_FLOOR   default 0.01
#   LOADTEST_ERROR_RATE_FACTOR  default 2.0
#   PROTECTED_THRESHOLD_PCT     default 15
#   RECALIBRATE_MIN_SAMPLES     minimum deltas before a recommendation is firm
#                               (default 8; below this it is marked LOW-DATA)

set -euo pipefail

LOADTEST_HISTORY="${1:-loadtest/baseline/history.ndjson}"
BENCH_HISTORY="${2:-benchmarks/history/protection/history.ndjson}"

CUR_FLOOR="${LOADTEST_ERROR_RATE_FLOOR:-0.01}"
CUR_FACTOR="${LOADTEST_ERROR_RATE_FACTOR:-2.0}"
CUR_PCT="${PROTECTED_THRESHOLD_PCT:-15}"
MIN_SAMPLES="${RECALIBRATE_MIN_SAMPLES:-8}"
HISTORY_FACTOR="${PROTECTED_HISTORY_FACTOR:-1.75}"
HISTORY_WINDOW="${PROTECTED_HISTORY_WINDOW:-40}"

python3 - "${LOADTEST_HISTORY}" "${BENCH_HISTORY}" "${CUR_FLOOR}" "${CUR_FACTOR}" "${CUR_PCT}" "${MIN_SAMPLES}" "${HISTORY_FACTOR}" "${HISTORY_WINDOW}" <<'PY'
import json
import pathlib
import statistics
import sys

loadtest_path = pathlib.Path(sys.argv[1])
bench_path = pathlib.Path(sys.argv[2])
cur_floor = float(sys.argv[3])
cur_factor = float(sys.argv[4])
history_factor = float(sys.argv[7]) if len(sys.argv) > 7 else 1.75
history_window = int(sys.argv[8]) if len(sys.argv) > 8 else 40
cur_pct = float(sys.argv[5])
min_samples = int(sys.argv[6])


def read_ndjson(path):
    rows = []
    if not path.is_file():
        return rows
    for raw in path.read_text().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            rows.append(json.loads(raw))
        except ValueError:
            continue
    return rows


def mean_plus_3sigma(values):
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    mu = statistics.mean(values)
    sigma = statistics.pstdev(values)
    return mu + 3.0 * sigma


def low_data(n):
    return " (LOW-DATA)" if n < min_samples else ""


lines = []
lines.append("# Gate Recalibration (advisory)")
lines.append("")
lines.append(
    "Recommendations from the baseline histories. Nothing is changed "
    "automatically. The protected-benchmark gate is a slowdown factor against "
    "the recent median; load-harness error rate still uses a floor and a "
    f"factor. `mean + 3σ` describes noise; sample sizes below {min_samples} "
    "are flagged LOW-DATA."
)
lines.append("")

# ---- Load harness -------------------------------------------------------
load_rows = read_ndjson(loadtest_path)
lines.append(f"## Load harness — {len(load_rows)} runs in `{loadtest_path}`")
lines.append("")
if len(load_rows) < 2:
    lines.append("_Not enough runs yet to compute a distribution (need ≥2)._")
    lines.append("")
else:
    # Peak per-run error rate across channels; the gate is per-channel, but the
    # floor/factor are global, so the worst channel per run bounds them.
    peak_err = []
    for row in load_rows:
        channels = row.get("channels", {}) or {}
        rates = [float(v.get("error_rate", 0.0) or 0.0) for v in channels.values()]
        peak_err.append(max(rates) if rates else 0.0)

    observed_max = max(peak_err)
    observed_mean = statistics.mean(peak_err)
    # A floor a little above the observed worst run avoids gating on noise;
    # round up to a clean fraction.
    suggested_floor = max(0.005, round(observed_max * 1.5, 4))

    # Latency volatility (advisory): relative run-to-run p95/p99 deltas.
    def rel_deltas(metric):
        series = {}
        for row in load_rows:
            for name, ch in (row.get("channels", {}) or {}).items():
                series.setdefault(name, []).append(float(ch.get(metric, 0.0) or 0.0))
        out = []
        for values in series.values():
            for a, b in zip(values, values[1:]):
                if a > 0:
                    out.append(abs(b - a) / a * 100.0)
        return out

    p95_deltas = rel_deltas("p95_ms")
    p99_deltas = rel_deltas("p99_ms")

    lines.append(f"- Observed peak error rate: max **{observed_max*100:.3f}%**, mean {observed_mean*100:.3f}%.")
    lines.append(
        f"- Suggested error-rate floor: **{suggested_floor:.4f}** "
        f"(configured {cur_floor:.4f}); keep factor **{cur_factor:g}×** unless the "
        "floor is raised."
    )
    if p95_deltas:
        p95_band = mean_plus_3sigma(p95_deltas)
        lines.append(
            f"- p95 run-to-run volatility (advisory): mean+3σ ≈ **{p95_band:.1f}%**{low_data(len(p95_deltas))} "
            f"over {len(p95_deltas)} deltas — latency stays advisory until this is small and stable."
        )
    if p99_deltas:
        p99_band = mean_plus_3sigma(p99_deltas)
        lines.append(
            f"- p99 run-to-run volatility (advisory): mean+3σ ≈ **{p99_band:.1f}%**{low_data(len(p99_deltas))} "
            f"over {len(p99_deltas)} deltas."
        )
    lines.append("")

# ---- Protected benchmarks ----------------------------------------------
bench_rows = read_ndjson(bench_path)
lines.append(f"## Protected benchmarks — {len(bench_rows)} runs in `{bench_path}`")
lines.append("")
if len(bench_rows) < 2:
    lines.append("_Not enough runs yet to compute a distribution (need ≥2)._")
    lines.append("")
else:
    series = {}
    for row in bench_rows:
        for name, entry in (row.get("benchmarks", {}) or {}).items():
            ns = entry.get("ns_per_op")
            if ns is None:
                continue
            series.setdefault(name, []).append(float(ns))

    lines.append("| Benchmark | Runs | Δ mean+3σ |")
    lines.append("| --- | ---: | ---: |")
    for name in sorted(series):
        values = series[name]
        deltas = []
        for a, b in zip(values, values[1:]):
            if a > 0:
                deltas.append(abs(b - a) / a * 100.0)
        band = mean_plus_3sigma(deltas) if deltas else None
        if band is None:
            lines.append(f"| {name} | {len(values)} | n/a |")
            continue
        lines.append(f"| {name} | {len(values)} | {band:.1f}%{low_data(len(deltas))} |")
    lines.append("")
    lines.append(
        "Run-to-run percents describe runner noise. They are not the pull-request "
        f"fail gate. `PROTECTED_THRESHOLD_PCT` ({cur_pct:.0f}%) only marks informational "
        "suite-shift outliers in the comparison report."
    )

    def ns_of(row, name):
        entry = (row.get("benchmarks") or {}).get(name) or {}
        ns = entry.get("ns_per_op")
        if ns is None:
            return None
        try:
            ns = float(ns)
        except (TypeError, ValueError):
            return None
        return ns if ns > 0 else None

    names = sorted({name for row in bench_rows for name in (row.get("benchmarks") or {})})
    gated = 0
    considered = 0
    for i, cur in enumerate(bench_rows):
        prev = bench_rows[max(0, i - history_window):i]
        if len(prev) < min(8, history_window):
            continue
        considered += 1
        hit = False
        for name in names:
            vals = [v for v in (ns_of(r, name) for r in prev) if v]
            cur_ns = ns_of(cur, name)
            if not vals or cur_ns is None:
                continue
            med = statistics.median(vals)
            if med > 0 and cur_ns > med * history_factor:
                hit = True
                break
        if hit:
            gated += 1
    if considered:
        lines.append(
            f"- At `{history_factor:g}×` the median of the previous {history_window} runs, "
            f"**{gated}/{considered}** later runs in this file would have failed the pull-request gate."
        )
        if considered >= min_samples and gated / considered > 0.10:
            lines.append(
                "- That is more than a tenth of recorded runs. Raise `PROTECTED_HISTORY_FACTOR` "
                "before treating the gate as quiet."
            )
    lines.append("")

print("\n".join(lines))
PY
