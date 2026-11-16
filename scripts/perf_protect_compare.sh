#!/usr/bin/env bash

# Compare a protected-benchmark snapshot with a baseline snapshot.
#
# The fail gate is absolute, against the recent main-history band: a benchmark
# regresses only when its current median ns/op is slower than
# PROTECTED_HISTORY_FACTOR (default 1.75) times the median of the last
# PROTECTED_HISTORY_WINDOW (default 40) history entries. GitHub-hosted runners
# move this suite by about 1.5× between machines, so a 15% suite-relative rule
# fails on noise — including benchmarks that got faster but lagged a runner-wide
# speedup. The suite-shift table stays in the report as information.
#
# When PROTECTED_HISTORY_FILE is unset or has no samples for a benchmark, the
# gate falls back to that same factor against the baseline snapshot alone.
#
# Usage:
#   scripts/perf_protect_compare.sh <baseline_dir> <current_dir> [output_dir]
#
# Env:
#   PROTECTED_HISTORY_FILE     NDJSON history from perf_history_append.sh
#   PROTECTED_HISTORY_FACTOR   slowdown factor, default 1.75
#   PROTECTED_HISTORY_WINDOW   trailing history entries, default 40 (0 = all)
#   REGRESSION_THRESHOLD_PCT   informational suite-excess marker, default 15
#   ENFORCEMENT_MODE           label written into the report, default advisory
#   FAIL_ON_REGRESSION         "1" to exit 2 when the history-band gate fails

set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
	echo "usage: $0 <baseline_dir> <current_dir> [output_dir]" >&2
	exit 1
fi

BASELINE_DIR="$1"
CURRENT_DIR="$2"
OUTPUT_DIR="${3:-benchmarks/compare}"

if [[ ! -d "${BASELINE_DIR}/benchmarks" ]]; then
	echo "baseline benchmarks directory not found: ${BASELINE_DIR}/benchmarks" >&2
	exit 1
fi
if [[ ! -d "${CURRENT_DIR}/benchmarks" ]]; then
	echo "current benchmarks directory not found: ${CURRENT_DIR}/benchmarks" >&2
	exit 1
fi

mkdir -p "${OUTPUT_DIR}"

THRESHOLD_PCT="${REGRESSION_THRESHOLD_PCT:-15}"
MODE="${ENFORCEMENT_MODE:-advisory}"
FAIL_ON_REGRESSION="${FAIL_ON_REGRESSION:-0}"
HISTORY_FILE="${PROTECTED_HISTORY_FILE:-}"
HISTORY_FACTOR="${PROTECTED_HISTORY_FACTOR:-1.75}"
HISTORY_WINDOW="${PROTECTED_HISTORY_WINDOW:-40}"

SUMMARY_MD="${OUTPUT_DIR}/protected-summary.md"
SUMMARY_JSON="${OUTPUT_DIR}/protected-summary.json"

python3 - "${BASELINE_DIR}" "${CURRENT_DIR}" "${SUMMARY_MD}" "${SUMMARY_JSON}" "${THRESHOLD_PCT}" "${MODE}" "${HISTORY_FILE}" "${HISTORY_FACTOR}" "${HISTORY_WINDOW}" <<'PY'
import json
import pathlib
import re
import statistics
import sys

baseline_dir = pathlib.Path(sys.argv[1])
current_dir = pathlib.Path(sys.argv[2])
summary_md = pathlib.Path(sys.argv[3])
summary_json = pathlib.Path(sys.argv[4])
threshold = float(sys.argv[5])
mode = sys.argv[6]
history_file = sys.argv[7].strip()
factor = float(sys.argv[8])
window = int(sys.argv[9])

targets = [
    "BenchmarkServiceGetThreadWindow",
    "BenchmarkServiceGetUserInfos",
    "BenchmarkServiceGetEventBatch",
    "BenchmarkWSGatewayDispatchCacheCallThreadView",
    "BenchmarkWSGatewayDispatchCacheCallUserInfos",
    "BenchmarkLoadFixtureFile",
    "BenchmarkDeriveEventReferences",
    "BenchmarkComputeIterativeGlobalRankLarge",
]

pattern = re.compile(r'^(Benchmark\S+?)(?:-\d+)?\s+\d+\s+([\d.]+)\s+ns/op')


def load_metrics(snapshot_dir: pathlib.Path):
    rows = {}
    for path in sorted((snapshot_dir / "benchmarks").glob("*.txt")):
        for line in path.read_text().splitlines():
            match = pattern.match(line.strip())
            if not match:
                continue
            rows.setdefault(match.group(1), []).append(float(match.group(2)))
    return rows


def load_history(path_str: str, limit: int):
    if not path_str:
        return []
    path = pathlib.Path(path_str)
    if not path.is_file():
        print(f"protected history not found: {path}", file=sys.stderr)
        return []
    entries = []
    for raw in path.read_text().splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except ValueError:
            continue
        benches = obj.get("benchmarks") or {}
        if benches:
            entries.append(benches)
    if limit > 0:
        entries = entries[-limit:]
    return entries


def history_median(entries, name):
    vals = []
    for benches in entries:
        entry = benches.get(name) or {}
        ns = entry.get("ns_per_op")
        if ns is None:
            continue
        try:
            ns = float(ns)
        except (TypeError, ValueError):
            continue
        if ns > 0:
            vals.append(ns)
    if not vals:
        return None, 0
    return statistics.median(vals), len(vals)


old = load_metrics(baseline_dir)
new = load_metrics(current_dir)
history = load_history(history_file, window)
rows = []
measured = []

for name in targets:
    if name not in old or name not in new:
        rows.append({"benchmark": name, "status": "missing"})
        continue
    old_median = statistics.median(old[name])
    new_median = statistics.median(new[name])
    delta_pct = ((new_median - old_median) / old_median) * 100 if old_median else 0.0
    hist_median, hist_n = history_median(history, name)
    row = {
        "benchmark": name,
        "status": "ok",
        "baseline_ns_op": old_median,
        "current_ns_op": new_median,
        "delta_pct": delta_pct,
        "history_median_ns_op": hist_median,
        "history_samples": hist_n,
    }
    rows.append(row)
    measured.append(row)

# Suite shift stays in the report so a single outlier is visible. It is not
# the fail gate: a benchmark that trails a runner-wide speedup, or that moves
# a few percent on a microbenchmark, is noise on these runners.
suite_shift = statistics.median(row["delta_pct"] for row in measured) if measured else 0.0
suite_factor = 1.0 + suite_shift / 100.0
regressions = []
outliers = []
for row in measured:
    bench_factor = 1.0 + row["delta_pct"] / 100.0
    if suite_factor <= 0:
        excess = row["delta_pct"]
    else:
        excess = (bench_factor / suite_factor - 1.0) * 100.0
    row["excess_over_suite_pct"] = excess

    if row["history_median_ns_op"]:
        basis_ns = row["history_median_ns_op"]
        basis = "history"
    else:
        basis_ns = row["baseline_ns_op"]
        basis = "baseline"
    row["gate_basis"] = basis
    row["gate_basis_ns_op"] = basis_ns
    row["gate_limit_ns_op"] = basis_ns * factor if basis_ns else None
    if basis_ns and basis_ns > 0 and row["current_ns_op"] > basis_ns * factor:
        regressions.append(row)
    elif excess >= threshold:
        outliers.append(row)

history_label = history_file if history_file else "none"
if history:
    gate_line = (
        f"slower than `{factor:g}×` the median of the last "
        f"`{len(history)}` main snapshots"
    )
else:
    gate_line = (
        f"slower than `{factor:g}×` the baseline snapshot "
        "(no history samples)"
    )

md_lines = [
    "# Protected Benchmark Comparison",
    "",
    f"- Mode: `{mode}`",
    f"- Gate: {gate_line}",
    f"- History: `{history_label}`",
    f"- Suite median shift vs latest snapshot: `{suite_shift:+.2f}%` (informational)",
    f"- Suite-shift marker: `{threshold:.1f}%` beyond the suite (informational; does not fail the job)",
    f"- Baseline snapshot: `{baseline_dir}`",
    f"- Current snapshot: `{current_dir}`",
    "",
    "| Benchmark | History median ns/op | Baseline ns/op | Current ns/op | vs history | vs baseline | suite excess |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
]


def fmt_ns(value):
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def fmt_pct(value):
    if value is None:
        return "n/a"
    return f"{value:+.2f}%"


for row in rows:
    if row["status"] == "missing":
        md_lines.append(f"| `{row['benchmark']}` | n/a | n/a | n/a | missing | missing | missing |")
        continue
    vs_history = None
    if row["history_median_ns_op"]:
        vs_history = ((row["current_ns_op"] - row["history_median_ns_op"]) / row["history_median_ns_op"]) * 100
    md_lines.append(
        f"| `{row['benchmark']}` | {fmt_ns(row['history_median_ns_op'])} | "
        f"{row['baseline_ns_op']:.2f} | {row['current_ns_op']:.2f} | "
        f"{fmt_pct(vs_history)} | {row['delta_pct']:+.2f}% | {row['excess_over_suite_pct']:+.2f}% |"
    )

if regressions:
    md_lines.append("")
    md_lines.append("## Regressions")
    for row in regressions:
        md_lines.append(
            f"- `{row['benchmark']}` {row['current_ns_op']:.2f} ns/op is "
            f"{row['current_ns_op'] / row['gate_basis_ns_op']:.2f}× the "
            f"{row['gate_basis']} median {row['gate_basis_ns_op']:.2f} "
            f"(limit {factor:g}×)"
        )
else:
    md_lines.append("")
    if history:
        md_lines.append("No protected benchmark exceeded the history-band gate.")
    else:
        md_lines.append(f"No protected benchmark exceeded {factor:g}× the baseline snapshot.")

if outliers:
    md_lines.append("")
    md_lines.append("## Suite-shift outliers (informational)")
    md_lines.append("")
    md_lines.append("These lagged the rest of the suite and are inside the history-band gate.")
    for row in outliers:
        md_lines.append(
            f"- `{row['benchmark']}` delta `{row['delta_pct']:+.2f}%` "
            f"({row['excess_over_suite_pct']:+.2f}% beyond the suite shift)"
        )

summary_md.write_text("\n".join(md_lines) + "\n")
summary_json.write_text(json.dumps({
    "rows": rows,
    "regressions": regressions,
    "suite_outliers": outliers,
    "suite_shift_pct": suite_shift,
    "history_samples": len(history),
    "history_factor": factor,
    "history_file": history_file or None,
}, indent=2) + "\n")

print(f"regression_count={len(regressions)}")
print(f"history_samples={len(history)}")
print("comparison_ready=true")
PY

REGRESSION_COUNT="$(python3 - "${SUMMARY_JSON}" <<'PY'
import json
import sys
path = sys.argv[1]
data = json.load(open(path))
print(len(data.get("regressions", [])))
PY
)"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
	{
		echo "regression_count=${REGRESSION_COUNT}"
		echo "comparison_ready=true"
	} >>"${GITHUB_OUTPUT}"
fi

echo "wrote protected benchmark comparison:"
echo "  ${SUMMARY_MD}"
echo "  ${SUMMARY_JSON}"

if [[ "${FAIL_ON_REGRESSION}" == "1" && "${REGRESSION_COUNT}" != "0" ]]; then
	echo "protected benchmark gate failed (see ${SUMMARY_MD})." >&2
	exit 2
fi
