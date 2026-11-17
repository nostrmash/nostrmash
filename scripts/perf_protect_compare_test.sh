#!/usr/bin/env bash

# Locks the protected-benchmark gate to the history band. A benchmark that
# merely trails a runner-wide shift must not fail, and a doubling must.

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

names=(
	BenchmarkServiceGetThreadWindow
	BenchmarkServiceGetUserInfos
	BenchmarkServiceGetEventBatch
	BenchmarkWSGatewayDispatchCacheCallThreadView
	BenchmarkWSGatewayDispatchCacheCallUserInfos
	BenchmarkLoadFixtureFile
	BenchmarkDeriveEventReferences
	BenchmarkComputeIterativeGlobalRankLarge
)

write_snapshot() {
	local dir="$1"
	shift
	mkdir -p "${dir}/benchmarks"
	: >"${dir}/benchmarks/bench.txt"
	local name ns
	while [[ $# -ge 2 ]]; do
		name="$1"
		ns="$2"
		shift 2
		printf '%s-8  100  %s ns/op\n' "${name}" "${ns}" >>"${dir}/benchmarks/bench.txt"
	done
}

write_flat_history() {
	local path="$1"
	local ns="$2"
	local i line benches
	: >"${path}"
	for i in $(seq 1 40); do
		benches="{"
		local first=1
		for name in "${names[@]}"; do
			if [[ "${first}" -eq 0 ]]; then
				benches+=","
			fi
			first=0
			benches+="\"${name}\":{\"ns_per_op\":${ns}}"
		done
		benches+="}"
		printf '{"ts":"2026-01-01T00:00:00Z","sha":"hist%02d","benchmarks":%s}\n' "${i}" "${benches}" >>"${path}"
	done
}

assert_count() {
	local json="$1"
	local want="$2"
	local got
	got="$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1])).get("regressions", [])))' "${json}")"
	if [[ "${got}" != "${want}" ]]; then
		echo "expected regression_count=${want}, got ${got}" >&2
		cat "${json}" >&2
		exit 1
	fi
}

assert_benchmark() {
	local json="$1"
	local name="$2"
	python3 -c '
import json, sys
data = json.load(open(sys.argv[1]))
names = [row["benchmark"] for row in data.get("regressions", [])]
if sys.argv[2] not in names:
    raise SystemExit("missing regression %s in %s" % (sys.argv[2], names))
' "${json}" "${name}"
}

# Flat 1000 ns history and baseline. One benchmark at 2000 ns fails.
# The same benchmark at 1400 ns (1.4×) does not. A suite that speeds up
# 22% while two benchmarks speed up 3% does not.
pairs=()
for name in "${names[@]}"; do
	pairs+=("${name}" 1000)
done
write_snapshot "${work}/baseline" "${pairs[@]}"
write_flat_history "${work}/history.ndjson" 1000

current_pairs=("${pairs[@]}")
current_pairs[7]=2000
write_snapshot "${work}/current-double" "${current_pairs[@]}"

set +e
FAIL_ON_REGRESSION=1 \
PROTECTED_HISTORY_FILE="${work}/history.ndjson" \
	bash "${root}/scripts/perf_protect_compare.sh" \
	"${work}/baseline" "${work}/current-double" "${work}/out-double"
status=$?
set -e
if [[ "${status}" -ne 2 ]]; then
	echo "doubling a benchmark should fail the gate, exit ${status}" >&2
	exit 1
fi
assert_count "${work}/out-double/protected-summary.json" 1
assert_benchmark "${work}/out-double/protected-summary.json" BenchmarkWSGatewayDispatchCacheCallThreadView

current_pairs=("${pairs[@]}")
current_pairs[7]=1400
write_snapshot "${work}/current-noise" "${current_pairs[@]}"
FAIL_ON_REGRESSION=1 \
PROTECTED_HISTORY_FILE="${work}/history.ndjson" \
	bash "${root}/scripts/perf_protect_compare.sh" \
	"${work}/baseline" "${work}/current-noise" "${work}/out-noise"
assert_count "${work}/out-noise/protected-summary.json" 0

# Six benchmarks at 780 ns (-22%), two at 970 ns (-3%). Excess over the
# suite is about +24%, and both are still under 1.75× the history median.
speed_pairs=()
for name in "${names[@]}"; do
	speed_pairs+=("${name}" 780)
done
speed_pairs[7]=970
speed_pairs[9]=970
write_snapshot "${work}/current-speedup" "${speed_pairs[@]}"
FAIL_ON_REGRESSION=1 \
PROTECTED_HISTORY_FILE="${work}/history.ndjson" \
REGRESSION_THRESHOLD_PCT=15 \
	bash "${root}/scripts/perf_protect_compare.sh" \
	"${work}/baseline" "${work}/current-speedup" "${work}/out-speedup"
assert_count "${work}/out-speedup/protected-summary.json" 0
python3 -c '
import json, sys
data = json.load(open(sys.argv[1]))
names = {row["benchmark"] for row in data.get("suite_outliers", [])}
want = {
    "BenchmarkWSGatewayDispatchCacheCallThreadView",
    "BenchmarkWSGatewayDispatchCacheCallUserInfos",
}
missing = want - names
if missing:
    raise SystemExit("expected informational outliers %s" % sorted(missing))
' "${work}/out-speedup/protected-summary.json"

# Without history, the gate is 1.75× the baseline snapshot.
slow_pairs=("${pairs[@]}")
slow_pairs[1]=1800
write_snapshot "${work}/current-no-history" "${slow_pairs[@]}"
set +e
FAIL_ON_REGRESSION=1 \
PROTECTED_HISTORY_FILE="" \
	bash "${root}/scripts/perf_protect_compare.sh" \
	"${work}/baseline" "${work}/current-no-history" "${work}/out-no-history"
status=$?
set -e
if [[ "${status}" -ne 2 ]]; then
	echo "1.8× the baseline snapshot should fail without history, exit ${status}" >&2
	exit 1
fi
assert_benchmark "${work}/out-no-history/protected-summary.json" BenchmarkServiceGetThreadWindow

# The recorded ThreadView sample from the red pull request (14771 ns/op)
# is inside the committed history band.
repo_history="${root}/benchmarks/history/protection/history.ndjson"
if [[ -s "${repo_history}" ]]; then
	# Each benchmark sits on its own history median, except ThreadView at the
	# 14771 ns/op sample from the red pull request.
	incident_pairs=()
	while IFS=$'\t' read -r name ns; do
		incident_pairs+=("${name}" "${ns}")
	done < <(python3 -c '
import json, statistics, sys
rows = [json.loads(line) for line in open(sys.argv[1]) if line.strip()]
rows = rows[-40:]
names = sys.argv[2:]
for name in names:
    vals = [row["benchmarks"][name]["ns_per_op"] for row in rows if name in row.get("benchmarks", {})]
    print("%s\t%s" % (name, statistics.median(vals)))
' "${repo_history}" "${names[@]}")
	incident_pairs[7]=14771
	write_snapshot "${work}/baseline-repo" "${incident_pairs[@]}"
	write_snapshot "${work}/current-incident" "${incident_pairs[@]}"
	FAIL_ON_REGRESSION=1 \
	PROTECTED_HISTORY_FILE="${repo_history}" \
		bash "${root}/scripts/perf_protect_compare.sh" \
		"${work}/baseline-repo" "${work}/current-incident" "${work}/out-incident"
	assert_count "${work}/out-incident/protected-summary.json" 0
fi

# Same commit refreshes one history line; a new commit appends.
append_src="${work}/append-src"
write_snapshot "${append_src}" "${pairs[@]}"
append_hist="${work}/append-history.ndjson"
HISTORY_SHA=abc HISTORY_TS=2026-01-01T00:00:00Z \
	bash "${root}/scripts/perf_history_append.sh" benchmarks "${append_src}" "${append_hist}"
HISTORY_SHA=abc HISTORY_TS=2026-01-02T00:00:00Z \
	bash "${root}/scripts/perf_history_append.sh" benchmarks "${append_src}" "${append_hist}"
HISTORY_SHA=def HISTORY_TS=2026-01-03T00:00:00Z \
	bash "${root}/scripts/perf_history_append.sh" benchmarks "${append_src}" "${append_hist}"
python3 -c '
import json, sys
rows = [json.loads(line) for line in open(sys.argv[1]) if line.strip()]
if [row["sha"] for row in rows] != ["abc", "def"]:
    raise SystemExit("history shas %s" % [row["sha"] for row in rows])
if rows[0]["ts"] != "2026-01-02T00:00:00Z":
    raise SystemExit("rerun did not replace the same sha, ts=%s" % rows[0]["ts"])
' "${append_hist}"

echo "perf-protect-compare gate checks passed"
