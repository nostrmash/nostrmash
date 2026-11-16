#!/usr/bin/env bash

# Download the newest non-expired Actions artifact of a given name.
#
# The Performance Snapshot baseline jobs publish this instead of pushing to
# main. Callers fall back to the committed seed when the download is
# unavailable (first run, fork pull request, or API error).
#
# Usage:
#   scripts/perf_fetch_baseline_artifact.sh <artifact-name> <dest-dir>
#
# Prints found=true or found=false. The same line is appended to
# $GITHUB_OUTPUT when that variable is set. Exit 0 when the artifact is
# missing so the caller can fall back. Exit 2 on a usage error.
#
# A successful download is laid out as:
#   <dest>/history.ndjson
#   <dest>/main-latest/...

set -euo pipefail

if [[ $# -ne 2 ]]; then
	echo "usage: $0 <artifact-name> <dest-dir>" >&2
	exit 2
fi

name="$1"
dest="$2"
repo="${GITHUB_REPOSITORY:-}"

report() {
	local found="$1"
	echo "found=${found}"
	if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
		echo "found=${found}" >>"${GITHUB_OUTPUT}"
		echo "dir=${dest}" >>"${GITHUB_OUTPUT}"
	fi
}

if [[ -z "${repo}" ]]; then
	echo "perf-fetch-baseline: GITHUB_REPOSITORY is unset; skipping ${name}." >&2
	report false
	exit 0
fi

if ! command -v gh >/dev/null 2>&1; then
	echo "perf-fetch-baseline: gh is not installed; skipping ${name}." >&2
	report false
	exit 0
fi

if [[ -z "${GH_TOKEN:-${GITHUB_TOKEN:-}}" ]]; then
	echo "perf-fetch-baseline: no token; skipping ${name}." >&2
	report false
	exit 0
fi

err="$(mktemp)"
trap 'rm -f "${err}"' EXIT

set +e
payload="$(gh api "/repos/${repo}/actions/artifacts?name=${name}&per_page=20" 2>"${err}")"
status=$?
set -e
if [[ "${status}" -ne 0 ]]; then
	echo "perf-fetch-baseline: listing ${name} failed; using the committed seed." >&2
	cat "${err}" >&2 || true
	report false
	exit 0
fi

set +e
meta="$(printf '%s' "${payload}" | python3 -c '
import json, sys
name = sys.argv[1]
try:
    data = json.load(sys.stdin)
except ValueError:
    data = {}
arts = [a for a in data.get("artifacts", []) if a.get("name") == name and not a.get("expired")]
arts.sort(key=lambda a: a.get("created_at") or "", reverse=True)
if not arts:
    sys.stdout.write("\n\n")
else:
    run = arts[0].get("workflow_run") or {}
    sys.stdout.write("%s\n%s\n" % (arts[0].get("id", ""), run.get("id", "")))
' "${name}")"
status=$?
set -e
if [[ "${status}" -ne 0 ]]; then
	echo "perf-fetch-baseline: could not parse the artifact list; using the committed seed." >&2
	report false
	exit 0
fi
artifact_id="$(printf '%s\n' "${meta}" | sed -n '1p')"
run_id="$(printf '%s\n' "${meta}" | sed -n '2p')"

if [[ -z "${artifact_id}" || -z "${run_id}" ]]; then
	echo "perf-fetch-baseline: no ${name} artifact on record." >&2
	report false
	exit 0
fi

rm -rf "${dest}"
mkdir -p "${dest}"
set +e
gh run download "${run_id}" --name "${name}" --dir "${dest}" 2>"${err}"
status=$?
set -e
if [[ "${status}" -ne 0 ]]; then
	echo "perf-fetch-baseline: download of ${name} from run ${run_id} failed; using the committed seed." >&2
	cat "${err}" >&2 || true
	rm -rf "${dest}"
	mkdir -p "${dest}"
	report false
	exit 0
fi

# upload-artifact stores files relative to the staged directory. If a
# wrapper directory was included, hoist the baseline up to dest.
if [[ ! -f "${dest}/history.ndjson" && ! -d "${dest}/main-latest" ]]; then
	nested="$(find "${dest}" -mindepth 2 -maxdepth 6 -name history.ndjson -print -quit || true)"
	if [[ -n "${nested}" ]]; then
		parent="$(dirname "${nested}")"
		shopt -s dotglob nullglob
		mv "${parent}/"* "${dest}/"
		shopt -u dotglob nullglob
	fi
fi

if [[ -f "${dest}/history.ndjson" || -d "${dest}/main-latest" ]]; then
	echo "perf-fetch-baseline: downloaded ${name} from run ${run_id} into ${dest}." >&2
	report true
	exit 0
fi

echo "perf-fetch-baseline: ${name} from run ${run_id} had no baseline layout." >&2
report false
