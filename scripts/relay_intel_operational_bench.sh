#!/bin/sh
# Operational Relay Intelligence benchmark.
# Uses a disposable schema on the local development database. It does not
# probe public relays and it does not touch production infrastructure.
set -eu
cd "$(dirname "$0")/.."

export TEST_DATABASE_URL="${TEST_DATABASE_URL:-postgres://nostrmash:nostrmash@127.0.0.1:5432/nostrmash?sslmode=disable}"
export RELAY_INTEL_OPERATIONAL_BENCH=1
export RELAY_INTEL_OPERATIONAL_SCALE="${RELAY_INTEL_OPERATIONAL_SCALE:-both}"
export RELAY_INTEL_OPERATIONAL_OUT="${RELAY_INTEL_OPERATIONAL_OUT:-benchmarks/relay-intelligence/operational}"
export RELAY_INTEL_OPERATIONAL_MIXED_EVENTS="${RELAY_INTEL_OPERATIONAL_MIXED_EVENTS:-200}"
export RELAY_INTEL_OPERATIONAL_MIXED_REPEATS="${RELAY_INTEL_OPERATIONAL_MIXED_REPEATS:-3}"
export RELAY_INTEL_OPERATIONAL_MIXED_WINDOW_MS="${RELAY_INTEL_OPERATIONAL_MIXED_WINDOW_MS:-30000}"
export RELAY_INTEL_OPERATIONAL_BURST_REPEATS="${RELAY_INTEL_OPERATIONAL_BURST_REPEATS:-2}"

mkdir -p "$RELAY_INTEL_OPERATIONAL_OUT"
FROZEN_CORE=678254924cb49f9a34db19aa879563b48f8e7d91
INTEGRATED=1b144ea9b0a90e77788ba1b0752fd5674d8f3f21
echo "tooling $(git rev-parse HEAD)"
echo "implementation $INTEGRATED"
echo "core $FROZEN_CORE"
echo "remote_capability_tip $(git rev-parse origin/feat/relay-capability-probes 2>/dev/null || echo absent)"
if [ "${RELAY_INTEL_OPERATIONAL_COMPILE:-}" = "1" ]; then
  compiled="${TMPDIR:-/tmp}/relayintelbench.test"
  go test -tags relayintelbench -c -o "$compiled" ./internal/api/
  rm -f "$compiled"
  echo "tagged compile ok"
  exit 0
fi
if ! git cat-file -e "$INTEGRATED^{commit}" || ! git cat-file -e "$FROZEN_CORE^{commit}"; then
  echo "required commit is not in this repository" >&2
  exit 1
fi
if ! git merge-base --is-ancestor "$FROZEN_CORE" "$INTEGRATED"; then
  echo "implementation $INTEGRATED does not contain frozen core $FROZEN_CORE" >&2
  exit 1
fi
if git merge-base --is-ancestor "$INTEGRATED" HEAD && git merge-base --is-ancestor "$FROZEN_CORE" HEAD; then
  echo "implementation incorporated $INTEGRATED"
  echo "core incorporated $FROZEN_CORE"
  export RELAY_INTEL_OPERATIONAL_REQUIRE_FINAL=1
  unset RELAY_INTEL_OPERATIONAL_PARTIAL
else
  echo "HEAD does not contain implementation $INTEGRATED and core $FROZEN_CORE" >&2
  if [ "${RELAY_INTEL_OPERATIONAL_ALLOW_PARTIAL:-}" != "1" ]; then
    echo "refusing the account-scale benchmark" >&2
    exit 1
  fi
  echo "PARTIAL benchmark. This output is not operational sign-off." >&2
  export RELAY_INTEL_OPERATIONAL_PARTIAL=1
  unset RELAY_INTEL_OPERATIONAL_REQUIRE_FINAL
fi
echo "scale $RELAY_INTEL_OPERATIONAL_SCALE"
echo "output $RELAY_INTEL_OPERATIONAL_OUT"

go test -tags relayintelbench ./internal/api/ \
  -run TestOperationalRelayIntelligence \
  -v -count=1 -timeout 240m
