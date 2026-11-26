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
echo "commit $(git rev-parse HEAD)"
echo "implementation $(git rev-parse origin/feat/relay-capability-probes)"
echo "core $(git rev-parse origin/fix/relay-intelligence)"
CORE=31df4c808129f633593d7bcd605cb178dad40727
if [ "${RELAY_INTEL_OPERATIONAL_COMPILE:-}" = "1" ]; then
  compiled="${TMPDIR:-/tmp}/relayintelbench.test"
  go test -tags relayintelbench -c -o "$compiled" ./internal/api/
  rm -f "$compiled"
  echo "tagged compile ok"
  exit 0
fi
if git merge-base --is-ancestor "$CORE" HEAD; then
  echo "core incorporated $CORE"
  export RELAY_INTEL_OPERATIONAL_REQUIRE_FINAL=1
else
  echo "core not incorporated $CORE"
  if [ "${RELAY_INTEL_OPERATIONAL_ALLOW_PARTIAL:-}" != "1" ]; then
    echo "refusing the account-scale benchmark until that core commit is in HEAD" >&2
    exit 1
  fi
fi
echo "scale $RELAY_INTEL_OPERATIONAL_SCALE"
echo "output $RELAY_INTEL_OPERATIONAL_OUT"

go test -tags relayintelbench ./internal/api/ \
  -run TestOperationalRelayIntelligence \
  -v -count=1 -timeout 240m
