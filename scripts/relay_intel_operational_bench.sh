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
export RELAY_INTEL_OPERATIONAL_MIXED_REPEATS="${RELAY_INTEL_OPERATIONAL_MIXED_REPEATS:-2}"

mkdir -p "$RELAY_INTEL_OPERATIONAL_OUT"
echo "commit $(git rev-parse HEAD)"
echo "scale $RELAY_INTEL_OPERATIONAL_SCALE"
echo "output $RELAY_INTEL_OPERATIONAL_OUT"

go test ./internal/api/ \
  -run TestOperationalRelayIntelligence \
  -v -count=1 -timeout 180m
