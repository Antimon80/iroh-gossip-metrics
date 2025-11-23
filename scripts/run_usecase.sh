#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/run_usecase.sh uc1 [peers] [num] [rate] [size]
#   scripts/run_usecase.sh uc2 [peers] [num] [rate] [size]
#   scripts/run_usecase.sh uc3 [peers] [num] [rate] [size]
#
# Scenario is optional via ENV:
#   SCENARIO=scripts/scenarios/netem-loss10.sh scripts/run_usecase.sh uc3

UC="${1:-uc1}"
shift || true

PEERS="${1:-20}"
NUM="${2:-2000}"
RATE="${3:-50}"
SIZE="${4:-256}"

case "$UC" in
  uc1|uc2|uc3)
    exec "scripts/usecases/${UC}.sh" "$PEERS" "$NUM" "$RATE" "$SIZE"
    ;;
  *)
    echo "Unknown use case: $UC (use uc1, uc2, uc3)" >&2
    exit 2
    ;;
esac
