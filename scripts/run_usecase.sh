#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/run_usecase.sh uc1 [scenario] [peers] [num] [rate] [size]
#   scripts/run_usecase.sh uc2 [scenario] [peers] [num] [rate] [size]
#
# Examples:
#   scripts/run_usecase.sh uc1 scripts/scenarios/netem-none.sh 20 2000 50 256
#   scripts/run_usecase.sh uc2 scripts/scenarios/netem-loss10.sh 20 2000 50 256

UC="${1:-uc1}"
SCENARIO="${2:-scripts/scenarios/netem-none.sh}"
PEERS="${3:-20}"
NUM="${4:-2000}"
RATE="${5:-50}"
SIZE="${6:-256}"

case "$UC" in
  uc1)
    exec scripts/usecases/uc1.sh "$SCENARIO" "$PEERS" "$NUM" "$RATE" "$SIZE"
    ;;
  uc2)
    exec scripts/usecases/uc2.sh "$SCENARIO" "$PEERS" "$NUM" "$RATE" "$SIZE"
    ;;
  *)
    echo "Unknown use case: $UC (use uc1 or uc2)" >&2
    exit 2
    ;;
esac
