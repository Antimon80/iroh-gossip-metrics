#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/run_usecase.sh uc1 [peers] [num] [rate] [size] [repeat]
#   scripts/run_usecase.sh uc2 [peers] [num] [rate] [size] [repeat]
#   scripts/run_usecase.sh uc3 [peers] [num] [rate] [size] [repeat]
#
# The last optional argument specifies how many times to run the use case.
# A fixed 5-second pause is inserted between runs.

UC="${1:-uc1}"
shift || true

PEERS="${1:-50}"    # total peers
NUM="${2:-2000}"    # messages per test
RATE="${3:-10}"     # messages per second
SIZE="${4:-256}"    # payload size (bytes)
REPEAT="${5:-1}"

for run in $(seq 1 "$REPEAT"); do
  echo "===================================="
  echo " Running $UC (run $run of $REPEAT)"
  echo "===================================="

  case "$UC" in
    uc1|uc2|uc3|uc4|uc5|uc6|uc7)
      scripts/usecases/${UC}.sh "$PEERS" "$NUM" "$RATE" "$SIZE"
      ;;
    *)
      echo "Unknown use case: $UC (use uc1, uc2, uc3, uc4)" >&2
      exit 2
      ;;
  esac

  # Pause only if another run will follow
  if [[ $run -lt $REPEAT ]]; then
    echo "Waiting 5 seconds before next run..."
    sleep 5
  fi
done
