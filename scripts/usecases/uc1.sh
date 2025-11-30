#!/usr/bin/env bash
set -euo pipefail

# UC1: all peers in same LAN, Direct discovery (no relay, static bootstraps)

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
source "$ROOT/scripts/netns/common_netns.sh"

SCENARIO="${SCENARIO:-scripts/scenarios/netem-none.sh}"
PEERS="$1"
NUM="$2"
RATE="$3"
SIZE="$4"
DISCOVERY="${5:-DIRECT}"

# Number of dedicated bootstrap peers (default 5)
BOOTSTRAP_COUNT="${BOOTSTRAP_COUNT:-5}"

TOPIC="${TOPIC:-lab}"
BASELOG="${LOGDIR:-logs/uc1}"

# Create parameter-tagged run directory
TS=$(date +"%Y%m%d-%H%M%S")
TAG="uc1_p${PEERS}_m${NUM}_r${RATE}_s${SIZE}_${DISCOVERY}"
RUN_ID="run-${TS}_${TAG}"

LOGDIR="$BASELOG/$RUN_ID"
BIN="$ROOT/target/release/iroh-gossip-metrics"

mkdir -p "$LOGDIR"

echo "== UC1 Direct LAN with $PEERS peers (bootstraps=$BOOTSTRAP_COUNT) =="
echo "SCENARIO=$SCENARIO NUM=$NUM RATE=$RATE SIZE=$SIZE TOPIC=$TOPIC"
echo

cleanup_netns || true
setup_bridge
setup_namespaces

echo "== Build project =="
cargo build --release

echo "== Apply scenario on bridge $BR =="
bash "$ROOT/$SCENARIO" "$BR"

#############################################
# 1) BOOTSTRAP RECEIVERS (PEER 1..BOOTSTRAP_COUNT)
#############################################

BOOTSTRAP_IDS=()
RECV_PIDS=()

for i in $(seq 1 "$BOOTSTRAP_COUNT"); do
  BOOT_RLOG="$LOGDIR/peer${i}-recv.jsonl"
  BOOT_RERR="$LOGDIR/peer${i}-recv.stderr"
  BOOT_SUM="$LOGDIR/peer${i}-summary.json"

  : > "$BOOT_RERR"

  echo "== Start bootstrap receiver in peer$i =="
  run_in_ns "$i" "$BIN" \
    --role receiver \
    --log "$BOOT_RLOG" \
    --idle-report-ms 3000 \
    --topic-name "$TOPIC" \
    --discovery direct \
    1> "$BOOT_SUM" \
    2> "$BOOT_RERR" &

  pid=$!
  RECV_PIDS+=("$pid")

  NODE_ID=""
  for _ in {1..80}; do
    if grep -q "node_id=" "$BOOT_RERR"; then
      NODE_ID="$(grep -m1 'node_id=' "$BOOT_RERR" | sed -E 's/.*node_id=([[:alnum:]]+).*/\1/')"
      break
    fi
    sleep 0.1
  done

  if [[ -z "$NODE_ID" ]]; then
    echo "ERROR: Could not detect bootstrap node_id for peer$i" >&2
    # cleanup and abort
    for p in "${RECV_PIDS[@]}"; do
      kill "$p" 2>/dev/null || true
    done
    cleanup_netns
    exit 1
  fi

  BOOTSTRAP_IDS+=("$NODE_ID")

  # small pause to let gossip stabilize a bit
  sleep 0.5
done

# Build comma-separated bootstrap list for CLI
# Example: "id1,id2,id3,id4,id5"
BOOTSTRAP_CSV="$(IFS=,; echo "${BOOTSTRAP_IDS[*]}")"
echo "== Using bootstrap list: $BOOTSTRAP_CSV =="

#############################################
# 2) START REMAINING RECEIVERS (PEER BOOTSTRAP_COUNT+1 .. PEERS)
#############################################

echo "== Start receivers peer$((BOOTSTRAP_COUNT+1))..peer$PEERS =="

for i in $(seq $((BOOTSTRAP_COUNT + 1)) "$PEERS"); do
  RLOG="$LOGDIR/peer${i}-recv.jsonl"
  RERR="$LOGDIR/peer${i}-recv.stderr"
  RSUM="$LOGDIR/peer${i}-summary.json"

  run_in_ns "$i" "$BIN" \
    --role receiver \
    --log "$RLOG" \
    --idle-report-ms 3000 \
    --topic-name "$TOPIC" \
    --discovery direct \
    --bootstrap "$BOOTSTRAP_CSV" \
    1> "$RSUM" \
    2> "$RERR" &

  RECV_PIDS+=("$!")
done

# give the overlay a bit of time to converge
sleep 3

#############################################
# 3) START SENDER IN PEER 1 (also with all bootstraps)
#############################################
SLOG="$LOGDIR/send.jsonl"

echo "== Start sender in peer1 (bootstraps=$BOOTSTRAP_CSV) =="
run_in_ns 1 "$BIN" \
  --role sender \
  --log "$SLOG" \
  --num "$NUM" \
  --rate "$RATE" \
  --size "$SIZE" \
  --topic-name "$TOPIC" \
  --discovery direct \
  --bootstrap "$BOOTSTRAP_CSV"

#############################################
# 4) WAIT FOR ALL RECEIVERS
#############################################
echo "== Waiting for ALL receivers to finish =="

for pid in "${RECV_PIDS[@]}"; do
  wait "$pid" || true
done

sleep 1   # short settle time

#############################################
# 5) CLEANUP
#############################################

echo "== Clear scenario =="
bash "$ROOT/scripts/scenarios/netem-none.sh" "$BR"

echo "== [netns] cleanup =="
cleanup_netns

echo "== UC1 done. Logs in $LOGDIR =="
