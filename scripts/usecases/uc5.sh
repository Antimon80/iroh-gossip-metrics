#!/usr/bin/env bash
set -euo pipefail

# UC5: Churn and Recovery
#
# Same basic idea as UC1:
# - there is exactly one bootstrap peer (peer1)
# - all other peers bootstrap using peer1's node_id
#
# Additionally:
# - after CHURN_START seconds, a fixed random subset of non-bootstrap peers
#   (from 2..PEERS) is killed ("churned")
# - after CHURN_DOWN seconds, exactly this subset of peers rejoins

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
source "$ROOT/scripts/netns/common_netns.sh"

# Network scenario on the bridge (default: no loss/delay)
SCENARIO="${SCENARIO:-scripts/scenarios/netem-none.sh}"

PEERS="$1"
NUM="$2"
RATE="$3"
SIZE="$4"
DISCOVERY="${5:-DIRECT}"

TOPIC="${TOPIC:-lab}"

# Churn parameters
CHURN_START="${CHURN_START:-5}"   # seconds until churn starts
CHURN_DOWN="${CHURN_DOWN:-10}"   # seconds peers stay offline
CHURN_COUNT="${CHURN_COUNT:-0}"  # number of non-bootstrap peers to churn (0 = choose default)

# group logs by peer count
BASELOG="${LOGDIR:-logs/uc5}/p${PEERS}"

# Create parameter-tagged run directory
TS=$(date +"%Y%m%d-%H%M%S")
TAG="uc5_p${PEERS}_m${NUM}_r${RATE}_s${SIZE}_${DISCOVERY}"
RUN_ID="run-${TS}_${TAG}"

LOGDIR="$BASELOG/$RUN_ID"
BIN="$ROOT/target/release/iroh-gossip-metrics"

mkdir -p "$LOGDIR"

echo "== UC5 Churn & Recovery with $PEERS peers (single bootstrap peer1) =="
echo "SCENARIO=$SCENARIO NUM=$NUM RATE=$RATE SIZE=$SIZE TOPIC=$TOPIC"
echo "CHURN_START=${CHURN_START}s CHURN_DOWN=${CHURN_DOWN}s CHURN_COUNT=${CHURN_COUNT:-auto}"
echo "LOGDIR=$LOGDIR"
echo

cleanup_netns || true
setup_bridge
setup_namespaces

echo "== Build project =="
cargo build --release

echo "== Apply scenario on bridge $BR =="
bash "$ROOT/$SCENARIO" "$BR"

#############################################
# 1) BOOTSTRAP RECEIVER (PEER 1)
#############################################

declare -a RECV_PIDS

BOOT_RLOG="$LOGDIR/peer1-recv.jsonl"
BOOT_RERR="$LOGDIR/peer1-recv.stderr"
BOOT_SUM="$LOGDIR/peer1-summary.json"

: > "$BOOT_RERR"

echo "== Start bootstrap receiver in peer1 =="
run_in_ns 1 "$BIN" \
  --role receiver \
  --log "$BOOT_RLOG" \
  --idle-report-ms 3000 \
  --topic-name "$TOPIC" \
  --discovery direct \
  1> "$BOOT_SUM" \
  2> "$BOOT_RERR" &
BOOT_PID=$!
RECV_PIDS[1]=$BOOT_PID

#############################################
# 2) EXTRACT bootstrap node_id
#############################################

echo "== Waiting for bootstrap node_id from peer1 =="
NODE_ID=""
for _ in {1..80}; do
  if grep -q "node_id=" "$BOOT_RERR"; then
    NODE_ID="$(grep -m1 'node_id=' "$BOOT_RERR" | sed -E 's/.*node_id=([[:alnum:]]+).*/\1/')"
    break
  fi
  sleep 0.25
done

if [[ -z "$NODE_ID" ]]; then
  echo "ERROR: Could not detect bootstrap node_id from peer1" >&2
  kill "$BOOT_PID" || true
  cleanup_netns
  exit 1
fi

echo "== Bootstrap node_id (peer1): $NODE_ID =="

#############################################
# 3) START REMAINING RECEIVERS (PEER 2..PEERS)
#############################################

if (( PEERS > 1 )); then
  echo "== Start receivers peer2..peer$PEERS =="

  for i in $(seq 2 "$PEERS"); do
    RLOG="$LOGDIR/peer${i}-recv.jsonl"
    RERR="$LOGDIR/peer${i}-recv.stderr"
    RSUM="$LOGDIR/peer${i}-summary.json"

    run_in_ns "$i" "$BIN" \
      --role receiver \
      --log "$RLOG" \
      --idle-report-ms 3000 \
      --topic-name "$TOPIC" \
      --discovery direct \
      --bootstrap "$NODE_ID" \
      1> "$RSUM" \
      2> "$RERR" &

    RECV_PIDS[$i]=$!
  done
fi

#############################################
# 4) START SENDER IN PEER 1 (BACKGROUND)
#############################################

SLOG="$LOGDIR/send.jsonl"

echo "== Start sender in peer1 (bootstraps=$NODE_ID) in background =="
run_in_ns 1 "$BIN" \
  --role sender \
  --log "$SLOG" \
  --num "$NUM" \
  --rate "$RATE" \
  --size "$SIZE" \
  --topic-name "$TOPIC" \
  --discovery direct \
  --bootstrap "$NODE_ID" &
SENDER_PID=$!

#############################################
# 5) CHURN PHASE: DISCONNECT + REJOIN OF RANDOM SUBSET OF PEERS 2..PEERS
#############################################

if (( PEERS > 1 )); then
  # Determine how many peers to churn if not explicitly set
  # Default: half of the non-bootstrap peers (rounded up), but at least 1
  NON_BOOTSTRAP=$((PEERS - 1))

  if (( CHURN_COUNT <= 0 )); then
    CHURN_COUNT=$(((NON_BOOTSTRAP + 1) / 2))
  fi

  # Clamp CHURN_COUNT to [1, NON_BOOTSTRAP]
  if (( CHURN_COUNT < 1 )); then
    CHURN_COUNT=1
  fi
  if (( CHURN_COUNT > NON_BOOTSTRAP )); then
    CHURN_COUNT=$NON_BOOTSTRAP
  fi

  # Randomly select CHURN_COUNT distinct peers from 2..PEERS
  # This subset remains fixed for kill + rejoin.
  mapfile -t CHURN_PEERS < <(seq 2 "$PEERS" | shuf -n "$CHURN_COUNT" | sort -n)

  echo "== Churn phase: random subset of non-bootstrap peers =="
  echo "   Selected churn peers: ${CHURN_PEERS[*]}"
  echo "== Waiting ${CHURN_START}s before starting churn =="
  sleep "$CHURN_START"

  echo "== Churn: killing receivers in peers ${CHURN_PEERS[*]} =="
  for i in "${CHURN_PEERS[@]}"; do
    pid=${RECV_PIDS[$i]:-}
    if [[ -n "${pid:-}" ]]; then
      echo "   -> kill receiver in peer$i (pid=$pid)"
      kill "$pid" 2>/dev/null || true
      RECV_PIDS[$i]=
    fi
  done

  echo "== Churn: selected peers offline for ${CHURN_DOWN}s =="
  sleep "$CHURN_DOWN"

  echo "== Rejoin: restart receivers in churned peers ${CHURN_PEERS[*]} =="
  for i in "${CHURN_PEERS[@]}"; do
    RLOG="$LOGDIR/peer${i}-recv.jsonl"
    RERR="$LOGDIR/peer${i}-recv.stderr"
    RSUM="$LOGDIR/peer${i}-summary.json"

    echo "   -> restart receiver in peer$i"
    run_in_ns "$i" "$BIN" \
      --role receiver \
      --log "$RLOG" \
      --idle-report-ms 3000 \
      --topic-name "$TOPIC" \
      --discovery direct \
      --bootstrap "$NODE_ID" \
      1> "$RSUM" \
      2> "$RERR" &

    RECV_PIDS[$i]=$!
  done
else
  echo "== No churn peers (PEERS <= 1), skipping churn phase =="
fi

#############################################
# 6) WAIT FOR SENDER AND ALL RECEIVERS
#############################################

echo "== Waiting for sender to finish =="
wait "$SENDER_PID" || true

echo "== Waiting for ALL receivers to finish =="
for pid in "${RECV_PIDS[@]}"; do
  if [[ -n "${pid:-}" ]]; then
    wait "$pid" || true
  fi
done

#############################################
# 7) CLEANUP
#############################################

echo "== Clear scenario (reset netem) =="
bash "$ROOT/scripts/scenarios/netem-none.sh" "$BR"

echo "== [netns] cleanup =="
cleanup_netns

echo "== UC5 done. Logs in $LOGDIR =="
