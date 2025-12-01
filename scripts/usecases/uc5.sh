#!/usr/bin/env bash
set -euo pipefail

# UC4: Churn and Recovery
#
# Same basic setup as UC1 (direct LAN, static bootstraps), but with
# an additional churn phase where a subset of peers leave and later
# rejoin the system.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
source "$ROOT/scripts/netns/common_netns.sh"

# Network scenario on the bridge (default: no loss/delay)
SCENARIO="${SCENARIO:-scripts/scenarios/netem-none.sh}"

PEERS="$1"
NUM="$2"
RATE="$3"
SIZE="$4"
DISCOVERY="${5:-DIRECT}"

# Number of dedicated bootstrap peers (always kept alive)
BOOTSTRAP_COUNT="${BOOTSTRAP_COUNT:-5}"

# Churn parameters
# Time after sender start until churn begins (seconds)
CHURN_START="${CHURN_START:-5}"
# How long churn peers stay offline before they rejoin (seconds)
CHURN_DOWN="${CHURN_DOWN:-10}"

TOPIC="${TOPIC:-lab}"

# group logs by peer count
BASELOG="${LOGDIR:-logs/uc5}/p${PEERS}"

# Create parameter-tagged run directory
TS=$(date +"%Y%m%d-%H%M%S")
TAG="uc5_p${PEERS}_m${NUM}_r${RATE}_s${SIZE}_${DISCOVERY}"
RUN_ID="run-${TS}_${TAG}"

LOGDIR="$BASELOG/$RUN_ID"
BIN="$ROOT/target/release/iroh-gossip-metrics"

mkdir -p "$LOGDIR"

echo "== UC5 Churn & Recovery with $PEERS peers (bootstraps=$BOOTSTRAP_COUNT) =="
echo "SCENARIO=$SCENARIO NUM=$NUM RATE=$RATE SIZE=$SIZE TOPIC=$TOPIC"
echo "CHURN_START=${CHURN_START}s CHURN_DOWN=${CHURN_DOWN}s"
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

# RECV_PIDS is indexed by peer id: RECV_PIDS[peer_id]=pid
declare -a RECV_PIDS
BOOTSTRAP_IDS=()

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
  RECV_PIDS[$i]=$pid

  NODE_ID=""
  for _ in {1..80}; do
    if grep -q "node_id=" "$BOOT_RERR"; then
      NODE_ID="$(grep -m1 'node_id=' "$BOOT_RERR" | sed -E 's/.*node_id=([[:alnum:]]+).*/\1/')"
      break
    fi
  done

  if [[ -z "$NODE_ID" ]]; then
    echo "ERROR: Could not detect bootstrap node_id for peer$i" >&2
    # Cleanup and abort on failure
    for p in "${RECV_PIDS[@]}"; do
      kill "$p" 2>/dev/null || true
    done
    cleanup_netns
    exit 1
  fi

  BOOTSTRAP_IDS+=("$NODE_ID")
done

# Build comma-separated bootstrap list for CLI (e.g. "id1,id2,id3,id4,id5")
BOOTSTRAP_CSV="$(IFS=,; echo "${BOOTSTRAP_IDS[*]}")"
echo "== Using bootstrap list: $BOOTSTRAP_CSV =="

#############################################
# 2) START REMAINING RECEIVERS (PEER BOOTSTRAP_COUNT+1 .. PEERS)
#############################################

if (( PEERS > BOOTSTRAP_COUNT )); then
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

    RECV_PIDS[$i]=$!
  done
fi

#############################################
# 3) START SENDER IN PEER 1 (FOREGROUND WORKLOAD)
#############################################
SLOG="$LOGDIR/send.jsonl"

echo "== Start sender in peer1 (bootstraps=$BOOTSTRAP_CSV) in background =="
run_in_ns 1 "$BIN" \
  --role sender \
  --log "$SLOG" \
  --num "$NUM" \
  --rate "$RATE" \
  --size "$SIZE" \
  --topic-name "$TOPIC" \
  --discovery direct \
  --bootstrap "$BOOTSTRAP_CSV" &
SENDER_PID=$!

#############################################
# 4) CHURN PHASE: DISCONNECT + REJOIN OF PEERS
#############################################

# We churn only non-bootstrap peers (keep bootstrap peers stable).
if (( PEERS > BOOTSTRAP_COUNT )); then
  echo "== Churn phase: peers $((BOOTSTRAP_COUNT+1))..$PEERS =="
  echo "== Waiting ${CHURN_START}s before starting churn =="
  sleep "$CHURN_START"

  echo "== Churn: killing non-bootstrap receivers =="
  for i in $(seq $((BOOTSTRAP_COUNT + 1)) "$PEERS"); do
    pid=${RECV_PIDS[$i]:-}
    if [[ -n "${pid:-}" ]]; then
      echo "   -> kill receiver in peer$i (pid=$pid)"
      kill "$pid" 2>/dev/null || true
      RECV_PIDS[$i]=
    fi
  done

  echo "== Peers offline for ${CHURN_DOWN}s =="
  sleep "$CHURN_DOWN"

  echo "== Rejoin: restart receivers in churned peers =="
  for i in $(seq $((BOOTSTRAP_COUNT + 1)) "$PEERS"); do
    RLOG="$LOGDIR/peer${i}-recv.jsonl"
    RERR="$LOGDIR/peer${i}-recv.stderr"
    RSUM="$LOGDIR/peer${i}-summary.json"

    # Overwrite previous summary for this peer; logs start fresh after rejoin.
    echo "   -> restart receiver in peer$i"
    run_in_ns "$i" "$BIN" \
      --role receiver \
      --log "$RLOG" \
      --idle-report-ms 3000 \
      --topic-name "$TOPIC" \
      --discovery direct \
      --bootstrap "$BOOTSTRAP_CSV" \
      1> "$RSUM" \
      2> "$RERR" &

    RECV_PIDS[$i]=$!
  done
else
  echo "== No churn peers (PEERS <= BOOTSTRAP_COUNT), skipping churn phase =="
fi

#############################################
# 5) WAIT FOR SENDER AND ALL RECEIVERS
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
# 6) CLEANUP
#############################################

echo "== Clear scenario (reset netem) =="
bash "$ROOT/scripts/scenarios/netem-none.sh" "$BR"

echo "== [netns] cleanup =="
cleanup_netns

echo "== UC5 done. Logs in $LOGDIR =="
