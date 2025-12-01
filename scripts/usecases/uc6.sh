#!/usr/bin/env bash
set -euo pipefail

# UC6: Churn and Recovery (Relay discovery)
#
# Same basic setup as UC2 (all peers in same LAN, relay-assisted
# discovery via a single bootstrap peer), but with an additional
# churn phase where a subset of peers leave and later rejoin.
#
# By default this uses a clean LAN (netem-none). To emulate a
# degraded relay network, override:
#   SCENARIO=scripts/scenarios/netem-loss30-delay50.sh

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
source "$ROOT/scripts/netns/common_netns.sh"
export NETNS_INTERNET=1

# Network scenario on the bridge (default: no loss/delay)
SCENARIO="${SCENARIO:-scripts/scenarios/netem-none.sh}"

PEERS="$1"
NUM="$2"
RATE="$3"
SIZE="$4"
DISCOVERY="${5:-RELAY}"

TOPIC="${TOPIC:-lab}"

# group logs by peer count
BASELOG="${LOGDIR:-logs/uc6}/p${PEERS}"

# Churn parameters
# Time after sender start until churn begins (seconds)
CHURN_START="${CHURN_START:-5}"
# How long churn peers stay offline before they rejoin (seconds)
CHURN_DOWN="${CHURN_DOWN:-10}"

# Create parameter-tagged run directory
TS=$(date +"%Y%m%d-%H%M%S")
TAG="uc6_p${PEERS}_m${NUM}_r${RATE}_s${SIZE}_${DISCOVERY}"
RUN_ID="run-${TS}_${TAG}"

LOGDIR="$BASELOG/$RUN_ID"
BIN="$ROOT/target/release/iroh-gossip-metrics"

mkdir -p "$LOGDIR"

echo "== UC6 Relay Churn & Recovery with $PEERS peers =="
echo "SCENARIO=$SCENARIO NUM=$NUM RATE=$RATE SIZE=$SIZE TOPIC=$TOPIC"
echo "CHURN_START=${CHURN_START}s CHURN_DOWN=${CHURN_DOWN}s"
echo

cleanup_netns || true
setup_bridge
setup_namespaces
enable_internet_for_netns

echo "== Build project =="
cargo build --release

echo "== Apply scenario on bridge $BR =="
bash "$ROOT/$SCENARIO" "$BR"

#############################################
# 1) BOOTSTRAP RECEIVER (PEER 1, relay discovery)
#############################################

BOOT_RLOG="$LOGDIR/peer1-recv.jsonl"
BOOT_RERR="$LOGDIR/peer1-recv.stderr"
BOOT_SUM="$LOGDIR/peer1-summary.json"

: > "$BOOT_RERR"

echo "== Start bootstrap receiver in peer1 (relay) =="
run_in_ns 1 "$BIN" \
  --role receiver \
  --log "$BOOT_RLOG" \
  --idle-report-ms 12000 \
  --topic-name "$TOPIC" \
  --discovery relay \
  1> "$BOOT_SUM" \
  2> "$BOOT_RERR" &
BOOT_PID=$!

# RECV_PIDS is indexed by peer id: RECV_PIDS[peer_id]=pid
declare -a RECV_PIDS
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
done
if [[ -z "$NODE_ID" ]]; then
  echo "ERROR: Could not detect bootstrap node_id" >&2
  kill "$BOOT_PID" || true
  cleanup_netns
  exit 1
fi
echo "== Bootstrap node_id: $NODE_ID =="

#############################################
# 3) START REMAINING RECEIVERS (PEER 2..PEERS)
#############################################
echo "== Start receivers peer2..peer$PEERS (relay, bootstrap=$NODE_ID) =="

if (( PEERS > 1 )); then
  for i in $(seq 2 "$PEERS"); do
    RLOG="$LOGDIR/peer${i}-recv.jsonl"
    RERR="$LOGDIR/peer${i}-recv.stderr"
    RSUM="$LOGDIR/peer${i}-summary.json"

    run_in_ns "$i" "$BIN" \
      --role receiver \
      --log "$RLOG" \
      --idle-report-ms 12000 \
      --topic-name "$TOPIC" \
      --discovery relay \
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

echo "== Start sender in peer1 (relay, bootstrap=$NODE_ID) in background =="
run_in_ns 1 "$BIN" \
  --role sender \
  --log "$SLOG" \
  --num "$NUM" \
  --rate "$RATE" \
  --size "$SIZE" \
  --topic-name "$TOPIC" \
  --discovery relay \
  --bootstrap "$NODE_ID" &
SENDER_PID=$!

#############################################
# 5) CHURN PHASE: DISCONNECT + REJOIN (PEERS 2..PEERS)
#############################################

if (( PEERS > 1 )); then
  echo "== Churn phase: peers 2..$PEERS =="
  echo "== Waiting ${CHURN_START}s before starting churn =="
  sleep "$CHURN_START"

  echo "== Churn: killing relay receivers in peers 2..$PEERS =="
  for i in $(seq 2 "$PEERS"); do
    pid=${RECV_PIDS[$i]:-}
    if [[ -n "${pid:-}" ]]; then
      echo "   -> kill receiver in peer$i (pid=$pid)"
      kill "$pid" 2>/dev/null || true
      RECV_PIDS[$i]=
    fi
  done

  echo "== Peers offline for ${CHURN_DOWN}s =="
  sleep "$CHURN_DOWN"

  echo "== Rejoin: restart receivers in churned peers 2..$PEERS =="
  for i in $(seq 2 "$PEERS"); do
    RLOG="$LOGDIR/peer${i}-recv.jsonl"
    RERR="$LOGDIR/peer${i}-recv.stderr"
    RSUM="$LOGDIR/peer${i}-summary.json"

    echo "   -> restart receiver in peer$i (relay, bootstrap=$NODE_ID)"
    run_in_ns "$i" "$BIN" \
      --role receiver \
      --log "$RLOG" \
      --idle-report-ms 12000 \
      --topic-name "$TOPIC" \
      --discovery relay \
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

echo "== Clear scenario =="
bash "$ROOT/scripts/scenarios/netem-none.sh" "$BR"

echo "== [netns] cleanup =="
cleanup_netns

echo "== UC6 done. Logs in $LOGDIR =="
