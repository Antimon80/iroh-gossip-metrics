#!/usr/bin/env bash
set -euo pipefail

# UC10: Relay-assisted discovery, degraded network (delay/loss)
# same as UC8 but with netem impairments injected on the bridge.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
source "$ROOT/scripts/netns/common_netns.sh"
export NETNS_INTERNET=1

# Default: degraded scenario
SCENARIO="${SCENARIO:-scripts/scenarios/netem-loss30-delay50.sh}"
PEERS="$1"
NUM="$2"
RATE="$3"
SIZE="$4"
DISCOVERY="${5:-RELAY}"

TOPIC="${TOPIC:-lab}"

# group logs by peer count
BASELOG="${LOGDIR:-logs/uc10}/p${PEERS}"

# Create parameter-tagged run directory
TS=$(date +"%Y%m%d-%H%M%S")
TAG="uc10_p${PEERS}_m${NUM}_r${RATE}_s${SIZE}_${DISCOVERY}"
RUN_ID="run-${TS}_${TAG}"

LOGDIR="$BASELOG/$RUN_ID"
BIN="$ROOT/target/release/iroh-gossip-metrics"

mkdir -p "$LOGDIR"

echo "== UC10 Relay Degraded with $PEERS peers =="
echo "SCENARIO=$SCENARIO NUM=$NUM RATE=$RATE SIZE=$SIZE TOPIC=$TOPIC"
echo "LOGDIR=$LOGDIR"
echo

cleanup_netns || true
setup_bridge
setup_namespaces
enable_internet_for_netns

echo "== Build project =="
cargo build --release

echo "== Apply scenario on bridge $BR =="
export PEERS
bash "$ROOT/$SCENARIO" "$BR"

#############################################
# 0) CHOOSE BOOTSTRAP PEERS
#############################################

# Number of bootstrap peers: 1 per 10 peers (ceil)
BOOTSTRAP_COUNT=$(( (PEERS + 9) / 10 ))

# Always include peer1 as bootstrap for determinism
declare -a BOOTSTRAP_PEERS
BOOTSTRAP_PEERS=(1)

if (( BOOTSTRAP_COUNT > 1 )); then
  EXTRA=$(( BOOTSTRAP_COUNT - 1 ))
  # Pick EXTRA distinct peers from 2..PEERS at random
  mapfile -t EXTRA_PEERS < <(seq 2 "$PEERS" | shuf -n "$EXTRA")
  BOOTSTRAP_PEERS+=("${EXTRA_PEERS[@]}")
fi

echo "== Bootstrap peers (count=$BOOTSTRAP_COUNT): ${BOOTSTRAP_PEERS[*]} =="

#############################################
# 1) START BOOTSTRAP RECEIVERS
#############################################

declare -a RECV_PIDS
declare -a BOOT_NODE_IDS

echo "== Start bootstrap receivers in peers ${BOOTSTRAP_PEERS[*]} =="

for p in "${BOOTSTRAP_PEERS[@]}"; do
  BOOT_RLOG="$LOGDIR/peer${p}-recv.jsonl"
  BOOT_RERR="$LOGDIR/peer${p}-recv.stderr"
  BOOT_SUM="$LOGDIR/peer${p}-summary.json"

  : > "$BOOT_RERR"

  echo "== Start bootstrap receiver in peer$p =="
  run_in_ns "$p" "$BIN" \
    --role receiver \
    --log "$BOOT_RLOG" \
    --idle-report-ms 12000 \
    --topic-name "$TOPIC" \
    --num "$NUM" \
    --rate "$RATE" \
    --discovery relay \
    1> "$BOOT_SUM" \
    2> "$BOOT_RERR" &

  RECV_PIDS+=("$!")
done

#############################################
# 2) EXTRACT NODE_IDs OF ALL BOOTSTRAP PEERS
#############################################
echo "== Waiting for bootstrap node_ids =="

idx=0
for p in "${BOOTSTRAP_PEERS[@]}"; do
  BOOT_RERR="$LOGDIR/peer${p}-recv.stderr"
  NODE_ID=""

  echo "  - waiting for node_id from peer$p ..."
  for _ in {1..80}; do
    if grep -q "node_id=" "$BOOT_RERR"; then
      NODE_ID="$(grep -m1 'node_id=' "$BOOT_RERR" | sed -E 's/.*node_id=([[:alnum:]]+).*/\1/')"
      break
    fi
    sleep 0.25
  done

  if [[ -z "$NODE_ID" ]]; then
    echo "ERROR: Could not detect bootstrap node_id for peer$p" >&2
    for pid in "${RECV_PIDS[@]}"; do
      kill "$pid" 2>/dev/null || true
    done
    cleanup_netns
    exit 1
  fi

  BOOT_NODE_IDS[$idx]="$NODE_ID"
  echo "    peer$p node_id: $NODE_ID"
  idx=$((idx + 1))
done

# Build single comma-separated list for --bootstrap "<id1>,<id2>,<id3>"
BOOTSTRAP_LIST=""
for nid in "${BOOT_NODE_IDS[@]}"; do
  if [[ -z "$BOOTSTRAP_LIST" ]]; then
    BOOTSTRAP_LIST="$nid"
  else
    BOOTSTRAP_LIST+=",${nid}"
  fi
done

echo "== Bootstrap list: $BOOTSTRAP_LIST =="

sleep 0.5

#############################################
# 3) START REMAINING RECEIVERS
#############################################
echo "== Start receivers peer1..peer$PEERS (excluding bootstraps) =="

for i in $(seq 1 "$PEERS"); do
  # Skip peers that are already started as bootstraps
  is_bootstrap=0
  for bp in "${BOOTSTRAP_PEERS[@]}"; do
    if [[ "$bp" -eq "$i" ]]; then
      is_bootstrap=1
      break
    fi
  done
  if [[ "$is_bootstrap" -eq 1 ]]; then
    continue
  fi

  RLOG="$LOGDIR/peer${i}-recv.jsonl"
  RERR="$LOGDIR/peer${i}-recv.stderr"
  RSUM="$LOGDIR/peer${i}-summary.json"

  run_in_ns "$i" "$BIN" \
    --role receiver \
    --log "$RLOG" \
    --idle-report-ms 12000 \
    --topic-name "$TOPIC" \
    --num "$NUM" \
    --rate "$RATE" \
    --discovery relay \
    --bootstrap "$BOOTSTRAP_LIST" \
    1> "$RSUM" \
    2> "$RERR" &

  RECV_PIDS+=("$!")
done

sleep 3   # relay discovery may take a while

#############################################
# 4) START SENDER IN PEER 1
#############################################
SLOG="$LOGDIR/send.jsonl"

echo "== Start sender in peer1 =="
run_in_ns 1 "$BIN" \
  --role sender \
  --log "$SLOG" \
  --num "$NUM" \
  --rate "$RATE" \
  --size "$SIZE" \
  --topic-name "$TOPIC" \
  --discovery relay \
  --bootstrap "$BOOTSTRAP_LIST"

#############################################
# 5) WAIT FOR ALL RECEIVERS
#############################################
echo "== Waiting for ALL receivers to finish =="

for pid in "${RECV_PIDS[@]}"; do
  wait "$pid" || true
done

#############################################
# 6) CLEANUP
#############################################
echo "== Clear scenario =="
bash "$ROOT/scripts/scenarios/netem-none.sh" "$BR"

echo "== [netns] cleanup =="
cleanup_netns

echo "== UC10 done. Logs in $LOGDIR =="
