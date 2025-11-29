set -euo pipefail

# UC1: all peers in same LAN, Direct discovery (mDNS)

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
source "$ROOT/scripts/netns/common_netns.sh"

SCENARIO="${SCENARIO:-scripts/scenarios/netem-none.sh}"
PEERS="$1"
NUM="$2"
RATE="$3"
SIZE="$4"
DISCOVERY="${5:-DIRECT}"

TOPIC="${TOPIC:-lab}"
BASELOG="${LOGDIR:-logs/uc1}"

# Create parameter-tagged run directory
TS=$(date +"%Y%m%d-%H%M%S")
TAG="uc1_p${PEERS}_m${NUM}_r${RATE}_s${SIZE}_${DISCOVERY}"
RUN_ID="run-${TS}_${TAG}"

LOGDIR="$BASELOG/$RUN_ID"
BIN="$ROOT/target/release/iroh-gossip-metrics"

mkdir -p "$LOGDIR"

echo "== UC1 Direct LAN with $PEERS peers =="
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
# 1) BOOTSTRAP RECEIVER (PEER 1)
#############################################

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

#############################################
# 2) EXTRACT bootstrap node_id
#############################################
echo "== Waiting for bootstrap node_id =="
NODE_ID=""
for _ in {1..80}; do
  if grep -q "node_id=" "$BOOT_RERR"; then
    NODE_ID="$(grep -m1 'node_id=' "$BOOT_RERR" | sed -E 's/.*node_id=([[:alnum:]]+).*/\1/')"
    break
  fi
  sleep 0.1
done
if [[ -z "$NODE_ID" ]]; then
  echo "ERROR: Could not detect bootstrap node_id" >&2
  kill "$BOOT_PID" || true
  cleanup_netns
  exit 1
fi
echo "== Bootstrap node_id: $NODE_ID =="

sleep 0.5

#############################################
# 3) START REMAINING RECEIVERS
#############################################
echo "== Start receivers peer2..peer$PEERS =="

RECV_PIDS=("$BOOT_PID")

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
  
  RECV_PIDS+=("$!")
done

sleep 2

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
  --discovery direct \
  --bootstrap "$NODE_ID"

#############################################
# 5) WAIT FOR ALL RECEIVERS
#############################################
echo "== Waiting for ALL receivers to finish =="

for pid in "${RECV_PIDS[@]}"; do
  wait "$pid" || true
done

sleep 1   # short settle time

#############################################
# 6) CLEANUP
#############################################

echo "== Clear scenario =="
bash "$ROOT/scripts/scenarios/netem-none.sh" "$BR"

echo "== [netns] cleanup =="
cleanup_netns

echo "== UC1 done. Logs in $LOGDIR =="