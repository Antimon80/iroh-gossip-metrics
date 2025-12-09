#!/usr/bin/env bash
set -euo pipefail

# UC5: Churn and Recovery
#
# Direct discovery, no netem. Bootstrap set scales with peer count (1 per 10 peers, ceil).
# Additionally: after CHURN_START seconds, a fixed random subset of non-bootstrap peers
# (from 2..PEERS) is killed ("churned") and rejoins after CHURN_DOWN seconds.

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
CHURN_START="${CHURN_START:-40}"                # seconds until churn starts
CHURN_DOWN="${CHURN_DOWN:-10}"                  # seconds peers stay offline
CHURN_COUNT="${CHURN_COUNT:-0}"                 # number of non-bootstrap peers to churn (0 = choose default)
CHURN_PCT="${CHURN_PCT:-0}"                     # percentage of peers to churn (relative to total PEERS), overrides default count when >0

# group logs by peer count and churn pct
BASELOG="${LOGDIR:-logs/uc5}/p${PEERS}/c${CHURN_PCT}"

# Create parameter-tagged run directory
TS=$(date +"%Y%m%d-%H%M%S")
TAG="uc5_p${PEERS}_c${CHURN_PCT}_m${NUM}_r${RATE}_s${SIZE}_${DISCOVERY}"
RUN_ID="run-${TS}_${TAG}"

LOGDIR="$BASELOG/$RUN_ID"
BIN="$ROOT/target/release/iroh-gossip-metrics"

mkdir -p "$LOGDIR"

echo "== UC5 Churn & Recovery with $PEERS peers (Direct discovery) =="
echo "SCENARIO=$SCENARIO NUM=$NUM RATE=$RATE SIZE=$SIZE TOPIC=$TOPIC"
echo "CHURN_START=${CHURN_START}s CHURN_DOWN=${CHURN_DOWN}s CHURN_COUNT=${CHURN_COUNT:-auto}"
echo "CHURN_PCT=${CHURN_PCT}%"
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
# 0) CHOOSE BOOTSTRAP PEERS
#############################################

declare -a RECV_PIDS
declare -a BOOT_NODE_IDS

# Number of bootstrap peers: 1 per 10 peers (ceil)
BOOTSTRAP_COUNT=$(((PEERS + 9) / 10))

# Always include peer1 as bootstrap for determinism
declare -a BOOTSTRAP_PEERS
BOOTSTRAP_PEERS=(1)

if (( BOOTSTRAP_COUNT > 1 )); then
  EXTRA=$((BOOTSTRAP_COUNT - 1))
  mapfile -t EXTRA_PEERS < <(seq 2 "$PEERS" | shuf -n "$EXTRA")
  BOOTSTRAP_PEERS+=("${EXTRA_PEERS[@]}")
fi

echo "== Bootstrap peers (count=$BOOTSTRAP_COUNT): ${BOOTSTRAP_PEERS[*]} =="

#############################################
# 1) START BOOTSTRAP RECEIVERS
#############################################

echo "== Start bootstrap receivers in peers ${BOOTSTRAP_PEERS[*]} =="

for p in "${BOOTSTRAP_PEERS[@]}"; do
  RLOG="$LOGDIR/peer${p}-recv.jsonl"
  RERR="$LOGDIR/peer${p}-recv.stderr"
  RSUM="$LOGDIR/peer${p}-summary.json"

  : > "$RERR"

  run_in_ns "$p" "$BIN" \
    --role receiver \
    --log "$RLOG" \
    --idle-report-ms 3000 \
    --num "$NUM" \
    --rate "$RATE" \
    --topic-name "$TOPIC" \
    --churn-pct "$CHURN_PCT" \
    --discovery direct \
    1> "$RSUM" \
    2> "$RERR" &

  RECV_PIDS[$p]=$!
done

#############################################
# 2) EXTRACT NODE_IDs OF ALL BOOTSTRAP PEERS
#############################################

echo "== Waiting for bootstrap node_ids =="

idx=0
for p in "${BOOTSTRAP_PEERS[@]}"; do
  RERR="$LOGDIR/peer${p}-recv.stderr"
  NODE_ID=""

  echo "  - waiting for node_id from peer$p ..."
  for _ in {1..80}; do
    if grep -q "node_id=" "$RERR"; then
      NODE_ID="$(grep -m1 'node_id=' "$RERR" | sed -E 's/.*node_id=([[:alnum:]]+).*/\1/')"
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

#############################################
# 3) START REMAINING RECEIVERS (PEER 2..PEERS)
#############################################

if (( PEERS > 1 )); then
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
      --idle-report-ms 3000 \
      --num "$NUM" \
      --rate "$RATE" \
      --topic-name "$TOPIC" \
      --churn-pct "$CHURN_PCT" \
      --discovery direct \
      --bootstrap "$BOOTSTRAP_LIST" \
      1> "$RSUM" \
      2> "$RERR" &

    RECV_PIDS[$i]=$!
  done
fi

#############################################
# 4) START SENDER IN PEER 1 (BACKGROUND)
#############################################

SLOG="$LOGDIR/send.jsonl"

echo "== Start sender in peer1 (bootstraps=$BOOTSTRAP_LIST) in background =="
run_in_ns 1 "$BIN" \
  --role sender \
  --log "$SLOG" \
  --num "$NUM" \
  --rate "$RATE" \
  --size "$SIZE" \
  --topic-name "$TOPIC" \
  --churn-pct "$CHURN_PCT" \
  --discovery direct \
  --bootstrap "$BOOTSTRAP_LIST" &
SENDER_PID=$!

#############################################
# 5) CHURN PHASE: DISCONNECT + REJOIN OF RANDOM SUBSET OF PEERS 2..PEERS
#############################################

if (( PEERS > 1 )); then
  # Build churn pool from non-bootstrap peers (excluding peer1 and any extra bootstraps)
  CHURN_POOL=()
  for i in $(seq 2 "$PEERS"); do
    is_bootstrap=0
    for bp in "${BOOTSTRAP_PEERS[@]}"; do
      if [[ "$bp" -eq "$i" ]]; then
        is_bootstrap=1
        break
      fi
    done
    if (( is_bootstrap == 0 )); then
      CHURN_POOL+=("$i")
    fi
  done

  NON_BOOTSTRAP=${#CHURN_POOL[@]}
  if (( NON_BOOTSTRAP == 0 )); then
    echo "== No non-bootstrap peers available for churn, skipping churn phase =="
  else
    if (( CHURN_COUNT <= 0 )) && (( CHURN_PCT > 0 )); then
      CHURN_COUNT=$(((PEERS * CHURN_PCT + 99) / 100)) # ceil(PEERS * pct / 100)
    fi

    # Determine how many peers to churn if not explicitly set
    # Default: half of the non-bootstrap peers (rounded up), but at least 1
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

    # Randomly select CHURN_COUNT distinct peers from churn pool
    # This subset remains fixed for kill + rejoin.
    mapfile -t CHURN_PEERS < <(printf "%s\n" "${CHURN_POOL[@]}" | shuf -n "$CHURN_COUNT" | sort -n)

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
        --num "$NUM" \
      --rate "$RATE" \
      --topic-name "$TOPIC" \
      --churn-pct "$CHURN_PCT" \
      --discovery direct \
      --bootstrap "$BOOTSTRAP_LIST" \
      1> "$RSUM" \
        2> "$RERR" &

      RECV_PIDS[$i]=$!
    done
  fi
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
