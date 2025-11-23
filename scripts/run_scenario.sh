#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/run_scenario.sh <scenario> <discovery> [num] [rate] [size]
#
# Examples:
#   scripts/run_scenario.sh scenarios/netem-none.sh direct 2000 50 256
#   scripts/run_scenario.sh scenarios/netem-loss10.sh relay  2000 50 256
#
# Optional environment variables:
#   TOPIC=lab                # Topic name (default: lab)
#   BOOTSTRAPS=<id1,id2>     # Bootstrap Node IDs (overrides auto-bootstrap)
#   IFACE=lo                 # Network interface for netem scenarios (default: lo)
#
# Description:
#   This script automates an iroh-gossip experiment run.
#   It builds the binary, applies a network scenario (e.g., delay/loss via netem),
#   launches a receiver node and a sender node, waits for completion, and resets the network state.
#   Both discovery modes are supported:
#     - direct: peers connect directly using bootstrap node IDs
#     - relay: peers discover each other through a relay server
#
#   The receiver's node_id is automatically extracted from stderr and passed
#   as a bootstrap parameter to the sender (unless BOOTSTRAPS is manually set).
#   Logs for both nodes are stored in the "logs" directory.

SCENARIO="${1:-scenarios/netem-none.sh}"
DISCOVERY="${2:-direct}"        # discovery mode: direct | relay
NUM="${3:-2000}"                # total number of messages to send
RATE="${4:-50}"                 # send rate (messages per second)
SIZE="${5:-256}"                # payload size in bytes

IFACE="${IFACE:-lo}"
TOPIC="${TOPIC:-lab}"
LOGDIR="logs"
mkdir -p "$LOGDIR"

if [[ "$DISCOVERY" != "direct" && "$DISCOVERY" != "relay" ]]; then
  echo "Error: discovery must be 'direct' or 'relay'." >&2
  exit 2
fi

echo "== Build project =="
if command -v cargo >/dev/null 2>&1; then
  cargo build --release
elif [[ ! -x ./target/release/iroh-gossip-metrics ]]; then
  echo "Error: cargo not found and binary ./target/release/iroh-gossip-metrics does not exist." >&2
  exit 1
fi

echo "== Apply scenario: $SCENARIO on $IFACE =="
bash "$SCENARIO" "$IFACE"

RLOG="$LOGDIR/${DISCOVERY}-recv.jsonl"
SLOG="$LOGDIR/${DISCOVERY}-send.jsonl"
RERR="$LOGDIR/${DISCOVERY}-recv.stderr"
: > "$RERR"

# ==== Start receiver ====
echo "== Start receiver (discovery=$DISCOVERY) =="
./target/release/iroh-gossip-metrics \
  --role receiver \
  --log "$RLOG" \
  --idle-report-ms "$([[ "$DISCOVERY" == "relay" ]] && echo 12000 || echo 3000)" \
  --topic-name "$TOPIC" \
  --discovery "$DISCOVERY" \
  2> "$RERR" &
RECV_PID=$!

BOOTSTRAPS="${BOOTSTRAPS:-}"

# ==== Auto-bootstrap: extract receiver node_id from stderr ====
if [[ -z "$BOOTSTRAPS" ]]; then
  echo "== Waiting for receiver node_id =="
  NODE_ID=""
  for _ in {1..60}; do
    if grep -q "node_id=" "$RERR"; then
      NODE_ID="$(grep -m1 'node_id=' "$RERR" | sed -E 's/.*node_id=([[:alnum:]]+).*/\1/')"
      break
    fi
    sleep 0.1
  done
  if [[ -z "$NODE_ID" ]]; then
    echo "ERROR: Could not detect receiver node_id (see $RERR)" >&2
    kill "$RECV_PID" || true
    bash scenarios/netem-none.sh "$IFACE"
    exit 1
  fi
  echo "== Receiver node_id: $NODE_ID =="
  BOOTSTRAPS="$NODE_ID"
else
  echo "== Using BOOTSTRAPS from environment: $BOOTSTRAPS =="
fi

sleep 0.5  # give the receiver time to initialize

# ==== Start sender ====
echo "== Start sender (NUM=$NUM, RATE=$RATE, SIZE=$SIZE) =="
SENDER_CMD=( ./target/release/iroh-gossip-metrics
  --role sender
  --log "$SLOG"
  --num "$NUM"
  --rate "$RATE"
  --size "$SIZE"
  --topic-name "$TOPIC"
  --discovery "$DISCOVERY"
)

# Both modes (direct & relay) use bootstrap information
if [[ -n "$BOOTSTRAPS" ]]; then
  SENDER_CMD+=( --bootstrap "$BOOTSTRAPS" )
fi

echo "Running: ${SENDER_CMD[*]}"
"${SENDER_CMD[@]}"

# ==== Wait for receiver to finish ====
echo "== Waiting for receiver to finish (idle timeout) =="
wait "$RECV_PID" || true

# ==== Reset network conditions ====
echo "== Clear scenario =="
bash scenarios/netem-none.sh "$IFACE"

echo "== Done. Logs written to: $LOGDIR =="
