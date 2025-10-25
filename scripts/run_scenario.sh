#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/run_scenario.sh <scenario> <transport> [num] [rate] [size]
# Examples:
#   scripts/run_scenario.sh scenarios/netem-none.sh udp 2000 50 256
#   scripts/run_scenario.sh scenarios/netem-loss10.sh gossip 2000 50 256

SCENARIO="${1:-scenarios/netem-none.sh}"
TRANSPORT="${2:-udp}"   # udp | gossip
NUM="${3:-2000}"
RATE="${4:-50}"
SIZE="${5:-256}"

IFACE="lo"              # lokal über Loopback testen
LOGDIR="logs"
TOPIC="${TOPIC:-lab}"   # gemeinsamer Topic-Name für gossip
mkdir -p "$LOGDIR"

echo "== Build =="
if command -v cargo >/dev/null 2>&1; then
  cargo build --release
elif [[ ! -x ./target/release/iroh-gossip-metrics ]]; then
  echo "Fehler: cargo fehlt und Binary ./target/release/iroh-gossip-metrics existiert nicht." >&2
  exit 1
fi

echo "== Apply scenario: $SCENARIO on $IFACE =="
bash "$SCENARIO" "$IFACE"

RLOG="$LOGDIR/${TRANSPORT}-recv.jsonl"
SLOG="$LOGDIR/${TRANSPORT}-send.jsonl"
RERR="$LOGDIR/${TRANSPORT}-recv.stderr"
: > "$RERR"

echo "== Start receiver =="
if [[ "$TRANSPORT" == "gossip" ]]; then
  ./target/release/iroh-gossip-metrics \
    --transport "$TRANSPORT" \
    --role receiver \
    --log "$RLOG" \
    --udp-bind 127.0.0.1:4001 \
    --idle-report-ms 12000 \
    --topic-name "$TOPIC" \
    2> "$RERR" &
  RECV_PID=$!

  echo "== Wait for gossip node_id from receiver =="
  NODE_ID=""
  for _ in {1..50}; do
    if grep -q "node_id=" "$RERR"; then
      NODE_ID="$(grep -m1 'node_id=' "$RERR" | sed -E 's/.*node_id=([[:alnum:]]+).*/\1/')"
      break
    fi
    sleep 0.1
  done

  if [[ -z "$NODE_ID" ]]; then
    echo "ERROR: konnte node_id des Receivers nicht erkennen (siehe $RERR)" >&2
    kill "$RECV_PID" || true
    bash scenarios/netem-none.sh "$IFACE"
    exit 1
  fi
  echo "== Receiver node_id: $NODE_ID =="
else
  ./target/release/iroh-gossip-metrics \
    --transport "$TRANSPORT" \
    --role receiver \
    --log "$RLOG" \
    --udp-bind 127.0.0.1:4001 \
    --idle-report-ms 3000 \
    2> "$RERR" &
  RECV_PID=$!
fi

sleep 0.5  # Receiver bereit werden lassen

echo "== Start sender =="
if [[ "$TRANSPORT" == "gossip" ]]; then
  ./target/release/iroh-gossip-metrics \
    --transport "$TRANSPORT" \
    --role sender \
    --log "$SLOG" \
    --num "$NUM" \
    --rate "$RATE" \
    --size "$SIZE" \
    --udp-bind 127.0.0.1:4000 \
    --udp-peers 127.0.0.1:4001 \
    --topic-name "$TOPIC" \
    --bootstrap "$NODE_ID"
else
  ./target/release/iroh-gossip-metrics \
    --transport "$TRANSPORT" \
    --role sender \
    --log "$SLOG" \
    --num "$NUM" \
    --rate "$RATE" \
    --size "$SIZE" \
    --udp-bind 127.0.0.1:4000 \
    --udp-peers 127.0.0.1:4001
fi

echo "== Wait for receiver to finish (idle timeout) =="
wait "$RECV_PID" || true

echo "== Clear scenario =="
bash scenarios/netem-none.sh "$IFACE"

echo "== Done. Logs in: $LOGDIR =="

