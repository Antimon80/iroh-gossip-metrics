#!/usr/bin/env bash
set -euo pipefail

# Apply *only delay* (no packet loss) to veth-peer2..veth-peer$PEERS

PEERS="${PEERS:-20}"
DELAY="${DELAY:-10ms}"

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  SUDO="sudo"
fi

echo "Applying netem delay=$DELAY ms (no loss) to peer2..peer$PEERS (keeping peer1 clean)…"

for i in $(seq 2 "$PEERS"); do
  DEV="veth-peer$i"

  # Remove any existing qdisc (ignore errors if none present)
  $SUDO tc qdisc del dev "$DEV" root 2>/dev/null || true

  # Add constant delay
  $SUDO tc qdisc add dev "$DEV" root netem delay "$DELAY"
done
