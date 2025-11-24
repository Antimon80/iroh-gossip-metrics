#!/usr/bin/env bash
set -euo pipefail

PEERS="${PEERS:-20}"
SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then SUDO="sudo"; fi

echo "Applying netem loss+delay to peer2..peer$PEERS (keeping peer1 clean)…"

for i in $(seq 2 "$PEERS"); do
  DEV="veth-peer$i"
  $SUDO tc qdisc del dev "$DEV" root 2>/dev/null || true
  $SUDO tc qdisc add dev "$DEV" root netem loss 30% delay 50ms 5ms distribution normal
done
