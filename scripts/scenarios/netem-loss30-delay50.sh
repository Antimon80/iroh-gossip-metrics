#!/usr/bin/env bash
set -euo pipefail

DEV="${1:-eth0}"
SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then SUDO="sudo"; fi

$SUDO tc qdisc replace dev "$DEV" root netem loss 30% delay 50ms 5ms distribution normal
echo "[netem-loss30-delay50] loss=30%, delay=50ms±5ms on $DEV"
$SUDO tc qdisc show dev "$DEV"
