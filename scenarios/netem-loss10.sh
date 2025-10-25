#!/usr/bin/env bash
set -euo pipefail

DEV="${1:-eth0}"
SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then SUDO="sudo"; fi

$SUDO tc qdisc replace dev "$DEV" root netem loss 10%
echo "[netem-loss10] loss=10% on $DEV"
$SUDO tc qdisc show dev "$DEV"
