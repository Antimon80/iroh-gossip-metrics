#!/usr/bin/env bash
set -euo pipefail

DEV="${1:-eth0}"
SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then SUDO="sudo"; fi

$SUDO tc qdisc del dev "$DEV" root 2>/dev/null || true
$SUDO tc qdisc del dev "$DEV" ingress 2>/dev/null || true

echo "[netem-none] cleared qdisc on $DEV"
$SUDO tc qdisc show dev "$DEV"
