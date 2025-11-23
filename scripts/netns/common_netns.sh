#!/usr/bin/env bash
set -euo pipefail

# Common helpers for netns-based LAN simulation.

BR="${BR:-br-gossip}"
SUBNET="${SUBNET:-10.10.0.0/24}"
BASE_IP="${BASE_IP:-10.10.0}"     # we will use .11, .12, ...
PEERS="${PEERS:-20}"

# Optional: give namespaces internet access via host NAT.
# UC1: leave unset / 0
# UC2: export NETNS_INTERNET=1 before calling scripts
NETNS_INTERNET="${NETNS_INTERNET:-0}"

# Optional DNS server for namespaces when NETNS_INTERNET=1
NETNS_DNS="${NETNS_DNS:-1.1.1.1}"

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then SUDO="sudo"; fi

ns_name() { echo "peer$1"; }
veth_host() { echo "veth-peer$1"; }
veth_ns() { echo "eth0"; }

ip_for() {
  # peer1 -> 10.10.0.11, peer2 -> .12, ...
  local idx="$1"
  echo "${BASE_IP}.$((10 + idx))"
}

# ---------------- Internet option (only if NETNS_INTERNET=1) ----------------

WAN_IFACE=""

detect_wan_iface() {
  WAN_IFACE="$(ip route get 1.1.1.1 | awk '{for(i=1;i<=NF;i++) if($i=="dev"){print $(i+1); exit}}')"
  if [[ -z "$WAN_IFACE" ]]; then
    echo "ERROR: Could not detect host internet interface." >&2
    return 1
  fi
}

enable_internet_for_netns() {
  [[ "$NETNS_INTERNET" == "1" ]] || return 0

  detect_wan_iface

  echo "== [netns] enable internet for namespaces via host ($WAN_IFACE) =="

  # Allow host to forward packets
  $SUDO sysctl -w net.ipv4.ip_forward=1 >/dev/null

  # NAT: traffic from SUBNET goes out via WAN_IFACE
  $SUDO iptables -t nat -C POSTROUTING -s "$SUBNET" -o "$WAN_IFACE" -j MASQUERADE 2>/dev/null \
    || $SUDO iptables -t nat -A POSTROUTING -s "$SUBNET" -o "$WAN_IFACE" -j MASQUERADE

  # Forwarding rules
  $SUDO iptables -C FORWARD -i "$BR" -o "$WAN_IFACE" -j ACCEPT 2>/dev/null \
    || $SUDO iptables -A FORWARD -i "$BR" -o "$WAN_IFACE" -j ACCEPT

  $SUDO iptables -C FORWARD -i "$WAN_IFACE" -o "$BR" -m state --state ESTABLISHED,RELATED -j ACCEPT 2>/dev/null \
    || $SUDO iptables -A FORWARD -i "$WAN_IFACE" -o "$BR" -m state --state ESTABLISHED,RELATED -j ACCEPT
}

disable_internet_for_netns() {
  [[ "$NETNS_INTERNET" == "1" ]] || return 0

  # If WAN_IFACE wasn't detected yet, try now (best effort)
  if [[ -z "$WAN_IFACE" ]]; then
    detect_wan_iface || return 0
  fi

  echo "== [netns] disable internet for namespaces =="

  $SUDO iptables -t nat -D POSTROUTING -s "$SUBNET" -o "$WAN_IFACE" -j MASQUERADE 2>/dev/null || true
  $SUDO iptables -D FORWARD -i "$BR" -o "$WAN_IFACE" -j ACCEPT 2>/dev/null || true
  $SUDO iptables -D FORWARD -i "$WAN_IFACE" -o "$BR" -m state --state ESTABLISHED,RELATED -j ACCEPT 2>/dev/null || true
}

# --------------------------------------------------------------------------

setup_bridge() {
  echo "== [netns] create bridge $BR ($SUBNET) =="
  $SUDO ip link del "$BR" 2>/dev/null || true
  $SUDO ip link add name "$BR" type bridge
  $SUDO ip addr add "${BASE_IP}.1/24" dev "$BR"
  $SUDO ip link set "$BR" up
}

setup_namespaces() {
  echo "== [netns] create $PEERS namespaces =="
  for i in $(seq 1 "$PEERS"); do
    local ns; ns="$(ns_name "$i")"
    local vh; vh="$(veth_host "$i")"

    $SUDO ip netns del "$ns" 2>/dev/null || true
    $SUDO ip link del "$vh" 2>/dev/null || true

    $SUDO ip netns add "$ns"
    $SUDO ip link add "$vh" type veth peer name "$(veth_ns "$i")" netns "$ns"

    # attach host side to bridge
    $SUDO ip link set "$vh" master "$BR"
    $SUDO ip link set "$vh" up

    # bring up ns side + ip
    local ipaddr; ipaddr="$(ip_for "$i")"
    $SUDO ip netns exec "$ns" ip link set lo up
    $SUDO ip netns exec "$ns" ip link set "$(veth_ns "$i")" up
    $SUDO ip netns exec "$ns" ip addr add "$ipaddr/24" dev "$(veth_ns "$i")"

    # default route via bridge
    $SUDO ip netns exec "$ns" ip route add default via "${BASE_IP}.1" dev "$(veth_ns "$i")" 2>/dev/null || true

    # only when internet is enabled: set DNS inside namespace
    if [[ "$NETNS_INTERNET" == "1" ]]; then
      $SUDO ip netns exec "$ns" bash -c "echo 'nameserver $NETNS_DNS' > /etc/resolv.conf"
    fi
  done
}

cleanup_netns() {
  echo "== [netns] cleanup =="

  # remove NAT/forwarding if it was enabled
  disable_internet_for_netns || true

  # clear netem on bridge if any
  $SUDO tc qdisc del dev "$BR" root 2>/dev/null || true
  $SUDO tc qdisc del dev "$BR" ingress 2>/dev/null || true

  for i in $(seq 1 "$PEERS"); do
    local ns; ns="$(ns_name "$i")"
    local vh; vh="$(veth_host "$i")"
    $SUDO ip netns del "$ns" 2>/dev/null || true
    $SUDO ip link del "$vh" 2>/dev/null || true
  done

  $SUDO ip link del "$BR" 2>/dev/null || true
}

run_in_ns() {
  local i="$1"; shift
  local ns; ns="$(ns_name "$i")"
  $SUDO ip netns exec "$ns" "$@"
}
