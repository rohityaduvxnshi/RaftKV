#!/usr/bin/env bash
# Kill the current leader; assert the cluster re-elects and stays writable.
set -euo pipefail
CLIENT=chaos-kill
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "== kill-leader chaos =="
L=$(wait_leader) || { echo "FAIL: no leader"; exit 1; }
echo "leader = raft$L"
put "$L" survive v1 1
echo "wrote survive=v1 via raft$L"

echo "killing raft$L ..."
"${COMPOSE[@]}" kill "raft$L" >/dev/null

NEW=$(wait_leader "$L") || { echo "FAIL: no new leader after kill"; exit 1; }
echo "new leader = raft$NEW (failover)"
put "$NEW" survive v2 2
val=$(get "$NEW" survive)
echo "cluster still writable; read survive=$val"
[[ "$val" == *'"v2"'* ]] || { echo "FAIL: unexpected value '$val'"; exit 1; }

echo "restarting raft$L ..."
"${COMPOSE[@]}" start "raft$L" >/dev/null
echo "PASS: leader failover survived; cluster stayed available"
