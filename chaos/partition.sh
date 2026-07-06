#!/usr/bin/env bash
# Isolate the leader from the cluster network; assert the majority (other 4)
# elects a new leader and stays available, the isolated node cannot serve, and
# the cluster converges after healing.
set -euo pipefail
CLIENT=chaos-part-$$   # fresh session per run so the write isn't deduped
VAL="v$$"              # unique value so we prove THIS run's write survived
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

echo "== partition chaos (isolate the leader) =="
L=$(wait_leader) || { echo "FAIL: no leader"; exit 1; }
echo "leader = raft$L; disconnecting it from $NET"
docker network disconnect "$NET" "raftkv5-raft$L-1"

NEW=$(wait_leader "$L") || { echo "FAIL: majority did not elect a new leader"; exit 1; }
echo "majority elected raft$NEW; writing through it"
put "$NEW" part "$VAL" 1
[[ "$(get "$NEW" part)" == *"$VAL"* ]] || { echo "FAIL: majority not writable"; exit 1; }

old=$(curl -s -m 3 -o /dev/null -w '%{http_code}' "http://localhost:$(port "$L")/kv/part" || echo timeout)
echo "isolated raft$L read status = $old (expect NOT 200 — no quorum, so no stale read)"
[ "$old" != "200" ] || { echo "FAIL: isolated node served a read"; exit 1; }

echo "healing partition ..."
docker network connect "$NET" "raftkv5-raft$L-1"

# The committed write must survive the heal (Leader Completeness). Retry through
# the post-heal election churn until a leader serves the value.
v=""
for i in $(seq 1 20); do
  F=$(find_leader) || true
  if [ -n "$F" ]; then v=$(get "$F" part 2>/dev/null || true); [[ "$v" == *"$VAL"* ]] && break; fi
  sleep 1
done
[[ "$v" == *"$VAL"* ]] || { echo "FAIL: committed value lost after heal (got '$v')"; exit 1; }
echo "after heal: leader = raft$F, part = $v (committed write survived)"
echo "PASS: minority unavailable, majority available, committed data survived heal"
