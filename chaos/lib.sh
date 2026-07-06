# Shared helpers for the chaos scripts. Assumes the 5-node compose cluster is up
# (deploy/docker-compose.5node.yml) with node HTTP APIs on host ports 8080..8084.
#
# Leader/partition chaos here uses Docker primitives (kill, network disconnect),
# so it runs anywhere Docker does — no Linux tc/netem/iptables. Latency injection
# (Linux-only) is out of scope for these scripts; see CLAUDE.md §4/§7.
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE=(docker compose -f "$DIR/../deploy/docker-compose.5node.yml")
NET=raftkv5_default
port() { echo $((8080 + $1)); }

# find_leader echoes the leader's node id (0-4), or nothing. The leader serves
# reads (200/404); followers redirect (307); a down/partitioned node times out.
find_leader() {
  local id code
  for id in 0 1 2 3 4; do
    code=$(curl -s -m 3 -o /dev/null -w '%{http_code}' "http://localhost:$(port "$id")/kv/_probe" || true)
    case "$code" in 200 | 404) echo "$id"; return ;; esac
  done
}

# wait_leader [exclude_id]: up to 30s for a leader (optionally != exclude); echoes id.
wait_leader() {
  local exclude="${1:-}" i l
  for i in $(seq 1 30); do
    l=$(find_leader)
    if [ -n "$l" ] && [ "$l" != "$exclude" ]; then echo "$l"; return 0; fi
    sleep 1
  done
  return 1
}

# CLIENT scopes the exactly-once session; each script sets its own so their
# sequence numbers don't dedup against each other.
CLIENT="${CLIENT:-chaos}"
put() { curl -fs -m 5 -X PUT "http://localhost:$(port "$1")/kv/$2" -d "$3" -H "X-Client-Id: $CLIENT" -H "X-Seq-No: $4" >/dev/null; }
get() { curl -fs -m 5 "http://localhost:$(port "$1")/kv/$2"; }
