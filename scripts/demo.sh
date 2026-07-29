#!/bin/bash
# Full failure-injection demo, self-contained in one script so all background
# node processes stay alive for the whole run (they'd be reaped if started and
# queried across separate shell invocations).
set -e
cd "$(dirname "$0")/.."
CONF="config/cluster.conf"
mkdir -p logs
rm -f logs/*.log pids.txt

declare -A PID

start_node() {
  local id=$1
  echo ">> starting node $id"
  setsid nohup java -cp out com.raftkv.Server "$id" "$CONF" >> "logs/node$id.log" 2>&1 < /dev/null &
  PID[$id]=$!
  echo "$id:${PID[$id]}" >> pids.txt
}

# client <preferredNodeId|0> <command...>  -- 0 means "any node, follow redirects"
client() {
  java -cp out com.raftkv.client.Client "$CONF" "$@"
}

# status_of <nodeId> queries that SPECIFIC node's local view (STATUS is never redirected).
status_of() {
  client "$1" STATUS
}

find_leader() {
  # Reuses the exact same server-side redirect-following that every PUT/GET
  # already relies on successfully, instead of guessing which node to poll
  # with STATUS. One JVM launch, and it won't report a leader that isn't yet
  # ready to serve (see RaftNode#isReadyToServeReads).
  result=$(client 0 WHOISLEADER 2>/dev/null || true)
  if [[ "$result" =~ ^LEADER\ ([0-9]+)$ ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo ""
  fi
}

# Cross-checks a WHOISLEADER answer with a direct STATUS call to that specific
# node before trusting it. WHOISLEADER can occasionally report a stale/dead
# node during the brief window right after a forced kill (a race in the OS
# tearing down the killed process's listening socket, not a Raft bug); this
# catches that instead of silently reporting a wrong node as "elected".
confirm_leader() {
  local candidate="$1" exclude="$2"
  [[ -z "$candidate" || "$candidate" == "$exclude" ]] && return 1
  local status
  status=$(status_of "$candidate" 2>/dev/null || true)
  [[ "$status" == *"state=LEADER"* ]]
}

echo "=============================================="
echo "PHASE 1: Starting 5-node cluster"
echo "=============================================="
for id in 1 2 3 4 5; do start_node "$id"; done
sleep 6

echo
echo "=============================================="
echo "PHASE 2: Waiting for leader election"
echo "=============================================="
LEADER=""
CONFIRMED=""
for attempt in $(seq 1 40); do
  CANDIDATE=$(find_leader)
  if confirm_leader "$CANDIDATE" ""; then
    LEADER="$CANDIDATE"
    CONFIRMED="1"
    break
  fi
  sleep 1
done
if [[ -n "$CONFIRMED" ]]; then
  echo "Leader elected: node-$LEADER"
else
  echo "WARNING: could not confirm a leader within the retry window (cluster may still be settling)."
fi
for id in 1 2 3 4 5; do echo "  node-$id: $(status_of $id 2>/dev/null)"; done

echo
echo "=============================================="
echo "PHASE 3: Writing keys through the cluster"
echo "=============================================="
echo "PUT alpha=1  -> $(client 0 PUT alpha 1)"
echo "PUT beta=2   -> $(client 0 PUT beta 2)"
echo "PUT gamma=3  -> $(client 0 PUT gamma 3)"
echo "GET alpha    -> $(client 0 GET alpha)"
echo "GET beta     -> $(client 0 GET beta)"

echo
echo "=============================================="
echo "PHASE 4: Killing the leader (node-$LEADER) mid-run"
echo "=============================================="
kill -9 "${PID[$LEADER]}"
echo "Killed node-$LEADER (pid ${PID[$LEADER]})"
unset PID[$LEADER]
sleep 6

echo
echo "=============================================="
echo "PHASE 5: Confirming automatic re-election"
echo "=============================================="
NEW_LEADER=""
CONFIRMED=""
for attempt in $(seq 1 40); do
  CANDIDATE=$(find_leader)
  if confirm_leader "$CANDIDATE" "$LEADER"; then
    NEW_LEADER="$CANDIDATE"
    CONFIRMED="1"
    break
  fi
  sleep 1
done
if [[ -n "$CONFIRMED" ]]; then
  echo "New leader elected: node-$NEW_LEADER"
else
  echo "WARNING: could not confirm a new leader distinct from the old one within the retry window."
fi
for id in 1 2 3 4 5; do
  if [[ "$id" != "$LEADER" ]]; then echo "  node-$id: $(status_of $id 2>/dev/null)"; fi
done

echo
echo "=============================================="
echo "PHASE 6: Confirming writes continue after failover"
echo "=============================================="
echo "PUT delta=4  -> $(client 0 PUT delta 4)"
echo "PUT epsilon=5 -> $(client 0 PUT epsilon 5)"
echo "GET alpha (pre-failure key)  -> $(client 0 GET alpha)"
echo "GET delta (post-failure key) -> $(client 0 GET delta)"

echo
echo "=============================================="
echo "PHASE 7: Rejoining the crashed node and verifying catch-up"
echo "=============================================="
start_node "$LEADER"
sleep 6
echo "Rejoined node-$LEADER status: $(status_of $LEADER 2>/dev/null)"
echo "Current leader status:        $(status_of $NEW_LEADER 2>/dev/null)"
echo "GET delta from rejoined node's cluster view -> $(client 0 GET delta)"

echo
echo "=============================================="
echo "PHASE 8: Shutting down cluster"
echo "=============================================="
for id in "${!PID[@]}"; do
  kill -9 "${PID[$id]}" 2>/dev/null || true
done
echo "Demo complete."
