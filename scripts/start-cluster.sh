#!/bin/bash
# Starts every node listed in config/cluster.conf as its own background JVM process.
set -e
cd "$(dirname "$0")/.."
mkdir -p logs
rm -f pids.txt
CONF="config/cluster.conf"

while IFS=',' read -r id host raftPort clientPort; do
  [[ "$id" =~ ^#.*$ || -z "$id" ]] && continue
  echo "Starting node $id (raftPort=$raftPort, clientPort=$clientPort)..."
  nohup java -cp out com.raftkv.Server "$id" "$CONF" > "logs/node$id.log" 2>&1 &
  echo "$id:$!" >> pids.txt
done < "$CONF"

echo "Cluster started. PIDs recorded in pids.txt"
cat pids.txt
