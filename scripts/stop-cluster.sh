#!/bin/bash
cd "$(dirname "$0")/.."
if [[ ! -f pids.txt ]]; then
  echo "No pids.txt found, nothing to stop."
  exit 0
fi
while IFS=':' read -r id pid; do
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" 2>/dev/null
    echo "Killed node $id (pid $pid)"
  fi
done < pids.txt
rm -f pids.txt
