#!/bin/bash
# mvn-build.sh - Builds the project using Maven instead of the plain-javac
# scripts/build.sh. Requires Maven (`mvn`) installed and normal internet
# access (Maven Central), neither of which is available in the sandbox this
# project was built in -- this script has NOT been run/verified by Claude.
# If it fails, scripts/build.sh + scripts/demo.sh are the verified fallback.
set -e
cd "$(dirname "$0")/.."
mvn -q package
echo "Build OK -> target/raft-kv-store.jar"
echo "Run a node with:   java -jar target/raft-kv-store.jar <nodeId> config/cluster.conf"
echo "Run other tools with -cp, e.g.:"
echo "  java -cp target/raft-kv-store.jar com.raftkv.client.Client config/cluster.conf 0 STATUS"
