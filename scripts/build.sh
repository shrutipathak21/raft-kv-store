#!/bin/bash
set -e
cd "$(dirname "$0")/.."
rm -rf out
mkdir -p out
find src -name "*.java" > sources.txt
javac -d out -encoding UTF-8 @sources.txt
echo "Build OK -> out/"
