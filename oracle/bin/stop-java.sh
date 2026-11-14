#!/usr/bin/env bash
# Stops the Java oracle started by start-java.sh.
#
#   oracle/bin/stop-java.sh [port]         default port 10000
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oracle="$(dirname "$here")"
port="${1:-10000}"
run_dir="${ORACLE_RUN_DIR:-$oracle/.run}"
pid_file="$run_dir/java-$port.pid"

if [[ ! -f "$pid_file" ]]; then
  echo "no Java oracle recorded for port $port"
  exit 0
fi
pid="$(cat "$pid_file")"
if kill -0 "$pid" 2>/dev/null; then
  kill "$pid"
  for _ in $(seq 20); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid"
  fi
fi
rm -f "$pid_file"
echo "stopped Java oracle on port $port"
