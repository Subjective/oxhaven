#!/usr/bin/env bash
# Starts the Java reference (Deephaven 42.9) with the tables of the IDE acceptance test.
#
#   oracle/ide/bin/start-java-ide.sh [port] [mode] [extra java_ide_server.py arguments]
#
# port defaults to 10500, mode to `scope` (see java_ide_server.py: scope, app, disabled).
# Environment as for oracle/bin/start-java.sh: ORACLE_PSK, ORACLE_HEAP (default 1g),
# ORACLE_RUN_DIR, ORACLE_WAIT. Prints the http URL. Stop with oracle/bin/stop-java.sh <port>.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oracle="$(cd "$here/../.." && pwd)"
port="${1:-10500}"
mode="${2:-scope}"
if (($# > 2)); then shift 2; else shift $#; fi
psk="${ORACLE_PSK:-oxhaven}"
heap="${ORACLE_HEAP:-1g}"
run_dir="${ORACLE_RUN_DIR:-$oracle/.run}"
wait_s="${ORACLE_WAIT:-120}"
python="$oracle/.venv/bin/python"
log="$run_dir/java-$port.log"
pid_file="$run_dir/java-$port.pid"

if [[ ! -x "$python" ]]; then
  echo "error: $python not found; create the venv first (see oracle/README.md)" >&2
  exit 1
fi
if ! command -v java >/dev/null && [[ -z "${JAVA_HOME:-}" ]]; then
  echo "error: no java on PATH and JAVA_HOME unset; Deephaven 42.9 needs a JDK" >&2
  exit 1
fi

mkdir -p "$run_dir"
if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
  echo "error: a Java oracle is already running on port $port (pid $(cat "$pid_file"))" >&2
  exit 1
fi

OXHAVEN_IDE_APP_DIR="$here/../app.d" nohup "$python" "$here/java_ide_server.py" \
  --port "$port" --psk "$psk" --heap "$heap" --mode "$mode" "$@" >"$log" 2>&1 &
pid=$!
echo "$pid" >"$pid_file"

for ((i = 0; i < wait_s; i++)); do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "error: the Java server exited during startup; last lines of $log:" >&2
    tail -n 30 "$log" >&2
    rm -f "$pid_file"
    exit 1
  fi
  if grep -q '^ORACLE_READY' "$log"; then
    echo "http://127.0.0.1:$port"
    exit 0
  fi
  sleep 1
done

echo "error: the Java server was not ready after ${wait_s}s; last lines of $log:" >&2
tail -n 30 "$log" >&2
kill "$pid" 2>/dev/null || true
rm -f "$pid_file"
exit 1
