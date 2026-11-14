#!/usr/bin/env bash
# Starts the Java oracle (Deephaven 42.9, embedded server from PyPI) and waits until it answers.
#
#   oracle/bin/start-java.sh [port]        default port 10000
#
# Environment:
#   ORACLE_PSK      pre-shared key (default: oxhaven)
#   ORACLE_HEAP     JVM heap (default: 2g)
#   ORACLE_RUN_DIR  where the log and pid file go (default: oracle/.run)
#   ORACLE_WAIT     seconds to wait for health (default: 120)
#
# Prints the server URL on success. Stop it with oracle/bin/stop-java.sh [port].
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oracle="$(dirname "$here")"
port="${1:-10000}"
psk="${ORACLE_PSK:-oxhaven}"
heap="${ORACLE_HEAP:-2g}"
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
  echo "error: no java on PATH and JAVA_HOME unset; Deephaven 42.9 needs a JDK (verified with OpenJDK 21)" >&2
  exit 1
fi

mkdir -p "$run_dir"
if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
  echo "error: a Java oracle is already running on port $port (pid $(cat "$pid_file"))" >&2
  exit 1
fi

nohup "$python" "$here/java_server.py" --port "$port" --psk "$psk" --heap "$heap" \
  >"$log" 2>&1 &
pid=$!
echo "$pid" >"$pid_file"

for ((i = 0; i < wait_s; i++)); do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "error: the Java server exited during startup; last lines of $log:" >&2
    tail -n 30 "$log" >&2
    rm -f "$pid_file"
    exit 1
  fi
  if grep -q '^ORACLE_READY' "$log" &&
    (cd "$oracle" && "$python" -m harness.health "dh+psk://$psk@127.0.0.1:$port" >/dev/null 2>&1); then
    echo "dh+psk://$psk@127.0.0.1:$port"
    exit 0
  fi
  sleep 1
done

echo "error: the Java server was not healthy after ${wait_s}s; last lines of $log:" >&2
tail -n 30 "$log" >&2
kill "$pid" 2>/dev/null || true
rm -f "$pid_file"
exit 1
