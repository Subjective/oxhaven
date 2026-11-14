#!/usr/bin/env bash
# The replay gate: builds Oxhaven, starts it, replays every recorded Java result under
# oracle/corpus and runs every client smoke check against it, stops it, and compares the outcome
# with oracle/replay-baseline.json. Needs no Java server. See oracle/README.md, "The replay gate".
#
#   oracle/bin/ci-replay.sh
#
# Exit status: 0 when the outcome is the baseline's; 1 when it is not; 2 when the server could
# not be built or started, exited during the run, or a scenario or check could not be run.
#
# Environment:
#   ORACLE_GATE_PORT      port for the server; default 0, which lets the system pick a free one
#   ORACLE_GATE_OUT       directory for report.json and server.log; default oracle/.run/gate
#   ORACLE_GATE_TIMEOUT   seconds the replay and the smokes may take together; default 900
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oracle="$(dirname "$here")"
repo="$(dirname "$oracle")"
port="${ORACLE_GATE_PORT:-0}"
out="${ORACLE_GATE_OUT:-$oracle/.run/gate}"
limit="${ORACLE_GATE_TIMEOUT:-900}"
psk=oxhaven
log="$out/server.log"
report="$out/report.json"
python="$oracle/.venv/bin/python"

fail() {
  echo "replay gate: $*" >&2
  exit 2
}

[[ -x "$python" ]] || fail "no Python environment at $oracle/.venv; see oracle/README.md"
mkdir -p "$out" || fail "cannot create $out"
rm -f "$report" "$log"

echo "== build"
(cd "$repo" && cargo build --locked -p oxhaven-server) || fail "the server did not build"
binary="${CARGO_TARGET_DIR:-$repo/target}/debug/oxhaven-server"
[[ -x "$binary" ]] || fail "no server binary at $binary"

echo "== start"
NO_COLOR=1 "$binary" --host 127.0.0.1 --port "$port" --psk "$psk" >"$log" 2>&1 &
server=$!

stop_server() {
  kill -0 "$server" 2>/dev/null || return 0
  kill -INT "$server" 2>/dev/null
  for _ in $(seq 50); do
    kill -0 "$server" 2>/dev/null || return 0
    sleep 0.1
  done
  echo "replay gate: the server did not stop on SIGINT within 5 seconds; killing it" >&2
  kill -KILL "$server" 2>/dev/null
}
trap stop_server EXIT

server_died() {
  echo "replay gate: the server exited $1; the last lines of $log:" >&2
  tail -n 40 "$log" >&2
  exit 2
}

# The server logs the address it listens on; with port 0 that is where the port comes from.
url=
for _ in $(seq 300); do
  kill -0 "$server" 2>/dev/null || server_died "while starting"
  listening="$(grep -o 'Listening on 127\.0\.0\.1:[0-9]*' "$log" | head -n 1)"
  if [[ -n "$listening" ]]; then
    url="dh+psk://$psk@127.0.0.1:${listening##*:}"
    if PYTHONPATH="$oracle" "$python" -m harness.health "$url" >/dev/null 2>&1; then
      break
    fi
  fi
  url=
  sleep 0.2
done
[[ -n "$url" ]] || fail "the server did not answer a query within 60 seconds; see $log"
echo "server $server at $url"

echo "== replay and client smokes"
timeout --signal=INT --kill-after=10 "$limit" \
  "$here/harness" gate --url "$url" --report "$report"
status=$?
if [[ $status -eq 124 || $status -eq 137 ]]; then
  echo "replay gate: the replay and the smokes did not finish within $limit seconds" >&2
  status=2
fi

# A server that died is a failure whatever the comparison said.
kill -0 "$server" 2>/dev/null || server_died "during the run"

echo "== stop"
stop_server
trap - EXIT
[[ -f "$report" ]] && echo "report: $report"
echo "server log: $log"
exit "$status"
