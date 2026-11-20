#!/usr/bin/env bash
# The remote-table gate: builds Oxhaven with its Python console, starts it twice, once as the
# remote with the content of oracle/corpus/remote/seed and once as the server under test,
# replays the remote scenarios (oracle/scenarios/remote) against what the Java reference's
# console made of the same remote (oracle/corpus/remote), and compares the outcome with
# oracle/remote-baseline.json. Needs no Java. See oracle/README.md, "The remote-table corpus".
#
#   oracle/bin/remote-replay.sh [scenario names, comma-separated]
#
# Needs the Rust toolchain, a Python that PyO3 can embed, and oracle/.venv made from
# requirements-replay.txt or requirements.txt.
#
# Exit status: 0 when the outcome is the baseline's; 1 when it is not; 2 when a server did not
# start, exited during the run, or a scenario could not be run.
#
# Environment:
#   PYO3_PYTHON           the Python to embed; default python3 on PATH
#   ORACLE_GATE_PORT      port for the server under test; default 0, which lets the system
#                         pick a free one (the remote always takes a free one)
#   ORACLE_REMOTE_OUT     directory for report.json and the logs; default oracle/.run/remote
#   ORACLE_GATE_TIMEOUT   seconds the replay may take; default 600
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oracle="$(dirname "$here")"
repo="$(dirname "$oracle")"
port="${ORACLE_GATE_PORT:-0}"
out="${ORACLE_REMOTE_OUT:-$oracle/.run/remote}"
limit="${ORACLE_GATE_TIMEOUT:-600}"
psk=oxhaven
log="$out/server.log"
report="$out/report.json"
python="$oracle/.venv/bin/python"
export VIRTUAL_ENV="${VIRTUAL_ENV:-$oracle/.venv}"
# shellcheck source=oracle/bin/remote-services.sh
source "$here/remote-services.sh"

fail() {
  echo "remote gate: $*" >&2
  exit 2
}

[[ -x "$python" ]] || fail "no Python environment at $oracle/.venv; see oracle/README.md"
mkdir -p "$out" || fail "cannot create $out"
rm -f "$report" "$log"
started=$SECONDS

echo "== build"
# As ci-replay.sh builds it: the same command, so a job that ran the replay gate has the
# binary already.
embedded="${PYO3_PYTHON:-python3}"
libdir="$("$embedded" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")' 2>/dev/null)" ||
  fail "no Python to embed ($embedded does not run); the remote scenarios need the console"
(cd "$repo" && cargo build --locked -p oxhaven-server --features python) ||
  fail "the server did not build with --features python"
binary="${CARGO_TARGET_DIR:-$repo/target}/debug/oxhaven-server"
[[ -x "$binary" ]] || fail "no server binary at $binary"
built=$SECONDS

server=
stop() {
  if [[ -n "$server" ]] && kill -0 "$server" 2>/dev/null; then
    kill -INT "$server" 2>/dev/null
    for _ in $(seq 50); do
      kill -0 "$server" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$server" 2>/dev/null
  fi
  remote_services_stop
}
trap stop EXIT
trap 'exit 130' INT TERM

echo "== remote"
remote_services_start "$out" oxhaven "$binary" || fail "the remote did not start"

echo "== start"
# The scripts read ORACLE_REMOTE_TARGET from this environment.
NO_COLOR=1 LD_LIBRARY_PATH="${libdir:+$libdir:}${LD_LIBRARY_PATH:-}" \
  "$binary" --host 127.0.0.1 --port "$port" --psk "$psk" >"$log" 2>&1 &
server=$!
url=
for _ in $(seq 300); do
  kill -0 "$server" 2>/dev/null || { tail -n 40 "$log" >&2; fail "the server exited while starting"; }
  listening="$(grep -o 'Listening on 127\.0\.0\.1:[0-9]*' "$log" | head -n 1)"
  if [[ -n "$listening" ]]; then
    url="dh+psk://$psk@127.0.0.1:${listening##*:}"
    if PYTHONPATH="$oracle" timeout 20 "$python" -m harness.health "$url" >/dev/null 2>&1; then
      break
    fi
  fi
  url=
  sleep 0.2
done
[[ -n "$url" ]] || fail "the server did not answer a query after 300 attempts; see $log"
echo "server $server at $url"

echo "== replay"
PYTHONUNBUFFERED=1 timeout --signal=INT --kill-after=10 "$limit" \
  "$here/harness" remote-gate --url "$url" --remote "$ORACLE_REMOTE_URL" --report "$report" \
  ${1:+--scripts "$1"}
status=$?
if [[ $status -eq 124 || $status -eq 137 ]]; then
  echo "remote gate: the replay did not finish within $limit seconds" >&2
  status=2
fi
for pid in "$server" "$remote_pid"; do
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "remote gate: a server ($pid) exited during the run; see $log and $out/remote-oxhaven.log" >&2
    tail -n 40 "$log" >&2
    status=2
  fi
done
[[ -f "$report" ]] && echo "report: $report"
echo "server log: $log; remote log: $out/remote-oxhaven.log"
echo "remote gate: $((SECONDS - built)) s after the build ($((built - started)) s build)"
exit "$status"
