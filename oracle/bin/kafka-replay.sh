#!/usr/bin/env bash
# The Kafka gate: starts a broker with the topics of oracle/corpus/kafka/topics.json and their
# schema registry, builds Oxhaven with its Python console and starts it, replays the Kafka
# scenarios (oracle/scenarios/kafka) against what the Java reference made of the same topics
# (oracle/corpus/kafka), and compares the outcome with oracle/kafka-baseline.json. Needs no
# Java server. See oracle/README.md, "The Kafka corpus".
#
#   oracle/bin/kafka-replay.sh [scenario names, comma-separated]
#
# Needs Docker (DOCKER_HOST selects the daemon), the Rust toolchain, a Python that PyO3 can
# embed, and oracle/.venv made from requirements-replay.txt or requirements.txt.
#
# Exit status: 0 when the outcome is the baseline's; 1 when it is not; 2 when the broker, the
# registry or the server did not start, the server exited during the run, or a scenario
# could not be run.
#
# Environment: ORACLE_KAFKA_PORT, ORACLE_KAFKA_REGISTRY_PORT (kafka-services.sh);
#   PYO3_PYTHON           the Python to embed; default python3 on PATH
#   ORACLE_GATE_PORT      port for the server; default 0, which lets the system pick a free one
#   ORACLE_KAFKA_OUT      directory for report.json and the logs; default oracle/.run/kafka
#   ORACLE_GATE_TIMEOUT   seconds the replay may take; default 600
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oracle="$(dirname "$here")"
repo="$(dirname "$oracle")"
port="${ORACLE_GATE_PORT:-0}"
out="${ORACLE_KAFKA_OUT:-$oracle/.run/kafka}"
limit="${ORACLE_GATE_TIMEOUT:-600}"
psk=oxhaven
log="$out/server.log"
report="$out/report.json"
python="$oracle/.venv/bin/python"
export VIRTUAL_ENV="${VIRTUAL_ENV:-$oracle/.venv}"
# shellcheck source=oracle/bin/kafka-services.sh
source "$here/kafka-services.sh"

fail() {
  echo "kafka gate: $*" >&2
  exit 2
}

[[ -x "$python" ]] || fail "no Python environment at $oracle/.venv; see oracle/README.md"
mkdir -p "$out" || fail "cannot create $out"
rm -f "$report" "$log"

echo "== build"
# As ci-replay.sh builds it: the same command, so a job that ran the replay gate has the
# binary already.
embedded="${PYO3_PYTHON:-python3}"
libdir="$("$embedded" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")' 2>/dev/null)" ||
  fail "no Python to embed ($embedded does not run); the Kafka scenarios need the console"
(cd "$repo" && cargo build --locked -p oxhaven-server --features python) ||
  fail "the server did not build with --features python"
binary="${CARGO_TARGET_DIR:-$repo/target}/debug/oxhaven-server"
[[ -x "$binary" ]] || fail "no server binary at $binary"

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
  kafka_services_stop
}
trap stop EXIT
trap 'exit 130' INT TERM

echo "== broker and registry"
kafka_services_start "$out" || fail "the broker or the registry did not start"

echo "== start"
# The scripts read ORACLE_KAFKA_BROKER and ORACLE_KAFKA_REGISTRY from this environment.
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
  "$here/harness" kafka-gate --url "$url" --report "$report" ${1:+--scripts "$1"}
status=$?
if [[ $status -eq 124 || $status -eq 137 ]]; then
  echo "kafka gate: the replay did not finish within $limit seconds" >&2
  status=2
fi
if ! kill -0 "$server" 2>/dev/null; then
  echo "kafka gate: the server exited during the run; the last lines of $log:" >&2
  tail -n 40 "$log" >&2
  status=2
fi
[[ -f "$report" ]] && echo "report: $report"
echo "server log: $log"
exit "$status"
