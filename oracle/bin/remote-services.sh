# Sourced by remote-replay.sh and remote-record.sh: the remote server that the remote-table
# scenarios (oracle/scenarios/remote) read, started from oracle/corpus/remote/seed.
#
#   remote_services_start <directory for logs> <kind> [<oxhaven binary>]
#       kind oxhaven: the Oxhaven server binary given, with its Python console
#       kind deephaven: Deephaven 42.9 from the deephaven-server wheel of oracle/.venv
#   remote_services_stop
#
# Both listen on 127.0.0.1, on a port the system chose, with anonymous authentication (the
# URI resolvers connect anonymously, as Java's do), start the applications of the seed
# directory, and then run its scope.py in their console (harness remote-seed). Nothing but
# this machine connects, and nothing holds a secret.
#
# After the start, ORACLE_REMOTE_TARGET (host:port, which the scenarios' scripts read from
# the environment of the server that runs them) and ORACLE_REMOTE_URL (the harness's URL of
# the remote) are exported.

remote_pid=

remote_services_stop() {
  if [[ -n "$remote_pid" ]] && kill -0 "$remote_pid" 2>/dev/null; then
    kill -INT "$remote_pid" 2>/dev/null
    for _ in $(seq 100); do
      kill -0 "$remote_pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$remote_pid" 2>/dev/null
    wait "$remote_pid" 2>/dev/null
  fi
  remote_pid=
}

remote_services_start() {
  local out="$1" kind="$2" binary="${3:-}" here oracle seed log python port= libdir
  here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  oracle="$(dirname "$here")"
  seed="$oracle/corpus/remote/seed"
  python="$oracle/.venv/bin/python"
  log="$out/remote-$kind.log"
  mkdir -p "$out" || return 1
  case "$kind" in
    oxhaven)
      [[ -x "$binary" ]] || { echo "no Oxhaven binary at $binary" >&2; return 1; }
      libdir="$("${PYO3_PYTHON:-python3}" -c 'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")' 2>/dev/null)"
      NO_COLOR=1 LD_LIBRARY_PATH="${libdir:+$libdir:}${LD_LIBRARY_PATH:-}" \
        VIRTUAL_ENV="${VIRTUAL_ENV:-$oracle/.venv}" \
        "$binary" --host 127.0.0.1 --port 0 --auth anonymous --app-dir "$seed" >"$log" 2>&1 &
      remote_pid=$!
      for _ in $(seq 300); do
        kill -0 "$remote_pid" 2>/dev/null || { tail -n 40 "$log" >&2; return 1; }
        port="$(grep -o 'Listening on 127\.0\.0\.1:[0-9]*' "$log" | head -n 1)"
        port="${port##*:}"
        [[ -n "$port" ]] && break
        sleep 0.2
      done
      ;;
    deephaven)
      "$python" "$here/remote_deephaven.py" --app-dir "$seed" >"$log" 2>&1 &
      remote_pid=$!
      for _ in $(seq 240); do
        kill -0 "$remote_pid" 2>/dev/null || { tail -n 40 "$log" >&2; return 1; }
        port="$(grep -o '^REMOTE_READY port=[0-9]*' "$log" | head -n 1)"
        port="${port##*=}"
        [[ -n "$port" ]] && break
        sleep 1
      done
      ;;
    *)
      echo "unknown remote kind $kind: oxhaven or deephaven" >&2
      return 1
      ;;
  esac
  [[ -n "$port" ]] || { echo "the $kind remote did not start; see $log" >&2; return 1; }
  ORACLE_REMOTE_URL="dh://127.0.0.1:$port"
  local ready=
  for _ in $(seq 100); do
    if PYTHONPATH="$oracle" timeout 20 "$python" -m harness.health "$ORACLE_REMOTE_URL" >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 0.2
  done
  [[ -n "$ready" ]] || { echo "the $kind remote does not answer a query; see $log" >&2; return 1; }
  "$here/harness" remote-seed --remote "$ORACLE_REMOTE_URL" || return 1
  export ORACLE_REMOTE_URL
  export ORACLE_REMOTE_TARGET="127.0.0.1:$port"
  echo "remote ($kind) $remote_pid at $ORACLE_REMOTE_URL"
}
