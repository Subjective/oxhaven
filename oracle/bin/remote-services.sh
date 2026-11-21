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
# In front of the remote, a TLS relay (tls_front.py) on another port of 127.0.0.1, for the
# `dh://` connections of the scenarios: its certificate names `localhost` alone and is
# signed by a certificate authority made with openssl for this run, under <directory>/tls,
# with a second authority that signed nothing. The keys are made anew at every start and
# never leave that directory.
#
# After the start, ORACLE_REMOTE_TARGET (host:port, which the scenarios' scripts read from
# the environment of the server that runs them) and ORACLE_REMOTE_URL (the harness's URL of
# the remote) are exported, and for TLS ORACLE_REMOTE_TLS_TARGET (localhost:port of the
# relay), ORACLE_REMOTE_TLS_ROOTS (the PEM of the authority) and
# ORACLE_REMOTE_TLS_OTHER_ROOTS (the PEM of the other one).

remote_pid=
front_pid=

# Makes the certificates of the TLS relay under $1.
remote_tls_certificates() {
  local tls="$1" ec=(-newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes)
  rm -rf "$tls" && mkdir -p "$tls" || return 1
  openssl req -x509 "${ec[@]}" -days 2 -subj "/CN=oxhaven oracle authority" \
    -keyout "$tls/ca.key" -out "$tls/ca.pem" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" &&
    openssl req -x509 "${ec[@]}" -days 2 -subj "/CN=oxhaven oracle other authority" \
      -keyout "$tls/other.key" -out "$tls/other.pem" \
      -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" &&
    openssl req "${ec[@]}" -subj "/CN=localhost" -keyout "$tls/server.key" -out "$tls/server.csr" &&
    printf '%s\n' "subjectAltName=DNS:localhost" "basicConstraints=CA:FALSE" \
      "keyUsage=critical,digitalSignature" "extendedKeyUsage=serverAuth" >"$tls/server.ext" &&
    openssl x509 -req -in "$tls/server.csr" -CA "$tls/ca.pem" -CAkey "$tls/ca.key" \
      -CAcreateserial -days 2 -extfile "$tls/server.ext" -out "$tls/server.pem"
}

# Starts the TLS relay to port $3 of the remote, logging to $1, with the certificates of $2.
remote_tls_start() {
  local out="$1" tls="$2" to="$3" python="$4" here="$5" log="$1/tls-front.log" port=
  remote_tls_certificates "$tls" >"$out/tls-certificates.log" 2>&1 ||
    { cat "$out/tls-certificates.log" >&2; echo "openssl did not make the certificates" >&2; return 1; }
  "$python" "$here/tls_front.py" --to "$to" --cert "$tls/server.pem" --key "$tls/server.key" \
    >"$log" 2>&1 &
  front_pid=$!
  for _ in $(seq 100); do
    kill -0 "$front_pid" 2>/dev/null || { tail -n 20 "$log" >&2; return 1; }
    port="$(grep -o '^TLS_FRONT_READY port=[0-9]*' "$log" | head -n 1)"
    port="${port##*=}"
    [[ -n "$port" ]] && break
    sleep 0.1
  done
  [[ -n "$port" ]] || { echo "the TLS relay did not start; see $log" >&2; return 1; }
  export ORACLE_REMOTE_TLS_TARGET="localhost:$port"
  export ORACLE_REMOTE_TLS_ROOTS="$tls/ca.pem"
  export ORACLE_REMOTE_TLS_OTHER_ROOTS="$tls/other.pem"
  echo "TLS relay $front_pid at $ORACLE_REMOTE_TLS_TARGET"
}

remote_services_stop() {
  if [[ -n "$front_pid" ]] && kill -0 "$front_pid" 2>/dev/null; then
    kill -TERM "$front_pid" 2>/dev/null
    wait "$front_pid" 2>/dev/null
  fi
  front_pid=
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
  remote_tls_start "$out" "$out/tls" "$port" "$python" "$here" || return 1
}
