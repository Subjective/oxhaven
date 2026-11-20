#!/usr/bin/env bash
# Records the remote-table corpus: starts the remote from oracle/corpus/remote/seed and the
# Java reference's console, and stores what that console makes of the remote scenarios
# (oracle/scenarios/remote) under oracle/corpus/remote. See oracle/README.md, "The
# remote-table corpus".
#
#   oracle/bin/remote-record.sh [scenario names, comma-separated]
#
# The remote is an Oxhaven server built with its console, as in the gate: Java's client
# reading Oxhaven's server is the expectation of Oxhaven's client reading Oxhaven's server.
# With ORACLE_REMOTE_KIND=deephaven the remote is Deephaven 42.9 instead, and the recordings
# go to oracle/.run/remote-deephaven-corpus (or ORACLE_REMOTE_CORPUS), never to
# oracle/corpus: they are for `bin/harness remote-compare`, which shows that the Oxhaven
# remote's recordings are the Deephaven remote's.
#
# Needs a JDK, the Rust toolchain, and oracle/.venv made from requirements.txt (the Java
# server). Never run it against anything but the Java reference: the recordings are the
# expectation.
#
# Environment:
#   ORACLE_REMOTE_KIND       oxhaven (default) or deephaven: the remote
#   ORACLE_REMOTE_CORPUS     where the recordings go; default above
#   ORACLE_REMOTE_JAVA_PORT  the port of the Java console; default: a free one
#   ORACLE_REMOTE_HOLD       when set, starts everything, prints the URLs and waits, for
#                            writing a scenario
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oracle="$(dirname "$here")"
repo="$(dirname "$oracle")"
out="$oracle/.run/remote-record"
kind="${ORACLE_REMOTE_KIND:-oxhaven}"
python="$oracle/.venv/bin/python"
java_port="${ORACLE_REMOTE_JAVA_PORT:-$("$python" -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')}"
if [[ "$kind" == deephaven ]]; then
  corpus="${ORACLE_REMOTE_CORPUS:-$oracle/.run/remote-deephaven-corpus}"
else
  corpus="${ORACLE_REMOTE_CORPUS:-$oracle/corpus/remote}"
fi
# shellcheck source=oracle/bin/remote-services.sh
source "$here/remote-services.sh"

stop() {
  ORACLE_RUN_DIR="$out" "$here/stop-java.sh" "$java_port" >/dev/null 2>&1
  remote_services_stop
}
trap stop EXIT
trap 'exit 130' INT TERM

binary=
if [[ "$kind" == oxhaven ]]; then
  (cd "$repo" && cargo build --locked -p oxhaven-server --features python) || exit 2
  binary="${CARGO_TARGET_DIR:-$repo/target}/debug/oxhaven-server"
fi
remote_services_start "$out" "$kind" "$binary" || exit 2
# The Java console inherits ORACLE_REMOTE_TARGET from here.
url="$(ORACLE_RUN_DIR="$out" "$here/start-java.sh" "$java_port")" || exit 2
echo "Java reference at $url, remote at $ORACLE_REMOTE_URL"
if [[ -n "${ORACLE_REMOTE_HOLD:-}" ]]; then
  echo "interrupt to stop both servers"
  while true; do sleep 1; done
fi
"$here/harness" remote-record --url "$url" --remote "$ORACLE_REMOTE_URL" --corpus "$corpus" \
  --remote-kind "$kind" ${1:+--scripts "$1"}
