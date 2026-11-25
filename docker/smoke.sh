#!/usr/bin/env bash
# Starts a built image once and checks that it serves: the web IDE and the JS API answer, the
# console started in the image's environment, the key of START_OPTS is the one in use, and
# docker stop ends the server.
#
#   docker/smoke.sh [image]        (default: oxhaven)
#
# Needs docker and curl. The container gets a free port of 127.0.0.1 and is removed at the end.
set -euo pipefail

image="${1:-oxhaven}"
key="smoke$RANDOM$RANDOM"
name="oxhaven-smoke-$$"

cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  docker logs "$name" >&2 || true
  exit 1
}

started=$(date +%s.%N)
docker run -d --name "$name" -p 127.0.0.1::10000 -e START_OPTS="-Xmx4g -Dauthentication.psk=$key" \
  "$image" >/dev/null
port="$(docker port "$name" 10000/tcp | head -n 1 | sed 's/.*://')"
base="http://127.0.0.1:$port"

for _ in $(seq 1 600); do
  if curl -fsS -o /dev/null "$base/ide/" 2>/dev/null; then
    break
  fi
  if [[ "$(docker inspect -f '{{.State.Running}}' "$name")" != true ]]; then
    fail "the container stopped"
  fi
  sleep 0.1
done
curl -fsS -o /dev/null "$base/ide/" || fail "no answer from $base/ide/"
echo "serving $(awk -v a="$started" -v b="$(date +%s.%N)" 'BEGIN { printf "%.1f", b - a }') s after docker run"

curl -fsS -o /dev/null "$base/jsapi/dh-core.js" || fail "no /jsapi/dh-core.js"
curl -fsS -o /dev/null "$base/iframe/table/" || fail "no /iframe/table/"
logs="$(docker logs "$name" 2>&1)"
grep -q "use $key to connect" <<<"$logs" || fail "the key of START_OPTS is not the one in use"
grep -q "skipping the JVM option -Xmx4g" <<<"$logs" || fail "-Xmx4g was not skipped"
grep -q "packages from /opt/venv" <<<"$logs" || fail "the console does not use /opt/venv"

stop_started=$(date +%s)
docker stop -t 30 "$name" >/dev/null
stopped_in=$(($(date +%s) - stop_started))
[[ $stopped_in -lt 10 ]] || fail "docker stop took ${stopped_in}s: the server did not stop on its signal"
docker logs "$name" 2>&1 | grep -q "Shutting down" || fail "no shutdown in the log"

echo "ok: $image serves the IDE and the JS API with the key of START_OPTS and stops on docker stop"
