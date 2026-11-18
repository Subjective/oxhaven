# Sourced by kafka-replay.sh and kafka-record.sh: the broker and the schema registry that the
# Kafka scenarios (oracle/scenarios/kafka) read, holding oracle/corpus/kafka/topics.json.
#
#   kafka_services_start <directory for logs>    starts both and publishes the topics
#   kafka_services_stop                          removes the container and stops the registry
#
# The broker is apache/kafka in Docker (DOCKER_HOST selects the daemon), in KRaft mode on
# 127.0.0.1 with one plaintext listener, as crates/oxhaven-kafka/tests/broker.sh starts it;
# it is a fresh one every time, so that the offsets are those of the topics file. The
# registry is the harness's own (harness/kafka.py). Nothing but this machine connects to
# either, and neither holds a secret.
#
# Environment:
#   ORACLE_KAFKA_PORT            the broker's port; default 19192. The controller takes the next
#   ORACLE_KAFKA_REGISTRY_PORT   the registry's port; default 19194
#
# After the start, ORACLE_KAFKA_BROKER and ORACLE_KAFKA_REGISTRY are exported: the scripts of
# the scenarios read them from the environment of the server that runs them.

kafka_image="apache/kafka:3.9.1"
kafka_container=
kafka_registry_pid=

kafka_services_stop() {
  if [[ -n "$kafka_registry_pid" ]] && kill -0 "$kafka_registry_pid" 2>/dev/null; then
    kill -INT "$kafka_registry_pid" 2>/dev/null
    wait "$kafka_registry_pid" 2>/dev/null
  fi
  kafka_registry_pid=
  if [[ -n "$kafka_container" ]]; then
    docker rm -f "$kafka_container" >/dev/null 2>&1
  fi
  kafka_container=
}

kafka_services_start() {
  local out="$1" harness port controller registry_port
  harness="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/harness"
  port="${ORACLE_KAFKA_PORT:-19192}"
  controller=$((port + 1))
  registry_port="${ORACLE_KAFKA_REGISTRY_PORT:-19194}"
  mkdir -p "$out" || return 1

  "$harness" kafka-topics || return 1

  kafka_container="oracle-kafka-corpus-$$"
  docker run -d --name "$kafka_container" --network host \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e "KAFKA_LISTENERS=PLAINTEXT://127.0.0.1:$port,CONTROLLER://127.0.0.1:$controller" \
    -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$port" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e "KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:$controller" \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
    -e KAFKA_HEAP_OPTS="-Xmx384m -Xms128m" "$kafka_image" >/dev/null || return 1
  local ready=
  for _ in $(seq 60); do
    if docker exec "$kafka_container" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "127.0.0.1:$port" --list >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ -z "$ready" ]]; then
    echo "the broker in container $kafka_container does not answer" >&2
    return 1
  fi
  "$harness" kafka-publish --broker "127.0.0.1:$port" || return 1

  "$harness" kafka-registry --port "$registry_port" --log "$out/registry-requests.log" \
    >"$out/registry.log" 2>&1 &
  kafka_registry_pid=$!
  ready=
  for _ in $(seq 100); do
    if grep -q '^REGISTRY_READY' "$out/registry.log" 2>/dev/null; then
      ready=1
      break
    fi
    kill -0 "$kafka_registry_pid" 2>/dev/null || break
    sleep 0.1
  done
  if [[ -z "$ready" ]]; then
    echo "the schema registry did not start; $out/registry.log:" >&2
    cat "$out/registry.log" >&2
    return 1
  fi
  export ORACLE_KAFKA_BROKER="127.0.0.1:$port"
  export ORACLE_KAFKA_REGISTRY="http://127.0.0.1:$registry_port"
  echo "broker $ORACLE_KAFKA_BROKER (container $kafka_container), registry $ORACLE_KAFKA_REGISTRY"
}
