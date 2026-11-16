#!/usr/bin/env bash
# Runs the broker tests (tests/broker.rs) against a Kafka broker started in Docker.
#
#   crates/oxhaven-kafka/tests/broker.sh
#
# Starts apache/kafka on 127.0.0.1 (the port in OXHAVEN_KAFKA_PORT, default 19092, and the
# one after it for the controller), fills the topics the tests read, runs the tests and
# removes the container. Needs a Docker daemon; DOCKER_HOST selects one. The Avro and
# Protobuf round trips serve their own schema registry from the test: no registry image.
set -euo pipefail

image="apache/kafka:3.9.1"
port="${OXHAVEN_KAFKA_PORT:-19092}"
controller=$((port + 1))
name="oxhaven-kafka-test-$$"
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

docker run -d --name "$name" --network host \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e "KAFKA_LISTENERS=PLAINTEXT://127.0.0.1:$port,CONTROLLER://127.0.0.1:$controller" \
  -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$port" \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e "KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:$controller" \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_HEAP_OPTS="-Xmx384m -Xms128m" "$image" >/dev/null
trap 'docker rm -f "$name" >/dev/null' EXIT

kafka() {
  local tool="$1"
  shift
  docker exec -i "$name" "/opt/kafka/bin/$tool" --bootstrap-server "127.0.0.1:$port" "$@"
}

await_broker() {
  for _ in $(seq 60); do
    if kafka kafka-topics.sh --list >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  echo "the broker in container $name does not answer" >&2
  return 1
}
await_broker

kafka kafka-topics.sh --create --topic oxhaven_one --partitions 1 >/dev/null
kafka kafka-topics.sh --create --topic oxhaven_two --partitions 2 >/dev/null
# Written by the tests that produce: their records' partitions are checked.
kafka kafka-topics.sh --create --topic oxhaven_produce_two --partitions 2 >/dev/null
keyed=(--property parse.key=true --property "key.separator=|")
kafka kafka-console-producer.sh --topic oxhaven_one "${keyed[@]}" <<'RECORDS'
k1|{"N": 1, "S": "a"}
k2|{"N": 2, "S": "b"}
k3|{"S": 1.50}
k4|{"N": 4, "S": null}
RECORDS
kafka kafka-console-producer.sh --topic oxhaven_two "${keyed[@]}" <<'RECORDS'
a|v1
b|v2
c|v3
d|v4
e|v5
f|v6
RECORDS
# The build of librdkafka in the tests has no zlib, so it cannot write this topic itself.
kafka kafka-console-producer.sh --topic oxhaven_gzip --compression-codec gzip <<'RECORDS'
a record in a batch compressed with gzip
RECORDS

cd "$repo"
export OXHAVEN_KAFKA_BROKER="127.0.0.1:$port"
restarted=a_table_goes_on_after_its_broker_restarts
cargo test -p oxhaven-kafka --test broker -- --ignored --skip "$restarted"
# Alone, because the other tests would lose their broker: the container is restarted
# while a table reads from it.
OXHAVEN_KAFKA_RESTART="docker restart $name" \
  cargo test -p oxhaven-kafka --test broker -- --ignored --exact "$restarted"
