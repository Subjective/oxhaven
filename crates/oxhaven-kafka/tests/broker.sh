#!/usr/bin/env bash
# Runs the broker tests (tests/broker.rs, tests/security.rs) against a Kafka broker started
# in Docker.
#
#   crates/oxhaven-kafka/tests/broker.sh
#
# Starts apache/kafka on 127.0.0.1 with five listeners, from the port in OXHAVEN_KAFKA_PORT
# (default 19092) on: PLAINTEXT, the controller, SSL (which asks for a client certificate),
# SASL_SSL and SASL_PLAINTEXT, the last two with PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512. The
# certificates are made here with openssl, in a directory that is removed at the end: none
# is kept in the repository. Then it fills the topics the tests read, runs the tests and
# removes the container. Needs a Docker daemon (DOCKER_HOST selects one) and openssl. The
# Avro and Protobuf round trips serve their own schema registry from the test: no registry
# image. With OXHAVEN_KAFKA_HOLD set, it starts the broker, prints what the tests would be
# given and waits, without running a test, until it is interrupted.
set -euo pipefail

image="apache/kafka:3.9.1"
port="${OXHAVEN_KAFKA_PORT:-19092}"
controller=$((port + 1))
ssl=$((port + 2))
sasl_ssl=$((port + 3))
sasl_plaintext=$((port + 4))
name="oxhaven-kafka-test-$$"
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
certs="$(mktemp -d)"
trap 'rm -rf "$certs"' EXIT

# A certificate authority that signs the broker's certificate and the client's, another
# that signs nothing the broker shows (for the test of an untrusted broker), a broker
# certificate for the name `localhost` only (so that a connection to 127.0.0.1 fails
# hostname verification) and a client certificate, as PEM files and a PKCS#12 keystore.
make_certificates() (
  cd "$certs"
  local quiet=(-days 2 -noenc)
  openssl req -x509 -newkey rsa:2048 "${quiet[@]}" -subj /CN=oxhaven-test-ca \
    -keyout ca.key -out ca.pem 2>/dev/null
  openssl req -x509 -newkey rsa:2048 "${quiet[@]}" -subj /CN=oxhaven-other-ca \
    -keyout other-ca.key -out other-ca.pem 2>/dev/null
  for who in broker client; do
    openssl req -newkey rsa:2048 -noenc -subj "/CN=oxhaven-$who" \
      -keyout "$who.key" -out "$who.csr" 2>/dev/null
  done
  printf 'subjectAltName=DNS:localhost\n' >broker.ext
  openssl x509 -req -in broker.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
    -extfile broker.ext -out broker.pem 2>/dev/null
  openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
    -out client.pem 2>/dev/null
  # The image wants the broker's keystore and the files that hold its passwords.
  openssl pkcs12 -export -in broker.pem -inkey broker.key -passout pass:broker-secret \
    -out broker.p12
  echo broker-secret >broker-secret
  cat client.key client.pem >client-keystore.pem
  openssl pkcs12 -export -in client.pem -inkey client.key -passout pass:keystore-secret \
    -out client.p12
  cat >jaas.conf <<'JAAS'
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
    username="admin" password="admin-secret"
    user_admin="admin-secret" user_alice="alice-secret";
  org.apache.kafka.common.security.scram.ScramLoginModule required;
};
JAAS
  chmod 755 . && chmod 644 ./*
)
make_certificates

listeners="PLAINTEXT://127.0.0.1:$port,CONTROLLER://127.0.0.1:$controller"
listeners+=",SSL://127.0.0.1:$ssl,SASL_SSL://127.0.0.1:$sasl_ssl"
listeners+=",SASL_PLAINTEXT://127.0.0.1:$sasl_plaintext"
advertised="PLAINTEXT://127.0.0.1:$port,SSL://localhost:$ssl,SASL_SSL://localhost:$sasl_ssl"
advertised+=",SASL_PLAINTEXT://127.0.0.1:$sasl_plaintext"
protocols="CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_SSL:SASL_SSL"
protocols+=",SASL_PLAINTEXT:SASL_PLAINTEXT"
docker run -d --name "$name" --network host -v "$certs:/etc/kafka/secrets:ro" \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e "KAFKA_LISTENERS=$listeners" -e "KAFKA_ADVERTISED_LISTENERS=$advertised" \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=$protocols" \
  -e "KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:$controller" \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e KAFKA_LOG_DIRS=/tmp/kraft-combined-logs \
  -e KAFKA_SSL_KEYSTORE_FILENAME=broker.p12 -e KAFKA_SSL_KEYSTORE_TYPE=PKCS12 \
  -e KAFKA_SSL_KEYSTORE_CREDENTIALS=broker-secret -e KAFKA_SSL_KEY_CREDENTIALS=broker-secret \
  -e KAFKA_SSL_TRUSTSTORE_TYPE=PEM \
  -e KAFKA_SSL_TRUSTSTORE_LOCATION=/etc/kafka/secrets/ca.pem \
  -e KAFKA_LISTENER_NAME_SSL_SSL_CLIENT_AUTH=required \
  -e KAFKA_SASL_ENABLED_MECHANISMS=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512 \
  -e KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/secrets/jaas.conf \
  -e KAFKA_HEAP_OPTS="-Xmx384m -Xms128m" "$image" >/dev/null
trap 'docker rm -f "$name" >/dev/null; rm -rf "$certs"' EXIT

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

# The SCRAM users, stored by the controller.
for mechanism in SCRAM-SHA-256 SCRAM-SHA-512; do
  kafka kafka-configs.sh --alter --entity-type users --entity-name alice \
    --add-config "$mechanism=[password=alice-secret]" >/dev/null
done

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
# gzip as Java's producer writes it; the tests write their own with librdkafka's.
kafka kafka-console-producer.sh --topic oxhaven_gzip --compression-codec gzip <<'RECORDS'
a record in a batch compressed with gzip
RECORDS

cd "$repo"
export OXHAVEN_KAFKA_BROKER="127.0.0.1:$port"
export OXHAVEN_KAFKA_SSL="$ssl" OXHAVEN_KAFKA_SASL_SSL="$sasl_ssl"
export OXHAVEN_KAFKA_SASL_PLAINTEXT="$sasl_plaintext" OXHAVEN_KAFKA_CERTS="$certs"
if [[ -n "${OXHAVEN_KAFKA_HOLD:-}" ]]; then
  env | grep '^OXHAVEN_KAFKA_' | sort
  echo "the broker runs in container $name; interrupt to remove it"
  while true; do sleep 60; done
fi
cargo test -p oxhaven-kafka --test security -- --ignored
restarted=a_table_goes_on_after_its_broker_restarts
cargo test -p oxhaven-kafka --test broker -- --ignored --skip "$restarted"
# What `a_table_is_published_in_gzip_batches` wrote is stored in gzip batches.
gzipped=$(docker exec "$name" sh -c 'ls -d /tmp/kraft-combined-logs/oxhaven_produced_gzip_*-0')
for dir in $gzipped; do
  dump=$(docker exec "$name" /opt/kafka/bin/kafka-dump-log.sh --files "$dir/00000000000000000000.log")
  if ! grep -q 'compresscodec: gzip' <<<"$dump" || grep -q 'compresscodec: none' <<<"$dump"; then
    echo "$dir holds batches that are not gzip:" >&2
    echo "$dump" >&2
    exit 1
  fi
done
# Alone, because the other tests would lose their broker: the container is restarted
# while a table reads from it.
OXHAVEN_KAFKA_RESTART="docker restart $name" \
  cargo test -p oxhaven-kafka --test broker -- --ignored --exact "$restarted"
