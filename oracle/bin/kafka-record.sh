#!/usr/bin/env bash
# Records the Kafka corpus: starts a broker with the topics of oracle/corpus/kafka/topics.json
# and their schema registry, starts the Java reference, and stores what it makes of the Kafka
# scenarios (oracle/scenarios/kafka) under oracle/corpus/kafka. See oracle/README.md, "The
# Kafka corpus".
#
#   oracle/bin/kafka-record.sh [scenario names, comma-separated]
#
# Needs Docker, a JDK, and oracle/.venv made from requirements.txt (the Java server). Never
# run it against anything but the Java reference: the recordings are the expectation.
#
# Environment: ORACLE_KAFKA_PORT, ORACLE_KAFKA_REGISTRY_PORT (kafka-services.sh);
#   ORACLE_KAFKA_JAVA_PORT   the port of the Java server; default 10020
#   ORACLE_KAFKA_HOLD        when set, starts everything, prints the server's URL and waits,
#                            for writing a scenario (bin/harness show-scripts --url ...
#                            --scenarios oracle/scenarios/kafka --scripts <name>)
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
oracle="$(dirname "$here")"
out="$oracle/.run/kafka-record"
java_port="${ORACLE_KAFKA_JAVA_PORT:-10020}"
# shellcheck source=oracle/bin/kafka-services.sh
source "$here/kafka-services.sh"

stop() {
  ORACLE_RUN_DIR="$out" "$here/stop-java.sh" "$java_port" >/dev/null 2>&1
  kafka_services_stop
}
trap stop EXIT
trap 'exit 130' INT TERM

kafka_services_start "$out" || exit 2
url="$(ORACLE_RUN_DIR="$out" "$here/start-java.sh" "$java_port")" || exit 2
echo "Java reference at $url"
if [[ -n "${ORACLE_KAFKA_HOLD:-}" ]]; then
  echo "interrupt to stop the Java server, the broker and the registry"
  while true; do sleep 1; done
fi
"$here/harness" kafka-record --url "$url" ${1:+--scripts "$1"}
