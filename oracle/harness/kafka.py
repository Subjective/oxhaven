"""The Kafka corpus: script scenarios that consume fixed topics of a real broker.

No Java counterpart: this is the harness. A Kafka scenario is a script scenario
(`scripts.py`) under `scenarios/kafka/`; its recording is under `corpus/kafka/`. What makes
it a Kafka scenario is what it needs around the server: a broker that holds the topics of
`corpus/kafka/topics.json` and a schema registry that answers with its subjects. This module
is the three things the script scenarios do not have:

- `publish`: creates the topics on a broker and sends their records, one at a time, so that
  the offsets are those of the file;
- `Registry`: a schema registry that answers the requests Java's client and Oxhaven's make,
  from the subjects of the file, and nothing else;
- `gate`: the replay of every Kafka scenario against `kafka-baseline.json`, with the rules
  and the verdicts of the replay gate (`gate.py`, `baseline.py`).

The scripts find the two services through the environment of the server that runs them:
`ORACLE_KAFKA_BROKER` and `ORACLE_KAFKA_REGISTRY`, which `bin/kafka-services.sh` sets.
`bin/kafka-replay.sh` and `bin/kafka-record.sh` are the commands.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

from . import kafka_topics

SECTION = "replay-kafka"
TOPICS_FILE = "topics.json"


class FixtureError(RuntimeError):
    """The topics file cannot be used."""


def load_fixture(path: Path) -> dict[str, Any]:
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise FixtureError(f"cannot read the topics of the Kafka corpus at {path}: {error}") \
            from error
    if not isinstance(doc, dict) or doc.get("format") != kafka_topics.FORMAT:
        raise FixtureError(f"{path} is not a topics file of format {kafka_topics.FORMAT}")
    return doc


def digest(doc: dict[str, Any]) -> str:
    """Identifies the topic contents a recording was made from."""
    canonical = json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("ascii")).hexdigest()


def _bytes(shown: dict[str, str] | None) -> bytes | None:
    if shown is None:
        return None
    if "text" in shown:
        return shown["text"].encode("utf-8")
    return bytes.fromhex(shown["hex"])


# ---------------------------------------------------------------- the broker


def publish(doc: dict[str, Any], broker: str) -> int:
    """Creates every topic of `doc` on `broker` and sends its records; returns their number.

    Each record is sent and acknowledged before the next, to the partition and with the
    timestamp the file gives it, so its offset is its position among the records of its
    partition. A topic that already exists is an error: the offsets would not be the
    file's.

    The topics keep their records for ever (`retention.ms=-1`). The timestamps of the file
    are in 2023, and a broker with its default retention of seven days deletes such records
    at its next check, within minutes: the beginning of each partition would then be its end.
    """
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    admin = KafkaAdminClient(bootstrap_servers=broker, client_id="oracle-kafka-corpus")
    try:
        admin.create_topics([NewTopic(topic["name"], num_partitions=topic["partitions"],
                                      replication_factor=1,
                                      topic_configs={"retention.ms": "-1"})
                             for topic in doc["topics"]])
    finally:
        admin.close()
    sent = 0
    for topic in doc["topics"]:
        producer = KafkaProducer(bootstrap_servers=broker, client_id="oracle-kafka-corpus",
                                 acks="all", compression_type=topic["compression"],
                                 linger_ms=0)
        try:
            positions: dict[int, int] = {}
            for item in topic["records"]:
                headers = [(name, _bytes(value)) for name, value in item["headers"]]
                metadata = producer.send(
                    topic["name"], value=_bytes(item["value"]), key=_bytes(item["key"]),
                    headers=headers, partition=item["partition"],
                    timestamp_ms=item["timestamp"]).get(timeout=30)
                expected = positions.get(item["partition"], 0)
                if metadata.offset != expected:
                    raise FixtureError(
                        f"a record of {topic['name']}-{item['partition']} got offset "
                        f"{metadata.offset}, not {expected}: the broker is not a fresh one")
                positions[item["partition"]] = expected + 1
                sent += 1
        finally:
            producer.close()
    return sent


# ---------------------------------------------------------------- the schema registry

_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class Registry:
    """A schema registry over the subjects of the topics file, on 127.0.0.1.

    It answers what a Confluent registry answers to the requests of Java's client
    (`CachedSchemaRegistryClient`, as `docs/KAFKA.md` lists them) and of Oxhaven's:

        GET /subjects/<subject>/versions/latest
        GET /subjects/<subject>/versions/<n>
        GET /schemas/ids/<id>

    with Confluent's error bodies for a subject, version or id it does not have, and 404
    for every other path (Java's deserializer asks for `/associations/...` first and goes
    on after the 404). Query strings are ignored. It registers nothing.
    """

    def __init__(self, subjects: list[dict[str, Any]], port: int = 0):
        self._subjects = {entry["subject"]: entry["versions"] for entry in subjects}
        self._by_id = {version["id"]: version
                       for entry in subjects for version in entry["versions"]}
        self.requests: list[str] = []
        registry = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:  # noqa: N802 - the name http.server calls
                registry.requests.append(self.path)
                status, body = registry.answer(self.path)
                data = json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", _CONTENT_TYPE)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *_: Any) -> None:
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def answer(self, path: str) -> tuple[int, dict[str, Any]]:
        """The status and the body for a GET of `path`."""
        parts = [unquote(part) for part in urlsplit(path).path.split("/") if part]
        if len(parts) == 4 and parts[0] == "subjects" and parts[2] == "versions":
            versions = self._subjects.get(parts[1])
            if versions is None:
                return 404, {"error_code": 40401,
                             "message": f"Subject '{parts[1]}' not found."}
            if parts[3] == "latest":
                found = versions[-1]
            else:
                found = next((v for v in versions if str(v["version"]) == parts[3]), None)
                if found is None:
                    return 404, {"error_code": 40402,
                                 "message": f"Version {parts[3]} not found."}
            return 200, {"subject": parts[1], "version": found["version"], "id": found["id"],
                         **_schema_body(found)}
        if len(parts) == 3 and parts[:2] == ["schemas", "ids"]:
            found = self._by_id.get(int(parts[2])) if parts[2].isdigit() else None
            if found is None:
                return 404, {"error_code": 40403, "message": f"Schema {parts[2]} not found"}
            return 200, _schema_body(found)
        return 404, {"error_code": 404, "message": "HTTP 404 Not Found"}


def _schema_body(version: dict[str, Any]) -> dict[str, Any]:
    """A registry leaves `schemaType` out for Avro, its default."""
    body: dict[str, Any] = {"schema": version["schema"]}
    if version["type"] != "AVRO":
        body["schemaType"] = version["type"]
    return body


# ---------------------------------------------------------------- the gate


def gate(url: str, scenarios_dir: Path, corpus: Path, baseline_path: Path,
         report_path: Path | None, select: list[str] | None = None) -> int:
    """Replays the Kafka scenarios against the server at `url` and judges the outcome
    against the baseline: 0 when it is the baseline's, 1 when it is not, 2 when a scenario
    could not be run. With `select` only those scenarios run, and only their lines are
    judged."""
    from . import baseline as bl
    from . import gate as replay_gate
    from . import scenario as sc
    from . import scripts

    baseline = bl.load(baseline_path)
    scenarios = scripts.load_all(scenarios_dir, select)
    if not scenarios:
        raise sc.ScenarioError(f"no Kafka scenario under {scenarios_dir}")
    started = time.monotonic()
    report: dict[str, Any] = {"url": url, "sections": {SECTION: {}}}
    observed = {SECTION: replay_gate.replay_scripts(
        scripts.Target(url), scenarios, corpus, report["sections"][SECTION])}
    listed = {SECTION: baseline.get(SECTION, {})}
    if select:
        chosen = {scenario.name for scenario in scenarios}
        listed = {SECTION: {name: entry for name, entry in listed[SECTION].items()
                            if name in chosen}}
    problems = bl.judge(listed, observed)
    states = [entry.state for entry in observed[SECTION].values()]
    report["seconds"] = round(time.monotonic() - started, 2)
    report["problems"] = problems
    report["observed_as_baseline"] = bl.dump(replay_gate._with_reasons(observed, baseline))
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=1)
            handle.write("\n")
    print()
    print(f"{SECTION}: {len(states)} run, {states.count(bl.CLEAN)} clean, "
          f"{states.count(bl.DIFFERS)} differ, {states.count(bl.NOT_RUN)} not run")
    if not problems:
        print(f"kafka gate: the outcome is the baseline's ({baseline_path.name})")
        return 0
    print(f"kafka gate: FAILED, {len(problems)} entries do not match {baseline_path.name}:")
    for problem in problems:
        print(f"  {problem}")
    return 2 if bl.NOT_RUN in states else 1
