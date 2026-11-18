"""The Kafka corpus: the topics file, the `await` directive, the registry, the baseline.

Needs the recordings but no server and no broker.
"""

from __future__ import annotations

import json
import struct
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from harness import baseline as bl
from harness import kafka, kafka_topics, scripts
from harness.__main__ import KAFKA_RECORDED_FROM, main
from harness.results import ResultSet
from harness.scenario import ScenarioError

ORACLE = Path(__file__).resolve().parent.parent
SCENARIOS = ORACLE / "scenarios" / "kafka"
CORPUS = ORACLE / "corpus" / "kafka"
BASELINE = ORACLE / "kafka-baseline.json"


# ---------------------------------------------------------------- the topics file


def test_the_topics_file_is_what_the_definitions_give() -> None:
    text = (CORPUS / kafka.TOPICS_FILE).read_text(encoding="utf-8")
    assert text == kafka_topics.dump(kafka_topics.fixture())


def test_the_recordings_were_made_from_the_topics_file() -> None:
    recorded = json.loads((CORPUS / KAFKA_RECORDED_FROM).read_text(encoding="utf-8"))
    doc = kafka.load_fixture(CORPUS / kafka.TOPICS_FILE)
    assert recorded == {"topics_sha256": kafka.digest(doc), "reference": "42.9"}


def test_a_changed_record_changes_the_digest() -> None:
    doc = kafka.load_fixture(CORPUS / kafka.TOPICS_FILE)
    before = kafka.digest(doc)
    doc["topics"][0]["records"][0]["timestamp"] += 1
    assert kafka.digest(doc) != before


def test_nothing_in_the_topics_depends_on_the_clock_or_the_broker() -> None:
    doc = kafka.load_fixture(CORPUS / kafka.TOPICS_FILE)
    names = [topic["name"] for topic in doc["topics"]]
    assert len(set(names)) == len(names)
    for topic in doc["topics"]:
        assert topic["records"], topic["name"]
        for item in topic["records"]:
            # A timestamp of 0 is "now" to librdkafka's producer; none is the broker's clock.
            assert isinstance(item["timestamp"], int) and item["timestamp"] > 0
            assert 0 <= item["partition"] < topic["partitions"]
            assert item["key"] is not None or item["value"] is not None


def test_the_encodings_are_those_of_the_formats() -> None:
    # Avro's specification gives these: zig-zag, then seven bits a byte, low bits first.
    assert [kafka_topics.avro_long(n).hex() for n in (0, -1, 1, -2, 2, -64, 64, 8192)] == \
        ["00", "01", "02", "03", "04", "7f", "8001", "808001"]
    assert kafka_topics.avro_long(-(2 ** 63)).hex() == "ffffffffffffffffff01"
    assert kafka_topics.avro_string("foo").hex() == "06666f6f"
    assert kafka_topics.avro_array([]).hex() == "00"
    assert kafka_topics.avro_array([b"\x02", b"\x04"]).hex() == "04020400"
    assert kafka_topics.confluent(7, b"\x02") == b"\x00\x00\x00\x00\x07\x02"
    # Protobuf's encoding guide: field 1, varint 150; field 2, the text "testing".
    assert kafka_topics.proto_varint(1, 150).hex() == "089601"
    assert kafka_topics.proto_bytes(2, b"testing").hex() == "120774657374696e67"
    assert kafka_topics.proto_varint(2, -1).hex() == "10ffffffffffffffffff01"
    assert kafka_topics.be(">i", 1) == struct.pack(">i", 1) == b"\x00\x00\x00\x01"


def test_bytes_are_shown_as_text_only_when_they_are_text() -> None:
    assert kafka_topics._shown(None) is None
    assert kafka_topics._shown(b"") == {"text": ""}
    assert kafka_topics._shown("hé".encode()) == {"text": "hé"}
    assert kafka_topics._shown(b"\x00\x01") == {"hex": "0001"}
    assert kafka_topics._shown(b"\xff") == {"hex": "ff"}
    for shown in ({"text": "hé"}, {"hex": "0001"}, {"text": ""}, None):
        assert kafka_topics._shown(kafka._bytes(shown)) == shown


def test_a_topics_file_of_another_format_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "topics.json"
    path.write_text('{"format": 99}', encoding="utf-8")
    with pytest.raises(kafka.FixtureError, match="format"):
        kafka.load_fixture(path)
    with pytest.raises(kafka.FixtureError, match="cannot read"):
        kafka.load_fixture(tmp_path / "absent.json")


def test_the_gate_refuses_recordings_of_other_topics(tmp_path: Path, capsys) -> None:
    corpus = tmp_path / "corpus"
    corpus.mkdir()
    doc = kafka_topics.fixture()
    (corpus / kafka.TOPICS_FILE).write_text(kafka_topics.dump(doc), encoding="utf-8")
    (corpus / KAFKA_RECORDED_FROM).write_text(
        json.dumps({"topics_sha256": "0" * 64, "reference": "42.9"}), encoding="utf-8")
    # No server is asked: the refusal comes first.
    assert main(["kafka-gate", "--url", "dh://127.0.0.1:1", "--corpus", str(corpus)]) == 2
    assert "not the one the recordings were made from" in capsys.readouterr().err


# ---------------------------------------------------------------- the scenarios


def test_every_kafka_scenario_parses_and_has_its_recording() -> None:
    scenarios = scripts.load_all(SCENARIOS)
    assert len(scenarios) >= 15
    for scenario in scenarios:
        assert scenario.name.startswith("kafka_")
        run = scripts.load(CORPUS / scenario.name, scenario)
        assert len(run.outcomes) == len(scenario.sections)
    names = {scenario.name for scenario in scenarios}
    assert {p.name for p in CORPUS.iterdir() if p.is_dir()} == names


def test_every_kafka_scenario_waits_for_what_it_reads() -> None:
    # A Kafka table is filled by the consumer's thread: a scenario that made one and read
    # it without a wait would record whatever had arrived by then.
    for scenario in scripts.load_all(SCENARIOS):
        if scenario.name == "kafka_call_errors":
            continue
        assert any(section.awaits for section in scenario.sections), scenario.name


def test_no_scenario_names_a_host() -> None:
    # The broker and the registry come from the environment; the repository is public.
    for path in sorted(SCENARIOS.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        assert "127.0.0.1" not in text and "localhost" not in text, path.name
        assert 'os.environ["ORACLE_KAFKA_BROKER"]' in text, path.name


def test_the_kafka_baseline_lists_every_scenario_and_nothing_else() -> None:
    baseline = bl.load(BASELINE)
    assert set(baseline) == {kafka.SECTION}
    names = {scenario.name for scenario in scripts.load_all(SCENARIOS)}
    assert set(baseline[kafka.SECTION]) == names
    assert bl.dump(baseline) == BASELINE.read_text(encoding="utf-8")


# ---------------------------------------------------------------- the await directive

AWAIT = """\
# oracle: results t, u
t = 1
# oracle: await t 12, u fails
# oracle: step
u = 2
# oracle: step
# oracle: await u 0
"""


def test_await_belongs_to_the_step_it_is_written_in() -> None:
    scenario = scripts.parse("waits", AWAIT)
    assert [section.awaits for section in scenario.sections] == [
        (scripts.Await("t", 12), scripts.Await("u", None)), (), (scripts.Await("u", 0),)]
    # The directive stays in the script as a comment: line numbers are the file's.
    assert scenario.sections[0].script.splitlines()[2] == "# oracle: await t 12, u fails"


@pytest.mark.parametrize("line", [
    "# oracle: await", "# oracle: await t", "# oracle: await t many", "# oracle: await t -1",
    "# oracle: await t 1 2", "# oracle: await 1t 3", "# oracle: await t 1, t 2",
    "# oracle: await __oracle_x 1",
])
def test_await_refuses_what_it_does_not_understand(line: str) -> None:
    with pytest.raises(ScenarioError):
        scripts.parse("waits", f"# oracle: results t\nt = 1\n{line}\n")


def test_a_wait_that_was_not_met_cannot_be_recorded() -> None:
    scenario = scripts.parse("waits", AWAIT)
    met = scripts.Run([None, None, None], ResultSet("waits"))
    assert scripts.undeclared(scenario, met) == []
    unmet = scripts.Run([None, None, None], ResultSet("waits"),
                        ["step 0: t has 3 rows, not 12"])
    assert scripts.undeclared(scenario, unmet) == [
        "a wait was not met (step 0: t has 3 rows, not 12)"]


class _Table:
    is_refreshing = False

    def __init__(self, size: int):
        self.size = size


def test_await_asks_again_until_the_rows_are_there(monkeypatch) -> None:
    sizes = iter([0, 2, 5, 9])
    asked = []

    def opened(_session, name):
        asked.append(name)
        return _Table(next(sizes))

    monkeypatch.setattr(scripts, "_open", opened)
    monkeypatch.setattr(scripts.time, "sleep", lambda _seconds: None)
    far = scripts.time.monotonic() + 60
    assert scripts._await(None, scripts.Await("t", 5), far) is None
    assert asked == ["t", "t", "t"]


def test_await_gives_up_at_the_deadline_and_says_what_was_missing(monkeypatch) -> None:
    monkeypatch.setattr(scripts, "_open", lambda _session, _name: _Table(3))
    past = scripts.time.monotonic() - 1
    assert scripts._await(None, scripts.Await("t", 5), past) == "t has 3 rows, not 5"
    assert scripts._await(None, scripts.Await("t", None), past) == "t did not fail"


def test_await_ends_when_the_table_has_failed(monkeypatch) -> None:
    from harness.results import Failure

    monkeypatch.setattr(scripts, "_open", lambda _session, _name: Failure("NOT_FOUND"))
    far = scripts.time.monotonic() + 60
    assert scripts._await(None, scripts.Await("t", 5), far) is None
    assert scripts._await(None, scripts.Await("t", None), far) is None


# ---------------------------------------------------------------- the registry


@pytest.fixture
def registry():
    served = kafka.Registry(kafka_topics.subjects())
    yield served
    served.close()


def _get(registry: kafka.Registry, path: str) -> tuple[int, dict]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{registry.port}{path}") as answer:
            return answer.status, json.loads(answer.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def test_the_registry_answers_as_confluents_does(registry: kafka.Registry) -> None:
    status, latest = _get(registry, "/subjects/kc_avro-value/versions/latest")
    assert (status, latest["version"], latest["id"]) == (200, 2, kafka_topics.TRADE_ID)
    assert json.loads(latest["schema"]) == kafka_topics.TRADE and "schemaType" not in latest
    status, old = _get(registry, "/subjects/kc_avro-value/versions/1?deleted=false")
    assert (status, old["id"]) == (200, kafka_topics.TRADE_OLD_ID)
    status, by_id = _get(registry, "/schemas/ids/12?fetchMaxId=false&subject=kc_avro-value")
    assert status == 200 and json.loads(by_id["schema"]) == kafka_topics.TRADE_OLD
    status, proto = _get(registry, "/subjects/kc_proto-value/versions/latest")
    assert (status, proto["schemaType"], proto["schema"]) == \
        (200, "PROTOBUF", kafka_topics.TICK_PROTO)
    # A subject with dots, as a client writes it into the path.
    assert _get(registry, "/subjects/kc.inv.people-key/versions/latest")[0] == 200


def test_the_registry_refuses_as_confluents_does(registry: kafka.Registry) -> None:
    assert _get(registry, "/subjects/nope/versions/latest") == \
        (404, {"error_code": 40401, "message": "Subject 'nope' not found."})
    assert _get(registry, "/subjects/kc_avro-value/versions/9") == \
        (404, {"error_code": 40402, "message": "Version 9 not found."})
    assert _get(registry, "/schemas/ids/999")[1]["error_code"] == 40403
    assert _get(registry, "/associations/resources/x/kc_avro")[0] == 404
    assert registry.requests[-1] == "/associations/resources/x/kc_avro"
