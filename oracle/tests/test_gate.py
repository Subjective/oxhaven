"""The replay gate's ratchet: what `baseline.judge` fails and what `baseline.weakenings` flags.

Needs no server.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from harness import baseline as bl
from harness import gate, rules
from harness import scenario as sc
from harness import smoke

ORACLE = Path(__file__).resolve().parent.parent
CLEAN = bl.Entry(bl.CLEAN)


def differs(*kinds: str, tables: tuple[str, ...] = ("t",)) -> bl.Entry:
    return bl.Entry(bl.DIFFERS, tuple(sorted(kinds)), tables, "a known cause")


def judge(listed: bl.Entry | None, seen: bl.Entry | None) -> list[str]:
    baseline = {"replay": {"s": listed} if listed else {}}
    observed = {"replay": {"s": seen} if seen else {}}
    return bl.judge(baseline, observed)


def test_the_baseline_outcome_passes() -> None:
    assert judge(CLEAN, CLEAN) == []
    assert judge(differs("value", "row_count"), differs("row_count", "value")) == []


@pytest.mark.parametrize(
    ("listed", "seen", "verdict"),
    [
        (CLEAN, differs("value"), "REGRESSED"),
        (differs("value"), CLEAN, "PROMOTE"),
        (None, CLEAN, "UNLISTED"),
        (None, differs("value"), "UNLISTED"),
        (CLEAN, None, "STALE"),
        (CLEAN, bl.Entry(bl.NOT_RUN), "NOT RUN"),
        (differs("value"), bl.Entry(bl.NOT_RUN), "NOT RUN"),
        (differs("value"), differs("row_count"), "NEW KIND"),
        (differs("value"), differs("value", "row_count"), "NEW KIND"),
        (differs("value", "row_count"), differs("value"), "TIGHTEN"),
        (differs("value"), differs("value", tables=("t", "u")), "NEW TABLE"),
        (differs("value", tables=("t", "u")), differs("value", tables=("u",)), "TIGHTEN"),
    ],
)
def test_every_other_outcome_fails(listed, seen, verdict) -> None:
    problems = judge(listed, seen)
    assert len(problems) == 1 and problems[0].startswith(verdict), problems


def test_a_promotion_message_carries_the_line_to_write() -> None:
    (problem,) = judge(differs("value"), CLEAN)
    assert '"s": {"state": "clean"}' in problem


def test_a_missing_section_fails_in_both_directions() -> None:
    assert bl.judge({}, {"client-smoke flight": {"c": CLEAN}})[0].startswith("UNLISTED")
    assert bl.judge({"client-smoke flight": {"c": CLEAN}}, {})[0].startswith("STALE")


def test_only_stricter_changes_are_not_weakenings() -> None:
    def weaker(old: bl.Entry | None, new: bl.Entry | None) -> bool:
        return bool(bl.weakenings({"replay": {"s": old} if old else {}},
                                  {"replay": {"s": new} if new else {}}))

    assert not weaker(CLEAN, CLEAN)
    assert not weaker(differs("value"), CLEAN)
    assert not weaker(differs("value", "row_count"), differs("value"))
    assert not weaker(differs("value", tables=("t", "u")), differs("value", tables=("t",)))
    assert not weaker(differs("value"), bl.Entry(bl.DIFFERS, ("value",), ("t",), "reworded"))
    assert weaker(CLEAN, differs("value"))
    assert weaker(differs("value"), differs("row_count"))
    assert weaker(differs("value"), differs("value", "row_count"))
    assert weaker(differs("value"), differs("value", tables=("t", "u")))
    assert weaker(CLEAN, None)
    assert weaker(None, CLEAN)
    assert weaker(differs("value"), None)


def test_dump_and_parse_round_trip() -> None:
    sections = {
        "replay": {"b": differs("value", "row_count", tables=("x", "y")), "a": CLEAN},
        "client-smoke flight": {"a check": bl.Entry(bl.DIFFERS, (gate.FAILED,), (), "why")},
    }
    text = bl.dump(sections)
    assert bl.parse(text) == sections
    assert text.index('"replay"') < text.index('"client-smoke flight"')
    assert text.index('"a": ') < text.index('"b": ')


@pytest.mark.parametrize(
    "entry",
    [
        '{"state": "flaky"}',
        '{"state": "not_run"}',
        '{"state": "clean", "kinds": ["value"]}',
        '{"state": "clean", "why": "x"}',
        '{"state": "differs", "why": "x"}',
        '{"state": "differs", "kinds": [], "why": "x"}',
        '{"state": "differs", "kinds": ["value", "value"], "why": "x"}',
        '{"state": "differs", "kinds": ["value"]}',
        '{"state": "differs", "kinds": ["value"], "why": " "}',
        '{"state": "differs", "kinds": ["value"], "why": "FILL IN: what causes it"}',
        '{"state": "differs", "kinds": ["value"], "tables": [], "why": "x"}',
        '{"state": "differs", "kinds": ["value"], "why": "x", "allow_flaky": true}',
    ],
)
def test_a_malformed_entry_is_refused(entry: str) -> None:
    with pytest.raises(bl.BaselineError):
        bl.parse('{"format": 1, "replay": {"s": %s}}' % entry)


def test_a_duplicate_entry_is_refused() -> None:
    with pytest.raises(bl.BaselineError, match="listed twice"):
        bl.parse('{"format": 1, "replay": {"s": {"state": "clean"}, "s": {"state": "clean"}}}')


def test_another_format_is_refused() -> None:
    with pytest.raises(bl.BaselineError):
        bl.parse('{"format": 2, "replay": {}}')
    with pytest.raises(bl.BaselineError):
        bl.parse("[]")


def test_kinds_carry_the_status_code_and_the_metadata_key() -> None:
    def kinds(kind: str, **fields) -> set[str]:
        return gate.kinds_of(rules.Mismatch("s", "t", 0, kind, **fields))

    assert kinds(rules.VALUE, column="X", row=1, expected=1, actual=2) == {"value"}
    assert kinds(rules.FAILURE_UNEXPECTED, expected="succeeds",
                 actual="fails with UNIMPLEMENTED") == {"failure_unexpected:UNIMPLEMENTED"}
    assert kinds(rules.FAILURE_CODE, expected="INVALID_ARGUMENT",
                 actual="INTERNAL") == {"failure_code:INTERNAL"}
    assert kinds(rules.FAILURE_EXPECTED, expected="fails with INVALID_ARGUMENT",
                 actual="succeeds") == {"failure_expected"}
    assert kinds(
        rules.TABLE_METADATA,
        expected={"deephaven:attribute.AddOnly": "true",
                  "deephaven:attribute_type.AddOnly": "java.lang.Boolean",
                  "deephaven:attribute.IsFlat": "true"},
        actual={"deephaven:attribute.IsFlat": "true",
                "deephaven:attribute.BlinkTable": "true"},
    ) == {"table_metadata:AddOnly", "table_metadata:BlinkTable"}
    assert kinds(
        rules.COLUMN_METADATA, column="K",
        expected={"deephaven:type": "int", "deephaven:inputtable.isKey": "true"},
        actual={"deephaven:type": "int"},
    ) == {"column_metadata:inputtable.isKey"}


def test_the_checked_in_baseline_lists_every_scenario_and_every_smoke_area() -> None:
    baseline = bl.load(ORACLE / "replay-baseline.json")
    scenarios = {s.name for s in sc.load_all(ORACLE / "scenarios")}
    assert set(baseline[gate.REPLAY]) == scenarios
    # the script scenarios of the third section are checked in tests/test_scripts.py
    assert set(baseline) == {gate.REPLAY, gate.REPLAY_SCRIPTS} | {
        gate.smoke_section(a) for a in smoke.AREAS}
    for area in smoke.AREAS:
        # Building the list of checks does not connect.
        names = {name for name, _ in smoke._AREAS[area]("dh://127.0.0.1:1")}  # noqa: SLF001
        assert set(baseline[gate.smoke_section(area)]) == names


def test_the_checked_in_baseline_is_in_the_form_dump_writes() -> None:
    path = ORACLE / "replay-baseline.json"
    assert bl.dump(bl.load(path)) == path.read_text(encoding="utf-8")


def test_the_replay_requirements_are_pins_of_the_full_environment() -> None:
    def pins(name: str) -> set[str]:
        lines = (ORACLE / name).read_text(encoding="utf-8").splitlines()
        return {line.strip() for line in lines if line.strip() and not line.startswith("#")}

    replay, full = pins("requirements-replay.txt"), pins("requirements.txt")
    assert replay and all("==" in pin for pin in replay)
    assert replay <= full, sorted(replay - full)
