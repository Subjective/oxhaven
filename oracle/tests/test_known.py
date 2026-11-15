"""The fuzzer's known findings (`harness/known.py`): what an entry fits, what it must not
fit, and how the `fuzz` command reports the two apart. Needs no server."""

from __future__ import annotations

import json
import math

import pytest

from harness import __main__ as cli
from harness import known as kn
from harness import rules
from harness import scenario as sc
from harness.results import ResultSet

from test_fuzz import EchoTarget, _targets

OPERATIONS = [
    {"name": "a", "op": "agg_by", "source": "t", "by": ["K"], "aggs": [
        {"agg": "min", "columns": ["Low = X"]}, {"agg": "max", "columns": ["High = X"]},
        {"agg": "count", "column": "N"}]},
    {"name": "all", "op": "agg_all_by", "source": "t", "by": ["K"],
     "agg": {"agg": "min", "columns": []}},
    {"name": "u", "op": "update_by", "source": "t", "by": [], "ops": [
        {"op": "emstd_time", "columns": ["E = X"], "args": {"ts_col": "T", "decay_time": "PT1S"}}]},
    {"name": "w", "op": "where", "source": "a", "filters": ["Low < 1"]},
    {"name": "v", "op": "view", "source": "w", "columns": ["Renamed = Low"]},
    {"name": "other", "op": "where", "source": "t", "filters": ["X < 1"]},
]
MIN_ZERO = {"id": "min-zero", "class": "oxhaven-bug",
            "reason": "min keeps the other zero of two equal zeros",
            "match": {"kind": "value", "operation": "agg_by", "producer": "min",
                      "expected": -0.0, "actual": 0.0}}


def known(*entries: dict) -> kn.Known:
    return kn.Known(kn.parse({"format": 1, "findings": list(entries)}))


def value(table: str, column: str, expected, actual, step: int = 0) -> rules.Mismatch:
    return rules.Mismatch("s", table, step, "value", column, 0, expected, actual)


def fits(found: kn.Known, *mismatches: rules.Mismatch, **more) -> list[str | None]:
    return [f.id if f else None for f in found.classify(list(mismatches), OPERATIONS, **more)]


# ---------------------------------------------------------------- the file


def test_the_repository_file_is_valid_and_every_entry_says_why():
    findings = kn.load(cli.KNOWN)
    assert len({f.id for f in findings}) == len(findings)
    for finding in findings:
        assert finding.cls in kn.CLASSES and len(finding.reason) > 40, finding.id


@pytest.mark.parametrize("change", [
    lambda e: e.pop("reason"),
    lambda e: e.update(reason="because"),
    lambda e: e.update({"class": "ignored"}),
    lambda e: e.update(extra=1),
    lambda e: e["match"].pop("operation"),
    lambda e: e["match"].pop("kind"),
    lambda e: e["match"].update(operation="no_such_operation"),
    lambda e: e["match"].update(table="o1"),
    lambda e: e["match"].update(steps="all"),
    lambda e: e["match"].update(requires="anything"),
])
def test_an_entry_that_is_not_understood_is_refused(change):
    entry = json.loads(json.dumps(MIN_ZERO))
    change(entry)
    with pytest.raises(kn.KnownError):
        kn.parse({"format": 1, "findings": [entry]})


def test_the_file_format_and_ids_are_checked():
    with pytest.raises(kn.KnownError):
        kn.parse({"format": 2, "findings": []})
    with pytest.raises(kn.KnownError):
        kn.parse({"format": 1, "findings": [MIN_ZERO, MIN_ZERO]})
    assert kn.parse({"format": 1, "findings": []}) == []


# ---------------------------------------------------------------- what fits


def test_a_mismatch_fits_only_if_every_field_does():
    found = known(MIN_ZERO)
    assert fits(found, value("a", "Low", -0.0, 0.0)) == ["min-zero"]
    # The other way round, another value, another aggregation, operation or kind: not it.
    assert fits(found, value("a", "Low", 0.0, -0.0)) == [None]
    assert fits(found, value("a", "Low", -0.0, 1.0)) == [None]
    assert fits(found, value("a", "Low", 0, 0.0)) == [None]
    assert fits(found, value("a", "High", -0.0, 0.0)) == [None]
    assert fits(found, value("a", "N", -0.0, 0.0)) == [None]
    assert fits(found, value("a", "K", -0.0, 0.0)) == [None]
    assert fits(found, value("all", "X", -0.0, 0.0)) == [None]
    assert fits(found, value("other", "X", -0.0, 0.0)) == [None]
    assert fits(found, rules.Mismatch("s", "a", 0, "null", "Low", 0, -0.0, 0.0)) == [None]


def test_value_patterns():
    either = dict(MIN_ZERO, match=dict(MIN_ZERO["match"], expected={"type": "zero"},
                                       actual={"type": "zero"}))
    found = known(either)
    assert fits(found, value("a", "Low", 0.0, -0.0), value("a", "Low", -0.0, 0.0),
                value("a", "Low", 0.0, 1.0), value("a", "Low", math.nan, 0.0),
                value("a", "Low", None, 0.0)) == ["min-zero", "min-zero", None, None, None]
    any_float = dict(MIN_ZERO, match=dict(MIN_ZERO["match"], expected="NaN",
                                          actual={"type": "float"}))
    found = known(any_float)
    assert fits(found, value("a", "Low", math.nan, 2.5), value("a", "Low", math.nan, math.nan),
                value("a", "Low", 1.0, 2.5), value("a", "Low", math.nan, 2)) == [
        "min-zero", None, None, None]


def test_producer_names_what_made_the_column():
    by_name = {op["name"]: op for op in OPERATIONS}
    assert kn.producer(by_name["a"], "Low") == "min"
    assert kn.producer(by_name["a"], "N") == "count"
    assert kn.producer(by_name["a"], "K") == "key"
    assert kn.producer(by_name["a"], None) is None
    assert kn.producer(by_name["all"], "X") == "min"
    assert kn.producer(by_name["all"], "K") == "key"
    assert kn.producer(by_name["all"], None) == "min"
    assert kn.producer(by_name["u"], "E") == "emstd_time"
    assert kn.producer(by_name["u"], "X") == "source"
    assert kn.producer(by_name["w"], "Low") is None
    assert kn.producer(None, "Low") is None


def test_steps_and_metadata_keys():
    later = dict(MIN_ZERO, match=dict(MIN_ZERO["match"], steps="later"))
    assert fits(known(later), value("a", "Low", -0.0, 0.0, step=0),
                value("a", "Low", -0.0, 0.0, step=2)) == [None, "min-zero"]
    attribute = {"id": "attr", "class": "oxhaven-bug",
                 "reason": "the attribute is not copied to the result",
                 "match": {"kind": "table_metadata", "operation": "agg_all_by",
                           "producer": "min", "key": "x.AppendOnly"}}
    lost = rules.Mismatch("s", "all", 0, "table_metadata", None, None,
                          {"x.AppendOnly": "true", "x.Flat": "true"}, {"x.Flat": "true"})
    two = rules.Mismatch("s", "all", 0, "table_metadata", None, None,
                         {"x.AppendOnly": "true", "x.Flat": "true"}, {})
    assert fits(known(attribute), lost, two) == ["attr", None]


def test_requires_asks_the_reference_once_and_never_fits_unasked():
    entry = {"id": "em", "class": "reference-defect",
             "reason": "the reference disagrees with its own answer from scratch",
             "match": {"kind": "value", "operation": "update_by", "producer": "emstd_time",
                       "requires": "reference_inconsistent"}}
    found = known(entry)
    mismatches = [value("u", "E", math.inf, math.nan, 1), value("u", "E", math.inf, math.nan, 2),
                  value("a", "Low", 1.0, 2.0, 1)]
    calls = []

    def ask():
        calls.append(1)
        return {(1, "u")}

    assert fits(found, *mismatches, reference_inconsistent=ask) == ["em", None, None]
    assert len(calls) == 1
    assert fits(found, *mismatches) == [None, None, None]
    assert fits(found, *mismatches, reference_inconsistent=lambda: None) == [None, None, None]
    # Nothing else fits the entry: the reference is not asked at all.
    calls.clear()
    assert fits(found, value("a", "Low", 1.0, 2.0), reference_inconsistent=ask) == [None]
    assert calls == []


def test_a_table_defined_from_one_with_a_known_mismatch_repeats_it():
    found = known(MIN_ZERO)
    first = value("a", "Low", -0.0, 0.0)
    # `w` reads `a` and `v` reads `w`: the same wrong value again, also under a new name.
    assert fits(found, first, value("w", "Low", -0.0, 0.0), value("v", "Renamed", -0.0, 0.0)) == [
        "min-zero"] * 3
    # Another value, another step or a table that does not read `a`: a difference of its own.
    assert fits(found, first, value("w", "Low", -0.0, 1.0)) == ["min-zero", None]
    assert fits(found, first, value("w", "Low", -0.0, 0.0, step=1)) == ["min-zero", None]
    assert fits(found, first, value("other", "X", -0.0, 0.0)) == ["min-zero", None]
    assert fits(found, first, value("t", "X", -0.0, 0.0)) == ["min-zero", None]
    # Without the mismatch above it, nothing is inherited.
    assert fits(found, value("w", "Low", -0.0, 0.0)) == [None]


# ---------------------------------------------------------------- the command


def _entries(*operations: str) -> list[dict]:
    return [{"id": f"blink-{operation}", "class": "harness",
             "reason": "the test's faulty target changes the blink attribute",
             "match": {"kind": "table_metadata", "operation": operation,
                       "key": "deephaven:attribute.BlinkTable"}} for operation in operations]


def _fuzz(tmp_path, capsys, entries: list[dict], *more: str) -> tuple[int, str]:
    path = tmp_path / "known.json"
    path.write_text(json.dumps({"format": 1, "findings": entries}))
    code = cli.main(["fuzz", "--a", "x", "--b", "faulty:changed_table_attribute@x", "--ops",
                     "where,sort,slice", "--seeds", "12", "--out", str(tmp_path / "out"),
                     "--known", str(path), *more])
    return code, capsys.readouterr().out


def test_fuzz_sets_known_findings_apart(monkeypatch, tmp_path, capsys):
    _targets(monkeypatch, None)
    everything = _entries("where", "sort", "slice", sc.INPUT_TABLE)
    code, out = _fuzz(tmp_path, capsys, everything, "--keep-going")
    assert code == 0, out
    assert "GROUP" not in out
    known_lines = [line for line in out.splitlines() if line.startswith("KNOWN ")]
    assert known_lines and all("(harness)" in line for line in known_lines)
    summary = out.strip().splitlines()[-1]
    assert " 0 with differences in 0 groups, 0 not run; " in summary
    assert summary.endswith("more with known findings only (not counted as differences)")

    # The same run without the file reports them all.
    code, out = _fuzz(tmp_path, capsys, everything, "--keep-going", "--no-known")
    assert code == 1 and "KNOWN" not in out and "GROUP" in out

    # Stopping at the first difference: there is none to stop at.
    code, out = _fuzz(tmp_path, capsys, everything)
    assert code == 0, out
    assert "known seed " in out and "DIFF" not in out


def test_a_known_finding_does_not_hide_another_difference(monkeypatch, tmp_path, capsys):
    _targets(monkeypatch, None)
    some = _entries("where", sc.INPUT_TABLE)
    code, out = _fuzz(tmp_path, capsys, some, "--keep-going")
    assert code == 1, out
    groups = [line for line in out.splitlines() if line.startswith("GROUP ")]
    assert groups and all("operation=sort" in line or "operation=slice" in line
                          for line in groups), groups
    assert any(line.startswith("KNOWN blink-where") for line in out.splitlines())

    # The shrinker ends at a scenario whose difference is not a known one.
    code, out = _fuzz(tmp_path, capsys, some)
    assert code == 1, out
    seed = int(out.split("DIFF  seed ")[1].split(":")[0])
    minimal = sc.load(tmp_path / "out" / f"fuzz_{seed}_min.json")
    assert {op["op"] for op in minimal.operations} & {"sort", "slice"}


# ---------------------------------------------------------------- the reference against itself


def test_compare_with_fresh_reports_what_the_server_kept_differently():
    scenario = sc.parse({
        "format": 1, "name": "s",
        "inputs": [{"name": "t", "ticking": {"key_columns": []},
                    "columns": [{"name": "X", "type": "int32"}], "rows": [[1]]}],
        "operations": [{"name": "w", "op": "where", "source": "t", "filters": ["X > 0"]},
                       {"name": "snap", "op": "snapshot", "source": "t"},
                       {"name": "after", "op": "where", "source": "snap", "filters": ["X > 0"]}],
        "results": [{"table": "w"}, {"table": "snap"}, {"table": "after"}],
        "steps": [{"actions": [{"action": "add", "table": "t", "rows": [[2]]}]}],
    })
    columns = scenario.inputs[0].columns
    one, two = sc.arrow_table(columns, [[1]]), sc.arrow_table(columns, [[1], [2]])
    kept, fresh = ResultSet("s"), ResultSet("s")
    for table in ("w", "snap", "after"):
        kept.put(0, table, one)
        kept.put(1, table, one)  # nothing was kept up to date
        fresh.put(1, table, two)
    found = rules.compare_with_fresh(scenario, kept, fresh)
    # The snapshot and what reads it depend on when they were made: not compared.
    assert {(m.step, m.table) for m in found} == {(1, "w")}
    kept.put(1, "w", two)
    assert rules.compare_with_fresh(scenario, kept, fresh) == []
