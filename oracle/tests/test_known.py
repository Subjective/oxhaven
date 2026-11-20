"""The fuzzer's known findings (`harness/known.py`): what an entry fits, what it must not
fit, and how the `fuzz` command reports the two apart. Needs no server."""

from __future__ import annotations

import json
import math
from pathlib import Path

import pyarrow as pa
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


def _inconsistent_reference(tmp_path):
    """A scenario of one `where`, the results of a reference that kept one row where it
    answers two from scratch, and a triage with an entry for that."""
    import argparse

    scenario = sc.parse({
        "format": 1, "name": "s",
        "inputs": [{"name": "t", "ticking": {"key_columns": []},
                    "columns": [{"name": "X", "type": "int32"}], "rows": [[1]]}],
        "operations": [{"name": "w", "op": "where", "source": "t", "filters": ["X > 0"]}],
        "results": [{"table": "w"}],
        "steps": [{"actions": [{"action": "add", "table": "t", "rows": [[2]]}]}],
    })
    columns = scenario.inputs[0].columns
    tables = {rows: sc.arrow_table(columns, [[value] for value in range(1, rows + 1)])
              for rows in (1, 2, 3)}
    kept, fresh = ResultSet("s"), ResultSet("s")
    kept.put(0, "w", tables[1])
    kept.put(1, "w", tables[1])
    fresh.put(1, "w", tables[2])

    class Reference:
        def run_fresh(self, asked):
            assert asked is scenario
            return kept, fresh

    path = tmp_path / "known.json"
    path.write_text(json.dumps({"format": 1, "findings": [
        {"id": "kept", "class": "reference-defect",
         "reason": "the reference keeps a result it does not answer from scratch",
         "match": {"kind": "row_count", "operation": "where", "steps": "later",
                   "requires": "reference_inconsistent"}}]}))
    triage = cli._Triage(argparse.Namespace(known=str(path), no_known=False), Reference())

    def split(rows: int | None):
        """The mismatches left and the findings fitted when the actual side holds `rows`
        rows after the step; None compares without the actual side's results."""
        compared = ResultSet("s")
        compared.put(0, "w", tables[1])
        compared.put(1, "w", tables[rows or 2])
        found = rules.compare_scenario(scenario, kept, compared)
        assert [m.kind for m in found] == ["row_count"]
        actual = None if rows is None else compared
        left, fitted = triage.split(scenario, found, actual=actual)
        return len(left), [finding.id for finding in fitted]

    return scenario, fresh, tables, split


def test_an_inconsistent_reference_excuses_only_its_answer_from_scratch(tmp_path):
    _, _, _, split = _inconsistent_reference(tmp_path)
    # The actual side holds what the reference answers from scratch: the known finding.
    assert split(2) == (0, ["kept"])
    # It holds neither of the reference's two answers: a difference.
    assert split(3) == (1, [])
    # Without the actual side's results only the reference is asked.
    assert split(None) == (0, ["kept"])


def test_differs_from_fresh_names_what_the_actual_side_holds_differently(tmp_path):
    scenario, fresh, tables, _ = _inconsistent_reference(tmp_path)
    actual = ResultSet("s")
    actual.put(1, "w", tables[2])
    assert rules.differs_from_fresh(scenario, fresh, actual, [(1, "w")]) == set()
    actual.put(1, "w", tables[3])
    assert rules.differs_from_fresh(scenario, fresh, actual, [(1, "w")]) == {(1, "w")}
    # A result that one side does not hold is not known to agree.
    assert rules.differs_from_fresh(scenario, fresh, actual, [(0, "w")]) == {(0, "w")}


def test_an_inconsistent_reference_in_any_order_excuses_only_the_rows_from_scratch(tmp_path):
    """`reference_inconsistent_rows`: the actual side holds the rows of the answer from
    scratch in another order (an aggregation's groups after a key changed)."""
    scenario, fresh, tables, _ = _inconsistent_reference(tmp_path)
    kept = ResultSet("s")
    kept.put(1, "w", tables[1])
    swapped = sc.arrow_table(scenario.inputs[0].columns, [[2], [1]])
    found = known({"id": "rows", "class": "allowed-difference",
                   "reason": "the reference keeps a result it does not answer from scratch",
                   "match": {"kind": "row_count", "operation": "where",
                             "requires": "reference_inconsistent_rows"}})
    mismatch = rules.Mismatch("s", "w", 1, "row_count", expected=1, actual=2)
    for actual, ordered, rows in ((tables[2], True, True), (swapped, False, True),
                                  (tables[3], False, False)):
        result = ResultSet("s")
        result.put(1, "w", actual)

        def ask(any_order: bool = False, result=result):
            both = {(m.step, m.table) for m in rules.compare_with_fresh(
                scenario, kept, fresh, any_order=any_order)}
            return both - rules.differs_from_fresh(scenario, fresh, result, both,
                                                   any_order=any_order)

        assert bool(ask()) == ordered
        assert fits(found, mismatch, reference_inconsistent=ask) == ["rows" if rows else None]
    # An entry that requires the ordered form is not excused by rows in another order.
    ordered_entry = known({"id": "kept", "class": "reference-defect",
                           "reason": "the reference keeps a result it does not answer from scratch",
                           "match": {"kind": "row_count", "operation": "where",
                                     "requires": "reference_inconsistent"}})
    result = ResultSet("s")
    result.put(1, "w", swapped)
    assert fits(ordered_entry, mismatch, reference_inconsistent=lambda any_order=False: (
        {(1, "w")} - rules.differs_from_fresh(scenario, fresh, result, [(1, "w")],
                                              any_order=any_order))) == [None]


def test_operation_and_producer_may_be_lists():
    entry = dict(MIN_ZERO, match=dict(MIN_ZERO["match"], operation=["agg_all_by", "agg_by"],
                                      producer=["max", "min"]))
    found = known(entry)
    assert fits(found, value("a", "Low", -0.0, 0.0), value("a", "High", -0.0, 0.0),
                value("all", "X", -0.0, 0.0), value("a", "N", -0.0, 0.0),
                value("other", "X", -0.0, 0.0)) == ["min-zero"] * 3 + [None, None]
    for bad in ([], [""], "", ["agg_by", "no_such_operation"]):
        with pytest.raises(kn.KnownError):
            known(dict(MIN_ZERO, match=dict(MIN_ZERO["match"], operation=bad)))
    with pytest.raises(kn.KnownError):
        known(dict(MIN_ZERO, match=dict(MIN_ZERO["match"], producer=[])))
    with pytest.raises(kn.KnownError):
        known(dict(MIN_ZERO, match=dict(MIN_ZERO["match"], shape="no_such_shape")))
    with pytest.raises(kn.KnownError):
        known(dict(MIN_ZERO, match=dict(MIN_ZERO["match"], differs_in="anything")))
    with pytest.raises(kn.KnownError):
        known(dict(MIN_ZERO, match=dict(MIN_ZERO["match"], requires="anything")))


def test_differs_in_zero_sign_takes_only_vectors_that_differ_in_the_sign_of_a_zero():
    nan = math.nan
    assert kn.differs_in_zero_sign([0.0, 1.5], [-0.0, 1.5])
    assert kn.differs_in_zero_sign([[-0.0], [nan, 2.0]], [[0.0], [nan, 2.0]])
    assert not kn.differs_in_zero_sign([0.0, 1.5], [0.0, 1.5])  # nothing differs
    assert not kn.differs_in_zero_sign([0.0, 1.5], [-0.0, 2.5])  # something else does
    assert not kn.differs_in_zero_sign([0.0], [-0.0, 0.0])  # another length
    assert not kn.differs_in_zero_sign([0.0], [None])
    assert not kn.differs_in_zero_sign([nan], [0.0])
    assert not kn.differs_in_zero_sign(0.0, -0.0)  # not a vector
    assert not kn.differs_in_zero_sign([0], [0.0])
    entry = {"id": "zero", "class": "allowed-difference",
             "reason": "the rolling minimum keeps another of two equal vectors",
             "match": {"kind": "value", "operation": "update_by", "producer": "emstd_time",
                       "differs_in": "zero_sign"}}
    assert fits(known(entry), value("u", "E", [0.0], [-0.0]), value("u", "E", [0.0], [1.0]),
                value("u", "E", 0.0, -0.0)) == ["zero", None, None]


# ---------------------------------------------------------------- shapes

FINDINGS = Path(__file__).resolve().parents[2] / "tasks" / "12.1-fuzzer-coverage"


def _scene(file: str, lists: dict[str, dict[str, pa.DataType]] | None = None,
           static: bool = False) -> kn.Scene:
    """The scene of a scenario of task 12.1, with actual results that have the vector
    columns `lists` gives per table; `static` makes every input static."""
    doc = json.loads((FINDINGS / file).read_text())
    if static:
        for item in doc["inputs"]:
            item.pop("ticking", None)
        doc.pop("steps", None)
    actual = ResultSet(doc["name"])
    for table, columns in (lists or {}).items():
        actual.put(0, table, pa.table({name: pa.array([], kind)
                                       for name, kind in columns.items()}))
    return kn.Scene(sc.parse(doc), actual)


def _shape(scene: kn.Scene, table: str, column: str | None = None) -> list[str]:
    mismatch = rules.Mismatch("s", table, 1, "value", column, 0, 1, 2)
    return sorted(name for name, test in kn.SHAPES.items() if test(scene, mismatch))


def test_each_shape_fits_the_case_of_its_allowed_difference_and_no_other():
    ints = pa.list_(pa.int32())
    shorts = pa.list_(pa.int16())
    # A24 (F7): the three ungroups, and only these.
    scene = _scene("F7_ungroup_after_aggregations_over_vectors.json",
                   {"a": {"K": shorts, "A8": ints}, "p1": {"A": shorts, "D": ints}})
    assert _shape(scene, "f5") == ["ungroup_again_of_a_distinct"]
    assert _shape(scene, "f4") == []
    assert _shape(scene, "u") == ["null_fill_ungroup_of_vector_keys"]
    assert _shape(scene, "p") == ["null_fill_ungroup_of_a_distinct"]
    for table in ("u", "p"):
        scene.by_name[table] = dict(scene.by_name[table], null_fill=False)
        assert _shape(scene, table) == []
    # Without the actual side's schema an ungroup of every vector column is not known.
    unknown = _scene("F7_ungroup_after_aggregations_over_vectors.json")
    assert _shape(unknown, "u") == [] and _shape(unknown, "p") == []
    # A22 (F2, F8): what holds a refreshing group's vectors, and what is made from it.
    scene = _scene("F2_vector_of_a_refreshing_group_as_key.json")
    for table in ("a", "sd", "u"):
        assert _shape(scene, table) == ["refreshing_group_vectors"], table
    assert "refreshing_group_vectors" in _shape(scene, "dv")
    assert _shape(scene, "g") == []
    assert _shape(_scene("F2_vector_of_a_refreshing_group_as_key.json", static=True), "a") == []
    scene = _scene("F8_minimum_of_vectors_of_a_refreshing_group_read_on.json")
    assert _shape(scene, "o11") == ["refreshing_group_vectors"]
    assert _shape(scene, "o1") == []
    # A23 (F3, F6): the columns, not the tables.
    scene = _scene("F3_ungroup_of_a_minimum_of_string_vectors.json")
    assert _shape(scene, "um", "M") == ["ungrouped_extreme"]
    assert _shape(scene, "ur", "R") == ["ungrouped_extreme"]
    assert _shape(scene, "um", "K") == [] and _shape(scene, "u", "M") == []
    scene = _scene("F4_F6_conditions_between_windows_of_strings.json")
    assert _shape(scene, "c1", "U") == ["element_of_a_condition"]
    assert _shape(scene, "c2", "U") == ["element_of_a_condition"]
    assert _shape(scene, "f", "Y") == ["element_of_a_condition"]
    assert _shape(scene, "f", "X") == [] and _shape(scene, "s2", "U") == []
    # A13 (F1): the chain, not the join of the input table.
    scene = _scene("F1_asof_static_left_right_change_off_its_columns.json")
    assert _shape(scene, "o4") == ["as_of_chain"]
    assert _shape(scene, "o1") == []
    # A20: the vectors of a distinct, taken apart.
    scene = _scene("F2_vector_of_a_refreshing_group_as_key.json")
    assert "distinct_taken_apart" in _shape(scene, "dv", "D")
    assert "distinct_taken_apart" not in _shape(scene, "a", "C")


def test_the_repository_entries_with_a_shape_fit_their_findings():
    """The entries of the repository file for A23 and A24 fit the mismatches their
    findings' scenarios showed against the reference, and nothing near them."""
    found = kn.Known(kn.load(cli.KNOWN))
    scene = _scene("F3_ungroup_of_a_minimum_of_string_vectors.json")
    object_type = {"deephaven:type": "java.lang.Object"}
    string_type = {"deephaven:type": "java.lang.String", "deephaven:isSortable": "true"}
    mismatch = rules.Mismatch("s", "um", 0, "column_metadata", "M", None, object_type,
                              string_type)
    operations = list(scene.by_name.values())
    assert [f.id for f in found.classify([mismatch], operations, scene=scene)] == [
        "string-vector-extreme-type"]
    # Without the scene, or with other metadata, it fits nothing.
    assert found.classify([mismatch], operations) == [None]
    other = rules.Mismatch("s", "um", 0, "column_metadata", "M", None, object_type,
                           {"deephaven:type": "java.lang.String"})
    assert found.classify([other], operations, scene=scene) == [None]
    scene = _scene("F7_ungroup_after_aggregations_over_vectors.json",
                   {"a": {"K": pa.list_(pa.int16()), "A8": pa.list_(pa.int32())},
                    "p1": {"A": pa.list_(pa.int16()), "D": pa.list_(pa.int32())}})
    failures = [rules.Mismatch("s", table, 0, "failure_expected", None, None,
                               "fails with INVALID_ARGUMENT", "succeeds")
                for table in ("f5", "u", "p", "f4")]
    assert [f.id if f else None for f in found.classify(
        failures, list(scene.by_name.values()), scene=scene)] == [
        "ungroup-again-of-a-distinct", "null-fill-ungroup-of-vector-keys",
        "null-fill-ungroup-of-a-distinct", None]
