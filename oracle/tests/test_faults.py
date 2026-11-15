"""The comparator reports every injected fault that it must, and only those.

Needs no server. The faults are applied to hand-built tables and to every result of the recorded
Java results under corpus/, both as ordered results and as unordered ones.
"""

from __future__ import annotations

import math
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pytest

from harness import faults, rules
from harness import results as res
from harness import scenario as sc
from harness.results import Failure, ResultSet

ORACLE = Path(__file__).resolve().parent.parent
SCENARIOS = sc.load_all(ORACLE / "scenarios")
# Faults that change exactly one cell, which an ordered comparison must name.
CELL_FAULTS = {
    "changed_value", "null_to_value", "value_to_null", "double_beyond_tolerance",
    "float_beyond_tolerance", "exact_float_one_ulp", "exact_float_zero_sign",
}
ACCEPTED = {"double_inside_tolerance", "float_inside_tolerance", "computed_float_zero_sign"}
# The computed columns of `sample()`.
COMPUTED = frozenset({"C", "F"})


def _field(name: str, arrow_type: pa.DataType, engine_type: str) -> pa.Field:
    return pa.field(name, arrow_type, metadata={"deephaven:type": engine_type})


def sample() -> pa.Table:
    """K, S, B, T and D compare exactly; C and F are computed (rules.computed_columns)."""
    schema = pa.schema(
        [
            _field("K", pa.int32(), "int"),
            _field("S", pa.string(), "java.lang.String"),
            _field("D", pa.float64(), "double"),
            _field("C", pa.float64(), "double"),
            _field("F", pa.float32(), "float"),
            _field("B", pa.bool_(), "java.lang.Boolean"),
            _field("T", pa.timestamp("ns", tz="UTC"), "java.time.Instant"),
        ]
    )
    return pa.Table.from_arrays(
        [
            pa.array([1, 2, None, 2147483647, 5], pa.int32()),
            pa.array(["a", None, "", "z", "q"], pa.string()),
            pa.array([0.0, 1234.5, math.nan, None, 2.0], pa.float64()),
            pa.array([0.0, 1234.5, math.nan, None, 7.0], pa.float64()),
            pa.array([0.0, 2.5, math.nan, None, 1000.0], pa.float32()),
            pa.array([True, None, False, True, False], pa.bool_()),
            pa.array([1, 2, None, 4, 5], pa.timestamp("ns", tz="UTC")),
        ],
        schema=schema,
    )


def one_table(data: pa.Table | Failure) -> ResultSet:
    results = ResultSet("synthetic")
    results.put(2, "r", data)
    return results


def check(original: ResultSet, step: int, table: str, name: str, ordered: bool,
          computed: frozenset[str] = COMPUTED) -> bool:
    """Injects one fault and checks the comparator's verdict. False when not applicable."""
    fault = faults.FAULTS[name]
    try:
        damaged, change = faults.inject(original, step, table, name,
                                        faults.Context(computed=computed))
    except faults.NotApplicable:
        return False
    unordered = set() if ordered else {table}
    found = rules.compare(original, damaged, unordered, {table: computed})
    where = f"{original.scenario} step {step} table {table} fault {name} ordered={ordered}"

    if not fault.flagged(ordered):
        assert found == [], f"{where}: reported a fault that must be accepted: {found}"
        return True

    assert found, f"{where}: the fault was not reported"
    for mismatch in found:
        assert (mismatch.scenario, mismatch.table, mismatch.step) == (
            original.scenario, table, step), where
    kinds = [m.kind for m in found]
    if name in CELL_FAULTS and ordered:
        # Exactly the damaged cell, with its row and both values.
        assert kinds == [rules.VALUE], f"{where}: {found}"
        assert (found[0].column, found[0].row) == (change.column, change.row), where
        assert _same(found[0].expected, change.before), f"{where}: {found[0]}"
        assert _same(found[0].actual, change.after), f"{where}: {found[0]}"
    elif name in CELL_FAULTS or name == "nan_to_null":
        # In an unordered result the damaged row can sort to another position, which shows
        # as cell mismatches in several rows and columns.
        assert set(kinds) <= {rules.VALUE, rules.TRUNCATED}, f"{where}: {found}"
        if name == "nan_to_null" and ordered:
            assert change.column in {m.column for m in found}, where
    elif name == "swapped_rows":
        assert set(kinds) <= {rules.VALUE, rules.TRUNCATED}, where
        assert change.row in {m.row for m in found}, where
    elif name in ("dropped_row", "extra_row", "truncated_after_three"):
        assert rules.ROW_COUNT in kinds, where
        count = original.get(step, table).num_rows
        after = {"extra_row": count + 1, "dropped_row": count - 1, "truncated_after_three": 3}
        row_count = found[kinds.index(rules.ROW_COUNT)]
        assert (row_count.expected, row_count.actual) == (count, after[name]), where
    elif name == "renamed_column":
        assert kinds == [rules.COLUMN_NAME], where
        assert (found[0].expected, found[0].actual) == (change.before, change.after), where
    elif name == "changed_column_type":
        assert kinds == [rules.COLUMN_TYPE], where
        assert found[0].column == change.column, where
        assert (found[0].expected, found[0].actual) == (change.before, change.after), where
    elif name == "changed_engine_type":
        assert kinds == [rules.COLUMN_METADATA], where
        assert found[0].column == change.column, where
    elif name == "changed_table_attribute":
        assert kinds == [rules.TABLE_METADATA], where
    else:
        raise AssertionError(f"no expectation written for fault {name}")
    return True


def check_result_fault(original: ResultSet, step: int, table: str, name: str) -> bool:
    """The same for the faults on whether a result failed, and with which status code."""
    try:
        damaged, change = faults.inject(original, step, table, name)
    except faults.NotApplicable:
        return False
    found = rules.compare(original, damaged)
    where = f"{original.scenario} step {step} table {table} fault {name}"
    expected_kind = {
        "failure_to_table": rules.FAILURE_EXPECTED,
        "table_to_failure": rules.FAILURE_UNEXPECTED,
        "failure_code_changed": rules.FAILURE_CODE,
    }[name]
    assert [(m.kind, m.step, m.table) for m in found] == [(expected_kind, step, table)], (
        f"{where}: {found}"
    )
    if name == "failure_code_changed":
        assert (found[0].expected, found[0].actual) == (change.before.code, change.after.code)
    return True


def _same(a, b) -> bool:
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    return a == b and type(a) is type(b)


@pytest.mark.parametrize("ordered", [True, False])
@pytest.mark.parametrize("name", sorted(faults.FAULTS))
def test_fault_on_a_hand_built_table(name, ordered):
    assert check(one_table(sample()), 2, "r", name, ordered), "fault did not apply"


def test_result_faults_on_hand_built_results():
    table, failure = one_table(sample()), one_table(Failure("INVALID_ARGUMENT"))
    assert check_result_fault(table, 2, "r", "table_to_failure")
    assert check_result_fault(failure, 2, "r", "failure_to_table")
    assert check_result_fault(failure, 2, "r", "failure_code_changed")
    assert check_result_fault(one_table(Failure("INTERNAL")), 2, "r", "failure_code_changed")
    assert not check_result_fault(failure, 2, "r", "table_to_failure")
    assert not check_result_fault(table, 2, "r", "failure_to_table")
    assert not check_result_fault(table, 2, "r", "failure_code_changed")
    # A table fault has nothing to change in a failed result.
    assert not check(failure, 2, "r", "changed_value", ordered=True)


def test_fault_list_and_which_faults_are_accepted():
    assert set(faults.FAULTS) >= {
        "changed_value", "swapped_rows", "null_to_value", "value_to_null", "dropped_row",
        "extra_row", "renamed_column", "changed_column_type",
    } | CELL_FAULTS | ACCEPTED
    accepted = {n for n, f in faults.FAULTS.items() if not f.flagged(ordered=True)}
    assert accepted == ACCEPTED
    accepted_unordered = {n for n, f in faults.FAULTS.items() if not f.flagged(ordered=False)}
    assert accepted_unordered == ACCEPTED | {"swapped_rows"}
    assert set(faults.ALL_FAULTS) == set(faults.FAULTS) | set(faults.RESULT_FAULTS)


@pytest.mark.parametrize(
    ("kind", "arrow_type", "low", "high"),
    [("double", pa.float64(), 1e-9, 1e-8), ("float", pa.float32(), 1e-5, 1e-4)],
)
def test_tolerance_faults_sit_on_both_sides_of_the_boundary(kind, arrow_type, low, high):
    ctx = faults.Context(computed=COMPUTED)
    tolerance = rules.DEFAULT_RULES.tolerance(arrow_type)
    data = sample()
    inside = faults.FAULTS[f"{kind}_inside_tolerance"].apply(data, ctx)
    beyond = faults.FAULTS[f"{kind}_beyond_tolerance"].apply(data, ctx)
    for change in (inside, beyond):
        assert change.after != change.before
    assert rules.floats_within(inside.before, inside.after, tolerance)
    assert not rules.floats_within(beyond.before, beyond.after, tolerance)
    # The cell at zero exercises the absolute floor, a large one the relative tolerance.
    assert inside.before == 0.0 and 0 < inside.after < tolerance.absolute
    relative = pa.table({"C": pa.array([1234.5], arrow_type)})
    step = faults.FAULTS[f"{kind}_beyond_tolerance"].apply(relative, ctx)
    assert low < (step.after - step.before) / step.before < high


def test_tolerance_faults_need_a_computed_column():
    """The same cell moved by the same amount is a fault when the column is not computed."""
    none = faults.Context()
    for name in ("double_inside_tolerance", "double_beyond_tolerance",
                 "float_inside_tolerance", "computed_float_zero_sign"):
        with pytest.raises(faults.NotApplicable):
            faults.FAULTS[name].apply(sample(), none)
    inside = faults.FAULTS["double_inside_tolerance"].apply(
        sample(), faults.Context(computed=COMPUTED))
    assert rules.compare_tables("s", "r", 0, sample(), inside.table, computed=COMPUTED) == []
    found = rules.compare_tables("s", "r", 0, sample(), inside.table)
    assert [(m.kind, m.column) for m in found] == [(rules.VALUE, "C")]


def test_empty_table_faults():
    empty = sample().slice(0, 0)
    applicable = {"extra_row", "renamed_column", "changed_column_type", "changed_engine_type",
                  "changed_table_attribute"}
    for name in faults.FAULTS:
        assert check(one_table(empty), 2, "r", name, ordered=True) == (name in applicable), name


@pytest.mark.parametrize("ordered", [True, False])
def test_faults_on_a_char_column(ordered):
    """A `char` column is Arrow `uint16`: its largest code unit is beyond a short."""
    schema = pa.schema([_field("C", pa.uint16(), "char")])
    chars = pa.Table.from_arrays([pa.array([65535, None, 97], pa.uint16())], schema=schema)
    applied = {name for name in sorted(faults.FAULTS)
               if check(one_table(chars), 2, "r", name, ordered, computed=frozenset())}
    assert {"changed_value", "null_to_value", "value_to_null", "swapped_rows",
            "changed_column_type"} <= applied
    damaged, change = faults.inject(one_table(chars), 2, "r", "changed_value")
    assert (change.before, change.after) == (65535, 65534)
    assert damaged.get(2, "r").schema.field("C").type == pa.uint16()


def test_swapping_equal_rows_is_not_a_fault():
    same = pa.table({"K": pa.array([1, 1], pa.int32())})
    with pytest.raises(faults.NotApplicable):
        faults.inject(one_table(same), 2, "r", "swapped_rows")


def test_a_fault_touches_only_its_target():
    results = one_table(sample())
    results.put(3, "r", sample())
    damaged, _ = faults.inject(results, 2, "r", "changed_value")
    assert damaged.get(3, "r") is results.get(3, "r")
    assert rules.compare(results, one_table(sample()).copy()) != [], "sanity: step 3 is extra"
    assert rules.compare_tables("s", "r", 2, results.get(2, "r"), sample()) == [], (
        "inject must not change its input"
    )
    assert [(m.step, m.table) for m in rules.compare(results, damaged)] == [(2, "r")]


class _Fixed:
    """A target that returns prepared results, in place of a server."""

    def __init__(self, results: ResultSet):
        self.results = results

    def run(self, scenario) -> ResultSet:
        return self.results.copy()


def test_faulty_target_damages_only_the_named_operation():
    doc = {
        "format": 1,
        "name": "synthetic",
        "inputs": [{"name": "t", "columns": [{"name": "K", "type": "int32"}], "rows": [[1]]}],
        "operations": [
            {"name": "w", "op": "where", "source": "t", "filters": ["K > 0"]},
            {"name": "h", "op": "head", "source": "t", "rows": 1},
        ],
        "results": [{"table": "w"}, {"table": "h"}],
    }
    scenario = sc.parse(doc)
    data = pa.table({"K": pa.array([1], pa.int32())})
    results = ResultSet("synthetic")
    results.put(0, "w", data)
    results.put(0, "h", data)

    everywhere = faults.FaultyTarget("dropped_row", _Fixed(results)).run(scenario)
    assert {m.table for m in rules.compare_scenario(scenario, results, everywhere)} == {"w", "h"}
    only_head = faults.FaultyTarget("dropped_row:head", _Fixed(results)).run(scenario)
    assert {m.table for m in rules.compare_scenario(scenario, results, only_head)} == {"h"}
    # A fault with nothing to change leaves the results alone.
    untouched = faults.FaultyTarget("nan_to_null", _Fixed(results)).run(scenario)
    assert rules.compare_scenario(scenario, results, untouched) == []
    with pytest.raises(ValueError):
        faults.FaultyTarget("no_such_fault", _Fixed(results))


# ---------------------------------------------------------------- the recorded corpus

APPLIED: Counter = Counter()


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.name)
def test_faults_on_recorded_results(scenario):
    recorded = res.load(ORACLE / "corpus" / scenario.name, scenario.name, scenario.digest())
    assert rules.compare_scenario(scenario, recorded, recorded) == []
    assert recorded.keys(), "a recording without results proves nothing"
    computed = rules.computed_columns(scenario.operations)
    for step, table in recorded.keys():
        for name in faults.RESULT_FAULTS:
            if check_result_fault(recorded, step, table, name):
                APPLIED[(name, True)] += 1
                APPLIED[(name, False)] += 1
        for name in faults.FAULTS:
            for ordered in (True, False):
                if check(recorded, step, table, name, ordered,
                         computed.get(table, frozenset())):
                    APPLIED[(name, ordered)] += 1


def test_every_fault_was_exercised_on_the_corpus():
    """Runs after the corpus tests above (pytest keeps file order)."""
    missing = [
        (name, ordered)
        for name in faults.ALL_FAULTS
        for ordered in (True, False)
        if APPLIED[(name, ordered)] == 0
    ]
    assert not missing, f"faults never applied to any recorded result: {missing}"
    print("\nfault applications on recorded results:")
    for name in faults.ALL_FAULTS:
        fault = faults.FAULTS.get(name)
        verdict = [
            "flagged" if fault is None or fault.flagged(ordered) else "accepted"
            for ordered in (True, False)
        ]
        print(f"  {name:26} ordered: {APPLIED[(name, True)]:4} ({verdict[0]})   "
              f"unordered: {APPLIED[(name, False)]:4} ({verdict[1]})")
