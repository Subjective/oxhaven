"""Gate B: the comparator reports every injected fault that it must, and only those.

Needs no server. The faults are applied to hand-built tables and to every table of the recorded
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
from harness.results import ResultSet

ORACLE = Path(__file__).resolve().parent.parent
SCENARIOS = sc.load_all(ORACLE / "scenarios")
CELL_FAULTS = {"changed_value", "null_to_value", "value_to_null", "float_beyond_tolerance"}


def _field(name: str, arrow_type: pa.DataType, engine_type: str) -> pa.Field:
    return pa.field(name, arrow_type, metadata={"deephaven:type": engine_type})


def sample() -> pa.Table:
    schema = pa.schema(
        [
            _field("K", pa.int32(), "int"),
            _field("S", pa.string(), "java.lang.String"),
            _field("D", pa.float64(), "double"),
            _field("B", pa.bool_(), "java.lang.Boolean"),
            _field("T", pa.timestamp("ns", tz="UTC"), "java.time.Instant"),
        ]
    )
    return pa.Table.from_arrays(
        [
            pa.array([1, 2, None, 2147483647], pa.int32()),
            pa.array(["a", None, "", "z"], pa.string()),
            pa.array([0.0, 1234.5, math.nan, None], pa.float64()),
            pa.array([True, None, False, True], pa.bool_()),
            pa.array([1, 2, None, 4], pa.timestamp("ns", tz="UTC")),
        ],
        schema=schema,
    )


def one_table(data: pa.Table) -> ResultSet:
    results = ResultSet("synthetic")
    results.put(2, "r", data)
    return results


def check(original: ResultSet, step: int, table: str, name: str, ordered: bool) -> bool:
    """Injects one fault and checks the comparator's verdict. False when not applicable."""
    fault = faults.FAULTS[name]
    try:
        damaged, change = faults.inject(original, step, table, name)
    except faults.NotApplicable:
        return False
    unordered = set() if ordered else {table}
    found = rules.compare(original, damaged, unordered)
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
    elif name in CELL_FAULTS:
        # In an unordered result the damaged row can sort to another position, which shows
        # as cell mismatches in several rows and columns.
        assert set(kinds) <= {rules.VALUE, rules.TRUNCATED}, f"{where}: {found}"
    elif name == "swapped_rows":
        assert set(kinds) <= {rules.VALUE, rules.TRUNCATED}, where
        assert change.row in {m.row for m in found}, where
    elif name in ("dropped_row", "extra_row"):
        assert rules.ROW_COUNT in kinds, where
        count = original.get(step, table).num_rows
        row_count = found[kinds.index(rules.ROW_COUNT)]
        assert row_count.expected == count, where
        assert row_count.actual == count + (1 if name == "extra_row" else -1), where
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
    else:
        raise AssertionError(f"no expectation written for fault {name}")
    return True


def _same(a, b) -> bool:
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    return a == b and type(a) is type(b)


@pytest.mark.parametrize("ordered", [True, False])
@pytest.mark.parametrize("name", sorted(faults.FAULTS))
def test_fault_on_a_hand_built_table(name, ordered):
    assert check(one_table(sample()), 2, "r", name, ordered), "fault did not apply"


def test_fault_list_covers_the_required_faults():
    assert set(faults.FAULTS) >= {
        "changed_value", "swapped_rows", "null_to_value", "value_to_null", "dropped_row",
        "extra_row", "renamed_column", "changed_column_type", "float_beyond_tolerance",
        "float_inside_tolerance",
    }
    accepted = {n for n, f in faults.FAULTS.items() if not f.flagged(ordered=True)}
    assert accepted == {"float_inside_tolerance"}
    accepted_unordered = {n for n, f in faults.FAULTS.items() if not f.flagged(ordered=False)}
    assert accepted_unordered == {"float_inside_tolerance", "swapped_rows"}


def test_tolerance_faults_sit_on_both_sides_of_the_boundary():
    data = sample()
    inside = faults.FAULTS["float_inside_tolerance"].apply(data, rules.DEFAULT_RULES)
    beyond = faults.FAULTS["float_beyond_tolerance"].apply(data, rules.DEFAULT_RULES)
    for change in (inside, beyond):
        assert change.after != change.before
    assert rules.floats_equal(inside.before, inside.after)
    assert not rules.floats_equal(beyond.before, beyond.after)
    # The cell at zero exercises the absolute floor, the next one the relative tolerance.
    assert inside.before == 0.0 and 0 < inside.after < rules.DEFAULT_RULES.float_absolute_floor
    relative = pa.table({"D": pa.array([1234.5])})
    step = faults.FAULTS["float_beyond_tolerance"].apply(relative, rules.DEFAULT_RULES)
    assert 1e-9 < (step.after - step.before) / step.before < 1e-8


def test_empty_table_faults():
    empty = sample().slice(0, 0)
    for name in ("extra_row", "renamed_column", "changed_column_type", "changed_engine_type"):
        assert check(one_table(empty), 2, "r", name, ordered=True)
    for name in ("changed_value", "swapped_rows", "null_to_value", "value_to_null",
                 "dropped_row", "float_beyond_tolerance", "float_inside_tolerance"):
        assert not check(one_table(empty), 2, "r", name, ordered=True)


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


# ---------------------------------------------------------------- the recorded corpus

APPLIED: Counter = Counter()


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s.name)
def test_faults_on_recorded_results(scenario):
    recorded = res.load(ORACLE / "corpus" / scenario.name, scenario.name, scenario.digest())
    assert rules.compare(recorded, recorded) == []
    assert recorded.keys(), "a recording without tables proves nothing"
    for step, table in recorded.keys():
        for name in faults.FAULTS:
            for ordered in (True, False):
                if check(recorded, step, table, name, ordered):
                    APPLIED[(name, ordered)] += 1


def test_every_fault_was_exercised_on_the_corpus():
    """Runs after the corpus tests above (pytest keeps file order)."""
    missing = [
        (name, ordered)
        for name in faults.FAULTS
        for ordered in (True, False)
        if APPLIED[(name, ordered)] == 0
    ]
    assert not missing, f"faults never applied to any recorded table: {missing}"
    print("\nfault applications on recorded tables:")
    for name in faults.FAULTS:
        flagged = faults.FAULTS[name]
        print(f"  {name:24} ordered: {APPLIED[(name, True)]:4} "
              f"({'flagged' if flagged.flagged(True) else 'accepted'})   "
              f"unordered: {APPLIED[(name, False)]:4} "
              f"({'flagged' if flagged.flagged(False) else 'accepted'})")
