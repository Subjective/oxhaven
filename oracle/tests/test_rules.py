"""The comparison rules on hand-built tables. Needs no server."""

from __future__ import annotations

import math

import pyarrow as pa
import pytest

from harness import rules
from harness.results import ResultSet

NAN = math.nan


def table(**columns: pa.Array) -> pa.Table:
    return pa.table(columns)


def diff(expected: pa.Table, actual: pa.Table, ordered: bool = True) -> list[rules.Mismatch]:
    return rules.compare_tables("s", "t", 3, expected, actual, ordered=ordered)


def kinds(found: list[rules.Mismatch]) -> list[str]:
    return [m.kind for m in found]


BASE = table(
    I=pa.array([1, None, -5], pa.int32()),
    L=pa.array([2**62, 0, None], pa.int64()),
    D=pa.array([1.5, NAN, None], pa.float64()),
    S=pa.array(["a", None, ""], pa.string()),
    B=pa.array([True, False, None], pa.bool_()),
    T=pa.array([1, None, 2], pa.timestamp("ns", tz="UTC")),
)


def test_identical_tables_are_equal():
    assert diff(BASE, BASE) == []


def test_null_equals_null_and_nan_equals_nan():
    a = table(D=pa.array([None, NAN], pa.float64()))
    b = table(D=pa.array([None, NAN], pa.float64()))
    assert diff(a, b) == []


@pytest.mark.parametrize(
    ("expected", "actual"),
    [(None, NAN), (NAN, None), (None, 0.0), (0.0, None), (NAN, 0.0), (0.0, NAN)],
)
def test_null_nan_and_zero_are_distinct(expected, actual):
    found = diff(
        table(D=pa.array([expected], pa.float64())), table(D=pa.array([actual], pa.float64()))
    )
    assert kinds(found) == [rules.VALUE]


def test_null_is_not_an_integer_zero_or_an_empty_string():
    assert kinds(diff(table(I=pa.array([None], pa.int32())), table(I=pa.array([0], pa.int32()))))
    assert kinds(diff(table(S=pa.array([None], pa.string())), table(S=pa.array([""], pa.string()))))
    assert kinds(diff(table(B=pa.array([None], pa.bool_())), table(B=pa.array([False], pa.bool_()))))


def test_infinities_match_only_with_the_same_sign():
    inf = table(D=pa.array([math.inf], pa.float64()))
    assert diff(inf, inf) == []
    assert kinds(diff(inf, table(D=pa.array([-math.inf], pa.float64())))) == [rules.VALUE]
    assert kinds(diff(inf, table(D=pa.array([1.7976931348623157e308], pa.float64())))) == [
        rules.VALUE
    ]


@pytest.mark.parametrize(
    ("expected", "actual", "equal"),
    [
        (1.0, 1.0 + 5e-10, True),
        (1.0, 1.0 + 2e-9, False),
        (1e20, 1e20 * (1 + 5e-10), True),
        (1e20, 1e20 * (1 + 2e-9), False),
        (-3.0, -3.0 * (1 + 5e-10), True),
        (-3.0, 3.0, False),
        (0.0, 5e-13, True),
        (0.0, 2e-12, False),
        (0.0, -0.0, True),
        (1e-13, 3e-13, True),
        (1e-300, 2e-300, True),
    ],
)
def test_float_tolerance(expected, actual, equal):
    assert rules.floats_equal(expected, actual) is equal
    assert rules.floats_equal(actual, expected) is equal
    found = diff(
        table(D=pa.array([expected], pa.float64())), table(D=pa.array([actual], pa.float64()))
    )
    assert (found == []) is equal


def test_integers_have_no_tolerance():
    a = table(L=pa.array([2**62], pa.int64()))
    b = table(L=pa.array([2**62 + 1], pa.int64()))
    found = diff(a, b)
    assert kinds(found) == [rules.VALUE]
    assert (found[0].expected, found[0].actual) == (2**62, 2**62 + 1)


def test_timestamps_are_exact_to_the_nanosecond():
    a = table(T=pa.array([1700000000000000000], pa.timestamp("ns", tz="UTC")))
    b = table(T=pa.array([1700000000000000001], pa.timestamp("ns", tz="UTC")))
    found = diff(a, b)
    assert kinds(found) == [rules.VALUE]
    assert (found[0].expected, found[0].actual) == (1700000000000000000, 1700000000000000001)


def test_report_names_scenario_table_step_column_row_and_values():
    actual = BASE.set_column(3, "S", pa.array(["a", None, "x"], pa.string()))
    found = diff(BASE, actual)
    assert found == [
        rules.Mismatch(scenario="s", table="t", step=3, kind=rules.VALUE, column="S", row=2,
                       expected="", actual="x")
    ]
    text = str(found[0])
    for part in ("scenario=s", "table=t", "step=3", "column=S", "row=2", "expected=''",
                 "actual='x'"):
        assert part in text


def test_row_order_matters_when_ordered():
    a = table(I=pa.array([1, 2, 3], pa.int32()))
    b = table(I=pa.array([1, 3, 2], pa.int32()))
    found = diff(a, b)
    assert [(m.kind, m.row) for m in found] == [(rules.VALUE, 1), (rules.VALUE, 2)]


def test_unordered_compares_as_multisets():
    a = table(I=pa.array([1, 2, None, 2], pa.int32()), D=pa.array([0.5, NAN, None, 1.0]))
    b = table(I=pa.array([2, None, 1, 2], pa.int32()), D=pa.array([1.0, None, 0.5, NAN]))
    assert diff(a, b, ordered=False) == []
    assert diff(a, b, ordered=True) != []


def test_unordered_still_counts_duplicates():
    a = table(I=pa.array([1, 1, 2], pa.int32()))
    b = table(I=pa.array([1, 2, 2], pa.int32()))
    assert kinds(diff(a, b, ordered=False)) == [rules.VALUE]


def test_unordered_applies_the_float_tolerance():
    a = table(K=pa.array([1, 2], pa.int32()), D=pa.array([1.0, 2.0]))
    b = table(K=pa.array([2, 1], pa.int32()), D=pa.array([2.0 + 1e-10, 1.0 - 1e-10]))
    assert diff(a, b, ordered=False) == []
    c = table(K=pa.array([2, 1], pa.int32()), D=pa.array([2.0 + 1e-8, 1.0]))
    assert kinds(diff(a, c, ordered=False)) == [rules.VALUE]


def test_row_count():
    a = table(I=pa.array([1, 2, 3], pa.int32()))
    found = diff(a, a.slice(0, 2))
    assert kinds(found) == [rules.ROW_COUNT]
    assert (found[0].expected, found[0].actual) == (3, 2)
    assert kinds(diff(a.slice(0, 0), a)) == [rules.ROW_COUNT]


def test_column_name_order_and_count():
    a = table(X=pa.array([1], pa.int32()), Y=pa.array([2], pa.int32()))
    renamed = table(X=pa.array([1], pa.int32()), Z=pa.array([2], pa.int32()))
    found = diff(a, renamed)
    assert [(m.kind, m.column, m.expected, m.actual) for m in found] == [
        (rules.COLUMN_NAME, "Y", "Y", "Z")
    ]
    swapped = table(Y=pa.array([2], pa.int32()), X=pa.array([1], pa.int32()))
    assert kinds(diff(a, swapped)) == [rules.COLUMN_NAME, rules.COLUMN_NAME]
    fewer = table(X=pa.array([1], pa.int32()))
    assert kinds(diff(a, fewer)) == [rules.COLUMN_COUNT]
    assert kinds(diff(fewer, a)) == [rules.COLUMN_COUNT]


def test_column_type():
    a = table(X=pa.array([1], pa.int32()))
    found = diff(a, table(X=pa.array([1], pa.int64())))
    assert [(m.kind, m.column, m.expected, m.actual) for m in found] == [
        (rules.COLUMN_TYPE, "X", "int32", "int64")
    ]
    seconds = table(T=pa.array([1], pa.timestamp("s", tz="UTC")))
    nanos = table(T=pa.array([1_000_000_000], pa.timestamp("ns", tz="UTC")))
    assert kinds(diff(seconds, nanos)) == [rules.COLUMN_TYPE]
    assert kinds(diff(table(F=pa.array([1.0], pa.float32())), table(F=pa.array([1.0])))) == [
        rules.COLUMN_TYPE
    ]


def test_engine_type_metadata_is_part_of_the_type():
    def typed(engine_type: str) -> pa.Table:
        field = pa.field("X", pa.int64(), metadata={"deephaven:type": engine_type,
                                                    "deephaven:isSortable": "true"})
        return pa.Table.from_arrays([pa.array([1], pa.int64())], schema=pa.schema([field]))

    assert diff(typed("long"), typed("long")) == []
    found = diff(typed("long"), typed("java.time.Instant"))
    assert kinds(found) == [rules.COLUMN_METADATA]
    assert found[0].column == "X"


def test_metadata_outside_the_rules_is_ignored():
    plain = table(X=pa.array([1], pa.int32()))
    field = pa.field("X", pa.int32(), nullable=False, metadata={"deephaven:isSortable": "true"})
    noted = pa.Table.from_arrays(
        [pa.array([1], pa.int32())], schema=pa.schema([field], metadata={"anything": "else"})
    )
    assert diff(plain, noted) == []


def test_unsupported_types_are_reported_not_passed():
    a = table(X=pa.array([[1, 2]], pa.list_(pa.int32())))
    assert kinds(diff(a, a)) == [rules.UNSUPPORTED_TYPE]


def test_long_lists_of_mismatches_are_truncated_with_a_count():
    a = table(I=pa.array(range(100), pa.int32()))
    b = table(I=pa.array(range(1, 101), pa.int32()))
    found = diff(a, b)
    limit = rules.DEFAULT_RULES.max_value_mismatches_per_table
    assert kinds(found) == [rules.VALUE] * limit + [rules.TRUNCATED]
    assert found[-1].expected == 100


def test_missing_and_extra_result_tables():
    one = ResultSet("s")
    one.put(0, "a", table(I=pa.array([1], pa.int32())))
    one.put(1, "a", table(I=pa.array([1], pa.int32())))
    other = ResultSet("s")
    other.put(0, "a", table(I=pa.array([1], pa.int32())))
    other.put(0, "b", table(I=pa.array([1], pa.int32())))
    found = rules.compare(one, other)
    assert [(m.kind, m.step, m.table) for m in found] == [
        (rules.MISSING_TABLE, 1, "a"),
        (rules.EXTRA_TABLE, 0, "b"),
    ]


def test_compare_uses_the_unordered_set_per_table():
    one = ResultSet("s")
    other = ResultSet("s")
    for name in ("a", "b"):
        one.put(0, name, table(I=pa.array([1, 2], pa.int32())))
        other.put(0, name, table(I=pa.array([2, 1], pa.int32())))
    found = rules.compare(one, other, unordered={"b"})
    assert {m.table for m in found} == {"a"}
