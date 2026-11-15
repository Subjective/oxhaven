"""The comparison rules on hand-built tables. Needs no server."""

from __future__ import annotations

import math

import pyarrow as pa
import pytest

from harness import results as res
from harness import rules
from harness.results import ResultSet

NAN = math.nan


def table(**columns: pa.Array) -> pa.Table:
    return pa.table(columns)


def diff(expected: pa.Table, actual: pa.Table, ordered: bool = True,
         computed: frozenset[str] = frozenset()) -> list[rules.Mismatch]:
    return rules.compare_tables("s", "t", 3, expected, actual, ordered=ordered,
                                computed=computed)


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
def test_computed_double_tolerance(expected, actual, equal):
    """PARITY.md A1, double: relative 1e-9, absolute 1e-12."""
    tolerance = rules.DEFAULT_RULES.double_tolerance
    assert rules.floats_within(expected, actual, tolerance) is equal
    assert rules.floats_within(actual, expected, tolerance) is equal
    e = table(D=pa.array([expected], pa.float64()))
    a = table(D=pa.array([actual], pa.float64()))
    assert (diff(e, a, computed=frozenset({"D"})) == []) is equal
    # The same pair in a column that is not computed must be identical.
    assert diff(e, a) != []


@pytest.mark.parametrize(
    ("expected", "actual", "equal"),
    [
        (1.0, 1.0 + 5e-6, True),
        (1.0, 1.0 + 2e-5, False),
        (1000.0, 1000.005, True),
        (1000.0, 1000.02, False),
        (0.0, 5e-7, True),
        (0.0, 2e-6, False),
        (0.0, -0.0, True),
    ],
)
def test_computed_float_tolerance(expected, actual, equal):
    """PARITY.md A1, float: relative 1e-5, absolute 1e-6. Looser than double."""
    e = table(F=pa.array([expected], pa.float32()))
    a = table(F=pa.array([actual], pa.float32()))
    assert (diff(e, a, computed=frozenset({"F"})) == []) is equal
    assert diff(e, a) != []
    # A double column gets the double tolerance even with the same values.
    e64 = table(F=pa.array([expected], pa.float64()))
    a64 = table(F=pa.array([actual], pa.float64()))
    if abs(expected - actual) > 1e-8:
        assert diff(e64, a64, computed=frozenset({"F"})) != []


def test_floats_that_are_not_computed_are_exact_as_bit_patterns():
    """PARITY.md, Must match, item 5; A2 is proposed and not in force."""
    one = table(D=pa.array([1.0]), F=pa.array([1.0], pa.float32()))
    assert diff(one, one) == []
    next_double = table(D=pa.array([math.nextafter(1.0, 2.0)]), F=pa.array([1.0], pa.float32()))
    assert [m.column for m in diff(one, next_double)] == ["D"]
    next_float = table(D=pa.array([1.0]), F=pa.array([1.0000001192092896], pa.float32()))
    assert [m.column for m in diff(one, next_float)] == ["F"]
    zero = table(D=pa.array([0.0]))
    negative_zero = table(D=pa.array([-0.0]))
    assert kinds(diff(zero, negative_zero)) == [rules.VALUE]
    assert diff(negative_zero, negative_zero) == []


def test_tolerance_never_covers_null_nan_or_infinity():
    """PARITY.md A1: "Null, NaN and infinities are never covered by the tolerance"."""
    computed = frozenset({"D"})
    for expected, actual in [(None, 0.0), (NAN, 0.0), (math.inf, 1.7976931348623157e308),
                             (math.inf, -math.inf), (0.0, None), (1.0, NAN)]:
        e = table(D=pa.array([expected], pa.float64()))
        a = table(D=pa.array([actual], pa.float64()))
        assert kinds(diff(e, a, computed=computed)) == [rules.VALUE], (expected, actual)
    same = table(D=pa.array([None, NAN, math.inf, -math.inf], pa.float64()))
    assert diff(same, same, computed=computed) == []


def scenario_ops(*ops):
    return [dict(op) for op in ops]


def test_computed_columns_follow_parity_a1():
    ops = scenario_ops(
        {"name": "w", "op": "where", "source": "t", "filters": ["D > 0"]},
        {"name": "a", "op": "agg_by", "source": "w", "by": ["K"], "aggs": [
            {"agg": "sum", "columns": ["SumD = D", "SumI = I"]},
            {"agg": "avg", "columns": ["AvgI = I"]},
            {"agg": "min", "columns": ["MinD = D"]},
            {"agg": "last", "columns": ["LastD = D"]},
            {"agg": "count", "column": "N"}]},
        {"name": "s", "op": "sort", "source": "a",
         "by": [{"column": "K", "direction": "ascending"}]},
        {"name": "again", "op": "agg_by", "source": "s", "by": [], "aggs": [
            {"agg": "max", "columns": ["Biggest = SumD"]},
            {"agg": "first", "columns": ["FirstMin = MinD"]}]},
        {"name": "v", "op": "view", "source": "a",
         "formulas": ["Kept = SumD", "Twice = SumD * 2", "MinD", "E = exp(MinD)"]},
        {"name": "u", "op": "update", "source": "a", "formulas": ["SumD = MinD", "X = AvgI"]},
        {"name": "d", "op": "drop_columns", "source": "a", "columns": ["SumD"]},
        {"name": "c", "op": "update_by", "source": "t", "by": [],
         "ops": [{"op": "cum_sum", "columns": ["Run = D"]}]},
        {"name": "j", "op": "natural_join", "left": "t", "right": "a", "on": ["K"],
         "joins": ["Joined = SumD", "M = MinD"]},
        {"name": "j_all", "op": "join", "left": "t", "right": "a", "on": ["K"]},
        {"name": "m", "op": "merge", "sources": ["a", "s"]},
    )
    computed = rules.computed_columns(ops)
    assert computed["w"] == frozenset()
    # sum and avg are computed; min, max, first, last and count are not.
    assert computed["a"] == {"SumD", "SumI", "AvgI"}
    assert computed["s"] == computed["a"]
    # An aggregation that selects an element passes a computed cell through.
    assert computed["again"] == {"Biggest"}
    # A plain reference passes through; arithmetic is exact; an inexact function is computed.
    assert computed["v"] == {"Kept", "E"}
    assert computed["u"] == {"SumI", "AvgI", "X"}
    assert computed["d"] == {"SumI", "AvgI"}
    assert computed["c"] == {"Run"}
    assert computed["j"] == {"Joined"}
    assert computed["j_all"] == {"SumD", "SumI", "AvgI"}
    assert computed["m"] == computed["a"]


def test_failures_must_match_by_status_code_only():
    """PARITY.md, Must match, item 7, and A5."""
    def one(value):
        results = ResultSet("s")
        results.put(0, "t", value)
        return results

    data = table(I=pa.array([1], pa.int32()))
    invalid = res.Failure("INVALID_ARGUMENT")
    assert rules.compare(one(invalid), one(res.Failure("INVALID_ARGUMENT"))) == []
    found = rules.compare(one(invalid), one(res.Failure("FAILED_PRECONDITION")))
    assert [(m.kind, m.expected, m.actual) for m in found] == [
        (rules.FAILURE_CODE, "INVALID_ARGUMENT", "FAILED_PRECONDITION")]
    assert kinds(rules.compare(one(invalid), one(data))) == [rules.FAILURE_EXPECTED]
    assert kinds(rules.compare(one(data), one(invalid))) == [rules.FAILURE_UNEXPECTED]


def test_table_attributes_clients_act_on_are_compared():
    def marked(**attributes: str) -> pa.Table:
        metadata = {f"deephaven:attribute.{k}": v for k, v in attributes.items()}
        metadata["deephaven:unsent.attribute.BarrageSchema"] = ""
        return table(I=pa.array([1], pa.int32())).replace_schema_metadata(metadata)

    assert diff(marked(AppendOnly="true"), marked(AppendOnly="true")) == []
    for other in (marked(), marked(BlinkTable="true"), marked(AppendOnly="true", IsFlat="true")):
        found = diff(marked(AppendOnly="true"), other)
        assert kinds(found) == [rules.TABLE_METADATA]
    ignored = marked(AppendOnly="true").replace_schema_metadata(
        {"deephaven:attribute.AppendOnly": "true", "deephaven:attribute.SortedColumns": "X"})
    assert diff(marked(AppendOnly="true"), ignored) == []


def test_integers_have_no_tolerance():
    a = table(L=pa.array([2**62], pa.int64()))
    b = table(L=pa.array([2**62 + 1], pa.int64()))
    found = diff(a, b)
    assert kinds(found) == [rules.VALUE]
    assert (found[0].expected, found[0].actual) == (2**62, 2**62 + 1)


def test_chars_are_exact_code_units_and_no_other_unsigned_type_is_known():
    chars = lambda *units: table(C=pa.array(units, pa.uint16()))  # noqa: E731
    assert diff(chars(97, None, 65534), chars(97, None, 65534)) == []
    found = diff(chars(97, None, 65534), chars(98, 0, None))
    assert kinds(found) == [rules.VALUE] * 3
    assert [(m.row, m.expected, m.actual) for m in found] == [
        (0, 97, 98), (1, None, 0), (2, 65534, None)]
    # a char is not the short with the same bits
    short = table(C=pa.array([97], pa.int16()))
    assert kinds(diff(chars(97), short)) == [rules.COLUMN_TYPE]
    arrays = table(C=pa.array([[97, None], None], pa.list_(pa.uint16())))
    other = table(C=pa.array([[97, 0], None], pa.list_(pa.uint16())))
    assert diff(arrays, arrays) == []
    assert kinds(diff(arrays, other)) == [rules.VALUE]
    for unsigned in (pa.uint8(), pa.uint32(), pa.uint64()):
        unknown = table(X=pa.array([1], unsigned))
        assert kinds(diff(unknown, unknown)) == [rules.UNSUPPORTED_TYPE]


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
    computed = frozenset({"D"})
    assert diff(a, b, ordered=False, computed=computed) == []
    c = table(K=pa.array([2, 1], pa.int32()), D=pa.array([2.0 + 1e-8, 1.0]))
    assert kinds(diff(a, c, ordered=False, computed=computed)) == [rules.VALUE]


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
    field = pa.field("X", pa.int32(), nullable=False, metadata={"somebody:else": "true"})
    noted = pa.Table.from_arrays(
        [pa.array([1], pa.int32())], schema=pa.schema([field], metadata={"anything": "else"})
    )
    assert diff(plain, noted) == []


@pytest.mark.parametrize("key", ["isSortable", "componentType", "inputtable.isKey",
                                 "description", "isPartitioning"])
def test_field_metadata_parity_lists_as_compared(key):
    plain = table(X=pa.array([1], pa.int32()))
    field = pa.field("X", pa.int32(), metadata={f"deephaven:{key}": "true"})
    noted = pa.Table.from_arrays([pa.array([1], pa.int32())], schema=pa.schema([field]))
    assert kinds(diff(plain, noted)) == [rules.COLUMN_METADATA]
    assert diff(noted, noted) == []


def test_unsupported_types_are_reported_not_passed():
    for unknown in (pa.array([{"a": 1}], pa.struct([("a", pa.int32())])),
                    pa.array([b"x"], pa.binary()),
                    pa.array([[{"a": 1}]], pa.list_(pa.struct([("a", pa.int32())])))):
        a = table(X=unknown)
        assert kinds(diff(a, a)) == [rules.UNSUPPORTED_TYPE]


def test_array_cells_are_compared_element_by_element():
    ints = pa.list_(pa.int32())
    a = table(X=pa.array([[1, 2], [], None, [None, 3]], ints))
    assert diff(a, a) == []
    for other in ([[1, 2], [], None, [None, 4]],      # an element
                  [[1, 2], [], None, [None]],         # a length
                  [[1, 2], None, None, [None, 3]],    # empty array against null
                  [[2, 1], [], None, [None, 3]],      # the order of the elements
                  [[1, 2], [], [], [None, 3]]):       # null against empty array
        found = diff(a, table(X=pa.array(other, ints)))
        assert kinds(found) == [rules.VALUE], other
    wrong_type = table(X=pa.array([[1, 2], [], None, [None, 3]], pa.list_(pa.int64())))
    assert kinds(diff(a, wrong_type)) == [rules.COLUMN_TYPE]


def test_float_arrays_are_exact_unless_the_column_is_computed():
    doubles = pa.list_(pa.float64())
    a = table(X=pa.array([[1.0, math.nan, None], [0.0]], doubles))
    close = table(X=pa.array([[1.0 + 1e-12, math.nan, None], [0.0]], doubles))
    far = table(X=pa.array([[1.0 + 1e-6, math.nan, None], [0.0]], doubles))
    zero = table(X=pa.array([[1.0, math.nan, None], [-0.0]], doubles))
    nan_null = table(X=pa.array([[1.0, None, None], [0.0]], doubles))
    assert diff(a, a) == []
    assert kinds(diff(a, close)) == [rules.VALUE]
    assert kinds(diff(a, zero)) == [rules.VALUE]
    assert diff(a, close, computed={"X"}) == []
    assert diff(a, zero, computed={"X"}) == []
    assert kinds(diff(a, far, computed={"X"})) == [rules.VALUE]
    assert kinds(diff(a, nan_null, computed={"X"})) == [rules.VALUE]


def test_unordered_comparison_orders_array_cells():
    ints = pa.list_(pa.int32())
    a = table(X=pa.array([[2], [1, 5], None, []], ints))
    b = table(X=pa.array([None, [], [1, 5], [2]], ints))
    assert kinds(diff(a, b)) == [rules.VALUE] * 4
    assert diff(a, b, ordered=False) == []


def test_every_column_marker_of_agg_all_by():
    operations = [
        {"name": "s", "op": "agg_all_by", "source": "t", "agg": {"agg": "sum", "columns": []},
         "by": ["K"]},
        {"name": "m", "op": "agg_all_by", "source": "t", "agg": {"agg": "max", "columns": []},
         "by": ["K"]},
        {"name": "m_of_s", "op": "agg_all_by", "source": "s",
         "agg": {"agg": "max", "columns": []}, "by": ["K"]},
        {"name": "v", "op": "view", "source": "s", "formulas": ["K", "Y = X"]},
    ]
    computed = rules.computed_columns(operations)
    assert rules.is_computed(computed["s"], "X") and not rules.is_computed(computed["s"], "K")
    assert not rules.is_computed(computed["m"], "X")
    assert rules.is_computed(computed["m_of_s"], "X")
    assert not rules.is_computed(computed["m_of_s"], "K")
    assert computed["v"] == {"Y"}
    a = table(K=pa.array([1.0]), X=pa.array([2.0]))
    b = table(K=pa.array([1.0]), X=pa.array([2.0 + 1e-12]))
    c = table(K=pa.array([1.0 + 1e-13]), X=pa.array([2.0]))
    assert diff(a, b, computed=computed["s"]) == []
    assert kinds(diff(a, c, computed=computed["s"])) == [rules.VALUE]


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
