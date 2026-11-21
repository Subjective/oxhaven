"""The comparison rules on hand-built tables. Needs no server."""

from __future__ import annotations

import math
from types import SimpleNamespace

import grpc
import pyarrow as pa
import pytest

from harness import results as res
from harness import rules, runner
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


TEMPORAL = [
    (pa.date64(), 1700000000000, 1700086400000),       # a LocalDate, in milliseconds
    (pa.date32(), 19675, 19676),
    (pa.time64("ns"), 80000000000000, 80000000000001),  # a LocalTime
    (pa.duration("ns"), 3600000000000, 3600000000001),  # a Duration
]


@pytest.mark.parametrize("arrow_type, value, other", TEMPORAL)
def test_dates_times_of_day_and_durations_are_exact(arrow_type, value, other):
    """The wire form of `LocalDate`, `LocalTime` and `Duration`: no tolerance, also when
    the column is computed, null equals only null, and the same inside an array."""
    assert rules.comparison_class(arrow_type) == rules.EXACT
    a = table(X=pa.array([value, None], arrow_type))
    assert diff(a, a) == []
    for cells in ([other, None], [value, value], [None, None]):
        b = table(X=pa.array(cells, arrow_type))
        assert kinds(diff(a, b)) == [rules.VALUE], cells
        assert kinds(diff(a, b, computed=frozenset({"X"}))) == [rules.VALUE], cells
    found = diff(a, table(X=pa.array([other, None], arrow_type)))
    assert (found[0].expected, found[0].actual) == (value, other)
    lists = pa.list_(arrow_type)
    a = table(X=pa.array([[value, None], None], lists))
    assert diff(a, a) == []
    for cells in ([[other, None], None], [[value], None], [[value, None], []]):
        assert kinds(diff(a, table(X=pa.array(cells, lists)))) == [rules.VALUE], cells


def test_a_date_and_a_timestamp_are_different_column_types():
    a = table(X=pa.array([0], pa.date64()))
    assert rules.COLUMN_TYPE in kinds(diff(a, table(X=pa.array([0], pa.timestamp("ms")))))
    assert rules.COLUMN_TYPE in kinds(diff(a, table(X=pa.array([0], pa.date32()))))


def test_a_period_is_still_a_type_the_rules_do_not_know():
    """The interval of months a `Period` is sent as, and the other intervals."""
    for unknown in (pa.month_day_nano_interval(),):
        assert rules.comparison_class(unknown) is None
        assert rules.comparison_class(pa.list_(unknown)) is None


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


# ---------------------------------------------------------------- A11: failed in a cycle


class _Refused(grpc.RpcError):
    def code(self) -> grpc.StatusCode:
        return grpc.StatusCode.INVALID_ARGUMENT


class _Served:
    """A table as the client holds it: what its snapshot answers, whether it refreshes, and
    whether the server failed it in an update cycle, after which it refuses new operations."""

    def __init__(self, data: pa.Table, refreshing: bool = True, failed: bool = False):
        self.data, self.is_refreshing, self.failed = data, refreshing, failed
        self.probes = 0

    def where(self, _condition: str) -> "_Served":
        self.probes += 1
        if self.failed:
            raise RuntimeError("the client's wrapper") from _Refused()
        return self

    def to_arrow(self) -> pa.Table:
        return self.data


def _snapshot(**tables: _Served) -> ResultSet:
    """What the runner records of `tables` at step 1."""
    scenario = SimpleNamespace(results=[SimpleNamespace(table=name) for name in tables])
    out = ResultSet("s")
    runner._snapshot(None, scenario, dict(tables), 1, out)  # noqa: SLF001
    return out


def _ints(*values: int) -> pa.Table:
    return table(V=pa.array(values, pa.int32()))


def test_a_table_that_failed_in_a_cycle_is_compared_by_its_failure_alone():
    """PARITY.md A11: the rows a failed table still serves are not compared; that it failed,
    at which step and with which status code, is."""
    java = _snapshot(t=_Served(_ints(6, 9), failed=True))
    oxhaven = _snapshot(t=_Served(_ints(6), failed=True))
    assert java.get(1, "t") == res.Failure("INVALID_ARGUMENT")
    assert rules.compare(java, oxhaven) == []
    healthy = _snapshot(t=_Served(_ints(6, 9)))
    assert kinds(rules.compare(java, healthy)) == [rules.FAILURE_EXPECTED]
    assert kinds(rules.compare(healthy, oxhaven)) == [rules.FAILURE_UNEXPECTED]


def test_a_table_that_has_not_failed_is_compared_by_its_contents():
    live, static = _Served(_ints(6, 9)), _Served(_ints(6), refreshing=False)
    result = _snapshot(live=live, static=static)
    assert result.get(1, "live").equals(live.data)
    assert result.get(1, "static").equals(static.data)
    assert (live.probes, static.probes) == (1, 0)  # a static table cannot fail in a cycle
    other = _snapshot(live=_Served(_ints(6)), static=static)
    assert kinds(rules.compare(result, other)) == [rules.ROW_COUNT]


# ---------------------------------------------------------------- typed vectors (A27)

_TIMESTAMPS = pa.timestamp("ns", tz="UTC")


def _column(name: str, values: list, arrow_type: pa.DataType, **metadata: str) -> pa.Table:
    field = pa.field(name, arrow_type,
                     metadata={f"deephaven:{key}": value for key, value in metadata.items()})
    return pa.Table.from_arrays([pa.array(values, arrow_type)], schema=pa.schema([field]))


def _java_vector(values: list) -> pa.Table:
    """A vector column as Java sends a vector of `Object` (observed on 42.9)."""
    return _column("V", values, pa.list_(pa.string()), type="io.deephaven.vector.ObjectVector",
                   componentType="java.lang.String")


def _typed_vector(values: list, arrow_type: pa.DataType, vector: str, component: str,
                  ) -> pa.Table:
    return _column("V", values, pa.list_(arrow_type), type=vector, componentType=component)


_BOOLEANS = _typed_vector([[None, False], [True], [], None], pa.bool_(),
                          "io.deephaven.vector.ObjectVector", "java.lang.Boolean")
_JAVA_BOOLEANS = _java_vector([[None, "false"], ["true"], [], None])
_INSTANTS = _typed_vector(
    [[1_700_000_001_500_000_000, 1_700_000_006_000_123_004, 0, -1, None]], _TIMESTAMPS,
    "io.deephaven.vector.ObjectVector", "java.time.Instant")
_JAVA_INSTANTS = _java_vector([["2023-11-14T22:13:21.500Z", "2023-11-14T22:13:26.000123004Z",
                                "1970-01-01T00:00:00Z", "1969-12-31T23:59:59.999999999Z",
                                None]])


def typed_diff(expected: pa.Table, actual: pa.Table, taken_apart: bool = False,
               typed: bool = True) -> list[rules.Mismatch]:
    return rules.compare_tables("s", "t", 3, expected, actual,
                                typed={"V": taken_apart} if typed else None)


def test_a27_accepts_typed_vectors_whose_elements_are_javas_text():
    assert typed_diff(_JAVA_BOOLEANS, _BOOLEANS) == []
    assert typed_diff(_JAVA_INSTANTS, _INSTANTS) == []
    # Without A27 for the column, the schemas differ.
    assert kinds(typed_diff(_JAVA_BOOLEANS, _BOOLEANS, typed=False)) == [rules.COLUMN_TYPE]
    assert kinds(diff(_JAVA_INSTANTS, _INSTANTS)) == [rules.COLUMN_TYPE]


def test_a27_still_compares_each_element():
    for other in ([[None, True], [True], [], None],    # an element
                  [[False, None], [True], [], None],   # the order of the elements
                  [[None, False], [True], None, None],  # null against empty
                  [[None, False], [True], [], []]):    # empty against null
        typed = _typed_vector(other, pa.bool_(), "io.deephaven.vector.ObjectVector",
                              "java.lang.Boolean")
        assert kinds(typed_diff(_JAVA_BOOLEANS, typed)) == [rules.VALUE], other
    shifted = _typed_vector([[1_700_000_001_500_000_001, 1_700_000_006_000_123_004, 0, -1,
                              None]], _TIMESTAMPS, "io.deephaven.vector.ObjectVector",
                            "java.time.Instant")
    assert kinds(typed_diff(_JAVA_INSTANTS, shifted)) == [rules.VALUE]
    # A string that is not how Java writes the instant.
    padded = _java_vector([["2023-11-14T22:13:21.500000Z", "2023-11-14T22:13:26.000123004Z",
                            "1970-01-01T00:00:00Z", "1969-12-31T23:59:59.999999999Z", None]])
    assert kinds(typed_diff(padded, _INSTANTS)) == [rules.VALUE]


def test_a27_needs_every_other_field_metadata_key_to_match():
    wrong_component = _typed_vector([[None, False], [True], [], None], pa.bool_(),
                                    "io.deephaven.vector.ObjectVector", "java.time.Instant")
    assert kinds(typed_diff(_JAVA_BOOLEANS, wrong_component)) == [rules.COLUMN_TYPE]
    wrong_vector = _typed_vector([[None, False], [True], [], None], pa.bool_(),
                                 "io.deephaven.vector.BooleanVector", "java.lang.Boolean")
    assert kinds(typed_diff(_JAVA_BOOLEANS, wrong_vector)) == [rules.COLUMN_TYPE]
    sortable = _column("V", [[None, False], [True], [], None], pa.list_(pa.bool_()),
                       type="io.deephaven.vector.ObjectVector",
                       componentType="java.lang.Boolean", isSortable="true")
    assert kinds(typed_diff(_JAVA_BOOLEANS, sortable)) == [rules.COLUMN_TYPE]
    # Java's side must be the vector of strings that A27 describes.
    of_strings = _column("V", [[None, "false"], ["true"], [], None], pa.list_(pa.string()),
                         type="java.lang.String[]", componentType="java.lang.String")
    assert kinds(typed_diff(of_strings, _BOOLEANS)) == [rules.COLUMN_TYPE]


def test_a27_leaves_vectors_of_strings_as_they_are():
    strings = _typed_vector([["a"]], pa.string(), "io.deephaven.vector.ObjectVector",
                            "java.lang.String")
    assert typed_diff(_java_vector([["a"]]), strings) == []
    assert kinds(typed_diff(_java_vector([["a"]]), _typed_vector(
        [["b"]], pa.string(), "io.deephaven.vector.ObjectVector",
        "java.lang.String"))) == [rules.VALUE]


_DAY_MS = 86_400_000
_OBJECT_VECTOR = "io.deephaven.vector.ObjectVector"


def test_a27_covers_the_other_objects_inside_vectors():
    # Observed on 42.9 (`corpus/agg_object_vectors`): the texts Java sends for a minimum of
    # each class, and the cells Oxhaven sends for the same typed vector.
    cases = [
        (pa.date64(), "java.time.LocalDate", [[19675 * _DAY_MS, None, -1 * _DAY_MS]],
         [["2023-11-14", None, "1969-12-31"]]),
        (pa.time64("ns"), "java.time.LocalTime",
         [[80_000_000_000_000, 25_200_000_000_000, 86_399_999_999_999, 0, 80_001_500_000_000]],
         [["22:13:20", "07:00", "23:59:59.999999999", "00:00", "22:13:21.500"]]),
        (pa.duration("ns"), "java.time.Duration",
         [[3_600_000_000_000, -60_500_000_000, 90_061_000_000_123, -1, 0]],
         [["PT1H", "PT-1M-0.5S", "PT25H1M1.000000123S", "PT-0.000000001S", "PT0S"]]),
        (_TIMESTAMPS, "java.time.ZonedDateTime",
         [[1_700_000_000_000_000_000, 1_710_054_000_000_000_000, 1_730_617_200_000_000_001,
           -86_400_000_000_000]],
         [["2023-11-14T22:13:20Z[UTC]", "2024-03-10T16:00+09:00[Asia/Tokyo]",
           "2024-11-03T02:00:00.000000001-05:00[America/New_York]", "1969-12-31T00:00Z[UTC]"]]),
    ]
    for arrow_type, element, typed_cells, java_cells in cases:
        typed = _typed_vector(typed_cells, arrow_type, _OBJECT_VECTOR, element)
        assert typed_diff(_java_vector(java_cells), typed) == [], element
        # Each element is still compared.
        shifted = [[cell + 1 if cell is not None else None for cell in row]
                   for row in typed_cells]
        assert kinds(typed_diff(_java_vector(java_cells),
                                _typed_vector(shifted, arrow_type, _OBJECT_VECTOR, element))) == [
            rules.VALUE], element
        # Without A27 for the column, the schemas differ.
        assert kinds(typed_diff(_java_vector(java_cells), typed, typed=False)) == [
            rules.COLUMN_TYPE], element


def test_a27_compares_the_instant_of_a_zoned_date_time():
    zoned = _typed_vector([[1_700_000_000_000_000_000]], _TIMESTAMPS, _OBJECT_VECTOR,
                          "java.time.ZonedDateTime")
    # The zone is not sent: any text that names the instant is accepted ...
    for text in ("2023-11-14T22:13:20Z[UTC]", "2023-11-15T07:13:20+09:00[Asia/Tokyo]",
                 "2023-11-14T17:13:20-05:00[America/New_York]", "2023-11-14T22:13:20Z",
                 "2023-11-14T23:43:20+01:30"):
        assert typed_diff(_java_vector([[text]]), zoned) == [], text
    # ... and no other.
    for text in ("2023-11-14T22:13:20-05:00[America/New_York]", "2023-11-14T22:13:20.000000001Z",
                 "2023-11-14T22:13:20", "2023-11-14T22:13:20Z[UTC"):
        assert kinds(typed_diff(_java_vector([[text]]), zoned)) == [rules.VALUE], text


def test_a27_needs_the_arrow_type_of_the_class():
    # A date sent as days, a time in microseconds, a duration in milliseconds, a zoned date
    # and time in another unit, a time of day sent as a date: not how Oxhaven sends them.
    for arrow_type, element in ((pa.date32(), "java.time.LocalDate"),
                                (pa.time64("us"), "java.time.LocalTime"),
                                (pa.duration("ms"), "java.time.Duration"),
                                (pa.timestamp("ms", tz="UTC"), "java.time.ZonedDateTime"),
                                (pa.date64(), "java.time.LocalTime"),
                                (pa.int32(), "java.time.Period")):
        typed = _typed_vector([[0]], arrow_type, _OBJECT_VECTOR, element)
        assert kinds(typed_diff(_java_vector([["x"]]), typed)) == [rules.COLUMN_TYPE], element
    # Inside a vector, a class that both send as text has Java's component type.
    texts = _typed_vector([["2024-03-10T03:00"]], pa.string(), _OBJECT_VECTOR,
                          "java.time.LocalDateTime")
    assert kinds(typed_diff(_java_vector([["2024-03-10T03:00"]]), texts)) == [
        rules.COLUMN_METADATA]


def test_a27_covers_the_other_objects_taken_apart():
    java = lambda cells: _column("V", cells, pa.string(), type="java.lang.Object")  # noqa: E731
    cases = [
        (["2023-11-14", None], [19675 * _DAY_MS, None], pa.date64(), "java.time.LocalDate",
         True),
        (["07:00"], [25_200_000_000_000], pa.time64("ns"), "java.time.LocalTime", True),
        (["PT1H"], [3_600_000_000_000], pa.duration("ns"), "java.time.Duration", True),
        (["2023-11-15T07:13:20+09:00[Asia/Tokyo]"], [1_700_000_000_000_000_000], _TIMESTAMPS,
         "java.time.ZonedDateTime", True),
        (["2024-03-10T03:00"], ["2024-03-10T03:00"], pa.string(), "java.time.LocalDateTime",
         True),
        (["SUNDAY"], ["SUNDAY"], pa.string(), "java.time.DayOfWeek", True),
        (["Asia/Tokyo"], ["Asia/Tokyo"], pa.string(), "java.time.ZoneId", False),
    ]
    for java_cells, cells, arrow_type, element, sortable in cases:
        sorts = {"isSortable": "true"} if sortable else {}
        typed = _column("V", cells, arrow_type, type=element, **sorts)
        assert typed_diff(java(java_cells), typed, taken_apart=True) == [], element
        # Only for a column an ungroup took apart.
        assert typed_diff(java(java_cells), typed) != [], element
        # The sortability is that of the class.
        other = {} if sortable else {"isSortable": "true"}
        assert kinds(typed_diff(java(java_cells), _column("V", cells, arrow_type, type=element,
                                                          **other), taken_apart=True)) in (
            [rules.COLUMN_TYPE], [rules.COLUMN_METADATA]), element
    # A text that is not Java's.
    assert kinds(typed_diff(java(["MONDAY"]), _column(
        "V", ["SUNDAY"], pa.string(), type="java.time.DayOfWeek", isSortable="true"),
        taken_apart=True)) == [rules.VALUE]


def test_the_texts_of_java_time_classes():
    assert rules.java_local_date_text(0) == "1970-01-01"
    assert rules.java_local_date_text(-1) == "1969-12-31"
    assert rules.java_local_date_text(19_782) == "2024-02-29"
    assert rules.java_local_date_text(-719_528) == "0000-01-01"
    assert rules.java_local_date_text(-719_529) == "-0001-12-31"
    assert rules.java_local_date_text(2_932_897) == "+10000-01-01"
    assert rules.java_local_date_text(2_932_896) == "9999-12-31"
    for days in (-1_000_000, -719_529, -1, 0, 59, 60, 11_016, 2_932_897, 5_000_000):
        assert rules._days(*rules._civil(days)) == days
    assert rules.java_local_time_text(0) == "00:00"
    assert rules.java_local_time_text(61_000_000_000) == "00:01:01"
    assert rules.java_local_time_text(1_000_000) == "00:00:00.001"
    assert rules.java_local_time_text(1_000) == "00:00:00.000001"
    assert rules.java_local_time_text(1) == "00:00:00.000000001"
    assert rules.java_local_time_text(86_399_999_999_999) == "23:59:59.999999999"
    assert rules.java_duration_text(0) == "PT0S"
    assert rules.java_duration_text(-1) == "PT-0.000000001S"
    assert rules.java_duration_text(-60_500_000_000) == "PT-1M-0.5S"
    assert rules.java_duration_text(90_061_000_000_123) == "PT25H1M1.000000123S"
    assert rules.java_duration_text(3_600_000_000_001) == "PT1H0.000000001S"
    assert rules.java_duration_text(7_200_000_000_000) == "PT2H"
    assert rules.java_zoned_instant_text("2024-03-10T01:59:59.999999999-05:00[America/New_York]") \
        == "2024-03-10T06:59:59.999999999Z"
    assert rules.java_zoned_instant_text("1969-12-31T19:00-05:00[America/New_York]") \
        == "1970-01-01T00:00:00Z"
    assert rules.java_zoned_instant_text("2023-11-14T23:13:20+01:00:00") \
        == "2023-11-14T22:13:20Z"
    assert rules.java_zoned_instant_text("not a time") == "not a time"


def test_a27_accepts_a_typed_column_that_ungroup_took_apart():
    java = _column("V", ["true", None], pa.string(),
                   type="java.lang.Object")
    oxhaven = _column("V", [True, None], pa.bool_(), type="java.lang.Boolean",
                      isSortable="true")
    assert typed_diff(java, oxhaven, taken_apart=True) == []
    # Not a column that an ungroup took apart: a scalar of Object is compared as usual.
    assert kinds(typed_diff(java, oxhaven)) == [rules.COLUMN_TYPE]
    instants = _column("V", [1_700_000_001_500_000_000], _TIMESTAMPS,
                       type="java.time.Instant", isSortable="true")
    java_instant = _column("V", ["2023-11-14T22:13:21.500Z"], pa.string(),
                           type="java.lang.Object")
    assert typed_diff(java_instant, instants, taken_apart=True) == []
    strings = _column("V", ["a"], pa.string(), type="java.lang.String", isSortable="true")
    assert typed_diff(_column("V", ["a"], pa.string(), type="java.lang.Object"), strings,
                      taken_apart=True) == []
    # Java's column must be the unsortable Object column, and Oxhaven's sortable.
    sortable_java = _column("V", ["a"], pa.string(), type="java.lang.Object",
                            isSortable="true")
    assert kinds(typed_diff(sortable_java, strings, taken_apart=True)) == [
        rules.COLUMN_METADATA]
    unsortable = _column("V", ["a"], pa.string(), type="java.lang.String")
    assert kinds(typed_diff(_column("V", ["a"], pa.string(), type="java.lang.Object"),
                            unsortable, taken_apart=True)) == [rules.COLUMN_METADATA]
    assert kinds(typed_diff(java, _column("V", [False, None], pa.bool_(),
                                          type="java.lang.Boolean", isSortable="true"),
                            taken_apart=True)) == [rules.VALUE]


def test_java_instant_text_writes_as_instant_to_string():
    assert rules.java_instant_text(0) == "1970-01-01T00:00:00Z"
    assert rules.java_instant_text(1_500_000_000) == "1970-01-01T00:00:01.500Z"
    assert rules.java_instant_text(1_000_001_000) == "1970-01-01T00:00:01.000001Z"
    assert rules.java_instant_text(1_000_000_001) == "1970-01-01T00:00:01.000000001Z"
    assert rules.java_instant_text(-1) == "1969-12-31T23:59:59.999999999Z"
    assert rules.java_instant_text(-(2**63) + 1) == "1677-09-21T00:12:43.145224193Z"
    assert rules.java_instant_text(2**63 - 1) == "2262-04-11T23:47:16.854775807Z"


def test_typed_vector_columns_follow_the_operations_of_a27():
    ops = [
        {"name": "g", "op": "group_by", "source": "t", "by": ["H"]},
        {"name": "a", "op": "agg_by", "source": "g", "by": ["H"], "aggs": [
            {"agg": "min", "columns": ["MinB=B"]},
            {"agg": "first", "columns": ["FirstB=B"]},
            {"agg": "count_distinct", "columns": ["N=B"]},
            {"agg": "distinct", "columns": ["DisT=T"]}]},
        {"name": "u", "op": "update_by", "source": "g", "by": ["H"], "ops": [
            {"op": "cum_max", "columns": ["CMaxT=T"]},
            {"op": "rolling_min_tick", "columns": ["RMinB=B"]},
            {"op": "cum_sum", "columns": ["Sum=K"]}]},
        {"name": "s", "op": "sort", "source": "a", "columns": ["MinB"]},
        {"name": "v", "op": "view", "source": "s", "formulas": ["M=MinB", "X=MinB_[0]"]},
        {"name": "f", "op": "agg_by", "source": "a", "by": [], "aggs": [
            {"agg": "first", "columns": ["MinB"]}, {"agg": "last", "columns": ["L=N"]}]},
        {"name": "x", "op": "ungroup", "source": "a", "columns": ["DisT"]},
        {"name": "y", "op": "ungroup", "source": "u"},
        {"name": "d", "op": "drop_columns", "source": "y", "columns": ["CMaxT"]},
    ]
    found = rules.typed_vector_columns(ops)
    assert found["g"] == {}
    assert found["a"] == {"MinB": False, "DisT": False}
    assert found["u"] == {"CMaxT": False, "RMinB": False}
    assert found["s"] == found["a"]
    assert found["v"] == {"M": False}
    assert found["f"] == {"MinB": False}
    assert found["x"] == {"MinB": False, "DisT": True}
    assert found["y"] == {"CMaxT": True, "RMinB": True}
    assert found["d"] == {"RMinB": True}


def test_the_recorded_a27_scenario_names_its_typed_columns():
    from pathlib import Path

    from harness import scenario as sc
    path = Path(__file__).resolve().parent.parent / "scenarios/static/agg_typed_vectors.json"
    found = rules.typed_vector_columns(sc.load(path).operations)
    assert {"MinB", "MaxT", "MedS", "PctT"} <= set(found["extremes"]) | set(found["middles"])
    assert set(found["formulas"]) == {"FB", "FT", "FS"}
    assert found["kept_types"] == {}
    assert found["sorted"] == found["extremes"]
    assert found["taken_apart"] == {name: True for name in found["middles"]}
    assert set(found["nested_apart"]) == {"DisB", "DisT", "DisS", "GrpB", "GrpT", "GrpS"}


def test_the_recorded_scenario_of_other_objects_names_its_typed_columns():
    from pathlib import Path

    from harness import scenario as sc
    path = Path(__file__).resolve().parent.parent / "scenarios/static/agg_object_vectors.json"
    found = rules.typed_vector_columns(sc.load(path).operations)
    classes = ("D", "LT", "Dt", "Dur", "W", "Z")
    assert set(found["extremes"]) == {f"{p}{c}" for p in ("Min", "Max") for c in classes}
    assert set(found["formulas"]) == {f"F{c}" for c in classes + ("Zi",)}
    assert found["sorted"] == found["extremes"]
    assert found["taken_apart"] == {name: True for name in found["middles"]}
    assert found["cumulative_kept"] == {
        f"{p}{c}": False for p in ("CMin", "CMax", "RMin", "RMax") for c in classes}
    assert found["groups_apart"] == {f"Grp{c}": True for c in classes}
    for c in classes:
        assert found[f"distinct_{c}"] == {f"Dis{c}": True}
