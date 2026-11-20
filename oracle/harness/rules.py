"""The comparison rules: when two result sets count as the same.

No Java counterpart: this module is the harness's implementation of PARITY.md. Everything that
decides equality is here, as data (`Rules`, the tables below) and as code (`compare`); nothing
else in the harness compares values. Each rule names the PARITY.md section it implements. Where
PARITY.md marks a rule as proposed and not in force, or a claim as UNVERIFIED, it is not applied.

The rules, in the order they are applied to one result of one step:

0. Failure (Must match, item 7; A5). If the reference failed to produce the result, the other
   side must fail too, with the same gRPC status code. The message is not compared. A
   refreshing table that failed in an update cycle is a failure at that step and every later
   one, with the status code the server refuses a new operation on it with, whatever its
   snapshot still answers: its contents are not compared, and both sides must have failed by
   the same step (A11; `runner.failed_in_cycle`).
1. Schema (Must match, item 1; Wire representation). Column count, names, order and Arrow types
   are exact, and so are the field metadata keys PARITY.md lists as compared
   (`Rules.compared_field_metadata`) and the schema-level table attributes clients act on
   (`Rules.compared_schema_metadata`). Nullability flags and all other schema-level metadata
   are not compared (A6).
2. Row count is exact (Must match, item 2).
3. Rows are compared by position, never by row key (Must match, item 3; A4). A result that the
   scenario marks unordered is compared as a multiset (A3); in v42.9 no operation qualifies.
4. Cells (Must match, items 4 and 5). Null equals only null. Booleans, integers, `char`
   (Arrow `uint16`, Wire representation), strings, timestamps, dates, times of day and durations (as the
   integer count the wire holds) are exact. Floats are exact as bit patterns, except that any NaN equals any NaN;
   so 0.0 differs from -0.0 (A2 is proposed, not in force).
5. Computed floats (A1). A float or double cell produced by `sum`, `avg` or an accumulating
   `update_by` operator, or by a formula that calls a library function that is not correctly
   rounded, matches within `max(REL * max(|a|, |b|), ABS)` when both sides are finite. Null,
   NaN and infinities are never covered by the tolerance. `computed_columns` decides which
   columns these are from the scenario's operations.
6. A column of a type the rules do not know is reported as `unsupported_type`, never passed.

A script scenario (`scripts.py`) adds one rule, applied to the script of each step before the
results of that step are compared:

7. Script outcome (Must match, item 7). If the reference's script failed, the other side's
   must fail too, with the same Python exception type, and for a `DHError` the same message
   before the cause (`table update operation failed.`). The cause, the traceback and the text
   of any other exception are not compared (A5). The type and that part of the message are:
   a script catches an exception by its type, and the package reproduces the reference's
   message for each operation (`docs/CONSOLE.md`); `tasks/6.3-python-script-oracle.md` sets
   this rule, which is stricter than A5 alone. A script that fails because the server's
   `deephaven` package lacks a name the script uses is `missing` (`missing_name`), unless the
   reference failed with that same exception type; the comparison of the scenario ends at
   that step.

Not decided by PARITY.md, and therefore treated as exact here: a formula with `+ - * / %` over
a computed column. PARITY.md says such a formula is one correctly rounded operation, which
holds only for identical operands. Scenarios should not compute on computed columns.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import pyarrow as pa
import pyarrow.types as pat

from .results import Failure, ResultSet, ScriptError


@dataclass(frozen=True)
class Tolerance:
    relative: float
    absolute: float


_FIELD_KEYS = (
    "type", "componentType", "isPartitioning", "isSortable", "isStyle", "isRowStyle",
    "isNumberFormat", "isDateFormat", "styleColumn", "numberFormatColumn", "dateFormatColumn",
    "description", "inputtable.isKey", "inputtable.isValue",
)
# io.deephaven.engine.table.Table: ADD_ONLY_TABLE_ATTRIBUTE, APPEND_ONLY_TABLE_ATTRIBUTE,
# BLINK_TABLE_ATTRIBUTE; the flat marker is written by BarrageUtil#attributesToMetadata. The
# names were confirmed against the running reference as well.
_TABLE_ATTRIBUTES = ("IsFlat", "AddOnly", "AppendOnly", "BlinkTable")


@dataclass(frozen=True)
class Rules:
    # A1: tolerance for computed cells, per column type.
    double_tolerance: Tolerance = Tolerance(relative=1e-9, absolute=1e-12)
    float_tolerance: Tolerance = Tolerance(relative=1e-5, absolute=1e-6)
    # Wire representation, "Schema metadata keys", field level: every key marked compared.
    compared_field_metadata: tuple[str, ...] = tuple(f"deephaven:{k}" for k in _FIELD_KEYS)
    # Wire representation: the schema-level attributes clients change behavior on.
    compared_schema_metadata: tuple[str, ...] = tuple(
        f"deephaven:{tag}.{name}"
        for name in _TABLE_ATTRIBUTES
        for tag in ("attribute", "attribute_type")
    )
    # At most this many cell mismatches are listed per table and step; the rest are counted.
    max_value_mismatches_per_table: int = 20

    def tolerance(self, arrow_type: pa.DataType) -> Tolerance:
        return self.double_tolerance if arrow_type == pa.float64() else self.float_tolerance


DEFAULT_RULES = Rules()

# A1: aggregations and update_by operators whose float and double results are computed.
COMPUTED_AGGREGATIONS = frozenset({
    "sum", "abs_sum", "avg", "weighted_avg", "weighted_sum", "var", "std", "median",
})
# Aggregations whose result cells are cells of the source column (or arrays of them).
PASS_THROUGH_AGGREGATIONS = frozenset({
    "min", "max", "first", "last", "sorted_first", "sorted_last", "group", "distinct", "unique",
    "pct",
})
# A1: "update_by operators that accumulate (cumulative sum and product, EMA family, rolling
# sum, avg, std, weighted avg)". An operator matches by the start of its name.
COMPUTED_UPDATE_BY = ("cum_sum", "cum_prod", "ema_", "ems_", "emmin_", "emmax_", "emstd_",
                      "rolling_sum_", "rolling_avg_", "rolling_std_", "rolling_wavg_")
# update_by operators whose result cells are cells of the source column (or arrays of them).
PASS_THROUGH_UPDATE_BY = ("cum_min", "cum_max", "forward_fill", "rolling_min_", "rolling_max_",
                          "rolling_group_")
# In a set of computed columns: every column is computed, except those listed as "!Name".
# Used where the columns are not named by the operation (agg_all_by).
EVERY_COLUMN = "*"


def is_computed(computed: frozenset[str] | set[str], column: str) -> bool:
    if column in computed:
        return True
    return EVERY_COLUMN in computed and f"!{column}" not in computed
# A1: "library functions that are not correctly rounded (exp, log, pow, the trigonometric
# functions, and anything built on them)".
INEXACT_FUNCTIONS = frozenset({
    "exp", "expm1", "log", "log10", "log1p", "pow", "sin", "cos", "tan", "asin", "acos",
    "atan", "atan2", "sinh", "cosh", "tanh",
})

# Mismatch kinds.
MISSING_TABLE = "missing_table"  # expected a result that the actual side lacks
EXTRA_TABLE = "extra_table"  # the actual side has a result that was not expected
FAILURE_EXPECTED = "failure_expected"  # the reference failed, the actual side succeeded
FAILURE_UNEXPECTED = "failure_unexpected"  # the reference succeeded, the actual side failed
FAILURE_CODE = "failure_code"  # both failed, with different gRPC status codes
COLUMN_COUNT = "column_count"
COLUMN_NAME = "column_name"
COLUMN_TYPE = "column_type"
COLUMN_METADATA = "column_metadata"
TABLE_METADATA = "table_metadata"
ROW_COUNT = "row_count"
VALUE = "value"
UNSUPPORTED_TYPE = "unsupported_type"
TRUNCATED = "truncated"  # more cell mismatches than `max_value_mismatches_per_table`
# The kinds of a script outcome (rule 7). Their `table` is `SCRIPT`.
SCRIPT_FAILURE_EXPECTED = "script_failure_expected"  # the reference's script failed
SCRIPT_FAILURE_UNEXPECTED = "script_failure_unexpected"  # only the actual side's failed
SCRIPT_ERROR_TYPE = "script_error_type"  # both failed, with different exception types
SCRIPT_ERROR_MESSAGE = "script_error_message"  # both raised DHError, for different operations
MISSING = "missing"  # the script uses a name of the deephaven package that the server lacks
SCRIPT = "(script)"

EXACT = "exact"
FLOAT = "float"
# Array cells (Arrow lists): the elements are compared by the element class, in order.
LIST_EXACT = "list_exact"
LIST_FLOAT = "list_float"


def _is_temporal(arrow_type: pa.DataType) -> bool:
    """Dates, times of day and durations: exact values that are compared as the integer
    count the wire holds. Not the interval of months a `Period` is sent as: pyarrow cannot
    read its cells."""
    return pat.is_date(arrow_type) or pat.is_time(arrow_type) or pat.is_duration(arrow_type)


def comparison_class(arrow_type: pa.DataType) -> str | None:
    """How cells of `arrow_type` are compared: EXACT, FLOAT, a list of either, or None when
    the rules do not know the type."""
    if pat.is_list(arrow_type) or pat.is_large_list(arrow_type):
        inner = comparison_class(arrow_type.value_type)
        return {EXACT: LIST_EXACT, FLOAT: LIST_FLOAT}.get(inner)
    if pat.is_floating(arrow_type):
        return FLOAT
    if (
        pat.is_boolean(arrow_type)
        or pat.is_signed_integer(arrow_type)
        # `char`, the one unsigned type on the wire (PARITY.md, Wire representation)
        or arrow_type == pa.uint16()
        or pat.is_string(arrow_type)
        or pat.is_timestamp(arrow_type)
        # `LocalDate`, `LocalTime` and `Duration` on the wire
        or _is_temporal(arrow_type)
    ):
        return EXACT
    return None


@dataclass(frozen=True)
class Mismatch:
    scenario: str
    table: str
    step: int
    kind: str
    column: str | None = None
    row: int | None = None
    expected: Any = None
    actual: Any = None
    note: str | None = None

    def __str__(self) -> str:
        parts = [f"scenario={self.scenario}", f"table={self.table}", f"step={self.step}",
                 f"kind={self.kind}"]
        if self.column is not None:
            parts.append(f"column={self.column}")
        if self.row is not None:
            parts.append(f"row={self.row}")
        parts.append(f"expected={self.expected!r}")
        parts.append(f"actual={self.actual!r}")
        if self.note:
            parts.append(f"({self.note})")
        return " ".join(parts)


def floats_identical(expected: float, actual: float) -> bool:
    """Must match, item 5: the same bit pattern, except that any NaN matches any NaN."""
    if math.isnan(expected) or math.isnan(actual):
        return math.isnan(expected) and math.isnan(actual)
    return expected == actual and math.copysign(1.0, expected) == math.copysign(1.0, actual)


def floats_within(expected: float, actual: float, tolerance: Tolerance) -> bool:
    """A1: finite values within the tolerance; NaN and infinities only match themselves."""
    if math.isnan(expected) or math.isnan(actual):
        return math.isnan(expected) and math.isnan(actual)
    if math.isinf(expected) or math.isinf(actual):
        return expected == actual
    bound = max(tolerance.relative * max(abs(expected), abs(actual)), tolerance.absolute)
    return abs(expected - actual) <= bound


def cells_equal(kind: str, expected: Any, actual: Any, tolerance: Tolerance | None) -> bool:
    """`tolerance` is None for a cell that must be exact."""
    if expected is None or actual is None:
        return expected is None and actual is None
    if kind in (LIST_EXACT, LIST_FLOAT):
        inner = EXACT if kind == LIST_EXACT else FLOAT
        return len(expected) == len(actual) and all(
            cells_equal(inner, e, a, tolerance) for e, a in zip(expected, actual))
    if kind == FLOAT:
        if tolerance is None:
            return floats_identical(expected, actual)
        return floats_within(expected, actual, tolerance)
    return expected == actual


# ---------------------------------------------------------------- which columns are computed

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CALL = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
_MATCH = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:==|=|>=|<=|>|<)\s*([A-Za-z_][A-Za-z0-9_]*)\s*$")


def split_pair(text: str) -> tuple[str, str]:
    """`"Out = expression"` -> (`Out`, `expression`); a bare name stands for `Name = Name`."""
    if _IDENTIFIER.match(text.strip()):
        return text.strip(), text.strip()
    out, _, expression = text.partition("=")
    return out.strip(), expression.strip()


def right_key(match: str) -> str:
    """The right table's column in a join match such as `K`, `A = B` or `X >= Y`."""
    found = _MATCH.match(match)
    return found.group(2) if found else match.strip()


def _formula_outputs(formulas: Iterable[str], known: set[str], keep: bool) -> set[str]:
    """The computed columns after `formulas`, which see the columns computed so far."""
    current = set(known)
    produced: dict[str, bool] = {}
    for formula in formulas:
        out, expression = split_pair(formula)
        if _IDENTIFIER.match(expression):
            # A plain column passes its cells through.
            computed = is_computed(current, expression)
        else:
            computed = any(name in INEXACT_FUNCTIONS for name in _CALL.findall(expression))
        produced[out] = computed
        current.discard(out)
        if computed:
            current.add(out)
    if keep:  # update: the source columns stay
        return current
    return {out for out, computed in produced.items() if computed}  # view, select


def computed_columns(operations: Iterable[Mapping[str, Any]]) -> dict[str, frozenset[str]]:
    """For each table of a scenario, the columns whose float cells A1 covers.

    A column is computed when A1 lists the operation that produced it, and stays computed while
    operations pass its cells through unchanged (filters, sorts, joins, merges, plain column
    references, and aggregations that select an element).
    """
    tables: dict[str, frozenset[str]] = {}

    def of(name: str) -> set[str]:
        return set(tables.get(name, frozenset()))  # inputs have no computed columns

    def aggregated(source: set[str], aggs: Iterable[Mapping[str, Any]]) -> set[str]:
        out: set[str] = set()
        for agg in aggs:
            for pair in agg.get("columns", ()):
                name, column = split_pair(pair)
                if agg["agg"] in COMPUTED_AGGREGATIONS:
                    out.add(name)
                elif agg["agg"] in PASS_THROUGH_AGGREGATIONS and is_computed(source, column):
                    out.add(name)
        return out

    for op in operations:
        kind = op["op"]
        if kind in ("where", "sort", "head", "tail", "slice", "flatten", "reverse", "filter",
                    "head_by", "tail_by", "snapshot", "group_by", "ungroup", "ring_table"):
            out = of(op["source"])
        elif kind in ("partition_merge", "partition_get"):
            # The rows of the source, by constituent; without the keys when they are dropped.
            out = of(op["source"]) - set(op["by"] if op.get("drop_keys") else ())
        elif kind == "keyed_transpose":
            # An aggregation by the row and column keys whose columns are named after the
            # values of the column keys, so they cannot be listed: as in `agg_all_by`, every
            # column but an exact row key when an aggregation computes or passes on a
            # computed column.
            source = of(op["source"])
            out = {key for key in op["row_by"] if is_computed(source, key)}
            if aggregated(source, op["aggs"]):
                out = {EVERY_COLUMN} | {f"!{key}" for key in op["row_by"]
                                        if not is_computed(source, key)}
        elif kind == "where_in":
            out = of(op["left"])
        elif kind == "drop_columns":
            out = of(op["source"]) - set(op["columns"])
        elif kind == "select_distinct":
            source = of(op["source"])
            out = {c for c in op["columns"] if is_computed(source, c)} or (
                source if not op["columns"] else set())
        elif kind == "merge":
            out = set().union(*(of(name) for name in op["sources"]))
        elif kind == "multi_join":
            out = set().union(*(of(item["table"]) for item in op["inputs"]))
        elif kind in ("view", "select", "update", "lazy_update", "update_view"):
            out = _formula_outputs(op["formulas"], of(op["source"]),
                                   keep=kind in ("update", "lazy_update", "update_view"))
        elif kind == "agg_by":
            source = of(op["source"])
            out = {key for key in op["by"] if is_computed(source, key)}
            out |= aggregated(source, op["aggs"])
        elif kind == "agg_all_by":
            source = of(op["source"])
            if op["agg"]["agg"] in COMPUTED_AGGREGATIONS:
                out = {EVERY_COLUMN} | {f"!{key}" for key in op["by"]
                                        if not is_computed(source, key)}
            elif op["agg"]["agg"] in PASS_THROUGH_AGGREGATIONS:
                out = source
            else:
                out = {key for key in op["by"] if is_computed(source, key)}
        elif kind == "update_by":
            source = of(op["source"])
            out = set(source)
            for ub in op["ops"]:
                for pair in ub["columns"]:
                    name, column = split_pair(pair)
                    out.discard(name)
                    if ub["op"].startswith(COMPUTED_UPDATE_BY):
                        out.add(name)
                    elif ub["op"].startswith(PASS_THROUGH_UPDATE_BY) and is_computed(
                            source, column):
                        out.add(name)
        elif kind in ("natural_join", "exact_join", "join", "aj", "raj", "left_join"):
            right = of(op["right"])
            out = of(op["left"])
            if op.get("joins") is None:
                out |= right - {right_key(match) for match in op["on"]}
            else:
                for pair in op["joins"]:
                    name, column = split_pair(pair)
                    if is_computed(right, column):
                        out.add(name)
        elif kind == "range_join":
            out = of(op["left"]) | aggregated(of(op["right"]), op["aggs"])
        elif kind == "snapshot_when":
            trigger = of(op["trigger"])
            out = of(op["source"])
            for pair in op.get("stamp_columns", ()):
                name, column = split_pair(pair)
                if is_computed(trigger, column):
                    out.add(name)
        elif kind == "column_statistics":
            # io.deephaven.server.table.stats: sums, averages and deviations of the column.
            out = {"SUM", "SUM_ABS", "SUM_SQRD", "AVG", "AVG_ABS", "STD_DEV"}
        elif kind in ("time_table", "empty_table", "meta_table"):
            out = set()
        else:
            raise ValueError(f"rules.py does not know which columns {kind!r} computes")
        tables[op["name"]] = frozenset(out)
    return tables


# ---------------------------------------------------------------- comparison

def _cells(column: pa.ChunkedArray) -> list[Any]:
    """The cells of a column as plain Python values; timestamps as their integer count,
    also inside arrays."""
    def count(arrow_type: pa.DataType) -> pa.DataType:
        # a date in days and a time in seconds or milliseconds are 32 bits wide
        return pa.int32() if arrow_type.bit_width == 32 else pa.int64()

    if pat.is_timestamp(column.type) or _is_temporal(column.type):
        column = column.cast(count(column.type))
    elif (pat.is_list(column.type) or pat.is_large_list(column.type)) and (
            pat.is_timestamp(column.type.value_type) or _is_temporal(column.type.value_type)):
        column = column.cast(pa.list_(count(column.type.value_type)))
    return column.to_pylist()


def _metadata(raw: Mapping[bytes, bytes] | None, keys: tuple[str, ...]) -> dict[str, str]:
    raw = raw or {}
    return {key: raw[key.encode()].decode() for key in keys if key.encode() in raw}


def _sort_key(kind: str, cell: Any) -> tuple[int, Any]:
    if cell is None:
        return (0, 0)
    if kind in (LIST_EXACT, LIST_FLOAT):
        inner = EXACT if kind == LIST_EXACT else FLOAT
        return (1, tuple(_sort_key(inner, item) for item in cell))
    if kind == FLOAT and math.isnan(cell):
        return (2, 0)
    return (1, cell)


def _canonical_order(kinds: list[str], columns: list[list[Any]], rows: int) -> list[int]:
    """Row positions sorted into the canonical order used for unordered results (A3).

    Rows are ordered by their exact columns first (in schema order), then by their floating
    point columns; within a column nulls come first, then values, then NaN. Two sides that are
    equal as multisets under the cell rules get the same order, except where rows tie on every
    exact column and differ in a float column by less than the tolerance; such a pair can be
    paired the wrong way round and reported. The comparison can therefore report a difference
    that a cleverer matching would not, but it can never accept two sides that differ.
    """
    order = [i for i, k in enumerate(kinds) if k in (EXACT, LIST_EXACT)] + [
        i for i, k in enumerate(kinds) if k in (FLOAT, LIST_FLOAT)
    ]
    return sorted(
        range(rows),
        key=lambda row: tuple(_sort_key(kinds[i], columns[i][row]) for i in order),
    )


def compare_tables(
    scenario: str,
    table: str,
    step: int,
    expected: pa.Table,
    actual: pa.Table,
    ordered: bool = True,
    computed: frozenset[str] | set[str] = frozenset(),
    rules: Rules = DEFAULT_RULES,
) -> list[Mismatch]:
    """Compares one result table of one step. Returns the differences; empty means equal.

    `computed` names the columns whose float cells are covered by the A1 tolerance.
    """

    def mismatch(kind: str, **fields: Any) -> Mismatch:
        return Mismatch(scenario=scenario, table=table, step=step, kind=kind, **fields)

    out: list[Mismatch] = []

    # 1. Schema.
    if expected.num_columns != actual.num_columns:
        return [mismatch(COLUMN_COUNT, expected=expected.column_names,
                         actual=actual.column_names)]
    comparable = True
    for index, (e_field, a_field) in enumerate(zip(expected.schema, actual.schema)):
        if e_field.name != a_field.name:
            out.append(mismatch(COLUMN_NAME, column=e_field.name, expected=e_field.name,
                                actual=a_field.name, note=f"column position {index}"))
            comparable = False
            continue
        if e_field.type != a_field.type:
            out.append(mismatch(COLUMN_TYPE, column=e_field.name, expected=str(e_field.type),
                                actual=str(a_field.type)))
            comparable = False
            continue
        e_meta = _metadata(e_field.metadata, rules.compared_field_metadata)
        a_meta = _metadata(a_field.metadata, rules.compared_field_metadata)
        if e_meta != a_meta:
            out.append(mismatch(COLUMN_METADATA, column=e_field.name, expected=e_meta,
                                actual=a_meta))
    e_meta = _metadata(expected.schema.metadata, rules.compared_schema_metadata)
    a_meta = _metadata(actual.schema.metadata, rules.compared_schema_metadata)
    if e_meta != a_meta:
        out.append(mismatch(TABLE_METADATA, expected=e_meta, actual=a_meta))
    kinds: list[str] = []
    for field in expected.schema:
        kind = comparison_class(field.type)
        if kind is None and comparable:
            out.append(mismatch(UNSUPPORTED_TYPE, column=field.name, expected=str(field.type),
                                actual=str(field.type),
                                note="rules.py has no comparison for this type"))
        kinds.append(kind)
    if not comparable or None in kinds:
        # Without matching names and types the cells cannot be lined up.
        return out
    tolerances = [
        rules.tolerance(field.type.value_type if kind == LIST_FLOAT else field.type)
        if kind in (FLOAT, LIST_FLOAT) and is_computed(computed, field.name) else None
        for field, kind in zip(expected.schema, kinds)
    ]

    # 2. Row count.
    if expected.num_rows != actual.num_rows:
        out.append(mismatch(ROW_COUNT, expected=expected.num_rows, actual=actual.num_rows))

    # 3. Row alignment.
    e_columns = [_cells(c) for c in expected.columns]
    a_columns = [_cells(c) for c in actual.columns]
    if ordered:
        e_order = list(range(expected.num_rows))
        a_order = list(range(actual.num_rows))
    else:
        e_order = _canonical_order(kinds, e_columns, expected.num_rows)
        a_order = _canonical_order(kinds, a_columns, actual.num_rows)

    # 4 and 5. Cells, over the rows both sides have.
    found = 0
    for position, (e_row, a_row) in enumerate(zip(e_order, a_order)):
        for index, name in enumerate(expected.column_names):
            e_cell, a_cell = e_columns[index][e_row], a_columns[index][a_row]
            if cells_equal(kinds[index], e_cell, a_cell, tolerances[index]):
                continue
            found += 1
            if found > rules.max_value_mismatches_per_table:
                continue
            note = None
            if not ordered:
                note = (
                    "unordered result: row is the position in canonical order; "
                    f"expected side row {e_row}, actual side row {a_row}"
                )
            out.append(mismatch(VALUE, column=name, row=position, expected=e_cell,
                                actual=a_cell, note=note))
    if found > rules.max_value_mismatches_per_table:
        out.append(mismatch(TRUNCATED, expected=found, actual=found,
                            note=f"{found} cell mismatches in total; first "
                                 f"{rules.max_value_mismatches_per_table} listed"))
    return out


def compare(
    expected: ResultSet,
    actual: ResultSet,
    unordered: frozenset[str] | set[str] = frozenset(),
    computed: Mapping[str, frozenset[str]] | None = None,
    rules: Rules = DEFAULT_RULES,
) -> list[Mismatch]:
    """Compares two result sets of the same scenario; `expected` is the reference side.

    `unordered` names the result tables whose row order the scenario declares not guaranteed.
    `computed` maps a table to its computed columns (see `computed_columns`); without it every
    float must match exactly, which is the comparison for two runs of the same server
    (PARITY.md, Determinism).
    """
    scenario = expected.scenario
    computed = computed or {}
    out: list[Mismatch] = []
    actual_keys = set(actual.keys())
    for step, table in expected.keys():
        if (step, table) not in actual_keys:
            out.append(Mismatch(scenario, table, step, MISSING_TABLE, expected="present",
                                actual="absent"))
            continue
        e_result, a_result = expected.get(step, table), actual.get(step, table)
        e_failed, a_failed = isinstance(e_result, Failure), isinstance(a_result, Failure)
        if e_failed and a_failed:
            if e_result.code != a_result.code:
                out.append(Mismatch(scenario, table, step, FAILURE_CODE,
                                    expected=e_result.code, actual=a_result.code))
        elif e_failed:
            out.append(Mismatch(scenario, table, step, FAILURE_EXPECTED,
                                expected=f"fails with {e_result.code}", actual="succeeds"))
        elif a_failed:
            out.append(Mismatch(scenario, table, step, FAILURE_UNEXPECTED,
                                expected="succeeds", actual=f"fails with {a_result.code}"))
        else:
            out.extend(
                compare_tables(scenario, table, step, e_result, a_result,
                               ordered=table not in unordered,
                               computed=computed.get(table, frozenset()), rules=rules)
            )
    expected_keys = set(expected.keys())
    for step, table in actual.keys():
        if (step, table) not in expected_keys:
            out.append(Mismatch(scenario, table, step, EXTRA_TABLE, expected="absent",
                                actual="present"))
    return out


# Operations whose result depends on when it was made or on the changes it has seen, not only
# on what its sources hold now.
HISTORY_DEPENDENT = frozenset({"snapshot", "snapshot_when", "time_table", "ring_table",
                               "keyed_transpose"})


def compare_with_fresh(scenario: Any, kept: ResultSet, fresh: ResultSet,
                       rules: Rules = DEFAULT_RULES, any_order: bool = False) -> list[Mismatch]:
    """Compares the results one server kept up to date through the steps (`kept`, the
    expected side) with what it answers for the same operations applied again after each
    step (`fresh`; see `runner.run_scenario`). Empty when the server agrees with itself.

    Computed floats get the A1 tolerance: an incremental sum and a sum from scratch add in
    different orders. Tables that depend on a `HISTORY_DEPENDENT` operation are left out.
    `any_order` compares every table as a multiset of rows.
    """
    depends: set[str] = set()
    for op in scenario.operations:
        if op["op"] in HISTORY_DEPENDENT or depends.intersection(
                op[key] for key in ("source", "left", "right", "trigger", "initial_groups",
                                    "key") if key in op
        ) or depends.intersection(op.get("sources", ())) or depends.intersection(
                item["table"] for item in op.get("inputs", ())):
            depends.add(op["name"])
    both = ResultSet(kept.scenario)
    again = ResultSet(fresh.scenario)
    for step, table in fresh.keys():
        if table not in depends:
            both.put(step, table, kept.get(step, table))
            again.put(step, table, fresh.get(step, table))
    unordered = frozenset(r.table for r in scenario.results if any_order or not r.ordered)
    return compare(both, again, unordered, computed_columns(scenario.operations), rules)


def differs_from_fresh(scenario: Any, fresh: ResultSet, actual: ResultSet,
                       at: Iterable[tuple[int, str]],
                       rules: Rules = DEFAULT_RULES, any_order: bool = False,
                       ) -> set[tuple[int, str]]:
    """Of the results `at` (step, table), those for which `actual`, the results another
    server kept up to date, are not what the reference answers from scratch (`fresh`; see
    `compare_with_fresh`), or that one of the two does not hold. Computed floats get the
    A1 tolerance. `any_order` compares every table as a multiset of rows. Column metadata is
    not compared: it is fixed when a table is made, and the comparison of the first step,
    where both servers' tables stand, already holds it to the rules (so a difference there
    that PARITY.md A23 allows does not hide a later answer that is the one from scratch)."""
    one, other = ResultSet(fresh.scenario), ResultSet(actual.scenario)
    missing = set()
    for step, table in at:
        if (step, table) in fresh.keys() and (step, table) in actual.keys():
            one.put(step, table, fresh.get(step, table))
            other.put(step, table, actual.get(step, table))
        else:
            missing.add((step, table))
    unordered = frozenset(r.table for r in scenario.results if any_order or not r.ordered)
    found = compare(one, other, unordered, computed_columns(scenario.operations), rules)
    return missing | {(m.step, m.table) for m in found if m.kind != COLUMN_METADATA}


# What Python says when a name is not there. The module or class must be one of the
# `deephaven` package, or the object one of a class that is not a builtin: a missing attribute
# of `None` or of a list is a defect in the script or in the package, not an absent name.
_MISSING_NAME = (
    re.compile(r"^No module named 'deephaven[\w.]*'"),
    re.compile(r"^cannot import name '\w+' from 'deephaven[\w.]*'"),
    re.compile(r"^module 'deephaven[\w.]*' has no attribute '\w+'"),
    re.compile(r"^(?:type object )?'(?P<owner>\w+)'(?: object)? has no attribute '\w+'"),
    re.compile(r"^(?:\w+\.)*\w+\(\) got an unexpected keyword argument '\w+'"),
)
_MISSING_NAME_TYPES = ("ModuleNotFoundError", "ImportError", "AttributeError", "TypeError")
_BUILTIN_OWNERS = frozenset({
    "NoneType", "bool", "builtin_function_or_method", "bytes", "dict", "float", "function",
    "generator", "int", "list", "method", "module", "range", "set", "str", "tuple", "type",
})


def missing_name(error: ScriptError | None) -> bool:
    """Whether a script failed because the server's `deephaven` package lacks a name.

    Decided from the exception's type and the start of its own message, which Python words
    the same way in every version the harness meets: the import of a module or of a name that
    is not there, an attribute that a module or an object does not have, or a keyword
    argument that a function does not take.
    """
    if error is None or error.type not in _MISSING_NAME_TYPES:
        return False
    for pattern in _MISSING_NAME:
        found = pattern.match(error.text)
        if found:
            return found.groupdict().get("owner") not in _BUILTIN_OWNERS
    return False


def compare_script_outcome(scenario: str, step: int, expected: ScriptError | None,
                           actual: ScriptError | None) -> list[Mismatch]:
    """Rule 7 for the script of one step; `None` is a script that ran to its end."""

    def mismatch(kind: str, want: Any, got: Any, note: str | None = None) -> list[Mismatch]:
        return [Mismatch(scenario, SCRIPT, step, kind, expected=want, actual=got, note=note)]

    def shown(error: ScriptError | None) -> str:
        return "succeeds" if error is None else f"fails with {error.type}"

    if actual is not None and missing_name(actual) and (
            expected is None or expected.type != actual.type):
        return mismatch(MISSING, shown(expected), f"{actual.type}: {actual.text}")
    if expected is None and actual is None:
        return []
    if expected is None:
        return mismatch(SCRIPT_FAILURE_UNEXPECTED, "succeeds", shown(actual), actual.text)
    if actual is None:
        return mismatch(SCRIPT_FAILURE_EXPECTED, shown(expected), "succeeds")
    if expected.type != actual.type:
        return mismatch(SCRIPT_ERROR_TYPE, expected.type, actual.type, actual.text)
    if expected.operation != actual.operation:
        return mismatch(SCRIPT_ERROR_MESSAGE, expected.operation, actual.operation)
    return []


def compare_scenario(scenario: Any, expected: ResultSet, actual: ResultSet,
                     exact: bool = False, rules: Rules = DEFAULT_RULES) -> list[Mismatch]:
    """`compare` with the unordered tables and computed columns taken from the scenario.

    `exact` drops the A1 tolerance: for comparing two runs of one server with each other.
    """
    unordered = frozenset(r.table for r in scenario.results if not r.ordered)
    computed = None if exact else computed_columns(scenario.operations)
    return compare(expected, actual, unordered, computed, rules)
