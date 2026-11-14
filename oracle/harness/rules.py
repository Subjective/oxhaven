"""The comparison rules: when two result sets count as the same.

No Java counterpart: this module is the harness's statement of "matches Java". It is meant to be
read next to PARITY.md and changed only together with it. Everything that decides equality is
here, as data (`Rules`, the type tables) and as code (`compare`); nothing else in the harness
compares values.

The rules, in the order they are applied to one result table of one step:

1. Schema. Column count, names, order and Arrow types are exact. The field metadata keys in
   `Rules.compared_field_metadata` are exact too. Nullability flags and schema-level metadata
   are not compared.
2. Row count is exact.
3. Rows are compared by position, never by row key. If the scenario marks the result unordered,
   both sides are first put into one canonical order (see `_canonical_order`), which makes the
   comparison a multiset comparison.
4. Cells. Null equals null and nothing else. For the exact types (booleans, integers, strings,
   timestamps) values are equal when identical. For floating point types NaN equals NaN,
   infinities are equal when they have the same sign, and finite values `e` and `a` are equal
   when `|e - a| <= max(float_absolute_floor, float_relative_tolerance * max(|e|, |a|))`.
   A consequence: 0.0 and -0.0 are equal.
5. A column of any other type is reported as `unsupported_type`, not compared and not passed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.types as pat

from .results import ResultSet


@dataclass(frozen=True)
class Rules:
    # Finite floats are equal within this relative tolerance ...
    float_relative_tolerance: float = 1e-9
    # ... or within this absolute difference, which is what lets values at or near zero match.
    float_absolute_floor: float = 1e-12
    # Field metadata that is part of a column's type. `deephaven:type` is the engine type of the
    # column (for example `int`, `java.time.Instant`), which clients use to interpret the data.
    compared_field_metadata: tuple[str, ...] = ("deephaven:type",)
    # At most this many cell mismatches are listed per table and step; the rest are counted.
    max_value_mismatches_per_table: int = 20


DEFAULT_RULES = Rules()

# Mismatch kinds.
MISSING_TABLE = "missing_table"  # expected a result table that the actual side lacks
EXTRA_TABLE = "extra_table"  # the actual side has a result table that was not expected
COLUMN_COUNT = "column_count"
COLUMN_NAME = "column_name"
COLUMN_TYPE = "column_type"
COLUMN_METADATA = "column_metadata"
ROW_COUNT = "row_count"
VALUE = "value"
UNSUPPORTED_TYPE = "unsupported_type"
TRUNCATED = "truncated"  # more cell mismatches than `max_value_mismatches_per_table`

EXACT = "exact"
FLOAT = "float"


def comparison_class(arrow_type: pa.DataType) -> str | None:
    """How cells of `arrow_type` are compared: EXACT, FLOAT, or None when unsupported."""
    if pat.is_floating(arrow_type):
        return FLOAT
    if (
        pat.is_boolean(arrow_type)
        or pat.is_signed_integer(arrow_type)
        or pat.is_string(arrow_type)
        or pat.is_timestamp(arrow_type)
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


def floats_equal(expected: float, actual: float, rules: Rules = DEFAULT_RULES) -> bool:
    if math.isnan(expected) or math.isnan(actual):
        return math.isnan(expected) and math.isnan(actual)
    if math.isinf(expected) or math.isinf(actual):
        return expected == actual
    bound = max(
        rules.float_absolute_floor,
        rules.float_relative_tolerance * max(abs(expected), abs(actual)),
    )
    return abs(expected - actual) <= bound


def cells_equal(kind: str, expected: Any, actual: Any, rules: Rules = DEFAULT_RULES) -> bool:
    if expected is None or actual is None:
        return expected is None and actual is None
    if kind == FLOAT:
        return floats_equal(expected, actual, rules)
    return expected == actual


def _cells(column: pa.ChunkedArray) -> list[Any]:
    """The cells of a column as plain Python values; timestamps as their integer count."""
    if pat.is_timestamp(column.type):
        column = column.cast(pa.int64())
    return column.to_pylist()


def _metadata(field: pa.Field, keys: tuple[str, ...]) -> dict[str, str | None]:
    raw = field.metadata or {}
    out = {}
    for key in keys:
        value = raw.get(key.encode())
        out[key] = value.decode() if value is not None else None
    return out


def _sort_key(kind: str, cell: Any) -> tuple[int, Any]:
    if cell is None:
        return (0, 0)
    if kind == FLOAT and math.isnan(cell):
        return (2, 0)
    return (1, cell)


def _canonical_order(kinds: list[str], columns: list[list[Any]], rows: int) -> list[int]:
    """Row positions sorted into the canonical order used for unordered results.

    Rows are ordered by their exact columns first (in schema order), then by their floating
    point columns; within a column nulls come first, then values, then NaN. Two sides that are
    equal as multisets under the cell rules get the same order, except where rows tie on every
    exact column and differ in a float column by less than the tolerance; such a pair can be
    paired the wrong way round and reported. The comparison can therefore report a difference
    that a cleverer matching would not, but it can never accept two sides that differ.
    """
    order = [i for i, k in enumerate(kinds) if k == EXACT] + [
        i for i, k in enumerate(kinds) if k == FLOAT
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
    rules: Rules = DEFAULT_RULES,
) -> list[Mismatch]:
    """Compares one result table of one step. Returns the differences; empty means equal."""

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
        e_meta = _metadata(e_field, rules.compared_field_metadata)
        a_meta = _metadata(a_field, rules.compared_field_metadata)
        if e_meta != a_meta:
            out.append(mismatch(COLUMN_METADATA, column=e_field.name, expected=e_meta,
                                actual=a_meta))
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

    # 4. Cells, over the rows both sides have.
    found = 0
    for position, (e_row, a_row) in enumerate(zip(e_order, a_order)):
        for index, name in enumerate(expected.column_names):
            e_cell, a_cell = e_columns[index][e_row], a_columns[index][a_row]
            if cells_equal(kinds[index], e_cell, a_cell, rules):
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
    rules: Rules = DEFAULT_RULES,
) -> list[Mismatch]:
    """Compares two result sets of the same scenario.

    `unordered` names the result tables whose row order the scenario declares not guaranteed.
    """
    scenario = expected.scenario
    out: list[Mismatch] = []
    actual_keys = set(actual.keys())
    for step, table in expected.keys():
        if (step, table) not in actual_keys:
            out.append(Mismatch(scenario, table, step, MISSING_TABLE, expected="present",
                                actual="absent"))
            continue
        out.extend(
            compare_tables(scenario, table, step, expected.get(step, table),
                           actual.get(step, table), ordered=table not in unordered, rules=rules)
        )
    expected_keys = set(expected.keys())
    for step, table in actual.keys():
        if (step, table) not in expected_keys:
            out.append(Mismatch(scenario, table, step, EXTRA_TABLE, expected="absent",
                                actual="present"))
    return out
