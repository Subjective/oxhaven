"""Fault injection: deliberate damage to one side's results, to prove the comparator sees it.

No Java counterpart: this tests the harness itself. Each fault changes one result table in one
way. `Fault.flagged` says whether the comparator must report it: every fault must be reported
except a float moved by less than the tolerance, and rows swapped in a result that the scenario
declares unordered.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable

import pyarrow as pa
import pyarrow.types as pat

from . import rules as r
from .results import ResultSet


class NotApplicable(Exception):
    """The table has nothing this fault can change (for example no null to flip)."""


@dataclass(frozen=True)
class Change:
    """What a fault did, so a test can check that the report names it."""

    table: pa.Table
    column: str | None = None
    row: int | None = None
    before: Any = None
    after: Any = None


@dataclass(frozen=True)
class Fault:
    name: str
    apply: Callable[[pa.Table, r.Rules], Change]
    # Must the comparator report it in an ordered result / in an unordered result?
    flagged_ordered: bool = True
    flagged_unordered: bool = True

    def flagged(self, ordered: bool) -> bool:
        return self.flagged_ordered if ordered else self.flagged_unordered


def _cells(table: pa.Table, index: int) -> list[Any]:
    column = table.column(index)
    if pat.is_timestamp(column.type):
        column = column.cast(pa.int64())
    return column.to_pylist()


def _with_cells(table: pa.Table, index: int, cells: list[Any]) -> pa.Table:
    field = table.schema.field(index)
    if pat.is_timestamp(field.type):
        array = pa.array(cells, pa.int64()).cast(field.type)
    else:
        array = pa.array(cells, field.type)
    return table.set_column(index, field, array)


def _supported(table: pa.Table) -> list[int]:
    return [i for i, f in enumerate(table.schema) if r.comparison_class(f.type) is not None]


def _different_value(arrow_type: pa.DataType, value: Any) -> Any:
    """A value of the same type that the rules treat as different from `value`."""
    if pat.is_boolean(arrow_type):
        return not value
    if pat.is_string(arrow_type):
        return value + "x"
    if pat.is_floating(arrow_type):
        if math.isnan(value) or math.isinf(value):
            return 0.0
        return value * 2.0 + 1.0 if value >= 0 else value * 2.0 - 1.0
    # Integers and timestamps: step by one, away from the upper limit of the type.
    bits = 64 if pat.is_timestamp(arrow_type) else arrow_type.bit_width
    return value - 1 if value == (1 << (bits - 1)) - 1 else value + 1


def _default_value(arrow_type: pa.DataType) -> Any:
    if pat.is_boolean(arrow_type):
        return False
    if pat.is_string(arrow_type):
        return ""
    if pat.is_floating(arrow_type):
        return 0.0
    return 0


def _first_cell(table: pa.Table, want: Callable[[pa.DataType, Any], bool]) -> tuple[int, int, Any]:
    for index in _supported(table):
        arrow_type = table.schema.field(index).type
        for row, cell in enumerate(_cells(table, index)):
            if want(arrow_type, cell):
                return index, row, cell
    raise NotApplicable


def _set_cell(table: pa.Table, index: int, row: int, before: Any, after: Any) -> Change:
    cells = _cells(table, index)
    cells[row] = after
    return Change(_with_cells(table, index, cells), table.column_names[index], row, before, after)


def changed_value(table: pa.Table, rules: r.Rules) -> Change:
    index, row, cell = _first_cell(table, lambda _t, c: c is not None)
    after = _different_value(table.schema.field(index).type, cell)
    return _set_cell(table, index, row, cell, after)


def swapped_rows(table: pa.Table, rules: r.Rules) -> Change:
    """Swaps the first two rows that differ under the rules."""
    indexes = _supported(table)
    if len(indexes) != table.num_columns:
        raise NotApplicable
    kinds = [r.comparison_class(f.type) for f in table.schema]
    columns = [_cells(table, i) for i in indexes]

    def same(a: int, b: int) -> bool:
        return all(
            r.cells_equal(kinds[i], columns[i][a], columns[i][b], rules) for i in indexes
        )

    for first in range(table.num_rows):
        for second in range(first + 1, table.num_rows):
            if not same(first, second):
                order = list(range(table.num_rows))
                order[first], order[second] = second, first
                return Change(table.take(pa.array(order, pa.int64())), None, first)
    raise NotApplicable


def null_to_value(table: pa.Table, rules: r.Rules) -> Change:
    index, row, _cell = _first_cell(table, lambda _t, c: c is None)
    return _set_cell(table, index, row, None, _default_value(table.schema.field(index).type))


def value_to_null(table: pa.Table, rules: r.Rules) -> Change:
    index, row, cell = _first_cell(table, lambda _t, c: c is not None)
    return _set_cell(table, index, row, cell, None)


def dropped_row(table: pa.Table, rules: r.Rules) -> Change:
    if table.num_rows == 0:
        raise NotApplicable
    return Change(table.slice(0, table.num_rows - 1), None, table.num_rows - 1)


def extra_row(table: pa.Table, rules: r.Rules) -> Change:
    """Appends a copy of the last row, or a row of nulls to an empty table."""
    if table.num_rows > 0:
        extra = table.slice(table.num_rows - 1, 1)
    else:
        extra = pa.Table.from_arrays(
            [pa.nulls(1, f.type) for f in table.schema], schema=table.schema
        )
    return Change(pa.concat_tables([table, extra]), None, table.num_rows)


def renamed_column(table: pa.Table, rules: r.Rules) -> Change:
    if table.num_columns == 0:
        raise NotApplicable
    names = table.column_names
    before = names[0]
    names[0] = before + "_renamed"
    renamed = table.rename_columns(names)
    # rename_columns drops field metadata; put it back so that only the name differs.
    fields = [
        f.with_metadata(table.schema.field(i).metadata) if table.schema.field(i).metadata else f
        for i, f in enumerate(renamed.schema)
    ]
    renamed = renamed.cast(pa.schema(fields, metadata=table.schema.metadata))
    return Change(renamed, before, None, before, names[0])


_OTHER_TYPE = {
    pa.bool_(): pa.int8(),
    pa.int8(): pa.int16(),
    pa.int16(): pa.int32(),
    pa.int32(): pa.int64(),
    pa.int64(): pa.float64(),
    pa.float32(): pa.float64(),
    pa.float64(): pa.float32(),
    pa.string(): pa.large_string(),
}


def changed_column_type(table: pa.Table, rules: r.Rules) -> Change:
    """Gives the first possible column another Arrow type, keeping names and metadata."""
    for index, field in enumerate(table.schema):
        if pat.is_timestamp(field.type):
            other = pa.timestamp("us" if field.type.unit == "ns" else "ns", tz=field.type.tz)
        elif field.type in _OTHER_TYPE:
            other = _OTHER_TYPE[field.type]
        else:
            continue
        array = table.column(index).cast(other, safe=False)
        new_field = pa.field(field.name, other, field.nullable, field.metadata)
        return Change(table.set_column(index, new_field, array), field.name, None,
                      str(field.type), str(other))
    raise NotApplicable


def changed_engine_type(table: pa.Table, rules: r.Rules) -> Change:
    """Changes the `deephaven:type` annotation of the first column that has one."""
    key = b"deephaven:type"
    for index, field in enumerate(table.schema):
        metadata = dict(field.metadata or {})
        if key not in metadata:
            continue
        before = metadata[key].decode()
        metadata[key] = b"java.lang.Object"
        if before == "java.lang.Object":
            metadata[key] = b"int"
        new_field = field.with_metadata(metadata)
        return Change(table.set_column(index, new_field, table.column(index)), field.name, None,
                      before, metadata[key].decode())
    raise NotApplicable


def _float_shift(inside: bool) -> Callable[[pa.Table, r.Rules], Change]:
    """Moves one finite float64 cell just inside or just beyond the tolerance."""

    def apply(table: pa.Table, rules: r.Rules) -> Change:
        factor = 0.5 if inside else 2.0
        for index, field in enumerate(table.schema):
            # float32 cannot represent a relative step this small, so only float64 is used.
            if field.type != pa.float64():
                continue
            for row, cell in enumerate(_cells(table, index)):
                if cell is None or math.isnan(cell) or math.isinf(cell):
                    continue
                step = factor * max(
                    rules.float_absolute_floor, rules.float_relative_tolerance * abs(cell)
                )
                after = cell + step
                # The fault must be a real change, and on the intended side of the tolerance.
                if after == cell or math.isinf(after):
                    continue
                if r.floats_equal(cell, after, rules) != inside:
                    continue
                return _set_cell(table, index, row, cell, after)
        raise NotApplicable

    return apply


FAULTS: dict[str, Fault] = {
    f.name: f
    for f in (
        Fault("changed_value", changed_value),
        Fault("swapped_rows", swapped_rows, flagged_unordered=False),
        Fault("null_to_value", null_to_value),
        Fault("value_to_null", value_to_null),
        Fault("dropped_row", dropped_row),
        Fault("extra_row", extra_row),
        Fault("renamed_column", renamed_column),
        Fault("changed_column_type", changed_column_type),
        Fault("changed_engine_type", changed_engine_type),
        Fault("float_beyond_tolerance", _float_shift(inside=False)),
        Fault("float_inside_tolerance", _float_shift(inside=True),
              flagged_ordered=False, flagged_unordered=False),
    )
}


def inject(
    results: ResultSet,
    step: int,
    table: str,
    fault: str,
    rules: r.Rules = r.DEFAULT_RULES,
) -> tuple[ResultSet, Change]:
    """Returns a copy of `results` with `fault` applied to one table, and what was changed.

    Raises NotApplicable when the table offers nothing for this fault to change.
    """
    change = FAULTS[fault].apply(results.get(step, table), rules)
    damaged = results.copy()
    damaged.put(step, table, change.table)
    return damaged, change
