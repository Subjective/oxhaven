"""Fault injection: deliberate damage to one side's results, to prove the comparator sees it.

No Java counterpart: this tests the harness itself. Each fault changes one result in one way.
`Fault.flagged` says whether the comparator must report it: every fault must be reported except
a computed float moved by less than its tolerance, and rows swapped in a result that the
scenario declares unordered.

`FaultyTarget` wraps a server so that every result it returns is damaged where the fault
applies. It stands in for a server with a bug when the fuzzer and its shrinker are tested.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pyarrow as pa
import pyarrow.types as pat

from . import rules as r
from .results import Failure, ResultSet


class NotApplicable(Exception):
    """The result has nothing this fault can change (for example no null to flip)."""


@dataclass(frozen=True)
class Context:
    """What a fault needs to know about the table it damages."""

    rules: r.Rules = r.DEFAULT_RULES
    # The columns whose float cells the A1 tolerance covers (rules.computed_columns).
    computed: frozenset[str] = frozenset()


@dataclass(frozen=True)
class Change:
    """What a fault did, so a test can check that the report names it."""

    table: pa.Table | None
    column: str | None = None
    row: int | None = None
    before: Any = None
    after: Any = None


@dataclass(frozen=True)
class Fault:
    name: str
    apply: Callable[[pa.Table, Context], Change]
    # Must the comparator report it in an ordered result / in an unordered result?
    flagged_ordered: bool = True
    flagged_unordered: bool = True

    def flagged(self, ordered: bool) -> bool:
        return self.flagged_ordered if ordered else self.flagged_unordered


def _cells(table: pa.Table, index: int) -> list[Any]:
    return r._cells(table.column(index))  # noqa: SLF001


def _with_cells(table: pa.Table, index: int, cells: list[Any]) -> pa.Table:
    field = table.schema.field(index)
    if pat.is_timestamp(field.type):
        array = pa.array(cells, pa.int64()).cast(field.type)
    elif (pat.is_list(field.type) or pat.is_large_list(field.type)) and pat.is_timestamp(
            field.type.value_type):
        array = pa.array(cells, pa.list_(pa.int64())).cast(field.type)
    else:
        array = pa.array(cells, field.type)
    return table.set_column(index, field, array)


def _supported(table: pa.Table) -> list[int]:
    return [i for i, f in enumerate(table.schema) if r.comparison_class(f.type) is not None]


def _tolerance(table: pa.Table, index: int, ctx: Context) -> r.Tolerance | None:
    field = table.schema.field(index)
    if pat.is_floating(field.type) and r.is_computed(ctx.computed, field.name):
        return ctx.rules.tolerance(field.type)
    return None


def _round_to(arrow_type: pa.DataType, value: float) -> float:
    """`value` as the column type stores it."""
    return float(np.float32(value)) if arrow_type == pa.float32() else value


def _different_value(arrow_type: pa.DataType, value: Any) -> Any:
    """A value of the same type that the rules treat as different from `value`, under the
    widest tolerance."""
    if pat.is_boolean(arrow_type):
        return not value
    if pat.is_string(arrow_type):
        return value + "x"
    if pat.is_list(arrow_type) or pat.is_large_list(arrow_type):
        return list(value) + [None]
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
    if pat.is_list(arrow_type) or pat.is_large_list(arrow_type):
        return []
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
    damaged = _with_cells(table, index, cells)
    return Change(damaged, table.column_names[index], row, before, _cells(damaged, index)[row])


def changed_value(table: pa.Table, ctx: Context) -> Change:
    index, row, cell = _first_cell(table, lambda _t, c: c is not None)
    after = _different_value(table.schema.field(index).type, cell)
    return _set_cell(table, index, row, cell, after)


def swapped_rows(table: pa.Table, ctx: Context) -> Change:
    """Swaps the first two rows that differ under the rules."""
    indexes = _supported(table)
    if len(indexes) != table.num_columns:
        raise NotApplicable
    kinds = [r.comparison_class(f.type) for f in table.schema]
    columns = [_cells(table, i) for i in indexes]
    tolerances = [_tolerance(table, i, ctx) for i in indexes]

    def same(a: int, b: int) -> bool:
        return all(
            r.cells_equal(kinds[i], columns[i][a], columns[i][b], tolerances[i])
            for i in indexes
        )

    for first in range(table.num_rows):
        for second in range(first + 1, table.num_rows):
            if not same(first, second):
                order = list(range(table.num_rows))
                order[first], order[second] = second, first
                return Change(table.take(pa.array(order, pa.int64())), None, first)
    raise NotApplicable


def null_to_value(table: pa.Table, ctx: Context) -> Change:
    index, row, _cell = _first_cell(table, lambda _t, c: c is None)
    return _set_cell(table, index, row, None, _default_value(table.schema.field(index).type))


def value_to_null(table: pa.Table, ctx: Context) -> Change:
    index, row, cell = _first_cell(table, lambda _t, c: c is not None)
    return _set_cell(table, index, row, cell, None)


def dropped_row(table: pa.Table, ctx: Context) -> Change:
    if table.num_rows == 0:
        raise NotApplicable
    return Change(table.slice(0, table.num_rows - 1), None, table.num_rows - 1)


def extra_row(table: pa.Table, ctx: Context) -> Change:
    """Appends a copy of the last row, or a row of nulls to an empty table."""
    if table.num_columns == 0:
        # pyarrow cannot add a row to a table without columns (the result of empty_table):
        # concat_tables and from_arrays both answer with zero rows.
        raise NotApplicable
    if table.num_rows > 0:
        extra = table.slice(table.num_rows - 1, 1)
    else:
        extra = pa.Table.from_arrays(
            [pa.nulls(1, f.type) for f in table.schema], schema=table.schema
        )
    return Change(pa.concat_tables([table, extra]), None, table.num_rows)


def renamed_column(table: pa.Table, ctx: Context) -> Change:
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


def changed_column_type(table: pa.Table, ctx: Context) -> Change:
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


def changed_engine_type(table: pa.Table, ctx: Context) -> Change:
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


def changed_table_attribute(table: pa.Table, ctx: Context) -> Change:
    """Adds or removes the blink marker in the schema metadata."""
    if table.num_columns == 0:
        # replace_schema_metadata answers with zero rows for a table without columns.
        raise NotApplicable
    metadata = dict(table.schema.metadata or {})
    key = b"deephaven:attribute.BlinkTable"
    before = metadata.get(key)
    if before is None:
        metadata[key] = b"true"
    else:
        del metadata[key]
    return Change(table.replace_schema_metadata(metadata), None, None, before, metadata.get(key))


def _float_shift(arrow_type: pa.DataType, inside: bool) -> Callable[[pa.Table, Context], Change]:
    """Moves one finite computed cell of `arrow_type` just inside or just beyond its tolerance."""

    def apply(table: pa.Table, ctx: Context) -> Change:
        tolerance = ctx.rules.tolerance(arrow_type)
        factor = 0.5 if inside else 2.0
        for index, field in enumerate(table.schema):
            if field.type != arrow_type or not r.is_computed(ctx.computed, field.name):
                continue
            for row, cell in enumerate(_cells(table, index)):
                if cell is None or math.isnan(cell) or math.isinf(cell):
                    continue
                step = factor * max(tolerance.absolute, tolerance.relative * abs(cell))
                after = _round_to(arrow_type, cell + step)
                # The fault must be a real change, and on the intended side of the tolerance.
                if after == cell or math.isinf(after):
                    continue
                if r.floats_within(cell, after, tolerance) != inside:
                    continue
                return _set_cell(table, index, row, cell, after)
        raise NotApplicable

    return apply


def _exact_float_cell(table: pa.Table, ctx: Context,
                      want: Callable[[float], bool]) -> tuple[int, int, float]:
    for index, field in enumerate(table.schema):
        if not pat.is_floating(field.type) or r.is_computed(ctx.computed, field.name):
            continue
        for row, cell in enumerate(_cells(table, index)):
            if cell is not None and want(cell):
                return index, row, cell
    raise NotApplicable


def exact_float_one_ulp(table: pa.Table, ctx: Context) -> Change:
    """Moves a float that must be bit-exact (not computed) to the next representable value."""
    index, row, cell = _exact_float_cell(
        table, ctx, lambda c: not math.isnan(c) and not math.isinf(c) and c != 0.0
    )
    if table.schema.field(index).type == pa.float32():
        after = float(np.nextafter(np.float32(cell), np.float32(np.inf)))
    else:
        after = math.nextafter(cell, math.inf)
    if math.isinf(after):
        raise NotApplicable
    return _set_cell(table, index, row, cell, after)


def exact_float_zero_sign(table: pa.Table, ctx: Context) -> Change:
    """Flips the sign of a zero in a float column that must be bit-exact."""
    index, row, cell = _exact_float_cell(table, ctx, lambda c: c == 0.0)
    return _set_cell(table, index, row, cell, -cell)


def computed_float_zero_sign(table: pa.Table, ctx: Context) -> Change:
    """Flips the sign of a zero in a computed float column: within tolerance, so accepted."""
    for index, field in enumerate(table.schema):
        if not pat.is_floating(field.type) or not r.is_computed(ctx.computed, field.name):
            continue
        for row, cell in enumerate(_cells(table, index)):
            if cell is not None and cell == 0.0:
                return _set_cell(table, index, row, cell, -cell)
    raise NotApplicable


def nan_to_null(table: pa.Table, ctx: Context) -> Change:
    """Turns every NaN into null. Applies only to a table that holds a NaN."""
    changed = None
    for index, field in enumerate(table.schema):
        if not pat.is_floating(field.type):
            continue
        cells = _cells(table, index)
        if any(c is not None and math.isnan(c) for c in cells):
            cells = [None if c is not None and math.isnan(c) else c for c in cells]
            table = _with_cells(table, index, cells)
            changed = changed or field.name
    if changed is None:
        raise NotApplicable
    return Change(table, changed)


def truncated_after_three(table: pa.Table, ctx: Context) -> Change:
    """Keeps only the first three rows. Applies only to a table with more."""
    if table.num_rows <= 3:
        raise NotApplicable
    return Change(table.slice(0, 3), None, 3)


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
        Fault("changed_table_attribute", changed_table_attribute),
        Fault("double_beyond_tolerance", _float_shift(pa.float64(), inside=False)),
        Fault("double_inside_tolerance", _float_shift(pa.float64(), inside=True),
              flagged_ordered=False, flagged_unordered=False),
        Fault("float_beyond_tolerance", _float_shift(pa.float32(), inside=False)),
        Fault("float_inside_tolerance", _float_shift(pa.float32(), inside=True),
              flagged_ordered=False, flagged_unordered=False),
        Fault("exact_float_one_ulp", exact_float_one_ulp),
        Fault("exact_float_zero_sign", exact_float_zero_sign),
        Fault("computed_float_zero_sign", computed_float_zero_sign,
              flagged_ordered=False, flagged_unordered=False),
        Fault("nan_to_null", nan_to_null),
        Fault("truncated_after_three", truncated_after_three),
    )
}

# Faults on a result as a whole: whether it failed, and with which status code. All flagged.
RESULT_FAULTS = ("failure_to_table", "table_to_failure", "failure_code_changed")
ALL_FAULTS = tuple(FAULTS) + RESULT_FAULTS


def inject(
    results: ResultSet,
    step: int,
    table: str,
    fault: str,
    ctx: Context = Context(),
) -> tuple[ResultSet, Change]:
    """Returns a copy of `results` with `fault` applied to one result, and what was changed.

    Raises NotApplicable when the result offers nothing for this fault to change.
    """
    target = results.get(step, table)
    damaged = results.copy()
    if fault in RESULT_FAULTS:
        after: pa.Table | Failure
        if fault == "table_to_failure":
            if isinstance(target, Failure):
                raise NotApplicable
            after = Failure("INVALID_ARGUMENT")
        elif not isinstance(target, Failure):
            raise NotApplicable
        elif fault == "failure_to_table":
            after = pa.table({"X": pa.array([1], pa.int32())})
        else:
            after = Failure("INTERNAL" if target.code != "INTERNAL" else "UNKNOWN")
        damaged.put(step, table, after)
        return damaged, Change(None, before=target, after=after)
    if isinstance(target, Failure):
        raise NotApplicable
    change = FAULTS[fault].apply(target, ctx)
    damaged.put(step, table, change.table)
    return damaged, change


class FaultyTarget:
    """A server whose results come back damaged: `fault` is applied to every result where it
    applies. Written `faulty:<fault>@<url>` on the command line (runner.open_target), or
    `faulty:<fault>:<op>@<url>` to damage only the results of one kind of operation, like a
    server with a bug in that operator."""

    def __init__(self, fault: str, inner: Any):
        self.spec = fault
        self.fault, _, self.op = fault.partition(":")
        if self.fault not in ALL_FAULTS:
            raise ValueError(f"unknown fault {self.fault!r}; known: {sorted(ALL_FAULTS)}")
        self.inner = inner

    def __str__(self) -> str:
        return f"faulty:{self.spec}@{self.inner}"

    def run(self, scenario: Any) -> ResultSet:
        results = self.inner.run(scenario)
        computed = r.computed_columns(scenario.operations)
        kinds = {op["name"]: op["op"] for op in scenario.operations}
        for step, table in results.keys():
            if self.op and kinds.get(table) != self.op:
                continue
            ctx = Context(computed=computed.get(table, frozenset()))
            try:
                results, _ = inject(results, step, table, self.fault, ctx)
            except NotApplicable:
                continue
        return results
