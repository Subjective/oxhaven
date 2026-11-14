"""Compares the outcome of a case on a server with its recorded Java outcome.

No Java counterpart: harness code. Everything is decided by `harness.rules`: failures by its
rule 0 (same gRPC status code, message not compared), values by `rules.compare_tables` (exact
cells, floats as bit patterns with NaN equal to NaN, the engine type in `deephaven:type` part of
the column type), and the A1 tolerance only for a formula that calls a function in
`rules.INEXACT_FUNCTIONS`. What this module adds, because a formula outcome is stored as JSON:

- `char` columns. They travel as Arrow `uint16`, which `rules.comparison_class` does not know.
  After the Arrow type and the engine type have been compared exactly, the code units are
  compared as integers.
- Result types the rules do not know (arrays, `BigDecimal`, dates, ...). The type is compared;
  the values are compared for exact equality of their recorded form, and the case is counted as
  `uncompared`, never as passed.
- Outcomes recorded as nondeterministic: only the type is compared.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa

from .. import rules
from .. import scenario as sc
from .corpus import POSITION, Case

_ARROW = {
    "bool": pa.bool_(), "int8": pa.int8(), "int16": pa.int16(), "int32": pa.int32(),
    "int64": pa.int64(), "float": pa.float32(), "double": pa.float64(), "string": pa.string(),
    "timestamp[ns, tz=UTC]": pa.timestamp("ns", tz="UTC"),
    # char: compared as integers once the types are known to agree.
    "uint16": pa.int32(),
}


def _column(outcome: dict[str, Any]) -> pa.Table:
    kind = _ARROW[outcome["arrow"]]
    values = outcome["values"]
    if pa.types.is_floating(kind):
        values = [sc.decode_cell("value", "float64", v) for v in values]
    field = pa.field("R", kind, metadata={"deephaven:type": outcome["type"] or ""})
    return pa.Table.from_arrays([pa.array(values, kind)], schema=pa.schema([field]))


def compare(case: Case, expected: dict[str, Any], actual: dict[str, Any], values: bool = True
            ) -> tuple[list[rules.Mismatch], bool]:
    """Returns the differences, and whether part of the outcome could not be compared.

    With `values` false only failure, status code and result type are compared.
    """

    def mismatch(kind: str, **fields: Any) -> rules.Mismatch:
        return rules.Mismatch(scenario=case.id, table=case.kind, step=0, kind=kind, **fields)

    e_error, a_error = expected.get("error"), actual.get("error")
    if e_error or a_error:
        if e_error and a_error:
            if e_error["status"] != a_error["status"]:
                return [mismatch(rules.FAILURE_CODE, expected=e_error["status"],
                                 actual=a_error["status"])], False
            return [], False
        if e_error:
            return [mismatch(rules.FAILURE_EXPECTED, expected=e_error["status"],
                             actual="no failure",
                             note=e_error.get("cause") or e_error.get("message"))], False
        return [mismatch(rules.FAILURE_UNEXPECTED, expected="no failure",
                         actual=a_error["status"], note=f"stage {a_error['stage']}")], False

    if case.kind == "where":
        if expected.get("nondeterministic"):
            return [], False
        e_table = pa.table({POSITION: pa.array(expected["rows"], pa.int64())})
        a_table = pa.table({POSITION: pa.array(actual["rows"], pa.int64())})
        return rules.compare_tables(case.id, "where", 0, e_table, a_table), False

    out: list[rules.Mismatch] = []
    for key, kind in (("arrow", rules.COLUMN_TYPE), ("type", rules.COLUMN_METADATA),
                      ("component", rules.COLUMN_METADATA)):
        if expected.get(key) != actual.get(key):
            out.append(mismatch(kind, column="R", expected=expected.get(key),
                                actual=actual.get(key), note=key))
    if out:
        return out, False
    if expected.get("nondeterministic") or not values:
        return [], False
    if expected["arrow"] not in _ARROW:
        e_values, a_values = expected["values"], actual["values"]
        if e_values is None or a_values is None:
            if e_values is not a_values:
                out.append(mismatch(rules.VALUE, column="R", expected=e_values, actual=a_values,
                                    note="values unreadable on one side only"))
            return out, True
        if len(e_values) != len(a_values):
            return [mismatch(rules.ROW_COUNT, expected=len(e_values), actual=len(a_values))], True
        for row, (e_cell, a_cell) in enumerate(zip(e_values, a_values)):
            if e_cell != a_cell:
                out.append(mismatch(rules.VALUE, column="R", row=row, expected=e_cell,
                                    actual=a_cell, note="type unknown to rules.py, exact form"))
        return out[: rules.DEFAULT_RULES.max_value_mismatches_per_table], True
    # A1: a formula that calls a library function that is not correctly rounded is computed.
    operation = {"name": "r", "op": "update", "source": "t", "formulas": [f"R = {case.formula}"]}
    computed = rules.computed_columns([operation])["r"]
    return rules.compare_tables(case.id, "update", 0, _column(expected), _column(actual),
                                computed=computed), False
