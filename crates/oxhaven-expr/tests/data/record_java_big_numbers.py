"""Records what Java computes for formulas over BigDecimal and BigInteger values.

No Java counterpart: this produces java_big_numbers.json, which the test
`tests/formulas/big_numbers.rs` compares Oxhaven's formula compiler against.

A client is sent a BigDecimal without the zeros at the end of its digits, so it cannot see
the scale of a result. This script therefore runs Deephaven 42.9 embedded in Python, from the
`deephaven-server` wheel of the oracle's environment (`oracle/requirements.txt`), as a
script of the console does, and records the `toString` text of every value:

    oracle/.venv/bin/python crates/oxhaven-expr/tests/data/record_java_big_numbers.py

Each line of java_big_numbers.txt is a formula, recorded as the type and values of the
column `R = <formula>` over the table FIXTURE makes, or, after `nonnull `, over its rows 2 to
7, where the big number columns hold no null. A line that starts with `#` is a comment.
When Java refuses the formula, the record says whether it failed to compile or while it was
evaluated, with the message of the cause.
"""

import json
import os
import sys
from pathlib import Path

from deephaven_server import Server

HERE = Path(__file__).resolve().parent

FIXTURE = [
    'BD = (java.math.BigDecimal) (i == 0 ? null : i == 1 ? new java.math.BigDecimal("1.50")'
    ' : i == 2 ? new java.math.BigDecimal("-2.345") : i == 3 ? new java.math.BigDecimal("0")'
    ' : i == 4 ? new java.math.BigDecimal("12345678901234567890.123456789")'
    ' : i == 5 ? new java.math.BigDecimal("1E+3") : i == 6 ? new java.math.BigDecimal("-0.5")'
    ' : new java.math.BigDecimal("1E-7"))',
    'BD2 = (java.math.BigDecimal) (i == 0 ? new java.math.BigDecimal("1.5") : i == 1 ? null'
    ' : i == 2 ? new java.math.BigDecimal("0.10") : i == 3 ? new java.math.BigDecimal("-7")'
    ' : i == 4 ? new java.math.BigDecimal("3") : i == 5 ? new java.math.BigDecimal("7.000")'
    ' : i == 6 ? new java.math.BigDecimal("2E+1") : new java.math.BigDecimal("-0.3"))',
    'BI = (java.math.BigInteger) (i == 0 ? null : i == 1 ? new java.math.BigInteger("7")'
    ' : i == 2 ? new java.math.BigInteger("-7") : i == 3 ? new java.math.BigInteger("0")'
    ' : i == 4 ? new java.math.BigInteger("123456789012345678901234567890")'
    ' : i == 5 ? new java.math.BigInteger("10") : i == 6 ? new java.math.BigInteger("-5")'
    ' : new java.math.BigInteger("9223372036854775808"))',
    'BI2 = (java.math.BigInteger) (i == 0 ? new java.math.BigInteger("3") : i == 1 ? null'
    ' : i == 2 ? new java.math.BigInteger("2") : i == 3 ? new java.math.BigInteger("5")'
    ' : i == 4 ? new java.math.BigInteger("-4") : i == 5 ? new java.math.BigInteger("3")'
    ' : i == 6 ? new java.math.BigInteger("-3") : new java.math.BigInteger("-1"))',
    "I = i == 1 ? NULL_INT : i == 2 ? -3 : i == 4 ? 7 : i == 6 ? 2 : i == 7 ? -1 : i == 3 ? 4 : 3",
    "L = i == 1 ? NULL_LONG : i == 2 ? 10000000000L : i == 4 ? -9223372036854775807L"
    " : i == 7 ? 6L : (long) (i + 1)",
    "D = i == 1 ? NULL_DOUBLE : i == 2 ? -1.5 : i == 4 ? 1e20 : i == 5 ? 3.0 : i == 6 ? -2.5"
    " : i == 7 ? 1e-7 : 0.1",
    "F = i == 1 ? NULL_FLOAT : i == 2 ? 2.5f : i == 4 ? 1e-5f : i == 6 ? -0.5f : i == 7 ? 3.0f"
    " : 0.1f",
    "C = i == 1 ? NULL_CHAR : i == 2 ? 'b' : i == 4 ? 'z' : 'a'",
    "B = i == 1 ? NULL_BYTE : i == 2 ? (byte) -2 : i == 4 ? (byte) 127 : (byte) 3",
    "S = i == 1 ? NULL_SHORT : i == 2 ? (short) 300 : i == 4 ? (short) -32767 : (short) 2",
    "Z = i == 1 ? null : i % 2 == 0",
    "Str = i == 1 ? null : `s` + i",
]
NAMES = [line.split(" = ", 1)[0] for line in FIXTURE]
NONNULL_FROM = 2


def texts(table, name, jpy):
    """The `toString` text of each cell of a column, None for null; a `char` is its number,
    as Python receives it."""
    floats = jpy.get_type("java.lang.Float")
    objects = jpy.get_type("java.util.Objects")
    j_table = table.j_table
    column = j_table.getDefinition().getColumn(name)
    source = j_table.getColumnSource(name)
    rows = j_table.getRowSet()
    values = []
    for position in range(j_table.size()):
        value = source.get(rows.get(position))
        if value is None:
            values.append(None)
        elif column.getDataType().getName() == "float":
            values.append(floats.toString(value))
        else:
            values.append(objects.toString(value))
    return column.getDataType().getName(), values


def cause(error):
    """The innermost exception of a failure, and whether the formula failed to compile."""
    text = str(error)
    compiled = ("FormulaCompilationException", "Error Compiling Formula Expression")
    when = "compile" if any(mark in text for mark in compiled) else "evaluate"
    lines = [line.strip() for line in text.splitlines() if "Exception" in line]
    return {"when": when, "cause": " | ".join(lines[:4])[:400]}


def main():
    server = Server(port=int(os.environ.get("RECORD_PORT", "10077")), jvm_args=["-Xmx1g"])
    server.start()
    import jpy
    from deephaven import empty_table

    base = empty_table(8).update(FIXTURE)
    tables = {"all": base, "nonnull": base.where(f"i >= {NONNULL_FROM}")}
    document = {
        "fixture": [
            dict(zip(["name", "java", "values"], [name, *texts(base, name, jpy)]))
            for name in NAMES
        ],
        "nonnull_from": NONNULL_FROM,
        "cases": [],
    }
    for line in (HERE / "java_big_numbers.txt").read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        rows, formula = "all", line
        if line.startswith("nonnull "):
            rows, formula = "nonnull", line[len("nonnull ") :]
        case = {"formula": formula, "rows": rows}
        try:
            result = tables[rows].update(f"R = {formula}")
            java, values = texts(result, "R", jpy)
            case["result"] = {"java": java, "values": values}
        except Exception as error:  # noqa: BLE001
            case["error"] = cause(error)
        document["cases"].append(case)
    with open(HERE / "java_big_numbers.json", "w", encoding="utf-8") as out:
        out.write("{\n")
        out.write(f'"fixture": {json.dumps(document["fixture"])},\n')
        out.write(f'"nonnull_from": {NONNULL_FROM},\n')
        out.write('"cases": [\n')
        out.write(",\n".join(json.dumps(case) for case in document["cases"]))
        out.write("\n]\n}\n")
    sys.stdout.flush()
    os._exit(0)


if __name__ == "__main__":
    main()
