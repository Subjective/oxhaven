"""Records what the Java reference server answers for each formula of a list.

No Java counterpart: this produces java_formulas.json and java_functions.json, which the
test `tests/formulas/recorded.rs` compares Oxhaven's formula compiler against.

Run it from the `oracle/` directory, with the oracle's Python environment and a Java
reference server that `oracle/bin/start-java.sh` started (see `oracle/README.md`):

    .venv/bin/python ../crates/oxhaven-expr/tests/data/record_java_formulas.py \
        dh+psk://oxhaven@127.0.0.1:10000 .run/java-10000.log [java_functions]

The last argument names the list: `<name>.txt` is read and `<name>.json` written. Without it
the list is java_formulas.

Each line of the list is `update <expression>` or `where <expression>`; a line that
starts with `#` is a comment. An `update` line is recorded as the type and values of the
column `Result = <expression>`, a `where` line as the values of column `R` in the rows the
filter keeps. When the server refuses the request, the record says whether the formula
failed to compile or failed while it was evaluated, read from the server's log, with the
innermost cause.
"""

import json
import math
import os
import re
import sys
import time
from pathlib import Path

import pyarrow as pa

from harness.connect import open_session

HERE = Path(__file__).resolve().parent
MAX_LONG = 9223372036854775807

# One row per position; row 1 is null in every column that can be null.
COLUMNS = [
    ("R", "int32", [0, 1, 2, 3, 4, 5, 6, 7]),
    ("Y", "int8", [1, None, -3, 0, 127, -127, 5, 100]),
    ("H", "int16", [1, None, -3, 0, 32767, -32767, 300, 7]),
    ("C", "uint16", [97, None, 65, 48, 0xFFFE, 0x20AC, 122, 32]),
    ("I", "int32", [1, None, -3, 0, 2147483647, -2147483647, 65, 300]),
    ("L", "int64", [10, None, -30, 0, MAX_LONG, -MAX_LONG, 7, 2**40]),
    ("F", "float32", [1.5, None, "NaN", 0.0, "Infinity", "-Infinity", -2.25, 1e10]),
    ("D", "float64", [1.5, None, "NaN", 0.0, "Infinity", "-Infinity", -2.25, 1e-300]),
    ("S", "string", ["a", None, "", "b", "  Hello ", "B", "aa", "héllo\U0001f600"]),
    ("S2", "string", ["a", "x", None, "", "Hello", "b", "a", "llo"]),
    ("SN", "string", ["a", "x", "", "b", "  Hello ", "B", "aa", "héllo\U0001f600"]),
    ("SM", "string", ["a", "x", "", "", "Hello", "b", "a", "llo"]),
    ("B", "bool", [True, None, False, True, None, False, True, False]),
    ("B2", "bool", [True, True, None, False, False, None, True, False]),
    ("BN", "bool", [True, False, False, True, True, False, True, False]),
    (
        "T",
        "timestamp",
        [
            1700000000000000000,
            None,
            0,
            1,
            1700000000123456789,
            -1,
            1700000000500000000,
            1700000000000001000,
        ],
    ),
    ("T2", "timestamp", [1700000000000000000, None, None, 0, 5, -1, 1700000000500000001, 7]),
]
ARROW = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "uint16": pa.uint16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
    "bool": pa.bool_(),
    "timestamp": pa.timestamp("ns", tz="UTC"),
}


def decode(value):
    if isinstance(value, str) and value in ("NaN", "Infinity", "-Infinity"):
        return float(value.replace("Infinity", "inf"))
    return value


def encode(value):
    """A cell as JSON: the float values JSON lacks are spelled as strings."""
    if isinstance(value, list):
        return [encode(v) for v in value]
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        if value == 0 and math.copysign(1, value) < 0:
            return "-0.0"
    return value


def input_table():
    arrays = []
    for _, kind, values in COLUMNS:
        if kind in ("float32", "float64"):
            values = [decode(v) for v in values]
        arrays.append(pa.array(values, ARROW[kind]))
    return pa.table(arrays, names=[name for name, _, _ in COLUMNS])


def column(result: pa.Table, name: str):
    field = result.schema.field(name)
    java_type = (field.metadata or {}).get(b"deephaven:type", b"").decode()
    record = {"java": java_type, "arrow": str(field.type)}
    try:
        data = result.column(name).combine_chunks()
        if pa.types.is_timestamp(field.type):
            data = data.cast(pa.int64())
        record["values"] = [encode(v) for v in data.to_pylist()]
        json.dumps(record["values"])
    except Exception:  # noqa: BLE001
        # A string that is not valid UTF-8 (Java sent half of a surrogate pair), or a type
        # that pyarrow or JSON cannot hold.
        record["values"] = None
    return record


def failure(log_path, offset):
    """What the server logged for the request that just failed."""
    size = -1
    while size != os.path.getsize(log_path):
        size = os.path.getsize(log_path)
        time.sleep(0.2)
    with open(log_path, encoding="utf-8", errors="replace") as log:
        log.seek(offset)
        text = log.read()
    when = "evaluate" if "FormulaEvaluationException" in text else "compile"
    cause = ""
    for line in text.splitlines():
        if line.startswith(("Caused by:", "Exception message")):
            cause = line
    return {"when": when, "cause": re.sub(r"\s+", " ", cause)[:240]}


def main():
    url, log_path = sys.argv[1:3]
    name = sys.argv[3] if len(sys.argv) > 3 else "java_formulas"
    session = open_session(url)
    source = session.import_table(input_table())
    cases = []
    for line in (HERE / f"{name}.txt").read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        op, formula = line.split(" ", 1)
        case = {"op": op, "formula": formula}
        offset = os.path.getsize(log_path)
        try:
            if op == "update":
                result = source.update([f"Result = {formula}"]).view(["Result"]).to_arrow()
            else:
                result = source.where([formula]).view(["R"]).to_arrow()
        except Exception:  # noqa: BLE001
            case["error"] = failure(log_path, offset)
        else:
            if op == "update":
                case["result"] = column(result, "Result")
            else:
                case["kept"] = result.column("R").to_pylist()
        cases.append(case)
    columns = [{"name": name, "type": kind, "values": values} for name, kind, values in COLUMNS]
    with open(HERE / f"{name}.json", "w", encoding="utf-8") as out:
        out.write('{\n"columns": [\n')
        out.write(",\n".join(json.dumps(c) for c in columns))
        out.write('\n],\n"cases": [\n')
        out.write(",\n".join(json.dumps(c) for c in cases))
        out.write("\n]\n}\n")
    failed = sum(1 for case in cases if "error" in case)
    print(f"{len(cases)} formulas recorded, {failed} of them refused by Java")


if __name__ == "__main__":
    main()
