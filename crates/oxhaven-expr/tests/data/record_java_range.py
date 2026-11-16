"""Records what the Java reference server answers for formulas that make an object of a
class and call it: a `io.deephaven.plot.datasets.histogram.DiscretizedRangeEqual`
(java_range) and a `java.util.Random` (java_random).

No Java counterpart: this produces java_range.json and java_random.json, which the test
`tests/formulas/recorded.rs` compares Oxhaven's formula compiler against, in the format of
`record_java_formulas.py`.

Java refuses the class to a table operation of a client, so each formula is run by a script
of the console, as `deephaven.plot.express` runs its own. Run it from the `oracle/`
directory, with the oracle's Python environment and a Java reference server with a Python
console (see `oracle/README.md`):

    .venv/bin/python ../crates/oxhaven-expr/tests/data/record_java_range.py \
        dh+psk://oxhaven@127.0.0.1:10000 [java_random]

The second argument names the recording, java_range without it. Each line of its `.txt` is `update <expression>` or `where <expression>`, as in
java_formulas.txt; `D` in an expression stands for the name of the class.

Row 12 is null in every column of numbers. `Lo`, `Hi` and `N` are a range per row, with a
null in each, a range without width, one without bins and one whose bounds are exchanged.
"""

import json
import re
import sys
from pathlib import Path

import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[3] / "oracle"))

from harness.connect import open_session  # noqa: E402
from record_java_formulas import ARROW, column, decode  # noqa: E402

CLASS = "io.deephaven.plot.datasets.histogram.DiscretizedRangeEqual"
NAN, INF = "NaN", "Infinity"
COLUMNS = [
    ("R", "int32", list(range(16))),
    (
        "V",
        "float64",
        [0.0, 0.5, 1.0, 2.5, 9.99, 10.0, 10.5, -0.1, "-0.0", NAN, INF, "-Infinity", None, 3.3333333333333335, 5.0, 1e308],
    ),
    ("I", "int32", [0, 1, 2, 3, 9, 10, 11, -1, 0, 5, 6, 7, None, 3, 5, 4]),
    ("L", "int64", [0, 1, 2, 3, 9, 10, 11, -1, 0, 5, 6, 7, None, 3, 5, 2**62]),
    ("F", "float32", [0.0, 0.5, 1.0, 2.5, 9.99, 10.0, 10.5, -0.1, "-0.0", NAN, INF, "-Infinity", None, 3.3, 5.0, 1e30]),
    ("Y", "int8", [0, 1, 2, 3, 9, 10, 11, -1, 0, 5, 6, 7, None, 3, 5, 127]),
    ("H", "int16", [0, 1, 2, 3, 9, 10, 11, -1, 0, 5, 6, 7, None, 3, 5, 32767]),
    ("C", "uint16", [0, 1, 2, 3, 9, 10, 11, 97, 0, 5, 6, 7, None, 3, 5, 0xFFFE]),
    ("Lo", "float64", [0.0, 0.0, 0.0, 0.0, None, 0.0, 5.0, 10.0, 0.0, 0.0, NAN, 0.0, 0.0, -1e308, 0.1, "-Infinity"]),
    ("Hi", "float64", [10.0, 10.0, 10.0, 10.0, 10.0, None, 5.0, 0.0, 10.0, 10.0, 10.0, INF, 10.0, 1e308, 0.7, 10.0]),
    ("N", "int32", [5, 3, 0, -2, 5, 5, 4, 5, None, 5, 5, 5, 5, 4, 6, 1]),
]


def input_table():
    arrays = []
    for _, kind, values in COLUMNS:
        if kind in ("float32", "float64"):
            values = [float(v) if v == "-0.0" else decode(v) for v in values]
        arrays.append(pa.array(values, ARROW[kind]))
    return pa.table(arrays, names=[name for name, _, _ in COLUMNS])


def failure(error):
    """Whether the formula failed to compile or while it was evaluated, with the cause."""
    text = str(error)
    when = "evaluate" if "FormulaEvaluationException" in text else "compile"
    cause = ""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith(("caused by", "Caused by:", "Exception message")):
            cause = line
    return {"when": when, "cause": re.sub(r"\s+", " ", cause)[:240]}


def main():
    session = open_session(sys.argv[1])
    session.bind_table("range_source", session.import_table(input_table()))
    cases = []
    name = sys.argv[2] if len(sys.argv) > 2 else "java_range"
    for line in (HERE / f"{name}.txt").read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        op, formula = line.split(" ", 1)
        java = re.sub(r"\bD\b", CLASS, formula)
        case = {"op": op, "formula": java}
        if op == "update":
            script = f"range_result = range_source.update(['Result = ' + {java!r}]).view(['Result'])"
        else:
            script = f"range_result = range_source.where([{java!r}]).view(['R'])"
        try:
            session.run_script(script)
            result = session.open_table("range_result").to_arrow()
        except Exception as error:  # noqa: BLE001
            case["error"] = failure(error)
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
