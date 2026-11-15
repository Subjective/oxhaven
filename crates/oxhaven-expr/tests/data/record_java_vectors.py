"""Records what the Java reference server answers for formulas over vector and array columns.

No Java counterpart: this produces java_vectors.json, which the test
`tests/formulas/recorded.rs` compares Oxhaven's formula compiler against, in the format of
`record_java_formulas.py`.

Run it from the `oracle/` directory, with the oracle's Python environment and a Java
reference server that `oracle/bin/start-java.sh` started (see `oracle/README.md`):

    .venv/bin/python ../crates/oxhaven-expr/tests/data/record_java_vectors.py \
        dh+psk://oxhaven@127.0.0.1:10000 .run/java-10000.log

Each line of java_vectors.txt is `update <expression>` or `where <expression>`, as in
java_formulas.txt.

The input table has one array column per element type, named `A` and a letter (`AI` is an
`int[]`), uploaded as an Arrow list, and one vector column named `V` and the letter (`VI` is
an `io.deephaven.vector.IntVector`). Row 1 is an empty array in every array column and row
2 a null cell. A vector of a primitive type holds the same cells as the array; the Java
server makes it with `vec`. A vector of `String`, `Boolean` or `Instant` is made with
`group_by` and a join, because an `ObjectVector` that a formula makes has lost its element
type; it cannot be empty, so its row 1 holds one element. The recording holds the columns
as the server returned them after that, with the Java type of each, so the test builds its
table from what Java held and not from this file.
"""

import json
import os
import sys
from pathlib import Path

import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[3] / "oracle"))

from harness.connect import open_session  # noqa: E402
from record_java_formulas import decode, encode, failure  # noqa: E402

NAN, INF = "NaN", "Infinity"
NINF = "-Infinity"


def integers(top):
    """The cells of an array column of a signed integer type whose greatest value is `top`."""
    return [
        [3, 1, 2],
        [],
        None,
        [5],
        [None],
        [None, None],
        [4, None, -2, 4, None, 7],
        [top, top, 1],
        [top, top],
        [0, 6, 0, -6],
        [-top, 2, 3, -top],
        [2, 3, 4, 5],
        [1, 1, 1],
        [7, 7],
        [None, 1, None, -1],
        [10, 20, 30, 40, 50],
    ]


def floats(huge):
    return [
        [3.5, 1.25, 2.0],
        [],
        None,
        [5.5],
        [None],
        [None, None],
        [4.0, None, -2.5, 4.0, None, 7.0],
        [NAN, 1.0, NAN],
        [INF, 1.0],
        [0.0, -0.0, 6.0],
        [NINF, INF, 2.0],
        [-0.0, 0.0, INF],
        [huge, huge, 3.0],
        [0.1, 0.2, 0.3, 0.4],
        [None, NINF, None, 1.0],
        [0.1, 0.1, 0.1],
    ]


CHARS = [
    [99, 97, 98],
    [],
    None,
    [120],
    [None],
    [None, None],
    [100, None, 65, 100, None, 122],
    [0xFFFE, 0xFFFE, 1],
    [0xFFFE, 0xFFFE],
    [0, 54, 0, 48],
    [0x20AC, 2, 3, 0x20AC],
    [50, 51, 52, 53],
    [49, 49, 49],
    [55, 55],
    [None, 49, None, 0],
    [10, 20, 30, 40, 50],
]
STRINGS = [
    ["b", "a", "c"],
    [],
    None,
    ["x"],
    [None],
    [None, None],
    ["a", None, "b", "a", None, "c"],
    ["", "B", "b"],
] + [["k"]] * 8
BOOLEANS = [
    [True, False, True],
    [],
    None,
    [False],
    [None],
    [None, None],
    [True, None, False, True, None, True],
    [False, False],
] + [[True]] * 8
INSTANTS = [
    [1700000000000000003, 1700000000000000001, 1700000000000000002],
    [],
    None,
    [5],
    [None],
    [None, None],
    [4, None, -2, 4, None, 7],
    [0, 1],
] + [[1700000000000000000]] * 8

SCALARS = [
    ("R", pa.int32(), list(range(16))),
    # An index into the arrays and vectors of the same row.
    ("N", pa.int32(), [0, 0, 0, 0, 0, 1, 5, 2, None, -1, 3, 4, 2, 1, 1, 4]),
    ("NL", pa.int64(), [2, 0, 0, 0, 0, 1, 5, 2, None, -1, 3, 4, 2, 1, 1, 4]),
    ("NH", pa.int16(), [1] * 16),
    ("NF", pa.float32(), [1.0] * 16),
    ("I", pa.int32(), [1, None, -3, 0, 2147483647, -2147483647, 65, 300, 1, 2, 3, 4, 5, 6, 7, 8]),
]
# The letter of each element type, its Arrow type and the cells of its array column.
ELEMENTS = [
    ("Y", pa.int8(), integers(127)),
    ("H", pa.int16(), integers(32767)),
    ("C", pa.uint16(), CHARS),
    ("I", pa.int32(), integers(2147483647)),
    ("L", pa.int64(), integers(9223372036854775807)),
    ("F", pa.float32(), floats(3e38)),
    ("D", pa.float64(), floats(1e308)),
    ("S", pa.string(), STRINGS),
    ("B", pa.bool_(), BOOLEANS),
    ("T", pa.timestamp("ns", tz="UTC"), INSTANTS),
]
PRIMITIVE = "YHCILFD"
KINDS = {
    pa.int8(): "int8",
    pa.int16(): "int16",
    pa.uint16(): "uint16",
    pa.int32(): "int32",
    pa.int64(): "int64",
    pa.float32(): "float32",
    pa.float64(): "float64",
    pa.string(): "string",
    pa.bool_(): "bool",
    pa.timestamp("ns", tz="UTC"): "timestamp",
}


def cells(values):
    return [None if cell is None else [decode(v) for v in cell] for cell in values]


def input_table():
    names = [name for name, _, _ in SCALARS] + ["A" + letter for letter, _, _ in ELEMENTS]
    arrays = [pa.array(values, kind) for _, kind, values in SCALARS]
    arrays += [pa.array(cells(values), pa.list_(kind)) for _, kind, values in ELEMENTS]
    return pa.table(arrays, names=names)


def kind_of(arrow_type):
    """The recording's name for an Arrow type: a scalar's, or `list<scalar>`."""
    if pa.types.is_binary(arrow_type):
        return "list<int8>"
    if pa.types.is_list(arrow_type):
        return f"list<{KINDS[arrow_type.value_type]}>"
    return KINDS[arrow_type]


def values_of(data, arrow_type):
    if pa.types.is_binary(arrow_type):
        # A `byte[]`: the null byte is the lowest one.
        signed = lambda b: None if b == 128 else b - 256 if b > 127 else b  # noqa: E731
        return [None if cell is None else [signed(b) for b in cell] for cell in data.to_pylist()]
    if pa.types.is_list(arrow_type) and pa.types.is_timestamp(arrow_type.value_type):
        data = data.cast(pa.list_(pa.int64()))
    elif pa.types.is_timestamp(arrow_type):
        data = data.cast(pa.int64())
    return [encode(v) for v in data.to_pylist()]


def column(result: pa.Table, name: str):
    field = result.schema.field(name)
    java_type = (field.metadata or {}).get(b"deephaven:type", b"").decode()
    record = {"java": java_type, "arrow": str(field.type)}
    try:
        record["values"] = values_of(result.column(name).combine_chunks(), field.type)
        record["kind"] = kind_of(field.type)
        json.dumps(record["values"])
    except Exception:  # noqa: BLE001
        # A type that pyarrow or JSON cannot hold.
        record["values"] = None
    return record


def main():
    url, log_path = sys.argv[1:3]
    session = open_session(url)
    source = session.import_table(input_table())
    # `vec` refuses a null array, and a vector column made by `group_by` has no empty or
    # null cell, so the vectors of primitives are made from the arrays, which have both.
    source = source.update([f"V{letter} = isNull(A{letter}) ? null : vec(A{letter})" for letter in PRIMITIVE])
    objects = [(letter, kind, values) for letter, kind, values in ELEMENTS if letter not in PRIMITIVE]
    for letter, kind, values in objects:
        values = [values[0][:1] if row == 1 else cell for row, cell in enumerate(values)]
        flat = [(row, element) for row, cell in enumerate(values) for element in cell or []]
        elements = pa.table(
            [pa.array([row for row, _ in flat], pa.int32()), pa.array([e for _, e in flat], kind)],
            names=["G", f"V{letter}"],
        )
        grouped = session.import_table(elements).group_by(["G"])
        source = source.natural_join(grouped, on=["R = G"], joins=[f"V{letter}"])
    held = source.to_arrow()
    columns = []
    for field in held.schema:
        record = column(held, field.name)
        columns.append(
            {
                "name": field.name,
                "type": record["kind"],
                "java": record["java"],
                "values": record["values"],
            }
        )
    cases = []
    for line in (HERE / "java_vectors.txt").read_text(encoding="utf-8").splitlines():
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
    with open(HERE / "java_vectors.json", "w", encoding="utf-8") as out:
        out.write('{\n"columns": [\n')
        out.write(",\n".join(json.dumps(c) for c in columns))
        out.write('\n],\n"cases": [\n')
        out.write(",\n".join(json.dumps(c) for c in cases))
        out.write("\n]\n}\n")
    failed = sum(1 for case in cases if "error" in case)
    print(f"{len(cases)} formulas recorded, {failed} of them refused by Java")


if __name__ == "__main__":
    main()
