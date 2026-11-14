"""Records what the Java reference server answers for `update_by`.

No Java counterpart: this produces java_update_by.json, which the test
`ops/update_by/corpus.rs` compares Oxhaven's `update_by` against. Nothing in that file is
written by hand: the inputs are generated here and the results come from the server.

Run it from the `oracle/` directory, with the oracle's Python environment and a Java
reference server that `oracle/bin/start-java.sh` started (see `oracle/README.md`):

    PYTHONPATH=. .venv/bin/python ../crates/oxhaven-engine/src/ops/update_by/fixtures/record_java_update_by.py \
        dh+psk://oxhaven@127.0.0.1:10830

Two kinds of record:

- `static`: one table with a column of every type; each case applies one list of
  operations to it and records the result columns the operations made, or that the server
  refused.
- `ticking`: keyed input tables changed step by step (rows added, replaced, moved to
  another bucket, deleted); the result columns are recorded after every step. Every step
  waits for several update cycles of the server, so this part takes some minutes.
"""

import json
import math
import random
import sys
from pathlib import Path

import pyarrow as pa
from pydeephaven import updateby as ub
from pydeephaven.updateby import DeltaControl

from harness.connect import open_session

HERE = Path(__file__).resolve().parent
MAX_LONG = 9223372036854775807
T0 = 1700000000000000000

ARROW = {
    "byte": pa.int8(),
    "short": pa.int16(),
    "char": pa.uint16(),
    "int": pa.int32(),
    "long": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "string": pa.string(),
    "boolean": pa.bool_(),
    "instant": pa.timestamp("ns", tz="UTC"),
}

# 16 rows. K and KD are bucket columns: K has a null bucket, KD has NaN, -0.0 and 0.0.
STATIC = [
    ("K", "string", ["a", "b", "a", None, "b", "a", "c", None, "a", "b", "a", "c", "b", "a", "a", "b"]),
    ("KD", "double", [1.0, "NaN", 0.0, "-0.0", None, "NaN", 1.0, 0.0, None, "-0.0", 1.0, "NaN", 0.0, 2.0, 2.0, 1.0]),
    ("Y", "byte", [1, None, -3, 0, 127, -127, 5, 100, None, None, 2, 7, -1, 0, 3, 127]),
    ("H", "short", [None, 1, -3, 0, 32767, -32767, 300, 7, None, 2, 2, 7, -1, 0, 3, 32767]),
    ("C", "char", [97, None, 65, 48, 0xFFFE, 0x20AC, 122, 32, None, 0, 1, 66, 65, 97, 98, 0xFFFE]),
    ("I", "int", [1, None, -3, 0, 2147483647, -2147483647, 65, 300, None, 2, 2, 7, -1, 0, 3, 2147483647]),
    ("L", "long", [10, None, -30, 0, MAX_LONG, MAX_LONG, 7, 2**40, None, 2, 2, -MAX_LONG, -MAX_LONG, -5, 3, 1]),
    ("L2", "long", [3, None, -2, 5, 1, 1, 7, 2**20, None, 2, 2, -4, 3, -5, 3, 1]),
    ("F", "float", [1.5, None, 2.5, 0.0, -2.25, 1e10, 3.0, None, None, 0.5, 4.0, -1.0, 2.0, 2.0, 8.0, 0.25]),
    ("D", "double", [1.5, None, 2.5, 0.0, -2.25, 1e10, 3.0, None, None, 0.5, 4.0, -1.0, 2.0, 2.0, 8.0, 0.25]),
    ("FS", "float", [1.5, None, "NaN", 0.0, "Infinity", "-Infinity", -2.25, 1e10, None, "-0.0", 4.0, "NaN", 2.0, "Infinity", 8.0, 0.25]),
    ("DS", "double", [1.5, None, "NaN", 0.0, "Infinity", "-Infinity", -2.25, 1e300, None, "-0.0", 4.0, "NaN", 2.0, "Infinity", 1e300, 1e300]),
    ("DI", "double", ["Infinity", 2.0, 1.0, None, "Infinity", 3.0, "Infinity", 5.0, 4.0, "Infinity", None, 0.5, 0.25, "Infinity", 7.0, 6.0]),
    ("S", "string", ["b", None, "", "a", "B", "aa", "hé", "\U0001f600", None, "z", "￮", "a", "c", "b", "Z", "y"]),
    ("B", "boolean", [True, None, False, True, None, False, True, False, None, None, True, True, False, True, True, False]),
    ("T", "instant", [T0, None, 0, 1, T0 + 5, -1, T0 + 2, T0 + 1, None, 7, 3, T0, T0 + 9, 2, 2, T0]),
]
VALUE_COLUMNS = [name for name, _, _ in STATIC if name not in ("K", "KD")]

CUMULATIVE = ["cum_sum", "cum_prod", "cum_min", "cum_max", "forward_fill", "delta"]
ROLLING = ["rolling_sum", "rolling_avg", "rolling_min", "rolling_max", "rolling_count", "rolling_prod"]
WINDOWS = [(1, 0), (3, 0), (2, 2), (0, 2), (-1, 3), (5, -2), (4, 1), (1000, 1000)]


def decode(value):
    if isinstance(value, str) and value in ("NaN", "Infinity", "-Infinity", "-0.0"):
        return float(value.replace("Infinity", "inf"))
    return value


def encode(value):
    """A cell as JSON: the float values JSON lacks are spelled as strings."""
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        if value == 0 and math.copysign(1, value) < 0:
            return "-0.0"
    return value


def arrow_column(kind, values):
    if kind in ("float", "double"):
        values = [decode(v) for v in values]
    return pa.array(values, ARROW[kind])


def make_op(op):
    """The pydeephaven operation for one recorded operation."""
    kind, cols = op["op"], op["columns"]
    if kind == "delta":
        control = {
            None: None,
            "null": DeltaControl.NULL_DOMINATES,
            "value": DeltaControl.VALUE_DOMINATES,
            "zero": DeltaControl.ZERO_DOMINATES,
        }[op.get("control")]
        return ub.delta(cols, control) if control is not None else ub.delta(cols)
    if kind in CUMULATIVE:
        return getattr(ub, kind)(cols)
    return getattr(ub, kind + "_tick")(cols, rev_ticks=op["rev"], fwd_ticks=op["fwd"])


def result_columns(result, names):
    out = []
    for name in names:
        field = result.schema.field(name)
        java_type = (field.metadata or {}).get(b"deephaven:type", b"").decode()
        data = result.column(name).combine_chunks()
        if pa.types.is_timestamp(field.type):
            data = data.cast(pa.int64())
        out.append({"name": name, "java": java_type, "values": [encode(v) for v in data.to_pylist()]})
    return out


def run(source, ops, by, source_names):
    """The names of all result columns and the columns the operations made, or None."""
    try:
        result = source.update_by([make_op(op) for op in ops], by).to_arrow()
    except Exception:  # noqa: BLE001
        return None
    made = [n for n in result.schema.names if n not in source_names or n in outputs_of(ops)]
    return {"names": result.schema.names, "columns": result_columns(result, made)}


def outputs_of(ops):
    return {c.split("=")[0].strip() for op in ops for c in op["columns"]}


def static_cases():
    cases = []

    def case(ops, by=()):
        cases.append({"ops": ops, "by": list(by)})

    for column in VALUE_COLUMNS:
        for by in ((), ("K",)):
            for kind in CUMULATIVE:
                case([{"op": kind, "columns": [f"R = {column}"]}], by)
            for kind in ROLLING:
                case([{"op": kind, "columns": [f"R = {column}"], "rev": 3, "fwd": 1}], by)
    for column in ("Y", "C", "I", "L", "L2", "F", "D", "FS", "DS", "DI", "S", "T"):
        for kind in ROLLING:
            for rev, fwd in WINDOWS:
                if (rev, fwd) != (3, 1) and (kind, column) not in (("rolling_sum", "S"), ("rolling_avg", "S"), ("rolling_prod", "S"), ("rolling_sum", "T"), ("rolling_avg", "T"), ("rolling_prod", "T")):
                    case([{"op": kind, "columns": [f"R = {column}"], "rev": rev, "fwd": fwd}])
        if column not in ("S", "T"):
            for control in ("null", "value", "zero"):
                case([{"op": "delta", "columns": [f"R = {column}"], "control": control}], ("K",))
    # Buckets on a double column, on two columns, and on a column that is also an input.
    for by in (("KD",), ("K", "KD"), ("B",), ("I",), ("T",)):
        case([{"op": "cum_sum", "columns": ["R = L2"]}, {"op": "rolling_count", "columns": ["N = D"], "rev": 2, "fwd": 0}], by)
    # No columns: every column the operation applies to.
    for kind in CUMULATIVE:
        case([{"op": kind, "columns": []}], ("K",))
        case([{"op": kind, "columns": []}])
    for kind in ROLLING:
        case([{"op": kind, "columns": [], "rev": 2, "fwd": 0}], ("K", "KD"))
    # Several operations, results that replace their input, several windows.
    case([
        {"op": "cum_sum", "columns": ["I", "SumL = L2"]},
        {"op": "rolling_sum", "columns": ["RS = I"], "rev": 2, "fwd": 0},
        {"op": "cum_max", "columns": ["D"]},
        {"op": "rolling_min", "columns": ["RM = I", "S"], "rev": 2, "fwd": 0},
        {"op": "rolling_avg", "columns": ["RA = L2"], "rev": 1, "fwd": 1},
        {"op": "forward_fill", "columns": ["B", "FT = T"]},
    ], ("K",))
    # Requests Java refuses.
    case([])
    case([{"op": "cum_sum", "columns": ["R = Nope"]}])
    case([{"op": "cum_sum", "columns": ["R = I"]}], ("Nope",))
    case([{"op": "cum_sum", "columns": ["R = I"]}, {"op": "cum_max", "columns": ["R = L"]}])
    case([{"op": "rolling_sum", "columns": ["R = I"], "rev": -3, "fwd": 2}])
    case([{"op": "rolling_sum", "columns": ["R = I"], "rev": 0, "fwd": 0}])
    case([{"op": "rolling_sum", "columns": ["R = I"], "rev": -2, "fwd": 2}])
    case([{"op": "rolling_min", "columns": ["R = D"], "rev": 0, "fwd": 0}])
    case([{"op": "cum_sum", "columns": ["K = I"]}], ("K",))
    return cases


def record_static(session):
    arrays = [arrow_column(kind, values) for _, kind, values in STATIC]
    names = [name for name, _, _ in STATIC]
    source = session.import_table(pa.table(arrays, names=names))
    cases = static_cases()
    for case in cases:
        case["result"] = run(source, case["ops"], case["by"], names)
    columns = [{"name": name, "type": kind, "values": values} for name, kind, values in STATIC]
    return {"columns": columns, "cases": cases}


class Barrier:
    """Waits until a cycle that began after the last change has completed.

    As `_Barrier` in oracle/harness/runner.py: two changes to a private input table, one
    after the other.
    """

    SCHEMA = pa.schema([("BarrierKey", pa.int32()), ("BarrierTick", pa.int64())])

    def __init__(self, session):
        self.session = session
        self.table = session.input_table(schema=self.SCHEMA, key_cols=["BarrierKey"])
        self.tick = 0

    def wait(self):
        for _ in range(2):
            self.tick += 1
            row = pa.Table.from_arrays(
                [pa.array([0], pa.int32()), pa.array([self.tick], pa.int64())], schema=self.SCHEMA
            )
            self.table.add(self.session.import_table(row))


TICKING_COLUMNS = [("Id", "int"), ("K", "string"), ("I", "int"), ("L", "long"), ("D", "double"), ("S", "string")]
TICKING_OPS = [
    {"op": "cum_sum", "columns": ["CsI = I", "CsD = D"]},
    {"op": "cum_prod", "columns": ["CpL = L"]},
    {"op": "cum_min", "columns": ["MinD = D", "MinS = S"]},
    {"op": "cum_max", "columns": ["MaxI = I"]},
    {"op": "forward_fill", "columns": ["FfI = I", "FfS = S"]},
    {"op": "delta", "columns": ["DlL = L", "DlD = D"]},
    {"op": "rolling_sum", "columns": ["RsI = I", "RsD = D"], "rev": 3, "fwd": 0},
    {"op": "rolling_avg", "columns": ["RaI = I", "RaD = D"], "rev": 3, "fwd": 0},
    {"op": "rolling_min", "columns": ["RnI = I", "RnD = D", "RnS = S"], "rev": 2, "fwd": 2},
    {"op": "rolling_max", "columns": ["RxI = I", "RxD = D"], "rev": 2, "fwd": 2},
    {"op": "rolling_count", "columns": ["RcS = S"], "rev": 2, "fwd": 2},
    {"op": "rolling_prod", "columns": ["RpL = L"], "rev": -1, "fwd": 3},
]


def random_row(rng, key):
    def maybe(value):
        return None if rng.random() < 0.15 else value

    return [
        key,
        rng.choice(["a", "b", "c", None]),
        maybe(rng.randint(-20, 20)),
        maybe(rng.randint(-4, 4)),
        maybe(rng.choice([rng.randint(-40, 40) / 4.0, 0.0, 1e6, -3.5])),
        maybe(rng.choice(["a", "b", "c", "d", "e"])),
    ]


def ticking_script(seed, steps):
    """Each step: rows to add (new keys and keys that exist, which replaces the row and may
    move it to another bucket) and keys to delete."""
    rng = random.Random(seed)
    alive = []
    next_key = 0
    out = []
    for _ in range(steps):
        add, delete = [], []
        for _ in range(rng.randint(0, 4)):
            add.append(random_row(rng, next_key))
            alive.append(next_key)
            next_key += 1
        for key in rng.sample(alive, min(len(alive), rng.randint(0, 3))):
            if all(row[0] != key for row in add):
                add.append(random_row(rng, key))
        for key in rng.sample(alive, min(len(alive), rng.randint(0, 2))):
            if all(row[0] != key for row in add):
                delete.append([key])
                alive.remove(key)
        out.append({"add": add, "delete": delete})
    return out


def record_ticking(session, name, seed, steps, by):
    schema = pa.schema([(n, ARROW[k]) for n, k in TICKING_COLUMNS])
    names = [n for n, _ in TICKING_COLUMNS]
    table = session.input_table(schema=schema, key_cols=["Id"])
    barrier = Barrier(session)
    result = table.update_by([make_op(op) for op in TICKING_OPS], by)
    made = sorted(outputs_of(TICKING_OPS))
    script = ticking_script(seed, steps)
    for number, step in enumerate(script):
        print(f"{name}: step {number} of {len(script)}", file=sys.stderr, flush=True)
        if step["add"]:
            rows = pa.table(
                [pa.array([r[i] for r in step["add"]], schema.field(i).type) for i in range(len(names))],
                schema=schema,
            )
            table.add(session.import_table(rows))
        if step["delete"]:
            keys = pa.table([pa.array([r[0] for r in step["delete"]], pa.int32())], names=["Id"])
            table.delete(session.import_table(keys))
        barrier.wait()
        snapshot = result.to_arrow()
        step["rows"] = result_columns(snapshot, names)
        step["result"] = result_columns(snapshot, made)
    return {
        "name": name,
        "columns": [{"name": n, "type": k} for n, k in TICKING_COLUMNS],
        "key_columns": ["Id"],
        "ops": TICKING_OPS,
        "by": by,
        "steps": script,
    }


def main():
    session = open_session(sys.argv[1])
    static = record_static(session)
    ticking = [
        record_ticking(session, "whole", 11, 25, []),
        record_ticking(session, "bucketed", 12, 25, ["K"]),
        record_ticking(session, "bucketed_long", 13, 60, ["K"]),
    ]
    with open(HERE / "java_update_by.json", "w", encoding="utf-8") as out:
        out.write('{\n"static": {\n"columns": [\n')
        out.write(",\n".join(json.dumps(c) for c in static["columns"]))
        out.write('\n],\n"cases": [\n')
        out.write(",\n".join(json.dumps(c) for c in static["cases"]))
        out.write('\n]\n},\n"ticking": [\n')
        out.write(",\n".join(json.dumps(t) for t in ticking))
        out.write("\n]\n}\n")
    refused = sum(1 for case in static["cases"] if case["result"] is None)
    print(f"{len(static['cases'])} static cases recorded, {refused} of them refused by Java")
    print(f"{len(ticking)} ticking scenarios recorded")


if __name__ == "__main__":
    main()
