"""Records what the Java reference server answers for the `update_by` operators that the
other two recorders do not cover: the rolling group, and the rolling and cumulative
count-where.

No Java counterpart: this produces java_update_by_group.json, which the test
`ops/update_by/corpus.rs` compares Oxhaven's `update_by` against. It has the sections of
java_update_by.json, over the same tables, and is recorded the same way:

    PYTHONPATH=. .venv/bin/python ../crates/oxhaven-engine/src/ops/update_by/fixtures/record_java_update_by_group.py \
        dh+psk://oxhaven@127.0.0.1:10910

Three things are new:

- a vector cell is a list (or null), and its column has a `component`, the element type
  Java reports;
- the section `stamped`: the cases of `timed` over a table with a timestamp in every row;
- the section `burst`: a table in which up to 200 rows share a timestamp, so that more
  rows enter a window in time at once than the ring buffers of the rolling operators hold
  when they are made (64 or 128), after the window has moved along single rows. It records
  the operators that add in the order of their buffer as well;
- a ticking scenario ends with a step that has `failed` when the result table failed in
  that step: a rolling group in time fails when it computes a row without a timestamp that
  stands before a row with one. `stale` counts the cells, over all steps, in which the
  result differs from what Java answers for the same operations over a static copy of the
  source at that step (none, when recorded).
"""

import gc
import json
import random
import sys
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parent))

import record_java_update_by as base  # noqa: E402
import record_java_update_by_rest as rest  # noqa: E402
from pydeephaven import updateby as ub  # noqa: E402

from harness.connect import open_session  # noqa: E402

HERE = Path(__file__).resolve().parent
T0 = base.T0

rest_make_op = base.make_op


def make_op(op):
    """The pydeephaven operation for one recorded operation."""
    kind = op["op"]
    if kind == "cum_count_where":
        return ub.cum_count_where(op["result"], op["filters"])
    if kind == "rolling_count_where":
        if "ts" in op:
            return ub.rolling_count_where_time(
                op["ts"], op["result"], op["filters"], rev_time=op["rev"], fwd_time=op["fwd"]
            )
        return ub.rolling_count_where_tick(op["result"], op["filters"], rev_ticks=op["rev"], fwd_ticks=op["fwd"])
    return rest_make_op(op)


def outputs_of(ops):
    out = set()
    for op in ops:
        if "result" in op:
            out.add(op["result"])
        out.update(c.split("=")[0].strip() for c in op["columns"])
    return out


def encode(value):
    if isinstance(value, list):
        return [base.encode(v) for v in value]
    return base.encode(value)


def result_columns(result, names):
    out = []
    for name in names:
        field = result.schema.field(name)
        metadata = field.metadata or {}
        data = result.column(name).combine_chunks()
        if pa.types.is_timestamp(field.type):
            data = data.cast(pa.int64())
        if pa.types.is_list(field.type) and pa.types.is_timestamp(field.type.value_type):
            data = data.cast(pa.list_(pa.int64()))
        column = {
            "name": name,
            "java": metadata.get(b"deephaven:type", b"").decode(),
            "values": [encode(v) for v in data.to_pylist()],
        }
        if b"deephaven:componentType" in metadata:
            column["component"] = metadata[b"deephaven:componentType"].decode()
        out.append(column)
    return out


base.make_op = make_op
base.outputs_of = outputs_of
base.result_columns = result_columns


def group(columns, rev, fwd, ts=None):
    op = {"op": "rolling_group", "columns": columns, "rev": rev, "fwd": fwd}
    if ts is not None:
        op["ts"] = ts
    return op


def static_cases():
    cases = []

    def case(ops, by=()):
        cases.append({"ops": ops, "by": list(by)})

    for column in base.VALUE_COLUMNS:
        for by in ((), ("K",)):
            case([group([f"R = {column}"], 3, 1)], by)
    for column in ("I", "D", "S", "B", "T", "C"):
        for rev, fwd in base.WINDOWS + [(0, 0), (2, -2), (0, 1), (1, -1)]:
            if (rev, fwd) != (3, 1):
                case([group([f"R = {column}"], rev, fwd)])
                case([group([f"R = {column}"], rev, fwd)], ("K",))
    # No columns: every column but the bucket columns.
    case([group([], 2, 0)], ("K", "KD"))
    case([group([], 2, 1)])
    # A bucket column as the input; buckets on a double column and on the input.
    case([group(["R = K"], 2, 0)], ("K",))
    case([group(["R = I", "Q = KD"], 2, 0)], ("KD",))
    case([group(["R = I"], 1, 1)], ("I",))
    # With other operations, sharing a window or not; two groups in one window and in
    # two; a result that replaces its input.
    case([
        group(["GI = I", "D"], 3, 0),
        {"op": "rolling_sum", "columns": ["RS = I"], "rev": 3, "fwd": 0},
        group(["GS = S"], 3, 0),
        group(["GB = B", "GI2 = I"], 2, 2),
        {"op": "cum_sum", "columns": ["CS = I"]},
        {"op": "rolling_count", "columns": ["RC = S"], "rev": 2, "fwd": 2},
    ], ("K",))
    case([group(["I"], 2, 0), {"op": "rolling_sum", "columns": ["RS = I"], "rev": 2, "fwd": 0}])
    # A window that holds no row fails the other rolling operators, not the group.
    case([group(["R = I"], 0, 0), {"op": "rolling_sum", "columns": ["RS = I"], "rev": 0, "fwd": 0}])
    case([group(["R = I"], 0, 0), {"op": "rolling_sum", "columns": ["RS = I"], "rev": 1, "fwd": 0}])
    # Requests Java refuses.
    case([group(["R = Nope"], 3, 0)])
    case([group(["R = I"], -3, 2)])
    case([group(["R = I"], 2, -3)])
    case([group(["R = I"], 3, 0), group(["R = D"], 3, 0)])
    case([group(["R = I"], 3, 0), group(["R = D"], 2, 0)])
    case([group(["R = I"], 3, 0)], ("Nope",))
    case([group(["K = I"], 3, 0)], ("K",))
    return cases


def long_cases():
    cases = []
    for column in ("DR", "IR"):
        for rev, fwd in rest.LONG_WINDOWS:
            for by in ((), ("K",)):
                cases.append({"ops": [group([f"R = {column}"], rev, fwd)], "by": list(by)})
    return cases


def timed_cases():
    cases = []

    def case(ops, by=()):
        cases.append({"ops": ops, "by": list(by)})

    for rev, fwd in base.TIME_WINDOWS + [(0, 3), (3, -3)]:
        for column in base.TIMED_VALUES:
            for by in ((), ("K",)):
                case([group([f"R = {column}"], rev, fwd, "TS")], by)
    # Other timestamp columns: a long, one that only rises within buckets, one that falls.
    for ts in ("TL", "TB", "TD", "TI", "Nope"):
        for by in ((), ("K",)):
            case([group(["R = I"], 5, 2, ts)], by)
    # No columns, which takes the timestamp column too; the timestamp column as an input.
    case([group([], 4, 0, "TL")], ("K",))
    case([group([], 4, 2, "TS")])
    case([group(["R = TL", "E = I"], 4, 0, "TL")])
    case([group(["R = TS"], 4, 3, "TS")], ("K",))
    case([group(["TS"], 4, 3, "TS")], ("K",))
    case([
        group(["GI = I", "GD = DS"], 5, 5, "TS"),
        {"op": "rolling_sum", "columns": ["RS = I"], "ts": "TS", "rev": 5, "fwd": 5},
        group(["GS = S"], 5, 5, "TS"),
        group(["GT = T"], 3, 0, "TS"),
        group(["GJ = I"], 3, 0),
        {"op": "ema", "columns": ["EA = D"], "ts": "TS", "decay": 5},
    ], ("K",))
    # Requests Java refuses.
    case([group(["R = I"], 5, 0, "TS"), group(["Q = I"], 5, 0, "TL")])
    case([group(["R = I"], -5, 2, "TS")])
    case([group(["R = I"], 5, -6, "TS")])
    return cases


def stamped_table():
    """The `timed` table with a timestamp in every row: a row that had none has the one of
    the row before it. Over the `timed` table itself Java refuses nearly every rolling
    group in time, because rows without a timestamp stand before rows with one."""
    out = []
    for name, kind, values in base.TIMED:
        if name in ("TS", "TL"):
            filled, last = [], T0 if name == "TS" else 0
            for value in values:
                last = last if value is None else value
                filled.append(last)
            values = filled
        out.append((name, kind, values))
    return out


STAMPED = stamped_table()


def burst_table():
    """370 rows: runs of rows a second apart, between them 70 and then 200 rows that share
    one timestamp, and a last run of 60 rows 10 nanoseconds apart. The values are those of
    the `long` table: their sums round."""
    rng = random.Random(17)
    stamps, now = [], T0
    for run, same in ((40, False), (70, True), (20, False), (200, True), (30, False)):
        now += 1000
        for _ in range(run):
            stamps.append(now)
            if not same:
                now += 1000
    for _ in range(10):
        now += 10
        stamps.append(now)
    n = len(stamps)

    def column(make):
        return [None if rng.random() < 0.1 else make() for _ in range(n)]

    return [
        ("K", "string", [rng.choice(["a", "b"]) for _ in range(n)]),
        ("TS", "instant", stamps),
        ("DR", "double", column(lambda: rng.choice([rng.uniform(-10, 10), rng.uniform(0.5, 2), 1e16, 0.1]))),
        ("FR", "float", column(lambda: rng.choice([rng.uniform(-10, 10), rng.uniform(0.5, 2), 1e8, 0.1]))),
        ("IR", "int", column(lambda: rng.choice([rng.randint(-99999, 99999), rng.randint(1, 9), 0]))),
        ("W", "double", column(lambda: rng.uniform(0.5, 2))),
    ]


BURST = burst_table()
BURST_WINDOWS = [(1500, 0), (0, 0), (2500, 1000), (100, 0), (5000, 5000)]


def burst_cases():
    cases = []
    for rev, fwd in BURST_WINDOWS:
        for by in ((), ("K",)):
            window = {"ts": "TS", "rev": rev, "fwd": fwd}
            for column in ("DR", "FR", "IR"):
                for kind in ("rolling_std", "rolling_sum", "rolling_avg", "rolling_prod"):
                    cases.append({"ops": [{"op": kind, "columns": [f"R = {column}"], **window}], "by": list(by)})
                op = {"op": "rolling_wavg", "columns": [f"R = {column}"], "weight": "W", **window}
                cases.append({"ops": [op], "by": list(by)})
            if rev <= 1500:
                cases.append({"ops": [group(["R = IR"], rev, fwd, "TS")], "by": list(by)})
    return cases


TICKING_OPS = [
    group(["GaI = I", "GaD = D", "GaS = S"], 3, 0),
    group(["GbL = L"], 2, 2),
    group(["GcD = D"], -1, 3),
    group(["GdI = I"], 0, 0),
    group(["GeK = K"], 1, 1),
    {"op": "rolling_sum", "columns": ["RsI = I"], "rev": 3, "fwd": 0},
]

TIMED_TICKING_OPS = [
    group(["GaI = I", "GaD = D"], 3000, 0, "Ts"),
    group(["GbD = D"], 2000, 2000, "Ts"),
    group(["GcI = I"], -1000, 4000, "Ts"),
    group(["GdT = Ts"], 0, 0, "Ts"),
    group(["GeI = I"], 2, 1),
    {"op": "rolling_sum", "columns": ["RsI = I"], "ts": "Ts", "rev": 3000, "fwd": 0},
]


def stamped_row(rng, key):
    """As `random_timed_row`, with a timestamp in every row."""
    row = base.random_timed_row(rng, key)
    row[2] = T0 + (key // 2) * 1000
    return row


def record_ticking(session, name, seed, steps, by, timed=False, make_row=None):
    """As the recorder of java_update_by.json, and for every step the same operations over
    a static copy of the source."""
    columns = base.TIMED_TICKING_COLUMNS if timed else base.TICKING_COLUMNS
    ops = TIMED_TICKING_OPS if timed else TICKING_OPS
    schema = pa.schema([(n, base.ARROW[k]) for n, k in columns])
    names = [n for n, _ in columns]
    table = session.input_table(schema=schema, key_cols=["Id"])
    barrier = base.Barrier(session)
    operations = [make_op(op) for op in ops]
    result = table.update_by(operations, by)
    made = sorted(outputs_of(ops))
    script = base.ticking_script(seed, steps, make_row or (base.random_timed_row if timed else None))
    stale = 0
    for number, step in enumerate(script):
        print(f"{name}: step {number} of {len(script)}", file=sys.stderr, flush=True)
        if step["add"]:
            rows = pa.table(
                [base.arrow_column(columns[i][1], [r[i] for r in step["add"]]) for i in range(len(names))],
                schema=schema,
            )
            table.add(session.import_table(rows))
        if step["delete"]:
            keys = pa.table([pa.array([r[0] for r in step["delete"]], pa.int32())], names=["Id"])
            table.delete(session.import_table(keys))
        barrier.wait()
        try:
            snapshot = result.to_arrow()
        except Exception:  # noqa: BLE001
            step["failed"] = True
            del script[number + 1:]
            break
        step["rows"] = result_columns(snapshot, names)
        step["result"] = result_columns(snapshot, made)
        if snapshot.num_rows == 0:
            # An upload without rows never completes.
            continue
        plain = pa.table([snapshot.column(n).cast(schema.field(n).type) for n in names], schema=schema)
        copy = session.import_table(plain)
        recomputed = result_columns(copy.update_by(operations, by).to_arrow(), made)
        for ticking, static in zip(step["result"], recomputed):
            stale += sum(1 for a, b in zip(ticking["values"], static["values"]) if a != b)
        del copy
        gc.collect()
    return {
        "name": name,
        "columns": [{"name": n, "type": k} for n, k in columns],
        "key_columns": ["Id"],
        "ops": ops,
        "by": by,
        "stale": stale,
        "steps": script,
    }


def main():
    gc.disable()
    session = open_session(sys.argv[1])
    sections = {
        "static": base.record_static(session, base.STATIC, static_cases()),
        "long": base.record_static(session, base.LONG, long_cases()),
        "timed": base.record_static(session, base.TIMED, timed_cases()),
        "stamped": base.record_static(session, STAMPED, timed_cases()),
        "burst": base.record_static(session, BURST, burst_cases()),
    }
    ticking = [
        record_ticking(session, "whole", 31, 30, []),
        record_ticking(session, "bucketed", 32, 40, ["K"]),
        record_ticking(session, "timed", 34, 30, [], timed=True),
        record_ticking(session, "timed_bucketed", 35, 40, ["K"], timed=True),
        record_ticking(session, "stamped", 36, 30, [], timed=True, make_row=stamped_row),
        record_ticking(session, "stamped_bucketed", 37, 40, ["K"], timed=True, make_row=stamped_row),
    ]
    with open(HERE / "java_update_by_group.json", "w", encoding="utf-8") as out:
        out.write("{\n")
        for name, section in sections.items():
            out.write(f'"{name}": {{\n"columns": [\n')
            out.write(",\n".join(json.dumps(c) for c in section["columns"]))
            out.write('\n],\n"cases": [\n')
            out.write(",\n".join(json.dumps(c) for c in section["cases"]))
            out.write("\n]\n},\n")
        out.write('"ticking": [\n')
        out.write(",\n".join(json.dumps(t) for t in ticking))
        out.write("\n]\n}\n")
    for name, section in sections.items():
        refused = sum(1 for case in section["cases"] if case["result"] is None)
        print(f"{name}: {len(section['cases'])} cases recorded, {refused} of them refused by Java")
    for scenario in ticking:
        failed = "failed" in scenario["steps"][-1]
        print(
            f"ticking {scenario['name']}: {len(scenario['steps'])} steps, failed at the end: {failed}, "
            f"{scenario['stale']} cells differ from the static recompute"
        )


if __name__ == "__main__":
    main()
