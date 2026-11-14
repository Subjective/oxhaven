"""Records what the Java reference answers for the fixtures of `ops/snapshot` and `ops/distinct`.

The oracle harness (`oracle/`) has no `snapshot`, `snapshot_when`, `select_distinct` or
`where_in` operation yet, so these operations have no scenarios in `oracle/corpus`. Until it
does, each fixture here is one JSON file that holds a scenario and, under `java`, the results
recorded from the reference server. The Rust corpus tests of the two modules replay the
scenario and compare.

    oracle/bin/start-java.sh 10820
    oracle/.venv/bin/python crates/oxhaven-engine/src/ops/snapshot/fixtures/record.py \
        --url dh+psk://oxhaven@127.0.0.1:10820 \
        crates/oxhaven-engine/src/ops/snapshot/fixtures/*.json \
        crates/oxhaven-engine/src/ops/distinct/fixtures/*.json
    oracle/bin/stop-java.sh 10820

Run it against the Java reference only (`oracle/REFERENCE.md`). It rewrites the `java` entry
of each file and nothing else; a re-recording changes a file only where the results changed.

A fixture:

    {"name": ..., "description": ...,
     "inputs": [{"name", "columns": [{"name", "type"}], "rows": [[cell, ...]],
                 "ticking": {"key_columns": [...]}}],        # no "ticking": a static table
     "operations": [{"name", "op", ...}],
     "results": ["table", ...],
     "steps": [{"actions": [{"action": "add" | "delete", "table", "rows"}]}],
     "fails": {"table": step},                               # optional, see below
     "java": {"steps": [{"table": {"columns", "attributes", "rows"} | {"failure": "CODE"}}]}}

Inputs, cells and steps are those of the oracle's scenario format (`oracle/README.md`), with
one more column type, `char` (a UTF-16 code unit as an integer). Step 0 of `java` is the state
after the operations were applied, step k the state after the k-th entry of `steps`. Every
action is applied in an update cycle of its own, in the order written.

`fails` is written by hand, not recorded. The Java server keeps answering snapshots of a
table that failed during an update cycle, with the rows it had (`PARITY.md`, Q11), so a
recording cannot show the failure. `fails` names the step at which the Java source says a
result fails; the recording supports it only in that the table stops changing there. The
Rust tests require the failure at that step and do not compare the table from then on.

The operations:

    {"op": "snapshot", "source"}
    {"op": "snapshot_when", "source", "trigger", "stamp_columns": [...],
     "initial": bool, "incremental": bool, "history": bool}
    {"op": "select_distinct", "source", "columns": [...]}
    {"op": "where_in" | "where_not_in", "source", "set", "columns": [...]}
    {"op": "sort", "source", "by": [{"column", "direction"}]}
    {"op": "head" | "tail", "source", "rows": n}

and every other operation of the oracle's scenario format (`where`, `merge`, ...). The Rust
replay runs none of those: a fixture that uses one is compared by a test of its own.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import pyarrow as pa

ORACLE = Path(__file__).resolve().parents[6] / "oracle"
sys.path.insert(0, str(ORACLE))

from harness.connect import open_session  # noqa: E402
from harness.results import Failure  # noqa: E402
from harness.runner import (  # noqa: E402
    _Barrier,
    _do_get_code,
    _import,
    apply_operation,
    failure_code,
)

TYPES = {
    "bool": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
    "timestamp": pa.timestamp("ns", tz="UTC"),
    "char": pa.uint16(),
}
WORDS = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}
ATTRIBUTES = ("IsFlat", "AddOnly", "AppendOnly", "BlinkTable")


def decode(type_name, cell):
    if isinstance(cell, str) and type_name in ("float32", "float64"):
        return WORDS[cell]
    return cell


def encode(cell):
    if isinstance(cell, float):
        if math.isnan(cell):
            return "NaN"
        if math.isinf(cell):
            return "Infinity" if cell > 0 else "-Infinity"
    return cell


def arrow_table(columns, rows):
    arrays = [
        pa.array([decode(c["type"], row[i]) for row in rows], type=TYPES[c["type"]])
        for i, c in enumerate(columns)
    ]
    schema = pa.schema([(c["name"], TYPES[c["type"]]) for c in columns])
    return pa.Table.from_arrays(arrays, schema=schema)


def type_name(arrow_type):
    for name, known in TYPES.items():
        if known == arrow_type:
            return name
    raise SystemExit(f"a result column of type {arrow_type}, which the fixtures cannot hold")


def recorded(table: pa.Table):
    columns = [{"name": field.name, "type": type_name(field.type)} for field in table.schema]
    cells = []
    for column in table.columns:
        if pa.types.is_timestamp(column.type):
            column = column.cast(pa.int64())
        cells.append([encode(cell) for cell in column.to_pylist()])
    metadata = table.schema.metadata or {}
    attributes = {
        name: metadata.get(f"deephaven:attribute.{name}".encode()) == b"true"
        for name in ATTRIBUTES
    }
    return {
        "columns": columns,
        "attributes": attributes,
        # A table without columns still has a number of rows: one empty row for each.
        "rows": [list(row) for row in zip(*cells)] if cells else [[]] * table.num_rows,
    }


def apply(session, tables, op):
    kind = op["op"]
    if "source" not in op:
        return apply_operation(session, tables, op)
    source = tables[op["source"]]
    if kind == "snapshot":
        return source.snapshot()
    if kind == "snapshot_when":
        return source.snapshot_when(
            tables[op["trigger"]],
            stamp_cols=op["stamp_columns"],
            initial=op["initial"],
            incremental=op["incremental"],
            history=op["history"],
        )
    if kind == "select_distinct":
        return source.select_distinct(op["columns"])
    if kind == "where_in":
        return source.where_in(tables[op["set"]], op["columns"])
    if kind == "where_not_in":
        return source.where_not_in(tables[op["set"]], op["columns"])
    return apply_operation(session, tables, op)


def define(session, tables, op):
    for key in ("source", "trigger", "set"):
        if key in op and isinstance(tables[op[key]], Failure):
            tables[op["name"]] = tables[op[key]]
            return
    try:
        tables[op["name"]] = apply(session, tables, op)
    except Exception as error:  # noqa: BLE001 - classified just below
        code = failure_code(error)
        if code is None:
            raise
        tables[op["name"]] = Failure(code)


def snapshot(session, fixture, tables):
    out = {}
    for name in fixture["results"]:
        table = tables[name]
        if isinstance(table, Failure):
            out[name] = {"failure": table.code}
            continue
        try:
            out[name] = recorded(table.to_arrow())
        except Exception:  # noqa: BLE001 - classified just below
            code = _do_get_code(session, table)
            if code is None:
                raise
            out[name] = {"failure": code}
    return out


def record(fixture, url):
    session = open_session(url)
    try:
        tables, inputs = {}, {}
        for item in fixture["inputs"]:
            if "ticking" not in item:
                tables[item["name"]] = _import(session, arrow_table(item["columns"], item["rows"]))
                continue
            schema = pa.schema([(c["name"], TYPES[c["type"]]) for c in item["columns"]])
            table = session.input_table(schema=schema, key_cols=item["ticking"]["key_columns"])
            inputs[item["name"]] = table
            tables[item["name"]] = table
        barrier = _Barrier(session) if inputs else None
        for item in fixture["inputs"]:
            if "ticking" in item and item["rows"]:
                data = arrow_table(item["columns"], item["rows"])
                inputs[item["name"]].add(_import(session, data))
        for op in fixture["operations"]:
            define(session, tables, op)
        if barrier:
            barrier.wait()
        steps = [snapshot(session, fixture, tables)]
        for step in fixture.get("steps", ()):
            for action in step["actions"]:
                item = next(i for i in fixture["inputs"] if i["name"] == action["table"])
                if action["action"] == "add":
                    data = arrow_table(item["columns"], action["rows"])
                    inputs[action["table"]].add(_import(session, data))
                else:
                    names = item["ticking"]["key_columns"]
                    keys = [c for c in item["columns"] if c["name"] in names]
                    inputs[action["table"]].delete(_import(session, arrow_table(keys, action["rows"])))
            barrier.wait()
            steps.append(snapshot(session, fixture, tables))
        return {"steps": steps}
    finally:
        session.close()


def dump(fixture):
    """One line per input row, operation, step and recorded row, so that diffs stay small."""

    def one(value):
        return json.dumps(value, ensure_ascii=True)

    def comma(index, items):
        return "," if index + 1 < len(items) else ""

    lines = [
        "{",
        f' "name": {one(fixture["name"])},',
        f' "description": {one(fixture["description"])},',
        ' "inputs": [',
    ]
    inputs = fixture["inputs"]
    for i, item in enumerate(inputs):
        head = {k: v for k, v in item.items() if k != "rows"}
        lines.append(f'  {one(head)[:-1]}, "rows": [')
        rows = item["rows"]
        lines.extend(f"   {one(row)}{comma(r, rows)}" for r, row in enumerate(rows))
        lines.append(f"  ]}}{comma(i, inputs)}")
    lines.append(" ],")
    lines.append(' "operations": [')
    ops = fixture["operations"]
    lines.extend(f"  {one(op)}{comma(i, ops)}" for i, op in enumerate(ops))
    lines.append(" ],")
    lines.append(f' "results": {one(fixture["results"])},')
    lines.append(' "steps": [')
    steps = fixture.get("steps", [])
    lines.extend(f"  {one(step)}{comma(i, steps)}" for i, step in enumerate(steps))
    lines.append(" ],")
    if "fails" in fixture:
        lines.append(f' "fails": {one(fixture["fails"])},')
    lines.append(' "java": {"steps": [')
    recorded_steps = fixture["java"]["steps"]
    for s, step in enumerate(recorded_steps):
        lines.append("  {")
        names = list(step)
        for n, name in enumerate(names):
            result = step[name]
            if "failure" in result:
                lines.append(f"   {one(name)}: {one(result)}{comma(n, names)}")
                continue
            head = {k: v for k, v in result.items() if k != "rows"}
            lines.append(f'   {one(name)}: {one(head)[:-1]}, "rows": [')
            rows = result["rows"]
            lines.extend(f"    {one(row)}{comma(r, rows)}" for r, row in enumerate(rows))
            lines.append(f"   ]}}{comma(n, names)}")
        lines.append(f"  }}{comma(s, recorded_steps)}")
    lines.append(" ]}")
    lines.append("}")
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", required=True, help="the Java reference, dh+psk://<key>@host:port")
    parser.add_argument("fixtures", nargs="+", type=Path)
    arguments = parser.parse_args()
    for path in arguments.fixtures:
        fixture = json.loads(path.read_text(encoding="utf-8"))
        if fixture["name"] != path.stem:
            raise SystemExit(f"{path}: the name must equal the file name")
        fixture.setdefault("steps", [])
        fixture["java"] = record(fixture, arguments.url)
        text = dump(fixture)
        if json.loads(text) != json.loads(json.dumps(fixture)):
            raise SystemExit(f"{path}: the written form would lose part of the fixture")
        path.write_text(text, encoding="utf-8")
        print(f"recorded {path.name}: {len(fixture['java']['steps'])} steps")


if __name__ == "__main__":
    main()
