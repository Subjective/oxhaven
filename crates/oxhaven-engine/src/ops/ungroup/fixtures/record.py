"""Records what the Java reference answers for the fixtures of `ops/ungroup`.

The oracle's scenarios make every array column with `group_by`, which the engine does not
have yet, and its corpus records tables, not updates. Each fixture here is one JSON file that
holds a scenario and, under `java`, what the reference server answered: the tables at every
step and the updates they delivered. `fixtures.rs` builds the source tables from the
recording, with their vector and array columns, drives them with the recorded updates,
ungroups them and compares.

    oracle/bin/start-java.sh 10890
    oracle/.venv/bin/python crates/oxhaven-engine/src/ops/ungroup/fixtures/record.py \
        --url dh+psk://oxhaven@127.0.0.1:10890 crates/oxhaven-engine/src/ops/ungroup/fixtures/*.json
    oracle/bin/stop-java.sh 10890

Run it against the Java reference only (`oracle/REFERENCE.md`). It rewrites the `java` entry
of each file and nothing else.

A fixture:

    {"name": ..., "description": ...,
     "inputs": [{"name", "columns": [{"name", "type"}], "rows": [[cell, ...]],
                 "ticking": {"key_columns": [...]}}],        # no "ticking": a static table
     "operations": [{"name", "op", ...}],
     "sources": ["table", ...],
     "results": ["table", ...],
     "steps": [{"actions": [{"action": "add" | "delete", "table", "rows"}]}],
     "java": {"steps": [{"table": {"columns", "rows"} | {"failure": "CODE"}}],
              "updates": [{"table": [{"added", "removed", "modified", "columns"}]}]}}

A type is `bool`, `int16`, `int32`, `int64`, `float32`, `float64`, `string`, `timestamp`
(nanoseconds since the epoch, UTC), or `array<T>` or `vector<T>` of one of those: a Java
array (`int[]`) or a Deephaven vector (`io.deephaven.vector.IntVector`), both Arrow lists.
An input column can be an array, not a vector: an uploaded list column is a Java array. A
cell of such a column is a JSON list, or `null` for a null cell, which is not an empty one.
A float that JSON cannot hold is written "NaN", "Infinity" or "-Infinity".

The operations are those of the oracle's scenario format (`oracle/README.md`), applied on
the Java server; `ungroup` is `{"op": "ungroup", "source", "columns": [...], "null_fill"}`.
`sources` names the tables that the `ungroup` operations of `results` read. The Rust test
does not compute them: it reads them from the recording, so they may come from any operation.

Step 0 of `java.steps` is the state after the operations were applied, step k the state
after the k-th entry of `steps`; it holds the sources and the results. Every step is one
update cycle. Entry k of `java.updates` holds, for each source and each result that is a
table, the updates it delivered during the k-th entry of `steps`: the added, removed and
modified rows as positions (removed rows by their position in the table before the update,
the others by their position after it) and the names of the modified columns. Row keys and
shifts are not recorded, since Java's row keys are not compared (`PARITY.md` A4).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import uuid
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

SCALARS = {
    "bool": pa.bool_(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
    "timestamp": pa.timestamp("ns", tz="UTC"),
}
WORDS = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}


def arrow_type(name):
    for kind in ("array<", "vector<"):
        if name.startswith(kind):
            return pa.list_(arrow_type(name[len(kind) : -1]))
    return SCALARS[name]


def decode(cell):
    if isinstance(cell, list):
        return [decode(element) for element in cell]
    return WORDS.get(cell, cell) if isinstance(cell, str) else cell


def decoded(type_name, cell):
    return decode(cell) if "float" in type_name else cell


def encode(cell):
    if isinstance(cell, list):
        return [encode(element) for element in cell]
    if isinstance(cell, float):
        if math.isnan(cell):
            return "NaN"
        if math.isinf(cell):
            return "Infinity" if cell > 0 else "-Infinity"
    return cell


def arrow_table(columns, rows):
    arrays = [
        pa.array([decoded(c["type"], row[i]) for row in rows], type=arrow_type(c["type"]))
        for i, c in enumerate(columns)
    ]
    schema = pa.schema([(c["name"], arrow_type(c["type"])) for c in columns])
    return pa.Table.from_arrays(arrays, schema=schema)


def scalar_name(found):
    for name, known in SCALARS.items():
        if known == found:
            return name
    raise SystemExit(f"a result column of type {found}, which the fixtures cannot hold")


def type_name(field):
    if not pa.types.is_list(field.type):
        return scalar_name(field.type)
    java = (field.metadata or {}).get(b"deephaven:type", b"").decode()
    element = scalar_name(field.type.value_type)
    if java.startswith("io.deephaven.vector."):
        return f"vector<{element}>"
    if java.endswith("[]"):
        return f"array<{element}>"
    raise SystemExit(f"a list column of the Java type {java!r}")


def recorded(table: pa.Table):
    columns = [{"name": field.name, "type": type_name(field)} for field in table.schema]
    cells = []
    for column in table.columns:
        if pa.types.is_timestamp(column.type):
            column = column.cast(pa.int64())
        elif pa.types.is_list(column.type) and pa.types.is_timestamp(column.type.value_type):
            column = column.cast(pa.list_(pa.int64()))
        cells.append([encode(cell) for cell in column.to_pylist()])
    return {"columns": columns, "rows": [list(row) for row in zip(*cells)]}


def define(session, tables, op):
    if isinstance(tables.get(op.get("source")), Failure):
        tables[op["name"]] = tables[op["source"]]
        return
    try:
        tables[op["name"]] = apply_operation(session, tables, op)
    except Exception as error:  # noqa: BLE001 - classified just below
        code = failure_code(error)
        if code is None:
            raise
        tables[op["name"]] = Failure(code)


def snapshot(session, names, tables):
    out = {}
    for name in names:
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


LISTEN = """
import json as _json
import jpy as _jpy
from deephaven.table_listener import listen as _listen

_{log}_sets = _jpy.get_type("io.deephaven.engine.table.ModifiedColumnSet")
_{log}_identity = _jpy.get_type("java.lang.System").identityHashCode

def _{log}_positions(rows, within):
    found = within.invert(rows)
    return [found.get(i) for i in range(found.size())]

def _{log}_listen(name, table):
    names = [c.name for c in table.columns]
    def on_update(update, is_replay):
        j = update.j_table_update
        rows = table.j_table.getRowSet()
        columns = []
        if j.modified().isNonempty():
            dirty = j.modifiedColumnSet()
            # ALL is one object that stands for every column and names none.
            if _{log}_identity(dirty) == _{log}_identity(_{log}_sets.ALL):
                columns = names
            else:
                columns = [n for n in names if n in set(dirty.dirtyColumnNames())]
        {log}.append(_json.dumps([name, dict(
            added=_{log}_positions(j.added(), rows),
            removed=_{log}_positions(j.removed(), rows.prev()),
            modified=_{log}_positions(j.modified(), rows),
            columns=columns,
        )]))
    return _listen(table, on_update)

{log} = []
{log}_handles = [_{log}_listen(name, table) for name, table in {tables}]
"""
DRAIN = """
from deephaven import new_table as _new_table
from deephaven.column import string_col as _string_col
{log}_table = _new_table([_string_col("Update", list({log}))])
del {log}[:]
"""
STOP = """
for _handle in {log}_handles:
    _handle.stop()
del {log}_handles, {log}
"""


class Updates:
    """Listens on the server to the refreshing tables among `names`."""

    def __init__(self, session, names, tables):
        self.session = session
        self.names = [
            n for n in names if not isinstance(tables[n], Failure) and tables[n].is_refreshing
        ]
        self.log = f"oxhaven_updates_{uuid.uuid4().hex}"
        if not self.names:
            return
        bound = []
        for index, name in enumerate(self.names):
            variable = f"{self.log}_{index}"
            session.bind_table(variable, tables[name])
            bound.append(f"({name!r}, {variable})")
        session.run_script(LISTEN.format(log=self.log, tables="[" + ", ".join(bound) + "]"))

    def drain(self):
        """The updates since the last call, by table. Call it after the barrier."""
        out = {name: [] for name in self.names}
        if not self.names:
            return out
        self.session.run_script(DRAIN.format(log=self.log))
        lines = self.session.open_table(f"{self.log}_table").to_arrow().column("Update")
        for line in lines.to_pylist():
            name, update = json.loads(line)
            out[name].append(update)
        return out

    def stop(self):
        if self.names:
            self.session.run_script(STOP.format(log=self.log))


def record(fixture, url):
    session = open_session(url)
    try:
        tables, inputs = {}, {}
        for item in fixture["inputs"]:
            if "ticking" not in item:
                tables[item["name"]] = _import(session, arrow_table(item["columns"], item["rows"]))
                continue
            schema = pa.schema([(c["name"], arrow_type(c["type"])) for c in item["columns"]])
            table = session.input_table(schema=schema, key_cols=item["ticking"]["key_columns"])
            inputs[item["name"]] = table
            tables[item["name"]] = table
        barrier = _Barrier(session) if inputs else None
        for item in fixture["inputs"]:
            if "ticking" in item and item["rows"]:
                data = arrow_table(item["columns"], item["rows"])
                inputs[item["name"]].add(_import(session, data))
        if barrier:
            barrier.wait()
        for op in fixture["operations"]:
            define(session, tables, op)
        names = fixture["sources"] + fixture["results"]
        updates = Updates(session, names if inputs else [], tables)
        if barrier:
            barrier.wait()
        if any(updates.drain().values()):
            raise SystemExit("an update before the first step")
        steps = [snapshot(session, names, tables)]
        delivered = []
        for number, step in enumerate(fixture.get("steps", ()), 1):
            for action in step["actions"]:
                item = next(i for i in fixture["inputs"] if i["name"] == action["table"])
                if action["action"] == "add":
                    data = arrow_table(item["columns"], action["rows"])
                    inputs[action["table"]].add(_import(session, data))
                else:
                    keys = item["ticking"]["key_columns"]
                    keys = [c for c in item["columns"] if c["name"] in keys]
                    data = arrow_table(keys, action["rows"])
                    inputs[action["table"]].delete(_import(session, data))
            barrier.wait()
            steps.append(snapshot(session, names, tables))
            delivered.append(updates.drain())
            for name, found in delivered[-1].items():
                if len(found) > 1:
                    raise SystemExit(f"step {number}: {name} delivered {len(found)} updates")
        updates.stop()
        return {"steps": steps, "updates": delivered}
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
    lines.append(f' "sources": {one(fixture["sources"])},')
    lines.append(f' "results": {one(fixture["results"])},')
    lines.append(' "steps": [')
    steps = fixture.get("steps", [])
    lines.extend(f"  {one(step)}{comma(i, steps)}" for i, step in enumerate(steps))
    lines.append(" ],")
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
    lines.append(" ],")
    lines.append(' "updates": [')
    delivered = fixture["java"]["updates"]
    lines.extend(f"  {one(step)}{comma(i, delivered)}" for i, step in enumerate(delivered))
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
        rows = sum(len(t.get("rows", ())) for s in fixture["java"]["steps"] for t in s.values())
        print(f"recorded {path.name}: {len(fixture['java']['steps'])} steps, {rows} rows")


if __name__ == "__main__":
    main()
