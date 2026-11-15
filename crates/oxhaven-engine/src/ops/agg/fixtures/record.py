"""Records what the Java reference answers for the fixtures of `ops/agg`.

The oracle harness (`oracle/`) knows only the aggregations `sum`, `avg`, `min`, `max`,
`first`, `last` and `count`, so the others have no scenarios in `oracle/corpus`. Until it
does, each fixture here is one JSON file that holds a scenario and, under `java`, the results
recorded from the reference server. The Rust test `ops::agg::tests::fixtures` replays the
scenario and compares.

    oracle/bin/start-java.sh 10862
    oracle/.venv/bin/python crates/oxhaven-engine/src/ops/agg/fixtures/record.py \
        --url dh+psk://oxhaven@127.0.0.1:10862 \
        crates/oxhaven-engine/src/ops/agg/fixtures/*.json
    oracle/bin/stop-java.sh 10862

Run it against the Java reference only (`oracle/REFERENCE.md`). It rewrites the `java` entry
of each file and nothing else; a re-recording changes a file only where the results changed.

A fixture:

    {"name": ..., "description": ...,
     "inputs": [{"name", "columns": [{"name", "type"}], "rows": [[cell, ...]],
                 "ticking": {"key_columns": [...]}}],        # no "ticking": a static table
     "operations": [{"name", "op", ...}],
     "results": ["table", ...],
     "steps": [{"actions": [{"action": "add" | "delete", "table", "rows"}]}],
     "updates": ["table", ...],
     "java": {"steps": [{"table": {"columns", "attributes", "rows"} | {"failure": "CODE"}}],
              "updates": [{"table": [{"added", "removed", "modified", "modified_columns"}]}]}}

Inputs, cells and steps are those of the oracle's scenario format (`oracle/README.md`), with
more column types: `char` (a UTF-16 code unit as an integer), and `vector<T>` and `array<T>`
for a Deephaven vector and a Java array of `T`, which may itself be one of the two. A cell of
those is a JSON list of cells of `T`, or null for a null cell, which is not an empty one. A
result column is a vector where Java's `deephaven:type` is a class of `io.deephaven.vector`
and an array otherwise; an element that is a list is told apart by `deephaven:componentType`
in the same way. An input column can only be an array: that is what Java makes of an Arrow
list.

For the tables named under `updates`, `java.updates` has, for each step after the first, the
updates the table delivered during it: the added, removed and modified rows by position
(removed rows by their position before the update), and the names of the columns in the
update's modified column set.

Step 0 of `java` is the state
after the operations were applied, step k the state after the k-th entry of `steps`. Every
action is applied in an update cycle of its own, in the order written. This is the format of
`ops/snapshot/fixtures/record.py` (task 4.9), of which this script is a copy with other
operations:

    {"op": "agg", "source", "by": [...], "aggs": [agg, ...]}
    {"op": "group_by", "source", "by": [...]}

and the operations of the oracle's scenarios (`natural_join`, `update_view`, ...), where each
`agg` is one of

    {"agg": "abs_sum" | "var" | "std" | "sum" | "avg" | "min" | "max" | "first" | "last"
            | "group", "cols": [...]}
    {"agg": "median", "cols", "average": bool}
    {"agg": "pct", "cols", "percentile": p, "average": bool}
    {"agg": "count_distinct", "cols", "count_nulls": bool}
    {"agg": "unique", "cols", "include_nulls": bool, "sentinel": null | {type: value}}
    {"agg": "distinct", "cols", "include_nulls": bool}
    {"agg": "count_where", "column": name, "filters": [filter, ...]}
    {"agg": "weighted_avg" | "weighted_sum", "cols", "weight": column}
    {"agg": "sorted_first" | "sorted_last", "cols", "order_by": column}
    {"agg": "count", "column": name}
    {"agg": "formula", "cols", "formula": formula, "param": token}
    {"agg": "formula", "formula": "Out = formula"}

with `cols` written `"Out=In"` or `"Col"`, as `pydeephaven.agg` takes them. The `type` of a
formula with `cols` is an `AggSpecFormula`, sent with the token as it is written, also an
empty one; a formula without is an `Aggregation.Formula`. The `type` of a
non-unique sentinel is the field of the request that carries it: `byte`, `short`, `int`,
`long`, `float`, `double`, `char`, `string` or `bool`; a floating point value may be one of
the words `"NaN"`, `"Infinity"` and `"-Infinity"`.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import uuid
from pathlib import Path

import pyarrow as pa
from pydeephaven import agg
from pydeephaven.agg import _AggregationColumns
from deephaven_core.proto import table_pb2

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


def element_type(type_name):
    """The element type of `vector<T>` or `array<T>`, or None for any other type."""
    for kind in ("vector<", "array<"):
        if type_name.startswith(kind) and type_name.endswith(">"):
            return type_name[len(kind):-1]
    return None


def arrow_type(type_name):
    element = element_type(type_name)
    if element is not None:
        return pa.list_(arrow_type(element))
    return TYPES[type_name]


def decode(type_name, cell):
    element = element_type(type_name)
    if element is not None:
        return None if cell is None else [decode(element, item) for item in cell]
    if isinstance(cell, str) and type_name in ("float32", "float64"):
        return WORDS[cell]
    return cell


def encode(cell):
    if isinstance(cell, list):
        return [encode(item) for item in cell]
    if isinstance(cell, float):
        if math.isnan(cell):
            return "NaN"
        if math.isinf(cell):
            return "Infinity" if cell > 0 else "-Infinity"
    return cell


def arrow_table(columns, rows):
    arrays = [
        pa.array([decode(c["type"], row[i]) for row in rows], type=arrow_type(c["type"]))
        for i, c in enumerate(columns)
    ]
    schema = pa.schema([(c["name"], arrow_type(c["type"])) for c in columns])
    return pa.Table.from_arrays(arrays, schema=schema)


def type_name(arrow_type, java_type=None, component_type=None):
    """The fixture's name of a column type. `java_type` and `component_type` are the column's
    `deephaven:type` and `deephaven:componentType`, which tell a vector from an array."""
    if pa.types.is_list(arrow_type):
        if java_type is None:
            raise SystemExit(f"a list within a list within a list: {arrow_type}")
        kind = "vector" if java_type.startswith("io.deephaven.vector.") else "array"
        return f"{kind}<{type_name(arrow_type.value_type, component_type)}>"
    for name, known in TYPES.items():
        if known == arrow_type:
            return name
    raise SystemExit(f"a result column of type {arrow_type}, which the fixtures cannot hold")


def nanoseconds(arrow_type, cell):
    """`cell` with every timestamp in it as nanoseconds since the epoch."""
    if cell is None:
        return None
    if pa.types.is_list(arrow_type):
        return [nanoseconds(arrow_type.value_type, item) for item in cell]
    if pa.types.is_timestamp(arrow_type):
        return cell.value
    return cell


def scalar_cell(scalar):
    """An Arrow scalar as a cell: lists as lists, timestamps as `pandas.Timestamp`."""
    if not scalar.is_valid:
        return None
    if pa.types.is_list(scalar.type):
        return [scalar_cell(item) for item in scalar.values]
    if pa.types.is_timestamp(scalar.type):
        return scalar
    return scalar.as_py()


def recorded(table: pa.Table):
    columns = []
    for field in table.schema:
        described = field.metadata or {}
        java_type = described.get(b"deephaven:type", b"").decode()
        component_type = described.get(b"deephaven:componentType", b"").decode()
        name = type_name(field.type, java_type, component_type)
        columns.append({"name": field.name, "type": name})
    cells = []
    for column in table.columns:
        if pa.types.is_timestamp(column.type):
            column = column.cast(pa.int64())
        elif pa.types.is_list(column.type):
            # `to_pylist` gives a timestamp within a list as a datetime, which has no
            # nanoseconds.
            values = [nanoseconds(column.type, scalar_cell(scalar)) for scalar in column]
            cells.append([encode(cell) for cell in values])
            continue
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
    if kind == "agg":
        return source.agg_by([aggregation(a) for a in op["aggs"]], op["by"])
    if kind == "group_by":
        return source.group_by(op["by"])
    return apply_operation(session, tables, op)


def aggregation(entry):
    """The `pydeephaven.agg` aggregation of one entry of an `agg` operation."""
    kind, cols = entry["agg"], entry.get("cols")
    if kind == "count":
        return agg.count_(entry["column"])
    if kind == "median":
        return agg.median(cols, average_evenly_divided=entry["average"])
    if kind == "pct":
        return agg.pct(entry["percentile"], cols, average_evenly_divided=entry["average"])
    if kind == "count_distinct":
        return agg.count_distinct(cols, count_nulls=entry["count_nulls"])
    if kind == "unique":
        return unique(cols, entry["include_nulls"], entry.get("sentinel"))
    if kind == "distinct":
        return agg.distinct(cols, include_nulls=entry["include_nulls"])
    if kind == "count_where":
        return agg.count_where(entry["column"], entry["filters"])
    if kind == "formula" and cols is None:
        return agg.formula(entry["formula"])
    if kind == "formula":
        # `agg.formula` takes an empty token for the other form.
        spec = table_pb2.AggSpec.AggSpecFormula(formula=entry["formula"], param_token=entry["param"])
        return _AggregationColumns(agg_spec=table_pb2.AggSpec(formula=spec), cols=cols)
    if kind in ("weighted_avg", "weighted_sum"):
        return getattr(agg, kind)(entry["weight"], cols)
    if kind in ("sorted_first", "sorted_last"):
        return getattr(agg, kind)(entry["order_by"], cols)
    return getattr(agg, {"sum": "sum_", "min": "min_", "max": "max_"}.get(kind, kind))(cols)


def unique(cols, include_nulls, sentinel):
    """`agg.unique` with the sentinel in the field of the request that the fixture names.
    `pydeephaven.agg.unique` picks the field from the Python type of the value, and sends
    a `bool` as a long."""
    if sentinel is None:
        return agg.unique(cols, include_nulls=include_nulls)
    ((kind, value),) = sentinel.items()
    if isinstance(value, str) and kind in ("float", "double"):
        value = WORDS[value]
    spec = table_pb2.AggSpec.AggSpecNonUniqueSentinel(**{f"{kind}_value": value})
    return _AggregationColumns(
        agg_spec=table_pb2.AggSpec(
            unique=table_pb2.AggSpec.AggSpecUnique(
                include_nulls=include_nulls, non_unique_sentinel=spec
            )
        ),
        cols=cols,
    )


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


LISTEN = """
import json as _json
from deephaven.table_listener import listen as _listen

def _{log}_positions(rows, within):
    found = within.invert(rows)
    return [found.get(i) for i in range(found.size())]

def _{log}_listen(name, table):
    def on_update(update, is_replay):
        j = update.j_table_update
        rows = table.j_table.getRowSet()
        {log}.append(_json.dumps([name, dict(
            added=_{log}_positions(j.added(), rows),
            removed=_{log}_positions(j.removed(), rows.prev()),
            modified=_{log}_positions(j.modified(), rows),
            modified_columns=[str(c) for c in j.modifiedColumnSet().dirtyColumnNames()]
                if j.modified().isNonempty() else [],
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
    """Listens on the server to the tables named by a fixture's `updates`."""

    def __init__(self, session, fixture, tables):
        self.session = session
        self.names = fixture.get("updates", [])
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
        for op in fixture["operations"]:
            define(session, tables, op)
        updates = Updates(session, fixture, tables)
        if barrier:
            barrier.wait()
        if any(updates.drain().values()):
            raise SystemExit("an update before the first step")
        steps = [snapshot(session, fixture, tables)]
        delivered = []
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
            delivered.append(updates.drain())
        updates.stop()
        if "updates" in fixture:
            return {"steps": steps, "updates": delivered}
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
    if "updates" in fixture:
        lines.append(f' "updates": {one(fixture["updates"])},')
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
    if "updates" in fixture["java"]:
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
        print(f"recorded {path.name}: {len(fixture['java']['steps'])} steps")


if __name__ == "__main__":
    main()
