"""The scenario document: inputs, operations, results, steps.

No Java counterpart: this is the harness's own neutral description of a query, independent of
the server that runs it. `oracle/README.md` describes the format; this module is its validator.

A scenario is plain JSON so that a generator can emit it without importing anything from here.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa

FORMAT = 1

# Column types a scenario may declare, and the Arrow type each one travels as.
COLUMN_TYPES: dict[str, pa.DataType] = {
    "bool": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
    # Nanoseconds since the epoch, UTC. Written in JSON as an integer.
    "timestamp": pa.timestamp("ns", tz="UTC"),
}

_FLOAT_WORDS = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Operation name -> (required keys, optional keys), besides "name" and "op".
OPERATIONS: dict[str, tuple[set[str], set[str]]] = {
    "where": ({"source", "filters"}, set()),
    "sort": ({"source", "by"}, set()),
    "head": ({"source", "rows"}, set()),
    "tail": ({"source", "rows"}, set()),
    "view": ({"source", "formulas"}, set()),
    "select": ({"source", "formulas"}, set()),
    "update": ({"source", "formulas"}, set()),
    "drop_columns": ({"source", "columns"}, set()),
    "merge": ({"sources"}, set()),
    "agg_by": ({"source", "aggs", "by"}, set()),
    "natural_join": ({"left", "right", "on"}, {"joins", "type"}),
    "exact_join": ({"left", "right", "on"}, {"joins"}),
    "join": ({"left", "right", "on"}, {"joins"}),
    "aj": ({"left", "right", "on"}, {"joins"}),
    "update_by": ({"source", "ops", "by"}, set()),
}
AGGREGATIONS = {"sum", "count", "min", "max", "first", "last", "avg"}
UPDATE_BY_OPS = {"cum_sum"}
NATURAL_JOIN_TYPES = {"error_on_duplicate", "first_match", "last_match", "exactly_one_match"}
ACTIONS = {"add", "delete"}


class ScenarioError(ValueError):
    """The scenario document is not valid."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str


@dataclass(frozen=True)
class Input:
    name: str
    columns: tuple[Column, ...]
    rows: tuple[tuple[Any, ...], ...]
    # None: a static table. Otherwise an input table; an empty tuple means append-only.
    key_columns: tuple[str, ...] | None

    @property
    def ticking(self) -> bool:
        return self.key_columns is not None


@dataclass(frozen=True)
class Result:
    table: str
    ordered: bool
    unordered_reason: str | None


@dataclass(frozen=True)
class Action:
    action: str
    table: str
    rows: tuple[tuple[Any, ...], ...]


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    inputs: tuple[Input, ...]
    operations: tuple[dict[str, Any], ...]
    results: tuple[Result, ...]
    steps: tuple[tuple[Action, ...], ...]
    document: dict[str, Any] = field(compare=False, repr=False)

    @property
    def ticking(self) -> bool:
        return any(i.ticking for i in self.inputs)

    def input(self, name: str) -> Input:
        for candidate in self.inputs:
            if candidate.name == name:
                return candidate
        raise KeyError(name)

    def result(self, table: str) -> Result:
        for candidate in self.results:
            if candidate.table == table:
                return candidate
        raise KeyError(table)

    def digest(self) -> str:
        """Identifies the scenario content, so a recording can tell it was made from it."""
        canonical = json.dumps(self.document, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()


def _fail(where: str, message: str) -> ScenarioError:
    return ScenarioError(f"{where}: {message}")


def _keys(where: str, doc: Any, required: set[str], optional: set[str]) -> None:
    if not isinstance(doc, dict):
        raise _fail(where, "expected an object")
    missing = required - doc.keys()
    unknown = doc.keys() - required - optional
    if missing:
        raise _fail(where, f"missing {sorted(missing)}")
    if unknown:
        raise _fail(where, f"unknown keys {sorted(unknown)}")


def _strings(where: str, value: Any, allow_empty: bool = True) -> None:
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise _fail(where, "expected a list of strings")
    if not allow_empty and not value:
        raise _fail(where, "must not be empty")


def decode_cell(where: str, type_name: str, value: Any) -> Any:
    """Turns one JSON cell into the Python value pyarrow expects; checks it fits the type."""
    if value is None:
        return None
    if type_name == "bool":
        if not isinstance(value, bool):
            raise _fail(where, f"expected true, false or null, got {value!r}")
        return value
    if type_name in ("float32", "float64"):
        if isinstance(value, str):
            if value not in _FLOAT_WORDS:
                raise _fail(where, f"expected a number or one of {sorted(_FLOAT_WORDS)}")
            return _FLOAT_WORDS[value]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise _fail(where, f"expected a number, got {value!r}")
        return float(value)
    if type_name == "string":
        if not isinstance(value, str):
            raise _fail(where, f"expected a string, got {value!r}")
        return value
    # Integers and timestamps.
    if isinstance(value, bool) or not isinstance(value, int):
        raise _fail(where, f"expected an integer, got {value!r}")
    bits = {"int8": 8, "int16": 16, "int32": 32, "int64": 64, "timestamp": 64}[type_name]
    if not -(1 << (bits - 1)) <= value < (1 << (bits - 1)):
        raise _fail(where, f"{value} does not fit {type_name}")
    return value


def encode_cell(value: Any) -> Any:
    """The inverse of `decode_cell` for floats: the JSON spelling of a Python value."""
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
    return value


def arrow_schema(columns: tuple[Column, ...]) -> pa.Schema:
    return pa.schema([pa.field(c.name, COLUMN_TYPES[c.type]) for c in columns])


def arrow_table(columns: tuple[Column, ...], rows: tuple[tuple[Any, ...], ...]) -> pa.Table:
    """Builds the Arrow table for `rows` laid out as `columns`."""
    arrays = [
        pa.array([row[i] for row in rows], type=COLUMN_TYPES[column.type])
        for i, column in enumerate(columns)
    ]
    return pa.Table.from_arrays(arrays, schema=arrow_schema(columns))


def _rows(where: str, columns: tuple[Column, ...], doc: Any) -> tuple[tuple[Any, ...], ...]:
    if not isinstance(doc, list):
        raise _fail(where, "expected a list of rows")
    rows = []
    for r, row in enumerate(doc):
        if not isinstance(row, list) or len(row) != len(columns):
            raise _fail(f"{where}[{r}]", f"expected {len(columns)} cells")
        rows.append(
            tuple(
                decode_cell(f"{where}[{r}].{column.name}", column.type, cell)
                for column, cell in zip(columns, row)
            )
        )
    return tuple(rows)


def _input(where: str, doc: Any) -> Input:
    _keys(where, doc, {"name", "columns", "rows"}, {"ticking"})
    if not isinstance(doc["columns"], list) or not doc["columns"]:
        raise _fail(where, "needs at least one column")
    columns = []
    for c, column in enumerate(doc["columns"]):
        _keys(f"{where}.columns[{c}]", column, {"name", "type"}, set())
        if column["type"] not in COLUMN_TYPES:
            raise _fail(f"{where}.columns[{c}]", f"unknown type {column['type']!r}")
        if not isinstance(column["name"], str) or not _NAME.match(column["name"]):
            raise _fail(f"{where}.columns[{c}]", f"bad column name {column['name']!r}")
        columns.append(Column(column["name"], column["type"]))
    names = [c.name for c in columns]
    if len(set(names)) != len(names):
        raise _fail(where, "duplicate column names")
    key_columns = None
    ticking = doc.get("ticking")
    if ticking is not None:
        _keys(f"{where}.ticking", ticking, {"key_columns"}, set())
        _strings(f"{where}.ticking.key_columns", ticking["key_columns"])
        unknown = set(ticking["key_columns"]) - set(names)
        if unknown:
            raise _fail(f"{where}.ticking", f"key columns {sorted(unknown)} are not columns")
        key_columns = tuple(ticking["key_columns"])
    return Input(doc["name"], tuple(columns), _rows(f"{where}.rows", tuple(columns), doc["rows"]),
                 key_columns)


def _operation(where: str, doc: Any, known: set[str]) -> None:
    if not isinstance(doc, dict) or "op" not in doc or "name" not in doc:
        raise _fail(where, "expected an object with 'name' and 'op'")
    op = doc["op"]
    if op not in OPERATIONS:
        raise _fail(where, f"unknown operation {op!r}")
    required, optional = OPERATIONS[op]
    _keys(where, doc, required | {"name", "op"}, optional)

    def source(key: str) -> None:
        if doc[key] not in known:
            raise _fail(where, f"{key} {doc[key]!r} is not defined before this operation")

    for key in ("source", "left", "right"):
        if key in doc:
            source(key)
    if op == "merge":
        _strings(f"{where}.sources", doc["sources"], allow_empty=False)
        for name in doc["sources"]:
            if name not in known:
                raise _fail(where, f"source {name!r} is not defined before this operation")
    for key in ("filters", "formulas", "columns"):
        if key in doc:
            _strings(f"{where}.{key}", doc[key], allow_empty=False)
    for key in ("by", "on"):
        if key in doc and op != "sort":
            _strings(f"{where}.{key}", doc[key])
    if doc.get("joins") is not None:
        _strings(f"{where}.joins", doc["joins"])
    if op in ("head", "tail"):
        if isinstance(doc["rows"], bool) or not isinstance(doc["rows"], int) or doc["rows"] < 0:
            raise _fail(where, "rows must be a non-negative integer")
    if op == "sort":
        if not isinstance(doc["by"], list) or not doc["by"]:
            raise _fail(where, "by must be a non-empty list")
        for b, by in enumerate(doc["by"]):
            _keys(f"{where}.by[{b}]", by, {"column", "direction"}, set())
            if by["direction"] not in ("ascending", "descending"):
                raise _fail(f"{where}.by[{b}]", "direction must be ascending or descending")
    if op == "agg_by":
        if not isinstance(doc["aggs"], list) or not doc["aggs"]:
            raise _fail(where, "aggs must be a non-empty list")
        for a, agg in enumerate(doc["aggs"]):
            if not isinstance(agg, dict) or agg.get("agg") not in AGGREGATIONS:
                raise _fail(f"{where}.aggs[{a}]", f"agg must be one of {sorted(AGGREGATIONS)}")
            if agg["agg"] == "count":
                _keys(f"{where}.aggs[{a}]", agg, {"agg", "column"}, set())
            else:
                _keys(f"{where}.aggs[{a}]", agg, {"agg", "columns"}, set())
                _strings(f"{where}.aggs[{a}].columns", agg["columns"], allow_empty=False)
    if op == "update_by":
        if not isinstance(doc["ops"], list) or not doc["ops"]:
            raise _fail(where, "ops must be a non-empty list")
        for o, ub in enumerate(doc["ops"]):
            _keys(f"{where}.ops[{o}]", ub, {"op", "columns"}, set())
            if ub["op"] not in UPDATE_BY_OPS:
                raise _fail(f"{where}.ops[{o}]", f"op must be one of {sorted(UPDATE_BY_OPS)}")
            _strings(f"{where}.ops[{o}].columns", ub["columns"], allow_empty=False)
    if op == "natural_join" and doc.get("type", "error_on_duplicate") not in NATURAL_JOIN_TYPES:
        raise _fail(where, f"type must be one of {sorted(NATURAL_JOIN_TYPES)}")


def parse(doc: Any) -> Scenario:
    """Validates a scenario document and returns it in typed form."""
    _keys("scenario", doc, {"format", "name", "inputs", "operations", "results"},
          {"description", "steps"})
    if doc["format"] != FORMAT:
        raise _fail("scenario", f"format {doc['format']!r} is not {FORMAT}")
    name = doc["name"]
    if not isinstance(name, str) or not _NAME.match(name):
        raise _fail("scenario", f"bad name {name!r}")
    where = f"scenario {name}"

    if not isinstance(doc["inputs"], list) or not doc["inputs"]:
        raise _fail(where, "needs at least one input")
    inputs = tuple(_input(f"{where}.inputs[{i}]", d) for i, d in enumerate(doc["inputs"]))
    known: set[str] = set()
    for item in inputs:
        if not isinstance(item.name, str) or not _NAME.match(item.name) or item.name in known:
            raise _fail(where, f"bad or duplicate table name {item.name!r}")
        known.add(item.name)

    if not isinstance(doc["operations"], list):
        raise _fail(where, "operations must be a list")
    for o, operation in enumerate(doc["operations"]):
        _operation(f"{where}.operations[{o}]", operation, known)
        op_name = operation["name"]
        if not isinstance(op_name, str) or not _NAME.match(op_name) or op_name in known:
            raise _fail(where, f"bad or duplicate table name {op_name!r}")
        known.add(op_name)

    if not isinstance(doc["results"], list) or not doc["results"]:
        raise _fail(where, "needs at least one result")
    results = []
    for r, result in enumerate(doc["results"]):
        _keys(f"{where}.results[{r}]", result, {"table"}, {"ordered", "unordered_reason"})
        if result["table"] not in known:
            raise _fail(f"{where}.results[{r}]", f"unknown table {result['table']!r}")
        ordered = result.get("ordered", True)
        if not isinstance(ordered, bool):
            raise _fail(f"{where}.results[{r}]", "ordered must be true or false")
        reason = result.get("unordered_reason")
        if ordered and reason is not None:
            raise _fail(f"{where}.results[{r}]", "unordered_reason given for an ordered result")
        if not ordered and not (isinstance(reason, str) and reason.strip()):
            raise _fail(
                f"{where}.results[{r}]",
                "an unordered result needs unordered_reason citing why the reference does not "
                "guarantee the order",
            )
        results.append(Result(result["table"], ordered, reason))
    if len({r.table for r in results}) != len(results):
        raise _fail(where, "a table is listed as a result twice")

    by_name = {i.name: i for i in inputs}
    steps = []
    step_docs = doc.get("steps", [])
    if not isinstance(step_docs, list):
        raise _fail(where, "steps must be a list")
    for s, step in enumerate(step_docs):
        step_where = f"{where}.steps[{s}]"
        _keys(step_where, step, {"actions"}, set())
        if not isinstance(step["actions"], list) or not step["actions"]:
            raise _fail(step_where, "needs at least one action")
        actions = []
        for a, action in enumerate(step["actions"]):
            action_where = f"{step_where}.actions[{a}]"
            _keys(action_where, action, {"action", "table", "rows"}, set())
            if action["action"] not in ACTIONS:
                raise _fail(action_where, f"action must be one of {sorted(ACTIONS)}")
            target = by_name.get(action["table"])
            if target is None or not target.ticking:
                raise _fail(action_where, f"{action['table']!r} is not a ticking input")
            columns = target.columns
            if action["action"] == "delete":
                if not target.key_columns:
                    raise _fail(action_where, "cannot delete from an append-only input table")
                # A delete names rows by key: one cell per key column, in key column order.
                columns = key_columns_of(target)
            actions.append(
                Action(action["action"], action["table"],
                       _rows(f"{action_where}.rows", columns, action["rows"]))
            )
        steps.append(tuple(actions))
    if steps and not any(i.ticking for i in inputs):
        raise _fail(where, "steps need a ticking input")

    return Scenario(
        name=name,
        description=doc.get("description", ""),
        inputs=inputs,
        operations=tuple(doc["operations"]),
        results=tuple(results),
        steps=tuple(steps),
        document=doc,
    )


def key_columns_of(item: Input) -> tuple[Column, ...]:
    by_name = {c.name: c for c in item.columns}
    return tuple(by_name[k] for k in item.key_columns or ())


def load(path: Path) -> Scenario:
    with open(path, encoding="utf-8") as handle:
        scenario = parse(json.load(handle))
    if scenario.name != path.stem:
        raise ScenarioError(f"{path}: scenario name {scenario.name!r} must equal the file name")
    return scenario


def load_all(directory: Path, names: list[str] | None = None) -> list[Scenario]:
    """Loads every scenario under `directory`, sorted by name; `names` narrows the set."""
    scenarios = sorted((load(p) for p in directory.rglob("*.json")), key=lambda s: s.name)
    seen: set[str] = set()
    for scenario in scenarios:
        if scenario.name in seen:
            raise ScenarioError(f"two scenarios are named {scenario.name!r}")
        seen.add(scenario.name)
    if names:
        missing = set(names) - seen
        if missing:
            raise ScenarioError(f"no scenario named {sorted(missing)}")
        scenarios = [s for s in scenarios if s.name in names]
    return scenarios
