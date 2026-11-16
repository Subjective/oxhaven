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

FLOAT_WORDS = {"NaN": math.nan, "Infinity": math.inf, "-Infinity": -math.inf}
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
    "agg_by": ({"source", "aggs", "by"}, {"initial_groups", "preserve_empty"}),
    "natural_join": ({"left", "right", "on"}, {"joins", "type"}),
    "exact_join": ({"left", "right", "on"}, {"joins"}),
    "join": ({"left", "right", "on"}, {"joins"}),
    "aj": ({"left", "right", "on"}, {"joins"}),
    "update_by": ({"source", "ops", "by"}, set()),
    "slice": ({"source", "start", "stop"}, set()),
    "flatten": ({"source"}, set()),
    "reverse": ({"source"}, set()),
    "lazy_update": ({"source", "formulas"}, set()),
    "update_view": ({"source", "formulas"}, set()),
    "select_distinct": ({"source", "columns"}, set()),
    "where_in": ({"left", "right", "on"}, {"inverted"}),
    "filter": ({"source", "conditions"}, set()),
    "left_join": ({"left", "right", "on"}, {"joins"}),
    "raj": ({"left", "right", "on"}, {"joins"}),
    "agg_all_by": ({"source", "agg", "by"}, set()),
    "group_by": ({"source", "by"}, set()),
    "ungroup": ({"source"}, {"columns", "null_fill"}),
    "head_by": ({"source", "rows", "by"}, set()),
    "tail_by": ({"source", "rows", "by"}, set()),
    "snapshot": ({"source"}, set()),
    "snapshot_when": ({"source", "trigger"},
                      {"stamp_columns", "initial", "incremental", "history"}),
    "range_join": ({"left", "right", "on", "aggs"}, {"script"}),
    "multi_join": ({"inputs"}, {"script"}),
    "time_table": ({"period", "start_time"}, {"blink"}),
    "empty_table": ({"size"}, set()),
    "meta_table": ({"source"}, set()),
    "column_statistics": ({"source", "column"}, {"unique_value_limit"}),
    "partition_merge": ({"source", "by"}, {"drop_keys"}),
    "partition_get": ({"source", "by", "key"}, {"drop_keys", "unique"}),
    "ring_table": ({"source", "capacity"}, {"initialize"}),
    "keyed_transpose": ({"source", "aggs", "row_by", "col_by"},
                        {"initial_groups", "new_columns"}),
}
# Operations that have no request of the table service and are made by a script in the
# server's Python console (`runner._scripted`); `multi_join` and `range_join` are when they
# say `"script": true`.
SCRIPT_OPERATIONS = frozenset({"ring_table", "keyed_transpose"})
# How `partition_get` asks for its constituents: `GetTableRequest.UniqueBehavior`.
UNIQUE_BEHAVIORS = {"not_set", "require_unique", "permit_multiple"}
# What `keyed_transpose` does with a column that a later row would add:
# `deephaven.table.NewColumnBehaviorType`.
NEW_COLUMN_BEHAVIORS = {"fail", "ignore"}
# Keys of an operation that name the tables it reads.
TABLE_KEYS = ("source", "left", "right", "trigger", "initial_groups", "key")

# Aggregations of `agg_by`, `agg_all_by` and `range_join`. The value is the set of argument
# names (`args`) the aggregation accepts besides its columns.
AGGREGATIONS: dict[str, set[str]] = {
    "sum": set(), "abs_sum": set(), "avg": set(), "min": set(), "max": set(), "first": set(),
    "last": set(), "std": set(), "var": set(), "group": set(),
    "count": set(),
    "count_where": {"filters"},
    "count_distinct": {"count_nulls"},
    "distinct": {"include_nulls"},
    "unique": {"include_nulls", "non_unique_sentinel"},
    "median": {"average_evenly_divided"},
    "pct": {"percentile", "average_evenly_divided"},
    "sorted_first": {"order_by"},
    "sorted_last": {"order_by"},
    "weighted_avg": {"wcol"},
    "weighted_sum": {"wcol"},
    "formula": {"formula", "formula_param"},
}
# Operators of `update_by`, by the name of the pydeephaven.updateby function. The arguments
# (`args`) are that function's keyword arguments other than `cols`.
UPDATE_BY_OPS = {
    "cum_sum", "cum_prod", "cum_min", "cum_max", "cum_count_where", "forward_fill", "delta",
    "ema_tick", "ema_time", "ems_tick", "ems_time", "emmin_tick", "emmin_time", "emmax_tick",
    "emmax_time", "emstd_tick", "emstd_time",
    "rolling_sum_tick", "rolling_sum_time", "rolling_group_tick", "rolling_group_time",
    "rolling_avg_tick", "rolling_avg_time", "rolling_min_tick", "rolling_min_time",
    "rolling_max_tick", "rolling_max_time", "rolling_prod_tick", "rolling_prod_time",
    "rolling_count_tick", "rolling_count_time", "rolling_std_tick", "rolling_std_time",
    "rolling_wavg_tick", "rolling_wavg_time", "rolling_formula_tick", "rolling_formula_time",
    "rolling_count_where_tick", "rolling_count_where_time",
}
NATURAL_JOIN_TYPES = {"error_on_duplicate", "first_match", "last_match", "exactly_one_match"}
ACTIONS = {"add", "delete"}
# Structured filter conditions: the key of a condition object, and for comparisons the operator.
COMPARE_OPS = {"<", "<=", ">", ">=", "==", "!="}


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
    # A blink input table: rows added in one update cycle are gone in the next.
    blink: bool = False

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
            if value not in FLOAT_WORDS:
                raise _fail(where, f"expected a number or one of {sorted(FLOAT_WORDS)}")
            return FLOAT_WORDS[value]
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
        _keys(f"{where}.ticking", ticking, {"key_columns"}, {"blink"})
        _strings(f"{where}.ticking.key_columns", ticking["key_columns"])
        unknown = set(ticking["key_columns"]) - set(names)
        if unknown:
            raise _fail(f"{where}.ticking", f"key columns {sorted(unknown)} are not columns")
        key_columns = tuple(ticking["key_columns"])
        if not isinstance(ticking.get("blink", False), bool):
            raise _fail(f"{where}.ticking", "blink must be true or false")
        if ticking.get("blink") and key_columns:
            raise _fail(f"{where}.ticking", "a blink input table has no key columns")
        if ticking.get("blink") and doc["rows"]:
            # Initial rows are added before the operations exist and are gone a cycle later.
            raise _fail(f"{where}.ticking", "a blink input table starts without rows")
    return Input(doc["name"], tuple(columns), _rows(f"{where}.rows", tuple(columns), doc["rows"]),
                 key_columns, bool(ticking and ticking.get("blink", False)))


def _aggregation(where: str, agg: Any) -> None:
    if not isinstance(agg, dict) or agg.get("agg") not in AGGREGATIONS:
        raise _fail(where, f"agg must be one of {sorted(AGGREGATIONS)}")
    kind = agg["agg"]
    if kind in ("count", "count_where"):
        _keys(where, agg, {"agg", "column"} | ({"args"} if kind == "count_where" else set()),
              set())
    else:
        _keys(where, agg, {"agg", "columns"}, {"args"})
        _strings(f"{where}.columns", agg["columns"])
    args = agg.get("args", {})
    if not isinstance(args, dict) or set(args) - AGGREGATIONS[kind]:
        raise _fail(where, f"args of {kind} may hold {sorted(AGGREGATIONS[kind])}")


def _value(where: str, value: Any) -> None:
    if not isinstance(value, dict) or len(value) != 1 or next(iter(value)) not in (
            "column", "literal", "long", "nanos"):
        raise _fail(where, "a value is {\"column\": name}, {\"literal\": string, number or "
                           "boolean}, {\"long\": integer} or {\"nanos\": integer}")


def _condition(where: str, condition: Any) -> None:
    """A structured filter condition, shaped like the `Condition` message of table.proto."""
    if not isinstance(condition, dict) or len(condition) != 1:
        raise _fail(where, "a condition is an object with one key")
    kind, body = next(iter(condition.items()))
    if kind in ("and", "or"):
        if not isinstance(body, list) or not body:
            raise _fail(where, f"{kind} needs a non-empty list of conditions")
        for index, item in enumerate(body):
            _condition(f"{where}.{kind}[{index}]", item)
    elif kind == "not":
        _condition(f"{where}.not", body)
    elif kind == "compare":
        _keys(where, body, {"op", "lhs", "rhs"}, {"ignore_case"})
        if body["op"] not in COMPARE_OPS:
            raise _fail(where, f"op must be one of {sorted(COMPARE_OPS)}")
        _value(f"{where}.lhs", body["lhs"])
        _value(f"{where}.rhs", body["rhs"])
    elif kind == "in":
        _keys(where, body, {"target", "candidates"}, {"ignore_case", "inverted", "nan_equals"})
        _value(f"{where}.target", body["target"])
        for index, item in enumerate(body["candidates"]):
            _value(f"{where}.candidates[{index}]", item)
    elif kind in ("is_null", "is_nan"):
        if not isinstance(body, str):
            raise _fail(where, f"{kind} names a column")
    elif kind in ("matches", "contains"):
        _keys(where, body, {"column", "text"}, {"ignore_case", "inverted"})
    elif kind == "invoke":
        _keys(where, body, {"method", "arguments"}, {"target"})
        for index, item in enumerate(body["arguments"]):
            _value(f"{where}.arguments[{index}]", item)
    elif kind == "search":
        _keys(where, body, {"text"}, {"columns"})
    else:
        raise _fail(where, f"unknown condition {kind!r}")


def _operation(where: str, doc: Any, known: set[str]) -> None:
    if not isinstance(doc, dict) or "op" not in doc or "name" not in doc:
        raise _fail(where, "expected an object with 'name' and 'op'")
    op = doc["op"]
    if op not in OPERATIONS:
        raise _fail(where, f"unknown operation {op!r}")
    required, optional = OPERATIONS[op]
    _keys(where, doc, required | {"name", "op"}, optional)

    def table(name: Any, key: str) -> None:
        if name not in known:
            raise _fail(where, f"{key} {name!r} is not defined before this operation")

    def count(key: str, negative: bool = False) -> None:
        value = doc[key]
        if isinstance(value, bool) or not isinstance(value, int) or (value < 0 and not negative):
            raise _fail(where, f"{key} must be {'an' if negative else 'a non-negative'} integer")

    def flag(key: str) -> None:
        if not isinstance(doc.get(key, False), bool):
            raise _fail(where, f"{key} must be true or false")

    for key in TABLE_KEYS:
        if key in doc:
            table(doc[key], key)
    if op == "merge":
        _strings(f"{where}.sources", doc["sources"], allow_empty=False)
        for name in doc["sources"]:
            table(name, "source")
    if op == "multi_join":
        if not isinstance(doc["inputs"], list) or not doc["inputs"]:
            raise _fail(where, "inputs must be a non-empty list")
        for i, item in enumerate(doc["inputs"]):
            _keys(f"{where}.inputs[{i}]", item, {"table", "on"}, {"joins"})
            table(item["table"], "table")
            # Without match columns only as a script: the table service has no such request.
            _strings(f"{where}.inputs[{i}].on", item["on"],
                     allow_empty=doc.get("script") is True)
            if item.get("joins") is not None:
                _strings(f"{where}.inputs[{i}].joins", item["joins"])
    for key in ("filters", "formulas", "columns"):
        if key in doc:
            _strings(f"{where}.{key}", doc[key],
                     allow_empty=op in ("select_distinct", "ungroup"))
    for key in ("by", "on", "stamp_columns", "row_by", "col_by"):
        if key in doc and op != "sort":
            _strings(f"{where}.{key}", doc[key])
    if doc.get("joins") is not None:
        _strings(f"{where}.joins", doc["joins"])
    if op in ("head", "tail", "head_by", "tail_by"):
        count("rows")
    if op == "slice":
        count("start", negative=True)
        count("stop", negative=True)
    if op == "empty_table":
        count("size")
    if op == "time_table":
        for key in ("period", "start_time"):
            if not isinstance(doc[key], str):
                raise _fail(where, f"{key} must be a string")
    if op == "column_statistics":
        if not isinstance(doc["column"], str):
            raise _fail(where, "column must be a string")
        if "unique_value_limit" in doc:
            count("unique_value_limit")
    for key in ("inverted", "null_fill", "initial", "incremental", "history", "blink",
                "preserve_empty", "script", "drop_keys", "initialize"):
        flag(key)
    if op == "ring_table":
        # Any integer: the reference has an answer for zero and for a negative capacity.
        count("capacity", negative=True)
    if op == "partition_get" and doc.get("unique", "not_set") not in UNIQUE_BEHAVIORS:
        raise _fail(where, f"unique must be one of {sorted(UNIQUE_BEHAVIORS)}")
    if op == "keyed_transpose" and doc.get("new_columns", "fail") not in NEW_COLUMN_BEHAVIORS:
        raise _fail(where, f"new_columns must be one of {sorted(NEW_COLUMN_BEHAVIORS)}")
    if op == "sort":
        if not isinstance(doc["by"], list) or not doc["by"]:
            raise _fail(where, "by must be a non-empty list")
        for b, by in enumerate(doc["by"]):
            _keys(f"{where}.by[{b}]", by, {"column", "direction"}, {"absolute"})
            if by["direction"] not in ("ascending", "descending"):
                raise _fail(f"{where}.by[{b}]", "direction must be ascending or descending")
            if not isinstance(by.get("absolute", False), bool):
                raise _fail(f"{where}.by[{b}]", "absolute must be true or false")
    if op in ("agg_by", "range_join", "keyed_transpose"):
        # A range join without aggregations only as a script, like a multi join without keys.
        may_be_empty = op == "range_join" and doc.get("script") is True
        if not isinstance(doc["aggs"], list) or not (doc["aggs"] or may_be_empty):
            raise _fail(where, "aggs must be a non-empty list")
        for a, agg in enumerate(doc["aggs"]):
            _aggregation(f"{where}.aggs[{a}]", agg)
            if agg["agg"] not in ("count", "count_where") and not agg["columns"]:
                raise _fail(f"{where}.aggs[{a}].columns", "must not be empty")
    if op == "agg_all_by":
        _aggregation(f"{where}.agg", doc["agg"])
        if doc["agg"]["agg"] in ("count", "count_where") or doc["agg"]["columns"]:
            raise _fail(f"{where}.agg", "agg_all_by takes an aggregation with empty columns")
    if op == "filter":
        if not isinstance(doc["conditions"], list) or not doc["conditions"]:
            raise _fail(where, "conditions must be a non-empty list")
        for c, condition in enumerate(doc["conditions"]):
            _condition(f"{where}.conditions[{c}]", condition)
    if op == "update_by":
        if not isinstance(doc["ops"], list) or not doc["ops"]:
            raise _fail(where, "ops must be a non-empty list")
        for o, ub in enumerate(doc["ops"]):
            _keys(f"{where}.ops[{o}]", ub, {"op", "columns"}, {"args"})
            if ub["op"] not in UPDATE_BY_OPS:
                raise _fail(f"{where}.ops[{o}]", f"op must be one of {sorted(UPDATE_BY_OPS)}")
            _strings(f"{where}.ops[{o}].columns", ub["columns"], allow_empty=False)
            if not isinstance(ub.get("args", {}), dict):
                raise _fail(f"{where}.ops[{o}]", "args must be an object")
    if op == "natural_join" and doc.get("type", "error_on_duplicate") not in NATURAL_JOIN_TYPES:
        raise _fail(where, f"type must be one of {sorted(NATURAL_JOIN_TYPES)}")


def references(op: Any) -> list[str]:
    """The names of the tables an operation reads."""
    names = [op[key] for key in TABLE_KEYS if key in op]
    names.extend(op.get("sources", ()))
    names.extend(item["table"] for item in op.get("inputs", ()))
    return names


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

    if not isinstance(doc["inputs"], list):
        raise _fail(where, "inputs must be a list")
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


# Other names for the harness operations: the TableService request each one is sent as, which is
# how the task ledger names operations. `input_table` is not an operation: it selects the
# scenarios that drive input tables (InputTableService), that is, the ticking ones.
OP_ALIASES = {
    "unstructured_filter": "where",
    "aggregate": "agg_by",
    "cross_join": "join",
    "aj_tables": "aj",
}
INPUT_TABLE = "input_table"


def resolve_ops(names: list[str]) -> frozenset[str]:
    """Checks a list of operation names from the command line and resolves aliases."""
    resolved = set()
    for name in names:
        name = OP_ALIASES.get(name, name)
        if name not in OPERATIONS and name != INPUT_TABLE:
            known = sorted(set(OPERATIONS) | set(OP_ALIASES) | {INPUT_TABLE})
            raise ScenarioError(
                f"the harness has no operation {name!r}; it knows: {', '.join(known)}"
            )
        resolved.add(name)
    return frozenset(resolved)


def ops_of(scenario: Scenario) -> frozenset[str]:
    """The operations a scenario exercises, plus `input_table` when it has ticking inputs."""
    ops = {op["op"] for op in scenario.operations}
    if scenario.ticking:
        ops.add(INPUT_TABLE)
    return frozenset(ops)


def select(
    scenarios: list[Scenario],
    ops: frozenset[str] | None = None,
    within: frozenset[str] | None = None,
) -> list[Scenario]:
    """Scenarios that use at least one of `ops` and nothing outside `within`."""
    out = []
    for scenario in scenarios:
        used = ops_of(scenario)
        if ops is not None and not used & ops:
            continue
        if within is not None and not used <= within:
            continue
        out.append(scenario)
    return out


def load_all(directory: Path, names: list[str] | None = None) -> list[Scenario]:
    """Loads every scenario under `directory`, sorted by name; `names` narrows the set.

    `directory` may also be a single scenario file.
    """
    paths = [directory] if directory.is_file() else directory.rglob("*.json")
    scenarios = sorted((load(p) for p in paths), key=lambda s: s.name)
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
