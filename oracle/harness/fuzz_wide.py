"""The fuzzer's wide generator: every operation the harness can send that has a clock-free
result, with the variants of each (`fuzz.generate(..., variants="all")`).

No Java counterpart: this is the harness's fuzzer. `fuzz._Generator` is the generator of the
recorded seeds and is not changed by anything here; this one draws from its own random stream,
so a seed means another scenario than it does there.

What it adds to `fuzz._Generator`:

- operations: `raj`, `left_join`, `where_in` (also inverted), `range_join`, `multi_join`,
  `agg_all_by`, `group_by`, `ungroup`, `snapshot`, `snapshot_when` and `meta_table`;
- `update_by` with the cumulative operators, `forward_fill`, `delta` with its controls, the
  `em*` family and the rolling operators in ticks and in time, with their windows, decays and
  bad-data controls; `agg_by` with every aggregation that takes no formula; joins with several
  match columns and with all right columns; structured filters with string, timestamp, `in`,
  pattern and case-insensitive conditions; absolute sorts;
- inputs: a timestamp column `T` that does not decrease from row to row (what the time windows
  of `update_by` need), start and end columns for `range_join`, a second right table for
  `multi_join`, and right tables whose key stays unique through the steps;
- array columns (`list:<type>` in the predicted columns), which only `ungroup`, the joins and
  the operations that pass columns through take as arguments.

Requests the reference refuses are part of the comparison, so every operation is given
arguments it refuses now and then: a column of a type it does not take, a window or decay out
of range, a percentile above one. Two kinds of refusal are kept out, because the reference
does not answer them the same way twice (`oracle/README.md`, "Known limits": a table that
fails during an update cycle is not a consistent table afterwards):

- a time window over a table whose timestamps can go backwards in a later step;
- on a refreshing table, arguments that are valid for the rows it has now and not for rows a
  step can add: an `ungroup` of arrays that only happen to have equal lengths, a `multi_join`
  of a table whose key only happens to be unique.

Not generated, each for a stated reason:

- `rolling_group_*`, `rolling_formula_*`, `cum_count_where`, `rolling_count_where_*` and the
  `formula` aggregation: Oxhaven answers them `UNIMPLEMENTED` by design (the engine lists
  them as not ported), so every scenario with one would differ in the same known way.
- `time_table`: its rows depend on the clock. `empty_table`: it has no columns to operate on.
  `column_statistics`: no handler in Oxhaven.
- An aggregation of an array column into arrays (`group` of a `group`): the comparison rules
  do not know nested arrays.
"""

from __future__ import annotations

from typing import Any, Callable

from . import rules
from . import scenario as sc
from .fuzz import (FLOATS, INT_BITS, JOINS, MAX_ROWS, NUMERIC, ORDERED_STAMPS, T0, _Generator,
                   _Table)

LIST = "list:"
SECOND = 1_000_000_000
ALL_TYPES = tuple(sc.COLUMN_TYPES)
KEY_TYPES = ("int32", "string", "int64", "int16", "bool", "float64", "int8", "timestamp")

# Operations that read a second table; an input `r` exists when one of them is asked for.
RIGHT_OPS = frozenset(JOINS) | {"left_join", "raj", "where_in", "range_join", "multi_join",
                                "snapshot_when"}
STAMPED_OPS = frozenset({"aj", "raj", "range_join"})
# Operations whose result has the rows of their first source in the same order, so a
# timestamp column that does not decrease there does not decrease in the result either.
ORDER_KEEPING = frozenset({
    "where", "filter", "head", "tail", "slice", "flatten", "drop_columns", "update",
    "update_view", "lazy_update", "update_by", "where_in", "head_by", "tail_by",
    "natural_join", "exact_join", "aj", "raj", "snapshot",
})
# Operations whose result has, per row of the first source, at most one row with its columns.
KEY_KEEPING = ORDER_KEEPING | {"sort", "reverse"}


def _same(kind: str) -> str:
    return kind


def _long_or_double(kind: str) -> str:
    return "float64" if kind in FLOATS else "int64"


def _long_or_same(kind: str) -> str:
    return kind if kind in FLOATS else "int64"


def _double(kind: str) -> str:
    return "float64"


def _long(kind: str) -> str:
    return "int64"


def _list(kind: str) -> str:
    return LIST + kind


_SUMMABLE = NUMERIC + ("bool",)
_ORDERED = tuple(t for t in ALL_TYPES if t != "bool")

# `update_by` operators: the input column types the reference takes, and the result type.
# Read from the running reference (42.9), one column of each type per operator.
UPDATE_BY: dict[str, tuple[tuple[str, ...], Callable[[str], str]]] = {
    "cum_sum": (_SUMMABLE, _long_or_double),
    "cum_prod": (NUMERIC, _long_or_double),
    "cum_min": (_ORDERED, _same),
    "cum_max": (_ORDERED, _same),
    "forward_fill": (ALL_TYPES, _same),
    "delta": (NUMERIC + ("timestamp",), lambda kind: "int64" if kind == "timestamp" else kind),
}
EM_OPERATORS = ("ema", "ems", "emmin", "emmax", "emstd")
ROLLING_OPERATORS: dict[str, tuple[tuple[str, ...], Callable[[str], str]]] = {
    "sum": (_SUMMABLE, _long_or_double),
    "avg": (_SUMMABLE, _double),
    "min": (_ORDERED, _same),
    "max": (_ORDERED, _same),
    "prod": (NUMERIC, _double),
    "count": (tuple(t for t in ALL_TYPES if t != "timestamp"), _long),
    "std": (_SUMMABLE, _double),
    "wavg": (NUMERIC, _double),
}
for _name in EM_OPERATORS:
    for _unit in ("tick", "time"):
        UPDATE_BY[f"{_name}_{_unit}"] = (NUMERIC, _double)
for _name, _entry in ROLLING_OPERATORS.items():
    for _unit in ("tick", "time"):
        UPDATE_BY[f"rolling_{_name}_{_unit}"] = _entry

# Aggregations: the input column types the reference takes, and the result type. `median`
# and `pct` are refined in `_Wide.aggregation` (the average of two integers is a double).
AGGREGATIONS: dict[str, tuple[tuple[str, ...], Callable[[str], str]]] = {
    "sum": (_SUMMABLE, _long_or_same),
    "abs_sum": (_SUMMABLE, _long_or_same),
    "avg": (NUMERIC, _double),
    "std": (NUMERIC, _double),
    "var": (NUMERIC, _double),
    "min": (ALL_TYPES, _same),
    "max": (ALL_TYPES, _same),
    "first": (ALL_TYPES, _same),
    "last": (ALL_TYPES, _same),
    "median": (ALL_TYPES, _same),
    "pct": (ALL_TYPES, _same),
    "unique": (ALL_TYPES, _same),
    "sorted_first": (ALL_TYPES, _same),
    "sorted_last": (ALL_TYPES, _same),
    "count_distinct": (ALL_TYPES, _long),
    "distinct": (ALL_TYPES, _list),
    "group": (ALL_TYPES, _list),
    "weighted_avg": (NUMERIC, _double),
    "weighted_sum": (NUMERIC, _long_or_double),
}
# Drawn for `agg_by`; the simple ones are listed twice so that a table with a few
# aggregations usually has one whose result can feed the next operation.
_AGG_DRAW = tuple(AGGREGATIONS) + ("count", "count_where", "sum", "min", "max", "first",
                                   "last", "avg")
_AGG_ALL_DRAW = tuple(name for name in AGGREGATIONS if not name.startswith("weighted"))

_UNIQUE_KEYS: dict[str, list[Any]] = {
    "string": list("abcdefghij"),
    "float64": [0.0, 1.0, 2.5, -1.0, 3.5, 4.0, 7.25, "NaN", 100.0, 0.5],
    "bool": [True, False],
}


def _canonical(cell: Any) -> str:
    """One spelling per key value of the engine: 0.0 and -0.0 are one key."""
    if cell == 0 and not isinstance(cell, bool):
        return "0.0"
    return repr(cell)


def _ascending(cell: Any) -> tuple[int, Any]:
    """The engine's ascending order: null first, NaN last."""
    if cell is None:
        return (0, 0)
    if isinstance(cell, str):  # "NaN", "Infinity", "-Infinity"
        return (2, 0) if cell == "NaN" else (1, sc.FLOAT_WORDS[cell])
    return (1, cell)


class _Wide(_Generator):
    """`fuzz._Generator` with the operations and variants listed in the module comment."""

    def __init__(self, rng: Any, ops: tuple[str, ...]):
        super().__init__(rng, ops)
        self.clock: dict[str, int] = {}  # nanoseconds after T0 of the last row's T
        self.row_t: dict[str, dict[int, Any]] = {}  # keyed inputs: T of each live row
        self.row_k: dict[str, dict[int, Any]] = {}  # keyed inputs: K of each live row
        self.live_keys: dict[str, set[str]] = {}  # inputs with a unique K: the keys in use
        self.next_id: dict[str, int] = {}

    def chance(self, probability: float) -> bool:
        return self.rng.random() < probability

    # ------------------------------------------------------------ inputs

    def mode(self) -> str:
        if "range_join" in self.ops and self.chance(0.75):
            return "static"  # the reference refuses a range join of a refreshing table
        return super().mode()

    def make_inputs(self) -> None:
        rng, ops = self.rng, set(self.ops)
        key_type = rng.choice(KEY_TYPES)
        columns = [("K", key_type)]
        for index in range(rng.randrange(2, 6)):
            columns.append((f"C{index}", rng.choice(ALL_TYPES)))
        if not any(kind in NUMERIC for _, kind in columns[1:]):
            columns[1] = ("C0", rng.choice(NUMERIC))
        stamp_type = None
        if ops & STAMPED_OPS:
            stamp_type = rng.choice(ORDERED_STAMPS)
            columns.append(("S", stamp_type))
            if "range_join" in ops:
                columns.append(("E", stamp_type))
        if "update_by" in ops or self.chance(0.3):
            columns.append(("T", "timestamp"))
        mode = self.mode()
        self.make_input("t", columns, {"K"}, mode, "Id")
        if ops & RIGHT_OPS:
            right = [("K", key_type)]
            for index in range(rng.randrange(1, 4)):
                right.append((f"R{index}", rng.choice(ALL_TYPES)))
            if stamp_type:
                right.append(("RS", stamp_type))
            self.unique_right = self.chance(0.8 if "multi_join" in ops else 0.6)
            self.make_input("r", right, {"K"}, self.mode(), "RId", unique_k=self.unique_right)
        if "multi_join" in ops:
            second = [("K", key_type)]
            for index in range(rng.randrange(1, 3)):
                second.append((f"Q{index}", rng.choice(ALL_TYPES)))
            self.make_input("q", second, {"K"}, self.mode(), "QId", unique_k=self.chance(0.85))
        if "merge" in ops:
            self.make_input("m", columns, {"K"}, mode, "Id")

    def make_input(self, name: str, columns: list[tuple[str, str]], key_roles: set[str],
                   mode: str, id_column: str, unique_k: bool = False) -> None:
        rng = self.rng
        doc: dict[str, Any] = {"name": name}
        keyed = mode == "keyed"
        if keyed:
            columns = [(id_column, "int32")] + columns
        if mode != "static":
            doc["ticking"] = {"key_columns": [id_column] if keyed else []}
        count = 0 if self.chance(0.06) else rng.randrange(1, 13)
        self.clock[name] = 0
        self.row_t[name], self.row_k[name] = {}, {}
        if unique_k:
            self.live_keys[name] = set()
        rows = []
        for row_id in range(1, count + 1):
            cells = self.wide_row(name, columns, row_id if keyed else None)
            if cells is not None:
                rows.append(cells)
        self.next_id[name] = count + 1
        names = [n for n, _ in columns]
        ordered = "T" in names
        if ordered and mode == "static" and len(rows) > 1 and self.chance(0.05):
            # Timestamps that go backwards: the reference refuses a time window over them.
            position = names.index("T")
            first, second = rng.sample(range(len(rows)), 2)
            rows[first][position], rows[second][position] = (
                rows[second][position], rows[first][position])
            ordered = False
        if "RS" in names and "range_join" in self.ops and mode == "static" and self.chance(0.85):
            # The reference refuses a range join whose right range column is not in
            # ascending order (io.deephaven.engine.exceptions.OutOfOrderException).
            position = names.index("RS")
            cells = sorted((row[position] for row in rows), key=_ascending)
            for row, cell in zip(rows, cells):
                row[position] = cell
        doc["columns"] = [{"name": n, "type": t} for n, t in columns]
        doc["rows"] = rows
        self.inputs.append(doc)
        self.tables[name] = _Table(name, list(columns), len(rows), live=mode != "static",
                                   ordered=ordered, unique=("K",) if unique_k else ())

    def unique_key(self, name: str, kind: str) -> tuple[bool, Any]:
        """A value for K that no live row of input `name` has; (False, None) when none is
        found."""
        rng = self.rng
        for _ in range(6):
            if self.chance(0.06):
                cell = None
            elif kind in INT_BITS:
                cell = rng.randrange(0, 12)
            elif kind == "timestamp":
                cell = T0 + rng.randrange(0, 12)
            else:
                cell = rng.choice(_UNIQUE_KEYS[kind])
            if _canonical(cell) not in self.live_keys[name]:
                return True, cell
        return False, None

    def wide_row(self, name: str, columns: list[tuple[str, str]], row_id: int | None,
                 replaces: bool = False) -> list[Any] | None:
        """One row for input `name`; None when its key has to be unique and no free key was
        found. `replaces`: the row takes the place of the live row `row_id`."""
        rng = self.rng
        unique = name in self.live_keys
        cells: list[Any] = []
        for column, kind in columns:
            if column == "K" and unique:
                if replaces:
                    cells.append(self.row_k[name][row_id])  # a replaced row keeps its key
                    continue
                found, cell = self.unique_key(name, kind)
                if not found:
                    return None
                self.live_keys[name].add(_canonical(cell))
                cells.append(cell)
            elif column == "T":
                if replaces:
                    # A keyed input table replaces a row in place: the same timestamp keeps
                    # the column from going backwards.
                    cells.append(self.row_t[name][row_id])
                    continue
                self.clock[name] += rng.choice([0, 0, 1, 1, 1, 2, 3]) * rng.choice(
                    [SECOND, SECOND, SECOND // 2])
                cells.append(None if self.chance(0.04) else T0 + self.clock[name])
            else:
                cells.append(self.value(kind, key=column == "K"))
        if row_id is not None:
            cells[0] = row_id
            names = [n for n, _ in columns]
            if "T" in names:
                self.row_t[name][row_id] = cells[names.index("T")]
            if unique:
                self.row_k[name][row_id] = cells[names.index("K")]
        return cells

    def make_steps(self) -> list[dict[str, Any]]:
        rng = self.rng
        ticking = [doc for doc in self.inputs if "ticking" in doc]
        if not ticking:
            return []
        live = {doc["name"]: [row[0] for row in doc["rows"]] if doc["ticking"]["key_columns"]
                else [] for doc in ticking}
        steps = []
        for _ in range(rng.randrange(1, 6)):
            actions = []
            for _ in range(rng.randrange(1, 3)):
                doc = rng.choice(ticking)
                name = doc["name"]
                keyed = bool(doc["ticking"]["key_columns"])
                columns = [(c["name"], c["type"]) for c in doc["columns"]]
                table = self.tables[name]
                choice = rng.random()
                if keyed and live[name] and choice < 0.3:
                    keys = rng.sample(live[name], rng.randrange(1, min(3, len(live[name])) + 1))
                    for key in keys:
                        live[name].remove(key)
                        if name in self.live_keys:
                            self.live_keys[name].discard(_canonical(self.row_k[name][key]))
                    if self.chance(0.2):
                        keys.append(9999)
                    actions.append({"action": "delete", "table": name,
                                    "rows": [[key] for key in keys]})
                    continue
                rows = []
                for _ in range(rng.randrange(1, 4)):
                    if keyed and live[name] and choice < 0.65:
                        cells = self.wide_row(name, columns, rng.choice(live[name]), True)
                    elif keyed:
                        row_id = self.next_id[name]
                        cells = self.wide_row(name, columns, row_id)
                        if cells is not None:
                            self.next_id[name] += 1
                            live[name].append(row_id)
                    else:
                        cells = self.wide_row(name, columns, None)
                    if cells is not None:
                        rows.append(cells)
                if rows:
                    table.rows += len(rows)
                    actions.append({"action": "add", "table": name, "rows": rows})
            if actions:
                steps.append({"actions": actions})
        return steps

    # ------------------------------------------------------------ bookkeeping

    def add(self, op: dict[str, Any], columns: list[tuple[str, str]], rows: int) -> None:
        super().add(op, columns, rows)
        table = self.tables[op["name"]]
        kind = op["op"]
        sources = [self.tables[name] for name in sc.references(op)]
        first = sources[0]
        table.live = any(s.live for s in sources)
        if kind in ("snapshot", "meta_table"):
            table.live = False
        elif kind == "snapshot_when":
            table.live = self.tables[op["trigger"]].live
        names = dict(columns)
        table.ordered = (kind in ORDER_KEEPING and first.ordered
                         and names.get("T") == "timestamp")
        if kind in KEY_KEEPING and all(key in names for key in first.unique):
            table.unique = first.unique
        elif kind in ("agg_by", "agg_all_by", "group_by"):
            table.unique = tuple(op["by"])
        elif kind == "select_distinct":
            table.unique = tuple(op["columns"])
        elif kind == "multi_join":
            table.unique = (columns[0][0],)
        # Array columns with the same origin have the same length in every row.
        made_here = set()
        if kind in ("group_by", "agg_all_by"):
            made_here = set(names) - set(op["by"])
            if kind == "agg_all_by" and op["agg"]["agg"] != "group":
                made_here = set()
        elif kind in ("agg_by", "range_join"):
            made_here = {pair.split("=")[0].strip() for agg in op["aggs"]
                         if agg["agg"] == "group" for pair in agg["columns"]}
        for column, column_kind in columns:
            if not column_kind.startswith(LIST):
                continue
            if column in made_here:
                table.origin[column] = op["name"]
            elif (kind in KEY_KEEPING or kind in ("merge", "join")) and (
                    first.origin.get(column) and dict(first.columns).get(column) == column_kind):
                table.origin[column] = first.origin[column]
            else:
                table.origin[column] = f"{op['name']}.{column}"

    def pick(self, wanted: Callable[[_Table], bool]) -> _Table | None:
        """A table for which `wanted` holds, recent ones preferred; None when there is none."""
        tables = [t for t in self.tables.values() if wanted(t)]
        if not tables:
            return None
        return self.rng.choices(tables, [1 + index for index in range(len(tables))])[0]

    @staticmethod
    def has_lists(table: _Table, besides: tuple[str, ...] | list[str] = ()) -> bool:
        return any(kind.startswith(LIST) for name, kind in table.columns if name not in besides)

    # ------------------------------------------------------------ operations

    def build(self, kind: str) -> bool:
        handler = getattr(self, f"build_{kind}", None)
        if handler is not None and kind not in JOINS:
            return handler(self.fresh("o"))
        if not super().build(kind):
            return False
        op = self.operations[-1]
        table = self.tables[op["name"]]
        if kind == "sort":
            for by in op["by"]:
                if table.type_of(by["column"]) in NUMERIC and self.chance(0.15):
                    by["absolute"] = True
        elif kind in ("head_by", "tail_by") and self.chance(0.04):
            op["rows"] = 0  # refused by the reference
        elif kind == "select_distinct" and self.chance(0.06) and not self.has_lists(
                self.tables[op["source"]]):
            source = self.tables[op["source"]]
            op["columns"] = []  # every column
            table.columns = list(source.columns)
            table.unique = ()
            table.computed = rules.computed_columns(self.operations)[op["name"]]
        return True

    # -- filters

    def build_filter(self, name: str) -> bool:
        source = self.pick_table()
        conditions = [c for c in (self.wide_condition(source)
                                  for _ in range(self.rng.randrange(1, 3))) if c]
        if not conditions:
            return False
        self.add({"name": name, "op": "filter", "source": source.name,
                  "conditions": conditions}, list(source.columns), source.rows)
        return True

    def wide_condition(self, table: _Table, depth: int = 0) -> dict[str, Any] | None:
        """A structured condition over any column type. Unlike `condition`, it also builds
        what the reference refuses (the NaN test, a string in a range comparison)."""
        rng = self.rng
        if self.chance(0.5):
            return self.condition(table, depth)
        columns = table.usable()
        if not columns:
            return None
        if depth < 2 and self.chance(0.25):
            both = [c for c in (self.wide_condition(table, depth + 1) for _ in range(2)) if c]
            if not both:
                return None
            return {"not": both[0]} if self.chance(0.3) else {rng.choice(["and", "or"]): both}
        column = rng.choice(columns)
        kind = table.type_of(column)
        target = {"column": column}
        if kind == "string":
            shape = rng.choice(["compare", "in", "matches", "contains"])
            text = rng.choice(["a", "b", "A", "aa", "", "z"])
            flags: dict[str, Any] = {}
            if self.chance(0.3):
                flags["ignore_case"] = True
            if shape == "compare":
                op = rng.choice(["==", "!="]) if self.chance(0.85) else "<"
                return {"compare": {"op": op, "lhs": target, "rhs": {"literal": text}, **flags}}
            if self.chance(0.3):
                flags["inverted"] = True
            if shape == "in":
                return {"in": {"target": target, "candidates": [
                    {"literal": rng.choice(["a", "b", "A", "aa", ""])}
                    for _ in range(rng.randrange(1, 4))], **flags}}
            if shape == "matches":
                return {"matches": {"column": column, "text": rng.choice(
                    ["a", "a.*", "[ab]", ".", "a|b", "A+", "^$"]), **flags}}
            return {"contains": {"column": column, "text": text, **flags}}
        if kind == "bool":
            return {"compare": {"op": rng.choice(["==", "!="]), "lhs": target,
                                "rhs": {"literal": self.chance(0.5)}}}
        if kind == "timestamp":
            stamp = {"nanos": T0 + rng.randrange(-2, 6) * rng.choice([1, SECOND])}
            if self.chance(0.3):
                return {"in": {"target": target, "candidates": [stamp]}}
            return {"compare": {"op": rng.choice(sorted(sc.COMPARE_OPS)), "lhs": target,
                                "rhs": stamp}}
        if kind in FLOATS:
            if self.chance(0.1):
                return {"is_nan": column}
            literal: dict[str, Any] = {"literal": rng.choice([0.0, 1.0, -1.0, 0.5, 1.5, -0.0])}
            if self.chance(0.06):
                literal = {"literal": rng.choice(["NaN", "Infinity"])}
        else:
            literal = {"long": rng.randrange(-2, 5)}
        if self.chance(0.3):
            flags = {"inverted": True} if self.chance(0.3) else {}
            if kind in FLOATS and self.chance(0.4):
                flags["nan_equals"] = self.chance(0.5)
            return {"in": {"target": target, "candidates": [literal], **flags}}
        return {"compare": {"op": rng.choice(["==", "!=", "<", ">="]), "lhs": target,
                            "rhs": literal}}

    # -- update_by

    def timestamps(self, source: _Table) -> str | None:
        """The timestamp column for a time window over `source`, if one may be used."""
        if source.ordered:
            return "T"
        if source.live:
            return None  # its timestamps can go backwards in a later step
        stamps = source.usable(("timestamp",))
        if stamps and self.chance(0.3):
            return self.rng.choice(stamps)  # refused unless it happens to be ordered
        return None

    def update_by_operator(self, source: _Table, columns: list[tuple[str, str]],
                           used: set[str]) -> dict[str, Any] | None:
        rng = self.rng
        family = rng.choice(["cum", "cum", "em", "em", "rolling", "rolling", "rolling", "fill",
                             "delta"])
        stamps = self.timestamps(source)
        unit = "time" if stamps and self.chance(0.5) else "tick"
        args: dict[str, Any] = {}
        if family == "cum":
            op = rng.choice(["cum_sum", "cum_prod", "cum_min", "cum_max"])
        elif family == "fill":
            op = "forward_fill"
        elif family == "delta":
            op = "delta"
            if self.chance(0.6):
                args["delta_control"] = rng.choice(
                    ["NULL_DOMINATES", "VALUE_DOMINATES", "ZERO_DOMINATES"])
        elif family == "em":
            op = f"{rng.choice(EM_OPERATORS)}_{unit}"
            if unit == "tick":
                args["decay_ticks"] = rng.choice([1, 2, 3, 5, 10, 1.5, 0.5])
                if self.chance(0.03):
                    args["decay_ticks"] = rng.choice([0, -1])
            else:
                args["ts_col"] = stamps
                args["decay_time"] = rng.choice(["PT1S", "PT2S", "PT5S", "PT0.5S", "PT1M"])
                if self.chance(0.03):
                    args["decay_time"] = rng.choice(["PT0S", "PT-1S"])
            if self.chance(0.3):
                # THROW fails the table when a null or NaN arrives, which on a refreshing
                # table would be a failure during an update cycle.
                behaviors = ["SKIP", "RESET", "POISON"] + ([] if source.live else ["THROW"])
                args["op_control"] = {"on_null": rng.choice(behaviors),
                                      "on_nan": rng.choice(behaviors)}
        else:
            name = rng.choice(list(ROLLING_OPERATORS))
            op = f"rolling_{name}_{unit}"
            if unit == "tick":
                args["rev_ticks"] = rng.choice([1, 1, 2, 3, 4, 0, 5])
                args["fwd_ticks"] = rng.choice([0, 0, 0, 1, 2, -1])
                if self.chance(0.03):
                    args["rev_ticks"] = -2
            else:
                args["ts_col"] = stamps
                args["rev_time"] = rng.choice(["PT0S", "PT1S", "PT2S", "PT5S", "PT1M"])
                args["fwd_time"] = rng.choice(["PT0S", "PT0S", "PT1S", "PT2S"])
                if self.chance(0.03):
                    args["rev_time"] = "PT-3S"
            if name == "wavg":
                weights = source.usable(NUMERIC if not self.chance(0.04) else None)
                if not weights:
                    return None
                args["wcol"] = rng.choice(weights)
        accepted, result = UPDATE_BY[op]
        pool = source.usable(None if self.chance(0.05) else accepted)
        if not pool:
            return None
        pairs = []
        for column in rng.sample(pool, rng.randrange(1, min(2, len(pool)) + 1)):
            out = self.fresh("U")
            kind = source.type_of(column)
            pairs.append(f"{out} = {column}")
            columns.append((out, result(kind) if kind in accepted else kind))
            used.add(column)
        spec: dict[str, Any] = {"op": op, "columns": pairs}
        if args:
            spec["args"] = args
        return spec

    def build_update_by(self, name: str) -> bool:
        source = self.pick_table()
        columns = list(source.columns)
        used: set[str] = set()
        ops = [made for made in (self.update_by_operator(source, columns, used)
                                 for _ in range(self.rng.randrange(1, 4))) if made]
        if not ops:
            return False
        by = self.keys(source, 0, 2 if self.chance(0.3) else 1, exclude=used)
        self.add({"name": name, "op": "update_by", "source": source.name, "ops": ops, "by": by},
                 columns, source.rows)
        return True

    # -- aggregations

    def aggregation(self, source: _Table, candidates: list[str],
                    columns: list[tuple[str, str]]) -> dict[str, Any] | None:
        rng = self.rng
        agg = rng.choice(_AGG_DRAW)
        if agg == "count":
            out = self.fresh("N")
            columns.append((out, "int64"))
            return {"agg": "count", "column": out}
        if agg == "count_where":
            filters = [f for f in (self.filter(source) for _ in range(rng.randrange(1, 3))) if f]
            if not filters:
                return None
            out = self.fresh("N")
            columns.append((out, "int64"))
            return {"agg": "count_where", "column": out, "args": {"filters": filters}}
        accepted, result = AGGREGATIONS[agg]
        pool = [c for c in candidates if source.type_of(c) in accepted]
        if agg not in ("group", "distinct") and self.chance(0.04):
            pool = candidates  # now and then an aggregation the reference refuses
        if not pool:
            return None
        column = rng.choice(pool)
        kind = source.type_of(column)
        args: dict[str, Any] = {}
        predicted = result(kind) if kind in accepted else kind
        if agg in ("median", "pct"):
            if agg == "pct":
                args["percentile"] = rng.choice([0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0])
                if self.chance(0.03):
                    args["percentile"] = rng.choice([1.5, -0.1])
            if agg == "pct" or self.chance(0.5):
                args["average_evenly_divided"] = self.chance(0.5)
            if args.get("average_evenly_divided", agg == "median") and kind in (
                    "int32", "int64"):
                predicted = "float64"
        elif agg == "count_distinct":
            if self.chance(0.5):
                args["count_nulls"] = self.chance(0.5)
        elif agg == "distinct":
            if self.chance(0.5):
                args["include_nulls"] = self.chance(0.5)
        elif agg == "unique":
            if self.chance(0.5):
                args["include_nulls"] = self.chance(0.5)
            if self.chance(0.4):
                if kind in INT_BITS:
                    args["non_unique_sentinel"] = rng.choice([-1, 99])
                elif kind in FLOATS:
                    args["non_unique_sentinel"] = -1.5
                elif kind == "string":
                    args["non_unique_sentinel"] = "many"
        elif agg in ("sorted_first", "sorted_last"):
            order = [c for c in source.usable() if c != column]
            if not order:
                return None
            args["order_by"] = rng.choice(order)
        elif agg in ("weighted_avg", "weighted_sum"):
            weights = [c for c in source.usable(NUMERIC if not self.chance(0.04) else None)
                       if c != column]
            if not weights:
                return None
            args["wcol"] = rng.choice(weights)
            if agg == "weighted_sum" and source.type_of(args["wcol"]) in FLOATS:
                predicted = "float64"
        out = self.fresh("A")
        columns.append((out, predicted))
        spec: dict[str, Any] = {"agg": agg, "columns": [f"{out} = {column}"]}
        if args:
            spec["args"] = args
        return spec

    def build_agg(self, base: dict[str, Any], source: _Table) -> bool:
        by = self.keys(source, 0, 2)
        columns = [(c, source.type_of(c)) for c in by]
        candidates = [c for c in source.usable() if c not in by]
        aggs = [made for made in (self.aggregation(source, candidates, columns)
                                  for _ in range(self.rng.randrange(1, 5))) if made]
        if not aggs:
            return False
        self.add({**base, "op": "agg_by", "aggs": aggs, "by": by}, columns, max(source.rows, 1))
        return True

    def build_agg_all_by(self, name: str) -> bool:
        rng = self.rng
        source = self.pick_table()
        by = self.keys(source, 0, 2)
        rest = [(c, kind) for c, kind in source.columns if c not in by]
        if not rest:
            return False
        agg = rng.choice(_AGG_ALL_DRAW)
        accepted, result = AGGREGATIONS[agg]
        if self.has_lists(source, by) and agg in ("group", "distinct"):
            return False  # arrays of arrays
        if not all(kind in accepted for _, kind in rest) and not self.chance(0.1):
            # The reference refuses the whole request for one column it cannot aggregate;
            # keep those rare and take an aggregation of every type instead.
            agg = rng.choice(["min", "max", "first", "last", "count_distinct", "unique",
                              "median"])
            accepted, result = AGGREGATIONS[agg]
        spec: dict[str, Any] = {"agg": agg, "columns": []}
        even = agg == "median"
        if agg == "pct":
            even = self.chance(0.5)
            spec["args"] = {"percentile": rng.choice([0.0, 0.25, 0.5, 0.9, 1.0]),
                            "average_evenly_divided": even}
        elif agg in ("sorted_first", "sorted_last"):
            spec["args"] = {"order_by": rng.choice([c for c, _ in rest])}
        columns = [(c, source.type_of(c)) for c in by]
        for column, kind in rest:
            predicted = result(kind) if kind in accepted else kind
            if even and kind in ("int32", "int64"):
                predicted = "float64"
            columns.append((column, predicted))
        self.add({"name": name, "op": "agg_all_by", "source": source.name, "agg": spec,
                  "by": by}, columns, max(source.rows, 1))
        return True

    def build_group_by(self, name: str) -> bool:
        source = self.pick(lambda t: not self.has_lists(t)) or self.pick_table()
        by = self.keys(source, 0, 2)
        if self.has_lists(source, by):
            return False
        # Observed on the reference: the key columns come first, in the order asked for.
        columns = [(c, source.type_of(c)) for c in by] + [
            (c, _list(kind)) for c, kind in source.columns if c not in by]
        self.add({"name": name, "op": "group_by", "source": source.name, "by": by}, columns,
                 max(source.rows, 1))
        return True

    def build_ungroup(self, name: str) -> bool:
        rng = self.rng
        source = self.pick(self.has_lists)
        if source is None:
            if not self.chance(0.1):
                return False
            source = self.pick_table()  # nothing to ungroup
            self.add({"name": name, "op": "ungroup", "source": source.name},
                     list(source.columns), source.rows)
            return True
        lists = [c for c, kind in source.columns if kind.startswith(LIST)]
        origins = {source.origin.get(c, c) for c in lists}
        chosen: list[str] = []
        if len(origins) > 1 and (source.live or not self.chance(0.15)):
            # Arrays of different lengths in one row are refused; on a refreshing table
            # the lengths can come apart in a later step.
            origin = rng.choice(sorted(origins))
            lists = [c for c in lists if source.origin.get(c, c) == origin]
            chosen = rng.sample(lists, rng.randrange(1, len(lists) + 1))
        elif self.chance(0.5):
            chosen = rng.sample(lists, rng.randrange(1, len(lists) + 1))
        taken = chosen or lists
        op: dict[str, Any] = {"name": name, "op": "ungroup", "source": source.name}
        if chosen:
            op["columns"] = chosen
        if self.chance(0.4):
            op["null_fill"] = self.chance(0.5)
        columns = [(c, kind[len(LIST):] if c in taken else kind) for c, kind in source.columns]
        self.add(op, columns, source.rows)
        return True

    # -- joins

    def matches(self, left: _Table, right: _Table) -> list[tuple[str, str]]:
        pairs = [(lc, rc) for lc in left.usable() for rc in right.usable()
                 if left.type_of(lc) == right.type_of(rc)]
        if not left.live and not right.live and self.chance(0.03):
            # Columns of different types: refused.
            pairs = [(lc, rc) for lc in left.usable() for rc in right.usable()
                     if left.type_of(lc) != right.type_of(rc)] or pairs
        return pairs

    @staticmethod
    def match(pair: tuple[str, str]) -> str:
        return pair[0] if pair[0] == pair[1] else f"{pair[0]} = {pair[1]}"

    def build_join(self, kind: str, name: str) -> bool:
        rng = self.rng
        left = self.pick_table()
        right = self.pick_table() if self.chance(0.4) else self.tables.get("r", left)
        if kind == "exact_join" and self.unique_right and self.chance(0.6):
            left = right = self.tables["r"]  # every row has exactly one match in itself
        pairs = self.matches(left, right)
        on: list[str] = []
        used_right: set[str] = set()
        stamp_column: list[tuple[str, str]] = []
        exact = [p for p in pairs if p[0] == p[1] == "K"] or pairs
        if kind in ("aj", "raj"):
            stamps = [p for p in pairs if left.type_of(p[0]) in ORDERED_STAMPS and p[0] != "K"]
            if not stamps:
                return False
            lc, rc = rng.choice([p for p in stamps if p[0] == "S"] or stamps)
            if exact and self.chance(0.7):
                key = rng.choice(exact)
                if key[0] != lc and key[1] != rc:
                    on.append(self.match(key))
                    used_right.add(key[1])
            rule = rng.choice([">=", ">"] if kind == "aj" else ["<=", "<"])
            if self.chance(0.03):
                rule = rng.choice(["<=", "<"] if kind == "aj" else [">=", ">"])  # refused
            on.append(f"{lc} {rule} {rc}")
            used_right.add(rc)
            rows = left.rows
            if rc != lc:
                stamp_column = [(rc, right.type_of(rc))]
        else:
            if exact and (kind != "join" or self.chance(0.85)):
                key = rng.choice(exact)
                on.append(self.match(key))
                used_right.add(key[1])
                if self.chance(0.15):
                    second = rng.choice(pairs)  # a second match column
                    if second[0] != key[0] and second[1] != key[1]:
                        on.append(self.match(second))
                        used_right.add(second[1])
            elif kind != "join":
                return False
            rows = left.rows * max(right.rows, 1) if kind == "join" else left.rows
            if rows > MAX_ROWS:
                return False
        joinable = [c for c, _ in right.columns if c not in used_right]
        if not joinable:
            return False
        if stamp_column and stamp_column[0][0] in dict(left.columns):
            return False  # the right as-of column would collide with a left column
        columns = list(left.columns) + stamp_column
        op: dict[str, Any] = {"name": name, "op": kind, "left": left.name, "right": right.name,
                              "on": on}
        taken = {c for c, _ in columns}
        if self.chance(0.12) and not taken & set(joinable):
            # No `joins`: every right column that is not matched on.
            columns += [(c, kind_) for c, kind_ in right.columns
                        if c in joinable and (c, kind_) not in stamp_column]
        else:
            joins = []
            for column in rng.sample(joinable, rng.randrange(1, min(3, len(joinable)) + 1)):
                out = self.fresh("J")
                joins.append(f"{out} = {column}")
                columns.append((out, right.type_of(column)))
            op["joins"] = joins
        if kind == "natural_join":
            op["type"] = rng.choice(["error_on_duplicate", "first_match", "last_match",
                                     "first_match", "last_match", "exactly_one_match"])
        self.add(op, columns, rows)
        return True

    def build_left_join(self, name: str) -> bool:
        # The reference answers every left join UNIMPLEMENTED, and nothing defined from a
        # refused table is compared, so most draws of it are passed over.
        return self.chance(0.25) and self.build_join("left_join", name)

    def build_raj(self, name: str) -> bool:
        return self.build_join("raj", name)

    def build_where_in(self, name: str) -> bool:
        rng = self.rng
        left = self.pick_table()
        right = self.pick_table() if self.chance(0.4) else self.tables.get("r", left)
        pairs = self.matches(left, right)
        if not pairs:
            return False
        keyed = [p for p in pairs if p[0] == p[1] == "K"]
        first = rng.choice(keyed) if keyed and self.chance(0.6) else rng.choice(pairs)
        chosen = [first]
        if self.chance(0.2):
            second = rng.choice(pairs)
            if second[0] != first[0] and second[1] != first[1]:
                chosen.append(second)
        op: dict[str, Any] = {"name": name, "op": "where_in", "left": left.name,
                              "right": right.name, "on": [self.match(p) for p in chosen]}
        if self.chance(0.5):
            op["inverted"] = True
        self.add(op, list(left.columns), left.rows)
        return True

    def build_range_join(self, name: str) -> bool:
        rng = self.rng
        left = self.pick(lambda t: len(t.usable(ORDERED_STAMPS)) > 1) or self.pick_table()
        right = self.tables.get("r", left) if self.chance(0.7) else self.pick_table()
        bounds = [c for c in left.usable(ORDERED_STAMPS) if c != "K"]
        if len(bounds) < 2:
            return False
        low, high = ("S", "E") if {"S", "E"} <= set(bounds) and self.chance(0.8) \
            else rng.sample(bounds, 2)
        if left.type_of(low) != left.type_of(high):
            return False
        inner = [c for c in right.usable((left.type_of(low),)) if c != "K"]
        if not inner:
            return False
        middle = "RS" if "RS" in inner and self.chance(0.8) else rng.choice(inner)
        on = []
        if ("K" in left.usable() and "K" in right.usable()
                and left.type_of("K") == right.type_of("K") and self.chance(0.6)):
            on.append("K")
        # `<-` and `->` also take the row before and after the range; the reference refuses
        # them next to a strict comparison.
        before = "<- " if self.chance(0.2) else ""
        after = " ->" if self.chance(0.2) else ""
        on.append(f"{before}{low} {rng.choice(['<=', '<=', '<'])} {middle} "
                  f"{rng.choice(['<=', '<=', '<'])} {high}{after}")
        values = [c for c, kind in right.columns
                  if c not in on and not kind.startswith(LIST) and c != middle]
        if not values:
            return False
        rows = left.rows * max(right.rows, 1)
        if rows > MAX_ROWS:
            return False
        columns = list(left.columns)
        pairs = []
        for column in rng.sample(values, rng.randrange(1, min(2, len(values)) + 1)):
            out = self.fresh("G")
            pairs.append(f"{out} = {column}")
            columns.append((out, _list(right.type_of(column))))
        agg = "group" if not self.chance(0.06) else rng.choice(["sum", "first", "max"])
        if agg != "group":  # refused; the columns would not be arrays
            columns = list(left.columns) + [(pair.split(" = ")[0], right.type_of(
                pair.split(" = ")[1])) for pair in pairs]
        self.add({"name": name, "op": "range_join", "left": left.name, "right": right.name,
                  "on": on, "aggs": [{"agg": agg, "columns": pairs}]}, columns, rows)
        return True

    def build_multi_join(self, name: str) -> bool:
        rng = self.rng
        first = self.pick(lambda t: len(t.unique) == 1)
        strict = True
        if first is None or self.chance(0.08):
            # Tables whose key is not known to be unique: refused when it is not. Only
            # static ones, where that is decided when the join is made.
            first = self.pick(lambda t: not t.live and "K" in t.usable())
            strict = False
            if first is None:
                return False
        key = first.unique[0] if strict else "K"
        key_type = first.type_of(key)

        def fits(table: _Table) -> bool:
            if strict:
                return len(table.unique) == 1 and table.type_of(table.unique[0]) == key_type
            return not table.live and dict(table.columns).get("K") == key_type

        chosen = [first]
        for _ in range(rng.choice([0, 1, 1, 2])):
            other = self.pick(fits)
            if other is not None and (other not in chosen or self.chance(0.2)):
                chosen.append(other)
        columns = [(key, key_type)]
        taken = {key}
        inputs = []
        rows = 0
        for table in chosen:
            table_key = table.unique[0] if strict else "K"
            item: dict[str, Any] = {"table": table.name,
                                    "on": [self.match((key, table_key))]}
            values = [(c, kind) for c, kind in table.columns if c != table_key]
            if self.chance(0.2) and not taken & {c for c, _ in values}:
                # No `joins`, or an empty list: every other column.
                columns += values
                if self.chance(0.3):
                    item["joins"] = []
            else:
                item["joins"] = []
                picked = rng.sample(values, rng.randrange(1, min(2, len(values)) + 1)) \
                    if values else []
                for column, kind in picked:
                    out = self.fresh("J")
                    item["joins"].append(f"{out} = {column}")
                    columns.append((out, kind))
            taken = {c for c, _ in columns}
            rows += table.rows
            inputs.append(item)
        if rows > MAX_ROWS:
            return False
        self.add({"name": name, "op": "multi_join", "inputs": inputs}, columns, max(rows, 1))
        return True

    # -- snapshots and the rest

    def build_snapshot(self, name: str) -> bool:
        source = self.pick_table()
        self.add({"name": name, "op": "snapshot", "source": source.name},
                 list(source.columns), source.rows)
        return True

    def build_snapshot_when(self, name: str) -> bool:
        rng = self.rng
        source = self.pick_table()
        trigger = (self.pick(lambda t: t.live and t is not source) if self.chance(0.7)
                   else None) or self.pick_table()
        op: dict[str, Any] = {"name": name, "op": "snapshot_when", "source": source.name,
                              "trigger": trigger.name}
        columns = list(source.columns)
        names = {c for c, _ in columns}
        if self.chance(0.1) and not names & {c for c, _ in trigger.columns}:
            columns += trigger.columns  # no stamp columns named: every trigger column
        else:
            stamps = []
            pool = [c for c, _ in trigger.columns]
            for column in rng.sample(pool, rng.randrange(1, min(2, len(pool)) + 1)):
                out = self.fresh("P")
                stamps.append(f"{out} = {column}")
                columns.append((out, trigger.type_of(column)))
            op["stamp_columns"] = stamps
        if self.chance(0.6):
            op["initial"] = True
        if self.chance(0.3):
            op["incremental"] = True
        if self.chance(0.1):
            op["history"] = True  # with `initial` or `incremental`: refused
        self.add(op, columns, min(MAX_ROWS, source.rows * 6))
        return True

    def build_meta_table(self, name: str) -> bool:
        source = self.pick_table()
        self.add({"name": name, "op": "meta_table", "source": source.name},
                 [("Name", "string"), ("DataType", "string"), ("ColumnType", "string"),
                  ("IsPartitioning", "bool")], len(source.columns))
        return True
