"""Random scenarios from a seed, and shrinking of a scenario that shows a difference.

No Java counterpart: this is the harness's fuzzer. `generate(seed, ops)` is a pure function of
its arguments: the same seed and operation set give the same scenario document on every run.

What it generates:

- inputs with random schemas over every column type of the scenario format, with nulls, NaN,
  infinities, signed zero, the extremes of each integer type and the reference's null sentinels;
  static tables, keyed input tables and append-only input tables;
- for input tables, steps that add rows, replace rows (an add with an existing key) and delete
  rows, including deletes of keys that are not there;
- a chain of operations drawn from the requested set, each with arguments that are valid for the
  columns it sees. Result column types are predicted from PARITY.md so that later operations
  get valid arguments too. A wrong prediction is not an error of the comparison: the operation
  then fails on both servers, and failing alike is a match.

What it avoids, each for a stated reason:

- Float magnitudes outside about 1e-3..1e6. PARITY.md Q1: the A1 tolerance cannot bound a sum
  with heavy cancellation, and the proposal assumes inputs within a range where it holds.
- Using a computed float column (PARITY.md A1) as a key, sort column, filter operand or formula
  operand: a difference within tolerance there could change which rows come out.
"""

from __future__ import annotations

import copy
import json
import random
import re
from dataclasses import dataclass, field
from typing import Any, Callable

from . import rules
from . import scenario as sc

T0 = 1_700_000_000_000_000_000
INT_BITS = {"int8": 8, "int16": 16, "int32": 32, "int64": 64}
INTS = tuple(INT_BITS)
FLOATS = ("float32", "float64")
NUMERIC = INTS + FLOATS
ORDERED_STAMPS = ("int32", "int64", "float64", "timestamp")  # as-of match columns
JOINS = ("natural_join", "exact_join", "join", "aj")
# The operations of the recorded generator (`_Generator`, variants "recorded"). The scenario
# for a seed depends on the set asked for and on nothing else, and the seeds in the run log
# were found with it: nothing may change what it builds for a seed.
RECORDED_OPERATIONS = frozenset({
    "where", "sort", "head", "tail", "view", "select", "update", "drop_columns", "merge",
    "agg_by", "natural_join", "exact_join", "join", "aj", "update_by",
    "lazy_update", "update_view", "slice", "flatten", "reverse", "filter", "select_distinct",
    "head_by", "tail_by",
})
# Every operation that can be generated: those above and the ones only the wide generator
# builds (`fuzz_wide._Wide`, variants "all").
OPERATIONS = RECORDED_OPERATIONS | {
    "raj", "left_join", "where_in", "range_join", "multi_join", "agg_all_by", "group_by",
    "ungroup", "snapshot", "snapshot_when", "meta_table",
}
VARIANTS = ("recorded", "all")
# Operation sets to fuzz one area at a time; `--family` on the command line.
FAMILIES: dict[str, frozenset[str]] = {
    "update_by": frozenset({"update_by"}),
    "aggregations": frozenset({"agg_by", "agg_all_by", "drop_columns"}),
    "groups": frozenset({"group_by", "ungroup", "head_by", "tail_by"}),
    "joins": frozenset({"natural_join", "exact_join", "join", "left_join"}),
    "asof": frozenset({"aj", "raj"}),
    "range_join": frozenset({"range_join"}),
    "multi_join": frozenset({"multi_join"}),
    "sets": frozenset({"where_in", "select_distinct"}),
    "snapshots": frozenset({"snapshot", "snapshot_when"}),
    "shape": frozenset({"merge", "reverse", "flatten", "slice", "head", "tail", "sort"}),
    "filters": frozenset({"filter", "where"}),
    "formulas": frozenset({"view", "select", "update", "update_view", "lazy_update",
                           "drop_columns", "meta_table"}),
    "all": OPERATIONS,
}
MAX_ROWS = 400  # a generated table is never expected to grow beyond this

_STRINGS = ["", "a", "b", "A", "aa", "ab", "z", "é", "ß", "\U0001d11e"]
_FLOAT_VALUES = [-2.5, -1.0, -0.0, 0.0, 0.5, 1.0, 1.5, 3.25, 0.1, 1000.0, 0.001]
_FLOAT_SPECIALS = ["NaN", "Infinity", "-Infinity"]


@dataclass
class _Table:
    name: str
    columns: list[tuple[str, str]]  # (name, scenario type)
    rows: int  # upper bound on the row count, over all steps
    computed: frozenset[str] = frozenset()
    # Kept by the wide generator only (`fuzz_wide`):
    live: bool = False  # derived from a ticking input, so a step can change it
    ordered: bool = False  # has a timestamp column `T` that does not decrease
    unique: tuple[str, ...] = ()  # columns whose values name at most one row
    origin: dict[str, str] = field(default_factory=dict)  # array column -> what sized it

    def type_of(self, column: str) -> str:
        return dict(self.columns)[column]

    def usable(self, types: tuple[str, ...] | None = None) -> list[str]:
        """Columns that may be used as operands: not computed, and of one of `types`; of
        any type but an array when `types` is None."""
        return [
            name for name, kind in self.columns
            if name not in self.computed
            and (not kind.startswith("list:") if types is None else kind in types)
        ]


@dataclass
class _Generator:
    rng: random.Random
    ops: tuple[str, ...]
    inputs: list[dict[str, Any]] = field(default_factory=list)
    operations: list[dict[str, Any]] = field(default_factory=list)
    tables: dict[str, _Table] = field(default_factory=dict)
    counter: int = 0
    unique_right: bool = False  # the initial rows of input `r` have distinct K values

    # ------------------------------------------------------------ values

    def value(self, kind: str, key: bool = False) -> Any:
        rng = self.rng
        if rng.random() < (0.1 if key else 0.15):
            return None
        if kind == "bool":
            return rng.random() < 0.5
        if kind in INT_BITS:
            if key:
                return rng.randrange(0, 4)
            if rng.random() < 0.12:
                bits = INT_BITS[kind]
                low, high = -(1 << (bits - 1)), (1 << (bits - 1)) - 1
                # The minimum is the reference's null sentinel; it is a legal input value.
                return rng.choice([high, high - 1, low + 1, low])
            return rng.randrange(-4, 9)
        if kind in FLOATS:
            if key:
                return rng.choice([0.0, 1.0, 2.5, -0.0, "NaN"])
            if rng.random() < 0.15:
                return rng.choice(_FLOAT_SPECIALS)
            return rng.choice(_FLOAT_VALUES)
        if kind == "string":
            return rng.choice(["a", "b", "c"]) if key else rng.choice(_STRINGS)
        if kind == "timestamp":
            if key:
                return T0 + rng.randrange(0, 3)
            if rng.random() < 0.08:
                return rng.choice([0, -1, -(1 << 63)])
            return T0 + rng.randrange(-3, 6) * rng.choice([1, 1_000_000_000])
        raise AssertionError(kind)

    def fresh(self, prefix: str) -> str:
        self.counter += 1
        return f"{prefix}{self.counter}"

    # ------------------------------------------------------------ inputs

    def mode(self) -> str:
        """How an input is created: a static table, a keyed or an append-only input table."""
        return self.rng.choice(["static", "static", "static", "keyed", "keyed", "append"])

    def make_input(self, name: str, columns: list[tuple[str, str]], key_roles: set[str],
                   mode: str, id_column: str, unique_k: bool = False) -> None:
        rng = self.rng
        doc: dict[str, Any] = {"name": name}
        keyed = mode == "keyed"
        if keyed:
            columns = [(id_column, "int32")] + columns
        if mode != "static":
            doc["ticking"] = {"key_columns": [id_column] if keyed else []}
        count = 0 if rng.random() < 0.06 else rng.randrange(1, 13)
        doc["columns"] = [{"name": n, "type": t} for n, t in columns]
        rows = [self.row(columns, key_roles, row_id if keyed else None)
                for row_id in range(1, count + 1)]
        if unique_k:
            # One row per value of K, so that a join against this table finds at most one
            # match (until a step adds a duplicate). 0.0 and -0.0 are one key in the engine.
            position = [n for n, _ in columns].index("K")
            seen: set[str] = set()
            kept = []
            for cells in rows:
                key = "0.0" if cells[position] == 0 and not isinstance(cells[position], bool) \
                    else repr(cells[position])
                if key not in seen:
                    seen.add(key)
                    kept.append(cells)
            rows = kept
        doc["rows"] = rows
        self.inputs.append(doc)
        self.tables[name] = _Table(name, list(columns), len(rows))

    def row(self, columns: list[tuple[str, str]], key_roles: set[str],
            row_id: int | None) -> list[Any]:
        cells = [self.value(kind, key=name in key_roles) for name, kind in columns]
        if row_id is not None:
            cells[0] = row_id
        return cells

    def make_inputs(self) -> None:
        rng = self.rng
        key_type = rng.choice(["int32", "string", "int64", "int16", "bool", "float64"])
        columns = [("K", key_type)]
        for index in range(rng.randrange(2, 6)):
            columns.append((f"C{index}", rng.choice(list(sc.COLUMN_TYPES))))
        mode = self.mode()
        stamp_type = None
        if "aj" in self.ops:
            stamp_type = rng.choice(ORDERED_STAMPS)
            columns.append(("S", stamp_type))
        self.make_input("t", columns, {"K"}, mode, "Id")
        if set(self.ops) & set(JOINS):
            right = [("K", key_type)]
            for index in range(rng.randrange(1, 4)):
                right.append((f"R{index}", rng.choice(list(sc.COLUMN_TYPES))))
            if stamp_type:
                right.append(("RS", stamp_type))
            self.unique_right = rng.random() < 0.6
            self.make_input("r", right, {"K"}, self.mode(), "RId", unique_k=self.unique_right)
        if "merge" in self.ops:
            # Same columns and same kind as `t`, so that the two can be merged.
            self.make_input("m", columns, {"K"}, mode, "Id")

    def make_steps(self) -> list[dict[str, Any]]:
        rng = self.rng
        ticking = [doc for doc in self.inputs if "ticking" in doc]
        if not ticking:
            return []
        live = {doc["name"]: [row[0] for row in doc["rows"]] if doc["ticking"]["key_columns"]
                else [] for doc in ticking}
        next_id = {doc["name"]: len(doc["rows"]) + 1 for doc in ticking}
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
                    # Delete: mostly keys that exist, sometimes one that does not.
                    keys = rng.sample(live[name], rng.randrange(1, min(3, len(live[name])) + 1))
                    for key in keys:
                        live[name].remove(key)
                    if rng.random() < 0.2:
                        keys.append(9999)
                    actions.append({"action": "delete", "table": name,
                                    "rows": [[key] for key in keys]})
                    continue
                rows = []
                for _ in range(rng.randrange(1, 4)):
                    if keyed and live[name] and choice < 0.65:
                        row_id = rng.choice(live[name])  # replaces an existing row
                    elif keyed:
                        row_id = next_id[name]
                        next_id[name] += 1
                        live[name].append(row_id)
                    else:
                        row_id = None
                    rows.append(self.row(columns, {"K"}, row_id))
                table.rows += len(rows)
                actions.append({"action": "add", "table": name, "rows": rows})
            steps.append({"actions": actions})
        return steps

    # ------------------------------------------------------------ operations

    def add(self, op: dict[str, Any], columns: list[tuple[str, str]], rows: int) -> None:
        self.operations.append(op)
        computed = rules.computed_columns(self.operations)[op["name"]]
        self.tables[op["name"]] = _Table(op["name"], columns, rows, computed)

    def pick_table(self) -> _Table:
        names = list(self.tables)
        # Prefer recent tables, so that chains form.
        weights = [1 + index for index in range(len(names))]
        return self.tables[self.rng.choices(names, weights)[0]]

    def literal(self, kind: str) -> str:
        rng = self.rng
        if kind in INT_BITS:
            return str(rng.randrange(-2, 5))
        if kind in FLOATS:
            return rng.choice(["0.0", "1.0", "-1.0", "0.5", "1.5", "-0.0"])
        if kind == "string":
            return "`" + rng.choice(["a", "b", "aa", "", "A"]) + "`"
        if kind == "bool":
            return rng.choice(["true", "false"])
        raise AssertionError(kind)

    def filter(self, table: _Table) -> str | None:
        rng = self.rng
        columns = table.usable()
        if not columns:
            return None
        column = rng.choice(columns)
        kind = table.type_of(column)
        shape = rng.choice(["match", "range", "condition", "null"])
        if shape == "null" or kind == "timestamp":
            other = [c for c in table.usable(("timestamp",)) if c != column]
            if kind == "timestamp" and other and rng.random() < 0.5:
                return f"{column} {rng.choice(['<', '<=', '>', '>=', '==', '!='])} {rng.choice(other)}"
            return rng.choice([f"isNull({column})", f"!isNull({column})", f"{column} == null",
                               f"{column} != null"])
        if shape == "match":
            if kind != "bool" and rng.random() < 0.3:
                word = rng.choice(["in", "not in"])
                return f"{column} {word} {self.literal(kind)}, {self.literal(kind)}"
            return f"{column} {rng.choice(['==', '!='])} {self.literal(kind)}"
        if shape == "range" and kind != "bool":
            return f"{column} {rng.choice(['<', '<=', '>', '>='])} {self.literal(kind)}"
        # Condition filters: column against column, arithmetic, conjunctions.
        group = NUMERIC if kind in NUMERIC else (kind,)
        other = rng.choice(table.usable(group))
        compare = rng.choice(["<", "<=", ">", ">=", "==", "!="])
        if kind == "bool":
            compare = rng.choice(["==", "!="])
        if kind in NUMERIC and rng.random() < 0.4:
            return f"{column} + 1 {compare} {other}"
        if kind in NUMERIC and rng.random() < 0.3:
            return f"{column} {compare} {other} && !isNull({column})"
        if kind in NUMERIC and rng.random() < 0.3:
            return f"{column} {compare} {other} || {other} > 0"
        return f"{column} {compare} {other}"

    def condition(self, table: _Table, depth: int = 0) -> dict[str, Any] | None:
        """A structured filter condition of the kinds Java takes on every column type.

        Left out because Java refuses them: a string or boolean literal in a range
        comparison, NaN and infinite literals, and the NaN test.
        """
        rng = self.rng
        columns = table.usable()
        if not columns:
            return None
        roll = rng.random()
        if depth < 2 and roll < 0.2:
            inner = self.condition(table, depth + 1)
            return {"not": inner} if inner else None
        if depth < 2 and roll < 0.4:
            both = [c for c in (self.condition(table, depth + 1) for _ in range(2)) if c]
            return {rng.choice(["and", "or"]): both} if both else None
        column = rng.choice(columns)
        kind = table.type_of(column)
        if kind not in NUMERIC or roll < 0.5:
            return {"is_null": column}
        if kind in INT_BITS:
            literal = lambda: {"long": rng.randrange(-2, 5)}  # noqa: E731
        else:
            literal = lambda: {"literal": rng.choice([0.0, 1.0, -1.0, 0.5, 1.5])}  # noqa: E731
        if kind in INT_BITS and roll < 0.65:
            return {"in": {"target": {"column": column},
                           "candidates": [literal() for _ in range(rng.randrange(1, 4))]}}
        same = [c for c in columns if c != column and table.type_of(c) == kind]
        other = {"column": rng.choice(same)} if same and roll > 0.9 else literal()
        return {"compare": {"op": rng.choice(["<", "<=", ">", ">="]),
                            "lhs": {"column": column}, "rhs": other}}

    def formula(self, table: _Table) -> tuple[str, str] | None:
        """One formula `(expression, predicted type)` over the usable columns of `table`."""
        rng = self.rng
        columns = table.usable()
        if not columns:
            return None
        a = rng.choice(columns)
        kind = table.type_of(a)
        if kind in NUMERIC:
            others = table.usable(NUMERIC)
            b = rng.choice(others)
            b_kind = table.type_of(b)
            shape = rng.choice(["binary", "binary", "literal", "divide", "negate", "cast",
                                "compare", "null", "remainder", "plain"])
            if shape == "binary":
                return f"{a} {rng.choice(['+', '-', '*'])} {b}", _promote(kind, b_kind)
            if shape == "literal":
                if rng.random() < 0.5:
                    return f"{a} {rng.choice(['+', '-', '*'])} {rng.randrange(1, 4)}", \
                        _promote(kind, "int32")
                return f"{a} {rng.choice(['+', '-', '*'])} 1.5", "float64"
            if shape == "divide":
                return f"{a} / {b}", _divide(kind, b_kind)
            if shape == "negate" and kind not in ("int8", "int16"):
                return f"-{a}", kind
            if shape == "cast":
                target = rng.choice(["int", "long", "double"])
                return f"({target}) {a}", {"int": "int32", "long": "int64",
                                           "double": "float64"}[target]
            if shape == "compare":
                return f"{a} {rng.choice(['<', '<=', '>', '>=', '==', '!='])} {b}", "bool"
            if shape == "remainder":
                if rng.random() < 0.85:
                    return f"{a} % {rng.randrange(2, 5)}", _promote(kind, "int32")
                return f"{a} % {b}", _promote(kind, b_kind)  # integer % 0 fails in Java
        if kind == "string" and rng.random() < 0.4:
            return f"{a} + `x`", "string"
        if kind == "bool" and rng.random() < 0.4:
            others = table.usable(("bool",))
            return f"{a} {rng.choice(['==', '!='])} {rng.choice(others)}", "bool"
        if rng.random() < 0.3:
            return f"isNull({a})", "bool"
        return a, kind

    def build(self, kind: str) -> bool:
        """Appends one operation of `kind`; False when no valid arguments exist."""
        rng = self.rng
        name = self.fresh("o")
        if kind == "merge":
            groups: dict[tuple, list[_Table]] = {}
            for table in self.tables.values():
                groups.setdefault(tuple(table.columns), []).append(table)
            group = rng.choice(list(groups.values()))
            sources = [rng.choice(group) for _ in range(rng.randrange(2, 4))]
            rows = sum(s.rows for s in sources)
            if rows > MAX_ROWS:
                return False
            computed_sets = {s.computed for s in sources}
            if len(computed_sets) > 1:
                return False  # a column must be computed in all sources or in none
            self.add({"name": name, "op": "merge", "sources": [s.name for s in sources]},
                     list(sources[0].columns), rows)
            return True
        if kind in JOINS:
            return self.build_join(kind, name)

        source = self.pick_table()
        base = {"name": name, "op": kind, "source": source.name}
        if kind == "where":
            filters = [f for f in (self.filter(source) for _ in range(rng.randrange(1, 3))) if f]
            if not filters:
                return False
            self.add({**base, "filters": filters}, list(source.columns), source.rows)
        elif kind == "sort":
            usable = source.usable()
            if not usable:
                return False
            chosen = rng.sample(usable, rng.randrange(1, min(3, len(usable)) + 1))
            by = [{"column": c, "direction": rng.choice(["ascending", "descending"])}
                  for c in chosen]
            self.add({**base, "by": by}, list(source.columns), source.rows)
        elif kind in ("head", "tail"):
            rows = rng.randrange(0, 8)
            self.add({**base, "rows": rows}, list(source.columns), min(rows, source.rows))
        elif kind == "drop_columns":
            if len(source.columns) < 2:
                return False
            names = [c for c, _ in source.columns]
            dropped = rng.sample(names, rng.randrange(1, min(3, len(names) - 1) + 1))
            self.add({**base, "columns": dropped},
                     [c for c in source.columns if c[0] not in dropped], source.rows)
        elif kind in ("flatten", "reverse"):
            self.add(base, list(source.columns), source.rows)
        elif kind == "slice":
            # Positions from the start, from the end, or one of each. Java refuses a slice
            # that ends before it starts, and nothing downstream of a refusal is compared.
            first = rng.randrange(0, 6)
            last = first + rng.randrange(0, 6)
            start, stop = rng.choice([(first, last), (-last, -first), (first, -rng.randrange(1, 4))])
            self.add({**base, "start": start, "stop": stop}, list(source.columns), source.rows)
        elif kind == "filter":
            conditions = [c for c in (self.condition(source) for _ in range(rng.randrange(1, 3)))
                          if c]
            if not conditions:
                return False
            self.add({**base, "conditions": conditions}, list(source.columns), source.rows)
        elif kind == "select_distinct":
            usable = source.usable()
            if not usable:
                return False
            chosen = self.keys(source, 1, 2)
            self.add({**base, "columns": chosen},
                     [(c, source.type_of(c)) for c in chosen], source.rows)
        elif kind in ("head_by", "tail_by"):
            by = self.keys(source, 1, 2)
            # Java refuses head_by and tail_by when every column is a key column.
            if not by or len(by) == len(source.columns):
                return False
            # Observed on the reference: the key columns come first.
            columns = ([(c, source.type_of(c)) for c in by]
                       + [c for c in source.columns if c[0] not in by])
            self.add({**base, "rows": rng.randrange(1, 4), "by": by}, columns, source.rows)
        elif kind in ("view", "select", "update", "lazy_update", "update_view"):
            keeps = kind in ("update", "lazy_update", "update_view")
            formulas, columns = [], (list(source.columns) if keeps else [])
            for _ in range(rng.randrange(1, 4)):
                made = self.formula(source)
                if made is None:
                    continue
                out = self.fresh("X")
                formulas.append(f"{out} = {made[0]}")
                columns.append((out, made[1]))
            if not formulas:
                return False
            self.add({**base, "formulas": formulas}, columns, source.rows)
        elif kind == "agg_by":
            return self.build_agg(base, source)
        elif kind == "update_by":
            numeric = source.usable(NUMERIC)
            if not numeric:
                return False
            chosen = rng.sample(numeric, rng.randrange(1, min(3, len(numeric)) + 1))
            columns = list(source.columns)
            pairs = []
            for column in chosen:
                out = self.fresh("U")
                pairs.append(f"{out} = {column}")
                kind_in = source.type_of(column)
                # Observed on the reference: a cumulative sum of float is double, of any
                # integer type long.
                columns.append((out, "float64" if kind_in in FLOATS else "int64"))
            by = self.keys(source, 0, 1, exclude=set(chosen))
            self.add({**base, "ops": [{"op": "cum_sum", "columns": pairs}], "by": by},
                     columns, source.rows)
        else:
            raise AssertionError(kind)
        return True

    def keys(self, table: _Table, low: int, high: int, exclude: set[str] = frozenset()) -> list[str]:
        usable = [c for c in table.usable() if c not in exclude]
        # Prefer the low-cardinality key column when there is one.
        usable.sort(key=lambda c: c != "K")
        count = min(self.rng.randrange(low, high + 1), len(usable))
        if count and self.rng.random() < 0.7:
            return usable[:count]
        return self.rng.sample(usable, count)

    def build_agg(self, base: dict[str, Any], source: _Table) -> bool:
        rng = self.rng
        by = self.keys(source, 0, 2)
        columns = [(c, source.type_of(c)) for c in by]
        aggs: list[dict[str, Any]] = []
        candidates = [c for c in source.usable() if c not in by]
        for _ in range(rng.randrange(1, 4)):
            agg = rng.choice(["sum", "count", "min", "max", "first", "last", "avg"])
            if agg == "count":
                out = self.fresh("N")
                aggs.append({"agg": "count", "column": out})
                columns.append((out, "int64"))
                continue
            wanted = NUMERIC if agg in ("sum", "avg") else None
            pool = [c for c in candidates if wanted is None or source.type_of(c) in wanted]
            if agg in ("sum", "avg") and rng.random() < 0.04:
                pool = candidates  # now and then an aggregation the reference rejects
            if not pool:
                continue
            column = rng.choice(pool)
            kind = source.type_of(column)
            out = self.fresh("A")
            aggs.append({"agg": agg, "columns": [f"{out} = {column}"]})
            if agg == "avg":
                columns.append((out, "float64"))
            elif agg == "sum":
                columns.append((out, kind if kind in FLOATS else "int64"))
            else:
                columns.append((out, kind))
        if not aggs:
            return False
        self.add({**base, "op": "agg_by", "aggs": aggs, "by": by}, columns, max(source.rows, 1))
        return True

    def build_join(self, kind: str, name: str) -> bool:
        rng = self.rng
        left = self.pick_table()
        right = self.pick_table() if rng.random() < 0.4 else self.tables.get("r", left)
        needs_one = kind == "exact_join"
        if needs_one and self.unique_right and rng.random() < 0.6:
            # Exactly one match for every left row is rare between unrelated tables. A table
            # with unique keys joined to itself has it, so most exact joins can succeed.
            left = right = self.tables["r"]
        pairs = [
            (lc, rc)
            for lc in left.usable() for rc in right.usable()
            if left.type_of(lc) == right.type_of(rc)
        ]
        on: list[str] = []
        used_right: set[str] = set()
        stamp_column: list[tuple[str, str]] = []
        exact = [p for p in pairs if p[0] == p[1] == "K"] or pairs
        if kind == "aj":
            stamps = [p for p in pairs if left.type_of(p[0]) in ORDERED_STAMPS and p[0] != "K"]
            if not stamps:
                return False
            lc, rc = rng.choice([p for p in stamps if p[0] == "S"] or stamps)
            if exact and rng.random() < 0.7:
                key = rng.choice(exact)
                if key[0] != lc and key[1] != rc:
                    on.append(key[0] if key[0] == key[1] else f"{key[0]} = {key[1]}")
                    used_right.add(key[1])
            on.append(f"{lc} {rng.choice(['>=', '>'])} {rc}")
            used_right.add(rc)
            rows = left.rows
            if rc != lc:
                # Observed on the reference: the right table's as-of column is part of the
                # result when its name differs from the left one.
                stamp_column = [(rc, right.type_of(rc))]
        else:
            if exact and (kind != "join" or rng.random() < 0.85):
                key = rng.choice(exact)
                on.append(key[0] if key[0] == key[1] else f"{key[0]} = {key[1]}")
                used_right.add(key[1])
            elif kind != "join":
                return False
            rows = left.rows * max(right.rows, 1) if kind == "join" else left.rows
            if rows > MAX_ROWS:
                return False
        joinable = [c for c, _ in right.columns if c not in used_right]
        if not joinable:
            return False
        chosen = rng.sample(joinable, rng.randrange(1, min(3, len(joinable)) + 1))
        if stamp_column and stamp_column[0][0] in dict(left.columns):
            return False  # the right as-of column would collide with a left column
        columns = list(left.columns) + stamp_column
        joins = []
        for column in chosen:
            out = self.fresh("J")
            joins.append(f"{out} = {column}")
            columns.append((out, right.type_of(column)))
        op: dict[str, Any] = {"name": name, "op": kind, "left": left.name, "right": right.name,
                              "on": on, "joins": joins}
        if kind == "natural_join":
            op["type"] = rng.choice(["error_on_duplicate", "first_match", "last_match",
                                     "first_match", "last_match", "exactly_one_match"])
        self.add(op, columns, rows)
        return True


def _promote(a: str, b: str) -> str:
    """PARITY.md, "Arithmetic: promotion": the result type of `a op b` for `+ - * %`."""
    if "float64" in (a, b):
        return "float64"
    if "float32" in (a, b):
        return "float32"
    if "int64" in (a, b):
        return "int64"
    return "int32"


def _divide(a: str, b: str) -> str:
    """PARITY.md: `/` is double, except float/float and integer/float, which are float."""
    if b == "float32" and a != "float64":
        return "float32"
    return "float64"


def default_variants(ops: frozenset[str] | set[str]) -> str:
    """ "recorded" for a set of operations the recorded generator builds, so that a seed
    from the run log is the scenario it was; "all" for a set with a newer operation."""
    return "recorded" if set(ops) <= RECORDED_OPERATIONS else "all"


def generate(seed: int, ops: frozenset[str] | set[str],
             variants: str | None = None) -> tuple[dict[str, Any], dict[str, list]]:
    """The scenario document for `seed` over `ops`, and the predicted columns of each table.

    `ops` are harness operation names (`scenario.OPERATIONS`). `variants` chooses the
    generator: "recorded" builds what the fuzzer built when the seeds in the run log were
    found, "all" every operation and variant of `fuzz_wide`; None is `default_variants`.
    """
    variants = variants or default_variants(ops)
    if variants not in VARIANTS:
        raise sc.ScenarioError(f"variants must be one of {', '.join(VARIANTS)}")
    unknown = sorted(set(ops) - OPERATIONS)
    if unknown:
        raise sc.ScenarioError(
            f"the fuzzer does not generate {', '.join(unknown)}; it generates "
            f"{', '.join(sorted(OPERATIONS))}")
    chosen = tuple(sorted(ops))
    if not chosen:
        raise sc.ScenarioError("fuzz needs at least one table operation")
    if variants == "recorded" and not set(ops) <= RECORDED_OPERATIONS:
        raise sc.ScenarioError(
            f"the recorded generator does not build "
            f"{', '.join(sorted(set(ops) - RECORDED_OPERATIONS))}; use the variants \"all\"")
    # A string seed is hashed by `random` in a way that does not vary between runs.
    if variants == "all":
        from .fuzz_wide import _Wide  # imported here: it imports this module

        gen: _Generator = _Wide(
            random.Random(f"oxhaven-fuzz-all:{seed}:{','.join(chosen)}"), chosen)
    else:
        gen = _Generator(random.Random(f"oxhaven-fuzz:{seed}:{','.join(chosen)}"), chosen)
    gen.make_inputs()
    steps = gen.make_steps()
    wanted = gen.rng.randrange(1, 6)
    for _ in range(wanted * 4):
        if len(gen.operations) >= wanted:
            break
        gen.build(gen.rng.choice(chosen))
    # When nothing fit (for example a join with no column of a common type) the scenario
    # compares its first input table as it is, so that it never uses an operation outside `ops`.
    doc: dict[str, Any] = {
        "format": sc.FORMAT,
        "name": f"fuzz_{seed}",
        "description": f"Generated by the fuzzer from seed {seed} over {', '.join(chosen)}.",
        "inputs": gen.inputs,
        "operations": gen.operations,
        "results": [{"table": op["name"]} for op in gen.operations] or [{"table": "t"}],
    }
    if steps:
        doc["steps"] = steps
    predicted = {name: list(table.columns) for name, table in gen.tables.items()}
    return doc, predicted


# ---------------------------------------------------------------- shrinking

_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _valid(doc: dict[str, Any]) -> bool:
    try:
        sc.parse(doc)
    except sc.ScenarioError:
        return False
    return True


def _references(op: dict[str, Any]) -> set[str]:
    return set(sc.references(op))


def _prune(doc: dict[str, Any]) -> dict[str, Any]:
    """Drops operations no result depends on, inputs nothing uses, and steps left empty."""
    doc = copy.deepcopy(doc)
    needed = {r["table"] for r in doc["results"]}
    for op in reversed(doc["operations"]):
        if op["name"] in needed:
            needed |= _references(op)
    doc["operations"] = [op for op in doc["operations"] if op["name"] in needed]
    doc["inputs"] = [i for i in doc["inputs"] if i["name"] in needed]
    kept = {i["name"] for i in doc["inputs"]}
    steps = []
    for step in doc.get("steps", []):
        actions = [a for a in step["actions"] if a["table"] in kept and a["rows"]]
        if actions:
            steps.append({"actions": actions})
    if steps:
        doc["steps"] = steps
    else:
        doc.pop("steps", None)
    return doc


def _words(doc: dict[str, Any]) -> set[str]:
    """Every identifier that appears in an operation argument."""
    found: set[str] = set()

    def walk(value: Any) -> None:
        if isinstance(value, str):
            found.update(_WORD.findall(value))
        elif isinstance(value, dict):
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    for op in doc["operations"]:
        walk({k: v for k, v in op.items() if k not in ("name", "op")})
    return found


def _candidates(doc: dict[str, Any]):
    """Smaller variants of `doc`, most drastic first. Not all are valid scenarios."""

    def variant(change: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        new = copy.deepcopy(doc)
        change(new)
        return _prune(new)

    # One result only.
    if len(doc["results"]) > 1:
        for index in range(len(doc["results"])):
            yield variant(lambda d, i=index: d.update(results=[d["results"][i]]))
    # Fewer steps: none, then drop from the end, then each one.
    steps = doc.get("steps", [])
    if steps:
        yield variant(lambda d: d.pop("steps"))
        for keep in range(len(steps) - 1, 0, -1):
            yield variant(lambda d, k=keep: d.update(steps=d["steps"][:k]))
        for index in range(len(steps)):
            yield variant(lambda d, i=index: d["steps"].pop(i))
        for s, step in enumerate(steps):
            for a, action in enumerate(step["actions"]):
                yield variant(lambda d, s=s, a=a: d["steps"][s]["actions"].pop(a))
                for r in range(len(action["rows"])):
                    yield variant(
                        lambda d, s=s, a=a, r=r: d["steps"][s]["actions"][a]["rows"].pop(r))
    # A ticking input with no steps left can be a static table.
    for index, item in enumerate(doc["inputs"]):
        if "ticking" in item and not any(
            a["table"] == item["name"] for st in steps for a in st["actions"]
        ):
            yield variant(lambda d, i=index: d["inputs"][i].pop("ticking"))
    # Fewer operations: bypass an operation by pointing its users at its source.
    for index, op in enumerate(doc["operations"]):
        source = op.get("source") or op.get("left") or (op.get("sources") or [None])[0]
        if source is None and op.get("inputs"):
            source = op["inputs"][0]["table"]
        if source is None:
            continue

        def bypass(d: dict[str, Any], i: int = index, name: str = op["name"],
                   source: str = source) -> None:
            d["operations"].pop(i)
            for other in d["operations"]:
                for key in sc.TABLE_KEYS:
                    if other.get(key) == name:
                        other[key] = source
                if "sources" in other:
                    other["sources"] = [source if s == name else s for s in other["sources"]]
                for item in other.get("inputs", ()):
                    if item["table"] == name:
                        item["table"] = source
            for result in d["results"]:
                if result["table"] == name:
                    result["table"] = source
            # A result must be an operation's table, not a bare input, to mean anything;
            # parse accepts it, and the candidate is kept only if it still shows a difference.

        yield variant(bypass)
    # Fewer arguments.
    for index, op in enumerate(doc["operations"]):
        for key in ("filters", "formulas", "aggs", "by", "columns", "sources", "joins", "on",
                    "ops", "inputs", "conditions", "stamp_columns"):
            if isinstance(op.get(key), list) and len(op[key]) > 1:
                for position in range(len(op[key])):
                    yield variant(lambda d, i=index, k=key, p=position: d["operations"][i][k].pop(p))
        if op["op"] in ("agg_by", "update_by") and len(op["by"]) == 1:
            yield variant(lambda d, i=index: d["operations"][i].update(by=[]))
    # Fewer rows: halves first, then one at a time.
    for index, item in enumerate(doc["inputs"]):
        count = len(item["rows"])
        if count > 1:
            half = count // 2
            yield variant(lambda d, i=index, h=half: d["inputs"][i].update(
                rows=d["inputs"][i]["rows"][:h]))
            yield variant(lambda d, i=index, h=half: d["inputs"][i].update(
                rows=d["inputs"][i]["rows"][h:]))
        for r in range(count):
            yield variant(lambda d, i=index, r=r: d["inputs"][i]["rows"].pop(r))
    # Fewer columns: one that no operation names and that is not a key.
    words = _words(doc)
    for index, item in enumerate(doc["inputs"]):
        keys = set(item.get("ticking", {}).get("key_columns", []))
        for position, column in enumerate(item["columns"]):
            if column["name"] in words or column["name"] in keys or len(item["columns"]) == 1:
                continue

            def drop(d: dict[str, Any], i: int = index, p: int = position,
                     name: str = item["name"]) -> None:
                d["inputs"][i]["columns"].pop(p)
                for row in d["inputs"][i]["rows"]:
                    row.pop(p)
                for step in d.get("steps", []):
                    for action in step["actions"]:
                        if action["table"] == name and action["action"] == "add":
                            for row in action["rows"]:
                                row.pop(p)

            yield variant(drop)


def _size(doc: dict[str, Any]) -> tuple[int, ...]:
    """What shrinking minimizes: steps, operations, rows, columns, argument text."""
    steps = doc.get("steps", [])
    return (
        len(steps),
        len(doc["operations"]),
        sum(len(i["rows"]) for i in doc["inputs"])
        + sum(len(a["rows"]) for s in steps for a in s["actions"]),
        sum(len(i["columns"]) for i in doc["inputs"]),
        len(doc["inputs"]),
        sum(1 for i in doc["inputs"] if "ticking" in i),
        len(doc["results"]),
        len(json.dumps(doc["operations"])),
    )


def shrink(
    doc: dict[str, Any],
    still_differs: Callable[[dict[str, Any]], bool],
    max_runs: int = 200,
) -> tuple[dict[str, Any], int]:
    """The smallest variant of `doc` found for which `still_differs` holds, and the runs used.

    Greedy: takes the first smaller valid variant that still differs and starts over from it,
    until no variant differs or `max_runs` calls of `still_differs` are spent.
    """
    current = _prune(doc)
    if not _valid(current) or not still_differs(current):
        current = copy.deepcopy(doc)
    runs = 0
    tried: set[str] = set()
    progress = True
    while progress and runs < max_runs:
        progress = False
        for candidate in _candidates(current):
            text = json.dumps(candidate, sort_keys=True)
            if text in tried or _size(candidate) >= _size(current) or not _valid(candidate):
                continue
            tried.add(text)
            if runs >= max_runs:
                break
            runs += 1
            if still_differs(candidate):
                current = candidate
                progress = True
                break
    return current, runs
