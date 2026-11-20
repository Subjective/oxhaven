"""The fuzzer's generator of the third parity pass (`fuzz.generate(..., variants="third")`):
what tasks 11.4 and 11.5 started serving, and chains of as-of joins drawn on purpose.

No Java counterpart: this is the harness's fuzzer. `fuzz_second._Second` is not changed by
anything here and its families keep their scenarios; this generator draws from a random
stream of its own, so a seed means another scenario than it does there. The findings of its
first runs are `tasks/12.1-fuzzer-coverage.md`.

What it adds to `fuzz_second._Second`:

- chains of as-of joins (`build_asof`): an `aj` or `raj` whose left table, right table or
  both are results of earlier as-of joins, over static and refreshing inputs. The older
  generators build such chains only where their draw of tables happens to make one. A join
  down a chain matches on the stamp column the chain already carries (`RS >= RS`), which the
  older generators pass over because a right stamp of another name would collide.
- vector columns (the `list:<type>` columns that `group_by`, the `group` aggregation and
  `rolling_group` make) offered where the older generators offer only plain columns:
  - `update_by`: `cum_min`, `cum_max`, `rolling_min_*` and `rolling_max_*` of a vector
    column (`vector_min_max`), buckets by a vector column (`keys`), and count-where filters
    that read one (`vector_filter`);
  - `agg_by`: group-by columns that are vectors (`keys`), the `distinct` aggregation of a
    vector column, and count-where filters that read one (`vector_aggregation`);
  - `select_distinct` of vector columns (`keys`).
- `rolling_formula_*` over a condition between a window and a slice of one, or between two
  windows (`condition_formula`): read by position, sliced, and handed to the functions that
  read a vector; with a parameter token and, with `formula_param` null, as a formula over
  several columns.

A table with a column of arrays of arrays (`distinct` of a vector column) is one the
comparison rules cannot read; it is compared through an `ungroup` of that column
(`results`), as `fuzz_times` compares a table with a `Period` column through a view.

As in the older generators, requests that are valid when made and fail the table in a later
update cycle are kept out, because a failed table cannot be compared (`oracle/README.md`,
Known limits). For what this module adds those are, on a refreshing table:

- a count-where filter that calls a method of a vector column, or reads an element of one,
  unless the column is one that never holds a null or an empty vector (`solid`): the
  reference fails the filter on a null vector (`NullPointerException`);
- a rolling formula that reads a position its window may not have: on a refreshing table
  only reads of position 0 of a window in ticks that holds the row itself, of slices that
  start at 0, and the functions that take any vector;
- `len` of a window of strings, which the reference fails for every row
  (`ClassCastException`): on a static table only, where it is a refusal of the request.

`RESTRICTIONS` lists what the generator steps around because an owner item or a finding of
`tasks/12.1-fuzzer-coverage.md` covers it; each is marked where it is made.

Not generated, each for a stated reason:

- a rolling formula whose value is a slice of its window: owner item 7.1 (the reference
  stores a view of its buffer; Oxhaven refuses).
- rolling formulas over vector or array columns, and `binSearchIndex` of a slice: Oxhaven's
  expression crate does not compile them yet (`tasks/11.4-updateby-refusals.md`, Handover,
  R13, R15 and R9).
- big number columns in aggregations (`tasks/11.5-agg-refusals.md`, A19 and A20): the
  scenario format has no `BigDecimal` input column.
"""

from __future__ import annotations

import re
from typing import Any

from .fuzz import FLOATS, INT_BITS, NUMERIC, ORDERED_STAMPS, _Table
from .fuzz_second import _Second
from .fuzz_wide import LIST, _Wide

# What the generator steps around, and where: an open owner item, or a finding of
# tasks/12.1-fuzzer-coverage.md that is an Oxhaven gap or not a difference to decide. What
# PARITY.md records as allowed (A13, A19 to A24) is drawn, and the comparison accepts it
# through the entries of oracle/fuzz-known-findings.json named for it.
RESTRICTIONS: dict[str, str] = {
    "V1": "vector_min_max, vector_aggregation: no minimum, maximum or distinct of a vector of "
          "booleans or timestamps (owner item 3.3: the reference's result has no component "
          "type, so a client receives strings)",
    "V3": "keys: no group-by or select_distinct column that is a vector of float or double "
          "(Oxhaven refuses them; owner item 7.6 and PARITY.md A19 cover update_by buckets "
          "only, which are drawn)",
    "V7": "condition_formula: no slice (subVector) of a window of strings (F4: Oxhaven's "
          "expression crate does not compile a method of a vector of objects yet)",
    "V8": "vector_filter: no count-where filter that reads an element of a vector of "
          "booleans (F5: Oxhaven's filter compiler refuses it)",
    "V13": "keys: on a refreshing table no group-by or select_distinct column that is a "
           "vector of booleans or timestamps (F10: the reference's result column is then a "
           "vector of strings; owner item 3.3)",
    "V12": "filter: no filter on the row key `k` over a refreshing sort result or a table "
           "made from one (F9: the servers give the rows of such a result different row keys, "
           "which PARITY.md A4 does not compare)",
}
_INTEGERS = tuple(INT_BITS)
# Element types of the vectors each request takes; see RESTRICTIONS for what is left out.
_COMPARED = _INTEGERS + ("string",) + FLOATS  # V1
_KEYED = _INTEGERS + ("string", "bool", "timestamp")  # V3
_KEYED_LIVE = _INTEGERS + ("string",)  # V13
_BUCKETS = _KEYED + FLOATS
_DISTINCT = _INTEGERS + ("string",) + FLOATS  # V1
# Operations whose result passes the cells of its source's vector columns on unchanged.
_CELL_KEEPING = frozenset({
    "where", "filter", "head", "tail", "slice", "flatten", "reverse", "sort", "update",
    "update_view", "lazy_update", "update_by", "where_in", "snapshot",
})
_VECTOR_OPERATIONS = ("update_by", "agg_by", "select_distinct")
_ROW_KEY = re.compile(r"\bk\b")


class _Third(_Second):
    """`fuzz_second._Second` with the forms listed in the module comment."""

    def __init__(self, rng: Any, ops: tuple[str, ...]):
        super().__init__(rng, ops)
        self.asof: list[str] = []  # the results of as-of joins, in the order made
        # Per table: its vector columns that hold neither a null nor an empty vector.
        self.solid: dict[str, set[str]] = {}
        self.rekeyed: set[str] = set()  # refreshing sort results and what is made from them

    # ------------------------------------------------------------ bookkeeping

    def add(self, op: dict[str, Any], columns: list[tuple[str, str]], rows: int) -> None:
        super().add(op, columns, rows)
        kind, name = op["op"], op["name"]
        if kind in ("aj", "raj"):
            self.asof.append(name)
        types = dict(columns)
        solid: set[str] = set()
        if kind == "group_by":
            solid = set(types) - set(op["by"])
        elif kind in ("agg_by", "select_distinct") or kind in _CELL_KEEPING:
            source = self.tables[op.get("source") or op["left"]]
            solid = {column for column in self.solid.get(source.name, ())
                     if types.get(column) == source.type_of(column)}
            if kind == "agg_by":
                solid &= set(op["by"])
                # A group that an aggregation shows has rows, unless it is asked to show
                # the groups it has none for (`_Second.build_agg` adds that afterwards).
                solid |= {pair.split("=")[0].strip() for agg in op["aggs"]
                          if agg["agg"] == "group" for pair in agg["columns"]}
        self.solid[name] = {column for column in solid if types[column].startswith(LIST)}
        first = self.tables[op.get("source") or op.get("left") or name]
        if self.tables[name].live and (kind == "sort" or first.name in self.rekeyed):
            self.rekeyed.add(name)  # V12

    def results(self) -> list[str]:
        """The tables to compare: each operation's, or for a table with a column of arrays
        of arrays an `ungroup` of that column."""
        names = []
        for op in list(self.operations):
            table = self.tables[op["name"]]
            nested = [c for c, kind in table.columns if kind.startswith(LIST + LIST)]
            if not nested:
                names.append(op["name"])
                continue
            names.append(op["name"] + "v")
            self.add({"name": names[-1], "op": "ungroup", "source": op["name"],
                      "columns": nested[:1]},
                     [(c, kind[len(LIST):] if c == nested[0] else kind)
                      for c, kind in table.columns], table.rows)
        return names

    @staticmethod
    def vectors(table: _Table, elements: tuple[str, ...]) -> list[str]:
        """The vector columns of `table` whose elements are of one of `elements` and are
        not computed (`fuzz`, module comment: a float within the tolerance must not decide
        which rows come out)."""
        return [name for name, kind in table.columns
                if kind.startswith(LIST) and kind[len(LIST):] in elements
                and name not in table.computed]

    def build(self, kind: str) -> bool:
        offered = any(_Wide.has_lists(table) for table in self.tables.values())
        if kind in _VECTOR_OPERATIONS and not offered and self.chance(0.8):
            # Something to offer.
            if not ("update_by" in self.ops and self.chance(0.35) and self.window_groups()):
                if "group_by" in self.ops:
                    super().build("group_by")
        elif kind == "group_by" and offered and self.chance(0.7):
            # Vector columns are there: more often an operation that takes them.
            kind = self.rng.choice([k for k in _VECTOR_OPERATIONS if k in self.ops] or [kind])
        return super().build(kind)

    def window_groups(self) -> bool:
        """An `update_by` with `rolling_group_tick` over a table whose timestamps are in
        order: vector columns in a table that a window over time can be asked of."""
        rng = self.rng
        source = self.pick(lambda table: table.ordered and not _Wide.has_lists(table))
        pool = [c for c in source.usable(_KEYED) if c != "T"] if source else []
        if not pool:
            return False
        columns = list(source.columns)
        pairs = []
        for column in rng.sample(pool, rng.randrange(1, min(2, len(pool)) + 1)):
            out = self.fresh("G")
            pairs.append(f"{out} = {column}")
            columns.append((out, LIST + source.type_of(column)))
        self.add({"name": self.fresh("o"), "op": "update_by", "source": source.name, "by": [],
                  "ops": [{"op": "rolling_group_tick", "columns": pairs,
                           "args": {"rev_ticks": rng.choice([1, 2, 3]),
                                    "fwd_ticks": rng.choice([0, 0, 1])}}]},
                 columns, source.rows)
        return True

    def filter(self, table: _Table) -> str | None:
        made = super().filter(table)
        if made is not None and table.name in self.rekeyed and _ROW_KEY.search(made):
            return "i > 1"  # V12
        return made

    def pick_table(self) -> _Table:
        if self.kind in _VECTOR_OPERATIONS and self.chance(0.6):
            with_vectors = self.pick(lambda t: bool(self.vectors(t, tuple(_KEYED) + FLOATS)))
            if with_vectors is not None:
                return with_vectors
        return super().pick_table()

    def keys(self, table: _Table, low: int, high: int,
             exclude: set[str] = frozenset()) -> list[str]:
        by = super().keys(table, low, high, exclude)
        if self.kind in _VECTOR_OPERATIONS and high > 0 and self.chance(0.5):
            # Buckets of update_by may be vectors of floats (PARITY.md A19); group-by and
            # select_distinct columns may not (V3), nor on a refreshing table vectors of
            # booleans or timestamps (V13).
            elements = _BUCKETS if self.kind == "update_by" else (
                _KEYED_LIVE if table.live else _KEYED)
            pool = [c for c in self.vectors(table, elements) if c not in exclude]
            if pool:
                column = self.rng.choice(pool)
                by = [by[0], column] if by and high > 1 and self.chance(0.4) else [column]
        return by

    # ------------------------------------------------------------ as-of chains

    def build_join(self, kind: str, name: str) -> bool:
        if kind in ("aj", "raj"):
            return self.build_asof(kind, name)
        return super().build_join(kind, name)

    def build_asof(self, kind: str, name: str) -> bool:
        """An as-of join, most often one whose left table, right table or both are results
        of earlier as-of joins."""
        rng = self.rng
        made = [self.tables[table] for table in self.asof]
        shape = rng.choice(["left", "left", "right", "right", "both", "any"]) if made else "any"

        def earlier() -> _Table:
            return rng.choices(made, [1 + index for index in range(len(made))])[0]

        left = earlier() if shape in ("left", "both") else self.pick_table()
        if shape == "right" and self.chance(0.6):
            # The shape of the chains of PARITY.md A13 (F1): a static left table and a
            # refreshing as-of result on the right.
            left = self.pick(lambda table: not table.live) or left
        if shape in ("right", "both"):
            right = earlier()
        else:
            right = self.pick_table() if self.chance(0.3) else self.tables.get("r", left)
        pairs = self.matches(left, right)
        have = dict(left.columns)
        # The right stamp column is added to the result under its own name unless the left
        # one has that name; a name the left table already has would be refused.
        stamps = [p for p in pairs if left.type_of(p[0]) in ORDERED_STAMPS and p[0] != "K"
                  and (p[0] == p[1] or p[1] not in have)]
        if not stamps:
            return False
        carried = [p for p in stamps if p[0] in ("S", "RS")]
        lc, rc = rng.choice(carried if carried and self.chance(0.8) else stamps)
        on: list[str] = []
        used_right = {rc}
        exact = [p for p in pairs if p[0] == p[1] == "K"] or pairs
        if exact and self.chance(0.7):
            key = rng.choice(exact)
            if key[0] != lc and key[1] != rc:
                on.append(self.match(key))
                used_right.add(key[1])
        rule = rng.choice([">=", ">"] if kind == "aj" else ["<=", "<"])
        if self.chance(0.02):
            rule = rng.choice(["<=", "<"] if kind == "aj" else [">=", ">"])  # refused
        on.append(f"{lc} {rule} {rc}")
        columns = list(left.columns) + ([(rc, right.type_of(rc))] if rc != lc else [])
        joinable = [c for c, _ in right.columns if c not in used_right]
        if not joinable:
            return False
        op: dict[str, Any] = {"name": name, "op": kind, "left": left.name, "right": right.name,
                              "on": on}
        taken = {c for c, _ in columns}
        if self.chance(0.1) and not taken & set(joinable):
            # No `joins`: every right column that is not matched on.
            columns += [(c, right.type_of(c)) for c in joinable]
        else:
            joins = []
            for column in rng.sample(joinable, rng.randrange(1, min(3, len(joinable)) + 1)):
                out = self.fresh("J")
                joins.append(f"{out} = {column}")
                columns.append((out, right.type_of(column)))
            op["joins"] = joins
        self.add(op, columns, left.rows)
        return True

    # ------------------------------------------------------------ vector columns

    def vector_filter(self, source: _Table) -> str | None:
        """A filter that reads a vector column, for a count where."""
        rng = self.rng
        lists = [(name, kind[len(LIST):]) for name, kind in source.columns
                 if kind.startswith(LIST) and not kind.startswith(LIST + LIST)
                 and name not in source.computed]
        if not lists:
            return None
        column, element = rng.choice(lists)
        count = rng.choice([0, 1, 1, 2, 3])
        forms = [f"{column} == null", f"{column} != null", f"isNull({column})",
                 f"len({column}) > {count}", f"len({column}) == {count}"]
        if not source.live or column in self.solid.get(source.name, ()):
            # A method of a null vector fails the reference's filter, and so does a read
            # of an element an empty vector does not have.
            forms += [f"{column}.size() > {count}", f"{column}.size() % 2 == 0",
                      f"{column}.size() > {count}"]
            if element in NUMERIC:
                literal = self.literal(element)
                forms += [f"{column}[0] {rng.choice(['>', '<=', '==', '!='])} {literal}",
                          f"isNull({column}[0])",
                          f"{column}[{column}.size() - 1] > {literal}"]
                if element in _INTEGERS:
                    forms += [f"sum({column}) > {literal}", f"max({column}) >= {literal}"]
            elif element == "string":
                forms += [f"{column}[0] == {self.literal(element)}", f"isNull({column}[0])"]
            elif element != "bool":  # V8
                forms += [f"isNull({column}[0])"]
        return rng.choice(forms)

    def vector_filters(self, source: _Table) -> list[str]:
        filters = [self.vector_filter(source)]
        if self.chance(0.3):
            other = self.vector_filter(source) if self.chance(0.5) else _Wide.filter(self, source)
            if other and not self.stops_reference(source, other):
                filters.append(other)
        return [text for text in filters if text]

    def aggregation(self, source: _Table, candidates: list[str],
                    columns: list[tuple[str, str]]) -> dict[str, Any] | None:
        if self.chance(0.35):
            made = self.vector_aggregation(source, columns)
            if made:
                return made
        return super().aggregation(source, candidates, columns)

    def vector_aggregation(self, source: _Table,
                           columns: list[tuple[str, str]]) -> dict[str, Any] | None:
        """A count where over vector columns, or the distinct vectors of one."""
        taken = {column for column, _ in columns}
        pool = [c for c in self.vectors(source, _DISTINCT) if c not in taken]  # V1
        nested = any(kind.startswith(LIST + LIST) for _, kind in columns)
        if pool and not nested and "ungroup" in self.ops and self.chance(0.5):
            # One per table: `results` reads it through an `ungroup` of that column.
            column = self.rng.choice(pool)
            out = self.fresh("A")
            columns.append((out, LIST + source.type_of(column)))
            spec: dict[str, Any] = {"agg": "distinct", "columns": [f"{out} = {column}"]}
            if self.chance(0.5):
                spec["args"] = {"include_nulls": self.chance(0.5)}
            return spec
        filters = self.vector_filters(source)
        if not filters:
            return None
        out = self.fresh("N")
        columns.append((out, "int64"))
        return {"agg": "count_where", "column": out, "args": {"filters": filters}}

    def build_agg(self, base: dict[str, Any], source: _Table) -> bool:
        if not super().build_agg(base, source):
            return False
        op = self.operations[-1]
        if "initial_groups" in op or op.get("preserve_empty"):
            # A group without rows has an empty vector.
            self.solid[op["name"]] &= set(op["by"])
        return True

    # ------------------------------------------------------------ update_by

    def window(self, source: _Table, ticks: bool = False) -> tuple[str, dict[str, Any]]:
        """A rolling window that holds the row itself: its unit and its arguments."""
        rng = self.rng
        stamps = None if ticks else self.timestamps(source)
        if stamps and self.chance(0.4):
            return "time", {"ts_col": stamps,
                            "rev_time": rng.choice(["PT0S", "PT1S", "PT2S", "PT1M"]),
                            "fwd_time": rng.choice(["PT0S", "PT0S", "PT1S"])}
        return "tick", {"rev_ticks": rng.choice([1, 2, 3, 4, 5]),
                        "fwd_ticks": rng.choice([0, 0, 1, 2])}

    def update_by_operator(self, source: _Table, columns: list[tuple[str, str]],
                           used: set[str]) -> dict[str, Any] | None:
        roll = self.rng.random()
        made = None
        if roll < 0.3:
            made = self.vector_min_max(source, columns, used)
        elif roll < 0.45:
            made = self.vector_count_where(source, columns)
        elif roll < 0.8:
            made = self.condition_formula(source, columns, used)
        return made or super().update_by_operator(source, columns, used)

    def vector_min_max(self, source: _Table, columns: list[tuple[str, str]],
                       used: set[str]) -> dict[str, Any] | None:
        rng = self.rng
        rolling = self.chance(0.5)
        pool = self.vectors(source, _COMPARED)
        if not pool:
            return None
        which = rng.choice(["min", "max"])
        args: dict[str, Any] = {}
        op = f"cum_{which}"
        if rolling:
            unit, args = self.window(source)
            op = f"rolling_{which}_{unit}"
        pairs = []
        for column in rng.sample(pool, rng.randrange(1, min(2, len(pool)) + 1)):
            out = self.fresh("U")
            pairs.append(f"{out} = {column}")
            columns.append((out, source.type_of(column)))
            used.add(column)
        spec: dict[str, Any] = {"op": op, "columns": pairs}
        if args:
            spec["args"] = args
        return spec

    def vector_count_where(self, source: _Table,
                           columns: list[tuple[str, str]]) -> dict[str, Any] | None:
        filters = self.vector_filters(source)
        if not filters:
            return None
        out = self.fresh("U")
        columns.append((out, "int64"))
        if self.chance(0.5):
            return {"op": "cum_count_where", "columns": [out], "args": {"filters": filters}}
        unit, window = self.window(source)
        return {"op": f"rolling_count_where_{unit}", "columns": [out],
                "args": {"filters": filters, **window}}

    def condition_formula(self, source: _Table, columns: list[tuple[str, str]],
                          used: set[str]) -> dict[str, Any] | None:
        """A rolling formula over a condition between a window and a slice of one, or
        between two windows of one type."""
        rng = self.rng
        pool = source.usable(NUMERIC + ("string",))
        if not pool:
            return None
        column = rng.choice(pool)
        element = source.type_of(column)
        twins = [c for c in pool if c != column and source.type_of(c) == element]
        token = not twins or self.chance(0.4)
        window = "each" if token else column
        other = window if token else rng.choice(twins)
        count = rng.choice([0, 1, 1, 2])
        conditions = [f"{window}.size() > {count}", f"isNull({window}[0])"]
        if element in NUMERIC:
            # `len` of a window of objects fails when the row is read (see below).
            conditions += [f"len({window}) > {count}"]
            conditions += [f"{window}[0] > {self.literal(element)}"] * 2
        test = rng.choice(conditions)
        # Reads that every window with the row itself in it can answer.
        forms: list[tuple[str, str]] = [
            (f"({test} ? {window} : {window}.subVector(0, 1))[0]", element),
            (f"({test} ? {window} : {other}).subVector(0, 1)[0]", element),
            (f"({test} ? {window} : {window}.subVector(0, 1)).size()", "int64"),
            (f"({test} ? {window} : ({window}.size() > 2 ? {other} : "
             f"{window}.subVector(0, 1)))[0]", element),
        ]
        if element == "string":  # V7
            # An element of a condition between windows of strings is a java.lang.Object
            # column to the reference (PARITY.md A23).
            forms = [(f"({test} ? {window} : {other}).size()", "int64"),
                     (f"({test} ? {other} : {window}).size()", "int64"),
                     (f"({test} ? {window} : ({window}.size() > 2 ? {other} : {window}))"
                      f".size()", "int64"),
                     (f"({test} ? {window} : {other})[0]", element),
                     (f"({test} ? {other} : {window})[0]", element),
                     (f"({test} ? {window} : ({window}.size() > 2 ? {other} : {window}))[0]",
                      element)]
        if element in NUMERIC:
            forms += [
                (f"first({test} ? {window} : {other})", element),
                (f"last({test} ? {window} : {window}.subVector(0, 1))", element),
                (f"max({test} ? {window} : {window}.subVector(0, 2))", element),
                (f"min({test} ? {other} : {window}.subVector(0, 2))", element),
            ]
        if element in _INTEGERS:
            forms += [(f"sum(({test} ? {window} : {other}).subVector(1, 3))", "int64"),
                      (f"sum({test} ? {window} : {window}.subVector(0, 1))", "int64")]
        if not source.live and self.chance(0.12):
            # Reads outside a short window, and `len` of a window of objects: the
            # reference fails them when the row is read, which for a static table is a
            # refusal of the request.
            forms = [(f"len({test} ? {window} : {other})", "int64")]
            if element in NUMERIC:
                forms = [(f"({test} ? {window} : {window})[1]", element),
                         (f"({test} ? {window} : {window}.subVector(1, 2))[0]", element),
                         (f"({test} ? {window} : {other}).subVector(1, 2)[0]", element),
                         (f"first({test} ? {window} : {window}.subVector(1, 3))", element)]
        formula, result = rng.choice(forms)
        # On a refreshing table only windows in ticks: a row with a null timestamp has an
        # empty window over time, and a read of its position 0 fails in the cycle.
        unit, args = self.window(source, ticks=source.live)
        out = self.fresh("U")
        columns.append((out, result))
        if token:
            used.add(column)
            return {"op": f"rolling_formula_{unit}", "columns": [f"{out} = {column}"],
                    "args": {"formula": formula, "formula_param": "each", **args}}
        used.update((column, other))
        # Without a parameter token the formula names its columns and its result; the
        # operator's own column list is not read.
        return {"op": f"rolling_formula_{unit}", "columns": [out],
                "args": {"formula": f"{out} = {formula}", "formula_param": None, **args}}
