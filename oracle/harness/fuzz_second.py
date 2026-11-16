"""The fuzzer's generator of the second parity pass (`fuzz.generate(..., variants="second")`):
what the engine gained after `fuzz_wide` was written.

No Java counterpart: this is the harness's fuzzer. `fuzz_wide._Wide` is not changed by
anything here and its families keep their scenarios; this generator draws from a random
stream of its own, so a seed means another scenario than it does there. The findings of its
first runs are `tasks/4.20-fuzz-findings-2.md`.

What it adds to `fuzz_wide._Wide`:

- formulas (`view`, `select`, `update`, `update_view`, `lazy_update`) and filters (`where`)
  with the row variables `i`, `ii` and `k`, column arrays (`C_`, `C_.size()`, `C_[0]`,
  functions of `C_`), fixed-distance reads (`C_[i-1]`, `C_[ii+2]`), arithmetic on array
  columns (`A + 1`, `A * B`), and time literals with a zone, durations and periods added to
  a timestamp;
- `agg_by` with initial groups and with `preserve_empty`;
- `partition_merge` and `partition_get`: a partitioned table of the partitioned table
  service, merged again or asked for the constituents of a key table;
- operations that only a script in the server's Python console can make
  (`scenario.SCRIPT_OPERATIONS`, `runner._scripted`): `ring_table`, `keyed_transpose`, a
  `multi_join` without match columns and a `range_join` without aggregations; a
  `multi_join` with match columns is sent that way now and then too;
- `update_by` with `rolling_formula_*`, `cum_count_where` and `rolling_count_where_*`;
- `column_statistics`.

As in `fuzz_wide`, requests that are valid when made and fail the table in a later update
cycle are kept out, because a failed table cannot be compared (`oracle/README.md`, Known
limits). For the operations of this module those are:

- arithmetic on two array columns of a refreshing table unless both come from one grouping:
  arrays of different lengths fail when read;
- a `ring_table` over a refreshing table that can modify or shift rows (the reference fails
  the ring: "Not expecting modifies or shifts"): only static tables and tables that are
  append-only by construction get one;
- a `keyed_transpose` of a refreshing table with the new-column behaviour `fail`;
- a `multi_join` without match columns of a refreshing table that can come to hold more
  than one row;
- a period of months or years added to a timestamp of a refreshing table: the reference
  fails when a row has a timestamp that is not null.

Restrictions that exist only because of a finding of `tasks/4.20-fuzz-findings-2.md` or
`tasks/4.23-fuzz-refusals.md` are marked with the finding's number where they are made
(`RESTRICTIONS` lists them).

Not generated, each for a stated reason:

- rollups and trees: their result is a view that only the script scenarios read
  (`harness/views.py`).
- `function_generated_table`, `time_table`, `now()` and the random numbers: their results
  depend on the clock or on the server's generator.
- partitioned table transforms, proxies and `partitioned_agg_by`: they take Python callables
  or return objects that are not tables; `partition_merge` and `partition_get` reach the
  partitioning itself.
- comparisons of arrays (`A == 1`): the result is a `boolean[]`, which the comparison rules
  do not know.
"""

from __future__ import annotations

import re
from typing import Any

from .fuzz import FLOATS, INT_BITS, NUMERIC, _Table, _promote
from .fuzz_wide import LIST, _Wide, _list

# Findings of tasks/4.20-fuzz-findings-2.md and tasks/4.23-fuzz-refusals.md that the generator
# steps around, and where.
RESTRICTIONS: dict[str, str] = {
    "J1": "stops_reference: no count where of update_by that compares two columns (with a "
          "boolean or timestamp column among those it reads, the reference shuts down)",
    "J2": "second_formula: no fixed-distance read in a select of anything but an input table",
    "J4": "stops_reference: no count where of update_by that compares two columns (the "
          "reference fails it when the first row is read)",
    "J5": "build_agg: no distinct of a timestamp column with preserve_empty on a refreshing "
          "table (the reference cannot write a group that lost its rows)",
    "J6": "build_agg: no initial groups of a refreshing aggregation with first and last",
    "J7": "build_agg: initial groups of a refreshing aggregation with first, last, "
          "sorted_first or sorted_last before any count only with preserve_empty (on an "
          "add-only table the reference's result fails when a group gets its first row)",
    "R5": "filter: one distance for the fixed-distance reads of a refreshing where",
    "R6": "with_arrays: no column array of an array column of booleans or timestamps (the "
          "reference sends its elements as strings) or of one a formula made (a Java array)",
}
_COLUMN_COMPARISON = re.compile(r"^([A-Za-z_]\w*) (?:<|<=|>|>=|==|!=) ([A-Za-z_]\w*)$")

_INTEGERS = tuple(INT_BITS)
# Zones the reference knows (`TimeZoneAliases`, default_time_zone_aliases.csv) and a few it
# does not, which it refuses.
_ZONES = ("UTC", "GMT", "ET", "CT", "MT", "PT", "HK", "JP", "LON", "America/New_York",
          "Europe/Paris", "Asia/Tokyo", "Australia/Sydney", "+01:00", "UTC-5",
          "SystemV/EST5", "SystemV/PST8PDT")
_NO_ZONES = ("NY", "EST", "et", "Europe/Nowhere")
# What follows the time directly: an offset, and now and then a zone in brackets after it.
_OFFSETS = ("Z", "z", "+01:00", "-05", "+05:30:00", "Z[UTC]", "+05:00[Europe/Paris]",
            "-05:00[America/New_York]", "+01:00[SystemV/EST5]", "+0100", "[Europe/Paris]")
# Instants around `fuzz.T0` (2023-11-14T22:13:20Z), and two that a clock change of the
# eastern United States skips or repeats.
_LOCAL_TIMES = ("2023-11-14T22:13:20", "2023-11-14T22:13:21", "2023-11-14T17:13:22",
                "2023-11-14T17:13:19.5", "2023-11-15T07:13:20", "2023-11-14T22:13:23.000000001",
                "2024-03-10T02:30:00", "2024-11-03T01:30:00", "2023-11-14")
_DURATIONS = ("PT1s", "PT0.5s", "PT1m", "PT1h", "PT24h", "P1d", "P1w", "PT0s")
# Periods of months and years, which the reference fails to add to an instant that is not
# null: a refusal for a static table that has such a row, and a failure in a later cycle
# for a refreshing one, so only a static table gets one.
_CALENDAR_PERIODS = ("P1m", "P1y", "P1y2m")
# Operations whose result adds rows to an append-only table and never changes or moves one.
_FORMULA_OPERATIONS = ("view", "select", "update", "update_view", "lazy_update")
_APPENDING = frozenset({"where", "update", "update_view", "view", "select", "lazy_update",
                        "drop_columns"})
# What `keyed_transpose` aggregates: exact results only, since its columns are named after
# values and the comparison rules cannot say which of them are sums.
_TRANSPOSE_AGGS = ("count", "first", "last", "min", "max", "sum", "count_distinct")
# Formulas of `rolling_formula_*` over the window's values `each`; integers only for sums
# and averages, as in `fuzz_wide.FORMULAS`.
_ROLLING_FORMULAS = {
    "sum(each)": (_INTEGERS, "int64"),
    "max(each)": (NUMERIC, None),
    "min(each)": (NUMERIC, None),
    "each.size()": (NUMERIC + ("string", "bool"), "int64"),
    "first(each)": (NUMERIC, None),
    "countDistinct(each)": (NUMERIC, "int64"),
}


class _Second(_Wide):
    """`fuzz_wide._Wide` with the operations and forms listed in the module comment."""

    def __init__(self, rng: Any, ops: tuple[str, ...]):
        super().__init__(rng, ops)
        self.appending: set[str] = set()  # append-only inputs and what only adds to them
        self.single: set[str] = set()  # tables that never hold more than one row
        self.input_names: set[str] = set()
        self.kind = ""  # the operation being built
        self.distance: str | None = None  # the distance its filters read at (R5)

    # ------------------------------------------------------------ inputs

    def make_inputs(self) -> None:
        # A second input with the key column of the first, for initial groups, key tables
        # and joins: `fuzz_wide` makes it when an operation that reads two tables is drawn.
        asked = self.ops
        self.ops = tuple(sorted(set(asked) | {"where_in"}))
        try:
            super().make_inputs()
        finally:
            self.ops = asked

    def make_input(self, name: str, columns: list[tuple[str, str]], key_roles: set[str],
                   mode: str, id_column: str, unique_k: bool = False) -> None:
        super().make_input(name, columns, key_roles, mode, id_column, unique_k)
        self.input_names.add(name)
        if mode == "append":
            self.appending.add(name)

    # ------------------------------------------------------------ bookkeeping

    def add(self, op: dict[str, Any], columns: list[tuple[str, str]], rows: int) -> None:
        super().add(op, columns, rows)
        kind, name = op["op"], op["name"]
        table = self.tables[name]
        if kind in _APPENDING and op["source"] in self.appending:
            self.appending.add(name)
        if kind in ("agg_by", "agg_all_by", "group_by") and not op["by"]:
            self.single.add(name)
        elif kind in ("head", "tail") and op["rows"] <= 1:
            self.single.add(name)
        elif kind in _APPENDING | {"sort", "reverse", "flatten"} and op["source"] in self.single:
            self.single.add(name)
        if kind in ("ring_table", "partition_merge", "partition_get"):
            table.unique = ()
            table.ordered = False
        if kind == "keyed_transpose":
            table.unique = tuple(op["row_by"])

    def static_or(self, names: set[str]):
        return lambda table: not table.live or table.name in names

    def build(self, kind: str) -> bool:
        self.kind, self.distance = kind, None
        try:
            return super().build(kind)
        finally:
            self.kind = ""

    def pick_table(self) -> _Table:
        # Array columns are rare among the tables: a formula gets one of theirs now and then.
        if self.kind in _FORMULA_OPERATIONS and self.chance(0.3):
            with_arrays = self.pick(lambda t: any(
                kind.startswith(LIST) and kind[len(LIST):] in NUMERIC and name not in t.computed
                for name, kind in t.columns))
            if with_arrays is not None:
                return with_arrays
        return super().pick_table()

    @staticmethod
    def with_arrays(table: _Table) -> list[str]:
        """The columns a column array is made of: array columns too, but for those of
        booleans and timestamps and those a formula made, which are Java arrays (R6)."""
        return table.usable() + [name for name, kind in table.columns
                                 if kind.startswith(LIST) and name not in table.computed
                                 and kind[len(LIST):] not in ("bool", "timestamp")
                                 and not re.fullmatch(r"X\d+", name)]

    # ------------------------------------------------------------ formulas and filters

    def formula(self, table: _Table) -> tuple[str, str] | None:
        if self.chance(0.3):
            return super().formula(table)
        return self.second_formula(table) or super().formula(table)

    def second_formula(self, table: _Table) -> tuple[str, str] | None:
        rng = self.rng
        columns = self.with_arrays(table)
        numeric = table.usable(NUMERIC)
        stamps = table.usable(("timestamp",))
        arrays = [name for name, kind in table.columns
                  if kind.startswith(LIST) and kind[len(LIST):] in NUMERIC
                  and name not in table.computed]
        shapes = ["row"]
        if columns:
            shapes += ["column_array"]
            if self.kind != "select" or table.name in self.input_names:  # J2
                shapes += ["shift", "shift"]
        if stamps:
            shapes += ["time", "time"]
        if arrays:
            shapes += ["array"] * 8
        if not shapes:
            return None
        shape = rng.choice(shapes)
        if shape == "row":
            if numeric and self.chance(0.4):
                a = rng.choice(numeric)
                return f"{a} + {rng.choice(['i', 'ii'])}", _promote(
                    table.type_of(a), rng.choice(["int32"]))
            return rng.choice([("i", "int32"), ("ii", "int64"), ("k", "int64"),
                               ("i % 3", "int32"), ("ii * 2", "int64"), ("i + ii", "int64"),
                               ("i == 0", "bool")])
        if shape == "shift":
            a = rng.choice(columns)
            kind = table.type_of(a)
            index = rng.choice(["i-1", "i-1", "i+1", "ii-1", "ii+2", "i-3", "i", "ii - 1",
                                "i - 1"])
            read = f"{a}_[{index}]"
            if kind in NUMERIC and self.chance(0.4):
                return f"{a} - {read}", _promote(kind, kind)
            if self.chance(0.15):
                return f"isNull({read})", "bool"
            return read, kind
        if shape == "column_array":
            a = rng.choice(columns)
            kind = table.type_of(a)
            choices = [(f"{a}_.size()", "int64"), (f"{a}_[0]", kind),
                       (f"{a}_[{a}_.size() - 1 - i]", kind), (f"{a}_[(int) (ii % 2)]", kind)]
            if kind in _INTEGERS:
                choices += [(f"sum({a}_)", "int64"), (f"max({a}_)", kind),
                            (f"min({a}_)", kind), (f"countDistinct({a}_)", "int64"),
                            (f"{a} - first({a}_)", _promote(kind, kind))]
            elif kind in FLOATS:
                choices += [(f"max({a}_)", kind), (f"min({a}_)", kind)]
            return rng.choice(choices)
        if shape == "time":
            a = rng.choice(stamps)
            form = rng.choice(["plus", "plus", "scaled", "compare", "literal"])
            duration = self.duration(table)
            if form == "plus":
                return f"{a} {rng.choice(['+', '-'])} '{duration}'", "timestamp"
            if form == "scaled":
                factor = rng.choice(["ii", "2", "i", "3"])
                return (f"{a} + '{duration}' {rng.choice(['*', '*', '/'])} {factor}"
                        if factor not in ("ii", "i") else f"{a} + '{duration}' * {factor}",
                        "timestamp")
            literal = self.instant()
            if form == "compare":
                return f"{a} {rng.choice(['<', '<=', '>', '>=', '==', '!='])} {literal}", "bool"
            return literal, "timestamp"
        a = rng.choice(arrays)
        element = table.type_of(a)[len(LIST):]
        same = [b for b in arrays if b != a and (
            not table.live or table.origin.get(a) == table.origin.get(b))]
        form = rng.choice(["scalar", "scalar", "pair", "column", "size"])
        operator = rng.choice(["+", "-", "*"])
        if form == "pair" and same:
            b = rng.choice(same + [a])
            return f"{a} {operator} {b}", _list(_promote(element, table.type_of(b)[len(LIST):]))
        if form == "column" and numeric and not table.live:
            b = rng.choice(numeric)
            return f"{a} {operator} {b}", _list(_promote(element, table.type_of(b)))
        if form == "size":
            return rng.choice([(f"{a}.size()", "int64"), (f"sum({a} * 2)", "int64")
                               if element in _INTEGERS else (f"{a}.size()", "int64")])
        if self.chance(0.2):
            return f"{a} {rng.choice(['/', '%'])} {rng.choice(['2', '3'])}", _list(
                "float64" if element in FLOATS else _promote(element, "int32"))
        literal = rng.choice(["1", "2", "1.5", "1L"])
        other = {"1.5": "float64", "1L": "int64"}.get(literal, "int32")
        if self.chance(0.3):
            return f"{literal} {operator} {a}", _list(_promote(element, other))
        return f"{a} {operator} {literal}", _list(_promote(element, other))

    def duration(self, table: _Table) -> str:
        """A duration or a period; for a static table now and then one of months or years."""
        if not table.live and self.chance(0.12):
            return self.rng.choice(_CALENDAR_PERIODS)
        return self.rng.choice(_DURATIONS)

    def instant(self) -> str:
        """A time literal with a zone; now and then one the reference refuses."""
        rng = self.rng
        if self.chance(0.15):
            return f"'{rng.choice(_LOCAL_TIMES)}{rng.choice(_OFFSETS)}'"
        zone = rng.choice(_NO_ZONES) if self.chance(0.05) else rng.choice(_ZONES)
        return f"'{rng.choice(_LOCAL_TIMES)} {zone}'"

    def filter(self, table: _Table) -> str | None:
        rng = self.rng
        if self.chance(0.45):
            return super().filter(table)
        stamps = table.usable(("timestamp",))
        columns = self.with_arrays(table)
        if stamps and self.chance(0.5):
            a = rng.choice(stamps)
            if self.chance(0.4):
                return f"{a} + '{self.duration(table)}' > {self.instant()}"
            # The comparison of a column with a literal is a filter of its own, which reads
            # the literal with another parser than a formula does.
            return f"{a} {rng.choice(['<', '<=', '>', '>=', '==', '!='])} {self.instant()}"
        if columns and self.chance(0.5):
            a = rng.choice(columns)
            kind = table.type_of(a)
            index = rng.choice(["i-1", "i+1", "ii-1"])
            if table.live:  # R5
                index = self.distance = self.distance or index
            if kind == "bool":
                return f"{a}_[{index}] == {a}"
            # A comparison of arrays is an array of booleans, which is no filter.
            if self.chance(0.3) or kind.startswith(LIST):
                return f"isNull({a}_[{index}])"
            return f"{a}_[{index}] {rng.choice(['<', '<=', '>', '!=', '=='])} {a}"
        return rng.choice(["i % 2 == 0", "ii < 3", "i > 1", "k >= 2", "ii % 3 != 1", "i == ii"])

    # ------------------------------------------------------------ aggregations

    def initial_groups(self, source: _Table, by: list[str], besides: str = "") -> str | None:
        """A table that has the columns `by` with the types `source` gives them; not the
        table named `besides`."""
        def fits(table: _Table) -> bool:
            have = dict(table.columns)
            return table.name != besides and all(
                have.get(c) == source.type_of(c) and c not in table.computed for c in by)

        other = self.pick(lambda t: fits(t) and t is not source)
        if other is None or self.chance(0.1):
            other = self.pick(fits)
        return other.name if other else None

    def build_agg(self, base: dict[str, Any], source: _Table) -> bool:
        if not super().build_agg(base, source):
            return False
        op = self.operations[-1]
        if self.chance(0.35):
            return True
        if (op["by"] or self.chance(0.1)) and self.chance(0.8):
            initial = self.initial_groups(source, op["by"], op["name"])
            if initial:
                op["initial_groups"] = initial
                self.tables[op["name"]].live |= self.tables[initial].live
        if "initial_groups" not in op or self.chance(0.4):
            op["preserve_empty"] = self.chance(0.8)
        live = self.tables[op["name"]].live
        kinds = {agg["agg"] for agg in op["aggs"]}
        if live and {"first", "last"} <= kinds:
            op.pop("initial_groups", None)  # J6
        if "initial_groups" in op and live and self.never_records(op["aggs"]):
            op["preserve_empty"] = True  # J7
        if live and op.get("preserve_empty") and any(
                agg["agg"] == "distinct" and source.type_of(
                    pair.split("=")[-1].strip()) == "timestamp"
                for agg in op["aggs"] for pair in agg.get("columns", ())):
            op.pop("initial_groups", None)  # J5
            op["preserve_empty"] = False
        return True

    @staticmethod
    def never_records(aggs: list[dict[str, Any]]) -> bool:
        """J7: whether the first of the aggregations that tells the reference when a group
        appears is a first or last, which on an add-only table tells it nothing."""
        for agg in aggs:
            if agg["agg"] == "count":
                return False
            if agg["agg"] in ("first", "last", "sorted_first", "sorted_last"):
                return True
        return False

    # ------------------------------------------------------------ partitioned tables

    def build_partition_merge(self, name: str) -> bool:
        source = self.pick_table()
        by = self.keys(source, 0 if self.chance(0.12) else 1, 2)
        op: dict[str, Any] = {"name": name, "op": "partition_merge", "source": source.name,
                              "by": by}
        columns = list(source.columns)
        if self.chance(0.3):
            op["drop_keys"] = self.chance(0.8)
            if op["drop_keys"]:
                columns = [c for c in columns if c[0] not in by]
        self.add(op, columns, source.rows)
        return True

    def build_partition_get(self, name: str) -> bool:
        rng = self.rng
        source = self.pick_table()
        by = self.keys(source, 1, 2)
        if not by:
            return False
        key = self.initial_groups(source, by)
        if key is None:
            return False
        op: dict[str, Any] = {"name": name, "op": "partition_get", "source": source.name,
                              "by": by, "key": key}
        # Anything but `permit_multiple` is refused unless the key table has one row.
        if self.chance(0.7):
            op["unique"] = "permit_multiple"
        elif self.chance(0.7):
            op["unique"] = rng.choice(["require_unique", "not_set"])
        columns = list(source.columns)
        if self.chance(0.25):
            op["drop_keys"] = self.chance(0.8)
            if op["drop_keys"]:
                columns = [c for c in columns if c[0] not in by]
        self.add(op, columns, source.rows)
        return True

    # ------------------------------------------------------------ script operations

    def build_ring_table(self, name: str) -> bool:
        rng = self.rng
        # A modify or a shift fails the reference's ring in the update cycle, so a refreshing
        # parent must be one that only ever adds rows.
        source = self.pick(self.static_or(self.appending))
        if source is None:
            return False
        op: dict[str, Any] = {"name": name, "op": "ring_table", "source": source.name,
                              "capacity": rng.choice([1, 2, 3, 3, 5, 8, 20, 400])}
        if self.chance(0.04):
            op["capacity"] = rng.choice([0, -1])  # refused
        if self.chance(0.3):
            op["initialize"] = self.chance(0.7)  # false over a static parent: refused
        self.add(op, list(source.columns), source.rows)
        return True

    def build_keyed_transpose(self, name: str) -> bool:
        rng = self.rng
        source = self.pick(lambda t: len(t.usable()) >= 2) or self.pick_table()
        usable = source.usable()
        if len(usable) < 2:
            return False
        row_by = self.keys(source, 1, 2)
        named = [c for c in usable if c not in row_by]
        if not named:
            return False
        col_by = rng.sample(named, 1 if self.chance(0.85) else min(2, len(named)))
        rest = [c for c in usable if c not in row_by and c not in col_by]
        aggs: list[dict[str, Any]] = []
        floats = False
        for _ in range(1 if self.chance(0.7) else 2):
            agg = rng.choice(_TRANSPOSE_AGGS)
            if agg == "count":
                aggs.append({"agg": "count", "column": self.fresh("N")})
                continue
            pool = [c for c in rest if agg != "sum" or source.type_of(c) in _INTEGERS]
            if not pool:
                continue
            column = rng.choice(pool)
            floats |= source.type_of(column) in FLOATS
            aggs.append({"agg": agg, "columns": [f"{self.fresh('A')} = {column}"]})
        if floats:
            # A sum gives every transposed column the float tolerance; none next to a float.
            aggs = [a for a in aggs if a["agg"] != "sum"]
        if not aggs:
            return False
        op: dict[str, Any] = {"name": name, "op": "keyed_transpose", "source": source.name,
                              "aggs": aggs, "row_by": row_by, "col_by": col_by}
        if self.chance(0.3):
            # The initial groups name the columns and, in the reference, the rows too.
            initial = self.initial_groups(source, row_by + col_by)
            if initial:
                op["initial_groups"] = initial
        if source.live or self.chance(0.2):
            # `fail` on a refreshing table fails it when a row brings a new column.
            op["new_columns"] = "ignore"
        # The other columns are named after values of the column keys: only the row keys can
        # be named by a later operation.
        self.add(op, [(c, source.type_of(c)) for c in row_by], max(source.rows, 1))
        return True

    def build_multi_join(self, name: str) -> bool:
        rng = self.rng
        if self.chance(0.45):
            # With match columns: as `fuzz_wide` builds it, and half of those through the
            # console instead of the table service.
            if not super().build_multi_join(name):
                return False
            if self.chance(0.5):
                self.operations[-1]["script"] = True
            return True
        # Without match columns: the one row of each table side by side. A table with more
        # rows is refused, which on a refreshing table would be a failure in a cycle.
        tables = [t for t in self.tables.values() if t.columns and (
            t.name in self.single or not t.live)]
        if not tables:
            return False
        single = [t for t in tables if t.name in self.single]
        if not single and not self.chance(0.1):
            return False
        chosen = []
        for _ in range(rng.choice([1, 2, 2, 3])):
            pool = single if single and self.chance(0.9) else tables
            table = rng.choice(pool)
            if table not in chosen:
                chosen.append(table)
        columns: list[tuple[str, str]] = []
        inputs = []
        for table in chosen:
            item: dict[str, Any] = {"table": table.name, "on": [], "joins": []}
            picked = rng.sample(table.columns, rng.randrange(1, min(2, len(table.columns)) + 1))
            for column, kind in picked:
                out = self.fresh("J")
                item["joins"].append(f"{out} = {column}")
                columns.append((out, kind))
            inputs.append(item)
        self.add({"name": name, "op": "multi_join", "inputs": inputs, "script": True}, columns, 1)
        self.single.add(name)
        return True

    def build_range_join(self, name: str) -> bool:
        if self.chance(0.5):
            if not super().build_range_join(name):
                return False
            if self.chance(0.4):
                self.operations[-1]["script"] = True
            return True
        # Without aggregations: the left table, once the ranges have been checked.
        if not super().build_range_join(name):
            return False
        op = self.operations[-1]
        op["aggs"] = []
        op["script"] = True
        table = self.tables[name]
        table.columns = list(self.tables[op["left"]].columns)
        table.origin = dict(self.tables[op["left"]].origin)
        return True

    # ------------------------------------------------------------ update_by

    def update_by_operator(self, source: _Table, columns: list[tuple[str, str]],
                           used: set[str]) -> dict[str, Any] | None:
        if self.chance(0.4):
            return super().update_by_operator(source, columns, used)
        rng = self.rng
        stamps = self.timestamps(source)
        unit = "time" if stamps and self.chance(0.4) else "tick"
        window: dict[str, Any] = {}
        if unit == "tick":
            window = {"rev_ticks": rng.choice([1, 2, 3, 4, 5]),
                      "fwd_ticks": rng.choice([0, 0, 1, 2])}
        else:
            window = {"ts_col": stamps, "rev_time": rng.choice(["PT0S", "PT1S", "PT2S", "PT1M"]),
                      "fwd_time": rng.choice(["PT0S", "PT0S", "PT1S"])}
        kind = rng.choice(["formula", "formula", "cum_count_where", "rolling_count_where"])
        if kind == "formula":
            formula = rng.choice(list(_ROLLING_FORMULAS))
            accepted, result = _ROLLING_FORMULAS[formula]
            pool = source.usable(accepted)
            if not pool:
                return None
            column = rng.choice(pool)
            out = self.fresh("U")
            columns.append((out, result or source.type_of(column)))
            used.add(column)
            return {"op": f"rolling_formula_{unit}", "columns": [f"{out} = {column}"],
                    "args": {"formula": formula, "formula_param": "each", **window}}
        filters = [f for f in (_Wide.filter(self, source)
                               for _ in range(rng.randrange(1, 3)))
                   if f and not self.stops_reference(source, f)]
        if not filters:
            return None
        out = self.fresh("U")
        columns.append((out, "int64"))
        if kind == "cum_count_where":
            return {"op": "cum_count_where", "columns": [out], "args": {"filters": filters}}
        return {"op": f"rolling_count_where_{unit}", "columns": [out],
                "args": {"filters": filters, **window}}

    @staticmethod
    def stops_reference(source: _Table, text: str) -> bool:
        """J1: a count where of `update_by` with a filter that compares a column with a
        column, when that filter or another one of the operator reads a boolean or timestamp
        column. The reference's operator fails when it is made (CountWhereOperator$Context:
        "originalFillContexts" is null), the failure is taken for an "Unexpected iteration
        job error", and the server process shuts down. J4: with other columns only, the
        reference fails when the first row is read ('"keys" is null'), which on a refreshing
        table without rows is in a later cycle. So no filter compares two columns."""
        found = _COLUMN_COMPARISON.match(text)
        if not found or found.group(2) in ("null", "true", "false"):
            return False
        return found.group(2) in dict(source.columns)

    # ------------------------------------------------------------ statistics

    def build_column_statistics(self, name: str) -> bool:
        source = self.pick(lambda t: not t.live) if self.chance(0.7) else self.pick_table()
        if source is None or not source.usable():
            return False
        op: dict[str, Any] = {"name": name, "op": "column_statistics", "source": source.name,
                              "column": self.rng.choice(source.usable())}
        if self.chance(0.3):
            op["unique_value_limit"] = self.rng.choice([0, 1, 2, 5])
        # Its columns depend on the column's type; nothing is defined from it.
        self.add(op, [], 1)
        return True

