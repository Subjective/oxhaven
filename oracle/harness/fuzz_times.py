"""The fuzzer's generator for the date and time column types
(`fuzz.generate(..., variants="times")`, the family `time_types`).

No Java counterpart: this is the harness's fuzzer. It is a generator of its own, with a
random stream of its own, so the families of `fuzz_wide` and `fuzz_second` keep their
scenarios. The findings of its runs are `tasks/4.24-fuzz-time-types.md`.

A scenario has two inputs `t` and `r` with a key `K`, a number `V` and an instant `T`
(static, keyed or append-only), and an `update` of each (`b` and `c`) that makes columns of
`LocalDate` (`D`), `LocalTime` (`L`), `LocalDateTime` (`M`), `Duration` (`U`), `DayOfWeek`
(`W`), `ZonedDateTime` (`Z`, one instant in three zones by `K`), `Period` (`P`) and `ZoneId`
(`N`), each with nulls. Over these it draws sorts, match and range filters with the
literal spellings of each type, natural, exact, cross and as-of joins with keys of these
types, `select_distinct`, and aggregations of and by them.

What the wire does not carry is not compared: the zone of a `ZonedDateTime` (sent as its
instant). A `view` with `hourOfDay(Z, true)` is drawn after an operation now and then,
which shows the zone. A `Period` is sent as an interval of months, whose cells pyarrow
cannot read, so a table with a `Period` column is compared through a view without it
(`results`): the periods are sort, filter, join and group keys, and what an aggregation of
one answers is compared only by whether it is refused.

Kept out because the table would fail in a later update cycle, which cannot be compared
(`oracle/README.md`, Known limits): a natural or exact join whose right table is not an
aggregation by the join keys of the left table's own parent.

An ordering aggregation of a `Period` (`min`, `count_distinct`, ...) fails a group once
two periods meet in it, which on a refreshing parent can be in a later cycle: that is
drawn, since the rules compare the cycle a table fails in (PARITY.md, A11).

`RESTRICTIONS` lists what it steps around because of a finding.
"""

from __future__ import annotations

from typing import Any

from .fuzz import T0, _Table
from .fuzz_second import _Second

# Findings of tasks/4.24-fuzz-time-types.md that the generator steps around, and where.
RESTRICTIONS: dict[str, str] = {
    "R4": "time_agg: no sorted first or last by a Period column",
    "J1": "time_agg: no min, max, median, percentile, count_distinct, unique or distinct of "
          "a ZoneId column, and no sorted first or last by one (the reference fails when "
          "two zones meet in a group)",
    "R6": "time_aj: no as-of join on a column of these types",
}

DATE, TIME, DATETIME, DURATION, DOW, ZONED, PERIOD, ZONE = (
    "date", "time", "datetime", "duration", "dow", "zoned", "period", "zone")
TIME_TYPES = (DATE, TIME, DATETIME, DURATION, DOW, ZONED, PERIOD, ZONE)
_SECOND = 1_000_000_000
# Offsets from `fuzz.T0` (2023-11-14T22:13:20Z) in nanoseconds: the same and nearby
# instants, other days, and the hours around the clock changes of New York in 2024.
_OFFSETS = (0, 0, 1, _SECOND, 3600 * _SECOND, -5 * 3600 * _SECOND, 9 * 3600 * _SECOND,
            86400 * _SECOND, -86400 * _SECOND, 2 * 86400 * _SECOND, 7 * 86400 * _SECOND,
            1_710_053_999 * _SECOND - T0, 1_710_054_000 * _SECOND - T0,
            1_730_611_800 * _SECOND - T0, 1_730_615_400 * _SECOND - T0, -T0)
_ZONES = ("UTC", "America/New_York", "Asia/Tokyo", "Europe/Paris", "ET", "JP")
_MAKERS = {
    DATE: "toLocalDate(T, '{zone}')",
    TIME: "toLocalTime(T, '{zone}')",
    DATETIME: "toLocalDateTime(T, '{zone}')",
    DURATION: "isNull(V) ? null : (V % 3 == 0 ? 'PT1H' : (V % 3 == 1 ? 'PT-2M' : 'PT0.5S'))",
    DOW: "dayOfWeek(T, '{zone}')",
    ZONED: "toZonedDateTime(T, K % 3 == 0 ? 'America/New_York' : "
           "(K % 3 == 1 ? 'UTC' : 'Asia/Tokyo'))",
    PERIOD: "isNull(V) ? null : (V % 3 == 0 ? 'P1Y2M' : (V % 3 == 1 ? 'P3D' : 'P14M'))",
    ZONE: "isNull(K) ? null : timeZone(K % 2 == 0 ? `America/New_York` : `Asia/Tokyo`)",
}
_NAMES = {DATE: "D", TIME: "L", DATETIME: "M", DURATION: "U", DOW: "W", ZONED: "Z",
          PERIOD: "P", ZONE: "N"}
# Literals of a filter, per type: spellings the reference's filter parser reads, and a few
# it may not.
_LITERALS = {
    DATE: ("'2023-11-14'", "'2023-11-15'", "'2024-03-10'", "'2023-11-13'", "'20231114'",
           "'2023-3-5'", "'2023-11-14T00:00'"),
    TIME: ("'22:13:20'", "'17:13:20'", "'07:13'", "'22:13:20.000000001'", "'9:13'",
           "'00:00'", "'24:00'"),
    DATETIME: ("'2023-11-14T22:13:20'", "'2023-11-15T07:13:20'", "'2023-11-14'",
               "'2023-11-14T22:13:20.000000001'", "'2023-11-15T7:13:20'", "'2023-11-14T24:00'",
               "'2024-03-10t02:00'", "'2023-11-14 22:13:20'"),
    DURATION: ("'PT1H'", "'PT-2M'", "'PT0.5S'", "'PT0S'"),
    DOW: ("MONDAY", "TUESDAY", "WEDNESDAY", "SUNDAY", "Tuesday", "`TUESDAY`"),
    ZONED: ("'2023-11-14T22:13:20 UTC'", "'2023-11-14T17:13:20 ET'",
            "'2023-11-15T07:13:20 Asia/Tokyo'", "'2023-11-14T22:13:21 UTC'",
            "'2024-03-10T07:00:00 UTC'", "'2023-11-14T22:13:20Z'"),
    PERIOD: ("'P1Y2M'", "'P3D'", "'P14M'"),
    ZONE: ("'America/New_York'", "`Asia/Tokyo`"),
}
_AGGS = ("min", "max", "first", "last", "count_distinct", "unique", "distinct",
         "sorted_first", "sorted_last", "median", "pct")
_ORDERED_AGGS = ("min", "max", "median", "pct", "count_distinct", "unique", "distinct")


class _Times(_Second):
    """The generator described in the module comment."""

    def __init__(self, rng: Any, ops: tuple[str, ...]):
        super().__init__(rng, ops)
        self.parent: dict[str, str] = {}  # table -> `b` or `c` when it has that one's rows

    # ------------------------------------------------------------ inputs

    def value(self, kind: str, key: bool = False) -> Any:
        if kind == "timestamp":
            return None if self.chance(0.12) else T0 + self.rng.choice(_OFFSETS)
        return super().value(kind, key)

    def make_inputs(self) -> None:
        rng = self.rng
        columns = [("K", "int32"), ("V", "int32"), ("T", "timestamp")]
        kinds = [kind for kind in TIME_TYPES if self.chance(0.6)]
        while len(kinds) < 2:
            kinds = sorted(set(kinds) | {rng.choice(TIME_TYPES)}, key=TIME_TYPES.index)
        zone = rng.choice(_ZONES)
        for name, base, id_column in (("t", "b", "Id"), ("r", "c", "RId")):
            mode = self.mode()
            keyed = mode == "keyed"
            have = ([(id_column, "int32")] if keyed else []) + columns
            doc: dict[str, Any] = {"name": name,
                                   "columns": [{"name": n, "type": t} for n, t in have]}
            if mode != "static":
                doc["ticking"] = {"key_columns": [id_column] if keyed else []}
            count = 0 if self.chance(0.05) else rng.randrange(1, 13)
            doc["rows"] = [self.row(have, {"K"}, row_id if keyed else None)
                           for row_id in range(1, count + 1)]
            self.inputs.append(doc)
            self.tables[name] = _Table(name, have, count, live=mode != "static")
            self.input_names.add(name)
            formulas = [f"{_NAMES[kind]} = {_MAKERS[kind].format(zone=zone)}" for kind in kinds]
            made = [(_NAMES[kind], kind) for kind in kinds]
            self.add({"name": base, "op": "update", "source": name, "formulas": formulas},
                     have + made, count + 30)
            self.parent[base] = base
        del self.tables["t"], self.tables["r"]  # every operation reads the time columns

    def make_steps(self) -> list[dict[str, Any]]:
        rng = self.rng
        ticking = [doc for doc in self.inputs if "ticking" in doc]
        if not ticking:
            return []
        live = {doc["name"]: [row[0] for row in doc["rows"]] if doc["ticking"]["key_columns"]
                else [] for doc in ticking}
        steps = []
        for _ in range(rng.randrange(1, 5)):
            actions = []
            for _ in range(rng.randrange(1, 3)):
                doc = rng.choice(ticking)
                name, ids = doc["name"], live[doc["name"]]
                keyed = bool(doc["ticking"]["key_columns"])
                columns = [(c["name"], c["type"]) for c in doc["columns"]]
                if keyed and ids and self.chance(0.3):
                    gone = rng.sample(ids, rng.randrange(1, min(3, len(ids)) + 1))
                    for row_id in gone:
                        ids.remove(row_id)
                    actions.append({"action": "delete", "table": name,
                                    "rows": [[row_id] for row_id in gone]})
                    continue
                rows = []
                for _ in range(rng.randrange(1, 4)):
                    row_id = None
                    if keyed and ids and self.chance(0.5):
                        row_id = rng.choice(ids)
                    elif keyed:
                        row_id = max(ids + [len(doc["rows"])]) + 1
                        ids.append(row_id)
                    rows.append(self.row(columns, {"K"}, row_id))
                actions.append({"action": "add", "table": name, "rows": rows})
            steps.append({"actions": actions})
        return steps

    def results(self) -> list[str]:
        """The tables to compare: each operation's, or a view of it without the `Period`
        columns."""
        names = []
        for op in list(self.operations):
            columns = self.tables[op["name"]].columns
            keep = [c for c, kind in columns if not kind.endswith(PERIOD)]
            if len(keep) == len(columns):
                names.append(op["name"])
            elif keep:
                names.append(op["name"] + "v")
                self.add({"name": names[-1], "op": "view", "source": op["name"],
                          "formulas": keep}, [c for c in columns if c[0] in keep],
                         self.tables[op["name"]].rows)
        return names

    # ------------------------------------------------------------ operations

    @staticmethod
    def timed(table: _Table) -> list[str]:
        return table.usable(TIME_TYPES)

    def source(self) -> _Table | None:
        return self.pick(lambda t: bool(self.timed(t)))

    def keep(self, op: dict[str, Any], source: _Table, columns: list[tuple[str, str]]) -> None:
        self.add(op, columns, source.rows)
        if source.name in self.parent and columns == source.columns:
            self.parent[op["name"]] = self.parent[source.name]

    def build(self, kind: str) -> bool:
        self.kind = kind
        handler = getattr(self, f"time_{kind}")
        source = self.source()
        return source is not None and handler(self.fresh("o"), source)

    def time_sort(self, name: str, source: _Table) -> bool:
        rng = self.rng
        chosen = [rng.choice(self.timed(source))]
        others = [c for c in source.usable() if c not in chosen]
        if others and self.chance(0.5):
            chosen.insert(rng.randrange(2), rng.choice(others))
        by = [{"column": c, "direction": rng.choice(["ascending", "descending"])}
              for c in chosen]
        self.keep({"name": name, "op": "sort", "source": source.name, "by": by}, source,
                  list(source.columns))
        return True

    def time_filter(self, source: _Table) -> str | None:
        rng = self.rng
        column = rng.choice(self.timed(source))
        kind = source.type_of(column)
        literal = lambda: "null" if self.chance(0.12) else rng.choice(_LITERALS[kind])  # noqa: E731
        shape = rng.choice(["match", "match", "in", "range", "range", "null"])
        if shape == "null":
            return rng.choice([f"{column} == null", f"{column} != null", f"isNull({column})",
                               f"{column} in null", f"{column} not in null"])
        if shape == "match":
            return f"{column} {rng.choice(['==', '!=', '=', '=='])} {literal()}"
        if shape == "in":
            return f"{column} {rng.choice(['in', 'not in'])} {literal()}, {literal()}"
        value = literal()
        return f"{column} {rng.choice(['<', '<=', '>', '>='])} {value}"

    def time_where(self, name: str, source: _Table) -> bool:
        filters = [self.time_filter(source) for _ in range(1 if self.chance(0.8) else 2)]
        if None in filters:
            return False
        self.keep({"name": name, "op": "where", "source": source.name, "filters": filters},
                  source, list(source.columns))
        return True

    def time_select_distinct(self, name: str, source: _Table) -> bool:
        chosen = [self.rng.choice(self.timed(source))]
        others = [c for c in source.usable() if c not in chosen]
        if others and self.chance(0.4):
            chosen.append(self.rng.choice(others))
        self.add({"name": name, "op": "select_distinct", "source": source.name,
                  "columns": chosen}, [(c, source.type_of(c)) for c in chosen], source.rows)
        return True

    def time_view(self, name: str, source: _Table) -> bool:
        """What the wire leaves out of a `ZonedDateTime`: its hour in its own zone."""
        zoned = source.usable((ZONED,))
        if not zoned:
            return False
        keep = [c for c in source.usable() if source.type_of(c) != PERIOD]
        formulas = keep + [f"{self.fresh('H')} = hourOfDay({z}, true)" for z in zoned]
        columns = [(c, source.type_of(c)) for c in keep] + [
            (f.split(" = ")[0], "int32") for f in formulas[len(keep):]]
        self.add({"name": name, "op": "view", "source": source.name, "formulas": formulas},
                 columns, source.rows)
        return True

    def time_update(self, name: str, source: _Table) -> bool:
        return self.time_view(name, source)

    def time_agg(self, source: _Table, by: list[str], count: int) -> tuple[list, list]:
        """`count` aggregations of the columns of `source` outside `by`, and their columns."""
        rng = self.rng
        aggs: list[dict[str, Any]] = []
        columns: list[tuple[str, str]] = []
        timed = [c for c in self.timed(source) if c not in by]
        for _ in range(count):
            if not timed or self.chance(0.1):
                out = self.fresh("N")
                aggs.append({"agg": "count", "column": out})
                columns.append((out, "int64"))
                continue
            column = rng.choice(timed)
            kind = source.type_of(column)
            agg = rng.choice(_AGGS)
            if kind == ZONE and agg in _ORDERED_AGGS:  # J1
                agg = rng.choice(["first", "last"])
            out = self.fresh("A")
            spec: dict[str, Any] = {"agg": agg, "columns": [f"{out} = {column}"]}
            predicted = kind
            if agg == "pct":
                spec["args"] = {"percentile": rng.choice([0.0, 0.25, 0.5, 0.9, 1.0])}
            elif agg == "count_distinct":
                predicted = "int64"
                if self.chance(0.5):
                    spec["args"] = {"count_nulls": self.chance(0.5)}
            elif agg in ("distinct", "unique") and self.chance(0.5):
                spec["args"] = {"include_nulls": self.chance(0.5)}
            elif agg in ("sorted_first", "sorted_last"):
                order = [c for c in source.usable() if c != column
                         and source.type_of(c) not in (PERIOD, ZONE)]  # R4, J1
                if not order:
                    continue
                spec["args"] = {"order_by": rng.choice(order)}
            if agg == "distinct":
                predicted = "list:" + kind
            aggs.append(spec)
            columns.append((out, predicted))
        return aggs, columns

    def time_agg_by(self, name: str, source: _Table) -> bool:
        rng = self.rng
        by: list[str] = []
        if self.chance(0.85):
            by = [rng.choice(self.timed(source))]
            others = [c for c in source.usable() if c not in by]
            if others and self.chance(0.3):
                by.append(rng.choice(others))
        aggs, columns = self.time_agg(source, by, rng.randrange(1, 4))
        if not aggs:
            return False
        self.add({"name": name, "op": "agg_by", "source": source.name, "aggs": aggs, "by": by},
                 [(c, source.type_of(c)) for c in by] + columns, max(source.rows, 1))
        return True

    def time_join(self, name: str, left: _Table) -> bool:
        """A join on a time column and now and then `K`. For a natural or an exact join
        the right table is an aggregation by the keys of the left table's parent, which has
        one row for each key the left table can have."""
        rng = self.rng
        kind = self.kind
        on = [rng.choice(self.timed(left))]
        if "K" in dict(left.columns) and self.chance(0.3):
            on.insert(rng.randrange(2), "K")
        if kind == "join":
            other = "c" if self.parent.get(left.name) == "b" else "b"
            right = self.tables[other if self.chance(0.7) else self.parent.get(left.name, "b")]
            if any(c not in dict(right.columns) for c in on) or self.chance(0.1):
                on = []
            if left.rows * right.rows > 2000:
                return False
            joined = [rng.choice(["V", "T"] + self.timed(right))]
        else:
            parent = self.parent.get(left.name)
            if parent is None:
                return False
            if kind == "natural_join" and self.chance(0.5):
                parent = "c" if parent == "b" else "b"  # some keys without a match
            aggs, made = self.time_agg(self.tables[parent], on, rng.randrange(1, 3))
            if not aggs:
                return False
            right_name = self.fresh("g")
            self.add({"name": right_name, "op": "agg_by", "source": parent, "aggs": aggs,
                      "by": on}, [(c, left.type_of(c)) for c in on] + made, left.rows)
            right = self.tables[right_name]
            joined = [c for c, _ in made]
        joins, columns = [], list(left.columns)
        for column in joined:
            out = self.fresh("J")
            joins.append(f"{out} = {column}")
            columns.append((out, right.type_of(column)))
        op: dict[str, Any] = {"name": name, "op": kind, "left": left.name,
                              "right": right.name, "on": on, "joins": joins}
        if kind == "natural_join" and self.chance(0.5):
            op["type"] = rng.choice(["error_on_duplicate", "first_match", "last_match",
                                     "exactly_one_match"])
        if kind == "natural_join" and right.name.startswith("g") and \
                op.get("type") == "exactly_one_match" and self.parent.get(left.name) != \
                self.operations[-1]["source"]:
            op["type"] = "first_match"  # a key without a match would fail in a later cycle
        self.add(op, columns, left.rows * right.rows if kind == "join" else left.rows)
        return True

    time_natural_join = time_exact_join = time_join

    def time_aj(self, name: str, left: _Table) -> bool:
        """An as-of join on a time column of the other parent, whatever its type."""
        if "R6" in RESTRICTIONS:
            return False
        rng = self.rng
        right = self.tables["c" if self.parent.get(left.name) == "b" else "b"]
        stamps = [c for c in self.timed(left) if c in dict(right.columns)]
        if not stamps:
            return False
        stamp = rng.choice(stamps)
        on = (["K"] if "K" in dict(left.columns) and self.chance(0.6) else []) + [
            f"{stamp} {rng.choice(['>=', '>'])} {stamp}"]
        out = self.fresh("J")
        self.add({"name": name, "op": "aj", "left": left.name, "right": right.name, "on": on,
                  "joins": [f"{out} = V"]}, list(left.columns) + [(out, "int32")], left.rows)
        return True
