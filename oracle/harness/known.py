"""Known findings of the fuzzer: differences that have been triaged and written down.

No Java counterpart: this is the harness. A long fuzz run against a server with a known
defect stops at, or is flooded by, that defect. `oracle/fuzz-known-findings.json` lists such
differences so that `fuzz` can set them apart and keep looking for others. It is a reading
aid for the fuzzer only: `diff`, `replay` and the gate never read it, and an entry changes
no comparison rule.

An entry names one cause and describes its mismatches narrowly:

    {"id": "agg-min-negative-zero",
     "class": "oxhaven-bug",
     "reason": "why the two servers differ, with the Java source that defines the behavior",
     "match": {"kind": "value", "operation": "agg_by", "producer": "min",
               "expected": -0.0, "actual": 0.0}}

`class` is one of `CLASSES`. `match` holds the signature; a mismatch is the finding's only if
every field given there fits:

- `kind` (required): the mismatch kind (`rules.py`).
- `operation` (required): the operation that defines the table (`input_table` for an input),
  or a list of them.
- `producer`: what made the mismatching column: the aggregation of `agg_by`, `agg_all_by`
  and `range_join`, the operator of `update_by`; `key` for a `by` column, `source` for a
  column the operation passes on. Also a list.
- `key`: the metadata keys whose values differ, sorted and joined by commas.
- `expected`, `actual`: both sides exactly as the mismatch reports them. A float matches by
  its bits, so 0.0 is not -0.0; NaN is written "NaN". `{"type": "float"}` in place of a value
  matches any float that is not NaN, for a finding whose values vary; `{"type": "zero"}`
  matches 0.0 and -0.0, for a finding in which either side can hold either zero.
- `steps`: `first` for step 0 only, `later` for the steps after it.
- `requires`: `reference_inconsistent`. The mismatch is the finding's only if the reference
  itself, asked for the same table again from the inputs as they are after that step, answers
  differently from what it kept up to date incrementally (`runner.run_fresh`), and the
  actual side holds for that table what the reference answers from scratch
  (`rules.differs_from_fresh`): a result that is neither of the reference's two answers is
  a difference. The harness checks this by running the scenario once more; a target that
  cannot be asked never fits. `reference_inconsistent_rows` is the same with every table
  compared as a multiset of rows, both the reference's two answers and the actual side's
  against the one from scratch: for a result whose row order depends on its history, as
  the order of an aggregation's groups does (a group made by a key that changed comes at
  the end), so that a table made again cannot give it. In this form only the operations
  that hold vectors of a refreshing group (`holds_group_vectors`), and what is made from
  them, are made again, from their sources' rows as kept (`runner.run_scenario`,
  `remake`), and a result made from one the reference disagrees with itself about in the
  same step counts as one too (`_Triage.split`): what the reference kept for it was read
  from a failed or stale table.
- `differs_in`: `zero_sign`. Both sides are vectors of the same shape whose elements are
  equal bit for bit except that a zero on one side is the other zero on the other side
  (every NaN equals every NaN), and at least one such pair is there.
- `shape`: the name of an entry of `SHAPES`, a test of the scenario's operations (and of
  the actual side's result schemas) that the mismatch's table must pass: what made the
  table or the column, as the allowed difference the entry is for describes it.

A table defined from one with a known mismatch shows it again (a wrong value passed on, a
refusal of everything downstream). Such a mismatch belongs to the same finding if the table
is defined, directly or through others, from the table of a mismatch that fits, in the same
step, with the same kind and metadata keys and with both sides exactly equal.

What keeps an entry from hiding anything else: a scenario counts as known only if every one
of its mismatches fits an entry, field by field; otherwise it is reported as differing, with
the mismatches that fit no entry. Generated table and column names are not part of a
signature (they vary by seed), so two causes that share every field given would be taken for
one: give `producer`, `expected` and `actual` wherever the cause fixes them.
"""

from __future__ import annotations

import json
import math
import re
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from . import rules
from . import scenario as sc

FORMAT = 1
CLASSES = ("oxhaven-bug", "allowed-difference", "reference-defect", "harness")
_MATCH_KEYS = {"kind", "operation", "producer", "key", "expected", "actual", "steps",
               "requires", "differs_in", "shape"}
_ABSENT = object()
_REQUIRES = ("reference_inconsistent", "reference_inconsistent_rows")


class KnownError(ValueError):
    """The known-findings file is not valid."""


@dataclass(frozen=True)
class Finding:
    id: str
    cls: str
    reason: str
    match: Mapping[str, Any]


def load(path: Path) -> list[Finding]:
    with open(path, encoding="utf-8") as handle:
        doc = json.load(handle)
    return parse(doc, str(path))


def parse(doc: Any, where: str = "known findings") -> list[Finding]:
    if not isinstance(doc, dict) or doc.get("format") != FORMAT or not isinstance(
            doc.get("findings"), list) or set(doc) - {"format", "findings"}:
        raise KnownError(f"{where}: expected {{\"format\": {FORMAT}, \"findings\": [...]}}")
    out: list[Finding] = []
    for index, item in enumerate(doc["findings"]):
        at = f"{where}: findings[{index}]"
        if not isinstance(item, dict) or set(item) != {"id", "class", "reason", "match"}:
            raise KnownError(f"{at}: needs exactly id, class, reason and match")
        if not isinstance(item["id"], str) or not item["id"] or any(
                f.id == item["id"] for f in out):
            raise KnownError(f"{at}: id must be a string that no other finding has")
        if item["class"] not in CLASSES:
            raise KnownError(f"{at}: class must be one of {', '.join(CLASSES)}")
        if not isinstance(item["reason"], str) or len(item["reason"].split()) < 5:
            raise KnownError(f"{at}: reason must say why the servers differ")
        match = item["match"]
        if not isinstance(match, dict) or set(match) - _MATCH_KEYS:
            raise KnownError(f"{at}: match may hold {sorted(_MATCH_KEYS)}")
        if not isinstance(match.get("kind"), str) or not _names(match.get("operation")):
            raise KnownError(f"{at}: match needs kind and operation")
        for operation in _names(match["operation"]):
            if operation not in sc.OPERATIONS and operation != sc.INPUT_TABLE:
                raise KnownError(f"{at}: unknown operation {operation!r}")
        if "producer" in match and not _names(match["producer"]):
            raise KnownError(f"{at}: producer must be a name or a list of names")
        if match.get("differs_in", "zero_sign") != "zero_sign":
            raise KnownError(f"{at}: differs_in must be zero_sign")
        if "shape" in match and match["shape"] not in SHAPES:
            raise KnownError(f"{at}: shape must be one of {', '.join(SHAPES)}")
        if match.get("steps", "first") not in ("first", "later"):
            raise KnownError(f"{at}: steps must be first or later")
        if match.get("requires", "reference_inconsistent") not in _REQUIRES:
            raise KnownError(f"{at}: requires must be one of {', '.join(_REQUIRES)}")
        out.append(Finding(item["id"], item["class"], item["reason"], match))
    return out


def _names(value: Any) -> list[str]:
    """`value` as a list of names: a name, or a list of at least one; [] if neither."""
    if isinstance(value, str) and value:
        return [value]
    if isinstance(value, list) and value and all(isinstance(v, str) and v for v in value):
        return list(value)
    return []


def producer(op: Mapping[str, Any] | None, column: str | None) -> str | None:
    """What made `column` of the table that `op` defines; see the module comment."""
    if op is None:
        return None
    kind = op["op"]
    if column is None:
        # A mismatch of the whole table: `agg_all_by` has one aggregation to name.
        return op["agg"]["agg"] if kind == "agg_all_by" else None
    if kind in ("agg_by", "range_join"):
        if kind == "agg_by" and column in op["by"]:
            return "key"
        for agg in op["aggs"]:
            names = [agg["column"]] if "column" in agg else [
                rules.split_pair(pair)[0] for pair in agg["columns"]]
            if column in names:
                return agg["agg"]
        return "source" if kind == "range_join" else None
    if kind == "agg_all_by":
        return "key" if column in op["by"] else op["agg"]["agg"]
    if kind == "update_by":
        for operator in op["ops"]:
            if column in [rules.split_pair(pair)[0] for pair in operator["columns"]]:
                return operator["op"]
        return "source"
    return None


def metadata_key(mismatch: rules.Mismatch) -> str:
    """The metadata keys whose values differ, for a mismatch whose sides are metadata."""
    if isinstance(mismatch.expected, dict) and isinstance(mismatch.actual, dict):
        return ",".join(sorted(
            str(k) for k in set(mismatch.expected) | set(mismatch.actual)
            if mismatch.expected.get(k) != mismatch.actual.get(k)))
    return ""


def _same_value(pattern: Any, value: Any) -> bool:
    if isinstance(pattern, dict) and set(pattern) == {"type"}:
        if not isinstance(value, float) or math.isnan(value):
            return False
        return pattern["type"] == "float" or (pattern["type"] == "zero" and value == 0.0)
    if pattern == "NaN":
        return isinstance(value, float) and math.isnan(value)
    if isinstance(pattern, float) or isinstance(value, float):
        if isinstance(pattern, bool) or isinstance(value, bool):
            return False
        if not isinstance(pattern, (int, float)) or not isinstance(value, (int, float)):
            return False
        if isinstance(pattern, float) != isinstance(value, float):
            return False
        return struct.pack("<d", pattern) == struct.pack("<d", value)
    if isinstance(pattern, list) and isinstance(value, list):
        return len(pattern) == len(value) and all(
            _same_value(p, v) for p, v in zip(pattern, value))
    return type(pattern) is type(value) and pattern == value


class Known:
    """The findings of one file, applied to the mismatches of one scenario at a time."""

    def __init__(self, findings: Iterable[Finding]):
        self.findings = list(findings)

    def __bool__(self) -> bool:
        return bool(self.findings)

    def classify(
        self,
        mismatches: list[rules.Mismatch],
        operations: Iterable[Mapping[str, Any]],
        reference_inconsistent: Callable[..., set[tuple[int, str]] | None] | None = None,
        scene: "Scene | None" = None,
    ) -> list[Finding | None]:
        """For each mismatch, the finding it belongs to, or None.

        `reference_inconsistent` is called at most once per form of `requires`, and only if
        an entry that fits otherwise needs it: it returns the (step, table) results for
        which the reference disagrees with itself, or None when that cannot be found out;
        for `reference_inconsistent_rows` it is called with True, to compare multisets of
        rows. `scene` is what the
        `shape` of an entry is tested on; without it no entry with a shape fits.
        """
        operations = list(operations)
        by_name = {op["name"]: op for op in operations}
        asked: dict[str, set[tuple[int, str]] | None] = {}

        def inconsistent(mismatch: rules.Mismatch, form: str) -> bool:
            if form not in asked:
                if reference_inconsistent is None:
                    asked[form] = None
                elif form == "reference_inconsistent_rows":
                    asked[form] = reference_inconsistent(True)
                else:
                    asked[form] = reference_inconsistent()
            return asked[form] is not None and (mismatch.step, mismatch.table) in asked[form]

        out: list[Finding | None] = []
        for mismatch in mismatches:
            op = by_name.get(mismatch.table)
            found = None
            for finding in self.findings:
                match = finding.match
                if match["kind"] != mismatch.kind:
                    continue
                if (op["op"] if op else sc.INPUT_TABLE) not in _names(match["operation"]):
                    continue
                if "producer" in match and producer(op, mismatch.column) not in _names(
                        match["producer"]):
                    continue
                if "key" in match and match["key"] != metadata_key(mismatch):
                    continue
                if "steps" in match and (match["steps"] == "first") != (mismatch.step == 0):
                    continue
                if match.get("expected", _ABSENT) is not _ABSENT and not _same_value(
                        match["expected"], mismatch.expected):
                    continue
                if match.get("actual", _ABSENT) is not _ABSENT and not _same_value(
                        match["actual"], mismatch.actual):
                    continue
                if "differs_in" in match and not differs_in_zero_sign(
                        mismatch.expected, mismatch.actual):
                    continue
                if "shape" in match and (scene is None or not SHAPES[match["shape"]](
                        scene, mismatch)):
                    continue
                if "requires" in match and not inconsistent(mismatch, match["requires"]):
                    continue
                found = finding
                break
            out.append(found)
        # What a table shows wrongly, the tables defined from it show too.
        changed = True
        while changed:
            changed = False
            for index, mismatch in enumerate(mismatches):
                if out[index] is not None or mismatch.table not in by_name:
                    continue
                above = ancestors(mismatch.table, by_name)
                for other, finding in zip(mismatches, out):
                    if finding is not None and other.table in above and _same_symptom(
                            mismatch, other):
                        out[index] = finding
                        changed = True
                        break
        return out


def ancestors(table: str, by_name: Mapping[str, Mapping[str, Any]]) -> set[str]:
    seen: set[str] = set()
    todo = [table]
    while todo:
        op = by_name.get(todo.pop())
        for name in sc.references(op) if op else ():
            if name not in seen:
                seen.add(name)
                todo.append(name)
    return seen


def _same_symptom(mismatch: rules.Mismatch, other: rules.Mismatch) -> bool:
    """`mismatch` repeats `other`: the same step, kind and metadata keys, and both sides
    exactly equal. The column is not compared, since an operation can rename it."""
    return (mismatch.step == other.step and mismatch.kind == other.kind
            and metadata_key(mismatch) == metadata_key(other)
            and _same_value(other.expected, mismatch.expected)
            and _same_value(other.actual, mismatch.actual))


def differs_in_zero_sign(expected: Any, actual: Any) -> bool:
    """`differs_in: zero_sign`: two vectors of the same shape, equal bit for bit but for at
    least one zero that is 0.0 on one side and -0.0 on the other (any NaN equals any NaN)."""
    flipped: list[bool] = []

    def same(one: Any, other: Any) -> bool:
        if isinstance(one, list) and isinstance(other, list):
            return len(one) == len(other) and all(same(a, b) for a, b in zip(one, other))
        if isinstance(one, float) and isinstance(other, float):
            if math.isnan(one) or math.isnan(other):
                return math.isnan(one) and math.isnan(other)
            if one == 0.0 and other == 0.0:
                flipped.append(math.copysign(1.0, one) != math.copysign(1.0, other))
                return True
            return struct.pack("<d", one) == struct.pack("<d", other)
        return type(one) is type(other) and one == other

    return (isinstance(expected, list) and isinstance(actual, list)
            and same(expected, actual) and any(flipped))


# ---------------------------------------------------------------- shapes

# What makes a column of vectors: the operation or aggregation, or the `update_by` operator.
VECTOR_MAKERS = frozenset({"group_by", "group", "distinct", "rolling_group_tick",
                           "rolling_group_time"})
# What Java's vectors of a refreshing group are views of (PARITY.md, A22).
GROUPS = frozenset({"group_by", "group"})
EXTREMES = frozenset({"cum_min", "cum_max", "rolling_min_tick", "rolling_max_tick",
                      "rolling_min_time", "rolling_max_time"})
_FORMULAS = frozenset({"update", "update_view", "lazy_update", "view", "select"})
_STATIC_RESULTS = frozenset({"snapshot", "meta_table"})
_NAME = re.compile(r"[A-Za-z_]\w*")


@dataclass(frozen=True)
class Origin:
    """Where a column was made, found by following its name up through the operations that
    pass it on: `op` made it (None for a column of an input table) by `how` (the operation,
    the aggregation or the `update_by` operator; `formula` for a formula of `update` and the
    like, `input` for an input), from the column `source` of the table `table`; `text` is
    the formula, if one made it. `ungrouped` says whether an `ungroup` on the way took the
    column apart."""

    op: Mapping[str, Any] | None
    how: str
    source: str | None
    table: str | None
    text: str | None
    ungrouped: bool


class Scene:
    """What the `shape` of a known finding is tested on: the operations and inputs of the
    scenario, and the results of the actual side (`ResultSet`, for the column types of a
    table the operations leave open, such as which columns an `ungroup` without a list takes).
    """

    def __init__(self, scenario: Any, actual: Any = None):
        self.by_name = {op["name"]: op for op in scenario.operations}
        self.ticking = {item.name for item in scenario.inputs if item.ticking}
        self.actual = actual

    def refreshing(self, table: str) -> bool:
        """Whether `table` is a ticking input or is made from one (a snapshot is static)."""
        seen: set[str] = set()
        todo = [table]
        while todo:
            name = todo.pop()
            if name in seen:
                continue
            seen.add(name)
            op = self.by_name.get(name)
            if op is None:
                if name in self.ticking:
                    return True
            elif op["op"] not in _STATIC_RESULTS:
                todo.extend(sc.references(op))
        return False

    def lists(self, table: str) -> set[str] | None:
        """The vector columns of `table` in the actual side's results, or None if they hold
        no table of that name. A table with a column of arrays of arrays is compared
        through an `ungroup` of that column named for it (`fuzz_third`, `results`): its
        vector columns are then those of that view and the column it takes apart."""
        if self.actual is None:
            return None
        for step, name in self.actual.keys():
            if name != table:
                continue
            data = self.actual.get(step, name)
            schema = getattr(data, "schema", None)
            if schema is not None:
                return {field.name for field in schema
                        if str(field.type).startswith(("list<", "large_list<"))}
        view = self.by_name.get(table + "v")
        if view is not None and view["op"] == "ungroup" and view["source"] == table and (
                view.get("columns")):
            lists = self.lists(view["name"])
            return None if lists is None else lists | set(view["columns"])
        return None

    def taken(self, op: Mapping[str, Any]) -> list[str] | None:
        """The columns an `ungroup` takes apart, or None if that is not known."""
        if op.get("columns"):
            return list(op["columns"])
        lists = self.lists(op["source"])
        return None if lists is None else sorted(lists)

    def origin(self, table: str, column: str) -> Origin | None:
        """Where `column` of `table` was made; None where the operations do not tell."""
        ungrouped = False
        for _ in range(len(self.by_name) + 1):
            op = self.by_name.get(table)
            if op is None:
                return Origin(None, "input", column, None, None, ungrouped)
            kind = op["op"]
            source = op.get("source") or op.get("left")
            if kind == "ungroup":
                taken = self.taken(op)
                if taken is None:
                    return None
                ungrouped = ungrouped or column in taken
            elif kind == "group_by" and column not in op["by"]:
                return Origin(op, kind, column, source, None, ungrouped)
            elif kind == "agg_by" and column not in op["by"]:
                for agg in op["aggs"]:
                    if agg.get("column") == column:
                        return Origin(op, agg["agg"], None, source, None, ungrouped)
                    for pair in agg.get("columns", ()):
                        out, made_from = rules.split_pair(pair)
                        if out == column:
                            return Origin(op, agg["agg"], made_from, source, None, ungrouped)
                return None
            elif kind == "update_by":
                for operator in op["ops"]:
                    for pair in operator["columns"]:
                        out, made_from = rules.split_pair(pair)
                        if out == column:
                            formula = operator.get("args", {}).get("formula")
                            return Origin(op, operator["op"], made_from, source, formula,
                                          ungrouped)
            elif kind in _FORMULAS:
                for text in op["formulas"]:
                    out, expression = rules.split_pair(text)
                    if out == column and expression != column:
                        if not _NAME.fullmatch(expression):
                            return Origin(op, "formula", None, source, expression, ungrouped)
                        column = expression  # a rename
                        break
            elif "right" in op:
                pairs = [rules.split_pair(pair) for pair in op.get("joins", ())]
                made = [made_from for out, made_from in pairs if out == column]
                if made:
                    table, column = op["right"], made[0]
                    continue
                if "joins" not in op:
                    return None  # the left or the right table's: the operations do not tell
            if source is None:
                return None
            table = source
        return None


def _ancestry(scene: Scene, table: str) -> list[Mapping[str, Any]]:
    """The operation of `table` and of every table it is made from."""
    names = [table] + sorted(ancestors(table, scene.by_name))
    return [scene.by_name[name] for name in names if name in scene.by_name]


def holds_group_vectors(scene: Scene, op: Mapping[str, Any]) -> bool:
    """PARITY.md A22: `op` holds vectors of a refreshing `group_by` (or `group`) as
    group-by, bucket or `select_distinct` columns, in `distinct`, or as the input of a
    cumulative or rolling minimum or maximum."""
    source = op.get("source")
    if source is None:
        return False

    def group_vector(column: str) -> bool:
        origin = scene.origin(source, column)
        return (origin is not None and origin.how in GROUPS and not origin.ungrouped
                and origin.table is not None and scene.refreshing(origin.table))

    kind = op["op"]
    if kind == "select_distinct":
        return any(group_vector(column) for column in op["columns"])
    if kind == "agg_by":
        return any(group_vector(column) for column in op["by"]) or any(
            group_vector(rules.split_pair(pair)[1]) for agg in op["aggs"]
            if agg["agg"] == "distinct" for pair in agg["columns"])
    if kind == "update_by":
        return any(group_vector(column) for column in op["by"]) or any(
            group_vector(rules.split_pair(pair)[1]) for operator in op["ops"]
            if operator["op"] in EXTREMES for pair in operator["columns"])
    return False


def _refreshing_group_vectors(scene: Scene, mismatch: rules.Mismatch) -> bool:
    """A22: the table, or one it is made from, holds vectors of a refreshing group."""
    return any(holds_group_vectors(scene, op) for op in _ancestry(scene, mismatch.table))


def _ungrouped_extreme(scene: Scene, mismatch: rules.Mismatch) -> bool:
    """A23, first case: the column is a cumulative or rolling minimum or maximum of vectors,
    taken apart by an `ungroup`."""
    origin = scene.origin(mismatch.table, mismatch.column) if mismatch.column else None
    return origin is not None and origin.how in EXTREMES and origin.ungrouped


_ELEMENT_OF_A_CONDITION = re.compile(r"\?.*:.*\)\s*\[")


def _element_of_a_condition(scene: Scene, mismatch: rules.Mismatch) -> bool:
    """A23, second case: the column is a formula that reads an element of a condition
    between two vectors, `(c ? S : S2)[0]`."""
    origin = scene.origin(mismatch.table, mismatch.column) if mismatch.column else None
    return (origin is not None and origin.text is not None and not origin.ungrouped
            and (origin.how == "formula" or origin.how.startswith("rolling_formula"))
            and bool(_ELEMENT_OF_A_CONDITION.search(origin.text)))


def _ungroup_of(scene: Scene, mismatch: rules.Mismatch,
                ) -> tuple[Mapping[str, Any], list[str]] | None:
    op = scene.by_name.get(mismatch.table)
    if op is None or op["op"] != "ungroup":
        return None
    taken = scene.taken(op)
    return None if taken is None else (op, taken)


def _ungroup_again_of_a_distinct(scene: Scene, mismatch: rules.Mismatch) -> bool:
    """A24, first row: an `ungroup` of a column that an earlier `ungroup` took out of the
    `distinct` of a vector column."""
    found = _ungroup_of(scene, mismatch)
    if found is None:
        return False
    op, taken = found
    for column in taken:
        origin = scene.origin(op["source"], column)
        if origin is not None and origin.how == "distinct" and origin.ungrouped:
            return True
    return False


def _null_fill_ungroup_of_vector_keys(scene: Scene, mismatch: rules.Mismatch) -> bool:
    """A24, second row: an `ungroup` with null fill of two or more columns, one of them a
    group-by column of an aggregation that is a vector."""
    found = _ungroup_of(scene, mismatch)
    if found is None:
        return False
    op, taken = found
    if not op.get("null_fill", True) or len(taken) < 2:
        return False
    for column in taken:
        table = op["source"]
        while table in scene.by_name:
            above = scene.by_name[table]
            if above["op"] == "agg_by" and column in above["by"]:
                origin = scene.origin(above["source"], column)
                if (origin is not None and origin.how in VECTOR_MAKERS
                        and not origin.ungrouped):
                    return True
                break
            if above["op"] in ("ungroup", "group_by") or "right" in above:
                break
            table = above.get("source") or above.get("left")
    return False


def _null_fill_ungroup_of_a_distinct(scene: Scene, mismatch: rules.Mismatch) -> bool:
    """A24, third row: an `ungroup` with null fill of two or more columns, one of them the
    vector of a `distinct` aggregation."""
    found = _ungroup_of(scene, mismatch)
    if found is None:
        return False
    op, taken = found
    if not op.get("null_fill", True) or len(taken) < 2:
        return False
    for column in taken:
        origin = scene.origin(op["source"], column)
        if origin is not None and origin.how == "distinct" and not origin.ungrouped:
            return True
    return False


def _distinct_taken_apart(scene: Scene, mismatch: rules.Mismatch) -> bool:
    """A20: the column holds the vectors of a `distinct` aggregation of a vector column,
    taken apart by an `ungroup` (how a table of arrays of arrays is compared)."""
    origin = scene.origin(mismatch.table, mismatch.column) if mismatch.column else None
    return origin is not None and origin.how == "distinct" and origin.ungrouped


def _as_of_chain(scene: Scene, mismatch: rules.Mismatch) -> bool:
    """A13, the chain case: an as-of join with exact match columns, a static left table and
    a refreshing right table that is not an input table."""
    op = scene.by_name.get(mismatch.table)
    return (op is not None and op["op"] in ("aj", "raj") and len(op["on"]) > 1
            and not scene.refreshing(op["left"]) and scene.refreshing(op["right"])
            and op["right"] in scene.by_name)


SHAPES: dict[str, Callable[[Scene, rules.Mismatch], bool]] = {
    "as_of_chain": _as_of_chain,
    "refreshing_group_vectors": _refreshing_group_vectors,
    "ungrouped_extreme": _ungrouped_extreme,
    "element_of_a_condition": _element_of_a_condition,
    "ungroup_again_of_a_distinct": _ungroup_again_of_a_distinct,
    "null_fill_ungroup_of_vector_keys": _null_fill_ungroup_of_vector_keys,
    "null_fill_ungroup_of_a_distinct": _null_fill_ungroup_of_a_distinct,
    "distinct_taken_apart": _distinct_taken_apart,
}
