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
- `operation` (required): the operation that defines the table (`input_table` for an input).
- `producer`: what made the mismatching column: the aggregation of `agg_by`, `agg_all_by`
  and `range_join`, the operator of `update_by`; `key` for a `by` column, `source` for a
  column the operation passes on.
- `key`: the metadata keys whose values differ, sorted and joined by commas.
- `expected`, `actual`: both sides exactly as the mismatch reports them. A float matches by
  its bits, so 0.0 is not -0.0; NaN is written "NaN". `{"type": "float"}` in place of a value
  matches any float that is not NaN, for a finding whose values vary; `{"type": "zero"}`
  matches 0.0 and -0.0, for a finding in which either side can hold either zero.
- `steps`: `first` for step 0 only, `later` for the steps after it.
- `requires`: `reference_inconsistent`. The mismatch is the finding's only if the reference
  itself, asked for the same table again from the inputs as they are after that step, answers
  differently from what it kept up to date incrementally (`runner.run_fresh`). The harness
  checks this by running the scenario once more; a target that cannot be asked never fits.

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
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from . import rules
from . import scenario as sc

FORMAT = 1
CLASSES = ("oxhaven-bug", "allowed-difference", "reference-defect", "harness")
_MATCH_KEYS = {"kind", "operation", "producer", "key", "expected", "actual", "steps",
               "requires"}
_ABSENT = object()


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
        if not isinstance(match.get("kind"), str) or not isinstance(
                match.get("operation"), str):
            raise KnownError(f"{at}: match needs kind and operation")
        if match["operation"] not in sc.OPERATIONS and match["operation"] != sc.INPUT_TABLE:
            raise KnownError(f"{at}: unknown operation {match['operation']!r}")
        if match.get("steps", "first") not in ("first", "later"):
            raise KnownError(f"{at}: steps must be first or later")
        if match.get("requires", "reference_inconsistent") != "reference_inconsistent":
            raise KnownError(f"{at}: requires must be reference_inconsistent")
        out.append(Finding(item["id"], item["class"], item["reason"], match))
    return out


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
        reference_inconsistent: Callable[[], set[tuple[int, str]] | None] | None = None,
    ) -> list[Finding | None]:
        """For each mismatch, the finding it belongs to, or None.

        `reference_inconsistent` is called at most once, and only if an entry that fits
        otherwise needs it: it returns the (step, table) results for which the reference
        disagrees with itself, or None when that cannot be found out.
        """
        by_name = {op["name"]: op for op in operations}
        asked: list[set[tuple[int, str]] | None] = []

        def inconsistent(mismatch: rules.Mismatch) -> bool:
            if not asked:
                asked.append(reference_inconsistent() if reference_inconsistent else None)
            return asked[0] is not None and (mismatch.step, mismatch.table) in asked[0]

        out: list[Finding | None] = []
        for mismatch in mismatches:
            op = by_name.get(mismatch.table)
            found = None
            for finding in self.findings:
                match = finding.match
                if match["kind"] != mismatch.kind:
                    continue
                if match["operation"] != (op["op"] if op else sc.INPUT_TABLE):
                    continue
                if "producer" in match and match["producer"] != producer(op, mismatch.column):
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
                if "requires" in match and not inconsistent(mismatch):
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
                above = _ancestors(mismatch.table, by_name)
                for other, finding in zip(mismatches, out):
                    if finding is not None and other.table in above and _same_symptom(
                            mismatch, other):
                        out[index] = finding
                        changed = True
                        break
        return out


def _ancestors(table: str, by_name: Mapping[str, Mapping[str, Any]]) -> set[str]:
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
