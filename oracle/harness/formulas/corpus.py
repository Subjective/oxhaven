"""The formula corpus on disk: fixtures, cases and the encoding of cells.

No Java counterpart: harness storage. The files are under `corpus/formulas/`:

    fixtures.json        the input tables, by name
    builtins.json        the default-imported function and constant names (see builtins.py)
    cases/<group>.jsonl  one case per line, with its recorded Java outcome

A fixture is `{"columns": [{"name", "type"}], "rows": [[cell, ...]]}` or `{"empty": n}` (a table
of n rows and no columns, as `emptyTable(n)` makes). Column types are the scenario types
(`bool`, `int8`, `int16`, `int32`, `int64`, `float32`, `float64`, `string`, `timestamp`), `char`
(a UTF-16 code unit, written as its number) and `<type>[]` for an array column. Cells are written
as in a scenario: `null`, and `"NaN"`, `"Infinity"`, `"-Infinity"` for the floats JSON lacks.

A case is one JSON object:

    id       <group>/<name>, unique
    kind     "update": `formula` is an expression, evaluated as `update("R = <formula>")`
             "where": `formula` is a filter string, evaluated as `where("<formula>")`
    fixture  the name of the input table
    setup    optional: operations applied to the fixture before the case, each
             [op, [strings]] with op one of update, select, view, where
    formula  the text sent to the server
    subset   "accepted" or "outside" (see tags.py)
    tags     feature tags (see tags.py)
    source   where the formula comes from: "grid", or a path in the reference checkout and a line
    java     the recorded outcome of the Java reference; absent until recorded

The outcome of an `update` case is the result column: `{"type": <deephaven:type>, "arrow":
<Arrow type>, "values": [cell per input row]}`, with `"component"` when the column has a
`deephaven:componentType`. Values that are not deterministic are left out and the outcome says
`"nondeterministic": true`; `"values": null` means the pinned pyarrow cannot read the column's
Arrow type. The outcome of a `where` case is `{"rows": [positions kept]}`, the
positions counted from 0 in the table the filter was applied to. A failed case is
`{"error": {"status": <gRPC status name>, "stage": "formula" | "setup" | "snapshot",
"message": <first line of the Java exception>, "cause": <first line of its root cause>}}`.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from .. import scenario as sc

ORACLE = Path(__file__).resolve().parent.parent.parent
DEFAULT_DIR = ORACLE / "corpus" / "formulas"
FIXTURES = "fixtures.json"
BUILTINS = "builtins.json"
CASES = "cases"
FORMAT = 1
KINDS = ("update", "where")
SETUP_OPS = ("update", "select", "view", "where")
# The position column the harness adds to every input table, so that a `where` outcome can be
# stated as positions. It is a `long` and no formula in the corpus may use the name.
POSITION = "OxIdx"
_ID = re.compile(r"^[a-z0-9-]+/[A-Za-z0-9_.\-]+$")


class CorpusError(ValueError):
    """The corpus files are not valid."""


def arrow_type(type_name: str) -> pa.DataType:
    if type_name.endswith("[]"):
        return pa.list_(arrow_type(type_name[:-2]))
    if type_name == "char":
        return pa.uint16()
    if type_name not in sc.COLUMN_TYPES:
        raise CorpusError(f"unknown column type {type_name!r}")
    return sc.COLUMN_TYPES[type_name]


def decode_cell(where: str, type_name: str, value: Any) -> Any:
    if value is None:
        return None
    if type_name.endswith("[]"):
        if not isinstance(value, list):
            raise CorpusError(f"{where}: expected a list, got {value!r}")
        return [decode_cell(where, type_name[:-2], item) for item in value]
    if type_name == "char":
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 0xFFFF:
            raise CorpusError(f"{where}: expected a code unit 0..65535, got {value!r}")
        return value
    try:
        return sc.decode_cell(where, type_name, value)
    except sc.ScenarioError as error:
        raise CorpusError(str(error)) from error


@dataclass(frozen=True)
class Fixture:
    name: str
    columns: tuple[sc.Column, ...]  # empty for an `empty` fixture
    rows: tuple[tuple[Any, ...], ...]
    empty: int | None  # row count of an `emptyTable(n)` fixture

    @property
    def size(self) -> int:
        return self.empty if self.empty is not None else len(self.rows)

    def column_types(self) -> dict[str, str]:
        return {c.name: c.type for c in self.columns}

    def arrow(self) -> pa.Table:
        """The table to upload: the columns, then the position column."""
        arrays, fields = [], []
        for index, column in enumerate(self.columns):
            kind = arrow_type(column.type)
            where = f"fixture {self.name} column {column.name}"
            cells = [decode_cell(where, column.type, row[index]) for row in self.rows]
            arrays.append(pa.array(cells, kind))
            fields.append(pa.field(column.name, kind))
        arrays.append(pa.array(range(len(self.rows)), pa.int64()))
        fields.append(pa.field(POSITION, pa.int64()))
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def parse_fixture(name: str, doc: Any) -> Fixture:
    if not isinstance(doc, dict):
        raise CorpusError(f"fixture {name}: expected an object")
    if set(doc) == {"empty"}:
        if isinstance(doc["empty"], bool) or not isinstance(doc["empty"], int) or doc["empty"] < 0:
            raise CorpusError(f"fixture {name}: empty needs a row count")
        return Fixture(name, (), (), doc["empty"])
    if set(doc) != {"columns", "rows"}:
        raise CorpusError(f"fixture {name}: expected columns and rows, or empty")
    columns = tuple(sc.Column(c["name"], c["type"]) for c in doc["columns"])
    names = [c.name for c in columns]
    if len(set(names)) != len(names) or POSITION in names:
        raise CorpusError(f"fixture {name}: column names must be unique and not {POSITION}")
    rows = []
    for number, row in enumerate(doc["rows"]):
        if not isinstance(row, list) or len(row) != len(columns):
            raise CorpusError(f"fixture {name} row {number}: expected {len(columns)} cells")
        for column, cell in zip(columns, row):
            decode_cell(f"fixture {name} row {number} column {column.name}", column.type, cell)
        rows.append(tuple(row))
    return Fixture(name, columns, tuple(rows), None)


def fixture_document(fixture: Fixture) -> dict[str, Any]:
    if fixture.empty is not None:
        return {"empty": fixture.empty}
    return {
        "columns": [{"name": c.name, "type": c.type} for c in fixture.columns],
        "rows": [list(row) for row in fixture.rows],
    }


@dataclass
class Case:
    id: str
    kind: str
    fixture: str
    formula: str
    subset: str
    tags: list[str]
    source: str
    setup: list[tuple[str, list[str]]]
    java: dict[str, Any] | None = None

    @property
    def group(self) -> str:
        return self.id.split("/", 1)[0]

    def setup_key(self) -> str:
        return json.dumps([self.fixture, self.setup])

    def document(self) -> dict[str, Any]:
        doc: dict[str, Any] = {"id": self.id, "kind": self.kind, "fixture": self.fixture}
        if self.setup:
            doc["setup"] = [[op, list(strings)] for op, strings in self.setup]
        doc.update(formula=self.formula, subset=self.subset, tags=self.tags, source=self.source)
        if self.java is not None:
            doc["java"] = self.java
        return doc


def parse_case(where: str, doc: Any) -> Case:
    if not isinstance(doc, dict):
        raise CorpusError(f"{where}: expected an object")
    required = {"id", "kind", "fixture", "formula", "subset", "tags", "source"}
    extra = set(doc) - required - {"setup", "java"}
    if required - set(doc) or extra:
        raise CorpusError(f"{where}: missing {sorted(required - set(doc))}, unknown {sorted(extra)}")
    if not _ID.match(doc["id"]):
        raise CorpusError(f"{where}: bad id {doc['id']!r}")
    if doc["kind"] not in KINDS:
        raise CorpusError(f"{where}: kind must be one of {KINDS}")
    if doc["subset"] not in ("accepted", "outside"):
        raise CorpusError(f"{where}: subset must be accepted or outside")
    setup = []
    for step in doc.get("setup", []):
        if (not isinstance(step, list) or len(step) != 2 or step[0] not in SETUP_OPS
                or not all(isinstance(s, str) for s in step[1])):
            raise CorpusError(f"{where}: bad setup step {step!r}")
        setup.append((step[0], list(step[1])))
    return Case(
        id=doc["id"], kind=doc["kind"], fixture=doc["fixture"], formula=doc["formula"],
        subset=doc["subset"], tags=list(doc["tags"]), source=doc["source"], setup=setup,
        java=doc.get("java"),
    )


@dataclass
class Corpus:
    fixtures: dict[str, Fixture]
    cases: list[Case]

    def groups(self) -> dict[str, list[Case]]:
        out: dict[str, list[Case]] = {}
        for case in self.cases:
            out.setdefault(case.group, []).append(case)
        return out


def load(directory: Path = DEFAULT_DIR) -> Corpus:
    with open(directory / FIXTURES, encoding="utf-8") as handle:
        doc = json.load(handle)
    if doc.get("format") != FORMAT:
        raise CorpusError(f"{directory / FIXTURES}: unknown format")
    fixtures = {name: parse_fixture(name, item) for name, item in doc["fixtures"].items()}
    cases: list[Case] = []
    seen: set[str] = set()
    for path in sorted((directory / CASES).glob("*.jsonl")):
        with open(path, encoding="utf-8") as handle:
            for number, line in enumerate(handle, start=1):
                where = f"{path.name}:{number}"
                case = parse_case(where, json.loads(line))
                if case.group != path.stem:
                    raise CorpusError(f"{where}: id {case.id} does not belong in this file")
                if case.id in seen:
                    raise CorpusError(f"{where}: duplicate id {case.id}")
                if case.fixture not in fixtures:
                    raise CorpusError(f"{where}: unknown fixture {case.fixture}")
                seen.add(case.id)
                cases.append(case)
    return Corpus(fixtures, cases)


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(", ", ": "))


def save_fixtures(directory: Path, fixtures: dict[str, Fixture]) -> None:
    lines = ["{", f'  "format": {FORMAT},', '  "fixtures": {']
    names = sorted(fixtures)
    for index, name in enumerate(names):
        fixture = fixtures[name]
        last = "" if index + 1 == len(names) else ","
        if fixture.empty is not None:
            lines.append(f"    {_dump(name)}: {_dump(fixture_document(fixture))}{last}")
            continue
        lines.append(f"    {_dump(name)}: {{")
        lines.append('      "columns": [')
        for number, column in enumerate(fixture.columns):
            comma = "" if number + 1 == len(fixture.columns) else ","
            lines.append(f"        {_dump({'name': column.name, 'type': column.type})}{comma}")
        lines.append("      ],")
        lines.append('      "rows": [')
        for number, row in enumerate(fixture.rows):
            comma = "" if number + 1 == len(fixture.rows) else ","
            lines.append(f"        {_dump(list(row))}{comma}")
        lines.append("      ]")
        lines.append(f"    }}{last}")
    lines += ["  }", "}"]
    directory.mkdir(parents=True, exist_ok=True)
    (directory / FIXTURES).write_text("\n".join(lines) + "\n", encoding="utf-8")


def save_cases(directory: Path, cases: list[Case], remove_stale: bool = False) -> None:
    """Writes each group's file. With `remove_stale`, deletes the files of groups not in `cases`."""
    out = directory / CASES
    out.mkdir(parents=True, exist_ok=True)
    groups: dict[str, list[Case]] = {}
    for case in cases:
        groups.setdefault(case.group, []).append(case)
    for group, members in groups.items():
        text = "".join(_dump(case.document()) + "\n" for case in members)
        (out / f"{group}.jsonl").write_text(text, encoding="utf-8")
    if remove_stale:
        for path in out.glob("*.jsonl"):
            if path.stem not in groups:
                path.unlink()
