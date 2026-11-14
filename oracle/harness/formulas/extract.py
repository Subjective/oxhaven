"""Extracts formula and filter strings from the reference checkout.

No Java counterpart: this reads the reference's tests and docs for the formulas people write. It
takes the text of each formula and an input table to run it against; the outcome comes from
running it on Java, never from the test's own assertions.

Sources and how the input table of each formula is found:

    java-parser          lang/TestQueryLanguageParser.java. Every `expression = "..."`, run
                         against the fixture `parser`, whose columns are the test's variables
                         that a table column can hold (primitives, String, Instant, arrays).
    java-formula-column  select/TestFormulaColumn.java. Every `check...("...")` expression, run
                         against the fixture `formula_column`, a copy of the test's own table.
    java-engine          Every test under engine/table/src/test/java: method chains that start
                         at `emptyTable(n)` and continue with update, updateView, lazyUpdate,
                         select, view or where on string literals. The chain before a formula is
                         its `setup`.
    py-server            py/server/tests: the same chains in Python (`empty_table(n)`,
                         `new_table([...])` of literal columns, and the suite's `test_table`).
    docs-python          The Python code blocks of docs/python, read the same way.

A formula is left out when it names something that is neither a column of its input table nor a
default import (a query scope variable, a Python or Groovy function, a test class): such a
formula depends on state the corpus cannot reproduce. `emptyTable(n)` is capped at 32 rows.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
import re
import warnings
from dataclasses import dataclass, field
from pathlib import Path

from .. import scenario as sc
from . import corpus as cp
from .corpus import Fixture
from .grid import Draft
from .tags import Builtins, classify

EMPTY_CAP = 32
NEW_TABLE_CAP = 64
ENGINE_TESTS = "engine/table/src/test/java"
PARSER_TEST = ENGINE_TESTS + "/io/deephaven/engine/table/impl/lang/TestQueryLanguageParser.java"
FORMULA_COLUMN_TEST = ENGINE_TESTS + "/io/deephaven/engine/table/impl/select/TestFormulaColumn.java"
PY_TESTS = "py/server/tests"
PY_TEST_TABLE = PY_TESTS + "/data/test_table.csv"
DOCS = "docs/python"

JAVA_OPS = {"update": "update", "updateView": "update", "lazyUpdate": "update",
            "select": "select", "view": "view", "where": "where"}
PY_OPS = {"update": "update", "update_view": "update", "lazy_update": "update",
          "select": "select", "view": "view", "where": "where"}
PY_COLUMNS = {"bool_col": "bool", "byte_col": "int8", "short_col": "int16", "int_col": "int32",
              "long_col": "int64", "float_col": "float32", "double_col": "float64",
              "string_col": "string", "char_col": "char"}

# Names that only exist inside the reference's test classes.
_TEST_ONLY = re.compile(r"LanguageParserDummy|SubclassOf|TestFormulaColumn|PyCallable|TestClass")
_ASSIGNMENT = re.compile(r"^\s*([A-Za-z_]\w*)\s*=(?!=)\s*(.+?)\s*$", re.S)
_BARE_COLUMN = re.compile(r"^\s*([A-Za-z_]\w*)\s*$")
_JAVA_STRING = r'"(?:\\.|[^"\\\n])*"'
_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f", "0": "\0", '"': '"',
            "'": "'", "\\": "\\"}


def java_unescape(literal: str) -> str:
    """The value of a Java string literal, given with its quotes."""
    body, out, position = literal[1:-1], [], 0
    while position < len(body):
        char = body[position]
        if char != "\\":
            out.append(char)
            position += 1
            continue
        following = body[position + 1]
        if following == "u":
            out.append(chr(int(body[position + 2:position + 6], 16)))
            position += 6
        else:
            out.append(_ESCAPES.get(following, following))
            position += 2
    return "".join(out)


def _slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.\-]", "-", text)


@dataclass
class Lineage:
    """A table whose content the corpus can rebuild: a fixture and the operations applied."""

    fixture: str
    setup: list[tuple[str, list[str]]]
    columns: dict[str, str]

    def then(self, op: str, strings: list[str], columns: dict[str, str]) -> "Lineage":
        return Lineage(self.fixture, self.setup + [(op, list(strings))], columns)


@dataclass
class Extractor:
    reference: Path
    builtins: Builtins
    fixtures: dict[str, Fixture] = field(default_factory=dict)
    drafts: list[Draft] = field(default_factory=list)
    seen: set[str] = field(default_factory=set)
    counts: dict[str, dict[str, int]] = field(default_factory=dict)

    # ------------------------------------------------------------------ emitting

    def _count(self, group: str, what: str) -> None:
        bucket = self.counts.setdefault(group, {})
        bucket[what] = bucket.get(what, 0) + 1

    def _emit(self, group: str, name: str, kind: str, formula: str, source: str,
              lineage: Lineage) -> bool:
        """Adds a case unless the formula depends on something outside its input table."""
        if _TEST_ONLY.search(formula) or cp.POSITION in formula or not formula.strip():
            self._count(group, "left out: test-only name")
            return False
        verdict = classify(formula, kind, lineage.columns, self.builtins)
        if verdict.unknown or "unreadable" in verdict.tags:
            self._count(group, "left out: names something outside the table")
            return False
        key = json.dumps([group, kind, lineage.fixture, lineage.setup, formula])
        if key in self.seen:
            self._count(group, "left out: duplicate")
            return True
        self.seen.add(key)
        name, repeat = _slug(name), 1
        while f"{group}/{name}" in self.seen:
            repeat += 1
            name = f"{_slug(name).rsplit('-r', 1)[0]}-r{repeat}"
        self.seen.add(f"{group}/{name}")
        self.drafts.append(Draft(f"{group}/{name}", kind, lineage.fixture, formula, source,
                                 list(lineage.setup), dict(lineage.columns)))
        self._count(group, "cases")
        return True

    def _apply(self, group: str, name: str, source: str, lineage: Lineage, op: str,
               strings: list[str]) -> Lineage | None:
        """Emits the cases of one operation and returns the table it produces."""
        if op == "where":
            good = True
            for number, text in enumerate(strings):
                before = lineage.then("where", strings[:number], lineage.columns) \
                    if number else lineage
                good &= self._emit(group, f"{name}.{number}", "where", text, source, before)
            return lineage.then("where", strings, lineage.columns) if good and strings else None
        columns = dict(lineage.columns)
        result = {} if op in ("select", "view") else columns
        good = True
        for number, text in enumerate(strings):
            bare = _BARE_COLUMN.match(text)
            if bare and bare.group(1) in columns:
                result[bare.group(1)] = columns[bare.group(1)]
                continue
            assignment = _ASSIGNMENT.match(text)
            if not assignment:
                return None
            before = Lineage(lineage.fixture, lineage.setup, dict(columns))
            if number:
                before = before.then(op, strings[:number], dict(columns))
            good &= self._emit(group, f"{name}.{number}", "update", assignment.group(2), source,
                               before)
            columns[assignment.group(1)] = "?"
            result[assignment.group(1)] = "?"
        if not good or not strings:
            return None
        return lineage.then(op, strings, dict(result))

    def _empty(self, rows: int) -> Lineage:
        rows = min(rows, EMPTY_CAP)
        name = f"empty_{rows}"
        self.fixtures[name] = Fixture(name, (), (), rows)
        return Lineage(name, [], {})

    # ------------------------------------------------------------------ Java

    def _java_literals(self, path: str, pattern: str, group: str, fixture: Fixture) -> None:
        text = (self.reference / path).read_text(encoding="utf-8")
        lineage = Lineage(fixture.name, [], fixture.column_types())
        self.fixtures[fixture.name] = fixture
        per_line: dict[int, int] = {}
        for match in re.finditer(pattern, text):
            formula = "".join(java_unescape(m) for m in re.findall(_JAVA_STRING, match.group(1)))
            line = text.count("\n", 0, match.start(1)) + 1
            number = per_line.get(line, 0)
            per_line[line] = number + 1
            self._emit(group, f"L{line}.{number}", "update", formula, f"{path}:{line}", lineage)

    def _parser_tests(self) -> None:
        columns = (
            ("myByte", "int8", [1, -3, None]), ("myShort", "int16", [2, -300, None]),
            ("myChar", "char", [97, 48, None]), ("myInt", "int32", [3, -70000, None]),
            ("myOtherInt", "int32", [2, 7, None]), ("myLong", "int64", [4, -5000000000, None]),
            ("myOtherLong", "int64", [3, 11, None]), ("myFloat", "float32", [1.5, -2.25, None]),
            ("myDouble", "float64", [2.5, -0.125, None]), ("myBoolean", "bool", [True, False, None]),
            ("myString", "string", ["hello", "", None]),
            ("myIntArray", "int32[]", [[1, 2, 3], [4, None, 6], [7, 8, 9]]),
            ("myLongArray", "int64[]", [[1, 2, 3], [4, None, 6], [7, 8, 9]]),
            ("myDoubleArray", "float64[]", [[1.5, 2.5, 3.5], [4.5, None, "NaN"], [7.5, 8.5, 9.5]]),
            ("myInstant", "timestamp", [1554989400000000000, 0, None]),
            ("ExampleQuantity", "int32", [10, 20, None]),
            ("ExampleQuantity2", "float64", [1.5, 2.5, None]),
            ("ExampleQuantity3", "float64", [3.5, 4.5, None]),
            ("ExampleQuantity4", "float64", [5.5, 6.5, None]),
            ("ExampleStr", "string", ["a", "b", None]),
        )
        fixture = Fixture(
            "parser", tuple(sc.Column(name, kind) for name, kind, _values in columns),
            tuple(tuple(values[row] for _n, _k, values in columns) for row in range(3)), None)
        pattern = rf"expression\s*=\s*((?:{_JAVA_STRING}\s*\+?\s*)+);"
        self._java_literals(PARSER_TEST, pattern, "java-parser", fixture)

    def _formula_column_tests(self) -> None:
        base = [0, 65, None]
        columns = (("BooleanCol", "bool", [False, True, None]), ("CharCol", "char", base),
                   ("ByteCol", "int8", base), ("ShortCol", "int16", base),
                   ("IntCol", "int32", base), ("LongCol", "int64", base),
                   ("FloatCol", "float32", base), ("DoubleCol", "float64", base))
        fixture = Fixture(
            "formula_column", tuple(sc.Column(name, kind) for name, kind, _values in columns),
            tuple(tuple(values[row] for _n, _k, values in columns) for row in range(3)), None)
        pattern = (rf"(?:check(?:Primitive)?|checkExpecting\w+)\(\s*(?:[\w.\[\]]+\s*,\s*)?"
                   rf"((?:{_JAVA_STRING}\s*\+?\s*)+)[,)]")
        self._java_literals(FORMULA_COLUMN_TEST, pattern, "java-formula-column", fixture)
        text = (self.reference / FORMULA_COLUMN_TEST).read_text(encoding="utf-8")
        lineage = Lineage(fixture.name, [], fixture.column_types())
        for match in re.finditer(rf"expression\s*=\s*((?:{_JAVA_STRING}\s*\+?\s*)+);", text):
            formula = "".join(java_unescape(m) for m in re.findall(_JAVA_STRING, match.group(1)))
            line = text.count("\n", 0, match.start(1)) + 1
            self._emit("java-formula-column", f"L{line}.e", "update", formula,
                       f"{FORMULA_COLUMN_TEST}:{line}", lineage)

    _JAVA_START = re.compile(
        r"(?:\bTableTools\.)?\bemptyTable\(\s*([\d_]+)\s*\)|\b([A-Za-z_]\w*)(?=\s*\.\s*"
        r"(?:update|updateView|lazyUpdate|select|view|where)\s*\()")
    _JAVA_CALL = re.compile(r"\s*\.\s*(\w+)\s*\(")
    _JAVA_ARGS = re.compile(
        rf"\s*((?:{_JAVA_STRING}\s*(?:\+\s*)?)+)\s*(,|\))")
    _JAVA_METHOD = re.compile(r"\n    (?:public|private|protected|static|void)[^;\n]*\([^;]*\{")
    _JAVA_BIND = re.compile(r"\b([A-Za-z_]\w*)\s*=\s*(?:\([\w.<>]+\)\s*)?$")

    def _java_chains(self) -> None:
        root = self.reference / ENGINE_TESTS
        for path in sorted(root.rglob("*.java")):
            relative = str(path.relative_to(self.reference))
            if relative in (PARSER_TEST, FORMULA_COLUMN_TEST):
                continue
            text = path.read_text(encoding="utf-8")
            if "emptyTable(" not in text:
                continue
            boundaries = [m.start() for m in self._JAVA_METHOD.finditer(text)]
            bound: dict[str, Lineage] = {}
            position, boundary = 0, 0
            while True:
                start = self._JAVA_START.search(text, position)
                if not start:
                    break
                while boundary < len(boundaries) and boundaries[boundary] < start.start():
                    boundary += 1
                    bound = {}
                position = start.end()
                if start.group(1) is not None:
                    lineage: Lineage | None = self._empty(int(start.group(1).replace("_", "")))
                elif start.group(2) in bound:
                    lineage = bound[start.group(2)]
                else:
                    continue
                call_number = 0
                while lineage is not None:
                    call = self._JAVA_CALL.match(text, position)
                    if not call or call.group(1) not in JAVA_OPS:
                        break
                    cursor, strings, complete = call.end(), [], False
                    while True:
                        argument = self._JAVA_ARGS.match(text, cursor)
                        if not argument:
                            break
                        strings.append("".join(
                            java_unescape(m) for m in re.findall(_JAVA_STRING, argument.group(1))))
                        cursor = argument.end()
                        if argument.group(2) == ")":
                            complete = True
                            break
                    if not complete:
                        lineage = None
                        break
                    line = text.count("\n", 0, call.start(1)) + 1
                    name = f"{path.stem}.L{line}.{call_number}"
                    call_number += 1
                    lineage = self._apply("java-engine", name, f"{relative}:{line}", lineage,
                                          JAVA_OPS[call.group(1)], strings)
                    position = cursor
                target = self._JAVA_BIND.search(text[max(0, start.start() - 120):start.start()])
                if target:
                    if lineage is not None and re.match(r"\s*;", text[position:position + 20]):
                        bound[target.group(1)] = lineage
                    else:
                        bound.pop(target.group(1), None)

    # ------------------------------------------------------------------ Python

    def _py_test_table(self) -> Lineage:
        name = "py_test_table"
        if name not in self.fixtures:
            with open(self.reference / PY_TEST_TABLE, encoding="utf-8", newline="") as handle:
                rows = list(csv.reader(handle))
            columns = tuple(sc.Column(column, "int32") for column in rows[0])
            data = tuple(tuple(int(cell) for cell in row) for row in rows[1:21])
            self.fixtures[name] = Fixture(name, columns, data, None)
        return Lineage(name, [], self.fixtures[name].column_types())

    def _py_new_table(self, node: ast.Call) -> Lineage | None:
        arguments = list(node.args) + [k.value for k in node.keywords if k.arg == "cols"]
        if len(arguments) != 1 or not isinstance(arguments[0], (ast.List, ast.Tuple)):
            return None
        columns, data = [], []
        for item in arguments[0].elts:
            if not (isinstance(item, ast.Call) and isinstance(item.func, ast.Name)
                    and item.func.id in PY_COLUMNS and len(item.args) == 2 and not item.keywords):
                return None
            kind = PY_COLUMNS[item.func.id]
            try:
                name, values = ast.literal_eval(item.args[0]), ast.literal_eval(item.args[1])
            except (ValueError, SyntaxError):
                return None
            if not isinstance(name, str) or not isinstance(values, (list, tuple)):
                return None
            if kind == "char":
                values = [ord(v) if isinstance(v, str) and len(v) == 1 else v for v in values]
            if kind in ("float32", "float64"):
                values = [float(v) if isinstance(v, int) and not isinstance(v, bool) else v
                          for v in values]
            try:
                for value in values:
                    cp.decode_cell("new_table", kind, value)
            except cp.CorpusError:
                return None
            columns.append(sc.Column(name, kind))
            data.append([sc.encode_cell(v) for v in values])
        if not columns or len({len(values) for values in data}) != 1:
            return None
        rows = tuple(zip(*data))
        if not 0 < len(rows) <= NEW_TABLE_CAP or len({c.name for c in columns}) != len(columns):
            return None
        document = json.dumps([[c.name, c.type] for c in columns] + [list(r) for r in rows])
        name = "table_" + hashlib.sha256(document.encode()).hexdigest()[:10]
        self.fixtures[name] = Fixture(name, tuple(columns), rows, None)
        return Lineage(name, [], {c.name: c.type for c in columns})

    @staticmethod
    def _py_strings(node: ast.Call) -> list[str] | None:
        values = list(node.args) + [k.value for k in node.keywords
                                    if k.arg in ("formulas", "filters")]
        if len(values) != 1 or len(node.keywords) > 1:
            return None
        value = values[0]
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return [value.value]
        if isinstance(value, (ast.List, ast.Tuple)) and all(
                isinstance(e, ast.Constant) and isinstance(e.value, str) for e in value.elts):
            return [e.value for e in value.elts]
        return None

    def _py_eval(self, node: ast.AST, env: dict[str, Lineage], group: str, stem: str,
                 source: str, offset: int) -> Lineage | None:
        """The table an expression evaluates to, if the corpus can rebuild it."""
        if isinstance(node, ast.Name):
            return env.get(node.id)
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            return env.get(f"{node.value.id}.{node.attr}")
        if isinstance(node, ast.Call):
            function = node.func
            if isinstance(function, ast.Name):
                if function.id == "empty_table" and len(node.args) == 1 and not node.keywords \
                        and isinstance(node.args[0], ast.Constant) \
                        and isinstance(node.args[0].value, int):
                    return self._empty(node.args[0].value)
                if function.id == "new_table":
                    return self._py_new_table(node)
                if function.id == "read_csv" and node.args \
                        and isinstance(node.args[0], ast.Constant) \
                        and str(node.args[0].value).endswith("data/test_table.csv"):
                    return self._py_test_table()
            if isinstance(function, ast.Attribute):
                receiver = self._py_eval(function.value, env, group, stem, source, offset)
                if receiver is not None and function.attr in PY_OPS:
                    if function.attr == "select" and not node.args and not node.keywords:
                        return receiver
                    strings = self._py_strings(node)
                    if strings is not None:
                        line = node.lineno + offset
                        return self._apply(group, f"{stem}.L{line}.{node.col_offset}",
                                           f"{source}:{line}", receiver, PY_OPS[function.attr],
                                           strings)
                    return None
                for argument in list(node.args) + [k.value for k in node.keywords]:
                    self._py_eval(argument, env, group, stem, source, offset)
                return None
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.expr):
                self._py_eval(child, env, group, stem, source, offset)
        return None

    def _py_walk(self, node: ast.AST, env: dict[str, Lineage], group: str, stem: str,
                 source: str, offset: int) -> None:
        arguments = (group, stem, source, offset)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            local = dict(env)
            for statement in node.body:
                self._py_walk(statement, local, *arguments)
            return
        if isinstance(node, ast.ClassDef):
            for statement in node.body:
                if isinstance(statement, ast.FunctionDef) and statement.name == "setUp":
                    for inner in statement.body:
                        self._py_walk(inner, env, *arguments)
            for statement in node.body:
                self._py_walk(statement, env, *arguments)
            return
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            value = self._py_eval(node.value, env, *arguments)
            key = None
            if isinstance(target, ast.Name):
                key = target.id
            elif isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name):
                key = f"{target.value.id}.{target.attr}"
            if key is not None:
                if value is not None:
                    env[key] = value
                else:
                    env.pop(key, None)
            return
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.stmt):
                self._py_walk(child, env, *arguments)
            elif isinstance(child, ast.expr):
                self._py_eval(child, env, *arguments)
            elif isinstance(child, (ast.ExceptHandler, ast.withitem, ast.match_case)):
                self._py_walk(child, env, *arguments)

    def _py_source(self, code: str, group: str, stem: str, source: str, offset: int,
                   env: dict[str, Lineage]) -> None:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", SyntaxWarning)
                tree = ast.parse(code)
        except (SyntaxError, ValueError):
            self._count(group, "code that does not parse")
            return
        for statement in tree.body:
            self._py_walk(statement, env, group, stem, source, offset)

    def _py_tests(self) -> None:
        for path in sorted((self.reference / PY_TESTS).glob("*.py")):
            relative = str(path.relative_to(self.reference))
            self._py_source(path.read_text(encoding="utf-8"), "py-server", path.stem, relative,
                            0, {})

    def _docs(self) -> None:
        root = self.reference / DOCS
        block = re.compile(r"^```python[^\n]*\n(.*?)^```", re.S | re.M)
        for path in sorted(root.rglob("*.md")):
            relative = str(path.relative_to(self.reference))
            stem = _slug(str(path.relative_to(root).with_suffix("")).replace("/", "."))
            text = path.read_text(encoding="utf-8")
            env: dict[str, Lineage] = {}
            for match in block.finditer(text):
                offset = text.count("\n", 0, match.start(1))
                self._py_source(match.group(1), "docs-python", stem, relative, offset, env)

    # ------------------------------------------------------------------ all of it

    def run(self) -> tuple[list[Draft], dict[str, Fixture], list[str]]:
        self._parser_tests()
        self._formula_column_tests()
        self._java_chains()
        self._py_tests()
        self._docs()
        report = []
        for group in sorted(self.counts):
            parts = ", ".join(f"{count} {what}" for what, count in sorted(self.counts[group].items()))
            report.append(f"extract {group}: {parts}")
        return self.drafts, self.fixtures, report
