"""Script scenarios: Python that runs on the server, and the variables to compare afterwards.

No Java counterpart: this is the harness. It is the gate of the Python API (`docs/CONSOLE.md`):
a scenario is run on the Java reference with `record-scripts`, and `replay-scripts` runs it on
another server and compares what the script left behind.

A script scenario is one Python file, `scenarios/python/<name>.py`. The file is the script,
and comment lines that start with `# oracle:` say what to do with it:

    # oracle: results t, by_key        the variables to compare; each must hold a table
    # oracle: views v = r, w = r k A   views to compare, of the hierarchical table in `r`:
                                       with its default expansions, and with those of the
                                       key table in `k`, whose action column is `A`
    # oracle: computed by_key.Sum      float columns under the tolerance of PARITY.md A1
    # oracle: fails                    the script of this step must fail on the reference
    # oracle: step                     what follows is the script of the next step

The text before the first `# oracle: step` is the script of step 0; each `step` starts the
script of the next one, which may be empty: a step that only waits. After the script of a
step the runner waits until the server has finished an update cycle (`runner._Barrier`, the
rule of every ticking scenario), then reads every result variable through its `s/<name>`
ticket, and every view through a subscription to it (`views.py`). A variable or a view that
cannot be read is a failure with the gRPC status code, as in a table scenario.

Every script of a scenario runs in the one Python session the server has, where the variables
of earlier scenarios would still be there. So the runner removes, after a scenario, every
global variable that was not there before it, and a scenario can use any name.

A recording is `corpus/python/<name>/`: the manifest and Arrow files of `results.py`, and
`scripts.json` with the outcome of each step's script.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
from pydeephaven import Session
from pydeephaven._table_ops import FetchTableOp
from pydeephaven.table import Table
from pydeephaven.ticket import ScopeTicket

from . import results as res
from . import rules
from . import views as hierarchical
from .connect import open_session
from .results import Failure, ResultSet, ScriptError
from .runner import RunError, _Barrier, _describe, _snapshot, failure_code
from .scenario import ScenarioError

DIRECTIVE = "# oracle:"
OUTCOMES = "scripts.json"
OUTCOMES_FORMAT = 1
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_COLUMN = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$")
# The names the runner itself puts into the session start with this.
_OWN = "__oracle_"

# Sent before and after the scripts of a scenario.
_REMEMBER = f"{_OWN}before = set(globals())\n"
_FORGET = (
    f"for {_OWN}name in [n for n in globals() "
    f"if n not in {_OWN}before and not n.startswith('{_OWN}')]:\n"
    f"    del globals()[{_OWN}name]\n"
    f"globals().pop('{_OWN}name', None)\n"
    f"del {_OWN}before\n"
)


@dataclass(frozen=True)
class Section:
    """The script of one step, and whether it must fail on the reference."""

    script: str
    fails: bool = False


@dataclass(frozen=True)
class Result:
    """One variable to compare. The shape `runner._snapshot` reads."""

    table: str
    ordered: bool = True


@dataclass(frozen=True)
class View:
    """A view to compare under `name`: the hierarchical table in the variable `table`, with
    the expansions of the key table in the variable `keys` (`None`: the default ones), whose
    actions are in the column `action` (`None`: every row expands)."""

    name: str
    table: str
    keys: str | None = None
    action: str | None = None


@dataclass(frozen=True)
class ScriptScenario:
    name: str
    text: str
    sections: tuple[Section, ...]
    results: tuple[Result, ...]
    views: tuple[View, ...]
    # Result variable -> its columns under the A1 tolerance.
    computed: dict[str, frozenset[str]]

    def digest(self) -> str:
        """Identifies the scenario content a recording was made from."""
        return hashlib.sha256(self.text.encode("utf-8")).hexdigest()


def parse(name: str, text: str) -> ScriptScenario:
    """The scenario in `text`; raises `ScenarioError` for anything it does not understand."""

    def fail(message: str) -> ScenarioError:
        return ScenarioError(f"script scenario {name}: {message}")

    if not _NAME.match(name):
        raise fail("the file name must be a Python identifier")
    results: list[str] = []
    views: list[View] = []
    computed: dict[str, set[str]] = {}
    sections: list[Section] = []
    lines: list[str] = []
    fails = False
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.startswith("# oracle"):
            lines.append(line)
            continue
        if not line.startswith(DIRECTIVE + " "):
            raise fail(f"line {number}: a directive is written `{DIRECTIVE} <word> ...`")
        word, _, rest = line[len(DIRECTIVE):].strip().partition(" ")
        items = [item.strip() for item in rest.split(",") if item.strip()]
        # The directive stays in the script as the comment it is, so that the line numbers
        # of a traceback are those of the file.
        lines.append(line)
        if word == "results" and items:
            for item in items:
                if not _NAME.match(item) or item.startswith(_OWN):
                    raise fail(f"line {number}: {item!r} is not a variable name")
                if item in results:
                    raise fail(f"line {number}: result {item!r} is listed twice")
                results.append(item)
        elif word == "views" and items:
            for item in items:
                view, equals, source = (part.strip() for part in item.partition("="))
                words = source.split()
                if not equals or not 1 <= len(words) <= 3 or \
                        not all(_NAME.match(w) for w in [view, *words]) or view.startswith(_OWN):
                    raise fail(f"line {number}: a view is written <name> = <hierarchical table> "
                               "[<key table> [<action column>]]")
                if view in results or any(other.name == view for other in views):
                    raise fail(f"line {number}: result {view!r} is listed twice")
                views.append(View(view, *words))
        elif word == "computed" and items:
            for item in items:
                found = _COLUMN.match(item)
                if not found:
                    raise fail(f"line {number}: a computed column is written <table>.<column>")
                computed.setdefault(found.group(1), set()).add(found.group(2))
        elif word == "fails" and not items:
            if fails:
                raise fail(f"line {number}: `fails` is given twice for one step")
            fails = True
        elif word == "step" and not items:
            lines.pop()
            sections.append(Section("\n".join(lines) + "\n", fails))
            # Empty lines in place of the earlier steps keep the file's line numbers.
            lines, fails = [""] * number, False
        else:
            raise fail(f"line {number}: unknown directive {line[len(DIRECTIVE):].strip()!r}; "
                       "the directives are results, views, computed, fails and step")
    sections.append(Section("\n".join(lines) + "\n", fails))
    if not results and not views:
        raise fail(f"no `{DIRECTIVE} results` or `{DIRECTIVE} views` line: nothing would be "
                   "compared")
    twice = sorted(set(results) & {view.name for view in views})
    if twice:
        raise fail(f"{twice} are both results and views")
    unknown = sorted(set(computed) - set(results) - {view.name for view in views})
    if unknown:
        raise fail(f"computed columns of {unknown}, which are not results")
    for section in sections:
        if section.fails and not _has_code(section.script):
            raise fail("a step without a script cannot fail")
    return ScriptScenario(name, text, tuple(sections), tuple(Result(r) for r in results),
                          tuple(views),
                          {table: frozenset(columns) for table, columns in computed.items()})


def _has_code(script: str) -> bool:
    return any(line.strip() and not line.lstrip().startswith("#")
               for line in script.splitlines())


def load_all(directory: Path, select: list[str] | None = None) -> list[ScriptScenario]:
    """Every script scenario under `directory` (or the one file), sorted by name.

    `select` narrows the set: each entry is a scenario's name or the start of names
    (`agg_`). An entry that selects nothing is an error, never an empty selection.
    """
    paths = [directory] if directory.is_file() else sorted(directory.glob("*.py"))
    scenarios = sorted((parse(path.stem, path.read_text(encoding="utf-8")) for path in paths),
                       key=lambda s: s.name)
    if not select:
        return scenarios
    chosen = []
    for entry in select:
        matching = [s for s in scenarios if s.name.startswith(entry)]
        if not matching:
            raise ScenarioError(f"no script scenario under {directory} is named, or starts "
                                f"with, {entry!r}")
        chosen.extend(s for s in matching if s not in chosen)
    return sorted(chosen, key=lambda s: s.name)


# ---------------------------------------------------------------- what a script answered


_TRACEBACK = "Traceback (most recent call last):"
_TOP_FRAME = re.compile(r'^  File "<string>", line \d+, in <module>$')
_RAISED = re.compile(r"^([A-Za-z_][\w.]*)(?:: (.*))?$")


def parse_error(error_message: str) -> ScriptError:
    """The exception behind the `error_message` of an `ExecuteCommand` answer.

    Both servers put Python's own traceback into it (`oracle/console/java-42.9.txt` and
    `oxhaven.txt`), Java in front of its stack trace. The exception that ended the script is
    named on the first line that is not indented after the frames of the last traceback
    that starts in the script itself (`File "<string>", line N, in <module>`); tracebacks
    before it are the causes, and a `DHError` carries another one inside its message. A
    syntax error has no traceback: it is named on the first line that reads like one.
    """
    lines = error_message.split("\n")
    start = 0
    for index in range(len(lines) - 1):
        if lines[index] == _TRACEBACK and _TOP_FRAME.match(lines[index + 1]):
            start = index + 1
    for line in lines[start:]:
        if not line or line[0].isspace() or line.endswith("Error in Python interpreter:"):
            continue
        found = _RAISED.match(line)
        if found is None:
            continue
        kind, text = found.group(1), found.group(2) or ""
        operation = None
        if kind.rpartition(".")[2] == "DHError":
            # deephaven.dherror.DHError#__str__: "<what failed> : <root cause>"
            operation = text.partition(" : ")[0].strip()
        return ScriptError(kind, text[:300], operation)
    return ScriptError("(unrecognized)", " ".join(error_message.split())[:300])


@dataclass
class Run:
    """What one run of a scenario gave: the outcome of each step's script (`None` when it
    ran to its end) and the result tables of each step."""

    outcomes: list[ScriptError | None]
    tables: ResultSet


def _open(session: Session, name: str) -> Table | Failure:
    """The table behind the scope variable `name`, as `Session.open_table` fetches it; a
    refusal is the variable's `Failure`. (`open_table` itself hides the status code.)"""
    try:
        scope = Table(session=session, ticket=ScopeTicket.scope_ticket(name))
        return session.table_service.grpc_table_op(scope, FetchTableOp())
    except Exception as error:  # noqa: BLE001 - classified just below
        code = failure_code(error)
        if code is None:
            raise
        return Failure(code)


def _view(session: Session, view: View) -> pa.Table | Failure:
    """The first snapshot of `view`; a refusal is its `Failure`."""
    try:
        return hierarchical.read_view(session, view.table, view.keys, view.action)
    except Exception as error:  # noqa: BLE001 - classified just below
        code = failure_code(error)
        if code is None:
            raise
        return Failure(code)


def _execute(session: Session, script: str) -> ScriptError | None:
    answer = session.console_service.run_script(script)
    return parse_error(answer.error_message) if answer.error_message else None


def run_scenario(scenario: ScriptScenario, url: str) -> Run:
    """Runs `scenario` on the server at `url`: step k is the state after the k-th script."""
    out = Run([], ResultSet(scenario.name))
    try:
        session = open_session(url)
    except Exception as error:  # noqa: BLE001
        raise RunError(f"cannot connect to {url}: {_describe(error)}") from error
    try:
        barrier = _Barrier(session)
        failed = _execute(session, _REMEMBER)
        if failed is not None:
            raise RunError(f"the session did not run the runner's own script: {failed}")
        try:
            for step, section in enumerate(scenario.sections):
                ran = _has_code(section.script)
                out.outcomes.append(_execute(session, section.script) if ran else None)
                barrier.wait()
                tables = {result.table: _open(session, result.table)
                          for result in scenario.results}
                _snapshot(session, scenario, tables, step, out.tables)
                for view in scenario.views:
                    out.tables.put(step, view.name, _view(session, view))
        finally:
            failed = _execute(session, _FORGET)
        if failed is not None:
            raise RunError(f"the scenario's variables could not be removed: {failed}")
    except Exception as error:
        raise RunError(f"script scenario {scenario.name} failed on {url}: "
                       f"{_describe(error)}") from error
    finally:
        session.close()
    return out


class Target:
    """A server that script scenarios run against."""

    def __init__(self, url: str):
        self.url = url

    def __str__(self) -> str:
        return self.url

    def run(self, scenario: ScriptScenario) -> Run:
        return run_scenario(scenario, self.url)


def has_python_console(url: str) -> bool:
    """Whether the server at `url` offers a Python console (`GetConsoleTypes`). An Oxhaven
    server built without the `python` feature does not."""
    from deephaven_core.proto import console_pb2, console_pb2_grpc

    session = open_session(url)
    try:
        stub = console_pb2_grpc.ConsoleServiceStub(session.grpc_channel)
        answer = stub.GetConsoleTypes(console_pb2.GetConsoleTypesRequest(),
                                      metadata=session.grpc_metadata, timeout=20)
        return "python" in answer.console_types
    finally:
        session.close()


# ---------------------------------------------------------------- comparing and storing


def compare(scenario: ScriptScenario, expected: Run, actual: Run,
            exact: bool = False) -> list[rules.Mismatch]:
    """The differences between two runs of `scenario`; `expected` is the reference's.

    Step by step: the script's outcome (rule 7 of `rules.py`), then the result tables. A
    step whose script is `missing` ends the comparison: nothing after it can be compared,
    and what was compared before it stands.
    """
    out: list[rules.Mismatch] = []
    steps = len(scenario.sections)
    for step in range(steps):
        found = rules.compare_script_outcome(
            scenario.name, step, expected.outcomes[step], actual.outcomes[step])
        out.extend(found)
        if any(mismatch.kind == rules.MISSING for mismatch in found):
            steps = step
            break
    out.extend(rules.compare(_until(expected.tables, steps), _until(actual.tables, steps),
                             computed=None if exact else scenario.computed))
    return out


def _until(tables: ResultSet, steps: int) -> ResultSet:
    kept = ResultSet(tables.scenario)
    for step, table in tables.keys():
        if step < steps:
            kept.put(step, table, tables.get(step, table))
    return kept


def undeclared(scenario: ScriptScenario, run: Run) -> list[str]:
    """Why a run on the reference cannot be recorded: a script failed without `fails`, or
    did not fail with it. A script that fails by mistake must not become the expectation."""
    out = []
    for step, (section, outcome) in enumerate(zip(scenario.sections, run.outcomes)):
        if outcome is not None and not section.fails:
            out.append(f"the script of step {step} failed ({outcome.type}: {outcome.text}); "
                       f"if it must, mark the step `{DIRECTIVE} fails`")
        elif outcome is None and section.fails:
            out.append(f"the script of step {step} is marked `fails` and did not fail")
    return out


def save(run: Run, directory: Path, scenario: ScriptScenario, reference: str) -> None:
    """Writes the recording of `scenario` under `directory`, replacing an earlier one."""
    res.save(run.tables, directory, scenario.digest(), reference)
    outcomes: list[dict[str, Any]] = []
    for step, outcome in enumerate(run.outcomes):
        if outcome is None:
            outcomes.append({"step": step, "outcome": "ok"})
            continue
        entry = {"step": step, "outcome": "error", "type": outcome.type, "text": outcome.text}
        if outcome.operation is not None:
            entry["operation"] = outcome.operation
        outcomes.append(entry)
    with open(directory / OUTCOMES, "w", encoding="utf-8") as handle:
        json.dump({"format": OUTCOMES_FORMAT, "outcomes": outcomes}, handle, indent=2)
        handle.write("\n")


def load(directory: Path, scenario: ScriptScenario) -> Run:
    """Reads the recording of `scenario`; refuses one made from other scenario content."""
    tables = res.load(directory, scenario.name, scenario.digest())
    path = directory / OUTCOMES
    try:
        with open(path, encoding="utf-8") as handle:
            doc = json.load(handle)
    except (OSError, json.JSONDecodeError) as error:
        raise res.RecordingError(f"cannot read {path}: {error}") from error
    entries = doc.get("outcomes") if isinstance(doc, dict) else None
    if doc.get("format") != OUTCOMES_FORMAT or not isinstance(entries, list) or \
            [entry.get("step") for entry in entries] != list(range(len(scenario.sections))):
        raise res.RecordingError(f"{path} does not hold the outcome of every step of "
                                 f"scenario {scenario.name}; record it again")
    outcomes: list[ScriptError | None] = []
    for entry in entries:
        if entry["outcome"] == "ok":
            outcomes.append(None)
        else:
            outcomes.append(ScriptError(entry["type"], entry["text"], entry.get("operation")))
    return Run(outcomes, tables)
