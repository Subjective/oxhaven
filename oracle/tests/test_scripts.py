"""Script scenarios: the file format, what a script answered, rule 7, recordings, the gate.

Needs the recordings but no server.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from harness import baseline as bl
from harness import gate, results as res, rules, scripts
from harness.__main__ import main
from harness.results import Failure, ResultSet, ScriptError
from harness.scenario import ScenarioError

ORACLE = Path(__file__).resolve().parent.parent
SCENARIOS = ORACLE / "scenarios" / "python"
CORPUS = ORACLE / "corpus" / "python"

TEXT = """\
# What it is for.
# oracle: results t, u
# oracle: computed u.Sum, u.Avg
from deephaven import empty_table
t = empty_table(1)
# oracle: step
# oracle: fails
u = t.update("X = Nope")
# oracle: step
"""

# What the two servers answer to `t.update("X = nosuchcolumn")`, shortened: the cause comes
# first, the exception that ended the script last, and a DHError repeats a traceback inside
# its message. Java wraps all of it in its own exception and appends its stack.
JAVA_DHERROR = (
    "java.lang.RuntimeException: Error in Python interpreter:\n"
    "Traceback (most recent call last):\n"
    '  File "/site-packages/deephaven/table.py", line 1372, in update\n'
    "    return Table(j_table=self.j_table.update(*formulas))\n"
    "RuntimeError: io.deephaven.engine.table.impl.select.FormulaCompilationException: boom\n"
    "\n"
    "The above exception was the direct cause of the following exception:\n"
    "\n"
    "Traceback (most recent call last):\n"
    '  File "<string>", line 3, in <module>\n'
    '  File "/site-packages/deephaven/table.py", line 1374, in update\n'
    '    raise DHError(e, "table update operation failed.") from e\n'
    "deephaven.dherror.DHError: table update operation failed. : Cannot find variable\n"
    "Traceback (most recent call last):\n"
    '  File "/site-packages/deephaven/table.py", line 1372, in update\n'
    "RuntimeError: io.deephaven.engine.table.impl.select.FormulaCompilationException: boom\n"
    "\n"
    "\tat org.jpy.PyLib.executeCode(Native Method)\n"
)
OXHAVEN_DHERROR = (
    "Error in Python interpreter:\n"
    "Traceback (most recent call last):\n"
    '  File "<oxhaven>/deephaven/table.py", line 51, in _op\n'
    "RuntimeError: X = nosuchcolumn: type error: cannot find column nosuchcolumn\n"
    "\n"
    "The above exception was the direct cause of the following exception:\n"
    "\n"
    "Traceback (most recent call last):\n"
    '  File "<string>", line 3, in <module>\n'
    '  File "<oxhaven>/deephaven/table.py", line 61, in update\n'
    "deephaven.dherror.DHError: table update operation failed. : X = nosuchcolumn: type "
    "error: cannot find column nosuchcolumn\n"
)


def error(kind: str, text: str = "", operation: str | None = None) -> ScriptError:
    return ScriptError(kind, text, operation)


# ---------------------------------------------------------------- the file format


def test_a_scenario_is_split_into_steps_and_keeps_its_line_numbers() -> None:
    scenario = scripts.parse("example", TEXT)
    assert [r.table for r in scenario.results] == ["t", "u"]
    assert scenario.computed == {"u": frozenset({"Sum", "Avg"})}
    assert [s.fails for s in scenario.sections] == [False, True, False]
    first, second, third = (s.script.splitlines() for s in scenario.sections)
    assert first[4] == "t = empty_table(1)" and len(first) == 5
    # the line of the file that a traceback of step 1 names
    assert second[7] == 'u = t.update("X = Nope")' and not any(second[:6])
    assert not any(line.strip() for line in third)


def test_views_name_a_hierarchical_table_its_key_table_and_its_action_column() -> None:
    scenario = scripts.parse(
        "example", "# oracle: views all = r, some = r k, acted = r k A\n# oracle: computed some.Sum\n")
    assert scenario.results == ()
    assert scenario.views == (
        scripts.View("all", "r"),
        scripts.View("some", "r", "k"),
        scripts.View("acted", "r", "k", "A"),
    )
    assert scenario.computed == {"some": frozenset({"Sum"})}


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("t = 1\n", "nothing would be compared"),
        ("# oracle: results t, t\n", "listed twice"),
        ("# oracle: results 1t\n", "not a variable name"),
        ("# oracle: results __oracle_x\n", "not a variable name"),
        ("# oracle: results t\n# oracle: computed u.Sum\n", "not results"),
        ("# oracle: results t\n# oracle: computed Sum\n", "<table>.<column>"),
        ("# oracle: results t\n# oracle: ordered t\n", "unknown directive"),
        ("# oracle: views v\n", "a view is written"),
        ("# oracle: views v = r k A more\n", "a view is written"),
        ("# oracle: views v = r, v = r k\n", "listed twice"),
        ("# oracle: results v\n# oracle: views v = r\n", "listed twice"),
        ("# oracle: results t\n# oracle: step 2\n", "unknown directive"),
        ("# oracle: results t\n# oracle:fails\n", "a directive is written"),
        ("# oracle: results t\n# oracle: fails\n# oracle: fails\nt = 1\n", "given twice"),
        ("# oracle: results t\nt = 1\n# oracle: step\n# oracle: fails\n", "cannot fail"),
    ],
)
def test_a_malformed_scenario_is_refused(text: str, message: str) -> None:
    with pytest.raises(ScenarioError, match=message):
        scripts.parse("example", text)


def test_the_file_name_is_the_scenario_name() -> None:
    with pytest.raises(ScenarioError, match="identifier"):
        scripts.parse("not-a-name", "# oracle: results t\n")


def test_scripts_are_selected_by_name_or_by_the_start_of_names() -> None:
    names = [s.name for s in scripts.load_all(SCENARIOS)]
    assert names == sorted(names) and len(names) >= 20
    chosen = [s.name for s in scripts.load_all(SCENARIOS, ["agg_", "table_sort"])]
    assert chosen == [n for n in names if n.startswith("agg_")] + ["table_sort"]
    assert len(chosen) >= 3
    with pytest.raises(ScenarioError, match="nosuch_"):
        scripts.load_all(SCENARIOS, ["agg_", "nosuch_"])


def test_the_digest_follows_the_content() -> None:
    assert scripts.parse("a", TEXT).digest() == scripts.parse("b", TEXT).digest()
    assert scripts.parse("a", TEXT).digest() != scripts.parse("a", TEXT + "\n").digest()


# ---------------------------------------------------------------- what a script answered


@pytest.mark.parametrize("text", [JAVA_DHERROR, OXHAVEN_DHERROR])
def test_the_exception_that_ended_the_script_is_found(text: str) -> None:
    found = scripts.parse_error(text)
    assert found.type == "deephaven.dherror.DHError"
    assert found.operation == "table update operation failed."


@pytest.mark.parametrize(
    ("text", "kind", "message"),
    [
        ("java.lang.RuntimeException: Error in Python interpreter:\n"
         "Traceback (most recent call last):\n"
         '  File "<string>", line 1, in <module>\n'
         "ZeroDivisionError: division by zero\n\n\tat org.jpy.PyLib.executeCode\n",
         "ZeroDivisionError", "division by zero"),
        ('Error in Python interpreter:\n  File "<string>", line 1\n    t = = 1\n'
         "        ^\nSyntaxError: invalid syntax\n",
         "SyntaxError", "invalid syntax"),
        ("Error in Python interpreter:\nTraceback (most recent call last):\n"
         '  File "<string>", line 4, in <module>\n'
         '  File "<string>", line 2, in helper\n'
         "    raise KeyError\nKeyError\n",
         "KeyError", ""),
        # a script that catches one exception and raises another: the last one ended it
        ("Error in Python interpreter:\nTraceback (most recent call last):\n"
         '  File "<string>", line 2, in <module>\n'
         "ZeroDivisionError: division by zero\n\n"
         "During handling of the above exception, another exception occurred:\n\n"
         "Traceback (most recent call last):\n"
         '  File "<string>", line 4, in <module>\n'
         "ValueError: no\n",
         "ValueError", "no"),
        ("Error in Python interpreter:\nTraceback (most recent call last):\n"
         '  File "<string>", line 1, in <module>\n'
         "AttributeError: 'Table' object has no attribute 'slice'. Did you mean: 'size'?\n",
         "AttributeError", "'Table' object has no attribute 'slice'. Did you mean: 'size'?"),
    ],
)
def test_other_exceptions_have_a_type_and_no_operation(text, kind, message) -> None:
    assert scripts.parse_error(text) == ScriptError(kind, message, None)


def test_text_that_names_no_exception_is_not_taken_for_one() -> None:
    assert scripts.parse_error("the session is gone").type == "(unrecognized)"


# ---------------------------------------------------------------- rule 7


@pytest.mark.parametrize(
    ("kind", "text"),
    [
        ("ModuleNotFoundError", "No module named 'deephaven.updateby'"),
        ("ImportError", "cannot import name 'agg' from 'deephaven' (unknown location)"),
        ("ImportError", "cannot import name 'datetime_col' from 'deephaven.column' (x.py)"),
        ("AttributeError", "module 'deephaven.agg' has no attribute 'pct'"),
        ("AttributeError", "'Table' object has no attribute 'slice'. Did you mean: 'size'?"),
        ("AttributeError", "type object 'SortDirection' has no attribute 'DESCENDING'"),
        ("TypeError", "Table.agg_by() got an unexpected keyword argument 'preserve_empty'"),
    ],
)
def test_a_name_the_package_lacks_is_missing(kind: str, text: str) -> None:
    assert rules.missing_name(error(kind, text))


@pytest.mark.parametrize(
    ("kind", "text"),
    [
        ("ModuleNotFoundError", "No module named 'pandas'"),
        ("ImportError", "cannot import name 'x' from 'numpy'"),
        ("AttributeError", "'NoneType' object has no attribute 'update'"),
        ("AttributeError", "'list' object has no attribute 'qst_type'"),
        ("AttributeError", "module 'math' has no attribute 'nope'"),
        ("NameError", "name 'empty_table' is not defined"),
        ("TypeError", "unsupported operand type(s) for /: 'Table' and 'int'"),
        ("deephaven.dherror.DHError", "table update operation failed. : 'Table' object has "
                                      "no attribute 'x'"),
        ("ValueError", "No module named 'deephaven.agg'"),
    ],
)
def test_other_failures_are_not_missing(kind: str, text: str) -> None:
    assert not rules.missing_name(error(kind, text))


DHERROR = error("deephaven.dherror.DHError", "table update operation failed. : x",
                "table update operation failed.")
ABSENT = error("AttributeError", "'Table' object has no attribute 'slice'")


@pytest.mark.parametrize(
    ("expected", "actual", "kind"),
    [
        (None, None, None),
        (DHERROR, DHERROR, None),
        # the cause and the rest of the text are not compared
        (DHERROR, error(DHERROR.type, "table update operation failed. : another cause",
                        DHERROR.operation), None),
        (error("ValueError", "one text"), error("ValueError", "another text"), None),
        (None, DHERROR, rules.SCRIPT_FAILURE_UNEXPECTED),
        (DHERROR, None, rules.SCRIPT_FAILURE_EXPECTED),
        (DHERROR, error("ValueError", "x"), rules.SCRIPT_ERROR_TYPE),
        (DHERROR, error(DHERROR.type, "table view operation failed. : x",
                        "table view operation failed."), rules.SCRIPT_ERROR_MESSAGE),
        (None, ABSENT, rules.MISSING),
        (DHERROR, ABSENT, rules.MISSING),
        # the reference itself fails for the absent name: that is the expected failure
        (ABSENT, ABSENT, None),
        (ABSENT, None, rules.SCRIPT_FAILURE_EXPECTED),
    ],
)
def test_script_outcomes_are_compared_by_type_and_operation(expected, actual, kind) -> None:
    found = rules.compare_script_outcome("s", 0, expected, actual)
    assert [m.kind for m in found] == ([kind] if kind else [])
    assert all(m.table == rules.SCRIPT for m in found)


def test_gate_kinds_of_a_script_carry_the_exception_type() -> None:
    def kinds(expected, actual) -> set[str]:
        (found,) = rules.compare_script_outcome("s", 0, expected, actual)
        return gate.kinds_of(found)

    assert kinds(None, DHERROR) == {"script_failure_unexpected:deephaven.dherror.DHError"}
    assert kinds(DHERROR, error("ValueError")) == {"script_error_type:ValueError"}
    assert kinds(None, ABSENT) == {"missing"}
    assert kinds(DHERROR, None) == {"script_failure_expected"}


# ---------------------------------------------------------------- comparing two runs


def table(*values: int) -> pa.Table:
    return pa.table({"X": pa.array(values, pa.int32())})


def run_of(outcomes: list, tables: dict[tuple[int, str], pa.Table | Failure]) -> scripts.Run:
    results = ResultSet("example")
    for (step, name), data in tables.items():
        results.put(step, name, data)
    return scripts.Run(outcomes, results)


def reference() -> scripts.Run:
    return run_of([None, DHERROR, None], {
        (0, "t"): table(1), (0, "u"): Failure("NOT_FOUND"),
        (1, "t"): table(1), (1, "u"): Failure("NOT_FOUND"),
        (2, "t"): table(1, 2), (2, "u"): table(3),
    })


def test_equal_runs_are_clean_and_a_changed_cell_is_not() -> None:
    scenario = scripts.parse("example", TEXT)
    assert scripts.compare(scenario, reference(), reference()) == []
    other = reference()
    other.tables.put(2, "t", table(1, 9))
    (found,) = scripts.compare(scenario, reference(), other)
    assert (found.kind, found.table, found.step) == (rules.VALUE, "t", 2)


def test_a_missing_name_ends_the_comparison_and_keeps_what_was_compared() -> None:
    scenario = scripts.parse("example", TEXT)
    actual = run_of([None, ABSENT, ABSENT], {
        (0, "t"): table(7), (0, "u"): Failure("NOT_FOUND"),
        (1, "t"): table(7), (1, "u"): Failure("NOT_FOUND"),
        (2, "t"): table(7), (2, "u"): Failure("NOT_FOUND"),
    })
    found = scripts.compare(scenario, reference(), actual)
    assert [(m.kind, m.table, m.step) for m in found] == [
        (rules.MISSING, rules.SCRIPT, 1), (rules.VALUE, "t", 0)]


def test_a_script_that_fails_by_mistake_is_not_recorded() -> None:
    scenario = scripts.parse("example", TEXT)
    assert scripts.undeclared(scenario, reference()) == []
    failed = run_of([DHERROR, DHERROR, None], {})
    assert "step 0 failed" in scripts.undeclared(scenario, failed)[0]
    passed = run_of([None, None, None], {})
    assert "did not fail" in scripts.undeclared(scenario, passed)[0]


def test_a_recording_round_trips_and_refuses_changed_content(tmp_path: Path) -> None:
    scenario = scripts.parse("example", TEXT)
    scripts.save(reference(), tmp_path, scenario, "42.9")
    loaded = scripts.load(tmp_path, scenario)
    assert loaded.outcomes == [None, DHERROR, None]
    assert scripts.compare(scenario, reference(), loaded) == []
    with pytest.raises(res.RecordingError, match="different scenario content"):
        scripts.load(tmp_path, scripts.parse("example", TEXT + "t = t.head(1)\n"))
    (tmp_path / scripts.OUTCOMES).unlink()
    with pytest.raises(res.RecordingError, match="cannot read"):
        scripts.load(tmp_path, scenario)


# ---------------------------------------------------------------- the checked-in files


@pytest.mark.parametrize("scenario", scripts.load_all(SCENARIOS), ids=lambda s: s.name)
def test_every_script_scenario_is_recorded_as_declared(scenario) -> None:
    recorded = scripts.load(CORPUS / scenario.name, scenario)
    assert scripts.undeclared(scenario, recorded) == []
    assert scripts.compare(scenario, recorded, recorded, exact=True) == []
    steps = len(scenario.sections)
    assert set(recorded.tables.keys()) == {
        (step, name) for step in range(steps)
        for name in [*(r.table for r in scenario.results), *(v.name for v in scenario.views)]}
    for step, name in recorded.tables.keys():
        data = recorded.tables.get(step, name)
        if isinstance(data, Failure):
            continue
        # a recording that the rules cannot compare could never be clean
        assert all(rules.comparison_class(f.type) for f in data.schema), (name, data.schema)
        assert scenario.computed.get(name, frozenset()) <= set(data.column_names)


def test_no_recording_is_left_without_its_scenario() -> None:
    names = {s.name for s in scripts.load_all(SCENARIOS)}
    assert {p.name for p in CORPUS.iterdir() if p.is_dir()} == names


def test_the_baseline_lists_every_script_scenario() -> None:
    baseline = bl.load(ORACLE / "replay-baseline.json")
    names = {s.name for s in scripts.load_all(SCENARIOS)}
    assert set(baseline[gate.REPLAY_SCRIPTS]) == names


def test_selecting_no_script_is_an_error(capsys) -> None:
    assert main(["replay-scripts", "--url", "dh://127.0.0.1:1", "--scripts", "nosuch_"]) == 2
    assert "nosuch_" in capsys.readouterr().err
