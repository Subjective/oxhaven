"""The formula corpus: its files, the classifier and the comparison. Needs no server."""

from __future__ import annotations

import copy

import pytest

from harness import rules
from harness.formulas import cli, compare, corpus as cp, engine, grid
from harness.formulas.tags import Builtins, classify

CORPUS = cp.load()
BUILTINS = Builtins.load(cp.DEFAULT_DIR / cp.BUILTINS)
PAIRS = CORPUS.fixtures["pairs"].column_types()
BY_ID = {case.id: case for case in CORPUS.cases}


def tags(formula: str, kind: str = "update") -> tuple[str, set[str]]:
    verdict = classify(formula, kind, PAIRS, BUILTINS)
    return verdict.subset, set(verdict.tags)


# ---------------------------------------------------------------- the files


def test_corpus_loads_and_every_case_is_recorded():
    assert len(CORPUS.cases) >= 3000
    assert all(case.java is not None for case in CORPUS.cases)
    for case in CORPUS.cases:
        outcome = case.java
        if "error" in outcome:
            assert outcome["error"]["status"] and outcome["error"]["stage"] in (
                "formula", "setup", "snapshot")
        elif case.kind == "where":
            assert outcome.get("nondeterministic") or outcome["rows"] == sorted(outcome["rows"])
        else:
            assert outcome["arrow"] and "type" in outcome


def test_result_lengths_match_the_fixture():
    for case in CORPUS.cases:
        if case.setup or "error" in case.java or case.kind != "update":
            continue
        values = case.java.get("values")
        if values is not None:
            assert len(values) == CORPUS.fixtures[case.fixture].size, case.id


def test_stored_tags_are_what_the_classifier_says_for_the_grid():
    for case in CORPUS.cases:
        if not case.group.startswith("grid-"):
            continue
        verdict = classify(case.formula, case.kind, CORPUS.fixtures[case.fixture].column_types(),
                           BUILTINS)
        assert (verdict.subset, verdict.tags) == (case.subset, case.tags), case.id


def test_grid_covers_every_operator_and_ordered_type_pair():
    for name, _op in grid.BINARY:
        for a in grid.ALL:
            for b in grid.ALL:
                assert f"grid-binary/{name}.{a}.{b}" in BY_ID


def test_fixture_round_trip_and_position_column(tmp_path):
    cp.save_fixtures(tmp_path, CORPUS.fixtures)
    cp.save_cases(tmp_path, CORPUS.cases)
    (tmp_path / cp.BUILTINS).write_text((cp.DEFAULT_DIR / cp.BUILTINS).read_text())
    again = cp.load(tmp_path)
    assert again.fixtures == CORPUS.fixtures
    assert [c.document() for c in again.cases] == [c.document() for c in CORPUS.cases]
    table = CORPUS.fixtures["pairs"].arrow()
    assert table.num_rows == 81
    assert table.column(cp.POSITION).to_pylist() == list(range(81))
    assert str(table.schema.field("C1").type) == "uint16"


def test_bad_case_is_rejected():
    good = BY_ID["grid-binary/plus.int.int"].document()
    cp.parse_case("x", good)
    for key, value in (("kind", "join"), ("subset", "maybe"), ("id", "no slash")):
        with pytest.raises(cp.CorpusError):
            cp.parse_case("x", {**good, key: value})
    with pytest.raises(cp.CorpusError):
        cp.parse_case("x", {**good, "extra": 1})


# ---------------------------------------------------------------- the classifier


def test_minimal_tag():
    assert tags("I1 + D2 * 2")[1] >= {"minimal", "arith", "type:int", "type:double"}
    assert "minimal" in tags("!isNull(I1) && I1 > 0 || T1 == \"a\"")[1]
    for formula in ("I1 > 0 ? 1 : 2", "(int)D1", "abs(I1)", "I1 == null", "NULL_INT",
                    "I1_[i - 1]", "I1 & I2", "i"):
        assert "minimal" not in tags(formula)[1], formula


def test_accepted_and_outside():
    assert tags("I1 > 0 ? (long)I1 : NULL_LONG") == (
        "accepted", {"compare", "ternary", "cast", "constant", "null", "type:int"})
    assert tags("(String)T1")[0] == "accepted"
    for formula, reason in (
        ("T1.length()", "method-call"), ("new String(\"a\")", "new"),
        ("Math.abs(I1)", "class-ref"), ("Integer.MAX_VALUE", "field-access"),
        ("(Object)I1", "cast-object"), ("I1 instanceof Object", "instanceof"),
        ("myFunction(I1)", "unknown-function"), ("someVariable + 1", "unknown-variable"),
        ("java.lang.Math.abs(I1)", "class-ref"),
    ):
        subset, found = tags(formula)
        assert subset == "outside" and reason in found, formula
    assert "method:length" in tags("T1.length()")[1]


def test_parenthesised_column_is_not_a_cast():
    assert "cast" not in tags("(I1) + I2")[1]
    assert "cast" in tags("(int) -D1")[1]


def test_where_tags():
    assert "filter:match" in tags("I1 == 1", "where")[1]
    assert "filter:range" in tags("D1 <= -2.5", "where")[1]
    assert "filter:condition" in tags("I1 == I2", "where")[1]
    assert tags("T1 icase not in \"a\", \"b\"", "where") == (
        "accepted", {"in", "filter:match", "string", "type:String"})
    assert "function:random" in tags("random()")[1] and "nondeterministic" in tags("random()")[1]


# ---------------------------------------------------------------- the comparison


def test_identical_outcomes_compare_clean():
    for case in CORPUS.cases:
        found, _partial = compare.compare(case, case.java, copy.deepcopy(case.java))
        assert found == [], case.id


def test_value_difference_is_reported():
    case = BY_ID["grid-binary/plus.int.int"]
    changed = copy.deepcopy(case.java)
    changed["values"][10] = 3
    found, _ = compare.compare(case, case.java, changed)
    assert [m.kind for m in found] == [rules.VALUE] and found[0].row == 10
    changed["values"][10] = None  # a value that should have been the null sentinel result
    assert compare.compare(case, case.java, changed)[0][0].kind == rules.VALUE


def test_signed_zero_and_nan():
    case = BY_ID["grid-binary/divide.int.int"]
    row = case.java["values"].index(-0.0)
    changed = copy.deepcopy(case.java)
    changed["values"][row] = 0.0
    assert [m.kind for m in compare.compare(case, case.java, changed)[0]] == [rules.VALUE]
    nan = case.java["values"].index("NaN")
    changed = copy.deepcopy(case.java)
    changed["values"][nan] = None
    assert [m.kind for m in compare.compare(case, case.java, changed)[0]] == [rules.VALUE]


def test_type_difference_is_reported_and_stops_there():
    case = BY_ID["grid-binary/plus.byte.byte"]
    changed = dict(case.java, type="byte", arrow="int8")
    kinds = {m.kind for m in compare.compare(case, case.java, changed)[0]}
    assert kinds == {rules.COLUMN_TYPE, rules.COLUMN_METADATA}
    # The engine type alone: a char column and a short column are both 16 bits wide.
    changed = dict(case.java, type="long")
    assert [m.kind for m in compare.compare(case, case.java, changed)[0]] == [
        rules.COLUMN_METADATA]


def test_char_values_are_compared():
    case = BY_ID["grid-cast/char.from-int"]
    assert case.java["arrow"] == "uint16"
    changed = copy.deepcopy(case.java)
    changed["values"][-1] = 65535  # Java gives null: the cast lands on the char sentinel
    assert case.java["values"][-1] is None
    assert [m.kind for m in compare.compare(case, case.java, changed)[0]] == [rules.VALUE]


def test_failures():
    failing = BY_ID["grid-binary/remainder.int.int"]
    assert failing.java["error"]["status"] == "INVALID_ARGUMENT"
    passing = BY_ID["grid-binary/plus.int.int"]
    assert [m.kind for m in compare.compare(failing, failing.java, passing.java)[0]] == [
        rules.FAILURE_EXPECTED]
    assert [m.kind for m in compare.compare(passing, passing.java, failing.java)[0]] == [
        rules.FAILURE_UNEXPECTED]
    other = {"error": {"status": "INTERNAL", "stage": "formula"}}
    assert [m.kind for m in compare.compare(failing, failing.java, other)[0]] == [
        rules.FAILURE_CODE]
    # The message is not compared.
    same = {"error": {"status": "INVALID_ARGUMENT", "stage": "formula"}}
    assert compare.compare(failing, failing.java, same)[0] == []


def test_where_rows():
    case = BY_ID["grid-where/literal.lt.double.01"]
    changed = {"rows": [r for r in case.java["rows"] if r >= 9]}  # drop the null rows
    assert changed != case.java
    assert compare.compare(case, case.java, changed)[0] != []


def test_tolerance_only_for_inexact_functions():
    exact = BY_ID["grid-binary/divide.int.int"]
    row = next(n for n, v in enumerate(exact.java["values"]) if v == 0.5)
    changed = copy.deepcopy(exact.java)
    changed["values"][row] = 0.5000000000000001
    assert compare.compare(exact, exact.java, changed)[0] != []
    inexact = BY_ID["grid-function/exp.double"]
    row = next(n for n, v in enumerate(inexact.java["values"])
               if isinstance(v, float) and v > 1)
    changed = copy.deepcopy(inexact.java)
    changed["values"][row] *= 1 + 1e-12
    assert compare.compare(inexact, inexact.java, changed)[0] == []
    changed["values"][row] *= 1 + 1e-6
    assert compare.compare(inexact, inexact.java, changed)[0] != []


def test_check_without_values_ignores_values_but_not_types():
    case = BY_ID["grid-binary/plus.int.int"]
    changed = copy.deepcopy(case.java)
    changed["values"][10] = 3
    assert compare.compare(case, case.java, changed, values=False)[0] == []
    assert compare.compare(case, case.java, dict(changed, type="long"), values=False)[0] != []


def test_unknown_result_type_is_never_passed():
    case = next(c for c in CORPUS.cases
                if "error" not in c.java and c.kind == "update"
                and c.java["arrow"].startswith("list") and c.java.get("values"))
    found, partial = compare.compare(case, case.java, copy.deepcopy(case.java))
    assert found == [] and partial


# ---------------------------------------------------------------- checks and the Java log


def test_checks_partition_the_accepted_plain_cases():
    members = {name: {c.id for c in CORPUS.cases if member(c)}
               for name, (member, _values, _text) in cli.CHECKS.items()}
    assert members["values"] == members["accept-reject"]
    assert not members["values"] & members["functions"]
    assert not members["values"] & members["arrays"]
    assert members["minimal"] <= members["values"]
    assert "grid-binary/plus.int.int" in members["minimal"]
    assert "grid-function/abs.int" in members["functions"]
    assert "grid-array/previous.int" in members["arrays"]
    covered = set().union(*(members[n] for n in ("values", "functions", "arrays", "chains",
                                                 "outside")))
    assert covered == members["all"]


def test_java_log_record_is_parsed(tmp_path):
    log = tmp_path / "java.log"
    log.write_text(
        "09:00:00.000 INFO  x | before\n"
        "09:00:00.100 ERROR i.d.s.s.SessionService    | Internal Error 'abc' a.B: Formula "
        "compilation error for: Z + 1\n"
        "\tat a.B.c(B.java:1)\n"
        "Caused by: p.Q$ParseException: \n"
        "\n"
        "Having trouble with the following expression:\n"
        "Exception type            : p.Q$ParserResolutionFailure\n"
        "Exception message         : Cannot find method plus(java.lang.Boolean, int)\n"
        "\n"
        "\t... 18 more\n"
        "09:00:00.200 ERROR i.d.s.s.SessionService    | Internal Error 'def' x.Y: outer\n"
        "Caused by: x.Z: middle\n"
        "\tat x.Z.f(Z.java:2)\n"
        "Caused by: java.lang.ArithmeticException: / by zero\n"
        "\tat x.Z.g(Z.java:3)\n"
        "09:00:00.300 INFO  x | after\n"
    )
    reader = engine.JavaLog(log)
    assert reader.lookup("abc") == (
        "a.B: Formula compilation error for: Z + 1",
        "p.Q$ParseException: ParserResolutionFailure: Cannot find method "
        "plus(java.lang.Boolean, int)",
    )
    assert reader.lookup("def") == ("x.Y: outer", "java.lang.ArithmeticException")
    assert reader.lookup("missing", wait=0.1) == (None, None)
