"""The generator of the third parity pass (`harness/fuzz_third.py`): it is deterministic,
what it builds is valid, it reaches every form it claims, it keeps the restrictions it
states, and it is reached only by its own variants. Needs no server."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

from harness import fuzz, fuzz_third
from harness import scenario as sc

ORACLE = Path(__file__).resolve().parent.parent
SEEDS = range(150)
GENERATED = {
    family: [fuzz.generate(seed, ops, "third") for seed in SEEDS]
    for family, ops in fuzz.THIRD_FAMILIES.items()
}
OPERATIONS = [(doc, predicted, op) for runs in GENERATED.values() for doc, predicted in runs
              for op in doc["operations"]]
LIST = "list:"
COMPARING = ("cum_min", "cum_max", "rolling_min", "rolling_max")
WORD = re.compile(r"[A-Za-z_]\w*")


def _text(doc: dict) -> str:
    return json.dumps(doc, sort_keys=True)


def _live(doc: dict) -> set[str]:
    """The refreshing tables of a scenario: the ticking inputs and what is made of them."""
    live = {table["name"] for table in doc["inputs"] if "ticking" in table}
    for op in doc["operations"]:
        if any(name in live for name in sc.references(op)) and op["op"] not in (
                "snapshot", "meta_table"):
            live.add(op["name"])
    return live


def _source(pair: str) -> str:
    return pair.split("=")[-1].strip()


def _lists(types: dict[str, str], text: str) -> list[str]:
    """The element types of the vector columns that `text` names."""
    return [types[word][len(LIST):] for word in WORD.findall(text)
            if types.get(word, "").startswith(LIST)]


def _filters(op: dict) -> list[str]:
    specs = op.get("ops", []) + op.get("aggs", [])
    return [text for spec in specs for text in spec.get("args", {}).get("filters", [])]


# ---------------------------------------------------------------- the other generators


def test_the_third_generator_is_reached_only_by_its_variants():
    assert "third" in fuzz.VARIANTS
    assert not set(fuzz.THIRD_FAMILIES) & (
        set(fuzz.FAMILIES) | set(fuzz.SECOND_FAMILIES) | set(fuzz.TIME_FAMILIES))
    for ops in ({"aj"}, {"aj", "raj", "where"}, {"update_by", "group_by"}):
        assert fuzz.default_variants(ops) in ("recorded", "all")
        assert fuzz.generate(3, frozenset(ops), "third")[0] != fuzz.generate(
            3, frozenset(ops), "all")[0]
    assert set().union(*fuzz.THIRD_FAMILIES.values()) <= fuzz.OPERATIONS


# ---------------------------------------------------------------- determinism


def test_generation_is_deterministic_per_seed():
    for family, ops in fuzz.THIRD_FAMILIES.items():
        for seed in (0, 7, 149):
            again, predicted = fuzz.generate(seed, ops, "third")
            assert _text(again) == _text(GENERATED[family][seed][0]), (family, seed)
            assert predicted == GENERATED[family][seed][1]
        texts = {_text(doc) for doc, _ in GENERATED[family]}
        assert len(texts) > 140, f"{family}: seeds mostly produce the same scenario"


def test_generation_is_the_same_in_another_process():
    code = ("import json; from harness import fuzz; "
            "print(json.dumps([fuzz.generate(s, fuzz.THIRD_FAMILIES['all3'], 'third')[0] "
            "for s in range(12)], sort_keys=True))")
    expected = json.dumps([GENERATED["all3"][seed][0] for seed in range(12)], sort_keys=True)
    for hash_seed in ("1", "2"):
        out = subprocess.run(
            [sys.executable, "-c", code], cwd=ORACLE, check=True, capture_output=True,
            text=True, env=dict(os.environ, PYTHONHASHSEED=hash_seed),
        ).stdout.strip()
        assert out == expected


@pytest.mark.parametrize("family, digest", [
    ("asof_chains", "5b92d8fa3d07643e"),
    ("vectors", "3bbf94d59b1435ad"),
    ("windows", "f0385c7187d59dc4"),
    ("all3", "d9d3b7551a1f223a"),
])
def test_a_seed_of_the_third_generator_is_the_scenario_it_was(family, digest):
    """Digests over the scenarios of seeds 0 to 99 as the generator was when the runs of
    `tasks/12.1-fuzzer-coverage.md` were made: a seed named there must keep naming its
    scenario."""
    digest_now = hashlib.sha256()
    for seed in range(100):
        digest_now.update(json.dumps(fuzz.generate(seed, fuzz.THIRD_FAMILIES[family], "third"),
                                     sort_keys=True).encode())
    assert digest_now.hexdigest()[:16] == digest


# ---------------------------------------------------------------- validity and coverage


def test_generated_scenarios_are_valid_and_stay_within_the_requested_operations():
    for family, runs in GENERATED.items():
        for seed, (doc, predicted) in enumerate(runs):
            scenario = sc.parse(doc)  # the format's own validator
            assert scenario.results, (family, seed)
            # `results` compares a table with a column of arrays of arrays through an
            # `ungroup`, also in a family that does not draw that operation.
            assert {op["op"] for op in doc["operations"]} <= (
                fuzz.THIRD_FAMILIES[family] | {"ungroup"})
            assert set(predicted) >= {op["name"] for op in doc["operations"]}
            for name, columns in predicted.items():
                names = [column for column, _ in columns]
                assert len(set(names)) == len(names), (family, seed, name)
            # No result is a table the comparison rules cannot read.
            for result in doc["results"]:
                assert not any(kind.startswith(LIST + LIST)
                               for _, kind in predicted[result["table"]]), (family, seed)


def test_every_form_is_drawn():
    seen: Counter = Counter()
    for doc, predicted, op in OPERATIONS:
        live = _live(doc)
        by_name = {o["name"]: o for o in doc["operations"]}
        kind = op["op"]
        types = dict(predicted[op.get("source") or op.get("left") or op["name"]])
        if kind in ("aj", "raj"):
            left = by_name.get(op["left"], {}).get("op") in ("aj", "raj")
            right = by_name.get(op["right"], {}).get("op") in ("aj", "raj")
            seen[f"{kind} of an as-of result on the left"] += left
            seen[f"{kind} of an as-of result on the right"] += right
            seen["as-of results on both sides"] += left and right
            if left or right:
                seen["refreshing chain"] += op["name"] in live
                seen["static chain"] += op["name"] not in live
                seen["chain with exact match columns"] += len(op["on"]) > 1
                seen["chain without joins"] += "joins" not in op
                stamp = WORD.findall(op["on"][-1])
                seen["chain on the carried stamp"] += stamp[0] == stamp[-1] and left
            depth, name = 0, op["name"]
            while by_name.get(name, {}).get("op") in ("aj", "raj"):
                depth, name = depth + 1, by_name[name]["left"]
            seen["chain of three"] += depth >= 3
        elif kind == "update_by":
            seen["bucket by a vector"] += any(types[c].startswith(LIST) for c in op["by"])
            for operator in op["ops"]:
                name = operator["op"]
                if name.startswith(COMPARING) and any(
                        types[_source(pair)].startswith(LIST) for pair in operator["columns"]):
                    seen[f"{name.split('_')[0]} {name.split('_')[1]} of a vector"] += 1
                    seen[f"vector minimum or maximum, {'refreshing' if op['name'] in live else 'static'}"] += 1
                    seen["rolling over time of a vector"] += name.endswith("_time")
                if "count_where" in name and any(
                        _lists(types, text) for text in operator["args"]["filters"]):
                    seen[f"{name} of a vector"] += 1
                if name.startswith("rolling_formula") and "?" in operator["args"]["formula"]:
                    formula = operator["args"]["formula"]
                    token = operator["args"]["formula_param"] is not None
                    seen["condition with a token" if token else "condition over columns"] += 1
                    seen[f"condition {name.rsplit('_', 1)[1]}"] += 1
                    seen["condition subVector"] += "subVector" in formula
                    seen["condition read by position"] += bool(re.search(r"\)\[\d\]", formula))
                    seen["condition function"] += bool(re.match(
                        r"(?:\w+ = )?(?:first|last|min|max|sum)\(", formula))
                    seen["condition refreshing"] += op["name"] in live
        elif kind == "agg_by":
            seen["group by a vector"] += any(types[c].startswith(LIST) for c in op["by"])
            for agg in op["aggs"]:
                if agg["agg"] == "distinct" and any(
                        types[_source(pair)].startswith(LIST) for pair in agg["columns"]):
                    seen["distinct of a vector"] += 1
                if agg["agg"] == "count_where" and any(
                        _lists(types, text) for text in agg["args"]["filters"]):
                    seen["agg count_where of a vector"] += 1
                    seen["agg count_where of a vector, refreshing"] += op["name"] in live
        elif kind == "select_distinct":
            seen["select_distinct of a vector"] += any(
                types[c].startswith(LIST) for c in op["columns"])
            seen["select_distinct of a vector and another column"] += len(op["columns"]) > 1 and any(
                types[c].startswith(LIST) for c in op["columns"])
    expected = [
            "aj of an as-of result on the left", "raj of an as-of result on the left",
            "aj of an as-of result on the right", "raj of an as-of result on the right",
            "as-of results on both sides", "refreshing chain", "static chain",
            "chain with exact match columns", "chain without joins",
            "chain on the carried stamp", "chain of three",
            "bucket by a vector", "cum min of a vector", "cum max of a vector",
            "rolling min of a vector", "rolling max of a vector",
            "rolling over time of a vector", "vector minimum or maximum, refreshing",
            "vector minimum or maximum, static", "cum_count_where of a vector",
            "rolling_count_where_tick of a vector", "rolling_count_where_time of a vector",
            "condition with a token", "condition over columns", "condition tick",
            "condition time", "condition subVector", "condition read by position",
            "condition function", "condition refreshing", "group by a vector",
            "distinct of a vector", "agg count_where of a vector",
            "agg count_where of a vector, refreshing", "select_distinct of a vector",
            "select_distinct of a vector and another column"]
    assert not {name: seen[name] for name in expected if seen[name] <= 5}, seen


# ---------------------------------------------------------------- restrictions


def _made_by(doc: dict, predicted: dict, table: str, column: str) -> str | None:
    """The `update_by` operator that made `column` of `table`, followed by name through
    the tables it was passed on by."""
    by_name = {o["name"]: o for o in doc["operations"]}
    while table in by_name:
        op = by_name[table]
        if op["op"] == "update_by":
            for operator in op["ops"]:
                if column in [pair.split("=")[0].strip() for pair in operator["columns"]]:
                    return operator["op"]
        table = op.get("source") or op.get("left")
        if table is None or column not in dict(predicted[table]):
            return None
    return None


def test_what_an_owner_item_or_a_finding_restricts_is_not_drawn():
    """`fuzz_third.RESTRICTIONS`, one check per entry."""
    seen: Counter = Counter()
    floats = ("float32", "float64")
    for doc, predicted, op in OPERATIONS:
        live = _live(doc)
        inputs = {table["name"] for table in doc["inputs"]}
        kind = op["op"]
        where = (doc["name"], op)
        types = dict(predicted[op.get("source") or op.get("left") or op["name"]])
        keys = op.get("by", []) if kind in ("agg_by", "update_by") else (
            op["columns"] if kind == "select_distinct" else [])
        for column in keys:
            if types[column].startswith(LIST):
                seen["V3"] += 1
                assert types[column][len(LIST):] not in floats, where
                seen["V5"] += 1
                assert op["source"] not in live, where
        if kind == "update_by":
            for operator in op["ops"]:
                name = operator["op"]
                if name.startswith(COMPARING):
                    for element in (types[_source(pair)] for pair in operator["columns"]):
                        if not element.startswith(LIST):
                            continue
                        seen["V1"] += 1
                        assert element[len(LIST):] not in ("bool", "timestamp"), where
                        seen["V2"] += name.startswith("rolling")
                        assert not (name.startswith("rolling")
                                    and element[len(LIST):] in floats), where
                if name.startswith("rolling_formula"):
                    formula = operator["args"]["formula"]
                    read = [_source(pair) for pair in operator["columns"]] + WORD.findall(formula)
                    if "?" in formula and any(types.get(c) == "string" for c in read):
                        seen["V7"] += 1
                        assert "subVector" not in formula, where
                        seen["V9"] += 1
                        assert not re.search(r"\)\[", formula), where
                        # `len` of a window of strings fails the table when a row is read.
                        assert "len(" not in formula or op["source"] not in live, where
        if kind == "agg_by":
            for agg in op["aggs"]:
                if agg["agg"] != "distinct":
                    continue
                for element in (types[_source(pair)] for pair in agg["columns"]):
                    if element.startswith(LIST):
                        seen["V4"] += 1
                        assert element[len(LIST):] not in floats + ("bool", "timestamp"), where
                        assert op["source"] not in live, where
        for text in _filters(op) if kind in ("agg_by", "update_by") else []:
            for column in re.findall(r"([A-Za-z_]\w*)\[", text):
                seen["V8"] += types.get(column, "").startswith(LIST)
                assert types.get(column) != LIST + "bool", where
        if kind == "ungroup":
            taken = op.get("columns") or [c for c, t in types.items() if t.startswith(LIST)]
            for column in taken:
                seen["V6"] += 1
                made = _made_by(doc, predicted, op["source"], column) or ""
                assert not (made.startswith(COMPARING) and types[column] == LIST + "string"), where
        if kind in ("aj", "raj") and len(op["on"]) > 1 and op["right"] in live and (
                op["left"] not in live):
            seen["C1"] += kind == "raj"
            seen["aj of the shape of C1"] += kind == "aj" and op["right"] not in inputs
            assert kind == "aj" or op["right"] in inputs, where
    assert not {name: seen[name] for name in fuzz_third.RESTRICTIONS if seen[name] <= 5}, seen
    # What C1 keeps from raj is drawn for aj (PARITY.md, A13).
    assert seen["aj of the shape of C1"] > 5, seen


def test_every_restriction_is_written_up():
    text = (ORACLE.parent / "tasks" / "12.1-fuzzer-coverage.md").read_text()
    for name, reason in fuzz_third.RESTRICTIONS.items():
        assert re.search(rf"^### {name}\. ", text, re.MULTILINE), name
        finding = re.search(r"\((F\d+):", reason)
        if finding:
            assert re.search(rf"^#### {finding.group(1)}\. ", text, re.MULTILINE), name
        else:
            assert "owner item" in reason, name
