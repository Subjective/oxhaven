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
    ("asof_chains", "5ddf633816b5306b"),
    ("vectors", "ae5f2f227553da7d"),
    ("windows", "bb1d2442ca9d0836"),
    ("all3", "14d1cedfa23e8bb0"),
])
def test_a_seed_of_the_third_generator_is_the_scenario_it_was(family, digest):
    """Digests over the scenarios of seeds 0 to 99 as the generator was when the runs of
    task 14.2 in `tasks/12.1-fuzzer-coverage.md` were made: a seed named for those runs
    must keep naming its scenario. (The seeds named for 12.1's own runs name other
    scenarios since 14.2 changed the generator; the F files keep those scenarios.)"""
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
            # PARITY.md A13 for raj: the shape of its chain case (once restriction C1).
            seen[f"{kind} of the shape of the chains of A13"] += (
                len(op["on"]) > 1 and op["left"] not in live and op["right"] in live
                and op["right"] in by_name)
        elif kind == "update_by":
            seen["bucket by a vector"] += any(types[c].startswith(LIST) for c in op["by"])
            seen["bucket by a vector, refreshing"] += op["name"] in live and any(
                types[c].startswith(LIST) for c in op["by"])
            seen["bucket by a vector of floats"] += any(
                types[c][len(LIST):] in ("float32", "float64") for c in op["by"])
            for operator in op["ops"]:
                name = operator["op"]
                if name.startswith(COMPARING) and any(
                        types[_source(pair)].startswith(LIST) for pair in operator["columns"]):
                    seen[f"{name.split('_')[0]} {name.split('_')[1]} of a vector"] += 1
                    seen[f"vector minimum or maximum, {'refreshing' if op['name'] in live else 'static'}"] += 1
                    seen["rolling over time of a vector"] += name.endswith("_time")
                    seen["rolling minimum or maximum of vectors of floats"] += name.startswith(
                        "rolling") and any(types[_source(pair)][len(LIST):] in (
                            "float32", "float64") for pair in operator["columns"])
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
                    # PARITY.md A23: an element of a condition between windows of strings.
                    seen["element of a condition between windows of strings"] += bool(
                        re.search(r"\)\[\d\]", formula)) and any(
                        types.get(c) == "string" for c in [_source(p) for p in operator[
                            "columns"]] + WORD.findall(formula))
        elif kind == "agg_by":
            seen["group by a vector"] += any(types[c].startswith(LIST) for c in op["by"])
            seen["group by a vector, refreshing"] += op["name"] in live and any(
                types[c].startswith(LIST) for c in op["by"])
            for agg in op["aggs"]:
                if agg["agg"] == "distinct" and any(
                        types[_source(pair)].startswith(LIST) for pair in agg["columns"]):
                    seen["distinct of a vector"] += 1
                    seen["distinct of a vector, refreshing"] += op["name"] in live
                    seen["distinct of a vector of floats"] += any(
                        types[_source(pair)][len(LIST):] in ("float32", "float64")
                        for pair in agg["columns"])
                if agg["agg"] == "count_where" and any(
                        _lists(types, text) for text in agg["args"]["filters"]):
                    seen["agg count_where of a vector"] += 1
                    seen["agg count_where of a vector, refreshing"] += op["name"] in live
        elif kind == "select_distinct":
            seen["select_distinct of a vector"] += any(
                types[c].startswith(LIST) for c in op["columns"])
            seen["select_distinct of a vector and another column"] += len(op["columns"]) > 1 and any(
                types[c].startswith(LIST) for c in op["columns"])
        elif kind == "ungroup" and op["name"] != op["source"] + "v":
            # PARITY.md A23 and A24: what holds vectors, taken apart.
            source = by_name.get(op["source"], {})
            seen["ungroup of an aggregation"] += source.get("op") == "agg_by"
            seen["ungroup with null fill of an aggregation by a vector"] += (
                source.get("op") == "agg_by" and op.get("null_fill", True) and any(
                    dict(predicted[source["source"]])[c].startswith(LIST) for c in source["by"]))
            seen["ungroup of a minimum or maximum of a vector"] += source.get("op") == "update_by" and any(
                o["op"].startswith(COMPARING) for o in source["ops"])
    # A22: a table read from a minimum or maximum of vectors of a refreshing table.
    for doc, predicted in [run for runs in GENERATED.values() for run in runs]:
        live = _live(doc)
        extremes = {op["name"] for op in doc["operations"] if op["op"] == "update_by"
                    and op["name"] in live and any(
                        o["op"].startswith(COMPARING) and dict(predicted[op["source"]])[
                            _source(pair)].startswith(LIST)
                        for o in op["ops"] for pair in o["columns"])}
        seen["read on after a minimum or maximum of vectors, refreshing"] += sum(
            bool(extremes.intersection(sc.references(op))) for op in doc["operations"])
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
            "select_distinct of a vector and another column",
            "raj of the shape of the chains of A13", "aj of the shape of the chains of A13",
            "bucket by a vector, refreshing", "bucket by a vector of floats",
            "rolling minimum or maximum of vectors of floats",
            "element of a condition between windows of strings",
            "group by a vector, refreshing", "distinct of a vector, refreshing",
            "distinct of a vector of floats", "ungroup of an aggregation",
            "ungroup with null fill of an aggregation by a vector",
            "ungroup of a minimum or maximum of a vector",
            "read on after a minimum or maximum of vectors, refreshing"]
    assert not {name: seen[name] for name in expected if seen[name] <= 5}, seen


# ---------------------------------------------------------------- restrictions


def _marks(doc: dict, predicted: dict) -> tuple[dict, dict]:
    """Per table, the vector columns made by `rolling_group` or by a minimum or maximum of
    one (V14), and the columns PARITY.md A23 types differently (V15), followed by name
    through the tables that pass them on."""
    windowed: dict[str, set[str]] = {}
    extremes: dict[str, set[str]] = {}
    objects: dict[str, set[str]] = {}
    for op in doc["operations"]:
        name, kind = op["name"], op["op"]
        types = dict(predicted[name])
        first = op.get("source") or op.get("left")
        before = dict(predicted[first]) if first else {}

        def kept(marks: dict) -> set[str]:
            out = {c for c in marks.get(first, ()) if types.get(c) == before.get(c)}
            if "right" in op:
                right = dict(predicted[op["right"]])
                carried = marks.get(op["right"], set())
                for pair in op.get("joins", []):
                    made, column = (part.strip() for part in pair.split("="))
                    if column in carried:
                        out.add(made)
                if "joins" not in op:
                    out |= {c for c in carried if types.get(c) == right.get(c)}
            return out

        win, ext, obj = kept(windowed), kept(extremes), kept(objects)
        if kind == "update_by":
            for operator in op["ops"]:
                outs = [(pair.split("=")[0].strip(), _source(pair)) for pair in operator["columns"]]
                if operator["op"].startswith("rolling_group"):
                    win |= {out for out, _ in outs}
                elif operator["op"].startswith(COMPARING):
                    win |= {out for out, made in outs if made in windowed.get(first, ())}
                    ext |= {out for out, _ in outs if types[out] == LIST + "string"}
                elif operator["op"].startswith("rolling_formula") and re.search(
                        r"\?.*:.*\)\s*\[", operator["args"]["formula"]):
                    obj |= {out for out, _ in outs if types[out] == "string"}
        if kind == "ungroup":
            taken = op.get("columns") or [c for c, t in before.items() if t.startswith(LIST)]
            obj |= {c for c in taken if c in extremes.get(first, ())}
        windowed[name], extremes[name], objects[name] = win, ext, obj
    return windowed, objects


def test_what_an_owner_item_or_a_finding_restricts_is_not_drawn():
    """`fuzz_third.RESTRICTIONS`, one check per entry."""
    seen: Counter = Counter()
    floats = ("float32", "float64")
    marked = {(family, seed): _marks(doc, predicted)
              for family, runs in GENERATED.items() for seed, (doc, predicted) in enumerate(runs)}
    for family, runs in GENERATED.items():
        for seed, (doc, predicted) in enumerate(runs):
            windowed, objects = marked[(family, seed)]
            live = _live(doc)
            for op in doc["operations"]:
                kind = op["op"]
                where = (family, doc["name"], op)
                first = op.get("source") or op.get("left")
                types = dict(predicted[first or op["name"]])
                keys = op.get("by", []) if kind in ("agg_by", "update_by") else (
                    op["columns"] if kind == "select_distinct" else [])
                for column in keys:
                    if not types[column].startswith(LIST):
                        continue
                    element = types[column][len(LIST):]
                    if kind != "update_by":
                        seen["V3"] += 1
                        assert element not in floats, where
                        if first in live:
                            seen["V13"] += 1
                            assert element not in ("bool", "timestamp"), where
                    if first in live:
                        seen["V14"] += 1
                        assert column not in windowed.get(first, ()), where
                if kind == "update_by":
                    for operator in op["ops"]:
                        name = operator["op"]
                        if name.startswith(COMPARING):
                            for element in (types[_source(pair)] for pair in operator["columns"]):
                                if element.startswith(LIST):
                                    seen["V1"] += 1
                                    assert element[len(LIST):] not in ("bool", "timestamp"), where
                        if name.startswith("rolling_formula"):
                            formula = operator["args"]["formula"]
                            read = [_source(pair) for pair in operator["columns"]] + WORD.findall(
                                formula)
                            if "?" in formula and any(types.get(c) == "string" for c in read):
                                seen["V7"] += 1
                                assert "subVector" not in formula, where
                                # `len` of a window of strings fails the table when a row is
                                # read.
                                assert "len(" not in formula or first not in live, where
                if kind == "agg_by" and first in live and any(
                        types[c].startswith(LIST) for c in op["by"]):
                    seen["V16"] += 1
                    assert "initial_groups" not in op and not op.get("preserve_empty"), where
                if kind == "agg_by":
                    for agg in op["aggs"]:
                        if agg["agg"] != "distinct":
                            continue
                        for column in (_source(pair) for pair in agg["columns"]):
                            if types[column].startswith(LIST):
                                seen["V1"] += 1
                                assert types[column][len(LIST):] not in ("bool", "timestamp"), where
                                if first in live:
                                    seen["V14"] += 1
                                    assert column not in windowed.get(first, ()), where
                for text in _filters(op) if kind in ("agg_by", "update_by") else []:
                    for column in re.findall(r"([A-Za-z_]\w*)\[", text):
                        seen["V8"] += types.get(column, "").startswith(LIST)
                        assert types.get(column) != LIST + "bool", where
                # V15: what the operation reads names no such column of its sources.
                read = set(WORD.findall(json.dumps({k: v for k, v in op.items()
                                                    if k not in ("name", "op")})))
                for source in sc.references(op):
                    seen["V15"] += bool(objects.get(source))
                    view = kind == "ungroup" and op["name"] == source + "v"  # made by `results`
                    assert view or not read & objects.get(source, set()), where
            # V12: the refreshing sort results and what is made from them.
            rekeyed: set[str] = set()
            for op in doc["operations"]:
                first = op.get("source") or op.get("left")
                if op["name"] in live and (op["op"] == "sort" or first in rekeyed):
                    rekeyed.add(op["name"])
                if first in rekeyed:
                    for text in op.get("filters", []) + _filters(op):
                        assert not re.search(r"\bk\b", text), (family, doc["name"], op)
            seen["V12"] += len(rekeyed)
    for seed in (724, 831):  # two seeds of all3 that drew a filter on `k` over a sort before V12
        doc, _ = fuzz.generate(seed, fuzz.THIRD_FAMILIES["all3"], "third")
        live = _live(doc)
        rekeyed = set()
        for op in doc["operations"]:
            first = op.get("source") or op.get("left")
            if op["name"] in live and (op["op"] == "sort" or first in rekeyed):
                rekeyed.add(op["name"])
            if first in rekeyed:
                for text in op.get("filters", []) + _filters(op):
                    assert not re.search(r"\bk\b", text), (seed, op)
    assert not {name: seen[name] for name in fuzz_third.RESTRICTIONS if seen[name] <= 5}, seen


def test_every_restriction_is_written_up():
    text = (ORACLE.parent / "tasks" / "12.1-fuzzer-coverage.md").read_text()
    for name, reason in fuzz_third.RESTRICTIONS.items():
        assert re.search(rf"^### {name}\. ", text, re.MULTILINE), name
        finding = re.search(r"\((F\d+):", reason)
        if finding:
            assert re.search(rf"^#### {finding.group(1)}\. ", text, re.MULTILINE), name
        else:
            assert "owner item" in reason, name
