"""The generator of the second parity pass (`harness/fuzz_second.py`): it is deterministic,
what it builds is valid, it reaches every operation and form it claims, it keeps the
restrictions it states, and it leaves the two older generators alone. Needs no server."""

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

from harness import fuzz, fuzz_second
from harness import scenario as sc

ORACLE = Path(__file__).resolve().parent.parent
SEEDS = range(150)
GENERATED = {
    family: [fuzz.generate(seed, ops, "second") for seed in SEEDS]
    for family, ops in fuzz.SECOND_FAMILIES.items()
}
OPERATIONS = [(doc, predicted, op) for runs in GENERATED.values() for doc, predicted in runs
              for op in doc["operations"]]
FORMULA_OPERATIONS = ("view", "select", "update", "update_view", "lazy_update")
ROW_VARIABLE = re.compile(r"(?<![A-Za-z_0-9.'])(?:i|ii|k)(?![A-Za-z_0-9(])")
SHIFT = re.compile(r"[A-Za-z_]\w*_\[(?:i|ii)(?: ?[-+] ?\d+)?\]")


def _text(doc: dict) -> str:
    return json.dumps(doc, sort_keys=True)


def _digest(seeds: int, ops, variants: str) -> str:
    digest = hashlib.sha256()
    for seed in range(seeds):
        digest.update(json.dumps(fuzz.generate(seed, frozenset(ops), variants),
                                 sort_keys=True).encode())
    return digest.hexdigest()[:16]


def _live(doc: dict) -> set[str]:
    """The refreshing tables of a scenario: the ticking inputs and what is made of them."""
    live = {table["name"] for table in doc["inputs"] if "ticking" in table}
    for op in doc["operations"]:
        if any(name in live for name in sc.references(op)):
            live.add(op["name"])
    return live


def _expression(formula: str) -> str:
    return formula.split("=", 1)[1]


# ---------------------------------------------------------------- the older generators


@pytest.mark.parametrize("family, digest", [
    ("aggregations", "41b8958a79517be8"),
    ("all", "ccc3d4b4277a9391"),
    ("formulas", "d3f3ded7ce1a5000"),
    ("update_by", "2f63358840898734"),
])
def test_a_seed_of_the_wide_generator_is_the_scenario_it_was(family, digest):
    """Digests over the scenarios of seeds 0 to 99, taken from the harness as it was before
    this module existed (commit ee30914): a seed of a family in `tasks/4.17-fuzz-findings.md`
    or in the run log must keep naming the scenario that showed the difference."""
    assert _digest(100, fuzz.FAMILIES[family], "all") == digest


def test_the_second_generator_is_reached_only_by_its_variants():
    assert fuzz.SECOND_OPERATIONS.isdisjoint(fuzz.OPERATIONS)
    assert fuzz.SECOND_OPERATIONS <= set(sc.OPERATIONS)
    assert fuzz.default_variants({"where", "ring_table"}) == "second"
    assert fuzz.default_variants({"where", "raj"}) == "all"
    assert fuzz.generate(3, frozenset({"where"}), "second")[0] != fuzz.generate(
        3, frozenset({"where"}), "all")[0]
    with pytest.raises(sc.ScenarioError):
        fuzz.generate(0, frozenset({"ring_table"}), "all")
    assert set().union(*fuzz.SECOND_FAMILIES.values()) >= fuzz.SECOND_OPERATIONS
    assert not set(fuzz.SECOND_FAMILIES) & set(fuzz.FAMILIES)


# ---------------------------------------------------------------- determinism


def test_generation_is_deterministic_per_seed():
    for family, ops in fuzz.SECOND_FAMILIES.items():
        for seed in (0, 7, 149):
            again, predicted = fuzz.generate(seed, ops, "second")
            assert _text(again) == _text(GENERATED[family][seed][0]), (family, seed)
            assert predicted == GENERATED[family][seed][1]
        texts = {_text(doc) for doc, _ in GENERATED[family]}
        assert len(texts) > 140, f"{family}: seeds mostly produce the same scenario"


def test_generation_is_the_same_in_another_process():
    code = ("import json; from harness import fuzz; "
            "print(json.dumps([fuzz.generate(s, fuzz.SECOND_FAMILIES['all2'], 'second')[0] "
            "for s in range(12)], sort_keys=True))")
    expected = json.dumps([GENERATED["all2"][seed][0] for seed in range(12)], sort_keys=True)
    for hash_seed in ("1", "2"):
        out = subprocess.run(
            [sys.executable, "-c", code], cwd=ORACLE, check=True, capture_output=True,
            text=True, env=dict(os.environ, PYTHONHASHSEED=hash_seed),
        ).stdout.strip()
        assert out == expected


@pytest.mark.parametrize("family, digest", [
    ("formulas2", "5e77dee2784b36b0"),
    ("partitions", "506155a6fb7783fb"),
    ("all2", "d9643651e877f7f3"),
])
def test_a_seed_of_the_second_generator_is_the_scenario_it_was(family, digest):
    """Digests over the scenarios of seeds 0 to 99 as the generator is since task 4.20
    lifted the restrictions D4, R2 and R3: filters draw time literals in any zone, and
    zones may be offsets. The seeds named in the findings of
    `tasks/4.20-fuzz-findings-2.md` were recorded before that and mean other scenarios
    now; each finding has its scenario file in `tasks/4.20-fuzz-findings-2/`."""
    assert _digest(100, fuzz.SECOND_FAMILIES[family], "second") == digest


# ---------------------------------------------------------------- validity and coverage


def test_generated_scenarios_are_valid_and_stay_within_the_requested_operations():
    for family, runs in GENERATED.items():
        for seed, (doc, predicted) in enumerate(runs):
            scenario = sc.parse(doc)  # the format's own validator
            assert scenario.results, (family, seed)
            assert {op["op"] for op in doc["operations"]} <= fuzz.SECOND_FAMILIES[family]
            assert set(predicted) >= {op["name"] for op in doc["operations"]}
            for name, columns in predicted.items():
                names = [column for column, _ in columns]
                assert len(set(names)) == len(names), (family, seed, name)


def test_every_operation_and_form_is_drawn():
    seen: Counter = Counter()
    for _, _, op in OPERATIONS:
        kind = op["op"]
        seen[kind] += 1
        if kind in FORMULA_OPERATIONS:
            for formula in op["formulas"]:
                text = _expression(formula)
                seen["row variable"] += bool(ROW_VARIABLE.search(text))
                seen["shift"] += bool(SHIFT.search(text))
                seen["column array"] += bool(re.search(r"\w_\.size\(\)|\w_\[0\]|\(\w+_\)", text))
                seen["duration"] += bool(re.search(r"'P[T\d]", text))
                seen["zoned"] += bool(re.search(r"'\d{4}-\d\d-\d\d[^']* [A-Za-z/_]+'", text))
        elif kind == "where":
            for text in op["filters"]:
                seen["where shift"] += bool(SHIFT.search(text))
                seen["where row variable"] += bool(ROW_VARIABLE.search(text))
                seen["where zoned"] += bool(re.search(r" (?!UTC)[A-Za-z/_]+'$", text))
                seen["where utc"] += text.endswith(" UTC'")
        elif kind == "agg_by":
            seen["initial groups"] += "initial_groups" in op
            seen["preserve empty"] += op.get("preserve_empty") is True
            seen["initial groups without preserve empty"] += (
                "initial_groups" in op and not op.get("preserve_empty"))
        elif kind == "partition_get":
            seen[f"unique {op.get('unique', 'absent')}"] += 1
            seen["drop keys"] += op.get("drop_keys") is True
        elif kind == "partition_merge":
            seen["merge drop keys"] += op.get("drop_keys") is True
            seen["merge by none"] += not op["by"]
        elif kind == "ring_table":
            seen["ring refused"] += op["capacity"] <= 0
            seen["ring not initialized"] += op.get("initialize") is False
        elif kind == "keyed_transpose":
            seen["transpose initial groups"] += "initial_groups" in op
            seen[f"transpose {op.get('new_columns', 'absent')}"] += 1
            seen["transpose two aggs"] += len(op["aggs"]) > 1
        elif kind == "multi_join":
            keyless = not op["inputs"][0]["on"]
            seen["multi join keyless"] += keyless
            seen["multi join script"] += bool(op.get("script")) and not keyless
            seen["multi join service"] += not op.get("script")
        elif kind == "range_join":
            seen["range join no aggs"] += not op["aggs"]
            seen["range join script"] += bool(op.get("script")) and bool(op["aggs"])
        elif kind == "update_by":
            for operator in op["ops"]:
                seen[operator["op"]] += 1
        elif kind == "column_statistics":
            seen["statistics limit"] += "unique_value_limit" in op
    arrays = sum(bool(re.search(r"= [A-Z]\w* [-+*] (?:\d|[A-Z])|= \d\S* [-+*] [A-Z]", formula))
                 for doc, predicted, op in OPERATIONS if op["op"] in FORMULA_OPERATIONS
                 for formula, (_, kind) in zip(op["formulas"], predicted[op["name"]][-len(
                     op["formulas"]):]) if kind.startswith("list:"))
    assert arrays > 10
    for name in sorted(fuzz.SECOND_OPERATIONS) + [
            "row variable", "shift", "column array", "duration", "zoned", "where shift",
            "where row variable", "where zoned", "where utc", "initial groups",
            "preserve empty", "initial groups without preserve empty",
            "unique permit_multiple", "unique require_unique", "unique not_set",
            "unique absent", "drop keys", "merge drop keys", "merge by none", "ring refused",
            "ring not initialized", "transpose initial groups", "transpose ignore",
            "transpose absent", "transpose two aggs", "multi join keyless",
            "multi join script", "multi join service", "range join no aggs",
            "range join script", "rolling_formula_tick", "rolling_formula_time",
            "cum_count_where", "rolling_count_where_tick", "rolling_count_where_time",
            "statistics limit"]:
        assert seen[name] > 3, (name, seen[name])


# ---------------------------------------------------------------- restrictions


def test_nothing_is_drawn_that_can_fail_a_refreshing_table_in_a_later_cycle():
    """The module comment of `fuzz_second`: a ring over a table that can modify or shift, a
    transpose that fails on a new column, a side-by-side join of a table that can grow."""
    seen: Counter = Counter()
    for doc, _, op in OPERATIONS:
        live = _live(doc)
        by_name = {o["name"]: o for o in doc["operations"]}
        inputs = {t["name"]: t for t in doc["inputs"]}
        kind = op["op"]
        if kind == "ring_table" and op["source"] in live:
            seen["ring"] += 1
            name = op["source"]
            while name in by_name:
                assert by_name[name]["op"] in fuzz_second._APPENDING, (doc["name"], op)
                name = by_name[name]["source"]
            assert inputs[name]["ticking"]["key_columns"] == [], (doc["name"], op)
        elif kind == "keyed_transpose" and op["source"] in live:
            seen["transpose"] += 1
            assert op.get("new_columns") == "ignore", (doc["name"], op)
        elif kind == "multi_join" and not op["inputs"][0]["on"]:
            for item in op["inputs"]:
                if item["table"] in live:
                    seen["side by side"] += 1
                    source = by_name[item["table"]]
                    while source["op"] in fuzz_second._APPENDING | {"sort", "reverse", "flatten"}:
                        source = by_name[source["source"]]
                    assert (source["op"] in ("agg_by", "agg_all_by", "group_by")
                            and not source["by"]) or (
                        source["op"] in ("head", "tail") and source["rows"] <= 1) or (
                        source["op"] == "multi_join" and not source["inputs"][0]["on"]), (
                        doc["name"], op)
    assert seen["ring"] > 5 and seen["transpose"] > 5 and seen["side by side"] > 0


def test_what_a_finding_restricts_is_not_drawn():
    """`fuzz_second.RESTRICTIONS`, one check per finding."""
    seen: Counter = Counter()
    for doc, predicted, op in OPERATIONS:
        live = _live(doc)
        inputs = {t["name"] for t in doc["inputs"]}
        kind = op["op"]
        if kind in FORMULA_OPERATIONS:
            types = dict(predicted[op["source"]])
            for formula in op["formulas"]:
                text = _expression(formula)
                for column in re.findall(r"([A-Za-z_]\w*)_(?:\[|\.|\))", text):
                    seen["R6"] += 1
                    assert not types[column].startswith("list:"), (doc["name"], op)
                if kind == "lazy_update":
                    seen["R1"] += 1
                    rest = SHIFT.sub("", text)
                    assert not re.search(r"\(\w+_\[", text), (doc["name"], op)
                    assert not ROW_VARIABLE.search(rest) and "_[" not in rest and (
                        "_." not in text), (doc["name"], op)
                if kind == "select" and SHIFT.search(text):
                    seen["J2"] += 1
                    assert op["source"] in inputs, (doc["name"], op)
        elif kind == "where":
            distances = set()
            for text in op["filters"]:
                distances |= set(re.findall(r"_\[(i[^\]]*)\]", text))
            if op["source"] in live and distances:
                seen["R5"] += 1
                assert len(distances) == 1, (doc["name"], op)
        elif kind == "agg_by" and op["name"] in live and (
                "initial_groups" in op or op.get("preserve_empty")):
            types = dict(predicted[op["source"]])
            if "initial_groups" in op:
                if fuzz_second._Second.never_records(op["aggs"]):
                    seen["J7"] += 1
                    assert op["preserve_empty"] is True, (doc["name"], op)
                seen["J6"] += 1
                assert not {"first", "last"} <= {agg["agg"] for agg in op["aggs"]}, (
                    doc["name"], op)
            if op.get("preserve_empty"):
                seen["J5"] += 1
                assert not any(
                    agg["agg"] == "distinct" and types[pair.split("=")[-1].strip()] == "timestamp"
                    for agg in op["aggs"] for pair in agg.get("columns", ())), (doc["name"], op)
        elif kind == "keyed_transpose":
            types = dict(predicted[op["source"]])
            seen["R7"] += 1
            assert not {types[c] for c in op["col_by"]} & {"float32", "timestamp"}, (
                doc["name"], op)
        elif kind == "update_by":
            types = dict(predicted[op["source"]])
            for operator in op["ops"]:
                for text in operator.get("args", {}).get("filters", []):
                    seen["J1"] += 1  # and J4
                    found = fuzz_second._COLUMN_COMPARISON.match(text)
                    assert not found or found.group(2) not in types, (doc["name"], op)
    for name in ("J1", "J2", "J5", "J6", "J7", "R1", "R5", "R6", "R7"):
        assert seen[name] > 5, (name, seen[name])


def test_every_restriction_names_a_finding_that_is_written_up():
    text = "\n".join((ORACLE.parent / "tasks" / name).read_text()
                     for name in ("4.20-fuzz-findings-2.md", "4.23-fuzz-refusals.md"))
    for finding in fuzz_second.RESTRICTIONS:
        assert re.search(rf"^### {finding}\. ", text, re.MULTILINE), finding
    known = json.loads((ORACLE / "fuzz-known-findings.json").read_text())["findings"]
    for entry in known:
        found = re.search(r"tasks/4\.20-fuzz-findings-2\.md, (\w+)\.", entry["reason"])
        if found:
            assert re.search(rf"^### {found.group(1)}\. ", text, re.MULTILINE), entry["id"]
