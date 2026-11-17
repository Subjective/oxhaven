"""The generator for the date and time column types (`harness/fuzz_times.py`): it is
deterministic, what it builds is valid, it reaches every operation, type and aggregation it
claims, it keeps the restrictions it states, and each restriction names a finding that is
written up. Needs no server."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

from harness import fuzz, fuzz_times
from harness import scenario as sc
from harness.__main__ import _arrow_name

ORACLE = Path(__file__).resolve().parent.parent
FAMILY = fuzz.TIME_FAMILIES["time_types"]
GENERATED = [fuzz.generate(seed, FAMILY, "times") for seed in range(300)]
OPERATIONS = [(doc, predicted, op) for doc, predicted in GENERATED for op in doc["operations"]]
NOT_COMPARABLE = (fuzz_times.PERIOD, fuzz_times.ZONE)


def _text(doc: dict) -> str:
    return json.dumps(doc, sort_keys=True)


def _live(doc: dict) -> set[str]:
    live = {table["name"] for table in doc["inputs"] if "ticking" in table}
    for op in doc["operations"]:
        if any(name in live for name in sc.references(op)):
            live.add(op["name"])
    return live


def test_the_generator_is_reached_only_by_its_variants():
    assert not set(fuzz.TIME_FAMILIES) & (set(fuzz.FAMILIES) | set(fuzz.SECOND_FAMILIES))
    assert FAMILY <= fuzz.OPERATIONS
    for variants in ("all", "second"):
        assert _text(fuzz.generate(3, FAMILY, variants)[0]) != _text(GENERATED[3][0])


def test_generation_is_deterministic_per_seed():
    for seed in (0, 7, 299):
        again, predicted = fuzz.generate(seed, FAMILY, "times")
        assert _text(again) == _text(GENERATED[seed][0]), seed
        assert predicted == GENERATED[seed][1]
    assert len({_text(doc) for doc, _ in GENERATED}) > 290


def test_generation_is_the_same_in_another_process():
    code = ("import json; from harness import fuzz; "
            "print(json.dumps([fuzz.generate(s, fuzz.TIME_FAMILIES['time_types'], 'times')[0] "
            "for s in range(12)], sort_keys=True))")
    expected = json.dumps([GENERATED[seed][0] for seed in range(12)], sort_keys=True)
    for hash_seed in ("1", "2"):
        out = subprocess.run(
            [sys.executable, "-c", code], cwd=ORACLE, check=True, capture_output=True,
            text=True, env=dict(os.environ, PYTHONHASHSEED=hash_seed),
        ).stdout.strip()
        assert out == expected


def test_a_seed_is_the_scenario_it_was():
    """A digest over the scenarios of seeds 0 to 99 as the generator is at the end of task
    4.24, with the restrictions `fuzz_times.RESTRICTIONS` lists. A change to what a seed
    builds must be meant: the findings of the task are kept as scenario files in
    `tasks/4.24-fuzz-time-types/`, not as seeds."""
    digest = hashlib.sha256()
    for doc, _ in GENERATED[:100]:
        digest.update(json.dumps(doc, sort_keys=True).encode())
    assert digest.hexdigest()[:16] == "c57be00e01b30260"


def test_generated_scenarios_are_valid_and_stay_within_the_family():
    for seed, (doc, predicted) in enumerate(GENERATED):
        scenario = sc.parse(doc)  # the format's own validator
        assert scenario.results, seed
        # the two updates that make the time columns, and the family
        assert [op["op"] for op in doc["operations"][:2]] == ["update", "update"]
        assert {op["op"] for op in doc["operations"][2:]} <= FAMILY, seed
        for result in doc["results"]:
            columns = predicted[result["table"]]
            # every compared table is one the rules can read, and its columns are predicted
            assert not any(kind.endswith(fuzz_times.PERIOD) for _, kind in columns), seed
            assert all(_arrow_name(kind) != kind or kind.startswith("int")
                       for _, kind in columns), (seed, columns)


def test_every_operation_type_and_aggregation_is_drawn():
    seen: Counter = Counter()
    for doc, predicted, op in OPERATIONS:
        kind = op["op"]
        seen[kind] += 1
        if kind == "update":
            for formula in op["formulas"]:
                seen["makes " + formula.split(" =")[0]] += 1
        elif kind == "agg_by":
            types = dict(predicted[op["source"]])
            for column in op["by"]:
                seen["by " + types[column]] += 1
            for agg in op["aggs"]:
                seen[agg["agg"]] += 1
        elif kind == "sort":
            types = dict(predicted[op["source"]])
            seen["sorts"] += len(op["by"]) > 1
            for by in op["by"]:
                seen["sort " + types[by["column"]]] += 1
                seen[by["direction"]] += 1
        elif kind == "where":
            types = dict(predicted[op["source"]])
            for text in op["filters"]:
                column = re.match(r"(?:isNull\()?(\w+)", text).group(1)
                shape = ("range" if re.search(r" [<>]=? ", text) else
                         "in" if " in " in text else "match")
                seen[f"{shape} {types[column]}"] += 1
        elif kind in ("join", "natural_join", "exact_join"):
            types = dict(predicted[op["left"]])
            for column in op["on"]:
                seen["key " + types[column]] += 1
        elif kind == "view" and any("hourOfDay" in f for f in op["formulas"]):
            seen["zone shown"] += 1
    for kind in FAMILY - {"aj"}:  # R6
        assert seen[kind] > 20, (kind, seen[kind])
    for name in "DLMUWZPN":
        assert seen["makes " + name] > 100, name
    for kind in fuzz_times.TIME_TYPES:
        assert seen["by " + kind] > 5, kind
        assert seen["key " + kind] > 0, kind
    for kind in set(fuzz_times.TIME_TYPES) - {fuzz_times.DATETIME}:  # R2
        assert seen["sort " + kind] > 0, kind
        assert seen["match " + kind] > 0, kind
        assert seen["range " + kind] > 0, kind
    for agg in fuzz_times._AGGS:
        assert seen[agg] > 10, agg
    assert seen["sorts"] > 10 and seen["descending"] > 20 and seen["zone shown"] > 20
    assert any("ticking" in t for doc, _ in GENERATED for t in doc["inputs"])
    assert any("steps" in doc for doc, _ in GENERATED)


def test_what_a_finding_restricts_is_not_drawn():
    """`fuzz_times.RESTRICTIONS`, one check per finding."""
    checked: Counter = Counter()
    for doc, predicted, op in OPERATIONS:
        kind = op["op"]
        assert kind != "aj", doc["name"]  # R6
        if kind == "where":
            types = dict(predicted[op["source"]])
            for text in op["filters"]:
                column = re.match(r"(?:isNull\()?(\w+)", text).group(1)
                checked["filter"] += 1
                assert types[column] != fuzz_times.DATETIME, (doc["name"], op)  # R2
                if re.search(r" [<>]=? null$", text):  # R7
                    checked["range with null"] += 1
                    assert types[column] in (fuzz_times.DATE, fuzz_times.TIME,
                                             fuzz_times.ZONED), (doc["name"], op)
        elif kind == "agg_by":
            types = dict(predicted[op["source"]])
            for agg in op["aggs"]:
                for pair in agg.get("columns", ()):
                    column = pair.split("=")[-1].strip()
                    if types[column] in NOT_COMPARABLE:  # R3, J1
                        checked["aggregated"] += 1
                        assert agg["agg"] in ("first", "last", "sorted_first",
                                              "sorted_last"), (doc["name"], op)
                order = agg.get("args", {}).get("order_by")
                if order is not None:  # R4, J1
                    checked["ordered"] += 1
                    assert types[order] not in NOT_COMPARABLE, (doc["name"], op)
    for name in ("filter", "aggregated", "ordered"):
        assert checked[name] > 5, (name, checked[name])
    assert checked["range with null"] > 0


def test_no_join_is_drawn_that_can_fail_in_a_later_cycle():
    """A natural or exact join reads an aggregation by its own keys, which has one row a
    key."""
    for doc, _, op in OPERATIONS:
        if op["op"] in ("natural_join", "exact_join"):
            right = next(o for o in doc["operations"] if o["name"] == op["right"])
            assert right["op"] == "agg_by" and right["by"] == op["on"], (doc["name"], op)


def test_every_restriction_names_a_finding_that_is_written_up():
    task = ORACLE.parent / "tasks"
    text = (task / "4.24-fuzz-time-types.md").read_text()
    files = [path.name for path in (task / "4.24-fuzz-time-types").glob("*.json")]
    for finding in fuzz_times.RESTRICTIONS:
        assert re.search(rf"^### {finding}\. ", text, re.MULTILINE), finding
        assert any(name.startswith(finding + "_") for name in files), finding
    for name in files:
        sc.parse(json.loads((task / "4.24-fuzz-time-types" / name).read_text()))
