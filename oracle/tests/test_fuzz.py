"""The fuzzer's generator and shrinker, and the `fuzz` command end to end. Needs no server:
the servers are replaced by an in-process target, and the faulty one by `faults.FaultyTarget`
around it, which is also how a faulty server is given on the command line."""

from __future__ import annotations

import copy
import json
import math
import os
import subprocess
import sys
from pathlib import Path

import pytest

from harness import __main__ as cli
from harness import faults, fuzz, rules
from harness import scenario as sc
from harness.results import ResultSet

ORACLE = Path(__file__).resolve().parent.parent
ALL_OPS = fuzz.OPERATIONS
SEEDS = range(200)
GENERATED = {seed: fuzz.generate(seed, ALL_OPS)[0] for seed in SEEDS}


def test_generation_is_deterministic():
    for seed in (0, 1, 17):
        again, _ = fuzz.generate(seed, ALL_OPS)
        assert json.dumps(again, sort_keys=True) == json.dumps(GENERATED[seed], sort_keys=True)
    texts = {json.dumps(doc, sort_keys=True) for doc in GENERATED.values()}
    assert len(texts) > 190, "seeds mostly produce the same scenario"
    # The operation set is part of the seed.
    narrow, _ = fuzz.generate(0, frozenset({"where", "sort"}))
    assert narrow != GENERATED[0]


def test_generation_is_the_same_in_another_process():
    """Python randomizes string hashes per process; the generator must not depend on them."""
    code = ("import json; from harness import fuzz, scenario as sc; "
            "print(json.dumps(fuzz.generate(5, fuzz.OPERATIONS)[0], sort_keys=True))")
    for hash_seed in ("1", "2"):
        out = subprocess.run(
            [sys.executable, "-c", code], cwd=ORACLE, check=True, capture_output=True,
            text=True, env=dict(os.environ, PYTHONHASHSEED=hash_seed),
        ).stdout.strip()
        assert out == json.dumps(GENERATED[5], sort_keys=True)


def test_generated_scenarios_are_valid_and_stay_within_the_requested_operations():
    for seed, doc in GENERATED.items():
        scenario = sc.parse(doc)
        assert scenario.results, seed
    for ops in ({"where"}, {"sort", "head"}, {"natural_join"}, {"agg_by", "update_by"}):
        for seed in range(40):
            doc, predicted = fuzz.generate(seed, frozenset(ops))
            used = {op["op"] for op in sc.parse(doc).operations}
            assert used <= ops, (seed, ops, used)
            assert set(predicted) >= {op["name"] for op in doc["operations"]}
    with pytest.raises(sc.ScenarioError):
        fuzz.generate(0, frozenset())


def test_generated_scenarios_cover_what_the_task_asks_for():
    scenarios = [sc.parse(doc) for doc in GENERATED.values()]
    assert {op["op"] for s in scenarios for op in s.operations} == ALL_OPS
    types = {c.type for s in scenarios for i in s.inputs for c in i.columns}
    assert types == set(sc.COLUMN_TYPES)
    cells = [cell for s in scenarios for i in s.inputs for row in i.rows for cell in row]
    floats = [c for c in cells if isinstance(c, float)]
    assert any(c is None for c in cells)
    assert any(math.isnan(c) for c in floats) and any(math.isinf(c) for c in floats)
    assert any(c == 0.0 and math.copysign(1.0, c) < 0 for c in floats), "no negative zero"
    ints = [c for c in cells if isinstance(c, int) and not isinstance(c, bool)]
    assert 2**63 - 1 in ints and 2**31 - 1 in ints and -(2**31) + 1 in ints
    assert any(not i.rows for s in scenarios for i in s.inputs), "no empty input"

    ticking = [s for s in scenarios if s.ticking]
    assert 40 < len(ticking) < 180, "static and ticking scenarios must both be common"
    actions = {a.action for s in ticking for step in s.steps for a in step}
    assert actions == {"add", "delete"}
    modifies = 0
    for s in ticking:
        for item in s.inputs:
            keys = [c.name for c in sc.key_columns_of(item)] if item.ticking else []
            if not keys:
                continue
            index = [i for i, c in enumerate(item.columns) if c.name in keys]
            present = {tuple(row[i] for i in index) for row in item.rows}
            for step in s.steps:
                for action in step:
                    if action.table == item.name and action.action == "add":
                        modifies += sum(tuple(r[i] for i in index) in present
                                        for r in action.rows)
    assert modifies > 20, "adds that replace an existing key (modifies) are rare"


# ---------------------------------------------------------------- shrinking


def _rows(doc: dict) -> int:
    return sum(len(i["rows"]) for i in doc["inputs"])


def test_shrink_reduces_to_what_the_difference_needs():
    """The difference here needs one sort operation and one row holding the value 7 in K."""
    doc = {
        "format": 1,
        "name": "big",
        "inputs": [
            {"name": "t", "ticking": {"key_columns": ["Id"]},
             "columns": [{"name": "Id", "type": "int32"}, {"name": "K", "type": "int32"},
                         {"name": "S", "type": "string"}],
             "rows": [[i, i, "s"] for i in range(12)]},
            {"name": "u", "columns": [{"name": "K", "type": "int32"}], "rows": [[1], [2]]},
        ],
        "operations": [
            {"name": "a", "op": "where", "source": "t", "filters": ["K > 2"]},
            {"name": "b", "op": "sort", "source": "a",
             "by": [{"column": "K", "direction": "descending"}]},
            {"name": "c", "op": "head", "source": "u", "rows": 1},
            {"name": "d", "op": "view", "source": "b", "formulas": ["K", "X = K + 1"]},
        ],
        "results": [{"table": "a"}, {"table": "b"}, {"table": "c"}, {"table": "d"}],
        "steps": [
            {"actions": [{"action": "add", "table": "t", "rows": [[20, 20, "x"]]}]},
            {"actions": [{"action": "delete", "table": "t", "rows": [[3]]}]},
        ],
    }
    calls = []

    def still_differs(candidate: dict) -> bool:
        calls.append(candidate)
        sc.parse(candidate)  # the shrinker only offers valid scenarios
        has_sort = any(op["op"] == "sort" for op in candidate["operations"])
        k = [c["name"] for c in candidate["inputs"][0]["columns"]]
        has_seven = "K" in k and any(
            row[k.index("K")] == 7 for row in candidate["inputs"][0]["rows"])
        return has_sort and has_seven

    before = copy.deepcopy(doc)
    small, runs = fuzz.shrink(doc, still_differs)
    assert doc == before, "shrink must not change its argument"
    assert still_differs(small)
    assert runs <= 200 and runs <= len(calls)
    assert [i["name"] for i in small["inputs"]] == ["t"]
    assert _rows(small) == 1
    assert [op["op"] for op in small["operations"]] == ["sort"]
    assert small.get("steps", []) == []
    assert "ticking" not in small["inputs"][0]
    assert [c["name"] for c in small["inputs"][0]["columns"]] == ["K"]


def test_shrink_removes_single_rows_when_halving_cannot():
    """The difference needs the first and the last row, which no half of the rows contains."""
    doc = {
        "format": 1,
        "name": "ends",
        "inputs": [{"name": "t", "columns": [{"name": "K", "type": "int32"}],
                    "rows": [[7], [1], [2], [3], [9]]}],
        "operations": [{"name": "h", "op": "head", "source": "t", "rows": 5}],
        "results": [{"table": "h"}],
    }

    def still_differs(candidate: dict) -> bool:
        cells = [row[0] for row in candidate["inputs"][0]["rows"]]
        return 7 in cells and 9 in cells

    small, _ = fuzz.shrink(doc, still_differs)
    assert small["inputs"][0]["rows"] == [[7], [9]]


def test_shrink_keeps_the_scenario_when_nothing_smaller_differs():
    doc = GENERATED[3]
    small, _ = fuzz.shrink(doc, lambda candidate: candidate == doc, max_runs=30)
    assert small == doc


def test_shrink_respects_the_run_limit():
    calls = []

    def always(candidate: dict) -> bool:
        calls.append(1)
        return True

    _, runs = fuzz.shrink(GENERATED[0], always, max_runs=3)
    assert runs == 3


# ---------------------------------------------------------------- the command


class EchoTarget:
    """Stands in for a server: every result is the input table that its operation chain
    starts from (following the first source of each operation), unchanged, at every step."""

    def __init__(self, name: str = "echo"):
        self.name = name

    def __str__(self) -> str:
        return self.name

    def run(self, scenario: sc.Scenario) -> ResultSet:
        results = ResultSet(scenario.name)
        operations = {op["name"]: op for op in scenario.operations}
        for result in scenario.results:
            name = result.table
            while name in operations:
                op = operations[name]
                name = op.get("source") or op.get("left") or op["sources"][0]
            root = scenario.input(name)
            data = sc.arrow_table(root.columns, root.rows)
            for step in range(len(scenario.steps) + 1):
                results.put(step, result.table, data)
        return results


def _targets(monkeypatch, faulty: str | None):
    def open_target(url: str):
        if url.startswith("faulty:"):
            spec, _, inner = url[len("faulty:"):].partition("@")
            return faults.FaultyTarget(spec, EchoTarget(inner))
        return EchoTarget(url)

    monkeypatch.setattr(cli, "open_target", open_target)


def test_fuzz_command_is_clean_when_both_sides_agree(monkeypatch, tmp_path, capsys):
    _targets(monkeypatch, None)
    code = cli.main(["fuzz", "--a", "x", "--ops", "where,sort,agg_by", "--seeds", "25",
                     "--out", str(tmp_path)])
    out = capsys.readouterr().out
    assert code == 0, out
    assert "25 seeds, 25 clean, 0 with differences, 0 not run" in out
    assert list(tmp_path.iterdir()) == []


def test_fuzz_keep_going_runs_every_seed_and_groups_the_differences(
        monkeypatch, tmp_path, capsys):
    _targets(monkeypatch, "changed_table_attribute")
    arguments = ["fuzz", "--a", "x", "--b", "faulty:changed_table_attribute@x", "--ops",
                 "where,sort,slice", "--seeds", "12", "--out", str(tmp_path), "--keep-going"]
    code = cli.main(arguments)
    out = capsys.readouterr().out
    assert code == 1, out
    assert "DIFF" not in out and "shrunk" not in out
    saved = sorted(path.name for path in tmp_path.iterdir())
    assert saved and all(name.startswith("fuzz_") and "_min" not in name for name in saved)
    summary = out.strip().splitlines()[-1]
    assert summary.startswith("fuzz: 12 seeds, ") and "0 not run" in summary
    differing = int(summary.split(" clean, ")[1].split(" with differences")[0])
    assert differing == len(saved)
    groups = [line for line in out.splitlines() if line.startswith("GROUP ")]
    assert groups and len(groups) == int(summary.split(" in ")[1].split(" groups")[0])
    # One cause, one group per operation it shows on: the fault changes one attribute.
    assert all(line.startswith(
        "GROUP kind=table_metadata key=deephaven:attribute.BlinkTable operation=")
        for line in groups), groups
    assert len({line.split(":")[0] + line.split("operation=")[1].split(":")[0]
                for line in groups}) == len(groups)
    for line in groups:
        seed = int(line.split("seeds ")[-1].split(",")[0].split(" ")[0])
        assert f"fuzz_{seed}.json" in saved

    _targets(monkeypatch, None)
    assert cli.main(arguments[:3] + arguments[5:]) == 0
    assert "12 seeds, 12 clean, 0 with differences in 0 groups, 0 not run" in capsys.readouterr().out


@pytest.mark.parametrize("fault", ["nan_to_null", "truncated_after_three", "dropped_row"])
def test_fuzz_command_catches_and_shrinks_a_faulty_target(monkeypatch, tmp_path, capsys, fault):
    _targets(monkeypatch, fault)
    code = cli.main(["fuzz", "--a", "x", "--b", f"faulty:{fault}@x", "--ops",
                     ",".join(sorted(ALL_OPS)), "--seeds", "200", "--out", str(tmp_path)])
    out = capsys.readouterr().out
    assert code == 1, out
    seed = int(out.split("DIFF  seed ")[1].split(":")[0])
    assert f"fuzz: seed {seed} differs" in out

    original = sc.load(tmp_path / f"fuzz_{seed}.json")
    minimal = sc.load(tmp_path / f"fuzz_{seed}_min.json")
    assert original.digest() == sc.parse(GENERATED[seed]).digest(), "the seed reproduces it"

    # The minimal scenario still shows the difference, and is as small as this fault allows.
    good, bad = EchoTarget(), faults.FaultyTarget(fault, EchoTarget())
    assert rules.compare_scenario(minimal, good.run(minimal), bad.run(minimal))
    need = {"nan_to_null": 1, "truncated_after_three": 4, "dropped_row": 1}[fault]
    assert sum(len(i.rows) for i in minimal.inputs) == need
    assert len(minimal.inputs) == 1 and len(minimal.steps) == 0
    # A result may name an input directly, so no operation has to remain.
    assert len(minimal.results) == 1 and len(minimal.operations) == 0
    assert len(minimal.inputs[0].columns) == 1
    if fault == "nan_to_null":
        assert math.isnan(minimal.inputs[0].rows[0][0])

    # `diff` reruns the written file.
    code = cli.main(["diff", "--a", "x", "--b", f"faulty:{fault}@x",
                     "--scenarios", str(tmp_path / f"fuzz_{seed}_min.json")])
    assert code == 1
    code = cli.main(["diff", "--a", "x", "--b", "x",
                     "--scenarios", str(tmp_path / f"fuzz_{seed}_min.json")])
    assert code == 0
