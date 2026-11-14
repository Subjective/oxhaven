"""The scenario format and the starter corpus. Needs no server."""

from __future__ import annotations

import copy
import json
import math
from pathlib import Path

import pyarrow as pa
import pytest

from harness import results as res
from harness import rules
from harness import scenario as sc

ORACLE = Path(__file__).resolve().parent.parent
SCENARIOS = sc.load_all(ORACLE / "scenarios")

MINIMAL = {
    "format": 1,
    "name": "minimal",
    "inputs": [
        {
            "name": "t",
            "columns": [{"name": "K", "type": "int32"}, {"name": "D", "type": "float64"}],
            "rows": [[1, 0.5], [None, "NaN"]],
            "ticking": {"key_columns": ["K"]},
        }
    ],
    "operations": [{"name": "w", "op": "where", "source": "t", "filters": ["K > 0"]}],
    "results": [{"table": "w"}],
    "steps": [{"actions": [{"action": "delete", "table": "t", "rows": [[1]]}]}],
}


def changed(**edits) -> dict:
    doc = copy.deepcopy(MINIMAL)
    doc.update(edits)
    return doc


def test_minimal_scenario_parses():
    scenario = sc.parse(MINIMAL)
    assert scenario.ticking
    assert scenario.inputs[0].rows[0] == (1, 0.5)
    assert scenario.inputs[0].rows[1][0] is None and math.isnan(scenario.inputs[0].rows[1][1])
    data = sc.arrow_table(scenario.inputs[0].columns, scenario.inputs[0].rows)
    assert data.schema.types == [pa.int32(), pa.float64()]
    assert scenario.steps[0][0].rows == ((1,),)


@pytest.mark.parametrize(
    "doc",
    [
        changed(format=2),
        changed(name="not a name"),
        changed(surprise=1),
        changed(operations=[{"name": "w", "op": "explode", "source": "t"}]),
        changed(operations=[{"name": "w", "op": "where", "source": "nope", "filters": ["K > 0"]}]),
        changed(operations=[{"name": "w", "op": "where", "source": "w", "filters": ["K > 0"]}]),
        changed(operations=[{"name": "t", "op": "where", "source": "t", "filters": ["K > 0"]}]),
        changed(operations=[{"name": "w", "op": "head", "source": "t", "rows": -1}]),
        changed(results=[{"table": "missing"}]),
        changed(results=[{"table": "w", "ordered": False}]),
        changed(results=[{"table": "w", "ordered": True, "unordered_reason": "x"}]),
        changed(steps=[{"actions": [{"action": "add", "table": "w", "rows": []}]}]),
        changed(steps=[{"actions": [{"action": "add", "table": "t", "rows": [[1]]}]}]),
        changed(steps=[{"actions": [{"action": "delete", "table": "t", "rows": [[1, 0.5]]}]}]),
        changed(steps=[{"actions": [{"action": "upsert", "table": "t", "rows": [[1, 0.5]]}]}]),
    ],
)
def test_invalid_scenarios_are_rejected(doc):
    with pytest.raises(sc.ScenarioError):
        sc.parse(doc)


@pytest.mark.parametrize(
    ("type_name", "cell"),
    [("int8", 128), ("int32", 2**31), ("int64", 2**63), ("int32", 1.5), ("int32", True),
     ("bool", 1), ("string", 1), ("float64", "nan"), ("float64", "abc"), ("timestamp", "2024")],
)
def test_cells_that_do_not_fit_their_type_are_rejected(type_name, cell):
    with pytest.raises(sc.ScenarioError):
        sc.decode_cell("cell", type_name, cell)


def test_an_unordered_result_needs_a_reason():
    doc = changed(results=[{"table": "w", "ordered": False,
                            "unordered_reason": "io.deephaven.Example#method does not order"}])
    assert sc.parse(doc).results[0].ordered is False


def test_digest_follows_content_not_layout():
    reordered = json.loads(json.dumps(MINIMAL, sort_keys=True, indent=7))
    assert sc.parse(reordered).digest() == sc.parse(MINIMAL).digest()
    other = changed(description="different")
    assert sc.parse(other).digest() != sc.parse(MINIMAL).digest()


def test_starter_corpus_size_and_coverage():
    static = [s for s in SCENARIOS if not s.ticking]
    ticking = [s for s in SCENARIOS if s.ticking]
    assert len(static) >= 20 and len(ticking) >= 5
    assert all(s.steps for s in ticking)
    used = {op["op"] for s in SCENARIOS for op in s.operations}
    assert used == set(sc.OPERATIONS)
    aggs = {a["agg"] for s in SCENARIOS for op in s.operations if op["op"] == "agg_by"
            for a in op["aggs"]}
    assert aggs == sc.AGGREGATIONS
    ticking_ops = {op["op"] for s in ticking for op in s.operations}
    assert ticking_ops == set(sc.OPERATIONS)
    actions = {a.action for s in ticking for step in s.steps for a in step}
    assert actions == sc.ACTIONS
    assert any(not i.rows for s in SCENARIOS for i in s.inputs), "no empty input table"


def test_result_set_round_trips_through_arrow_ipc(tmp_path):
    field = pa.field("D", pa.float64(), metadata={"deephaven:type": "double"})
    data = pa.Table.from_arrays(
        [pa.array([1.5, None, math.nan])], schema=pa.schema([field], metadata={"k": "v"})
    )
    results = res.ResultSet("demo")
    results.put(0, "a", data)
    results.put(1, "a", data.slice(0, 0))
    res.save(results, tmp_path / "demo", "digest", "42.9")
    loaded = res.load(tmp_path / "demo", "demo", "digest")
    assert loaded.keys() == [(0, "a"), (1, "a")]
    # Not Table.equals: Arrow's equality says NaN differs from NaN.
    assert rules.compare_tables("demo", "a", 0, data, loaded.get(0, "a")) == []
    assert loaded.get(0, "a").column("D").to_pylist()[1] is None
    assert loaded.get(0, "a").schema.field("D").metadata == {b"deephaven:type": b"double"}
    assert loaded.get(1, "a").num_rows == 0
    with pytest.raises(res.RecordingError):
        res.load(tmp_path / "demo", "demo", "another digest")
    with pytest.raises(res.RecordingError):
        res.load(tmp_path / "absent", "demo", "digest")
