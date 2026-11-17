"""The wide generator (`harness/fuzz_wide.py`): it is deterministic, what it builds is valid,
it reaches every operation and variant it claims, it keeps its two promises about refreshing
inputs, and it leaves the recorded generator alone. Needs no server."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

from harness import fuzz, fuzz_wide, rules
from harness import scenario as sc

ORACLE = Path(__file__).resolve().parent.parent
SEEDS = range(150)
GENERATED = {
    family: [fuzz.generate(seed, ops, "all") for seed in SEEDS]
    for family, ops in fuzz.FAMILIES.items()
}
OPERATIONS = [op for runs in GENERATED.values() for doc, _ in runs for op in doc["operations"]]


def _text(doc: dict) -> str:
    return json.dumps(doc, sort_keys=True)


# ---------------------------------------------------------------- the recorded generator


@pytest.mark.parametrize("seed, ops, digest", [
    (0, sorted(fuzz.RECORDED_OPERATIONS), "21bfb9f687ab740f"),
    (337, ["agg_by"], "58c72a31704b940c"),  # run log: `min` keeps 0.0 where Java keeps -0.0
    (39, ["aj"], "0180346eb7ae46a1"),  # run log: NaN stamps, both sides one ticking table
    (5, ["update_by"], "0e6aa1f3d9ee77b3"),
    (18, ["where", "sort"], "c5476450fba0bf94"),
])
def test_a_seed_of_the_recorded_generator_is_the_scenario_it_was(seed, ops, digest):
    """The digests were taken from the generator as it was before `fuzz_wide` existed. A seed
    in the run log must keep naming the scenario that showed the difference."""
    doc, _ = fuzz.generate(seed, frozenset(ops))
    assert hashlib.sha256(_text(doc).encode()).hexdigest()[:16] == digest
    assert fuzz.generate(seed, frozenset(ops), "recorded")[0] == doc


def test_variants_default_to_recorded_only_where_the_recorded_generator_can_build():
    assert fuzz.default_variants({"agg_by", "update_by"}) == "recorded"
    assert fuzz.default_variants({"agg_by", "raj"}) == "all"
    assert fuzz.generate(3, frozenset({"raj"}))[0] == fuzz.generate(3, frozenset({"raj"}), "all")[0]
    # The same seed and operations are another scenario under the other variants.
    assert fuzz.generate(3, frozenset({"agg_by"}))[0] != fuzz.generate(
        3, frozenset({"agg_by"}), "all")[0]
    with pytest.raises(sc.ScenarioError, match="recorded generator does not build raj"):
        fuzz.generate(0, frozenset({"raj"}), "recorded")
    with pytest.raises(sc.ScenarioError):
        fuzz.generate(0, frozenset({"where"}), "most")
    with pytest.raises(sc.ScenarioError):
        fuzz.generate(0, frozenset({"time_table"}), "all")
    assert fuzz.RECORDED_OPERATIONS < fuzz.OPERATIONS <= set(sc.OPERATIONS)
    assert set().union(*fuzz.FAMILIES.values()) == fuzz.OPERATIONS


# ---------------------------------------------------------------- determinism


def test_generation_is_deterministic_per_seed():
    for family, ops in fuzz.FAMILIES.items():
        for seed in (0, 7, 149):
            again, predicted = fuzz.generate(seed, ops, "all")
            assert _text(again) == _text(GENERATED[family][seed][0]), (family, seed)
            assert predicted == GENERATED[family][seed][1]
        texts = {_text(doc) for doc, _ in GENERATED[family]}
        assert len(texts) > 140, f"{family}: seeds mostly produce the same scenario"


def test_generation_is_the_same_in_another_process():
    """Python randomizes string hashes per process; the generator must not depend on them."""
    code = ("import json; from harness import fuzz; "
            "print(json.dumps([fuzz.generate(s, fuzz.OPERATIONS, 'all')[0] for s in range(12)], "
            "sort_keys=True))")
    expected = json.dumps([GENERATED["all"][seed][0] for seed in range(12)], sort_keys=True)
    for hash_seed in ("1", "2"):
        out = subprocess.run(
            [sys.executable, "-c", code], cwd=ORACLE, check=True, capture_output=True,
            text=True, env=dict(os.environ, PYTHONHASHSEED=hash_seed),
        ).stdout.strip()
        assert out == expected


# ---------------------------------------------------------------- validity


def _named_columns(op: dict) -> list[tuple[str, str]]:
    """(table, column) for every column an operation names in one of its tables."""
    kind = op["op"]
    out: list[tuple[str, str]] = []

    def source_side(texts, table):
        out.extend((table, rules.split_pair(text)[1]) for text in texts)

    if kind == "multi_join":
        for item in op["inputs"]:
            out.extend((item["table"], rules.right_key(match)) for match in item["on"])
            source_side(item.get("joins") or [], item["table"])
        return out
    if kind in ("natural_join", "exact_join", "join", "left_join", "aj", "raj", "where_in"):
        for match in op["on"]:
            left = match.replace("<", "=").replace(">", "=").split("=")[0].strip()
            out.append((op["left"], left))
            out.append((op["right"], rules.right_key(match)))
        source_side(op.get("joins") or [], op["right"])
        return out
    if kind == "range_join":
        out.extend((side, key) for key in op["on"][:-1] for side in (op["left"], op["right"]))
        low, middle, high = [part.strip() for part in op["on"][-1].replace("<-", "").replace(
            "->", "").replace("<=", "<").split("<")]
        out += [(op["left"], low), (op["right"], middle), (op["left"], high)]
        for agg in op["aggs"]:
            source_side(agg["columns"], op["right"])
        return out
    source = op.get("source")
    if kind == "snapshot_when":
        source_side(op.get("stamp_columns", []), op["trigger"])
    elif kind == "sort":
        out.extend((source, by["column"]) for by in op["by"])
    elif kind in ("drop_columns", "select_distinct", "ungroup"):
        out.extend((source, column) for column in op.get("columns", []))
    elif kind in ("head_by", "tail_by", "group_by"):
        out.extend((source, column) for column in op["by"])
    elif kind == "agg_all_by":
        out.extend((source, column) for column in op["by"])
        out.extend((source, value) for key, value in op["agg"].get("args", {}).items()
                   if key in ("order_by", "wcol"))
    elif kind == "agg_by":
        out.extend((source, column) for column in op["by"])
        for agg in op["aggs"]:
            source_side(agg.get("columns", []), source)
            out.extend((source, value) for key, value in agg.get("args", {}).items()
                       if key in ("order_by", "wcol"))
    elif kind == "update_by":
        out.extend((source, column) for column in op["by"])
        for operator in op["ops"]:
            source_side(operator["columns"], source)
            out.extend((source, value) for key, value in operator.get("args", {}).items()
                       if key in ("ts_col", "wcol"))
    return out


def test_generated_scenarios_are_valid_and_stay_within_the_requested_operations():
    for family, runs in GENERATED.items():
        for seed, (doc, predicted) in enumerate(runs):
            scenario = sc.parse(doc)  # the format's own validator
            assert scenario.results, (family, seed)
            assert {op["op"] for op in doc["operations"]} <= fuzz.FAMILIES[family]
            assert set(predicted) >= {op["name"] for op in doc["operations"]}
            # Every column an operation names is a column its table is predicted to have:
            # a request the reference refuses is refused for a type or an argument, never
            # for a name the generator made up.
            for op in doc["operations"]:
                for table, column in _named_columns(op):
                    assert column in dict(predicted[table]), (family, seed, op, column)
            for name, columns in predicted.items():
                names = [column for column, _ in columns]
                assert len(set(names)) == len(names), (family, seed, name)


def test_requests_are_mostly_ones_the_reference_takes():
    """Counted against the type tables read from the reference: an operator or aggregation
    over a column type the reference does not take is drawn, and rarely."""
    refused = total = 0
    for runs in GENERATED.values():
        for doc, predicted in runs:
            for op in doc["operations"]:
                if op["op"] == "update_by":
                    specs = [(o["op"], o["columns"], fuzz_wide.UPDATE_BY) for o in op["ops"]]
                elif op["op"] == "agg_by":
                    specs = [(a["agg"], a["columns"], fuzz_wide.AGGREGATIONS)
                             for a in op["aggs"]
                             if "columns" in a and a["agg"] != "formula"]
                else:
                    continue
                types = dict(predicted[op["source"]])
                for name, pairs, table in specs:
                    for pair in pairs:
                        total += 1
                        refused += types[rules.split_pair(pair)[1]] not in table[name][0]
    assert total > 1500
    assert 0 < refused < total * 0.1, (refused, total)


# ---------------------------------------------------------------- coverage


def test_every_operation_and_variant_is_drawn():
    used = Counter(op["op"] for op in OPERATIONS)
    assert set(used) == fuzz.OPERATIONS
    assert {op["op"] for doc, _ in GENERATED["all"] for op in doc["operations"]} == fuzz.OPERATIONS

    operators = Counter(o["op"] for op in OPERATIONS if op["op"] == "update_by"
                        for o in op["ops"])
    assert set(operators) == set(fuzz_wide.UPDATE_BY)
    # What is left out is what the module comment says, and nothing else.
    assert sc.UPDATE_BY_OPS - set(fuzz_wide.UPDATE_BY) == {
        "cum_count_where", "rolling_formula_tick", "rolling_formula_time",
        "rolling_count_where_tick", "rolling_count_where_time"}
    arguments = {key for op in OPERATIONS if op["op"] == "update_by" for o in op["ops"]
                 for key in o.get("args", {})}
    assert arguments == {"decay_ticks", "decay_time", "ts_col", "rev_ticks", "fwd_ticks",
                         "rev_time", "fwd_time", "wcol", "delta_control", "op_control"}
    assert any(len(op["by"]) == 0 for op in OPERATIONS if op["op"] == "update_by")
    assert any(len(op["by"]) == 2 for op in OPERATIONS if op["op"] == "update_by")

    aggregations = {a["agg"] for op in OPERATIONS if op["op"] == "agg_by" for a in op["aggs"]}
    assert aggregations == set(sc.AGGREGATIONS)
    assert {op["agg"]["agg"] for op in OPERATIONS if op["op"] == "agg_all_by"} >= {
        "sum", "avg", "min", "max", "first", "last", "std", "var", "median", "pct", "group",
        "count_distinct", "distinct", "unique", "sorted_first", "sorted_last", "abs_sum"}
    # A sentinel of each kind and each formula are rare; look at more seeds of the one family
    # that draws them.
    more = [a for seed in range(1500)
            for op in fuzz.generate(seed, fuzz.FAMILIES["aggregations"], "all")[0]["operations"]
            if op["op"] == "agg_by" for a in op["aggs"]]
    sentinels = [a["args"]["non_unique_sentinel"] for a in more
                 if "non_unique_sentinel" in a.get("args", {})]
    assert {type(value) for value in sentinels} == {int, float, str}
    assert {a["args"]["formula"] for a in more if a["agg"] == "formula"} == set(
        fuzz_wide.FORMULAS)

    joins = [op for op in OPERATIONS if op["op"] in (
        "natural_join", "exact_join", "join", "left_join", "aj", "raj")]
    assert {op["type"] for op in joins if op["op"] == "natural_join"} == sc.NATURAL_JOIN_TYPES
    assert any("joins" not in op for op in joins) and any(len(op["on"]) == 2 for op in joins)
    assert any(not op["on"] for op in joins if op["op"] == "join")
    for kind, rules_ in (("aj", (">=", ">")), ("raj", ("<=", "<"))):
        seen = {rule for op in joins if op["op"] == kind
                for rule in (">=", "<=", ">", "<") if f" {rule} " in op["on"][-1]}
        assert set(rules_) <= seen, kind
    assert {bool(op.get("inverted")) for op in OPERATIONS if op["op"] == "where_in"} == {
        True, False}
    ranges = [op["on"][-1] for op in OPERATIONS if op["op"] == "range_join"]
    assert any(text.startswith("<- ") for text in ranges)
    assert any(text.endswith(" ->") for text in ranges)
    assert any(" < " in text for text in ranges) and any(" <= " in text for text in ranges)
    assert {len(op["inputs"]) for op in OPERATIONS if op["op"] == "multi_join"} == {1, 2, 3}
    flags = {tuple(flag for flag in ("initial", "incremental", "history") if op.get(flag))
             for op in OPERATIONS if op["op"] == "snapshot_when"}
    assert {(), ("initial",), ("incremental",), ("history",),
            ("initial", "incremental")} <= flags
    assert any("columns" in op for op in OPERATIONS if op["op"] == "ungroup")
    assert any(by.get("absolute") for op in OPERATIONS if op["op"] == "sort" for by in op["by"])

    conditions: Counter = Counter()

    def walk(condition: dict) -> None:
        kind, body = next(iter(condition.items()))
        conditions[kind] += 1
        for inner in body if kind in ("and", "or") else [body] if kind == "not" else []:
            walk(inner)

    for op in OPERATIONS:
        for condition in op.get("conditions", []):
            walk(condition)
    assert set(conditions) == {"and", "or", "not", "compare", "in", "is_null", "is_nan",
                               "matches", "contains"}

    inputs = [i for runs in GENERATED.values() for doc, _ in runs for i in doc["inputs"]]
    assert {c["type"] for i in inputs for c in i["columns"]} == set(sc.COLUMN_TYPES)
    modes = Counter("static" if "ticking" not in i else
                    "keyed" if i["ticking"]["key_columns"] else "append" for i in inputs)
    assert all(modes[mode] > len(inputs) * 0.1 for mode in ("static", "keyed", "append"))


# ---------------------------------------------------------------- what refreshing inputs keep


def _replay(item: dict, steps: list[dict]):
    """The rows of an input after each step, as a keyed input table keeps them: a row with
    an existing key replaces that row in place, a new one goes to the end."""
    keyed = "ticking" in item and item["ticking"]["key_columns"]
    rows = [list(row) for row in item["rows"]]
    yield rows
    for step in steps:
        for action in step["actions"]:
            if action["table"] != item["name"]:
                continue
            for row in action["rows"]:
                position = next((i for i, old in enumerate(rows) if keyed and old[0] == row[0]),
                                None)
                if action["action"] == "delete":
                    if position is not None:
                        rows.pop(position)
                elif position is None:
                    rows.append(list(row))
                else:
                    rows[position] = list(row)
        yield rows


def test_timestamps_of_a_refreshing_input_never_go_backwards():
    """What lets a time window be drawn over a refreshing table: its `T` stays in order
    through every step, so the window never fails during an update cycle."""
    checked = 0
    for runs in GENERATED.values():
        for doc, _ in runs:
            for item in doc["inputs"]:
                names = [c["name"] for c in item["columns"]]
                if "ticking" not in item or "T" not in names:
                    continue
                for rows in _replay(item, doc.get("steps", [])):
                    stamps = [row[names.index("T")] for row in rows]
                    stamps = [stamp for stamp in stamps if stamp is not None]
                    assert stamps == sorted(stamps), (doc["name"], item["name"])
                    checked += 1
    assert checked > 500


def test_a_time_window_over_a_refreshing_table_reads_ordered_timestamps():
    windows = 0
    for runs in GENERATED.values():
        for doc, _ in runs:
            ticking = {i["name"] for i in doc["inputs"] if "ticking" in i}
            reorders = set()  # tables whose rows are not in the order of an input's rows
            refreshing = set(ticking)
            for op in doc["operations"]:
                sources = sc.references(op)
                if set(sources) & refreshing and op["op"] not in ("snapshot", "meta_table"):
                    refreshing.add(op["name"])
                if op["op"] not in fuzz_wide.ORDER_KEEPING or sources[0] in reorders:
                    reorders.add(op["name"])
                if op["op"] != "update_by" or op["source"] not in refreshing:
                    continue
                for operator in op["ops"]:
                    if "ts_col" in operator.get("args", {}):
                        windows += 1
                        assert operator["args"]["ts_col"] == "T"
                        assert op["source"] not in reorders, (doc["name"], op)
    assert windows > 30


def test_a_unique_right_key_stays_unique_through_the_steps():
    """What lets `multi_join` be drawn over refreshing tables: a duplicate key never arrives
    in a step."""
    checked = 0
    for seed in SEEDS:
        gen = fuzz_wide._Wide(__import__("random").Random(f"unique:{seed}"), ("multi_join",))
        gen.make_inputs()
        steps = gen.make_steps()
        for item in gen.inputs:
            if item["name"] not in gen.live_keys or "ticking" not in item:
                continue
            position = [c["name"] for c in item["columns"]].index("K")
            for rows in _replay(item, steps):
                keys = [fuzz_wide._canonical(row[position]) for row in rows]
                assert len(set(keys)) == len(keys), (seed, item["name"], keys)
                checked += 1
    assert checked > 100


def test_array_columns_are_only_used_where_they_are_valid():
    """Arrays are never keys, sort columns or operands, and never aggregated into arrays of
    arrays, which the comparison rules do not know."""
    for runs in GENERATED.values():
        for doc, predicted in runs:
            for op in doc["operations"]:
                kind = op["op"]
                types = {table: dict(columns) for table, columns in predicted.items()}
                if kind in ("group_by", "agg_all_by"):
                    nested = [c for c, t in predicted[op["source"]]
                              if t.startswith("list:") and c not in op["by"]]
                    if kind == "group_by" or op["agg"]["agg"] in ("group", "distinct"):
                        assert not nested, (doc["name"], op)
                for name, column_type in predicted[op["name"]]:
                    assert not column_type.startswith("list:list:"), (doc["name"], op)
                if kind in ("sort", "head_by", "tail_by", "group_by", "agg_by", "update_by",
                            "select_distinct"):
                    for table, column in _named_columns(op):
                        assert not types[table][column].startswith("list:"), (doc["name"], op)


# ---------------------------------------------------------------- what is kept out


def _refreshing(doc: dict) -> set[str]:
    """The tables of a scenario that change in its steps."""
    refreshing = {i["name"] for i in doc["inputs"] if "ticking" in i}
    for op in doc["operations"]:
        if op["op"] == "snapshot_when":
            if op["trigger"] in refreshing:
                refreshing.add(op["name"])
        elif set(sc.references(op)) & refreshing and op["op"] not in ("snapshot", "meta_table"):
            refreshing.add(op["name"])
    return refreshing


def test_nothing_is_drawn_that_can_fail_a_refreshing_table_in_a_later_cycle():
    """A table that fails during an update cycle cannot be compared (README, Known limits),
    so arguments that are valid now and fail on rows of a later step go to static tables."""
    seen: Counter = Counter()
    for runs in GENERATED.values():
        for doc, _ in runs:
            refreshing = _refreshing(doc)
            for op in doc["operations"]:
                kind = op["op"]
                live = bool(set(sc.references(op)) & refreshing)
                if kind == "update_by":
                    for operator in op["ops"]:
                        args = operator.get("args", {})
                        bad = bool(
                            args.get("rev_ticks", 1) + args.get("fwd_ticks", 0) < 1
                            or args.get("decay_ticks", 1) <= 0
                            or args.get("decay_time") in ("PT0S", "PT-1S")
                            or args.get("rev_time") == "PT-3S"
                            or {"THROW", "POISON"} & set(args.get("op_control", {}).values()))
                        seen["update_by", bad, live] += 1
                        assert not (bad and live), (doc["name"], op)
                elif kind in ("agg_by", "agg_all_by"):
                    for agg in op["aggs"] if kind == "agg_by" else [op["agg"]]:
                        bad = not 0 <= agg.get("args", {}).get("percentile", 0.5) <= 1
                        seen["percentile", bad, live] += 1
                        assert not (bad and live), (doc["name"], op)
                elif kind == "exact_join" or op.get("type") in (
                        "exactly_one_match", "error_on_duplicate"):
                    seen["strict join", live] += 1
                    if live and (kind == "exact_join" or op["type"] == "exactly_one_match"):
                        # Every row must match exactly once: only a table with itself, on a
                        # key the generator keeps unique through the steps (the test above).
                        assert op["left"] == op["right"], (doc["name"], op)
                        assert not any("=" in match for match in op["on"]), (doc["name"], op)
                for text in op.get("formulas", []):
                    if " % " in text and live:
                        seen["remainder", live] += 1
                        assert text.split(" % ")[1].strip().isdigit(), (doc["name"], op)
    assert seen["update_by", True, False] > 5 and seen["update_by", False, True] > 100
    assert seen["percentile", False, True] > 20
    assert seen["strict join", True] > 5 and seen["strict join", False] > 5
    assert seen["remainder", True] > 5


def test_requests_with_a_known_defect_of_one_side_are_not_drawn():
    """What the module comment of `fuzz_wide` lists as not generated."""
    seen: Counter = Counter()
    for runs in GENERATED.values():
        for doc, predicted in runs:
            refreshing = _refreshing(doc)
            for op in doc["operations"]:
                kind = op["op"]
                if kind == "agg_all_by":
                    source = predicted[op["source"]]
                    agg = op["agg"]["agg"]
                    rest = [t for c, t in source if c not in op["by"]]
                    if agg in ("first", "last"):
                        seen["first"] += 1
                        assert op["by"] == [c for c, _ in source][:len(op["by"])], (
                            doc["name"], op)
                    elif any(t.startswith("list:") for t in rest):
                        # Arrays only of a table no step changes, and no aggregation that the
                        # reference answers as an array of strings.
                        seen["array", agg] += 1
                        assert op["source"] not in refreshing, (doc["name"], op)
                        if agg in ("min", "max", "median", "pct", "unique"):
                            assert not {"list:bool", "list:timestamp"} & set(rest), (
                                doc["name"], op)
                        if agg == "count_distinct":
                            counted = dict(predicted[op["name"]])
                            assert all(counted[c] == "int64" for c, t in source
                                       if c not in op["by"]), (doc["name"], op)
                    if agg == "distinct":
                        assert "timestamp" not in rest, (doc["name"], op)
                elif kind == "agg_by":
                    types = dict(predicted[op["source"]])
                    for agg in op["aggs"]:
                        if agg["agg"] == "distinct" and types[
                                rules.split_pair(agg["columns"][0])[1]] == "timestamp":
                            seen["distinct"] += 1
                            assert agg["args"]["include_nulls"] is True, (doc["name"], op)
                elif kind in ("head_by", "tail_by"):
                    seen[kind] += 1
                    arrays = {t for c, t in predicted[op["source"]] if c not in op["by"]}
                    assert not arrays & {"list:string", "list:bool", "list:timestamp"}, (
                        doc["name"], op)
    assert seen["first"] > 5 and seen["distinct"] > 3
    assert sum(count for key, count in seen.items() if key[0] == "array") > 5, seen
    assert seen["head_by"] > 20 and seen["tail_by"] > 20
