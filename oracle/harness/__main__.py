"""Command line: `python -m harness <command>`, run from the `oracle/` directory.

No Java counterpart: harness entry point.

    diff          run scenarios on two servers (or twice on one) and compare the results
    record        run scenarios on the Java reference and store the results under corpus/
    replay        run scenarios on one server and compare with the recordings; needs no Java
    fuzz          generate scenarios from seeds, compare two servers, shrink the first difference
    client-smoke  exercise one area of the client protocol against one server
    show          run scenarios and print their results

Exit status: 0 when no difference was found, 1 when there are differences, 2 when a scenario
could not be run, a recording could not be used, or the arguments select nothing.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from . import fuzz as fz
from . import results as res
from . import rules
from . import scenario as sc
from .runner import RunError, open_target

ORACLE = Path(__file__).resolve().parent.parent
SCENARIOS = ORACLE / "scenarios"
CORPUS = ORACLE / "corpus"
FUZZ_OUT = ORACLE / ".fuzz"
REFERENCE = "42.9"  # oracle/REFERENCE.md


class _Tally:
    def __init__(self, label: str):
        self.label = label
        self.clean = 0
        self.different = 0
        self.failed = 0
        self.mismatches = 0

    def report(self, scenario: sc.Scenario, found: list[rules.Mismatch]) -> None:
        if found:
            self.different += 1
            self.mismatches += len(found)
            print(f"DIFF  {scenario.name}")
            for mismatch in found:
                print(f"      {mismatch}")
        else:
            self.clean += 1
            print(f"ok    {scenario.name}")

    def error(self, scenario: sc.Scenario, error: Exception) -> None:
        self.failed += 1
        print(f"ERROR {scenario.name}: {error}")

    def finish(self) -> int:
        total = self.clean + self.different + self.failed
        print(
            f"{self.label}: {total} scenarios, {self.clean} clean, {self.different} with "
            f"differences ({self.mismatches} mismatches), {self.failed} not run"
        )
        if self.failed:
            return 2
        return 1 if self.different else 0


def _ops(text: str | None) -> frozenset[str] | None:
    if text is None:
        return None
    return sc.resolve_ops([name.strip() for name in text.split(",") if name.strip()])


def _load(args: argparse.Namespace) -> list[sc.Scenario]:
    scenarios = sc.load_all(Path(args.scenarios), args.only or None)
    ops, within = _ops(args.ops), _ops(args.within)
    scenarios = sc.select(scenarios, ops, within)
    if not scenarios:
        # Selecting nothing must never look like a pass.
        raise sc.ScenarioError(
            f"no scenario under {args.scenarios} matches (--ops {args.ops}, --within "
            f"{args.within}, names {args.only or 'all'})"
        )
    return scenarios


def cmd_diff(args: argparse.Namespace) -> int:
    target_a = open_target(args.a)
    target_b = open_target(args.b or args.a)
    tally = _Tally(f"diff {target_a} vs {target_b}" + (" (exact)" if args.exact else ""))
    for scenario in _load(args):
        try:
            expected = target_a.run(scenario)
            actual = target_b.run(scenario)
        except RunError as error:
            tally.error(scenario, error)
            continue
        tally.report(scenario, rules.compare_scenario(scenario, expected, actual, args.exact))
    return tally.finish()


def cmd_record(args: argparse.Namespace) -> int:
    corpus = Path(args.corpus)
    target = open_target(args.url)
    failed = 0
    scenarios = _load(args)
    for scenario in scenarios:
        try:
            recorded = target.run(scenario)
        except RunError as error:
            failed += 1
            print(f"ERROR {scenario.name}: {error}")
            continue
        res.save(recorded, corpus / scenario.name, scenario.digest(), REFERENCE)
        print(f"saved {scenario.name}: {len(recorded.keys())} results, "
              f"{recorded.failures()} of them failures")
    print(f"record {args.url}: {len(scenarios)} scenarios, {len(scenarios) - failed} saved, "
          f"{failed} not run")
    return 2 if failed else 0


def cmd_replay(args: argparse.Namespace) -> int:
    corpus = Path(args.corpus)
    target = open_target(args.url)
    tally = _Tally(f"replay {target} vs recordings")
    for scenario in _load(args):
        try:
            expected = res.load(corpus / scenario.name, scenario.name, scenario.digest())
            actual = target.run(scenario)
        except (RunError, res.RecordingError) as error:
            tally.error(scenario, error)
            continue
        tally.report(scenario, rules.compare_scenario(scenario, expected, actual))
    return tally.finish()


def cmd_show(args: argparse.Namespace) -> int:
    target = open_target(args.url)
    for scenario in _load(args):
        results = target.run(scenario)
        for step, table in results.keys():
            data = results.get(step, table)
            if isinstance(data, res.Failure):
                print(f"--- {scenario.name} step {step} table {table}: fails with {data.code}")
                continue
            print(f"--- {scenario.name} step {step} table {table} ({data.num_rows} rows)")
            for field in data.schema:
                print(f"{field.name}: {field.type} = {data.column(field.name).to_pylist()}")
    return 0


def _write_scenario(directory: Path, doc: dict, name: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    doc = dict(doc, name=name)
    path = directory / f"{name}.json"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(doc, handle, indent=1)
        handle.write("\n")
    return path


_ARROW_NAMES = {"bool": "bool", "float32": "float", "float64": "double",
                "timestamp": "timestamp[ns, tz=UTC]"}


def _prediction_misses(predicted: dict[str, list], results: res.ResultSet) -> list[str]:
    """Result tables whose columns the generator predicted wrongly (a generator defect)."""
    out = []
    for step, table in results.keys():
        data = results.get(step, table)
        if step != 0 or isinstance(data, res.Failure):
            continue
        want = [(name, _ARROW_NAMES.get(kind, kind)) for name, kind in predicted[table]]
        have = [(f.name, str(f.type)) for f in data.schema]
        if want != have:
            out.append(f"{results.scenario}.{table}: predicted {want}, got {have}")
    return out


def cmd_fuzz(args: argparse.Namespace) -> int:
    ops = _ops(args.ops) - {sc.INPUT_TABLE}
    target_a = open_target(args.a)
    target_b = open_target(args.b or args.a)
    out_dir = Path(args.out)
    used: Counter = Counter()
    clean = not_run = results_total = failures_total = 0
    misses: list[str] = []
    print(f"fuzz {target_a} vs {target_b}: seeds {args.seed}..{args.seed + args.seeds - 1} "
          f"over {', '.join(sorted(ops))}")
    for seed in range(args.seed, args.seed + args.seeds):
        doc, predicted = fz.generate(seed, ops)
        scenario = sc.parse(doc)
        try:
            expected = target_a.run(scenario)
            actual = target_b.run(scenario)
        except RunError as error:
            not_run += 1
            print(f"ERROR seed {seed}: {error}")
            continue
        found = rules.compare_scenario(scenario, expected, actual, args.exact)
        used.update(op["op"] for op in scenario.operations)
        used[sc.INPUT_TABLE] += scenario.ticking
        results_total += len(expected.keys())
        failures_total += expected.failures()
        misses.extend(_prediction_misses(predicted, expected))
        if not found:
            clean += 1
            continue

        print(f"DIFF  seed {seed}: {len(found)} mismatches; the first:")
        print(f"      {found[0]}")
        original = _write_scenario(out_dir, doc, f"fuzz_{seed}")

        def still_differs(candidate: dict) -> bool:
            try:
                parsed = sc.parse(candidate)
                return bool(rules.compare_scenario(
                    parsed, target_a.run(parsed), target_b.run(parsed), args.exact))
            except RunError:
                return False

        small, runs = fz.shrink(doc, still_differs, args.max_shrink_runs)
        minimal = _write_scenario(out_dir, small, f"fuzz_{seed}_min")
        parsed = sc.parse(dict(small, name=f"fuzz_{seed}_min"))
        print(f"shrunk in {runs} runs to {len(parsed.inputs)} inputs, "
              f"{sum(len(i.rows) for i in parsed.inputs)} rows, "
              f"{sum(len(i.columns) for i in parsed.inputs)} columns, "
              f"{len(parsed.operations)} operations, {len(parsed.steps)} steps:")
        for mismatch in rules.compare_scenario(
                parsed, target_a.run(parsed), target_b.run(parsed), args.exact):
            print(f"      {mismatch}")
        print(f"original scenario: {original}")
        print(f"minimal scenario:  {minimal}")
        print(f"rerun: python -m harness diff --a {target_a} --b {target_b} "
              f"--scenarios {minimal}")
        print(f"fuzz: seed {seed} differs after {clean} clean seeds")
        return 1

    print(f"operations used: {', '.join(f'{k} {v}' for k, v in sorted(used.items()))}")
    print(f"results compared: {results_total}, of which failures on the reference side: "
          f"{failures_total}")
    if misses:
        print(f"generator predicted the columns of {len(misses)} tables wrongly, e.g. "
              f"{misses[0]}")
    print(f"fuzz: {args.seeds} seeds, {clean} clean, 0 with differences, {not_run} not run")
    return 2 if not_run else 0


def cmd_client_smoke(args: argparse.Namespace) -> int:
    from . import smoke

    return smoke.run(args.area, args.url)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m harness", description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--scenarios", default=str(SCENARIOS),
                       help="scenario directory, or one scenario file")
        p.add_argument("--ops", help="comma-separated operations: only scenarios that use at "
                                     "least one of them")
        p.add_argument("--within", help="comma-separated operations: only scenarios that use "
                                        "nothing else")
        p.add_argument("only", nargs="*", help="scenario names; default is all of them")

    p = sub.add_parser("diff", help="compare two servers, or two runs on one server")
    p.add_argument("--a", required=True, help="URL of the expected side (the reference)")
    p.add_argument("--b", help="URL of the actual side; default is --a again")
    p.add_argument("--exact", action="store_true",
                   help="no float tolerance: for two runs of the same server")
    common(p)
    p.set_defaults(run=cmd_diff)

    p = sub.add_parser("record", help="store the Java reference results under corpus/")
    p.add_argument("--url", required=True, help="URL of the Java reference server")
    p.add_argument("--corpus", default=str(CORPUS))
    common(p)
    p.set_defaults(run=cmd_record)

    p = sub.add_parser("replay", help="compare one server with the recordings")
    p.add_argument("--url", required=True, help="URL of the server under test")
    p.add_argument("--corpus", default=str(CORPUS))
    common(p)
    p.set_defaults(run=cmd_replay)

    p = sub.add_parser("show", help="run scenarios and print their results")
    p.add_argument("--url", required=True)
    common(p)
    p.set_defaults(run=cmd_show)

    p = sub.add_parser("fuzz", help="compare two servers on generated scenarios")
    p.add_argument("--a", required=True, help="URL of the expected side (the reference)")
    p.add_argument("--b", help="URL of the actual side; default is --a again")
    p.add_argument("--ops", required=True, help="comma-separated operations to draw from")
    p.add_argument("--seeds", type=int, default=100, help="how many seeds to run")
    p.add_argument("--seed", type=int, default=0, help="the first seed")
    p.add_argument("--exact", action="store_true",
                   help="no float tolerance: for two runs of the same server")
    p.add_argument("--out", default=str(FUZZ_OUT), help="where a differing scenario is written")
    p.add_argument("--max-shrink-runs", type=int, default=200)
    p.set_defaults(run=cmd_fuzz)

    p = sub.add_parser("client-smoke", help="exercise one area of the client protocol")
    p.add_argument("area", choices=["session", "tables", "flight", "barrage", "inputtable"])
    p.add_argument("--url", required=True)
    p.set_defaults(run=cmd_client_smoke)

    args = parser.parse_args(argv)
    try:
        return args.run(args)
    except (sc.ScenarioError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
