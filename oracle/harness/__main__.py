"""Command line: `python -m harness <command>`, run from the `oracle/` directory.

No Java counterpart: harness entry point.

    diff    run every scenario on two servers (or twice on one) and compare the results
    record  run every scenario on the Java reference and store the results under corpus/
    replay  run every scenario on one server and compare with the recordings; needs no Java
    show    run one scenario and print its results

Exit status: 0 when no difference was found, 1 when there are differences, 2 when a scenario
could not be run or a recording could not be used.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import results as res
from . import rules
from . import scenario as sc
from .runner import RunError, run_scenario

ORACLE = Path(__file__).resolve().parent.parent
SCENARIOS = ORACLE / "scenarios"
CORPUS = ORACLE / "corpus"
REFERENCE = "42.9"  # oracle/REFERENCE.md


def _unordered(scenario: sc.Scenario) -> frozenset[str]:
    return frozenset(r.table for r in scenario.results if not r.ordered)


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


def _load(args: argparse.Namespace) -> list[sc.Scenario]:
    scenarios = sc.load_all(Path(args.scenarios), args.only or None)
    if not scenarios:
        raise sc.ScenarioError(f"no scenarios under {args.scenarios}")
    return scenarios


def cmd_diff(args: argparse.Namespace) -> int:
    url_b = args.b or args.a
    tally = _Tally(f"diff {args.a} vs {url_b}")
    for scenario in _load(args):
        try:
            expected = run_scenario(scenario, args.a)
            actual = run_scenario(scenario, url_b)
        except RunError as error:
            tally.error(scenario, error)
            continue
        tally.report(scenario, rules.compare(expected, actual, _unordered(scenario)))
    return tally.finish()


def cmd_record(args: argparse.Namespace) -> int:
    corpus = Path(args.corpus)
    failed = 0
    scenarios = _load(args)
    for scenario in scenarios:
        try:
            recorded = run_scenario(scenario, args.url)
        except RunError as error:
            failed += 1
            print(f"ERROR {scenario.name}: {error}")
            continue
        res.save(recorded, corpus / scenario.name, scenario.digest(), REFERENCE)
        print(f"saved {scenario.name}: {len(recorded.keys())} tables")
    print(f"record {args.url}: {len(scenarios)} scenarios, {len(scenarios) - failed} saved, "
          f"{failed} not run")
    return 2 if failed else 0


def cmd_replay(args: argparse.Namespace) -> int:
    corpus = Path(args.corpus)
    tally = _Tally(f"replay {args.url} vs recordings")
    for scenario in _load(args):
        try:
            expected = res.load(corpus / scenario.name, scenario.name, scenario.digest())
            actual = run_scenario(scenario, args.url)
        except (RunError, res.RecordingError) as error:
            tally.error(scenario, error)
            continue
        tally.report(scenario, rules.compare(expected, actual, _unordered(scenario)))
    return tally.finish()


def cmd_show(args: argparse.Namespace) -> int:
    for scenario in _load(args):
        results = run_scenario(scenario, args.url)
        for step, table in results.keys():
            data = results.get(step, table)
            print(f"--- {scenario.name} step {step} table {table} ({data.num_rows} rows)")
            for field in data.schema:
                print(f"{field.name}: {field.type} = {data.column(field.name).to_pylist()}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m harness", description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--scenarios", default=str(SCENARIOS), help="scenario directory")
        p.add_argument("only", nargs="*", help="scenario names; default is all of them")

    p = sub.add_parser("diff", help="compare two servers, or two runs on one server")
    p.add_argument("--a", required=True, help="URL of the expected side")
    p.add_argument("--b", help="URL of the actual side; default is --a again")
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

    args = parser.parse_args(argv)
    try:
        return args.run(args)
    except sc.ScenarioError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
