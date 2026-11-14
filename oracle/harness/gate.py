"""The replay gate: every scenario and every client smoke check against one server, judged
against the checked-in baseline.

No Java counterpart: harness plumbing. It needs no Java server: scenarios are compared with the
recordings under corpus/, and the smoke checks carry their expectations in `smoke.py`.

`run` replays every scenario, runs every smoke area, writes what it saw to a report file and
prints why the outcome does not match `replay-baseline.json` (`baseline.judge`). It never
changes the baseline.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from . import baseline as bl
from . import results as res
from . import rules
from . import scenario as sc
from . import smoke
from .runner import RunError, Target

REPLAY = "replay"
# The one kind of a smoke check that fails. How it fails is in the report and not in the
# baseline, because it is not stable: when the server refuses a stream, the client reports it
# from the call that opened the stream or from the thread that reads it, whichever sees it
# first, and those are different exceptions.
FAILED = "failed"


def smoke_section(area: str) -> str:
    return f"client-smoke {area}"


def kinds_of(mismatch: rules.Mismatch) -> set[str]:
    """The difference kinds of one mismatch, as the baseline lists them.

    They are the comparator's kind, made specific enough that one known cause does not cover
    for another: a failure carries the status code the server answered with, and a metadata
    difference carries each key that differs (`table_metadata:AddOnly`,
    `column_metadata:inputtable.isKey`).
    """
    if mismatch.kind in (rules.FAILURE_UNEXPECTED, rules.FAILURE_CODE):
        code = str(mismatch.actual).removeprefix("fails with ")
        return {f"{mismatch.kind}:{code}"}
    if mismatch.kind in (rules.TABLE_METADATA, rules.COLUMN_METADATA):
        expected, actual = mismatch.expected, mismatch.actual
        if isinstance(expected, dict) and isinstance(actual, dict):
            keys = {key for key in set(expected) | set(actual)
                    if expected.get(key) != actual.get(key)}
            return {f"{mismatch.kind}:{_short(key)}" for key in keys}
    return {mismatch.kind}


def _short(key: str) -> str:
    """`deephaven:attribute.AddOnly` and `deephaven:attribute_type.AddOnly` are one attribute."""
    for prefix in ("deephaven:attribute_type.", "deephaven:attribute.", "deephaven:"):
        if key.startswith(prefix):
            return key[len(prefix):]
    return key


def replay(target: Target, scenarios: list[sc.Scenario], corpus: Path,
           report: dict[str, Any]) -> dict[str, bl.Entry]:
    observed: dict[str, bl.Entry] = {}
    for scenario in scenarios:
        started = time.monotonic()
        entry: dict[str, Any] = {}
        try:
            expected = res.load(corpus / scenario.name, scenario.name, scenario.digest())
            actual = target.run(scenario)
        except Exception as error:  # noqa: BLE001 - whatever it is, nothing was compared
            # RunError and RecordingError are the expected ones. A server that died can also
            # surface as a client exception from closing the session.
            observed[scenario.name] = bl.Entry(bl.NOT_RUN)
            text = str(error) if isinstance(error, (RunError, res.RecordingError)) \
                else f"{type(error).__name__}: {error}"
            entry = {"state": bl.NOT_RUN, "error": text}
            print(f"ERROR {scenario.name}: {text}")
        else:
            found = rules.compare_scenario(scenario, expected, actual)
            if found:
                kinds = tuple(sorted(set().union(*(kinds_of(m) for m in found))))
                tables = tuple(sorted({m.table for m in found}))
                observed[scenario.name] = bl.Entry(bl.DIFFERS, kinds, tables)
                entry = {"state": bl.DIFFERS, "kinds": list(kinds), "tables": list(tables),
                         "mismatches": [str(m) for m in found]}
                print(f"DIFF  {scenario.name}")
                for mismatch in found:
                    print(f"      {mismatch}")
            else:
                observed[scenario.name] = bl.Entry(bl.CLEAN)
                entry = {"state": bl.CLEAN}
                print(f"ok    {scenario.name}")
        entry["seconds"] = round(time.monotonic() - started, 2)
        report[scenario.name] = entry
    return observed


def smoke_area(area: str, url: str, report: dict[str, Any]) -> dict[str, bl.Entry]:
    observed: dict[str, bl.Entry] = {}
    try:
        outcomes = list(smoke.run_checks(area, url))
    except Exception as error:  # noqa: BLE001 - the area could not be set up
        text = f"{type(error).__name__}: {error}"
        print(f"ERROR client-smoke {area}: {text}")
        report["(setup)"] = {"state": bl.NOT_RUN, "error": text}
        return {"(setup)": bl.Entry(bl.NOT_RUN)}
    for name, error in outcomes:
        if error is None:
            observed[name] = bl.Entry(bl.CLEAN)
            report[name] = {"state": bl.CLEAN}
            print(f"ok    {area}: {name}")
            continue
        observed[name] = bl.Entry(bl.DIFFERS, (FAILED,))
        report[name] = {"state": bl.DIFFERS, "kinds": [FAILED],
                        "failure": smoke.describe(error)}
        print(f"FAIL  {area}: {name}: {smoke.describe(error)}")
    return observed


def run(url: str, scenarios_dir: Path, corpus: Path, baseline_path: Path,
        report_path: Path | None, areas: tuple[str, ...] = smoke.AREAS) -> int:
    """0 when the outcome is the baseline's, 1 when it is not, 2 when something did not run."""
    baseline = bl.load(baseline_path)  # before anything runs: a broken baseline is an error
    scenarios = sc.load_all(scenarios_dir)
    if not scenarios:
        raise sc.ScenarioError(f"no scenario under {scenarios_dir}")
    started = time.monotonic()
    report: dict[str, Any] = {"url": url, "sections": {REPLAY: {}}}
    observed: dict[str, dict[str, bl.Entry]] = {}
    observed[REPLAY] = replay(Target(url), scenarios, corpus, report["sections"][REPLAY])
    for area in areas:
        section = smoke_section(area)
        report["sections"][section] = {}
        observed[section] = smoke_area(area, url, report["sections"][section])
        if not observed[section]:
            raise sc.ScenarioError(f"client-smoke {area} has no checks")

    problems = bl.judge(baseline, observed)
    not_run = sum(e.state == bl.NOT_RUN for entries in observed.values()
                  for e in entries.values())
    report["seconds"] = round(time.monotonic() - started, 2)
    report["problems"] = problems
    report["observed_as_baseline"] = bl.dump(_with_reasons(observed, baseline))
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=1)
            handle.write("\n")

    print()
    for section, entries in observed.items():
        states = [e.state for e in entries.values()]
        print(f"{section}: {len(states)} run, {states.count(bl.CLEAN)} clean, "
              f"{states.count(bl.DIFFERS)} differ, {states.count(bl.NOT_RUN)} not run")
    if not problems:
        print(f"replay gate: the outcome is the baseline's ({baseline_path.name})")
        return 0
    print(f"replay gate: FAILED, {len(problems)} entries do not match {baseline_path.name}:")
    for problem in problems:
        print(f"  {problem}")
    return 2 if not_run else 1


def _with_reasons(observed: bl.Sections, baseline: bl.Sections) -> dict[str, dict[str, bl.Entry]]:
    """The observed outcomes with the baseline's `why` kept for entries that still differ."""
    out: dict[str, dict[str, bl.Entry]] = {}
    for section, entries in observed.items():
        out[section] = {}
        for name, entry in entries.items():
            known = baseline.get(section, {}).get(name)
            why = known.why if known is not None and entry.state == bl.DIFFERS else None
            out[section][name] = bl.Entry(entry.state, entry.kinds, entry.tables, why)
    return out
