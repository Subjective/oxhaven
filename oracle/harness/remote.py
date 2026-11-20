"""The remote-table corpus: script scenarios whose scripts read the tables of a second server.

No Java counterpart: this is the harness. A remote scenario is a script scenario
(`scripts.py`) under `scenarios/remote/`; its recording is under `corpus/remote/`. Its scripts
run in the console of the server under test, the *local* server, and reach a second server,
the *remote*, with `deephaven.uri.resolve("dh+plain://...")` and `deephaven.barrage`. What
the scripts make of the remote's tables is compared, as every script scenario's results are.

The remote is an Oxhaven server in replay and in the recordings: CI can start it without
Java, and the recordings are what the Deephaven 42.9 console makes of that same remote (Java's
client reading Oxhaven's server is the expectation of Oxhaven's client reading Oxhaven's
server). It starts from fixed content, `corpus/remote/seed/`:

- `*.app` and the scripts they name: the applications, started with the server
  (`--app-dir` of Oxhaven, `deephaven.application.dir` of Deephaven);
- `scope.py`: run in the remote's console once it is up, for the variables of its scope.

`corpus/remote/recorded-from.json` holds the digest of that directory; the gate refuses
recordings made from other seed content.

A scenario changes the remote between its steps with lines the script scenario format leaves
alone (they are comments to it, and part of the content its digest covers):

    # remote: <python>          a line of a script that runs on the remote before the step's
                                own script; consecutive lines make one script, indentation
                                kept after `# remote: `
    # remote-sync: a = r, b = s after the step's script, wait until the local variable `a`
                                holds the rows of the remote variable `r` (names, order,
                                values), and `b` those of `s`

After a step's remote script the runner waits for the remote's update cycle (`_Barrier`), so
the remote is at rest when the local script and the waits run. A subscription's rows arrive
on the client's own schedule, so a `remote-sync` wait is what makes a step's results those
of the remote's change: it asks the local server for the variable again until it equals the
remote's table, or until `ORACLE_AWAIT_SECONDS` (60). A wait that is not met is noted as
`await` notes it: such a reference run is not recorded, and another server's is compared as
it is. A remote step must change what it waits for, or the wait proves nothing.

The scripts find the remote through `ORACLE_REMOTE_TARGET` (`host:port`) in the environment
of the local server, which `bin/remote-services.sh` exports. Every remote variable a
scenario makes is removed afterwards, as the local ones are; the seed's stay.

`bin/remote-replay.sh` and `bin/remote-record.sh` are the commands.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from . import scripts
from .connect import open_session
from .results import ResultSet
from .runner import RunError, _Barrier, _describe, _snapshot

SECTION = "replay-remote"
SEED = "seed"
SCOPE_SCRIPT = "scope.py"
RECORDED_FROM = "recorded-from.json"
REMOTE_LINE = "# remote:"
SYNC_LINE = "# remote-sync:"
_STEP = re.compile(r"^# oracle:\s*step\s*$")
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class RemoteStep:
    """What a step does on the remote before its own script, and what it waits for after."""

    script: str
    syncs: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class RemoteScenario:
    """A script scenario and, step by step, its remote part."""

    local: scripts.ScriptScenario
    steps: tuple[RemoteStep, ...]

    @property
    def name(self) -> str:
        return self.local.name


def parse(name: str, text: str) -> RemoteScenario:
    """The scenario in `text`; raises `ScenarioError` for anything it does not understand."""
    from .scenario import ScenarioError

    local = scripts.parse(name, text)
    steps: list[RemoteStep] = []
    code: list[str] = []
    syncs: list[tuple[str, str]] = []

    def close_step() -> None:
        steps.append(RemoteStep("\n".join(code) + "\n" if code else "", tuple(syncs)))

    for number, line in enumerate(text.splitlines(), start=1):
        if _STEP.match(line):
            close_step()
            code, syncs = [], []
        elif line.startswith(SYNC_LINE):
            for item in line[len(SYNC_LINE):].split(","):
                left, equals, right = (part.strip() for part in item.partition("="))
                if not equals or not _NAME.match(left) or not _NAME.match(right):
                    raise ScenarioError(f"remote scenario {name}: line {number}: a wait is "
                                        "written <local variable> = <remote variable>")
                if any(other == left for other, _ in syncs):
                    raise ScenarioError(f"remote scenario {name}: line {number}: {left!r} "
                                        "is waited for twice in one step")
                syncs.append((left, right))
        elif line.startswith(REMOTE_LINE):
            rest = line[len(REMOTE_LINE):]
            code.append(rest[1:] if rest.startswith(" ") else rest)
        elif line.startswith("# remote"):
            raise ScenarioError(f"remote scenario {name}: line {number}: a remote line is "
                                f"`{REMOTE_LINE} <python>` or `{SYNC_LINE} <a> = <r>`")
    close_step()
    if len(steps) != len(local.sections):
        raise ScenarioError(f"remote scenario {name}: {len(steps)} remote steps for "
                            f"{len(local.sections)} steps")
    return RemoteScenario(local, tuple(steps))


def load_all(directory: Path, select: list[str] | None = None) -> list[RemoteScenario]:
    """Every remote scenario under `directory` (or the one file), sorted by name; `select`
    narrows the set as `scripts.load_all` does."""
    chosen = {s.name for s in scripts.load_all(directory, select)}
    paths = [directory] if directory.is_file() else sorted(directory.glob("*.py"))
    return [parse(path.stem, path.read_text(encoding="utf-8")) for path in paths
            if path.stem in chosen]


# ---------------------------------------------------------------- the seed


def seed_digest(seed: Path) -> str:
    """Identifies the seed content a recording was made from: every file of the directory,
    by name and content."""
    digest = hashlib.sha256()
    for path in sorted(p for p in seed.iterdir() if p.is_file()):
        digest.update(path.name.encode("utf-8") + b"\0")
        digest.update(path.read_bytes() + b"\0")
    return digest.hexdigest()


def recorded_from(corpus: Path) -> str | None:
    try:
        return json.loads((corpus / RECORDED_FROM).read_text(encoding="utf-8"))["seed_sha256"]
    except (OSError, ValueError, KeyError):
        return None


def seed_remote(remote_url: str, seed: Path) -> None:
    """Runs the seed's scope script in the remote's console."""
    session = open_session(remote_url)
    try:
        failed = scripts._execute(session, (seed / SCOPE_SCRIPT).read_text(encoding="utf-8"))
        if failed is not None:
            raise RunError(f"the remote's seed script failed: {failed.type}: {failed.text}")
        _Barrier(session).wait()
    finally:
        session.close()


# ---------------------------------------------------------------- running


def _cells(table: pa.Table) -> tuple[list[str], list[list[Any]]]:
    """Column names and cells; every NaN is the same cell. A column whose cells pyarrow
    cannot turn into Python values (a month interval, which is how a Period is sent) is
    its text."""

    def cell(value: Any) -> Any:
        if isinstance(value, float) and math.isnan(value):
            return "NaN"
        if isinstance(value, list):
            return [cell(v) for v in value]
        return value

    def cells(column: pa.ChunkedArray) -> list[Any]:
        try:
            return [cell(v) for v in column.to_pylist()]
        except Exception:  # noqa: BLE001 - a type pyarrow has no Python value for
            return [str(column.type), column.to_string(window=10 ** 9)]

    return table.column_names, [cells(column) for column in table.columns]


def _sync(local: Any, remote: Any, wait: tuple[str, str], deadline: float) -> str | None:
    """Waits until the local variable of `wait` holds the rows of the remote one; returns
    what was missing at `deadline`, `None` when the wait was met. A local variable that
    cannot be read, or has failed, ends the wait: its snapshot reports it."""
    from .runner import failed_in_cycle

    name, remote_name = wait
    remote_table = remote.open_table(remote_name).to_arrow()
    want = _cells(remote_table)
    while True:
        table = scripts._open(local, name)
        if isinstance(table, scripts.Failure) or failed_in_cycle(table) is not None:
            return None
        try:
            copy = table.to_arrow()
        except Exception:  # noqa: BLE001 - a table that cannot be read is no copy yet
            copy = None
        if copy is not None and _cells(copy) == want:
            return None
        if time.monotonic() >= deadline:
            rows = "?" if copy is None else copy.num_rows
            return (f"{name} does not hold the rows of the remote's {remote_name} "
                    f"({rows} rows, the remote has {remote_table.num_rows})")
        time.sleep(0.1)


def run_scenario(scenario: RemoteScenario, url: str, remote_url: str) -> scripts.Run:
    """Runs `scenario` on the local server at `url`, changing the remote at `remote_url`
    between its steps: step k is the state after the k-th remote and local scripts."""
    local = scenario.local
    out = scripts.Run([], ResultSet(local.name))
    try:
        session = open_session(url)
        remote = open_session(remote_url)
    except Exception as error:  # noqa: BLE001
        raise RunError(f"cannot connect to {url} and {remote_url}: {_describe(error)}") \
            from error
    try:
        barrier, remote_barrier = _Barrier(session), _Barrier(remote)
        for target in (session, remote):
            failed = scripts._execute(target, scripts._REMEMBER)
            if failed is not None:
                raise RunError(f"a session did not run the runner's own script: {failed}")
        try:
            for step, (section, extra) in enumerate(zip(local.sections, scenario.steps)):
                if scripts._has_code(extra.script):
                    failed = scripts._execute(remote, extra.script)
                    if failed is not None:
                        raise RunError(f"the remote script of step {step} failed: "
                                       f"{failed.type}: {failed.text}")
                    remote_barrier.wait()
                ran = scripts._has_code(section.script)
                out.outcomes.append(scripts._execute(session, section.script) if ran else None)
                deadline = time.monotonic() + scripts._await_seconds()
                for wait in section.awaits:
                    missing = scripts._await(session, wait, deadline)
                    if missing is not None:
                        out.unmet.append(f"step {step}: {missing}")
                for wait in extra.syncs:
                    missing = _sync(session, remote, wait, deadline)
                    if missing is not None:
                        out.unmet.append(f"step {step}: {missing}")
                barrier.wait()
                tables = {result.table: scripts._open(session, result.table)
                          for result in local.results}
                _snapshot(session, local, tables, step, out.tables)
        finally:
            failed = scripts._execute(session, scripts._FORGET)
            remote_failed = scripts._execute(remote, scripts._FORGET)
        if failed is not None or remote_failed is not None:
            raise RunError(f"the scenario's variables could not be removed: "
                           f"{failed or remote_failed}")
    except Exception as error:
        raise RunError(f"remote scenario {local.name} failed on {url} with the remote "
                       f"{remote_url}: {_describe(error)}") from error
    finally:
        session.close()
        remote.close()
    return out


class Target(scripts.Target):
    """A local server that remote scenarios run against, and its remote."""

    def __init__(self, url: str, remote_url: str, scenarios: dict[str, RemoteScenario]):
        super().__init__(url)
        self.remote_url = remote_url
        self._scenarios = scenarios

    def run(self, scenario: scripts.ScriptScenario) -> scripts.Run:
        return run_scenario(self._scenarios[scenario.name], self.url, self.remote_url)


# ---------------------------------------------------------------- record, gate, compare


def record(url: str, remote_url: str, scenarios: list[RemoteScenario], corpus: Path,
           reference: str) -> int:
    """Stores what the server at `url` (the Java reference) makes of each scenario."""
    target = Target(url, remote_url, {s.name: s for s in scenarios})
    failed = 0
    for scenario in scenarios:
        try:
            run = target.run(scenario.local)
            problems = scripts.undeclared(scenario.local, run)
            if problems:
                raise RunError("; ".join(problems))
        except RunError as error:
            failed += 1
            print(f"ERROR {scenario.name}: {error}")
            continue
        scripts.save(run, corpus / scenario.name, scenario.local, reference)
        errors = sum(outcome is not None for outcome in run.outcomes)
        print(f"saved {scenario.name}: {len(run.outcomes)} scripts, {errors} of them fail; "
              f"{len(run.tables.keys())} results, {run.tables.failures()} of them failures")
    print(f"remote-record {url} (remote {remote_url}): {len(scenarios)} scenarios, "
          f"{len(scenarios) - failed} saved, {failed} not run")
    return 2 if failed else 0


def gate(url: str, remote_url: str, scenarios: list[RemoteScenario], corpus: Path,
         baseline_path: Path, report_path: Path | None, selected: bool) -> int:
    """Replays the remote scenarios and judges the outcome against the baseline: 0 when it
    is the baseline's, 1 when it is not, 2 when a scenario could not be run. With
    `selected`, only the lines of the scenarios that ran are judged."""
    from . import baseline as bl
    from . import gate as replay_gate

    baseline = bl.load(baseline_path)
    started = time.monotonic()
    report: dict[str, Any] = {"url": url, "remote": remote_url, "sections": {SECTION: {}}}
    target = Target(url, remote_url, {s.name: s for s in scenarios})
    observed = {SECTION: replay_gate.replay_scripts(
        target, [s.local for s in scenarios], corpus, report["sections"][SECTION])}
    listed = {SECTION: baseline.get(SECTION, {})}
    if selected:
        chosen = {scenario.name for scenario in scenarios}
        listed = {SECTION: {n: e for n, e in listed[SECTION].items() if n in chosen}}
    problems = bl.judge(listed, observed)
    states = [entry.state for entry in observed[SECTION].values()]
    report["seconds"] = round(time.monotonic() - started, 2)
    report["problems"] = problems
    report["observed_as_baseline"] = bl.dump(replay_gate._with_reasons(observed, baseline))
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=1)
            handle.write("\n")
    print()
    print(f"{SECTION}: {len(states)} run, {states.count(bl.CLEAN)} clean, "
          f"{states.count(bl.DIFFERS)} differ, {states.count(bl.NOT_RUN)} not run, "
          f"{report['seconds']} s")
    if not problems:
        print(f"remote gate: the outcome is the baseline's ({baseline_path.name})")
        return 0
    print(f"remote gate: FAILED, {len(problems)} entries do not match {baseline_path.name}:")
    for problem in problems:
        print(f"  {problem}")
    return 2 if bl.NOT_RUN in states else 1


def compare_recordings(scenarios: list[RemoteScenario], expected: Path, actual: Path) -> int:
    """Compares two recordings of the same scenarios exactly: those made against a Deephaven
    remote with those made against an Oxhaven remote, by the same console. 0 when every
    scenario is the same, 1 when one differs, 2 when one has no recording."""
    different = failed = 0
    for scenario in scenarios:
        try:
            a = scripts.load(expected / scenario.name, scenario.local)
            b = scripts.load(actual / scenario.name, scenario.local)
        except Exception as error:  # noqa: BLE001 - nothing was compared
            failed += 1
            print(f"ERROR {scenario.name}: {error}")
            continue
        found = scripts.compare(scenario.local, a, b, exact=True)
        if found:
            different += 1
            print(f"DIFF  {scenario.name}")
            for mismatch in found:
                print(f"      {mismatch}")
        else:
            print(f"same  {scenario.name}")
    print(f"remote-compare {expected} vs {actual}: {len(scenarios)} scenarios, "
          f"{len(scenarios) - different - failed} the same, {different} differ, "
          f"{failed} not compared")
    return 2 if failed else 1 if different else 0
