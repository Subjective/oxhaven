"""Command line: `python -m harness <command>`, run from the `oracle/` directory.

No Java counterpart: harness entry point.

    diff          run scenarios on two servers (or twice on one) and compare the results
    record        run scenarios on the Java reference and store the results under corpus/
    replay        run scenarios on one server and compare with the recordings; needs no Java
    gate          replay every scenario and run every client smoke check against one server,
                  and compare the outcome with replay-baseline.json; needs no Java
    fuzz          generate scenarios from seeds, compare two servers, shrink the first difference
    selfcheck     run ticking scenarios on one server and compare what it kept up to date
                  through the steps with what it answers for the same operations from scratch
    client-smoke  exercise one area of the client protocol against one server
    show          run scenarios and print their results
    record-scripts  run the script scenarios on the Java reference and store what they leave
    replay-scripts  run the script scenarios on one server and compare with the recordings
    show-scripts    run script scenarios and print what they leave
    kafka-topics    write or check the topics file of the Kafka corpus
    kafka-publish   create the topics of the Kafka corpus on a broker and send their records
    kafka-registry  serve the schema registry of the Kafka corpus until interrupted
    kafka-record    store what the Java reference answers to the Kafka scenarios
    kafka-gate      replay the Kafka scenarios against the checked-in Kafka baseline
    remote-seed     run the seed's scope script in the console of a remote of the remote-table corpus
    remote-record   store what the Java reference's console makes of the remote scenarios
    remote-gate     replay the remote scenarios against the checked-in remote baseline
    remote-compare  compare two recordings of the remote scenarios exactly
    formulas      record the formula corpus from Java, or compare one server with it

With `ORACLE_STACKS_AFTER=<seconds>` in the environment, a command that is still running after
that time writes the stack of every thread to stderr, once, and carries on.

Exit status: 0 when no difference was found, 1 when there are differences, 2 when a scenario
could not be run, a recording could not be used, or the arguments select nothing.
"""

from __future__ import annotations

import argparse
import faulthandler
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from . import fuzz as fz
from . import known as kn
from . import results as res
from . import rules
from . import scenario as sc
from .runner import RunError, open_target

ORACLE = Path(__file__).resolve().parent.parent
SCENARIOS = ORACLE / "scenarios"
CORPUS = ORACLE / "corpus"
SCRIPT_SCENARIOS = SCENARIOS / "python"
SCRIPT_CORPUS = CORPUS / "python"
BASELINE = ORACLE / "replay-baseline.json"
KAFKA_SCENARIOS = SCENARIOS / "kafka"
KAFKA_CORPUS = CORPUS / "kafka"
KAFKA_BASELINE = ORACLE / "kafka-baseline.json"
# Written by `kafka-record` when it records every scenario: the topic contents they saw.
KAFKA_RECORDED_FROM = "recorded-from.json"
REMOTE_SCENARIOS = SCENARIOS / "remote"
REMOTE_CORPUS = CORPUS / "remote"
REMOTE_BASELINE = ORACLE / "remote-baseline.json"
FUZZ_OUT = ORACLE / ".fuzz"
KNOWN = ORACLE / "fuzz-known-findings.json"
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


def cmd_selfcheck(args: argparse.Namespace) -> int:
    target = open_target(args.url)
    tally = _Tally(f"selfcheck {target}: kept up to date vs from scratch")
    for scenario in _load(args):
        if not scenario.steps:
            continue  # nothing is kept up to date without steps
        try:
            kept, fresh = target.run_fresh(scenario)
        except RunError as error:
            tally.error(scenario, error)
            continue
        tally.report(scenario, rules.compare_with_fresh(scenario, kept, fresh))
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


def _load_scripts(args: argparse.Namespace) -> list[Any]:
    from . import scripts

    select = [name.strip() for name in (args.scripts or "").split(",") if name.strip()]
    scenarios = scripts.load_all(Path(args.scenarios), select)
    if not scenarios:
        raise sc.ScenarioError(f"no script scenario under {args.scenarios}")
    return scenarios


def cmd_record_scripts(args: argparse.Namespace) -> int:
    from . import scripts

    corpus = Path(args.corpus)
    target = scripts.Target(args.url)
    failed = 0
    scenarios = _load_scripts(args)
    for scenario in scenarios:
        try:
            recorded = target.run(scenario)
            problems = scripts.undeclared(scenario, recorded)
            if problems:
                raise RunError("; ".join(problems))
        except RunError as error:
            failed += 1
            print(f"ERROR {scenario.name}: {error}")
            continue
        scripts.save(recorded, corpus / scenario.name, scenario, REFERENCE)
        errors = sum(outcome is not None for outcome in recorded.outcomes)
        print(f"saved {scenario.name}: {len(recorded.outcomes)} scripts, {errors} of them "
              f"fail; {len(recorded.tables.keys())} results, {recorded.tables.failures()} of "
              "them failures")
    print(f"record-scripts {args.url}: {len(scenarios)} scenarios, "
          f"{len(scenarios) - failed} saved, {failed} not run")
    return 2 if failed else 0


def cmd_replay_scripts(args: argparse.Namespace) -> int:
    from . import scripts

    corpus = Path(args.corpus)
    target = scripts.Target(args.url)
    clean = different = missing = failed = mismatches = 0
    scenarios = _load_scripts(args)
    for scenario in scenarios:
        try:
            expected = scripts.load(corpus / scenario.name, scenario)
            actual = target.run(scenario)
        except (RunError, res.RecordingError) as error:
            failed += 1
            print(f"ERROR {scenario.name}: {error}")
            continue
        found = scripts.compare(scenario, expected, actual)
        absent = [m for m in found if m.kind == rules.MISSING]
        if absent:
            missing += 1
            print(f"MISSING {scenario.name}: step {absent[0].step}: {absent[0].actual}")
        elif found:
            different += 1
            print(f"DIFF  {scenario.name}")
        else:
            clean += 1
            print(f"ok    {scenario.name}")
        for mismatch in found:
            if mismatch.kind != rules.MISSING:
                mismatches += 1
                print(f"      {mismatch}")
    print(f"replay-scripts {target} vs recordings: {len(scenarios)} scenarios, {clean} clean, "
          f"{different} with differences ({mismatches} mismatches), {missing} missing, "
          f"{failed} not run")
    if failed:
        return 2
    return 1 if different or missing else 0


def _kafka_recorded_from(corpus: Path) -> str | None:
    try:
        return json.loads((corpus / KAFKA_RECORDED_FROM).read_text(encoding="utf-8"))["topics_sha256"]
    except (OSError, ValueError, KeyError):
        return None


def cmd_kafka_topics(args: argparse.Namespace) -> int:
    from . import kafka, kafka_topics

    path = Path(args.corpus) / kafka.TOPICS_FILE
    text = kafka_topics.dump(kafka_topics.fixture())
    if args.write:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        print(f"wrote {path}")
        return 0
    if not path.is_file() or path.read_text(encoding="utf-8") != text:
        print(f"{path} is not what harness/kafka_topics.py defines; write it with "
              "`kafka-topics --write` and record the Kafka scenarios again")
        return 1
    doc = kafka.load_fixture(path)
    records = sum(len(topic["records"]) for topic in doc["topics"])
    print(f"{path}: {len(doc['topics'])} topics, {records} records, "
          f"{len(doc['subjects'])} subjects, sha256 {kafka.digest(doc)}")
    return 0


def cmd_kafka_publish(args: argparse.Namespace) -> int:
    from . import kafka

    sent = kafka.publish(kafka.load_fixture(Path(args.corpus) / kafka.TOPICS_FILE), args.broker)
    print(f"published {sent} records to {args.broker}")
    return 0


def cmd_kafka_registry(args: argparse.Namespace) -> int:
    import signal
    import threading

    from . import kafka

    doc = kafka.load_fixture(Path(args.corpus) / kafka.TOPICS_FILE)
    registry = kafka.Registry(doc["subjects"], args.port)
    print(f"REGISTRY_READY port={registry.port}", flush=True)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    stop.wait()
    registry.close()
    if args.log:
        Path(args.log).write_text("".join(f"{line}\n" for line in registry.requests),
                                  encoding="utf-8")
    return 0


def cmd_kafka_record(args: argparse.Namespace) -> int:
    from . import kafka

    corpus = Path(args.corpus)
    topics = kafka.digest(kafka.load_fixture(corpus / kafka.TOPICS_FILE))
    if args.scripts and _kafka_recorded_from(corpus) != topics:
        print("error: the topics file is not the one the recordings were made from; record "
              "every Kafka scenario (no --scripts)", file=sys.stderr)
        return 2
    status = cmd_record_scripts(args)
    if status == 0 and not args.scripts:
        with open(corpus / KAFKA_RECORDED_FROM, "w", encoding="utf-8") as handle:
            json.dump({"topics_sha256": topics, "reference": REFERENCE}, handle, indent=2)
            handle.write("\n")
    return status


def _remote_scenarios(args: argparse.Namespace) -> list:
    from . import remote

    select = [name.strip() for name in (args.scripts or "").split(",") if name.strip()]
    return remote.load_all(Path(args.scenarios), select or None)


def cmd_remote_seed(args: argparse.Namespace) -> int:
    from . import remote

    remote.seed_remote(args.remote, REMOTE_CORPUS / remote.SEED)
    print(f"seeded {args.remote}")
    return 0


def cmd_remote_record(args: argparse.Namespace) -> int:
    from . import remote

    corpus = Path(args.corpus)
    seed = remote.seed_digest(REMOTE_CORPUS / remote.SEED)
    if args.scripts and remote.recorded_from(corpus) not in (None, seed):
        print("error: the seed is not the one the recordings were made from; record every "
              "remote scenario (no --scripts)", file=sys.stderr)
        return 2
    status = remote.record(args.url, args.remote, _remote_scenarios(args), corpus, REFERENCE)
    if status == 0 and not args.scripts:
        with open(corpus / remote.RECORDED_FROM, "w", encoding="utf-8") as handle:
            json.dump({"seed_sha256": seed, "reference": REFERENCE,
                       "remote": args.remote_kind}, handle, indent=2)
            handle.write("\n")
    return status


def cmd_remote_gate(args: argparse.Namespace) -> int:
    from . import remote

    corpus = Path(args.corpus)
    if remote.recorded_from(corpus) != remote.seed_digest(REMOTE_CORPUS / remote.SEED):
        print("error: the seed is not the one the recordings were made from; record the "
              "remote scenarios again against the Java reference", file=sys.stderr)
        return 2
    return remote.gate(args.url, args.remote, _remote_scenarios(args), corpus,
                       Path(args.baseline), Path(args.report) if args.report else None,
                       bool(args.scripts))


def cmd_remote_compare(args: argparse.Namespace) -> int:
    from . import remote

    return remote.compare_recordings(_remote_scenarios(args), Path(args.expected),
                                     Path(args.actual))


def cmd_kafka_gate(args: argparse.Namespace) -> int:
    from . import kafka

    corpus = Path(args.corpus)
    topics = kafka.digest(kafka.load_fixture(corpus / kafka.TOPICS_FILE))
    if _kafka_recorded_from(corpus) != topics:
        print("error: the topics file is not the one the recordings were made from; record "
              "the Kafka scenarios again against the Java reference", file=sys.stderr)
        return 2
    select = [name.strip() for name in (args.scripts or "").split(",") if name.strip()]
    return kafka.gate(args.url, Path(args.scenarios), corpus, Path(args.baseline),
                      Path(args.report) if args.report else None, select)


def cmd_show_scripts(args: argparse.Namespace) -> int:
    from . import scripts

    target = scripts.Target(args.url)
    for scenario in _load_scripts(args):
        run = target.run(scenario)
        for step, outcome in enumerate(run.outcomes):
            if outcome is not None:
                print(f"--- {scenario.name} step {step} script: fails with {outcome.type}: "
                      f"{outcome.text}")
            for table in (key[1] for key in run.tables.keys() if key[0] == step):
                data = run.tables.get(step, table)
                if isinstance(data, res.Failure):
                    print(f"--- {scenario.name} step {step} table {table}: fails with "
                          f"{data.code}")
                    continue
                print(f"--- {scenario.name} step {step} table {table} ({data.num_rows} rows)")
                for field in data.schema:
                    print(f"{field.name}: {field.type} = "
                          f"{data.column(field.name).to_pylist()}")
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
                "timestamp": "timestamp[ns, tz=UTC]",
                # the kinds of `fuzz_times`, as the wire carries them
                "date": "date64[ms]", "time": "time64[ns]", "datetime": "string",
                "duration": "duration[ns]", "dow": "string",
                "zoned": "timestamp[ns, tz=UTC]", "period": "month_interval", "zone": "string"}


def _arrow_name(kind: str) -> str:
    """How pyarrow prints the type of a predicted column; `list:<type>` is an array."""
    if kind.startswith("list:"):
        return f"list<: {_arrow_name(kind[len('list:'):])}>"
    return _ARROW_NAMES.get(kind, kind)


def _prediction_misses(predicted: dict[str, list], results: res.ResultSet) -> list[str]:
    """Result tables whose columns the generator predicted wrongly (a generator defect)."""
    out = []
    for step, table in results.keys():
        data = results.get(step, table)
        if step != 0 or isinstance(data, res.Failure):
            continue
        want = [(name, _arrow_name(kind)) for name, kind in predicted[table]]
        have = [(f.name, str(f.type)) for f in data.schema]
        if want != have:
            out.append(f"{results.scenario}.{table}: predicted {want}, got {have}")
    return out


def _signature(mismatch: rules.Mismatch, operations: dict[str, dict]) -> tuple[str, str, str]:
    """What a difference is about: its kind, the metadata keys that differ, the operation
    and, where the operation has several, the aggregation or `update_by` operator that made
    the column.

    Differences with one signature are taken to have one cause. Names of generated tables
    and columns are left out, since they vary from seed to seed.
    """
    op = operations.get(mismatch.table)
    operation = op["op"] if op else sc.INPUT_TABLE
    made_by = kn.producer(op, mismatch.column)
    if made_by not in (None, "key", "source"):
        operation = f"{operation}/{made_by}"
    return mismatch.kind, kn.metadata_key(mismatch), operation


class _Triage:
    """Sets the mismatches of known findings apart (`harness/known.py`)."""

    def __init__(self, args: argparse.Namespace, target_a: Any):
        self.known = kn.Known([] if args.no_known else kn.load(Path(args.known)))
        self.target_a = target_a
        self.seeds: dict[str, list[int]] = {}  # finding id -> seeds it was seen in
        self.count: Counter = Counter()

    def split(self, scenario: sc.Scenario, found: list[rules.Mismatch],
              seed: int | None = None, actual: res.ResultSet | None = None,
              ) -> tuple[list[rules.Mismatch], list[kn.Finding]]:
        """The mismatches that fit no known finding, and the findings the others fit.

        `actual` are the results of the actual side. With them, a result counts as one the
        reference is inconsistent about only if the actual side holds for it what the
        reference answers from scratch: that is what such a finding says of the actual
        side (`PARITY.md`, A13), and a result that is neither of the reference's two
        answers is a difference.
        """
        if not found or not self.known:
            return found, []

        def reference_inconsistent() -> set[tuple[int, str]] | None:
            if not hasattr(self.target_a, "run_fresh"):
                return None
            try:
                kept, fresh = self.target_a.run_fresh(scenario)
            except RunError:
                return None
            both = {(m.step, m.table) for m in rules.compare_with_fresh(scenario, kept, fresh)}
            if actual is not None:
                both -= rules.differs_from_fresh(scenario, fresh, actual, both)
            return both

        fits = self.known.classify(found, scenario.operations, reference_inconsistent)
        if seed is not None:
            for finding in fits:
                if finding is not None:
                    self.count[finding.id] += 1
                    if seed not in self.seeds.setdefault(finding.id, []):
                        self.seeds[finding.id].append(seed)
        new = [mismatch for mismatch, finding in zip(found, fits) if finding is None]
        return new, [finding for finding in fits if finding is not None]

    def report(self) -> None:
        by_id = {finding.id: finding for finding in self.known.findings}
        for name, seeds in sorted(self.seeds.items(), key=lambda item: -len(item[1])):
            print(f"KNOWN {name} ({by_id[name].cls}): {len(seeds)} seeds, "
                  f"{self.count[name]} mismatches; seeds {', '.join(map(str, seeds[:20]))}"
                  f"{' ...' if len(seeds) > 20 else ''}")

    def summary(self, known_only: int) -> str:
        if not known_only:
            return ""
        return f"; {known_only} more with known findings only (not counted as differences)"


def _fuzz_all(args: argparse.Namespace, ops: frozenset[str], target_a: Any, target_b: Any) -> int:
    """Runs every seed, and groups the differences instead of shrinking the first one."""
    out_dir = Path(args.out)
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    triage = _Triage(args, target_a)
    clean = not_run = differing = known_only = 0
    for seed in range(args.seed, args.seed + args.seeds):
        doc, _ = fz.generate(seed, ops, args.variants)
        scenario = sc.parse(doc)
        try:
            actual = target_b.run(scenario)
            found = rules.compare_scenario(scenario, target_a.run(scenario), actual, args.exact)
        except RunError as error:
            not_run += 1
            print(f"ERROR seed {seed}: {error}")
            continue
        if not found:
            clean += 1
            continue
        found, _ = triage.split(scenario, found, seed, actual)
        if not found:
            known_only += 1
            _write_scenario(out_dir, doc, f"fuzz_{seed}")
            continue
        differing += 1
        path = _write_scenario(out_dir, doc, f"fuzz_{seed}")
        operations = {op["name"]: op for op in doc["operations"]}
        for mismatch in found:
            group = groups.setdefault(_signature(mismatch, operations), {
                "seeds": [], "mismatches": 0, "example": mismatch, "path": path})
            group["mismatches"] += 1
            if seed not in group["seeds"]:
                group["seeds"].append(seed)
    ordered = sorted(groups.items(), key=lambda item: (-len(item[1]["seeds"]), item[0]))
    for (kind, key, operation), group in ordered:
        seeds = group["seeds"]
        print(f"GROUP kind={kind} key={key or '-'} operation={operation}: "
              f"{len(seeds)} seeds, {group['mismatches']} mismatches; seeds "
              f"{', '.join(map(str, seeds[:20]))}{' ...' if len(seeds) > 20 else ''}")
        print(f"      example, seed {seeds[0]}: {str(group['example'])[:600]}")
        print(f"      scenario: {group['path']}")
    triage.report()
    if differing or known_only:
        print(f"differing scenarios are in {out_dir}; rerun one with: python -m harness diff "
              f"--a {target_a} --b {target_b} --scenarios <file>")
    print(f"fuzz: {args.seeds} seeds, {clean} clean, {differing} with differences in "
          f"{len(groups)} groups, {not_run} not run{triage.summary(known_only)}")
    return 1 if differing else 2 if not_run else 0


def cmd_fuzz(args: argparse.Namespace) -> int:
    if (args.ops is None) == (args.family is None):
        raise sc.ScenarioError("fuzz needs --ops or --family, and not both")
    if args.family in fz.TIME_FAMILIES:
        ops = fz.TIME_FAMILIES[args.family]
        args.variants = args.variants or "times"
    elif args.family in fz.THIRD_FAMILIES:
        ops = fz.THIRD_FAMILIES[args.family]
        args.variants = args.variants or "third"
    elif args.family in fz.SECOND_FAMILIES:
        ops = fz.SECOND_FAMILIES[args.family]
        args.variants = args.variants or "second"
    elif args.family is not None:
        ops = fz.FAMILIES[args.family]
        args.variants = args.variants or "all"
    else:
        ops = _ops(args.ops) - {sc.INPUT_TABLE}
        args.variants = args.variants or fz.default_variants(ops)
    target_a = open_target(args.a)
    target_b = open_target(args.b or args.a)
    out_dir = Path(args.out)
    used: Counter = Counter()
    clean = not_run = results_total = failures_total = known_only = 0
    misses: list[str] = []
    print(f"fuzz {target_a} vs {target_b}: seeds {args.seed}..{args.seed + args.seeds - 1} "
          f"over {', '.join(sorted(ops))}")
    if args.variants == "recorded":
        print("variants: recorded (what the fuzzer built when the seeds in the run log were "
              "found; --variants all draws every variant of each operation)")
    else:
        print(f"variants: {args.variants}")
    if args.keep_going:
        return _fuzz_all(args, ops, target_a, target_b)
    triage = _Triage(args, target_a)
    for seed in range(args.seed, args.seed + args.seeds):
        doc, predicted = fz.generate(seed, ops, args.variants)
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
        found, fits = triage.split(scenario, found, seed, actual)
        if not found:
            known_only += 1
            print(f"known seed {seed}: {', '.join(sorted({f.id for f in fits}))}")
            continue

        print(f"DIFF  seed {seed}: {len(found)} mismatches that fit no known finding; the "
              f"first:" if fits else f"DIFF  seed {seed}: {len(found)} mismatches; the first:")
        print(f"      {found[0]}")
        original = _write_scenario(out_dir, doc, f"fuzz_{seed}")

        def still_differs(candidate: dict) -> bool:
            # A known finding does not count, or the shrinker would end at one of those.
            try:
                parsed = sc.parse(candidate)
                other = target_b.run(parsed)
                return bool(triage.split(parsed, rules.compare_scenario(
                    parsed, target_a.run(parsed), other, args.exact), None, other)[0])
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
        print(f"fuzz: seed {seed} differs after {clean} clean seeds"
              f"{triage.summary(known_only)}")
        return 1

    print(f"operations used: {', '.join(f'{k} {v}' for k, v in sorted(used.items()))}")
    print(f"results compared: {results_total}, of which failures on the reference side: "
          f"{failures_total}")
    if misses:
        print(f"generator predicted the columns of {len(misses)} tables wrongly, e.g. "
              f"{misses[0]}")
    triage.report()
    print(f"fuzz: {args.seeds} seeds, {clean} clean, 0 with differences, {not_run} not run"
          f"{triage.summary(known_only)}")
    return 2 if not_run else 0


def cmd_client_smoke(args: argparse.Namespace) -> int:
    from . import smoke

    return smoke.run(args.area, args.url)


def cmd_gate(args: argparse.Namespace) -> int:
    from . import gate

    return gate.run(args.url, Path(args.scenarios), Path(args.corpus), Path(args.baseline),
                    Path(args.report) if args.report else None)


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

    p = sub.add_parser("gate", help="replay and client smokes against the checked-in baseline")
    p.add_argument("--url", required=True, help="URL of the server under test")
    p.add_argument("--scenarios", default=str(SCENARIOS))
    p.add_argument("--corpus", default=str(CORPUS))
    p.add_argument("--baseline", default=str(BASELINE))
    p.add_argument("--report", help="file to write what was observed to, as JSON")
    p.set_defaults(run=cmd_gate)

    p = sub.add_parser("show", help="run scenarios and print their results")
    p.add_argument("--url", required=True)
    common(p)
    p.set_defaults(run=cmd_show)

    p = sub.add_parser("fuzz", help="compare two servers on generated scenarios")
    p.add_argument("--a", required=True, help="URL of the expected side (the reference)")
    p.add_argument("--b", help="URL of the actual side; default is --a again")
    p.add_argument("--ops", help="comma-separated operations to draw from")
    p.add_argument("--family", choices=sorted(set(fz.FAMILIES) | set(fz.SECOND_FAMILIES)
                                           | set(fz.TIME_FAMILIES) | set(fz.THIRD_FAMILIES)),
                   help="instead of --ops: a named set of operations (fuzz.FAMILIES), with "
                        "the variants \"all\" unless --variants says otherwise; or one of "
                        "the second parity pass (fuzz.SECOND_FAMILIES), with the variants "
                        "\"second\"; or one of fuzz.TIME_FAMILIES or fuzz.THIRD_FAMILIES, "
                        "with the variants \"times\" or \"third\"")
    p.add_argument("--variants", choices=fz.VARIANTS,
                   help="recorded: the generator of the seeds in the run log, the default "
                        "for --ops it can build. all: every operation and variant "
                        "(harness/fuzz_wide.py), the default when --ops names a newer "
                        "operation. second: the operations and forms of the second "
                        "parity pass (harness/fuzz_second.py). times: the date and time "
                        "column types (harness/fuzz_times.py). third: chains of as-of "
                        "joins, vector columns and conditions between windows "
                        "(harness/fuzz_third.py). A seed means a different scenario "
                        "under each")
    p.add_argument("--seeds", type=int, default=100, help="how many seeds to run")
    p.add_argument("--seed", type=int, default=0, help="the first seed")
    p.add_argument("--exact", action="store_true",
                   help="no float tolerance: for two runs of the same server")
    p.add_argument("--out", default=str(FUZZ_OUT), help="where a differing scenario is written")
    p.add_argument("--max-shrink-runs", type=int, default=200)
    p.add_argument("--keep-going", action="store_true",
                   help="run every seed: write each differing scenario to --out, group the "
                        "differences by kind, differing metadata keys and operation, and "
                        "print one example per group; nothing is shrunk")
    p.add_argument("--known", default=str(KNOWN),
                   help="the known findings to set apart (harness/known.py); default "
                        "fuzz-known-findings.json")
    p.add_argument("--no-known", action="store_true",
                   help="report every difference, known or not")
    p.set_defaults(run=cmd_fuzz)

    p = sub.add_parser("selfcheck", help="compare what one server kept up to date through "
                                         "the steps with its results from scratch")
    p.add_argument("--url", required=True)
    common(p)
    p.set_defaults(run=cmd_selfcheck)

    def scripts(p: argparse.ArgumentParser) -> None:
        p.add_argument("--url", required=True)
        p.add_argument("--scenarios", default=str(SCRIPT_SCENARIOS),
                       help="directory of script scenarios, or one scenario file")
        p.add_argument("--scripts", help="comma-separated names, or starts of names "
                                         "(agg_): only these script scenarios")

    p = sub.add_parser("record-scripts",
                       help="store what the Java reference answers to the script scenarios")
    scripts(p)
    p.add_argument("--corpus", default=str(SCRIPT_CORPUS))
    p.set_defaults(run=cmd_record_scripts)

    p = sub.add_parser("replay-scripts",
                       help="compare one server with the recordings of the script scenarios")
    scripts(p)
    p.add_argument("--corpus", default=str(SCRIPT_CORPUS))
    p.set_defaults(run=cmd_replay_scripts)

    p = sub.add_parser("show-scripts", help="run script scenarios and print what they leave")
    scripts(p)
    p.set_defaults(run=cmd_show_scripts)

    p = sub.add_parser("kafka-topics", help="check the topics file of the Kafka corpus "
                                            "against its definition, or write it")
    p.add_argument("--corpus", default=str(KAFKA_CORPUS))
    p.add_argument("--write", action="store_true")
    p.set_defaults(run=cmd_kafka_topics)

    p = sub.add_parser("kafka-publish", help="create the topics of the Kafka corpus on a "
                                             "fresh broker and send their records")
    p.add_argument("--broker", required=True, help="host:port of the broker")
    p.add_argument("--corpus", default=str(KAFKA_CORPUS))
    p.set_defaults(run=cmd_kafka_publish)

    p = sub.add_parser("kafka-registry", help="serve the schema registry of the Kafka corpus "
                                              "on 127.0.0.1 until interrupted")
    p.add_argument("--port", type=int, default=0)
    p.add_argument("--corpus", default=str(KAFKA_CORPUS))
    p.add_argument("--log", help="write the paths that were asked for to this file at the end")
    p.set_defaults(run=cmd_kafka_registry)

    def kafka_scripts(p: argparse.ArgumentParser) -> None:
        p.add_argument("--url", required=True)
        p.add_argument("--scenarios", default=str(KAFKA_SCENARIOS))
        p.add_argument("--corpus", default=str(KAFKA_CORPUS))
        p.add_argument("--scripts", help="comma-separated names, or starts of names: only "
                                         "these Kafka scenarios")

    p = sub.add_parser("kafka-record",
                       help="store what the Java reference answers to the Kafka scenarios")
    kafka_scripts(p)
    p.set_defaults(run=cmd_kafka_record)

    p = sub.add_parser("kafka-gate", help="replay the Kafka scenarios against the checked-in "
                                          "Kafka baseline")
    kafka_scripts(p)
    p.add_argument("--baseline", default=str(KAFKA_BASELINE))
    p.add_argument("--report", help="write the full outcome to this JSON file")
    p.set_defaults(run=cmd_kafka_gate)

    def remote_scripts(p: argparse.ArgumentParser) -> None:
        p.add_argument("--scenarios", default=str(REMOTE_SCENARIOS))
        p.add_argument("--scripts", help="comma-separated names, or starts of names: only "
                                         "these remote scenarios")

    p = sub.add_parser("remote-seed", help="run the seed's scope script on a remote")
    p.add_argument("--remote", required=True, help="the remote's URL (dh://host:port)")
    p.set_defaults(run=cmd_remote_seed)

    p = sub.add_parser("remote-record",
                       help="store what the Java reference answers to the remote scenarios")
    p.add_argument("--url", required=True)
    p.add_argument("--remote", required=True)
    p.add_argument("--corpus", default=str(REMOTE_CORPUS))
    p.add_argument("--remote-kind", default="oxhaven", choices=["oxhaven", "deephaven"])
    remote_scripts(p)
    p.set_defaults(run=cmd_remote_record)

    p = sub.add_parser("remote-gate", help="replay the remote scenarios against the "
                                           "checked-in remote baseline")
    p.add_argument("--url", required=True)
    p.add_argument("--remote", required=True)
    p.add_argument("--corpus", default=str(REMOTE_CORPUS))
    p.add_argument("--baseline", default=str(REMOTE_BASELINE))
    p.add_argument("--report", help="write the full outcome to this JSON file")
    remote_scripts(p)
    p.set_defaults(run=cmd_remote_gate)

    p = sub.add_parser("remote-compare", help="compare two recordings of the remote "
                                              "scenarios exactly")
    p.add_argument("--expected", required=True, help="a corpus directory")
    p.add_argument("--actual", default=str(REMOTE_CORPUS))
    remote_scripts(p)
    p.set_defaults(run=cmd_remote_compare)

    p = sub.add_parser("client-smoke", help="exercise one area of the client protocol")
    p.add_argument("area", choices=["session", "tables", "flight", "barrage", "inputtable",
                                    "console"])
    p.add_argument("--url", required=True)
    p.set_defaults(run=cmd_client_smoke)

    from .formulas import cli as formulas  # imported here: only this command needs it

    formulas.configure(sub.add_parser(
        "formulas", help="record or compare the formula corpus (corpus/formulas)"))

    args = parser.parse_args(argv)
    stacks_after = os.environ.get("ORACLE_STACKS_AFTER")
    if stacks_after:
        # a command still running after that many seconds has stalled: show where
        faulthandler.dump_traceback_later(float(stacks_after), exit=False)
    try:
        return args.run(args)
    except (sc.ScenarioError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
