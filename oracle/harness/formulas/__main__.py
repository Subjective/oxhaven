"""Command line: `python -m harness.formulas <command>`, run from the `oracle/` directory.

No Java counterpart: harness entry point.

    record    run the corpus on the Java reference and store each case's outcome
    replay    run the corpus on one server and compare with the recorded outcomes
    stats     count the corpus by group, subset and tag
    generate  rebuild the case list (the grid, and the formulas extracted from the reference
              checkout); recorded outcomes of unchanged cases are kept

`record`, `replay` and `stats` take the same selection: `--group`, `--tag` (a case must have
every tag given), `--not-tag`, `--subset`, and id prefixes as arguments.

Exit status: 0 when no difference was found, 1 when there are differences, 2 when the corpus
could not be used or a case selected for replay has no usable recording.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path

from . import corpus as cp
from .compare import compare
from .engine import Engine, JavaLog, default_log
from .tags import Builtins, classify


def _select(corpus: cp.Corpus, args: argparse.Namespace) -> list[cp.Case]:
    out = []
    for case in corpus.cases:
        if args.group and case.group not in args.group:
            continue
        if args.subset and case.subset != args.subset:
            continue
        if any(tag not in case.tags for tag in args.tag):
            continue
        if any(tag in case.tags for tag in args.not_tag):
            continue
        if args.only and not any(case.id.startswith(prefix) for prefix in args.only):
            continue
        out.append(case)
    return out


def _progress(text: str) -> None:
    if sys.stderr.isatty():
        print(f"\r{text}   ", end="", file=sys.stderr, flush=True)


def _is_error(case: cp.Case) -> bool:
    return case.java is not None and "error" in case.java


def cmd_record(args: argparse.Namespace) -> int:
    directory = Path(args.corpus)
    corpus = cp.load(directory)
    selected = _select(corpus, args)
    log_path = Path(args.java_log) if args.java_log else default_log(args.url)
    if log_path is None:
        print("error: no Java log found; pass --java-log (bin/start-java.sh writes "
              ".run/java-<port>.log). Without it the error messages cannot be recorded.",
              file=sys.stderr)
        return 2
    started = time.monotonic()
    engine = Engine(args.url, corpus, JavaLog(log_path), _progress)
    try:
        outcomes = engine.run(selected, _is_error)
    finally:
        engine.close()
    changed = 0
    tally: Counter[str] = Counter()
    for case in selected:
        outcome = outcomes[case.id]
        if case.java != outcome:
            changed += 1
        case.java = outcome
        error = outcome.get("error")
        tally[f"error {error['status']} ({error['stage']})" if error else "result"] += 1
    cp.save_cases(directory, corpus.cases)
    print(f"record {args.url}: {len(selected)} cases in {time.monotonic() - started:.0f}s, "
          f"{engine.calls} calls, {changed} outcomes changed")
    for key in sorted(tally):
        print(f"  {tally[key]:6d}  {key}")
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    corpus = cp.load(Path(args.corpus))
    selected = _select(corpus, args)
    unusable = [c for c in selected if c.java is None]
    # A case whose input table Java could not build says nothing about the formula.
    skipped = [c for c in selected if _is_error(c) and c.java["error"]["stage"] == "setup"]
    runnable = [c for c in selected if c.java is not None and c not in skipped]
    engine = Engine(args.url, corpus, None, _progress)
    try:
        outcomes = engine.run(runnable, _is_error)
    finally:
        engine.close()
    clean = different = uncompared = mismatches = 0
    by_kind: Counter[str] = Counter()
    for case in runnable:
        found, partial = compare(case, case.java, outcomes[case.id])
        if found:
            different += 1
            mismatches += len(found)
            print(f"DIFF  {case.id}  [{case.kind}] {case.formula}")
            for mismatch in found:
                by_kind[mismatch.kind] += 1
                print(f"      {mismatch}")
        elif partial:
            uncompared += 1
            if args.verbose:
                print(f"SKIP  {case.id}  result type not known to the rules")
        else:
            clean += 1
            if args.verbose:
                print(f"ok    {case.id}")
    for case in unusable:
        print(f"ERROR {case.id}: not recorded")
    print(f"replay {args.url} vs recordings: {len(selected)} cases, {clean} clean, "
          f"{different} with differences ({mismatches} mismatches), {uncompared} uncompared "
          f"(result type unknown to the rules), {len(skipped)} skipped (setup fails in Java), "
          f"{len(unusable)} not recorded")
    for kind in sorted(by_kind):
        print(f"  {by_kind[kind]:6d}  {kind}")
    if unusable:
        return 2
    return 1 if different else 0


def cmd_stats(args: argparse.Namespace) -> int:
    corpus = cp.load(Path(args.corpus))
    selected = _select(corpus, args)
    print(f"{len(selected)} cases, {len(corpus.fixtures)} fixtures")
    for title, key in (
        ("group", lambda c: [c.group]),
        ("kind", lambda c: [c.kind]),
        ("subset", lambda c: [c.subset]),
        ("outcome", lambda c: ["not recorded" if c.java is None else
                               f"error {c.java['error']['status']} ({c.java['error']['stage']})"
                               if "error" in c.java else "result"]),
        ("tag", lambda c: c.tags if args.tags else []),
    ):
        counts: Counter[str] = Counter()
        for case in selected:
            counts.update(key(case))
        if not counts:
            continue
        print(f"by {title}:")
        for name in sorted(counts):
            print(f"  {counts[name]:6d}  {name}")
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    from . import builtins as bi
    from . import grid

    directory = Path(args.corpus)
    directory.mkdir(parents=True, exist_ok=True)
    if args.builtins or not (directory / cp.BUILTINS).is_file():
        bi.write(directory / cp.BUILTINS)
    builtins = Builtins.load(directory / cp.BUILTINS)
    try:
        old = cp.load(directory)
    except FileNotFoundError:
        old = cp.Corpus({}, [])
    previous = {case.id: case for case in old.cases}

    fixtures = dict(grid.fixtures())
    drafts = list(grid.drafts())
    kept: list[cp.Case] = []
    if args.reference:
        from . import extract

        extracted = extract.Extractor(Path(args.reference), builtins)
        more, more_fixtures, report = extracted.run()
        drafts += more
        fixtures.update(more_fixtures)
        for line in report:
            print(line)
    else:
        # Without the reference checkout the extracted groups are kept as they are.
        kept = [case for case in old.cases if not case.group.startswith("grid-")]
        for case in kept:
            fixtures[case.fixture] = old.fixtures[case.fixture]

    cases: list[cp.Case] = []
    for draft in drafts:
        columns = draft.columns if draft.columns is not None else \
            fixtures[draft.fixture].column_types()
        verdict = classify(draft.formula, draft.kind, columns, builtins)
        case = cp.Case(id=draft.id, kind=draft.kind, fixture=draft.fixture,
                       formula=draft.formula, subset=verdict.subset, tags=verdict.tags,
                       source=draft.source, setup=list(draft.setup))
        before = previous.get(case.id)
        if before is not None and (before.kind, before.fixture, before.formula, before.setup) == \
                (case.kind, case.fixture, case.formula, case.setup) and \
                old.fixtures.get(case.fixture) == fixtures[case.fixture]:
            case.java = before.java
        cases.append(case)
    cases += kept
    used = {case.fixture for case in cases}
    cp.save_fixtures(directory, {name: fixtures[name] for name in sorted(used)})
    cp.save_cases(directory, cases, remove_stale=True)
    recorded = sum(1 for case in cases if case.java is not None)
    print(f"generate: {len(cases)} cases, {len(used)} fixtures, {recorded} with a kept outcome")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m harness.formulas",
                                     description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--corpus", default=str(cp.DEFAULT_DIR), help="corpus directory")

    def selection(p: argparse.ArgumentParser) -> None:
        common(p)
        p.add_argument("--group", action="append", default=[], help="only this group")
        p.add_argument("--tag", action="append", default=[], help="only cases with this tag")
        p.add_argument("--not-tag", action="append", default=[], help="no cases with this tag")
        p.add_argument("--subset", choices=("accepted", "outside"))
        p.add_argument("only", nargs="*", help="id prefixes; default is every case")

    p = sub.add_parser("record", help="store the Java outcomes; run against Java only")
    p.add_argument("--url", required=True, help="URL of the Java reference server")
    p.add_argument("--java-log", help="log of that server; default .run/java-<port>.log")
    selection(p)
    p.set_defaults(run=cmd_record)

    p = sub.add_parser("replay", help="compare one server with the recorded outcomes")
    p.add_argument("--url", required=True, help="URL of the server under test")
    p.add_argument("--verbose", action="store_true", help="also list the clean cases")
    selection(p)
    p.set_defaults(run=cmd_replay)

    p = sub.add_parser("stats", help="count the corpus")
    p.add_argument("--tags", action="store_true", help="also count by tag")
    selection(p)
    p.set_defaults(run=cmd_stats)

    p = sub.add_parser("generate", help="rebuild the case list")
    p.add_argument("--reference", help="the reference checkout (see REFERENCE.md); without it "
                                       "only the grid is rebuilt")
    p.add_argument("--builtins", action="store_true",
                   help="also rebuild builtins.json from the jars of the pinned wheel")
    common(p)
    p.set_defaults(run=cmd_generate)

    args = parser.parse_args(argv)
    try:
        return args.run(args)
    except cp.CorpusError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
