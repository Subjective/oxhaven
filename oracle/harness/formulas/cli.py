"""The `formulas` command of the harness: `oracle/bin/harness formulas ...`.

No Java counterpart: harness entry point.

    formulas --url URL                  compare a server with every recorded outcome
    formulas --check NAME --url URL     the same for the cases of one check (see CHECKS)
    formulas --record --url JAVA_URL    run the corpus on the Java reference and store the outcomes
    formulas --stats                    count the corpus by group, kind, subset and outcome
    formulas --generate                 rebuild the case list; recorded outcomes of unchanged
                                        cases are kept

Every mode takes the same selection, applied on top of the check: `--group`, `--tag` (a case
must have every tag given), `--not-tag`, `--subset`, and id prefixes as arguments.

Exit status: 0 when no difference was found, 1 when there are differences, 2 when the corpus
could not be used, the arguments select nothing, or a selected case has no recording.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Callable

from . import corpus as cp
from .compare import compare
from .engine import Engine, JavaLog, default_log
from .tags import Builtins, classify, nondeterministic


def _plain(case: cp.Case) -> bool:
    """Inside the accepted subset and run directly against a fixture."""
    return case.subset == "accepted" and not case.setup


def _calls(case: cp.Case) -> bool:
    return any(t.startswith("function:") and t != "function:isNull" for t in case.tags)


def _rows(case: cp.Case) -> bool:
    return "array" in case.tags or "row-var" in case.tags


# name -> (which cases, whether values are compared, what it is for)
CHECKS: dict[str, tuple[Callable[[cp.Case], bool], bool, str]] = {
    "minimal": (
        lambda c: _plain(c) and "minimal" in c.tags, True,
        "the subset of task 3.1: columns, literals, + - * / %, comparisons, && || !, isNull"),
    "accept-reject": (
        lambda c: _plain(c) and not _calls(c) and not _rows(c), False,
        "operators, literals, casts and the ternary: whether the formula is accepted, the "
        "status code when it is not, and the result type; values are not compared"),
    "values": (
        lambda c: _plain(c) and not _calls(c) and not _rows(c), True,
        "the cases of accept-reject, with values"),
    "functions": (
        lambda c: _plain(c) and _calls(c) and not _rows(c), True,
        "calls to the default-imported function library"),
    "arrays": (
        lambda c: _plain(c) and _rows(c), True,
        "column arrays and the row variables i, ii and k"),
    "chains": (
        lambda c: c.subset == "accepted" and bool(c.setup), True,
        "accepted formulas from the tests and docs that run after other operations"),
    "outside": (
        lambda c: c.subset == "outside", True,
        "formulas outside the accepted subset, as Java answers them"),
    "all": (lambda c: True, True, "every case"),
}


def _select(corpus: cp.Corpus, args: argparse.Namespace) -> list[cp.Case]:
    in_check = CHECKS[args.check or "all"][0]
    out = []
    for case in corpus.cases:
        if not in_check(case):
            continue
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


def _need_url(args: argparse.Namespace) -> bool:
    if args.url:
        return True
    print("error: --url is required", file=sys.stderr)
    return False


def cmd_record(args: argparse.Namespace, corpus: cp.Corpus, selected: list[cp.Case]) -> int:
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
    cp.save_cases(Path(args.corpus), corpus.cases)
    print(f"formulas record {args.url}: {len(selected)} cases in "
          f"{time.monotonic() - started:.0f}s, {engine.calls} calls, {changed} outcomes changed")
    for key in sorted(tally):
        print(f"  {tally[key]:6d}  {key}")
    return 0


def cmd_replay(args: argparse.Namespace, corpus: cp.Corpus, selected: list[cp.Case]) -> int:
    values = CHECKS[args.check or "all"][1]
    unusable = [c for c in selected if c.java is None]
    # A case whose input table Java could not build says nothing about the formula.
    skipped = {c.id for c in selected if _is_error(c) and c.java["error"]["stage"] == "setup"}
    runnable = [c for c in selected if c.java is not None and c.id not in skipped]
    engine = Engine(args.url, corpus, None, _progress)
    try:
        outcomes = engine.run(runnable, _is_error)
    finally:
        engine.close()
    clean = different = uncompared = mismatches = 0
    by_kind: Counter[str] = Counter()
    for case in runnable:
        found, partial = compare(case, case.java, outcomes[case.id], values)
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
    label = f"check {args.check}" if args.check else "replay"
    print(f"formulas {label} {args.url} vs recordings: {len(selected)} cases, {clean} clean, "
          f"{different} with differences ({mismatches} mismatches), {uncompared} uncompared "
          f"(result type unknown to the rules), {len(skipped)} skipped (setup fails in Java), "
          f"{len(unusable)} not recorded")
    for kind in sorted(by_kind):
        print(f"  {by_kind[kind]:6d}  {kind}")
    if unusable:
        return 2
    return 1 if different else 0


def cmd_stats(args: argparse.Namespace, corpus: cp.Corpus, selected: list[cp.Case]) -> int:
    print(f"{len(selected)} cases, {len(corpus.fixtures)} fixtures")
    for title, key in (
        ("group", lambda c: [c.group]),
        ("kind", lambda c: [c.kind]),
        ("subset", lambda c: [c.subset]),
        ("outcome", lambda c: ["not recorded" if c.java is None else
                               f"error {c.java['error']['status']} ({c.java['error']['stage']})"
                               if "error" in c.java else "result"]),
        ("check", lambda c: [name for name, (member, _v, _d) in CHECKS.items() if member(c)]),
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
        texts = [draft.formula] + [text for _op, strings in draft.setup for text in strings]
        if nondeterministic(texts) and "nondeterministic" not in verdict.tags:
            verdict.tags = sorted(verdict.tags + ["nondeterministic"])
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
    print(f"formulas generate: {len(cases)} cases, {len(used)} fixtures, {recorded} with a kept "
          "outcome")
    return 0


def configure(parser: argparse.ArgumentParser) -> None:
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--record", action="store_true",
                      help="store the Java outcomes; run against the Java reference only")
    mode.add_argument("--check", choices=sorted(CHECKS),
                      help="compare only the cases of this check")
    mode.add_argument("--stats", action="store_true", help="count the corpus")
    mode.add_argument("--generate", action="store_true", help="rebuild the case list")
    parser.add_argument("--url", help="URL of the server (the Java reference for --record)")
    parser.add_argument("--java-log", help="--record: the log of the Java server; default "
                                           ".run/java-<port>.log")
    parser.add_argument("--reference", help="--generate: the reference checkout (REFERENCE.md); "
                                            "without it only the grid is rebuilt")
    parser.add_argument("--builtins", action="store_true",
                        help="--generate: also rebuild builtins.json from the pinned jars")
    parser.add_argument("--verbose", action="store_true", help="also list the clean cases")
    parser.add_argument("--tags", action="store_true", help="--stats: also count by tag")
    parser.add_argument("--corpus", default=str(cp.DEFAULT_DIR), help="corpus directory")
    parser.add_argument("--group", action="append", default=[], help="only this group")
    parser.add_argument("--tag", action="append", default=[], help="only cases with this tag")
    parser.add_argument("--not-tag", action="append", default=[], help="no cases with this tag")
    parser.add_argument("--subset", choices=("accepted", "outside"))
    parser.add_argument("only", nargs="*", help="id prefixes; default is every case")
    parser.set_defaults(run=run)


def run(args: argparse.Namespace) -> int:
    try:
        if args.generate:
            return cmd_generate(args)
        corpus = cp.load(Path(args.corpus))
        selected = _select(corpus, args)
        if not selected:
            print("error: the arguments select no case", file=sys.stderr)
            return 2
        if args.stats:
            return cmd_stats(args, corpus, selected)
        if not _need_url(args):
            return 2
        if args.record:
            return cmd_record(args, corpus, selected)
        return cmd_replay(args, corpus, selected)
    except (cp.CorpusError, FileNotFoundError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
