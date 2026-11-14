"""The replay gate's baseline: the expected state of every scenario and every client smoke check.

No Java counterpart: harness bookkeeping. Standard library only, so that the check of a baseline
change (`python -m harness.baseline stricter`) runs without the harness's Python environment.

`replay-baseline.json` lists, per section (`replay`, `client-smoke <area>`), every scenario or
check with its state when run against Oxhaven:

    "where_int": {"state": "clean"}
    "aj_int_keys": {"state": "differs", "kinds": ["failure_unexpected:UNIMPLEMENTED"],
                    "tables": ["keyed", "strict", "unkeyed"], "why": "no handler for aj"}

`kinds` is the set of difference kinds the entry shows (see `gate.kinds_of`), `tables` the
result tables of the scenario that show a difference (a smoke check has none), and `why` says
in words what causes them. The gate (`judge`) is a ratchet in both directions:

- an entry listed `clean` must be clean;
- an entry listed `differs` must differ, with exactly the listed kinds in exactly the listed
  tables. If it is clean now, or shows fewer kinds or tables, the baseline must be made
  stricter in the same pull request; if it shows a kind or a table that is not listed, it
  differs in a new way;
- everything that ran must be listed, and everything listed must have run.

A baseline may only get stricter (`weakenings`): `differs` to `clean`, or fewer kinds or tables.
Any other change (a new entry, a removed entry, `clean` to `differs`, another kind or table) is
a protected change.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

FORMAT = 1
CLEAN = "clean"
DIFFERS = "differs"
NOT_RUN = "not_run"  # an observed state only: never valid in a baseline


class BaselineError(ValueError):
    """The baseline file cannot be used."""


@dataclass(frozen=True)
class Entry:
    """One line of the baseline, or one observed outcome (which may also be `NOT_RUN`)."""

    state: str
    kinds: tuple[str, ...] = ()
    tables: tuple[str, ...] = ()
    why: str | None = None

    def line(self) -> str:
        """The entry as it is written in the baseline file."""
        if self.state == CLEAN:
            return json.dumps({"state": CLEAN})
        fields: dict[str, object] = {"state": self.state, "kinds": list(self.kinds)}
        if self.tables:
            fields["tables"] = list(self.tables)
        fields["why"] = self.why or "FILL IN: what causes these differences"
        return json.dumps(fields)


Sections = Mapping[str, Mapping[str, Entry]]


def _no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    out: dict[str, object] = {}
    for key, value in pairs:
        if key in out:
            # json keeps the last one silently, which could hide a stricter entry.
            raise BaselineError(f"{key!r} is listed twice")
        out[key] = value
    return out


def parse(text: str, where: str = "the baseline") -> dict[str, dict[str, Entry]]:
    try:
        doc = json.loads(text, object_pairs_hook=_no_duplicates)
    except json.JSONDecodeError as error:
        raise BaselineError(f"{where} is not JSON: {error}") from error
    except BaselineError as error:
        raise BaselineError(f"{where}: {error}") from error
    if not isinstance(doc, dict) or doc.get("format") != FORMAT:
        raise BaselineError(f"{where} is not a replay baseline of format {FORMAT}")
    sections: dict[str, dict[str, Entry]] = {}
    for section, entries in doc.items():
        if section == "format":
            continue
        if not isinstance(entries, dict):
            raise BaselineError(f"{where}: section {section!r} is not an object")
        sections[section] = {}
        for name, raw in entries.items():
            label = f"{where}: {section} / {name}"
            if not isinstance(raw, dict) or set(raw) - {"state", "kinds", "tables", "why"}:
                raise BaselineError(f"{label}: expected state, and for differs kinds, tables "
                                    "and why")
            state, why = raw.get("state"), raw.get("why")
            kinds, tables = raw.get("kinds"), raw.get("tables")
            if state == CLEAN:
                if kinds is not None or tables is not None or why is not None:
                    raise BaselineError(f"{label}: a clean entry has only its state")
                sections[section][name] = Entry(CLEAN)
            elif state == DIFFERS:
                if not _distinct_names(kinds) or not kinds:
                    raise BaselineError(f"{label}: differs needs a non-empty list of distinct "
                                        "kinds")
                if tables is not None and (not _distinct_names(tables) or not tables):
                    raise BaselineError(f"{label}: tables is a non-empty list of distinct "
                                        "table names, or absent")
                if not isinstance(why, str) or not why.strip() or why.startswith("FILL IN"):
                    raise BaselineError(f"{label}: differs needs a why that names the cause")
                sections[section][name] = Entry(DIFFERS, tuple(sorted(kinds)),
                                                tuple(sorted(tables or ())), why)
            else:
                raise BaselineError(f"{label}: state must be {CLEAN!r} or {DIFFERS!r}, "
                                    f"not {state!r}")
    return sections


def _distinct_names(value: object) -> bool:
    return (isinstance(value, list) and all(isinstance(v, str) and v for v in value)
            and len(set(value)) == len(value))


def load(path: Path) -> dict[str, dict[str, Entry]]:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError as error:
        raise BaselineError(f"cannot read the baseline {path}: {error}") from error
    return parse(text, str(path))


def dump(sections: Sections) -> str:
    """The file form: one entry per line, sorted, so that a change is one changed line."""
    lines = ["{", f' "format": {FORMAT}']
    for section in sorted(sections, key=lambda s: (s != "replay", s)):
        lines[-1] += ","
        lines.append(f" {json.dumps(section)}: {{")
        entries = sections[section]
        for index, name in enumerate(sorted(entries)):
            comma = "," if index + 1 < len(entries) else ""
            lines.append(f"  {json.dumps(name)}: {entries[name].line()}{comma}")
        lines.append(" }")
    lines.append("}")
    return "\n".join(lines) + "\n"


def judge(baseline: Sections, observed: Sections) -> list[str]:
    """Why the observed outcomes do not match the baseline; empty when the gate passes."""
    problems: list[str] = []
    for section in sorted(set(baseline) | set(observed)):
        listed, seen = baseline.get(section, {}), observed.get(section, {})
        for name in sorted(set(listed) | set(seen)):
            label = f"{section} / {name}"
            want, got = listed.get(name), seen.get(name)
            if got is None:
                problems.append(
                    f"STALE      {label}: listed in the baseline but it did not run. If it was "
                    "removed or renamed on purpose, the baseline entry goes with it (a "
                    "protected change).")
            elif got.state == NOT_RUN:
                problems.append(f"NOT RUN    {label}: it could not be run, so nothing was "
                                "compared.")
            elif want is None:
                problems.append(
                    f"UNLISTED   {label}: not in the baseline. Add this line to section "
                    f"{json.dumps(section)} (adding an entry is a protected change): "
                    f"{json.dumps(name)}: {got.line()}")
            elif want.state == CLEAN and got.state == DIFFERS:
                problems.append(
                    f"REGRESSED  {label}: the baseline says clean, and it now differs "
                    f"({', '.join(got.kinds)}). Fix the change; clean to differs is a protected "
                    "change.")
            elif want.state == DIFFERS and got.state == CLEAN:
                problems.append(
                    f"PROMOTE    {label}: the baseline says differs, and it is now clean. "
                    "Promote it in this pull request: replace its baseline line with "
                    f"{json.dumps(name)}: {got.line()}")
            elif want.state == DIFFERS:
                new_kinds = sorted(set(got.kinds) - set(want.kinds))
                new_tables = sorted(set(got.tables) - set(want.tables))
                if new_kinds:
                    problems.append(
                        f"NEW KIND   {label}: differs in a way the baseline does not list: "
                        f"{', '.join(new_kinds)} (listed: {', '.join(want.kinds)}). Fix the "
                        "change; listing another kind is a protected change.")
                if new_tables:
                    problems.append(
                        f"NEW TABLE  {label}: result tables that the baseline does not list "
                        f"now differ: {', '.join(new_tables)} (listed: "
                        f"{', '.join(want.tables)}). Fix the change; listing another table is a "
                        "protected change.")
                if not new_kinds and not new_tables and (
                        got.kinds != want.kinds or got.tables != want.tables):
                    gone = sorted((set(want.kinds) - set(got.kinds))
                                  | (set(want.tables) - set(got.tables)))
                    fixed = Entry(DIFFERS, got.kinds, got.tables, want.why)
                    problems.append(
                        f"TIGHTEN    {label}: no longer shows {', '.join(gone)}. Tighten it "
                        "in this pull request: replace its baseline line with "
                        f"{json.dumps(name)}: {fixed.line()}")
    return problems


def weakenings(base: Sections, head: Sections) -> list[str]:
    """The changes from `base` to `head` that do not make the baseline stricter."""
    out: list[str] = []
    for section in sorted(set(base) | set(head)):
        before, after = base.get(section, {}), head.get(section, {})
        for name in sorted(set(before) | set(after)):
            label = f"{section} / {name}"
            old, new = before.get(name), after.get(name)
            if old is None:
                out.append(f"{label}: added")
            elif new is None:
                out.append(f"{label}: removed")
            elif old.state == CLEAN and new.state == DIFFERS:
                out.append(f"{label}: clean became differs")
            elif old.state == DIFFERS and new.state == DIFFERS:
                kinds = sorted(set(new.kinds) - set(old.kinds))
                tables = sorted(set(new.tables) - set(old.tables))
                if kinds:
                    out.append(f"{label}: allows another kind of difference: "
                               f"{', '.join(kinds)}")
                if tables:
                    out.append(f"{label}: allows another table to differ: {', '.join(tables)}")
    return out


def main(argv: list[str]) -> int:
    """`python -m harness.baseline stricter <base file> <head file>`: 0 when `head` is `base`
    made stricter or left alone, 1 when it weakens it, 2 when a file cannot be used."""
    if len(argv) != 4 or argv[1] != "stricter":
        print("usage: python -m harness.baseline stricter <base file> <head file>",
              file=sys.stderr)
        return 2
    try:
        found = weakenings(load(Path(argv[2])), load(Path(argv[3])))
    except BaselineError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    if found:
        print("The replay baseline may only get stricter (differs to clean, or fewer kinds). "
              "These changes are not that:")
        for line in found:
            print(f"  {line}")
        return 1
    print("the replay baseline did not get weaker")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
