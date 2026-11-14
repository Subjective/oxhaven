"""Runs corpus cases on a server and returns their outcomes.

No Java counterpart: harness code. Like `harness.runner` it speaks to the server only through
pydeephaven, so the same code records from Java and replays against any other server.

`update` cases that share an input table are sent many per `update` call. When a call fails the
batch is halved until the failing cases are alone, so every error is attributed to one formula.
Cases that are expected to fail (their recorded outcome is an error) are sent alone from the
start. `where` cases are sent one per call.

The Java server answers a failed request with a status and `Details Logged w/ID '<id>'`; the
exception itself is only in its log. When recording, `JavaLog` reads the exception and its root
cause from the log file that `bin/start-java.sh` writes.
"""

from __future__ import annotations

import datetime
import decimal
import re
import time
from pathlib import Path
from typing import Any, Callable, Iterable

import pyarrow as pa
import pyarrow.types as pat
from pydeephaven.table import Table

from .. import scenario as sc
from ..connect import open_session, parse_url
from ..runner import _do_get_code, _import, failure_code
from .corpus import POSITION, Case, Corpus, Fixture

RESULT = "OxR"  # result columns of a batch are OxR0, OxR1, ...
BATCH = 40

_DETAILS_ID = re.compile(r"Details Logged w/ID '([^']+)'")
_RECORD_START = re.compile(r"^\d\d:\d\d:\d\d\.\d+ +[A-Z]+ ")
_INTERNAL_ERROR = re.compile(r"Internal Error '([^']+)' (.*)$")


class JavaLog:
    """Finds the exception behind a `Details Logged w/ID` answer in the Java server's log."""

    def __init__(self, path: Path):
        self._path = path
        self._offset = 0
        self._lines: list[str] = []
        self._partial = ""

    def _read_more(self) -> bool:
        with open(self._path, encoding="utf-8", errors="replace") as handle:
            handle.seek(self._offset)
            data = handle.read()
            self._offset = handle.tell()
        if not data:
            return False
        pieces = (self._partial + data).split("\n")
        self._partial = pieces.pop()
        self._lines.extend(pieces)
        return True

    def lookup(self, error_id: str, wait: float = 10.0) -> tuple[str | None, str | None]:
        """Returns (first line of the exception, first line of its root cause)."""
        deadline = time.monotonic() + wait
        marker = f"Internal Error '{error_id}'"
        while True:
            self._read_more()
            start = next((n for n, line in enumerate(self._lines) if marker in line), None)
            if start is not None:
                end = next(
                    (n for n in range(start + 1, len(self._lines))
                     if _RECORD_START.match(self._lines[n])),
                    None,
                )
                # The record is complete once the next record has started, or the file is quiet.
                if end is None and not self._settled():
                    continue
                record = self._lines[start:end]
                del self._lines[: (end if end is not None else len(self._lines))]
                return _parse_record(record)
            if time.monotonic() > deadline:
                return None, None
            time.sleep(0.02)

    def _settled(self) -> bool:
        time.sleep(0.05)
        return not self._read_more()


def _parse_record(record: list[str]) -> tuple[str | None, str | None]:
    head = _INTERNAL_ERROR.search(record[0])
    message = head.group(2).strip() if head else None
    causes = [n for n, line in enumerate(record) if line.startswith("Caused by: ")]
    if not causes:
        return message, None
    last = causes[-1]
    cause = record[last][len("Caused by: "):].strip()
    detail: dict[str, str] = {}
    following = []
    for line in record[last + 1:]:
        if line.startswith("\tat ") or line.startswith("\t..."):
            break
        text = line.strip()
        if not text:
            continue
        following.append(text)
        key, _, value = text.partition(":")
        if key.strip() in ("Exception type", "Exception message"):
            detail[key.strip()] = value.strip()
    if "Exception message" in detail:
        kind = detail.get("Exception type", "").rsplit(".", 1)[-1].rsplit("$", 1)[-1]
        cause = f"{cause.rstrip(':').strip()}: {kind}: {detail['Exception message']}"
    elif cause.endswith(":") and following:
        cause = f"{cause} {following[0]}"
    if cause.startswith("java.lang."):
        # The JVM drops the message of an implicit exception it has thrown often
        # (-XX:+OmitStackTraceInFastThrow), so only the class is the same on every run.
        cause = cause.split(":", 1)[0]
    return message, cause


def default_log(url: str) -> Path | None:
    """The log `bin/start-java.sh` writes for a local server on the URL's port, if it exists."""
    from .corpus import ORACLE

    path = ORACLE / ".run" / f"java-{parse_url(url).port}.log"
    return path if path.is_file() else None


def _status(error: BaseException) -> tuple[str, str]:
    """The gRPC status name and details of a failed call, from the exception chain."""
    code = failure_code(error)
    seen: set[int] = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        details = getattr(error, "details", None)
        if callable(details):
            try:
                return code or "UNKNOWN", details() or ""
            except Exception:  # noqa: BLE001 - not a gRPC error after all
                pass
        error = error.__cause__ or error.__context__
    return code or "UNKNOWN", ""


def encode_value(value: Any) -> Any:
    """The JSON form of one result cell."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return sc.encode_cell(value)
    if isinstance(value, bytes):
        return "0x" + value.hex()
    if isinstance(value, (list, tuple)):
        return [encode_value(item) for item in value]
    if isinstance(value, (datetime.date, datetime.time, datetime.timedelta, decimal.Decimal)):
        return str(value)
    return str(value)


def encode_column(field: pa.Field, column: pa.ChunkedArray) -> dict[str, Any]:
    metadata = field.metadata or {}
    out: dict[str, Any] = {
        "type": metadata.get(b"deephaven:type", b"").decode() or None,
        "arrow": str(field.type),
    }
    component = metadata.get(b"deephaven:componentType")
    if component is not None:
        out["component"] = component.decode()
    if pat.is_timestamp(column.type) or pat.is_duration(column.type) or pat.is_time(column.type):
        column = column.cast(pa.int64())
    try:
        out["values"] = [encode_value(v) for v in column.to_pylist()]
    except (KeyError, pa.ArrowException):
        # An Arrow type the pinned pyarrow cannot turn into Python values.
        out["values"] = None
    return out


class Engine:
    """One session on one server, with the input tables of the corpus loaded on demand."""

    def __init__(self, url: str, corpus: Corpus, log: JavaLog | None = None,
                 progress: Callable[[str], None] | None = None):
        self._corpus = corpus
        self._log = log
        self._session = open_session(url)
        self._tables: dict[str, Table | dict[str, Any]] = {}
        # Every table made, kept until the session closes: releasing a pydeephaven table from
        # the garbage collector while the session is inside another call can block the client.
        self._keep: list[Table] = []
        self._progress = progress or (lambda _text: None)
        self.calls = 0

    def close(self) -> None:
        self._session.close()

    # ------------------------------------------------------------------ errors

    def _error(self, error: BaseException, stage: str, table: Table | None = None
               ) -> dict[str, Any]:
        status, details = _status(error)
        if table is not None and status == "UNKNOWN":
            # pyarrow.flight hides the status of a failed DoGet; ask again over plain gRPC.
            status = _do_get_code(self._session, table) or "UNKNOWN"
        out: dict[str, Any] = {"status": status, "stage": stage}
        if self._log is not None:
            logged = _DETAILS_ID.search(details)
            if logged:
                message, cause = self._log.lookup(logged.group(1))
            else:
                message, cause = (details.splitlines()[0] if details else None), None
            out["message"] = message
            out["cause"] = cause
        return {"error": out}

    # ------------------------------------------------------------------ input tables

    def _base(self, fixture: Fixture) -> Table:
        if fixture.empty is not None:
            self.calls += 1
            return self._session.empty_table(fixture.empty)
        self.calls += 1
        return _import(self._session, fixture.arrow())

    def _prepared(self, case: Case) -> Table | dict[str, Any]:
        """The table `case` runs against, or the error outcome of preparing it."""
        key = case.setup_key()
        if key in self._tables:
            return self._tables[key]
        fixture = self._corpus.fixtures[case.fixture]
        try:
            table = self._base(fixture)
            self._keep.append(table)
            for op, strings in case.setup:
                self.calls += 1
                table = getattr(table, op)(strings)
                self._keep.append(table)
            if fixture.empty is not None or case.setup:
                # No uploaded position column survives here for certain; number the rows.
                self.calls += 1
                table = table.update([f"{POSITION} = ii"])
                self._keep.append(table)
            self._tables[key] = table
        except Exception as error:  # noqa: BLE001 - any failure is the outcome
            self._tables[key] = self._error(error, "setup")
        return self._tables[key]

    # ------------------------------------------------------------------ cases

    def run(self, cases: Iterable[Case], expect_error: Callable[[Case], bool] = lambda _c: False
            ) -> dict[str, dict[str, Any]]:
        """Runs `cases`; returns each case's outcome by id."""
        outcomes: dict[str, dict[str, Any]] = {}
        by_table: dict[str, list[Case]] = {}
        for case in cases:
            by_table.setdefault(case.setup_key(), []).append(case)
        done = 0
        total = sum(len(group) for group in by_table.values())
        for group in by_table.values():
            table = self._prepared(group[0])
            if isinstance(table, dict):
                for case in group:
                    outcomes[case.id] = table
                done += len(group)
                continue
            alone = [c for c in group if c.kind == "update" and expect_error(c)]
            batched = [c for c in group if c.kind == "update" and not expect_error(c)]
            for case in alone:
                self._update(table, [case], outcomes)
            for start in range(0, len(batched), BATCH):
                self._update(table, batched[start:start + BATCH], outcomes)
                self._progress(f"{done + len(alone) + min(start + BATCH, len(batched))}/{total}")
            done += len(alone) + len(batched)
            for case in group:
                if case.kind == "where":
                    outcomes[case.id] = self._where(table, case)
                    done += 1
                    if done % 50 == 0:
                        self._progress(f"{done}/{total}")
        return outcomes

    def _update(self, table: Table, cases: list[Case], outcomes: dict[str, dict[str, Any]]
                ) -> None:
        formulas = [f"{RESULT}{n} = {case.formula}" for n, case in enumerate(cases)]
        self.calls += 1
        try:
            result = table.update(formulas)
            self._keep.append(result)
        except Exception as error:  # noqa: BLE001
            if len(cases) == 1:
                outcomes[cases[0].id] = self._error(error, "formula")
                return
            middle = len(cases) // 2
            self._update(table, cases[:middle], outcomes)
            self._update(table, cases[middle:], outcomes)
            return
        try:
            data = result.to_arrow()
        except Exception as error:  # noqa: BLE001
            if len(cases) == 1:
                outcomes[cases[0].id] = self._error(error, "snapshot", result)
                return
            middle = len(cases) // 2
            self._update(table, cases[:middle], outcomes)
            self._update(table, cases[middle:], outcomes)
            return
        for n, case in enumerate(cases):
            name = f"{RESULT}{n}"
            outcome = encode_column(data.schema.field(name), data.column(name))
            if "nondeterministic" in case.tags:
                del outcome["values"]
                outcome["nondeterministic"] = True
            outcomes[case.id] = outcome

    def _where(self, table: Table, case: Case) -> dict[str, Any]:
        self.calls += 1
        try:
            result = table.where([case.formula])
            self._keep.append(result)
        except Exception as error:  # noqa: BLE001
            return self._error(error, "formula")
        try:
            data = result.to_arrow()
        except Exception as error:  # noqa: BLE001
            return self._error(error, "snapshot", result)
        if "nondeterministic" in case.tags:
            return {"nondeterministic": True}
        return {"rows": data.column(POSITION).to_pylist()}
