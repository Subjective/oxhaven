"""Records `Table#headBy` and `Table#tailBy` of a table with vector and array columns.

The fixtures recorded by `ops/snapshot/fixtures/record.py` have no such columns. This script
makes a table of them in the Python console of the reference server and writes it and its
results to `java_lists.txt`. A table is a line with its name and its column names, a line
with the Java classes of its columns, and a line for each row, the cells separated by `|`
and the elements of a vector or an array in brackets:

    <source | head|tail <rows> <group-by columns, comma separated>>: <column>,...
    <class>,...
    <cell>|...

The source has the columns `A` (int), `B` (String), `C` (`IntVector`), `Arr` (`int[]`), `K`
(int) and `SArr` (`String[]`).

    oracle/bin/start-java.sh 10882
    oracle/.venv/bin/python crates/oxhaven-engine/src/ops/head_by/fixtures/record_lists.py \
        --url dh+psk://oxhaven@127.0.0.1:10882
    oracle/bin/stop-java.sh 10882
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[5] / "oracle"))

from harness.connect import open_session  # noqa: E402

CALLS = [
    ("head", 1, ["K"]),
    ("tail", 2, ["B"]),
    ("head", 2, ["A"]),
    ("tail", 1, []),
]

SCRIPT = """
from deephaven import new_table as _new_table
from deephaven.column import string_col as _string_col, int_col as _int_col
from deephaven.table import Table as _Table

lists_source = _new_table([
    _int_col("A", [1, 1, 2, 2, 1, 3]),
    _string_col("B", ["x", "y", "x", "x", "y", "x"]),
    _int_col("C", [1, 2, 3, 4, 5, 6]),
]).group_by(["A", "B"]).update(["Arr = C.toArray()", "K = A", "SArr = new String[]{{B}}"])

def _lists_result(kind, rows, by):
    j_table = lists_source.j_table
    return _Table(j_table.headBy(rows, *by) if kind == "head" else j_table.tailBy(rows, *by))

def _lists_classes(table):
    columns = table.j_table.getDefinition().getColumnsArray()
    return ",".join(str(column.getDataType().getName()) for column in columns)

_lists_calls = {calls!r}
_lists_tables = [lists_source] + [_lists_result(*call) for call in _lists_calls]
lists_classes = _new_table([_string_col("Classes", [_lists_classes(t) for t in _lists_tables])])
"""


def cell(value):
    if isinstance(value, list):
        return "[" + ", ".join(cell(element) for element in value) + "]"
    return "null" if value is None else str(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", required=True, help="the Java reference, dh+psk://<key>@host:port")
    arguments = parser.parse_args()
    session = open_session(arguments.url)
    try:
        session.run_script(SCRIPT.format(calls=CALLS))
        classes = session.open_table("lists_classes").to_arrow().column("Classes").to_pylist()
        names = ["source"] + [f"{kind} {rows} {','.join(by) or '-'}" for kind, rows, by in CALLS]
        lines = []
        for index, name in enumerate(names):
            if index == 0:
                table = session.open_table("lists_source").to_arrow()
            else:
                session.run_script(f"lists_result = _lists_tables[{index}]")
                table = session.open_table("lists_result").to_arrow()
            lines.append(f"{name}: {','.join(table.column_names)}")
            lines.append(classes[index])
            for row in table.to_pylist():
                lines.append("|".join(cell(value) for value in row.values()))
    finally:
        session.close()
    (HERE / "java_lists.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"recorded {len(names)} tables")


if __name__ == "__main__":
    main()
