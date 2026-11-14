"""Records what `Table#headBy` and `Table#tailBy` of the Java reference throw.

The fixtures recorded by `ops/snapshot/fixtures/record.py` go through the gRPC handler, which
shows a status code and no more, and cannot make a blink table. This script calls the
engine through the Python console of the reference server and writes the exception of each
call to `java_refusals.txt`, one line per call:

    <table> <head|tail> <rows> <group-by columns, comma separated, or -> => <exception: message | OK>

`static` is a table of the columns `A` (int) and `B` (String); `blink` is a blink time table
of the columns `Timestamp`, `A` and `B`. `lists` has a row for each `K` of 1, 2 and 3, with
the vector column `V` (`IntVector`, of 2, 1 and 1 elements) and the array columns `Arr`
(`int[]`), `SArr` (`String[]`), `Deep` (`int[][]`), `Times` (`Instant[]`), `Flags`
(`Boolean[]`) and `Bytes` (`byte[]`); `blinklists` is a blink table of `Timestamp`, `A`,
`Arr` (`int[]`) and `V` (`IntVector`). The last lines are the status codes of the gRPC
handler for a blink table.

    oracle/bin/start-java.sh 10882
    oracle/.venv/bin/python crates/oxhaven-engine/src/ops/head_by/fixtures/record_refusals.py \
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
from harness.runner import failure_code  # noqa: E402

CALLS = [
    ("static", "head", 0, ["Nope"]),
    ("static", "head", 0, ["A", "B"]),
    ("static", "head", 0, ["A", "A"]),
    ("static", "tail", 0, []),
    ("static", "tail", -1, ["A"]),
    ("static", "head", 1, ["Nope"]),
    ("static", "tail", 1, ["Nope"]),
    ("static", "head", 1, ["A", "A"]),
    ("static", "head", 1, ["A", "Nope"]),
    ("static", "head", 1, ["Nope", "Nope2"]),
    ("static", "head", 1, ["A", "B", "B"]),
    ("static", "tail", 1, ["A", "B", "Nope"]),
    ("static", "head", 1, ["Nope", "Nope2", "Nope3", "Nope4"]),
    ("static", "head", 1, ["A", "B"]),
    ("static", "tail", 1, ["B", "A"]),
    ("static", "head", 1, ["A"]),
    ("static", "tail", 9223372036854775807, []),
    ("blink", "head", 0, ["A"]),
    ("blink", "head", 1, ["Nope"]),
    ("blink", "head", 1, ["A", "A", "B", "B"]),
    ("blink", "head", 1, ["Timestamp", "A", "B"]),
    ("blink", "head", 1, ["A"]),
    ("blink", "tail", 1, []),
    ("lists", "head", 0, ["Arr"]),
    ("lists", "head", 1, ["Arr", "Arr"]),
    ("lists", "head", 1, ["K", "V", "Arr", "SArr", "Deep", "Times", "Flags", "Bytes"]),
    ("lists", "head", 1, ["Arr"]),
    ("lists", "tail", 1, ["SArr", "Arr"]),
    ("lists", "head", 1, ["K", "Arr", "SArr"]),
    ("lists", "head", 1, ["Deep"]),
    ("lists", "head", 1, ["Times"]),
    ("lists", "head", 1, ["Flags"]),
    ("lists", "head", 1, ["Bytes"]),
    ("lists", "head", 1, ["K"]),
    ("lists", "head", 1, ["V"]),
    ("lists", "head", 1, ["V", "Arr"]),
    ("blinklists", "head", 1, ["Arr"]),
    ("blinklists", "head", 1, ["V"]),
]

SCRIPT = """
from deephaven import new_table as _new_table, time_table as _time_table
from deephaven.column import string_col as _string_col, int_col as _int_col

_refusal_tables = dict(
    static=_new_table([_int_col("A", [1, 1, 2]), _string_col("B", ["x", "y", "x"])]),
    blink=_time_table("PT1S", blink_table=True).update(["A=1", "B=`x`"]),
    lists=_new_table([_int_col("K", [1, 1, 2, 3]), _int_col("V", [1, 2, 3, 4])])
    .group_by(["K"])
    .update([
        "Arr = new int[]{{K}}",
        "SArr = new String[]{{`x`}}",
        "Deep = new int[][]{{new int[]{{K}}}}",
        "Times = new java.time.Instant[]{{null}}",
        "Flags = new Boolean[]{{true}}",
        "Bytes = new byte[]{{(byte) 1}}",
    ]),
    blinklists=_time_table("PT1S", blink_table=True).update(
        ["A=1", "Arr = new int[]{{A}}", "V = new io.deephaven.vector.IntVectorDirect(A)"]
    ),
)
refusals_blink = _refusal_tables["blink"]

def _refusal(table, kind, rows, by):
    j_table = _refusal_tables[table].j_table
    try:
        if kind == "head":
            j_table.headBy(rows, *by)
        else:
            j_table.tailBy(rows, *by)
        return "OK"
    except Exception as error:
        return str(error).split("\\n")[0].strip()

_refusal_lines = [_refusal(*call) for call in {calls!r}]
refusals = _new_table([_string_col("Refusal", _refusal_lines)])
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", required=True, help="the Java reference, dh+psk://<key>@host:port")
    arguments = parser.parse_args()
    session = open_session(arguments.url)
    try:
        session.run_script(SCRIPT.format(calls=CALLS))
        answers = session.open_table("refusals").to_arrow().column("Refusal").to_pylist()
        lines = [
            f"{table} {kind} {rows} {','.join(by) or '-'} => {answer}"
            for (table, kind, rows, by), answer in zip(CALLS, answers)
        ]
        blink = session.open_table("refusals_blink")
        for kind, rows, by in (("head", 1, ["A"]), ("tail", 0, ["A"]), ("head", 1, ["A", "A"])):
            try:
                (blink.head_by if kind == "head" else blink.tail_by)(rows, by)
                code = "OK"
            except Exception as error:  # noqa: BLE001 - classified just below
                code = failure_code(error)
                if code is None:
                    raise
            lines.append(f"grpc-blink {kind} {rows} {','.join(by)} => {code}")
    finally:
        session.close()
    (HERE / "java_refusals.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"recorded {len(lines)} calls")


if __name__ == "__main__":
    main()
