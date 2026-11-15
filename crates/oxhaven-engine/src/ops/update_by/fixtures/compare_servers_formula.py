"""Sends the rolling formula requests of record_java_update_by_formula.py to two servers
through gRPC and compares what they answer: the result tables cell by cell, or the status
code and message of the refusal (an error id in a message is not compared). The requests
of its `unported` section are sent to the second server only, which is
expected to answer `UNIMPLEMENTED` or to refuse the formula; they are counted.

No Java counterpart. Run it from the `oracle/` directory against a Java reference server
and an Oxhaven server:

    PYTHONPATH=. .venv/bin/python ../crates/oxhaven-engine/src/ops/update_by/fixtures/compare_servers_formula.py \
        dh+psk://oxhaven@127.0.0.1:10920 dh+psk://oxhaven@127.0.0.1:10921

It prints one line per difference and a count, and exits with 1 if there is a difference.
"""

import gc
import math
import os
import re
import sys
from pathlib import Path

import grpc
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parent))

import record_java_update_by as base  # noqa: E402
import record_java_update_by_group as group  # noqa: E402
import record_java_update_by_formula as recorder  # noqa: E402

from harness.connect import open_session  # noqa: E402

UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def refusal(error):
    """The status code and message under a pydeephaven error."""
    while error is not None:
        if isinstance(error, grpc.RpcError):
            return f"{error.code().name}: {UUID.sub('<id>', error.details() or '')}"
        error = error.__cause__ or error.__context__
    return "no status"


def same(a, b):
    if isinstance(a, float) and isinstance(b, float):
        return (math.isnan(a) and math.isnan(b)) or (a == b and math.copysign(1, a) == math.copysign(1, b))
    return a == b


def answer(source, ops, by):
    try:
        table = source.update_by([recorder.make_op(op) for op in ops], by)
        result = table.to_arrow()
        del table
    except Exception as error:  # noqa: BLE001
        return refusal(error)
    finally:
        gc.collect()
    columns = {}
    for name in result.schema.names:
        field = result.schema.field(name)
        java = (field.metadata or {}).get(b"deephaven:type", b"").decode()
        columns[name] = (java, result.column(name).to_pylist())
    return columns


def compare(label, a, b):
    if isinstance(a, str) or isinstance(b, str):
        if a != b:
            return [f"{label}: {a if isinstance(a, str) else 'a table'} / {b if isinstance(b, str) else 'a table'}"]
        return []
    if list(a) != list(b):
        return [f"{label}: columns {list(a)} / {list(b)}"]
    out = []
    for name in a:
        (java_a, values_a), (java_b, values_b) = a[name], b[name]
        if java_a != java_b:
            out.append(f"{label}: {name} is {java_a} / {java_b}")
        elif len(values_a) != len(values_b) or not all(same(x, y) for x, y in zip(values_a, values_b)):
            out.append(f"{label}: {name} is {values_a} / {values_b}")
    return out


def main():
    gc.disable()
    sessions = [open_session(url) for url in sys.argv[1:3]]
    sections = [
        ("static", base.STATIC, recorder.static_cases()),
        ("validated", base.STATIC, recorder.validated_cases()),
        ("long", base.LONG, recorder.long_cases()),
        ("timed", base.TIMED, recorder.timed_cases()),
        ("stamped", group.STAMPED, recorder.timed_cases()),
    ]
    differences, answered, refused = [], 0, 0
    for name, table, cases in sections:
        arrays = [base.arrow_column(kind, values) for _, kind, values in table]
        data = pa.table(arrays, names=[n for n, _, _ in table])
        sources = [session.import_table(data) for session in sessions]
        for case in cases:
            a, b = (answer(source, case["ops"], case["by"]) for source in sources)
            if isinstance(a, str):
                refused += 1
            else:
                answered += 1
            differences += compare(f"{name} {case['ops']} by {case['by']}", a, b)
    unported = 0
    data = pa.table([base.arrow_column(kind, values) for _, kind, values in base.STATIC], names=[n for n, _, _ in base.STATIC])
    source = sessions[1].import_table(data)
    for case in recorder.unported_cases():
        b = answer(source, case["ops"], case["by"])
        if isinstance(b, str) and (b.startswith("UNIMPLEMENTED: not ported") or b.startswith("INVALID_ARGUMENT: Details Logged")):
            unported += 1
        else:
            differences.append(f"unported {case['ops']} by {case['by']}: the second server answers {b if isinstance(b, str) else 'a table'}")
    for difference in differences:
        print(difference)
    print(f"{unported} unported requests refused by the second server")
    print(f"{answered} requests answered and {refused} refused by the first server; {len(differences)} differences")
    sys.stdout.flush()
    os._exit(1 if differences else 0)


if __name__ == "__main__":
    main()
