"""Records what the Java reference server answers for the count-where operators of
`update_by`: the cumulative one and the rolling one over rows and over time.

No Java counterpart: this produces java_update_by_count_where.json, which the test
`ops/update_by/corpus.rs` compares Oxhaven's `update_by` against. It has the sections of
java_update_by_group.json (`static`, `long`, `timed`, `stamped`, `ticking`), over the same
tables, and is recorded the same way:

    PYTHONPATH=. .venv/bin/python ../crates/oxhaven-engine/src/ops/update_by/fixtures/record_java_update_by_count_where.py \
        dh+psk://oxhaven@127.0.0.1:10920

A count-where operation has a `result`, the name of its one result column, and `filters`.

Not recorded, because what Java answers is not a function of the table: a count-where
without filters, which counts the flags a pooled chunk was left with, and a comparison
with `i`, `ii` or `k` on one side (`I > ii`), which Java evaluates by the positions in the
chunk at hand.

Never add a case in which a comparison of two columns (`L < L2`, which Java makes a
formula inside a range filter) stands before the first filter that reads an `Instant` or
a `Boolean` column, or reads one itself (`T >= TB`; `L < L2` then `B == true`): the Java
server throws a NullPointerException on a worker thread and shuts down.
"""

import gc
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import record_java_update_by as base  # noqa: E402
import record_java_update_by_rest as rest  # noqa: E402
import record_java_update_by_group as group  # noqa: E402

from harness.connect import open_session  # noqa: E402

HERE = Path(__file__).resolve().parent


def cum(result, filters):
    return {"op": "cum_count_where", "columns": [], "result": result, "filters": filters}


def rolling(result, filters, rev, fwd, ts=None):
    op = {"op": "rolling_count_where", "columns": [], "result": result, "filters": filters, "rev": rev, "fwd": fwd}
    if ts is not None:
        op["ts"] = ts
    return op


# One list of filters per kind of filter and column type of the `static` table.
STATIC_FILTERS = [
    ["I > 0"],
    ["I >= 2", "D < 3"],
    ["I + 1 > 1"],
    ["isNull(I)"],
    ["I == null"],
    ["I != null", "I % 2 == 0"],
    ["Y > 0"],
    ["Y in 0, 127"],
    ["H <= 7"],
    ["C > 'A'"],
    ["C == 'a' || C == 'A'"],
    ["L > 5"],
    ["L < L2"],
    ["L2 * 2 > I"],
    ["F > 1.5"],
    ["F >= 2.0f"],
    ["D >= 2.5"],
    ["D != 2.0"],
    ["DS > 0"],
    ["DS == DS"],
    ["isNaN(DS)"],
    ["FS < 1"],
    ["DI > 4"],
    ["S in `a`, `b`"],
    ["S icase in `B`, `z`"],
    ["S not in `a`"],
    ["S > `a`"],
    ["S != null && S.length() > 0"],
    ["S.contains(`a`)"],
    ["B == true"],
    ["B == false"],
    ["B != null && B"],
    ["B"],
    ["B == null", "B"],
    ["T > '2023-01-01T00:00:00Z'"],
    ["T != null && epochNanos(T) > 5"],
    ["T == null"],
    ["K == `a`"],
    ["K in `a`, `c`", "I > 0"],
    ["KD > 0"],
    ["true"],
    ["false"],
    ["1 == 1"],
    ["I > 0", "D > 0", "S != null"],
    ["I > 0 && D > 0 || S == null"],
]


def static_cases():
    cases = []

    def case(ops, by=()):
        cases.append({"ops": ops, "by": list(by)})

    for filters in STATIC_FILTERS:
        for by in ((), ("K",)):
            case([cum("R", filters)], by)
            case([rolling("R", filters, 3, 1)], by)
    for rev, fwd in base.WINDOWS + [(0, 0), (2, -2), (0, 1), (1, -1), (-3, 2), (2, -3)]:
        if (rev, fwd) != (3, 1):
            for by in ((), ("K",), ("K", "KD")):
                case([rolling("R", ["I > 0"], rev, fwd)], by)
                case([rolling("R", ["S != null", "D + 1 > 1.5"], rev, fwd)], by)
    # Buckets on a double column, on two columns, and on a column the filters read.
    for by in (("KD",), ("K", "KD"), ("B",), ("I",), ("T",)):
        case([cum("R", ["I > 0"]), rolling("N", ["D > 1"], 2, 0)], by)
    # A result that replaces a column: one the filters read, a bucket column, another.
    case([cum("I", ["I > 0"])])
    case([rolling("I", ["I > 0"], 2, 1)], ("K",))
    case([cum("K", ["I > 0"])], ("K",))
    case([cum("S", ["I > 0"]), rolling("D", ["D > 1"], 2, 0)])
    # The column pairs of the operation are not used.
    case([{**cum("R", ["I > 0"]), "columns": ["Q = Nope", "D"]}])
    # With other operations, sharing a window or not; a filter that reads the result of
    # another operation reads the source column of that name.
    case([
        cum("CW", ["I > 0"]),
        {"op": "cum_sum", "columns": ["I", "CS = L2"]},
        rolling("RW", ["I > 0", "D > 0"], 3, 0),
        {"op": "rolling_sum", "columns": ["RS = I"], "rev": 3, "fwd": 0},
        rolling("RW2", ["S in `a`, `b`"], 2, 2),
        cum("CW2", ["D > 1"]),
        {"op": "rolling_count", "columns": ["RC = S"], "rev": 2, "fwd": 2},
    ], ("K",))
    # A window that holds no row fails every rolling operator but the group.
    case([rolling("R", ["I > 0"], 0, 0), group.group(["G = I"], 0, 0)])
    # Requests Java refuses.
    case([cum("R", ["i > 3"])])
    case([cum("R", ["I + 0 > ii"])])
    case([cum("R", ["k > 3"])])
    case([rolling("R", ["i > 3"], 2, 0)])
    case([cum("R", ["I_[i-1] > 3"])])
    case([cum("R", ["I_.size() > 3"])])
    case([rolling("R", ["I_.size() > 3"], 2, 0)])
    case([cum("R", ["Nope > 3"])])
    case([cum("R", ["I > 0", "Nope == 1"])])
    case([cum("R", ["I + 1"])])
    case([cum("R", ["(Boolean)null"])])
    case([cum("R", ["B && true"])])
    case([cum("R", ["I > 0", "B"])])
    case([cum("R", ["I > "])])
    case([cum("R", ["I > 0"]), cum("R", ["I > 1"])])
    case([cum("R", ["I > 0"]), {"op": "cum_sum", "columns": ["R = I"]}])
    case([cum("R", ["I > 0"]), rolling("R", ["I > 0"], 2, 0)])
    case([cum("R", ["I > 0"])], ("Nope",))
    return cases


def long_cases():
    cases = []
    for filters in (["IR > 0"], ["DR > 1", "FR < 5"], ["LR % 2 == 0"], ["isNull(DZ)"]):
        for by in ((), ("K",)):
            cases.append({"ops": [cum("R", filters)], "by": list(by)})
            for rev, fwd in rest.LONG_WINDOWS:
                cases.append({"ops": [rolling("R", filters, rev, fwd)], "by": list(by)})
    return cases


TIMED_FILTERS = [
    ["I > 0"],
    ["D > 0", "I != null"],
    ["S in `a`, `zz`"],
    ["B == true"],
    ["isNull(L)"],
    ["DS > 0 || isNaN(DS)"],
    ["TS > '2023-01-01T00:00:00Z'"],
    ["TL > 50"],
    ["TL + 0 > 50", "I > 0"],
    ["I < H"],
    ["B == true", "T >= TB"],
    ["true"],
]


def timed_cases():
    cases = []

    def case(ops, by=()):
        cases.append({"ops": ops, "by": list(by)})

    for rev, fwd in base.TIME_WINDOWS + [(0, 3), (3, -3)]:
        for filters in TIMED_FILTERS:
            for by in ((), ("K",)):
                case([rolling("R", filters, rev, fwd, "TS")], by)
    for filters in TIMED_FILTERS:
        for by in ((), ("K",)):
            case([cum("R", filters)], by)
            case([rolling("R", filters, 4, 2)], by)
    # Other timestamp columns: a long, one that only rises within buckets, one that falls.
    for ts in ("TL", "TB", "TD", "TI", "Nope"):
        for by in ((), ("K",)):
            case([rolling("R", ["I > 0"], 5, 2, ts)], by)
            case([rolling("R", [f"{ts} != null"], 5, 2, ts)], by)
    # A result that replaces the timestamp column, and one that replaces a filter column.
    case([rolling("TS", ["I > 0"], 5, 0, "TS")])
    case([rolling("TL", ["TL > 30"], 5, 0, "TL")], ("K",))
    case([rolling("I", ["I > 0"], 5, 5, "TS")], ("K",))
    case([
        rolling("RA", ["I > 0", "DS > 0"], 5, 5, "TS"),
        {"op": "rolling_sum", "columns": ["RS = I"], "ts": "TS", "rev": 5, "fwd": 5},
        rolling("RB", ["S != null"], 5, 5, "TS"),
        rolling("RC", ["B == true"], 3, 0, "TS"),
        rolling("RD", ["I > 0"], 3, 0),
        cum("CA", ["TS != null"]),
        {"op": "ema", "columns": ["EA = I"], "decay": 3},
    ], ("K",))
    # Requests Java refuses.
    case([rolling("R", ["I > 0"], 5, 0, "TS"), rolling("Q", ["I > 0"], 5, 0, "TL")])
    case([rolling("R", ["I > 0"], -5, 2, "TS")])
    case([rolling("R", ["I > 0"], 5, -6, "TS")])
    case([rolling("R", ["B"], 5, 0, "TS")])
    return cases


TICKING_OPS = [
    cum("CaI", ["I > 0"]),
    cum("CbD", ["D > 0", "S in `a`, `b`, `c`"]),
    cum("CcF", ["I != null && L != null && I + L > 0"]),
    rolling("RaI", ["I > 0"], 3, 0),
    rolling("RbS", ["S != null", "D >= 0"], 2, 2),
    rolling("RcL", ["L < 0 || L == null"], -1, 3),
    rolling("RdK", ["K == `a`"], 1, 1),
    {"op": "rolling_sum", "columns": ["RsI = I"], "rev": 3, "fwd": 0},
    {"op": "cum_sum", "columns": ["CsI = I"]},
]

TIMED_TICKING_OPS = [
    rolling("RaI", ["I > 0"], 3000, 0, "Ts"),
    rolling("RbD", ["D > 0", "I != null"], 2000, 2000, "Ts"),
    rolling("RcI", ["I < 0 || isNaN(D)"], -1000, 4000, "Ts"),
    rolling("RdT", ["Ts != null"], 0, 0, "Ts"),
    rolling("ReI", ["I > 0"], 2, 1),
    cum("CaI", ["I > 0", "Ts != null"]),
    {"op": "rolling_sum", "columns": ["RsI = I"], "ts": "Ts", "rev": 3000, "fwd": 0},
]


def record_ticking(session, name, seed, steps, by, timed=False, make_row=None):
    group.TICKING_OPS, group.TIMED_TICKING_OPS = TICKING_OPS, TIMED_TICKING_OPS
    return group.record_ticking(session, name, seed, steps, by, timed, make_row)


def main():
    gc.disable()
    session = open_session(sys.argv[1])
    sections = {
        "static": base.record_static(session, base.STATIC, static_cases()),
        "long": base.record_static(session, base.LONG, long_cases()),
        "timed": base.record_static(session, base.TIMED, timed_cases()),
        "stamped": base.record_static(session, group.STAMPED, timed_cases()),
    }
    ticking = [
        record_ticking(session, "whole", 41, 30, []),
        record_ticking(session, "bucketed", 42, 40, ["K"]),
        record_ticking(session, "timed", 44, 30, [], timed=True),
        record_ticking(session, "timed_bucketed", 45, 40, ["K"], timed=True),
        record_ticking(session, "stamped_bucketed", 47, 40, ["K"], timed=True, make_row=group.stamped_row),
    ]
    with open(HERE / "java_update_by_count_where.json", "w", encoding="utf-8") as out:
        out.write("{\n")
        for name, section in sections.items():
            out.write(f'"{name}": {{\n"columns": [\n')
            out.write(",\n".join(json.dumps(c) for c in section["columns"]))
            out.write('\n],\n"cases": [\n')
            out.write(",\n".join(json.dumps(c) for c in section["cases"]))
            out.write("\n]\n},\n")
        out.write('"ticking": [\n')
        out.write(",\n".join(json.dumps(t) for t in ticking))
        out.write("\n]\n}\n")
    for name, section in sections.items():
        refused = sum(1 for case in section["cases"] if case["result"] is None)
        print(f"{name}: {len(section['cases'])} cases recorded, {refused} of them refused by Java")
    for scenario in ticking:
        failed = "failed" in scenario["steps"][-1]
        print(
            f"ticking {scenario['name']}: {len(scenario['steps'])} steps, failed at the end: {failed}, "
            f"{scenario['stale']} cells differ from the static recompute"
        )
    sys.stdout.flush()
    # Leaving normally releases the session's tables from `__del__`, which can wait forever.
    os._exit(0)


if __name__ == "__main__":
    main()
