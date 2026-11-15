"""Records what the Java reference server answers for the `update_by` operators that
`record_java_update_by.py` does not cover: the rolling standard deviation, the rolling
weighted average and the exponential moving standard deviation.

No Java counterpart: this produces java_update_by_rest.json, which the test
`ops/update_by/corpus.rs` compares Oxhaven's `update_by` against. It has the sections of
java_update_by.json, over the same tables, and is recorded the same way:

    PYTHONPATH=. .venv/bin/python ../crates/oxhaven-engine/src/ops/update_by/fixtures/record_java_update_by_rest.py \
        dh+psk://oxhaven@127.0.0.1:10900
"""

import gc
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import record_java_update_by as base  # noqa: E402
from pydeephaven import updateby as ub  # noqa: E402
from pydeephaven.updateby import OperationControl  # noqa: E402

from harness.connect import open_session  # noqa: E402

HERE = Path(__file__).resolve().parent
NUMERIC = ("Y", "H", "C", "I", "L", "L2", "F", "D", "FS", "DS", "DI")
LONG_WINDOWS = base.LONG_WINDOWS + [(150, 100), (129, 0)]

base_make_op = base.make_op


def make_op(op):
    """The pydeephaven operation for one recorded operation."""
    kind, cols = op["op"], op["columns"]
    if kind == "rolling_wavg":
        if "ts" in op:
            return ub.rolling_wavg_time(op["ts"], op["weight"], cols, rev_time=op["rev"], fwd_time=op["fwd"])
        return ub.rolling_wavg_tick(op["weight"], cols, rev_ticks=op["rev"], fwd_ticks=op["fwd"])
    if kind == "emstd":
        control = None
        if "on_null" in op or "on_nan" in op:
            control = OperationControl(
                on_null=base.BEHAVIOR[op.get("on_null", "skip")], on_nan=base.BEHAVIOR[op.get("on_nan", "skip")]
            )
        if "ts" in op:
            return ub.emstd_time(op["ts"], op["decay"], cols, control)
        return ub.emstd_tick(op["decay"], cols, control)
    return base_make_op(op)


base.make_op = make_op


def static_cases():
    cases = []

    def case(ops, by=()):
        cases.append({"ops": ops, "by": list(by)})

    for column in base.VALUE_COLUMNS:
        for by in ((), ("K",)):
            case([{"op": "rolling_std", "columns": [f"R = {column}"], "rev": 3, "fwd": 1}], by)
            case([{"op": "rolling_wavg", "columns": [f"R = {column}"], "weight": "L2", "rev": 3, "fwd": 1}], by)
            case([{"op": "emstd", "columns": [f"R = {column}"], "decay": 2.5}], by)
    for column in NUMERIC:
        for rev, fwd in base.WINDOWS:
            if (rev, fwd) != (3, 1):
                case([{"op": "rolling_std", "columns": [f"R = {column}"], "rev": rev, "fwd": fwd}])
                case([{"op": "rolling_wavg", "columns": [f"R = {column}"], "weight": "D", "rev": rev, "fwd": fwd}])
    # Every type of weight, with an integer and a floating point input.
    for weight in base.VALUE_COLUMNS:
        for column in ("I", "D", "F"):
            case([{"op": "rolling_wavg", "columns": [f"R = {column}"], "weight": weight, "rev": 4, "fwd": 0}], ("K",))
    # No columns: every column the operation applies to.
    case([{"op": "rolling_std", "columns": [], "rev": 2, "fwd": 0}], ("K", "KD"))
    case([{"op": "rolling_std", "columns": [], "rev": 2, "fwd": 0}])
    case([{"op": "rolling_wavg", "columns": [], "weight": "L2", "rev": 2, "fwd": 0}], ("K", "KD"))
    case([{"op": "rolling_wavg", "columns": [], "weight": "D", "rev": 2, "fwd": 0}])
    case([{"op": "emstd", "columns": [], "decay": 3}], ("K",))
    case([{"op": "emstd", "columns": [], "decay": 3}])
    # With other operations, sharing a window or not; a result that replaces its input or
    # the weight.
    case([
        {"op": "rolling_std", "columns": ["SD = D", "I"], "rev": 3, "fwd": 0},
        {"op": "rolling_sum", "columns": ["RS = D"], "rev": 3, "fwd": 0},
        {"op": "rolling_wavg", "columns": ["WA = D", "L2"], "weight": "L2", "rev": 3, "fwd": 0},
        {"op": "rolling_wavg", "columns": ["WB = I"], "weight": "F", "rev": 2, "fwd": 2},
        {"op": "emstd", "columns": ["ES = D", "F"], "decay": 2},
        {"op": "ema", "columns": ["EA = D"], "decay": 2},
        {"op": "cum_sum", "columns": ["CS = I"]},
    ], ("K",))
    # Requests Java refuses.
    case([{"op": "rolling_std", "columns": ["R = Nope"], "rev": 3, "fwd": 0}])
    case([{"op": "rolling_std", "columns": ["R = I"], "rev": -3, "fwd": 2}])
    case([{"op": "rolling_std", "columns": ["R = I"], "rev": 0, "fwd": 0}])
    case([{"op": "rolling_wavg", "columns": ["R = I"], "weight": "Nope", "rev": 3, "fwd": 0}])
    case([{"op": "rolling_wavg", "columns": ["R = Nope"], "weight": "I", "rev": 3, "fwd": 0}])
    case([{"op": "rolling_wavg", "columns": ["R = S"], "weight": "S", "rev": 3, "fwd": 0}])
    case([{"op": "rolling_wavg", "columns": ["R = I"], "weight": "I", "rev": 0, "fwd": 0}])
    case([{"op": "emstd", "columns": ["R = Nope"], "decay": 2}])
    return cases


def long_cases():
    cases = []
    for column in ("DR", "DZ", "FR", "IR", "LR"):
        for rev, fwd in LONG_WINDOWS:
            for by in ((), ("K",)):
                cases.append({"ops": [{"op": "rolling_std", "columns": [f"R = {column}"], "rev": rev, "fwd": fwd}], "by": list(by)})
                for weight in ("DZ", "IR", "FR"):
                    op = {"op": "rolling_wavg", "columns": [f"R = {column}"], "weight": weight, "rev": rev, "fwd": fwd}
                    cases.append({"ops": [op], "by": list(by)})
        for decay in (1.0, 7.5, 0.25):
            for by in ((), ("K",)):
                cases.append({"ops": [{"op": "emstd", "columns": [f"R = {column}"], "decay": decay}], "by": list(by)})
    return cases


def timed_cases():
    cases = []

    def case(ops, by=()):
        cases.append({"ops": ops, "by": list(by)})

    for rev, fwd in base.TIME_WINDOWS:
        for column in base.TIMED_VALUES:
            for by in ((), ("K",)):
                case([{"op": "rolling_std", "columns": [f"R = {column}"], "ts": "TS", "rev": rev, "fwd": fwd}], by)
                case([{"op": "rolling_wavg", "columns": [f"R = {column}"], "weight": "D", "ts": "TS", "rev": rev, "fwd": fwd}], by)
        case([{"op": "rolling_wavg", "columns": ["R = D", "Q = I"], "weight": "H", "ts": "TS", "rev": rev, "fwd": fwd}], ("K",))
    for decay in base.EM_DECAYS:
        for column in base.TIMED_VALUES:
            for by in ((), ("K",)):
                case([{"op": "emstd", "columns": [f"R = {column}"], **decay}], by)
    for on_null, on_nan in (
        ("reset", "skip"), ("skip", "reset"), ("skip", "poison"), ("poison", "poison"),
        ("throw", "skip"), ("skip", "throw"), ("reset", "reset"), ("reset", "poison"),
    ):
        for column in ("D", "DS", "FS", "I"):
            for decay in ({"decay": 2.0}, {"decay": 10, "ts": "TS"}):
                for by in ((), ("K",)):
                    case([{"op": "emstd", "columns": [f"R = {column}"], "on_null": on_null, "on_nan": on_nan, **decay}], by)
    # Other timestamp columns: a long, one that only rises within buckets, one that falls.
    for ts in ("TL", "TB", "TD", "TI", "Nope"):
        for by in ((), ("K",)):
            case([{"op": "rolling_std", "columns": ["R = I"], "ts": ts, "rev": 5, "fwd": 2}], by)
            case([{"op": "rolling_wavg", "columns": ["R = I"], "weight": "D", "ts": ts, "rev": 5, "fwd": 2}], by)
            case([{"op": "emstd", "columns": ["R = D"], "ts": ts, "decay": 4}], by)
    # No columns; the timestamp column as an input and as the weight.
    case([{"op": "emstd", "columns": [], "ts": "TL", "decay": 4}], ("K",))
    case([{"op": "rolling_std", "columns": [], "ts": "TL", "rev": 4, "fwd": 0}], ("K",))
    case([{"op": "rolling_wavg", "columns": [], "weight": "I", "ts": "TL", "rev": 4, "fwd": 0}], ("K",))
    case([{"op": "emstd", "columns": ["R = TL"], "ts": "TL", "decay": 4}])
    case([{"op": "rolling_std", "columns": ["R = TL", "E = I"], "ts": "TL", "rev": 4, "fwd": 0}])
    case([{"op": "rolling_wavg", "columns": ["R = I"], "weight": "TL", "ts": "TL", "rev": 4, "fwd": 0}])
    case([
        {"op": "emstd", "columns": ["E1 = DS", "E2 = I"], "ts": "TS", "decay": 5},
        {"op": "ema", "columns": ["E3 = DS"], "ts": "TS", "decay": 5},
        {"op": "emstd", "columns": ["E4 = DS"], "ts": "TS", "decay": 3, "on_nan": "poison"},
        {"op": "rolling_std", "columns": ["RD = D"], "ts": "TS", "rev": 5, "fwd": 5},
        {"op": "rolling_wavg", "columns": ["RW = D"], "weight": "I", "ts": "TS", "rev": 5, "fwd": 5},
        {"op": "emstd", "columns": ["E5 = D"], "decay": 2.5},
    ], ("K",))
    return cases


TICKING_OPS = [
    {"op": "rolling_std", "columns": ["SdI = I", "SdD = D"], "rev": 3, "fwd": 0},
    {"op": "rolling_std", "columns": ["SeL = L"], "rev": 2, "fwd": 2},
    {"op": "rolling_std", "columns": ["SfD = D"], "rev": -1, "fwd": 3},
    {"op": "rolling_wavg", "columns": ["WaI = I", "WaD = D"], "weight": "L", "rev": 3, "fwd": 0},
    {"op": "rolling_wavg", "columns": ["WbL = L"], "weight": "D", "rev": 2, "fwd": 2},
    {"op": "emstd", "columns": ["EsI = I", "EsD = D"], "decay": 3},
    {"op": "emstd", "columns": ["EtD = D"], "decay": 1.5, "on_null": "reset"},
    {"op": "rolling_sum", "columns": ["RsI = I"], "rev": 3, "fwd": 0},
]

TIMED_TICKING_OPS = [
    {"op": "rolling_std", "columns": ["SdI = I", "SdD = D"], "ts": "Ts", "rev": 3000, "fwd": 0},
    {"op": "rolling_std", "columns": ["SeD = D"], "ts": "Ts", "rev": 2000, "fwd": 2000},
    {"op": "rolling_wavg", "columns": ["WaD = D"], "weight": "I", "ts": "Ts", "rev": 3000, "fwd": 0},
    {"op": "rolling_wavg", "columns": ["WbI = I"], "weight": "D", "ts": "Ts", "rev": -1000, "fwd": 4000},
    {"op": "emstd", "columns": ["EsI = I", "EsD = D"], "ts": "Ts", "decay": 2500},
    # Not poisoned by NaN in time: Java refuses the value after a NaN that came first.
    {"op": "emstd", "columns": ["EtD = D"], "ts": "Ts", "decay": 1500, "on_null": "reset"},
    {"op": "emstd", "columns": ["EuD = D"], "decay": 2, "on_nan": "reset"},
    {"op": "emstd", "columns": ["EvD = D"], "decay": 4, "on_nan": "poison"},
    {"op": "ema", "columns": ["EaD = D"], "ts": "Ts", "decay": 2500},
]

base.TICKING_OPS = TICKING_OPS
base.TIMED_TICKING_OPS = TIMED_TICKING_OPS


def main():
    gc.disable()
    session = open_session(sys.argv[1])
    sections = {
        "static": base.record_static(session, base.STATIC, static_cases()),
        "long": base.record_static(session, base.LONG, long_cases()),
        "timed": base.record_static(session, base.TIMED, timed_cases()),
    }
    ticking = [
        base.record_ticking(session, "whole", 21, 30, []),
        base.record_ticking(session, "bucketed", 22, 40, ["K"]),
        base.record_ticking(session, "timed", 24, 30, [], timed=True),
        base.record_ticking(session, "timed_bucketed", 25, 40, ["K"], timed=True),
    ]
    with open(HERE / "java_update_by_rest.json", "w", encoding="utf-8") as out:
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
    print(f"{len(ticking)} ticking scenarios recorded")


if __name__ == "__main__":
    main()
