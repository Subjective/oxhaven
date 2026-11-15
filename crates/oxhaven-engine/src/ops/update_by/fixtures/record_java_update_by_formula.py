"""Records what the Java reference server answers for the rolling formula of `update_by`,
over rows and over time, in both of its forms.

No Java counterpart: this produces java_update_by_formula.json, which the test
`ops/update_by/corpus.rs` compares Oxhaven's `update_by` against. It has the sections of
java_update_by_group.json (`static`, `long`, `timed`, `stamped`, `ticking`), over the same
tables, and is recorded the same way:

    PYTHONPATH=. .venv/bin/python ../crates/oxhaven-engine/src/ops/update_by/fixtures/record_java_update_by_formula.py \
        dh+psk://oxhaven@127.0.0.1:10920

A rolling formula operation has a `formula` and either a `param`, the token that stands
for the input column of each of its `columns`, or none: then the formula is
`Result = expression` over the columns of the table, and `columns` is not used. Where
the operation has no `columns`, `results` names the result columns to record.

One more section, `unported`, holds cases over the `static` table that the test does not
compare: formulas that call a function Oxhaven's expression crate refuses by name, and
formulas that take an element or a part of a window (`get`, an index, `subVector`), which
Oxhaven leaves unported. They are recorded so that the test notices when one of them is
no longer refused.

`validated_cases()` are not recorded: requests the Java server refuses before its engine
sees them, in `UpdateByGrpcImpl#validateFormulas`, which the engine alone would answer.
With a token, 42.9 validates `formula.replaceAll(token, input column)`: the token is
replaced wherever it occurs, also inside a name (`max(x)` becomes `maI(I)`). That is why
the cases here use the token `q`, which no function name holds.
`compare_servers_formula.py` sends every case, those too, to two servers.
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
from pydeephaven import updateby as ub  # noqa: E402

from harness.connect import open_session  # noqa: E402

HERE = Path(__file__).resolve().parent

group_make_op = group.make_op
group_outputs_of = group.outputs_of


def make_op(op):
    """The pydeephaven operation for one recorded operation."""
    if op["op"] != "rolling_formula":
        return group_make_op(op)
    if "ts" in op:
        return ub.rolling_formula_time(
            op["ts"], op["formula"], op.get("param"), op["columns"], rev_time=op["rev"], fwd_time=op["fwd"]
        )
    return ub.rolling_formula_tick(
        op["formula"], op.get("param"), op["columns"], rev_ticks=op["rev"], fwd_ticks=op["fwd"]
    )


def outputs_of(ops):
    out = set()
    for op in ops:
        if op["op"] != "rolling_formula":
            out |= group_outputs_of([op])
        elif "results" in op:
            out.update(op["results"])
        elif "param" in op:
            out.update(c.split("=")[0].strip() for c in op["columns"])
        else:
            out.add(op["formula"].split("=")[0].strip())
    return out


base.make_op = group.make_op = make_op
base.outputs_of = group.outputs_of = outputs_of


def each(formula, columns, rev, fwd, ts=None, param="q", results=None):
    """The form with a parameter token: one result per input column."""
    op = {"op": "rolling_formula", "formula": formula, "param": param, "columns": columns, "rev": rev, "fwd": fwd}
    if ts is not None:
        op["ts"] = ts
    if results is not None:
        op["results"] = results
    return op


def multi(formula, rev, fwd, ts=None, columns=()):
    """The form without a token: `Result = expression` over the table's columns."""
    op = {"op": "rolling_formula", "formula": formula, "columns": list(columns), "rev": rev, "fwd": fwd}
    if ts is not None:
        op["ts"] = ts
    return op


NUMBERS = ["Y", "H", "I", "L", "F", "D", "FS", "DS"]
# Formulas of the token `q` over a vector of numbers.
NUMERIC = [
    "sum(q)",
    "avg(q)",
    "min(q)",
    "max(q)",
    "first(q)",
    "last(q)",
    "count(q)",
    "len(q)",
    "q.size()",
    "absSum(q)",
    "product(q)",
    "median(q)",
    "std(q)",
    "var(q)",
    "countDistinct(q)",
    "sum(q) + 1",
    "avg(q) * 2 - min(q)",
    "len(q) > 2 ? max(q) : min(q)",
    "q.isEmpty()",
    "q",
    "sort(q)",
    "reverse(q)",
    "distinct(q)",
    "cumsum(q)",
    "array(q)",
]
# Formulas over a vector of any type.
ANY = ["len(q)", "q.size()", "countObj(q)", "q", "q.isEmpty()", "7", "len(q) + 0.5"]
OBJECTS = ["firstObj(q)", "lastObj(q)", "array(q)"]

# Formulas without a token, over the columns of the `static` table.
MULTI = [
    "R = sum(I)",
    "R = sum(I) + sum(L2)",
    "R = avg(D) * len(S)",
    "R = avg(D) * countObj(S)",
    "R = first(I) + last(H)",
    "R = len(K)",
    "R = max(F) > 2 ? sum(Y) : count(L)",
    "R = I",
    "R = S",
    "R = 1",
    "R = countObj(B)",
    "R = countObj(T)",
    "R = B",
    "R = firstObj(S)",
    "R = lastObj(T)",
    "R = sort(D)",
    "R = sum(I) > 3",
    "R = min(DS) + max(FS)",
    "R=std(D)",
    " R = len(C) ",
    "I = sum(I)",
    "S = len(I)",
    "I",
]
# With buckets on `K`, where `K` is a scalar in the formula.
MULTI_BY_K = [
    "R = K",
    "R = K + len(I)",
    "R = K == null ? -1 : K.length() + sum(I)",
    "R = isNull(K) ? first(I) : last(I)",
    "K = len(I)",
    "R = len(KD)",
    "K",
]


def static_cases():
    cases = []

    def case(ops, by=()):
        cases.append({"ops": ops, "by": list(by)})

    for formula in NUMERIC:
        for column in NUMBERS:
            # A `byte[]` reaches a client as Arrow `Binary`, which is not recorded.
            if column == "Y" and formula in ("sort(q)", "reverse(q)", "distinct(q)", "array(q)"):
                continue
            for by in ((), ("K",)):
                case([each(formula, [f"R = {column}"], 3, 1)], by)
    for formula in ANY:
        for column in ("C", "S", "B", "T", "K", "I"):
            # In `unported_cases`.
            if (formula, column) in (("q", "B"), ("countObj(q)", "C"), ("countObj(q)", "I")):
                continue
            for by in ((), ("KD",)):
                case([each(formula, [f"R = {column}"], 2, 1)], by)
    for formula in OBJECTS:
        for column in ("S", "T", "B"):
            case([each(formula, [f"R = {column}"], 2, 1)])
            case([each(formula, [f"R = {column}"], 2, 1)], ("K",))
    for formula in ("min(q)", "first(q)", "q", "count(q)", "sum(q)"):
        case([each(formula, ["R = C"], 2, 1)])
    for rev, fwd in base.WINDOWS + [(0, 0), (2, -2), (0, 1), (1, -1), (-3, 2), (2, -3)]:
        if (rev, fwd) != (3, 1):
            for by in ((), ("K",), ("K", "KD")):
                case([each("sum(q)", ["R = I"], rev, fwd)], by)
                case([each("avg(q)", ["R = D", "Q = F"], rev, fwd)], by)
                case([each("countObj(q)", ["R = S"], rev, fwd)], by)
                case([multi("R = sum(I) + countObj(S)", rev, fwd)], by)
    for formula in MULTI:
        for by in ((), ("KD",)):
            case([multi(formula, 3, 1)], by)
    for formula in MULTI_BY_K:
        case([multi(formula, 2, 2)], ("K",))
        case([multi(formula, 2, 2)], ("K", "KD"))
    # A formula that reads a bucket column is handed the value of the first bucket column.
    case([multi("R = KD + avg(D)", 2, 0)], ("K", "KD"))
    case([multi("R = KD + avg(D)", 2, 0)], ("KD",))
    case([multi("R = KD + avg(D)", 2, 0)], ("KD", "K"))
    case([multi("R = K", 2, 0)], ("S", "K"))
    case([multi("R = K + len(I)", 2, 0)], ("S", "K"))
    case([multi("R = L2 + sum(I)", 2, 0)], ("L", "L2"))
    case([multi("R = I + sum(L)", 2, 0)], ("Y", "I"))
    case([multi("R = B ? sum(I) : sum(L)", 2, 0)], ("B",))
    case([multi("R = B == null ? sum(I) : sum(L)", 2, 0)], ("B",))
    case([multi("R = T == null ? len(I) : 0", 2, 0)], ("T",))
    case([multi("R = I + sum(L)", 2, 0)], ("I",))
    # Other tokens; a token that is also a column, or part of a name.
    case([each("avg(each)", ["R = D"], 2, 0, param="each")])
    case([each("sum(x)", ["R = I"], 2, 0, param="x")], ("K",))
    case([each("sum(I)", ["R = L", "Q = I"], 2, 0, param="I")], ("K",))
    case([each("sum(q) + len(q) + q.size()", ["R = I"], 2, 0)])
    case([each("sum( y )+1", ["R = I"], 2, 0, param="y")])
    # No columns: every column but the bucket columns.
    names = [name for name, _, _ in base.STATIC]
    case([each("q.size()", [], 2, 0, results=[n for n in names if n not in ("K", "KD")])], ("K", "KD"))
    case([each("q.size()", [], 2, 1, results=names)])
    case([each("count(q)", [], 2, 1, results=names)])
    # Several columns of several types under one formula, a result that replaces its
    # input.
    case([each("sum(q)", ["A = I", "Bq = D", "Cq = L", "Dq = I", "Y"], 2, 1)], ("K",))
    case([each("max(q)", ["I", "D"], 2, 0)])
    # The column pairs of the form without a token are not used.
    case([multi("R = sum(I)", 2, 0, columns=["Q = Nope", "D"])])
    # With other operations, sharing a window or not.
    case([
        each("sum(q)", ["FA = I", "FB = D"], 3, 0),
        {"op": "rolling_sum", "columns": ["RS = I"], "rev": 3, "fwd": 0},
        multi("FC = sum(I) - sum(L2)", 3, 0),
        group.group(["G = I"], 3, 0),
        each("first(q)", ["FD = H"], 2, 2),
        {"op": "cum_sum", "columns": ["CS = I"]},
        multi("FE = K + countObj(S)", 2, 2),
        {"op": "cum_count_where", "columns": [], "result": "CW", "filters": ["I > 0"]},
    ], ("K",))
    # A formula that reads the result of another operation reads the source column.
    case([{"op": "cum_sum", "columns": ["I"]}, multi("R = sum(I)", 2, 0)])
    case([multi("I = len(I)", 2, 0), multi("R = sum(I)", 2, 0)])
    # A window that holds no row.
    case([each("len(q)", ["R = I"], 0, 0), group.group(["G = I"], 0, 0)])
    case([each("countObj(q)", ["R = S"], 0, 0)])
    case([each("countObj(q)", ["R = B"], 0, 0)])
    case([multi("R = len(I)", 0, 0)])
    case([multi("R = 1", 0, 0)])
    case([multi("R = K", 0, 0)], ("K",))
    for column in list(NUMBERS) + ["C"]:
        case([each("count(q)", [f"R = {column}"], 0, 0)])
    # A window of no rows that lies ahead of the row, or behind it.
    for rev, fwd in ((-1, 1), (-2, 2), (-5, 5), (3, -3), (6, -6)):
        for by in ((), ("K",)):
            case([each("sum(q)", ["R = I"], rev, fwd)], by)
            case([multi("R = sum(I)", rev, fwd)], by)
    # A formula that fails for a window.
    case([multi("R = 1 / (len(I) - 2)", 2, 0)])
    case([multi("R = 1 / (sum(I) - sum(I))", 2, 0)])
    # Requests Java refuses.
    case([each("sum(q) + I", ["R = I"], 2, 0)])
    case([each("q_[0]", ["R = I"], 2, 0)])
    case([each("nope(q)", ["R = I"], 2, 0)])
    case([each("sum(q", ["R = I"], 2, 0)])
    case([each("sum(q)", ["R = Nope"], 2, 0)])
    case([each("sum(q)", ["R = S"], 2, 0)])
    case([each("sum(q)", ["R = I", "R = D"], 2, 0)])
    case([each("sum(q)", ["R = I"], 2, 0)], ("Nope",))
    case([each("sum(y)", ["R = I"], 2, 0)])
    case([each("", ["R = I"], 2, 0)])
    case([multi("R = sum(I) + i", 2, 0)])
    case([multi("R = sum(I) + ii", 2, 0)], ("K",))
    case([multi("R = I_[0]", 2, 0)])
    case([multi("R = sum(Nope)", 2, 0)])
    case([multi("R = sum(K)", 2, 0)], ("K",))
    case([multi("R = K.length()", 2, 0)])
    case([multi("R = sum(I", 2, 0)])
    case([multi("sum(I)", 2, 0)])
    case([multi("R == sum(I)", 2, 0)])
    case([multi("1R = sum(I)", 2, 0)])
    case([multi("R = sum(I)", 2, 0), multi("R = sum(L)", 2, 0)])
    case([multi("R = sum(I)", 2, 0), {"op": "cum_sum", "columns": ["R = I"]}])
    return cases


# Requests refused by the server's validation of the formula alone.
def validated_cases():
    cases = []

    def case(ops, by=()):
        cases.append({"ops": ops, "by": list(by)})

    # The token is replaced inside names.
    case([each("max(x)", ["R = I"], 2, 0, param="x")])
    case([each("sum(x) + max(x_) ", ["R = I"], 2, 0, param="x")])
    case([each("sum(xx)", ["R = I"], 2, 0, param="x")])
    case([each("sum(sum)", ["R = I"], 2, 0, param="sum")])
    # The engine's own name for the input is not one the validation knows.
    case([each("sum(__PARAM_COLUMN__)", ["R = I"], 2, 0)])
    # The validation groups the input by the bucket columns: a bucket column is a scalar.
    case([each("q.size()", ["R = K"], 2, 0)], ("K",))
    case([each("q.size()", ["K"], 2, 0)], ("K",))
    case([each("sum(q)", ["R = KD"], 1, 1)], ("KD",))
    # It knows the bucket columns, which the engine's formula does not.
    case([each("sum(q) + K.length()", ["R = I"], 1, 1)], ("K",))
    return cases


# Functions the expression crate refuses by name, and an element or a part of a window.
UNPORTED = [
    each("wsum(q, q)", ["R = I"], 3, 1),
    each("wavg(q, q)", ["R = D"], 3, 1),
    each("percentile(0.5, q)", ["R = D"], 3, 1),
    each("cumprod(q)", ["R = I"], 3, 1),
    each("cummin(q)", ["R = I"], 3, 1),
    each("countPos(q)", ["R = I"], 3, 1),
    each("countZero(q)", ["R = I"], 3, 1),
    each("diff(1, q)", ["R = I"], 3, 1),
    each("in(2, q)", ["R = I"], 3, 1),
    each("firstIndexOf(q, 2)", ["R = I"], 3, 1),
    each("ste(q)", ["R = D"], 3, 1),
    each("tstat(q)", ["R = D"], 3, 1),
    each("containsNonFinite(q)", ["R = D"], 3, 1),
    each("anyNull(q)", ["R = I"], 3, 1),
    each("nth(1, q)", ["R = I"], 3, 1),
    each("q.toArray()", ["R = I"], 3, 1),
    # `countObj` of a vector of a primitive type.
    each("countObj(q)", ["R = C"], 3, 1),
    each("countObj(q)", ["R = I"], 3, 1),
    # `i`, `ii` and `k` are 0 in a formula with a token.
    each("sum(q) + i", ["R = I"], 3, 1),
    each("sum(q) + ii - k", ["R = I"], 3, 1),
    # A vector of objects other than strings: Java no longer knows the element type.
    each("q", ["R = B"], 3, 1),
    each("q.get(0)", ["R = I"], 3, 1),
    each("q[1]", ["R = D"], 3, 1),
    each("q[5]", ["R = I"], 3, 1),
    each("q.subVector(0, 1)", ["R = I"], 3, 1),
    each("q.get(1)", ["R = S"], 3, 1),
    multi("R = wavg(D, I)", 3, 1),
    multi("R = cov(D, F)", 3, 1),
    multi("R = cor(D, F)", 3, 1),
    multi("R = sum(I * 2)", 3, 1),
    multi("R = sum(I + L2)", 3, 1),
    multi("R = sum(D * F)", 3, 1),
    multi("R = S.get(0).length()", 3, 1),
    multi("R = I[0] + D.get(1)", 3, 1),
]


def unported_cases():
    return [{"ops": [op], "by": by} for op in UNPORTED for by in ([], ["K"])]


def long_cases():
    cases = []
    formulas = [
        each("sum(q)", ["R = IR", "Q = DR", "P = FR"], 0, 0),
        each("avg(q)", ["R = DR", "Q = LR"], 0, 0),
        each("product(q)", ["R = DZ"], 0, 0),
        each("std(q)", ["R = DR", "Q = FR"], 0, 0),
        multi("R = sum(IR) + len(DR) + avg(LR)", 0, 0),
        multi("R = max(DR) - min(DZ)", 0, 0),
    ]
    for formula in formulas:
        for by in ((), ("K",)):
            for rev, fwd in rest.LONG_WINDOWS:
                cases.append({"ops": [{**formula, "rev": rev, "fwd": fwd}], "by": list(by)})
    return cases


def timed_formulas(ts):
    return [
        lambda rev, fwd: each("sum(q)", ["R = I", "Q = D", "P = Y"], rev, fwd, ts),
        lambda rev, fwd: each("avg(q)", ["R = F", "Q = L"], rev, fwd, ts),
        lambda rev, fwd: each("q.size()", ["R = S", "Q = B"], rev, fwd, ts),
        lambda rev, fwd: each("q", ["R = I", "Q = S"], rev, fwd, ts),
        lambda rev, fwd: each("max(q) - min(q)", ["R = DS", "Q = FS", "P = H"], rev, fwd, ts),
        lambda rev, fwd: each("firstObj(q)", ["R = S"], rev, fwd, ts),
        lambda rev, fwd: each("len(q)", ["R = T"], rev, fwd, ts),
        lambda rev, fwd: multi("R = sum(I) + countObj(S) - count(D)", rev, fwd, ts),
        lambda rev, fwd: multi("R = len(TL) + len(TI)", rev, fwd, ts),
        lambda rev, fwd: multi("R = 3", rev, fwd, ts),
        lambda rev, fwd: multi("R = countObj(TS)", rev, fwd, ts),
        lambda rev, fwd: multi("R = len(H) - count(H)", rev, fwd, ts),
    ]


def timed_cases():
    cases = []

    def case(ops, by=()):
        cases.append({"ops": ops, "by": list(by)})

    for rev, fwd in base.TIME_WINDOWS + [(0, 3), (3, -3)]:
        for formula in timed_formulas("TS"):
            for by in ((), ("K",)):
                case([formula(rev, fwd)], by)
    for formula in timed_formulas(None):
        for by in ((), ("K",)):
            case([formula(4, 2)], by)
    case([multi("R = K + len(I)", 5, 5, "TS")], ("K",))
    case([multi("R = K == null ? null : K + lastObj(S)", 5, 0, "TS")], ("K",))
    # Other timestamp columns: a long, one that only rises within buckets, one that falls.
    for ts in ("TL", "TB", "TD", "TI", "Nope"):
        for by in ((), ("K",)):
            case([each("sum(q)", ["R = I"], 5, 2, ts)], by)
            case([multi("R = len(TI) + sum(I)", 5, 2, ts)], by)
    # The timestamp column as the input, with and without other columns, and as the
    # result; no columns at all.
    case([each("q.size()", ["R = TS"], 5, 0, "TS")])
    case([each("q.size()", ["R = I", "Q = TS"], 5, 0, "TS")])
    case([each("q.size()", ["TS = I"], 5, 0, "TS")])
    case([each("len(q)", ["R = TL"], 5, 0, "TS")])
    case([each("len(q)", ["R = I", "Q = TL"], 5, 0, "TL")])
    names = [name for name, _, _ in base.TIMED]
    case([each("q.size()", [], 5, 0, "TS", results=[n for n in names if n not in ("K", "TS")])], ("K",))
    case([each("q.size()", [], 5, 0, "TL", results=[n for n in names if n != "TL"])])
    case([multi("TS = len(I)", 5, 0, "TS")])
    case([multi("I = sum(I)", 5, 5, "TS")], ("K",))
    case([
        each("sum(q)", ["FA = I", "FB = DS"], 5, 5, "TS"),
        {"op": "rolling_sum", "columns": ["RS = I"], "ts": "TS", "rev": 5, "fwd": 5},
        multi("FC = countObj(S) + sum(I)", 5, 5, "TS"),
        multi("FD = K + count(D)", 3, 0, "TS"),
        each("first(q)", ["FE = I"], 3, 0),
        {"op": "ema", "columns": ["EA = I"], "decay": 3},
        group.group(["G = I"], 3, 0, "TS"),
    ], ("K",))
    # Requests Java refuses.
    case([each("sum(q)", ["R = I"], 5, 0, "TS"), multi("Q = sum(I)", 5, 0, "TL")])
    case([each("sum(q)", ["R = I"], -5, 2, "TS")])
    case([multi("R = sum(I)", 5, -6, "TS")])
    case([multi("R = sum(I) + i", 5, 0, "TS")])
    return cases


TICKING_OPS = [
    each("sum(q)", ["FaI = I", "FaD = D", "FaL = L"], 3, 0),
    each("len(q) > 1 ? last(q) - first(q) : 0", ["FbI = I"], 2, 2),
    each("countObj(q)", ["FcS = S"], -1, 3),
    each("q", ["FdI = I"], 1, 1),
    multi("FeM = sum(I) + countObj(S) + avg(D)", 3, 0),
    multi("FfM = max(L) > 0 ? count(I) : -count(D)", 2, 1),
    multi("FgM = countObj(K)", 2, 0),
    {"op": "rolling_sum", "columns": ["RsI = I"], "rev": 3, "fwd": 0},
    {"op": "cum_sum", "columns": ["CsI = I"]},
]

TIMED_TICKING_OPS = [
    each("sum(q)", ["FaI = I", "FaD = D"], 3000, 0, "Ts"),
    each("len(q)", ["FbD = D"], 2000, 2000, "Ts"),
    each("avg(q)", ["FcI = I"], -1000, 4000, "Ts"),
    multi("FdM = len(D) + count(I)", 0, 0, "Ts"),
    multi("FeM = sum(I) - sum(D)", 2500, 1500, "Ts"),
    each("max(q)", ["FfI = I"], 2, 1),
    {"op": "rolling_sum", "columns": ["RsI = I"], "ts": "Ts", "rev": 3000, "fwd": 0},
]


def record_ticking(session, name, seed, steps, by, timed=False, make_row=None):
    group.TICKING_OPS, group.TIMED_TICKING_OPS = TICKING_OPS, TIMED_TICKING_OPS
    if by:
        # `K` is a scalar in a formula without a token when it is a bucket column.
        group.TICKING_OPS = [
            {**op, "formula": "FgM = K + len(I)"} if op.get("formula", "").startswith("FgM") else op
            for op in TICKING_OPS
        ]
    return group.record_ticking(session, name, seed, steps, by, timed, make_row)


def main():
    gc.disable()
    session = open_session(sys.argv[1])
    sections = {
        "static": base.record_static(session, base.STATIC, static_cases()),
        "unported": base.record_static(session, base.STATIC, unported_cases()),
        "long": base.record_static(session, base.LONG, long_cases()),
        "timed": base.record_static(session, base.TIMED, timed_cases()),
        "stamped": base.record_static(session, group.STAMPED, timed_cases()),
    }
    ticking = [
        record_ticking(session, "whole", 51, 30, []),
        record_ticking(session, "bucketed", 52, 40, ["K"]),
        record_ticking(session, "timed", 54, 30, [], timed=True),
        record_ticking(session, "timed_bucketed", 55, 40, ["K"], timed=True),
        record_ticking(session, "stamped_bucketed", 57, 40, ["K"], timed=True, make_row=group.stamped_row),
    ]
    with open(HERE / "java_update_by_formula.json", "w", encoding="utf-8") as out:
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
