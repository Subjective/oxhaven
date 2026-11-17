"""End-to-end benchmark of table operations on two Deephaven-protocol servers.

    python bench/bench.py run --server java=dh+psk://oxhaven@127.0.0.1:10868 \
        --server oxhaven=dh+psk://oxhaven@127.0.0.1:10869 --out result.json
    python bench/bench.py report result.json [second.json ...] > result.md

`run` executes every case on every server through the Python client the oracle uses
(pydeephaven, sessions opened with `oracle/harness/connect.py`) and writes one JSON file.
`report` turns one or more such files into a Markdown table. docs/BENCHMARK.md says what is
measured and how to read it.

No Java counterpart: a measuring tool.
"""

from __future__ import annotations

import argparse
import datetime
import fnmatch
import json
import math
import os
import platform
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import grpc
import pyarrow as pa
import pyarrow.compute  # noqa: F401 - pa.compute

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "oracle"))

from harness.connect import collect_garbage, open_session  # noqa: E402
from pydeephaven import Session, Table, agg  # noqa: E402

# The source table every static case starts from. Keys are spread with multipliers coprime to
# their modulus, so consecutive rows have different keys and every key occurs equally often.
SOURCE_FORMULAS = [
    "Id = ii",
    "K10 = (int)(ii % 10)",
    "K10k = (int)((ii * 7919) % 10000)",
    "K1M = (int)((ii * 104729) % 1000000)",
    "A = (int)((ii * 31337) % 100003)",
    "B = ((ii * 15485863) % 1000003) / 7.0",
    'S = "s" + (ii % 1000)',
]

# Completion, for every case that returns a table: the client call returns when the server has
# answered the export with ExportedTableCreationResponse, which carries the result's size. Both
# servers answer only after the operation has run; formula columns of `update` are stored, not
# computed on read, on both (QueryTable#update; oxhaven-engine ops::select).
DONE_EXPORT = "export answered (result exists, size known)"


@dataclass
class Ctx:
    session: Session
    n: int
    tables: dict[str, Table] = field(default_factory=dict)


@dataclass
class Case:
    name: str
    group: str
    # Untimed. Returns what `run` needs. Each repetition gets a fresh parent table (a `head` of
    # the source, which Java does not memoize), so Java's memoized results
    # (QueryTable#memoizeResult: where, sort, aggregations, select of plain columns) are never
    # reused between repetitions.
    setup: Callable[[Ctx], Any]
    # Timed.
    run: Callable[[Ctx, Any], Any]
    done: str = DONE_EXPORT
    # Sizes this case runs at; None means every size.
    sizes: tuple[int, ...] | None = None


def fresh(t: Table, n: int) -> Table:
    return t.head(n)


def src(ctx: Ctx) -> Table:
    return fresh(ctx.tables["T"], ctx.n)


def narrow(key: str) -> Callable[[Ctx], Table]:
    def setup(ctx: Ctx) -> Table:
        return fresh(ctx.tables["T"].view([f"K = {key}", "A", "B"]), ctx.n)

    return setup


def build_sources(ctx: Ctx) -> None:
    s, n = ctx.session, ctx.n
    ctx.tables["T"] = s.empty_table(n).update(SOURCE_FORMULAS)
    ctx.tables["RS"] = s.empty_table(10_000).update(["K10k = (int)ii", "R1 = ii * 3"])
    ctx.tables["RL"] = s.empty_table(1_000_000).update(["K1M = (int)ii", "R2 = ii * 5"])
    ctx.tables["RA"] = s.empty_table(n // 10).update(
        ["K10 = (int)(ii % 10)", "Stamp = ii * 10 + 3", "R4 = ii"]
    )
    ctx.tables["G"] = ctx.tables["T"].view(["K10k", "A"]).group_by("K10k")
    ctx.tables["P"] = ctx.tables["T"].view(["Id", "A", "B"]).head(1_000_000)


def static_cases() -> list[Case]:
    c: list[Case] = []

    def add(name, group, setup, run, **kw):
        c.append(Case(name, group, setup, run, **kw))

    add("build", "source", lambda ctx: None, lambda ctx, _: ctx.session.empty_table(ctx.n).update(SOURCE_FORMULAS))
    add("update_arith", "update", src, lambda ctx, t: t.update(["C = A * 2 + 1", "D = B * 1.5 - A", "E = Id % 7"]))
    add("update_string", "update", src, lambda ctx, t: t.update(['S2 = S + "_x"']))
    add("update_cond", "update", src, lambda ctx, t: t.update(["F = A > 50000 ? B : -B"]))
    add("where_range", "where", src, lambda ctx, t: t.where("A >= 20000 && A < 60000"))
    add("where_string", "where", src, lambda ctx, t: t.where("S in `s1`, `s42`, `s999`"))
    add("where_conj", "where", src, lambda ctx, t: t.where(["K10 = 3", "B > 50000.0", "A < 50000"]))
    add("sort_int", "sort", src, lambda ctx, t: t.sort("A"))
    add("sort_two", "sort", src, lambda ctx, t: t.sort(["K10", "B"]))
    add("sort_string", "sort", src, lambda ctx, t: t.sort("S"))
    for card, key in (("low", "K10"), ("med", "K10k"), ("high", "K1M")):
        add(f"sum_by_{card}", "agg", narrow(key), lambda ctx, t: t.sum_by("K"))
        add(f"avg_by_{card}", "agg", narrow(key), lambda ctx, t: t.avg_by("K"))
        add(f"count_by_{card}", "agg", narrow(key), lambda ctx, t: t.count_by("N", "K"))
    add(
        "agg_by_multi",
        "agg",
        src,
        lambda ctx, t: t.agg_by(
            [agg.sum_(["SA = A"]), agg.avg(["AB = B"]), agg.min_(["MinA = A"]), agg.max_(["MaxB = B"]), agg.count_("N")],
            by="K10k",
        ),
    )
    add("last_by", "agg", src, lambda ctx, t: t.last_by("K10k"))
    add("natural_join_small", "join", src, lambda ctx, t: t.natural_join(ctx.tables["RS"], on="K10k", joins="R1"))
    add("natural_join_large", "join", src, lambda ctx, t: t.natural_join(ctx.tables["RL"], on="K1M", joins="R2"))
    add("exact_join_small", "join", src, lambda ctx, t: t.exact_join(ctx.tables["RS"], on="K10k", joins="R1"))
    add("exact_join_large", "join", src, lambda ctx, t: t.exact_join(ctx.tables["RL"], on="K1M", joins="R2"))
    add("join_on_key", "join", src, lambda ctx, t: t.join(ctx.tables["RS"], on="K10k", joins="R1"))
    add("aj", "join", src, lambda ctx, t: t.aj(ctx.tables["RA"], on=["K10", "Id >= Stamp"], joins="R4"))
    add("select_distinct_one", "other", src, lambda ctx, t: t.select_distinct("K10k"))
    add("select_distinct_two", "other", src, lambda ctx, t: t.select_distinct(["K10", "S"]))
    add("head_half", "other", src, lambda ctx, t: t.head(ctx.n // 2))
    add("tail_half", "other", src, lambda ctx, t: t.tail(ctx.n // 2))
    add("group_by", "other", lambda ctx: fresh(ctx.tables["T"].view(["K10k", "A"]), ctx.n), lambda ctx, t: t.group_by("K10k"))
    add("ungroup", "other", lambda ctx: fresh(ctx.tables["G"], 10_000), lambda ctx, t: t.ungroup("A"))
    add(
        "doget_1m",
        "read",
        lambda ctx: fresh(ctx.tables["P"], 1_000_000),
        lambda ctx, t: t.to_arrow(),
        done="every record batch of 1M rows x (long, int, double) received by the client",
    )
    return c


# ---------------------------------------------------------------------------------------------
# Ticking: an append-only input table, with `where` + `sum_by` downstream. One repetition adds
# a fixed batch and waits until the derived table shows it.

TICK_SCHEMA = pa.schema([("K", pa.int32()), ("V", pa.int64())])
TICK_BATCH = 100_000
TICK_KEYS = 100


class Barrier:
    """Two changes to a private input table, one after the other; see oracle/harness/runner.py
    `_Barrier` for why the second returning means every table derived from an earlier change
    is up to date."""

    SCHEMA = pa.schema([("BarrierKey", pa.int32()), ("BarrierTick", pa.int64())])

    def __init__(self, session: Session):
        self.session = session
        self.table = session.input_table(schema=self.SCHEMA, key_cols=["BarrierKey"])
        self.rows = [
            session.import_table(
                pa.Table.from_arrays([pa.array([0], pa.int32()), pa.array([i], pa.int64())], schema=self.SCHEMA)
            )
            for i in range(2)
        ]

    def wait(self) -> None:
        for row in self.rows:
            self.table.add(row)


def tick_batch(rep: int) -> pa.Table:
    base = rep * TICK_BATCH
    ks = [(base + i) % TICK_KEYS for i in range(TICK_BATCH)]
    vs = [base + i for i in range(TICK_BATCH)]
    return pa.Table.from_arrays([pa.array(ks, pa.int32()), pa.array(vs, pa.int64())], schema=TICK_SCHEMA)


class Ticking:
    def __init__(self, ctx: Ctx):
        s = ctx.session
        self.input = s.input_table(schema=TICK_SCHEMA)
        self.derived = self.input.where("V % 2 == 0").sum_by("K")
        self.barrier = Barrier(s)
        self.rep = 0

    def setup(self, ctx: Ctx) -> Table:
        t = ctx.session.import_table(tick_batch(self.rep))
        self.rep += 1
        return t

    def run(self, ctx: Ctx, batch: Table) -> pa.Table:
        self.input.add(batch)
        self.barrier.wait()
        return self.derived.to_arrow()

    def expected(self) -> dict[int, int]:
        sums: dict[int, int] = {}
        for i in range(self.rep * TICK_BATCH):
            if i % 2 == 0:
                sums[i % TICK_KEYS] = sums.get(i % TICK_KEYS, 0) + i
        return sums


# ---------------------------------------------------------------------------------------------
# Checksums: row count, the sum of every numeric column, a position-weighted sum of the first
# numeric column (so that order counts), and the first and last three rows.


def numeric_columns(schema: pa.Schema) -> list[str]:
    return [f.name for f in schema if pa.types.is_integer(f.type) or pa.types.is_floating(f.type)]


def scalar_columns(schema: pa.Schema) -> list[str]:
    return [f.name for f in schema if not (pa.types.is_list(f.type) or pa.types.is_large_list(f.type))]


def checksum(result: Any) -> dict:
    if isinstance(result, pa.Table):
        out: dict[str, Any] = {"rows": result.num_rows, "sums": {}}
        for name in numeric_columns(result.schema):
            out["sums"][name] = _num(pa.compute.sum(result.column(name)).as_py())
        return out
    t: Table = result
    out = {"rows": t.size, "sums": {}}
    nums = numeric_columns(t.schema)
    if nums and t.size > 0:
        row = t.view(nums).sum_by().to_arrow()
        out["sums"] = {c: _num(row.column(c)[0].as_py()) for c in nums}
        first = nums[0]
        w = t.view([f"W = (ii % 1009) * {first}"]).sum_by().to_arrow()
        out["weighted"] = _num(w.column("W")[0].as_py())
    cols = scalar_columns(t.schema)
    if cols and t.size > 0:
        out["head"] = _rows(t.view(cols).head(3).to_arrow())
        out["tail"] = _rows(t.view(cols).tail(3).to_arrow())
    return out


def _num(v):
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return str(v)
    return v


def _rows(t: pa.Table) -> list:
    return [[_num(v) for v in r.values()] for r in t.to_pylist()]


def same(a: Any, b: Any) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is b
        return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9)
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    return a == b


# ---------------------------------------------------------------------------------------------


def failure(e: BaseException) -> dict:
    err: BaseException | None = e
    while err is not None:
        if isinstance(err, grpc.RpcError):
            code = err.code().name
            kind = "refused" if code in ("UNIMPLEMENTED", "INVALID_ARGUMENT") else "error"
            return {"status": kind, "code": code, "message": " ".join(str(err.details()).split())[:300]}
        err = err.__cause__ or err.__context__
    return {"status": "error", "code": type(e).__name__, "message": " ".join(str(e).split())[:300]}


def measure(case: Case, ctx: Ctx, warmup: int, reps: int) -> dict:
    times: list[float] = []
    check = None
    try:
        for i in range(warmup + reps):
            state = case.setup(ctx)
            t0 = time.perf_counter()
            result = case.run(ctx, state)
            elapsed = time.perf_counter() - t0
            if i >= warmup:
                times.append(elapsed)
            if i == warmup + reps - 1:
                check = checksum(result) if not isinstance(case, TickCase) else case.check(result)
            del state, result
    except Exception as e:  # noqa: BLE001 - every failure is a result
        out = failure(e)
        out["times"] = times
        return out
    finally:
        collect_garbage()
    return {
        "status": "ok",
        "times": times,
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times),
        "check": check,
    }


class TickCase(Case):
    def __init__(self):
        super().__init__(
            "ticking_append_where_sum_by",
            "ticking",
            self._setup,
            self._run,
            done=(
                f"append of {TICK_BATCH} rows returned, then two barrier changes to another "
                "input table returned, then the derived table (100 rows) fetched by DoGet"
            ),
            sizes=None,
        )
        self.state: dict[int, Ticking] = {}

    def _setup(self, ctx: Ctx):
        tk = self.state.setdefault(id(ctx), Ticking(ctx))
        return (tk, tk.setup(ctx))

    def _run(self, ctx: Ctx, st):
        tk, batch = st
        return (tk, tk.run(ctx, batch))

    def check(self, result) -> dict:
        tk, table = result
        got = {r["K"]: r["V"] for r in table.to_pylist()}
        want = tk.expected()
        return {"rows": len(got), "correct": got == want}


def run(args: argparse.Namespace) -> int:
    servers = []
    for spec in args.server:
        name, url = spec.split("=", 1)
        servers.append((name, url))
    sizes = [int(s) for s in args.sizes.split(",")]
    cases = static_cases()
    tick = TickCase()
    cases.append(tick)
    if args.cases:
        pats = args.cases.split(",")
        cases = [c for c in cases if any(fnmatch.fnmatch(c.name, p) for p in pats)]
    meta = {
        "started": datetime.datetime.now().isoformat(timespec="seconds"),
        "host": platform.node(),
        "cpus": os.cpu_count(),
        "loadavg_start": os.getloadavg(),
        "warmup": args.warmup,
        "reps": args.reps,
        "servers": dict(servers),
        "done": {c.name: c.done for c in cases},
    }
    results: dict = {"meta": meta, "sizes": {}}
    for n in sizes:
        print(f"== size {n}", flush=True)
        ctxs = {}
        per_case: dict = {}
        for name, url in servers:
            ctx = Ctx(open_session(url), n)
            t0 = time.perf_counter()
            build_sources(ctx)
            print(f"   sources on {name}: {time.perf_counter() - t0:.1f}s", flush=True)
            ctxs[name] = ctx
        for case in cases:
            if case.sizes is not None and n not in case.sizes:
                continue
            reps = args.reps if n <= 1_000_000 else args.reps_large
            row = {}
            for name, _ in servers:
                row[name] = measure(case, ctxs[name], args.warmup, reps)
            oks = [r for r in row.values() if r["status"] == "ok"]
            if len(oks) == len(servers) and len(oks) > 1:
                first = oks[0]["check"]
                row["match"] = all(same(first, r["check"]) for r in oks[1:])
                if isinstance(case, TickCase):
                    row["match"] = row["match"] and all(r["check"]["correct"] for r in oks)
            per_case[case.name] = row
            summary = "  ".join(
                f"{k}={v['median'] * 1000:.1f}ms" if v["status"] == "ok" else f"{k}={v['status']}:{v.get('code')}"
                for k, v in row.items()
                if isinstance(v, dict)
            )
            print(f"   {case.name:28s} {summary}  match={row.get('match')}", flush=True)
            results["sizes"][str(n)] = per_case
            with open(args.out, "w") as f:
                json.dump(results, f, indent=1)
        for ctx in ctxs.values():
            ctx.tables.clear()
            collect_garbage()
            ctx.session.close()
        tick.state.clear()
        collect_garbage()
    meta["loadavg_end"] = os.getloadavg()
    meta["finished"] = datetime.datetime.now().isoformat(timespec="seconds")
    with open(args.out, "w") as f:
        json.dump(results, f, indent=1)
    return 0


def fmt_ms(v: float) -> str:
    ms = v * 1000
    return f"{ms:.0f}" if ms >= 100 else f"{ms:.1f}" if ms >= 1 else f"{ms:.2f}"


def report(args: argparse.Namespace) -> int:
    runs = []
    for path in args.files:
        with open(path) as f:
            runs.append(json.load(f))
    names = list(runs[0]["meta"]["servers"])
    a, b = names[0], names[1]
    print(f"Times in milliseconds, median of the timed repetitions (min-max). Ratio = {b} / {a}; "
          f"below 1 means {b} is faster.\n")
    for size in runs[0]["sizes"]:
        print(f"### {int(size):,} rows\n")
        hdr = ["case"]
        for i, _ in enumerate(runs):
            tag = f" (run {i + 1})" if len(runs) > 1 else ""
            hdr += [f"{a}{tag}", f"{b}{tag}", f"ratio{tag}"]
        hdr.append("same result")
        print("| " + " | ".join(hdr) + " |")
        print("|" + "---|" * len(hdr))
        for case in runs[0]["sizes"][size]:
            cells = [f"`{case}`"]
            match = []
            for r in runs:
                row = r["sizes"].get(size, {}).get(case)
                if row is None:
                    cells += ["", "", ""]
                    continue
                for name in (a, b):
                    v = row[name]
                    if v["status"] == "ok":
                        cells.append(f"{fmt_ms(v['median'])} ({fmt_ms(v['min'])}-{fmt_ms(v['max'])})")
                    else:
                        cells.append(f"{v['status']} ({v['code']})")
                if row[a]["status"] == "ok" and row[b]["status"] == "ok":
                    cells.append(f"{row[b]['median'] / row[a]['median']:.2f}")
                else:
                    cells.append("-")
                match.append({True: "yes", False: "NO", None: "-"}[row.get("match")])
            cells.append(" / ".join(match))
            print("| " + " | ".join(cells) + " |")
        print()
    return 0


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--server", action="append", required=True, help="name=url; give two, the first is the base of the ratio")
    r.add_argument("--sizes", default="1000000,10000000")
    r.add_argument("--warmup", type=int, default=2)
    r.add_argument("--reps", type=int, default=5, help="timed repetitions up to 1M rows")
    r.add_argument("--reps-large", type=int, default=3, help="timed repetitions above 1M rows")
    r.add_argument("--cases", help="comma-separated glob patterns of case names")
    r.add_argument("--out", required=True)
    rp = sub.add_parser("report")
    rp.add_argument("files", nargs="+")
    args = p.parse_args(argv[1:])
    return run(args) if args.cmd == "run" else report(args)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
