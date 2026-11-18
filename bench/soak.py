"""Soak test: one server under a fixed ticking workload for a long time, with invariants.

    python bench/soak.py run --binary target/release/oxhaven-server --profile long \
        --out soak.json
    python bench/soak.py report soak.json      # the summary of a finished run, judged again
    python bench/soak.py selftest              # the judging itself, on made-up series

`run` starts the server, drives it through the Python client (pydeephaven, sessions opened
with `oracle/harness/connect.py`, as `bench/bench.py` does), measures the process from
outside (`/proc/<pid>`), stops it, writes one JSON file, prints a summary and exits non-zero
when an invariant was violated. docs/SOAK.md says what runs, what is checked and how to read
a failure.

No Java counterpart: a testing tool.
"""

from __future__ import annotations

import argparse
import datetime
import faulthandler
import json
import os
import random
import secrets
import signal
import statistics
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import pyarrow as pa
import pyarrow.compute as pc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bench import Barrier, failure  # noqa: E402 - bench.py, which puts oracle/ on the path
from harness.connect import collect_garbage, open_session  # noqa: E402
from pydeephaven import Session, Table, agg, updateby  # noqa: E402
from pydeephaven.ticket import SharedTicket  # noqa: E402

# ---------------------------------------------------------------------------------------------
# Profiles: how long, and how strict the statistics are. The invariants are the same.


@dataclass(frozen=True)
class Profile:
    duration: float  # seconds of workload
    warmup: float  # seconds left out of the trend fits
    # Resident memory: the fitted slope, in MB per hour, must not exceed this. See
    # docs/SOAK.md, "Memory", for where the numbers come from.
    rss_slope_mb_per_hour: float
    # Processor time per round: the fitted slope times the fitted window must not exceed
    # this share of the median.
    cpu_growth: float
    # Open files and threads at the end of a round: at most this many above the median of
    # the first rounds after the warm-up.
    handles_slack: int
    # Seconds: the longest a barrier (a requested cycle, twice) may take, the longest the
    # subscriber may go without an update of the time table, the furthest the newest
    # row it has seen may be behind the clock, and the longest time from one cycle to the
    # next, as a listener in the server sees it.
    barrier_max: float
    update_gap_max: float
    lag_max: float
    cycle_gap_max: float
    # The median barrier time of the last third may be at most this many times the median
    # of the first third (plus 50 ms).
    barrier_growth: float


PROFILES = {
    "long": Profile(
        duration=2700, warmup=900, rss_slope_mb_per_hour=24.0, cpu_growth=0.20, handles_slack=8,
        barrier_max=10.0, update_gap_max=10.0, lag_max=10.0, cycle_gap_max=5.0, barrier_growth=2.0,
    ),
    "short": Profile(
        duration=120, warmup=40, rss_slope_mb_per_hour=1500.0, cpu_growth=0.60, handles_slack=8,
        barrier_max=20.0, update_gap_max=20.0, lag_max=20.0, cycle_gap_max=10.0, barrier_growth=3.0,
    ),
}

# Every unary call of the client gets this deadline; a step of the workload that does not end
# within STEP_TIMEOUT (--step-timeout) ends the run (the watchdog), whatever call it waits in.
RPC_TIMEOUT = 60.0
STEP_TIMEOUT = 180.0
SHUTDOWN_TIMEOUT = 15.0
# A session token lives this long. The sessions the workload abandons are ended by the
# server when theirs runs out; the others refresh theirs.
TOKEN_EXPIRE_MS = 30_000

# ---------------------------------------------------------------------------------------------
# The server process, seen from outside.


class Server:
    def __init__(self, binary: str, cycle_ms: int, log_path: str, console: str):
        self.psk = secrets.token_hex(8)
        self.log_path = log_path
        self.log = open(log_path, "wb")
        env = dict(os.environ, NO_COLOR="1")
        # A server with the Python console needs the libpython it was linked against.
        libdir = sysconfig.get_config_var("LIBDIR") or ""
        if libdir:
            env["LD_LIBRARY_PATH"] = libdir + os.pathsep + env.get("LD_LIBRARY_PATH", "")
        cmd = [
            binary, "--host", "127.0.0.1", "--port", "0", "--psk", self.psk,
            "--cycle-ms", str(cycle_ms), "--token-expire-ms", str(TOKEN_EXPIRE_MS),
        ]
        if console != "auto":
            cmd += ["--console", console]
        self.proc = subprocess.Popen(cmd, stdout=self.log, stderr=subprocess.STDOUT, env=env)
        self.pid = self.proc.pid
        self.url = self._wait_for_address()

    def _wait_for_address(self) -> str:
        """The server logs the address it listens on; with port 0 that is where the port
        comes from (as oracle/bin/ci-replay.sh reads it)."""
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"the server exited {self.proc.returncode} while starting; see {self.log_path}")
            with open(self.log_path, "rb") as f:
                text = f.read().decode("utf-8", "replace")
            at = text.find("Listening on 127.0.0.1:")
            if at >= 0:
                port = text[at:].split(":", 1)[1].split()[0]
                return f"dh+psk://{self.psk}@127.0.0.1:{int(port)}"
            time.sleep(0.1)
        raise RuntimeError(f"the server did not say where it listens; see {self.log_path}")

    def alive(self) -> bool:
        return self.proc.poll() is None

    def sample(self) -> dict | None:
        """Resident memory, processor time, threads and open files of the process, from
        /proc. None once it is gone."""
        try:
            with open(f"/proc/{self.pid}/status") as f:
                status = f.read()
            with open(f"/proc/{self.pid}/stat") as f:
                stat = f.read()
            fds = len(os.listdir(f"/proc/{self.pid}/fd"))
        except OSError:
            return None
        rss = hwm = threads = 0
        for line in status.splitlines():
            if line.startswith("VmRSS:"):
                rss = int(line.split()[1])
            elif line.startswith("VmHWM:"):
                hwm = int(line.split()[1])
            elif line.startswith("Threads:"):
                threads = int(line.split()[1])
        # The fields after the command name, which may hold spaces: utime and stime are the
        # 14th and 15th of the line.
        rest = stat[stat.rindex(")") + 2:].split()
        ticks = os.sysconf("SC_CLK_TCK")
        return {
            "rss_kb": rss, "hwm_kb": hwm, "threads": threads, "fds": fds,
            "cpu_s": (int(rest[11]) + int(rest[12])) / ticks,
        }

    def stop(self) -> dict:
        """SIGINT, which the server treats as a request to shut down."""
        if not self.alive():
            return {"clean": False, "exit_code": self.proc.returncode, "seconds": 0.0, "how": "already gone"}
        t0 = time.monotonic()
        self.proc.send_signal(signal.SIGINT)
        try:
            code = self.proc.wait(SHUTDOWN_TIMEOUT)
            how = "SIGINT"
        except subprocess.TimeoutExpired:
            self.proc.kill()
            code = self.proc.wait()
            how = f"killed: still running {SHUTDOWN_TIMEOUT:.0f}s after SIGINT"
        self.log.close()
        return {"clean": code == 0 and how == "SIGINT", "exit_code": code,
                "seconds": round(time.monotonic() - t0, 3), "how": how}

    def kill(self) -> None:
        if self.alive():
            self.proc.kill()
            self.proc.wait()


# ---------------------------------------------------------------------------------------------
# Bookkeeping shared by the threads of the driver.


class Run:
    def __init__(self, max_violations: int = 40):
        self.lock = threading.Lock()
        self.violations: list[dict] = []
        self.known: list[dict] = []
        self.max_violations = max_violations
        self.requests = 0
        self.checks: dict[str, int] = {}
        self.t0 = time.monotonic()
        self.heartbeat = (time.monotonic(), "start")

    def now(self) -> float:
        return round(time.monotonic() - self.t0, 3)

    def violate(self, invariant: str, what: str, detail: Any = None) -> None:
        entry = {"t": self.now(), "invariant": invariant, "what": what, "detail": detail}
        with self.lock:
            known = KNOWN_FAILURES.get(what)
            if known is not None:
                if len(self.known) < self.max_violations:
                    self.known.append(dict(entry, known=known))
                return
            if len(self.violations) < self.max_violations:
                self.violations.append(entry)
        print(f"VIOLATION [{invariant}] {what}: {json.dumps(detail, default=str)[:600]}", flush=True)

    def too_many(self) -> bool:
        with self.lock:
            return len(self.violations) >= self.max_violations

    def checked(self, name: str) -> None:
        with self.lock:
            self.checks[name] = self.checks.get(name, 0) + 1

    def count(self, n: int = 1) -> None:
        with self.lock:
            self.requests += n

    def beat(self, what: str) -> None:
        self.heartbeat = (time.monotonic(), what)


# Checks that are known to fail, by the name `violate` is given: each maps to where the
# reproduction is written down. A known failure is reported and does not fail the run; every
# other check stays enforced. Empty when nothing is known.
KNOWN_FAILURES: dict[str, str] = {}


def count_and_bound_client_calls(run: Run) -> None:
    """Every unary call the client library makes is counted and gets a deadline."""
    plain, bidi = Session.wrap_rpc, Session.wrap_bidi_rpc

    def wrap_rpc(self, stub_call, *args, **kwargs):
        run.count()
        kwargs.setdefault("timeout", RPC_TIMEOUT)
        return plain(self, stub_call, *args, **kwargs)

    def wrap_bidi_rpc(self, stub_call, *args, **kwargs):
        run.count()
        return bidi(self, stub_call, *args, **kwargs)

    Session.wrap_rpc = wrap_rpc
    Session.wrap_bidi_rpc = wrap_bidi_rpc


# ---------------------------------------------------------------------------------------------
# Comparing two results exactly.


def fetch_shared(session: Session, ticket: SharedTicket) -> Table:
    """The table another session published under `ticket`. The client's table then names the
    shared ticket itself, which is no export of this session: releasing it is refused, by
    the Java server as well, so the table is marked as needing no release."""
    t = session.fetch_table(ticket)
    t._closed = True
    return t


def canonical(t: pa.Table, keys: list[str] | None) -> pa.Table:
    t = t.combine_chunks()
    if keys:
        t = t.sort_by([(k, "ascending") for k in keys])
    return t


def difference(got: pa.Table, want: pa.Table, keys: list[str] | None) -> dict | None:
    """None when the two tables hold the same rows (in the same order when `keys` is None,
    otherwise as sets of rows with distinct `keys`); else what differs first."""
    if got.schema.names != want.schema.names or [f.type for f in got.schema] != [f.type for f in want.schema]:
        return {"schema_got": str(got.schema), "schema_want": str(want.schema)}
    if got.num_rows != want.num_rows:
        return {"rows_got": got.num_rows, "rows_want": want.num_rows}
    g, w = canonical(got, keys), canonical(want, keys)
    if g.equals(w):
        return None
    for name in g.schema.names:
        a, b = g.column(name).to_pylist(), w.column(name).to_pylist()
        for i, (x, y) in enumerate(zip(a, b)):
            if x != y and not (x != x and y != y):
                return {"column": name, "row": i, "got": g.slice(i, 1).to_pylist(), "want": w.slice(i, 1).to_pylist()}
    return None  # only NaN against NaN differed for `equals`


# ---------------------------------------------------------------------------------------------
# The long-lived graph: three keyed input tables with a fixed key space, so that the tables
# stop growing once every key has been written, and chains of ticking operations over them.

# Changes per round. A leak in an incremental path is per changed row, so the more rows a
# round changes, the smaller the leak per row that the memory limit catches.
WRITE_STEPS = 10
SYMS = [f"S{i:02d}" for i in range(20)]
SECTORS = ["tech", "energy", "retail", "bank"]
ID_SPACE = 2000
SLOTS = 8

TRADES = pa.schema([("Id", pa.int32()), ("Sym", pa.string()), ("Grp", pa.int32()),
                    ("Qty", pa.int64()), ("Px", pa.float64()), ("Ts", pa.int64())])
QUOTES = pa.schema([("Sym", pa.string()), ("Slot", pa.int32()), ("Bid", pa.float64()), ("QTs", pa.int64())])
DIMS = pa.schema([("Sym", pa.string()), ("Sector", pa.string()), ("Weight", pa.int64())])
EVENTS = pa.schema([("K", pa.int32()), ("V", pa.int64())])


@dataclass
class Pipeline:
    name: str
    # Builds the result from the three inputs: called once on the ticking inputs, and at
    # every check on static snapshots of them.
    build: Callable[[dict[str, Table]], Table]
    # None: row order is part of the result. Otherwise the columns that identify a row, and
    # the rows are compared as a set (an aggregation's groups are in order of first
    # appearance, which a recomputation from the rows as they are now does not reproduce).
    keys: list[str] | None = None


# Sums and averages are over whole numbers and prices are multiples of a quarter, so that an
# incrementally kept result and a recomputed one are equal to the last bit.
def pipelines() -> list[Pipeline]:
    multi = [agg.sum_(["SumQty = Qty"]), agg.count_("N"), agg.min_(["MinPx = Px"]), agg.max_(["MaxPx = Px"]),
             agg.avg(["AvgQty = Qty"]), agg.max_(["MaxTs = Ts"])]
    return [
        Pipeline("where", lambda i: i["trades"].where(["Qty % 3 != 0", "Px > 50.0"])),
        Pipeline("update", lambda i: i["trades"].update(["Notional = Qty * Px", "Bucket = (int)(Qty % 7)"])),
        Pipeline("where_update_view", lambda i: i["trades"].where("Grp < 6").update_view(["Half = Px / 2", "Odd = Qty % 2 == 1"])),
        Pipeline("agg_by_sym", lambda i: i["trades"].agg_by(multi, by=["Sym"]), ["Sym"]),
        Pipeline("agg_by_two_keys", lambda i: i["trades"].where("Qty % 5 != 0").agg_by(multi, by=["Grp", "Sym"]), ["Grp", "Sym"]),
        Pipeline("count_last_by", lambda i: i["trades"].view(["Grp", "Id", "Ts"]).sort("Ts").last_by("Grp"), ["Grp"]),
        Pipeline("natural_join", lambda i: i["trades"].natural_join(i["dims"], on="Sym", joins=["Sector", "Weight"])),
        Pipeline("aj", lambda i: i["trades"].aj(i["quotes"].sort("QTs"), on=["Sym", "Ts >= QTs"], joins=["Bid", "QTs"])),
        Pipeline("update_by", lambda i: i["trades"].sort("Ts").update_by(
            [updateby.cum_sum(["CumQty = Qty"]), updateby.cum_max(["CumMaxPx = Px"]),
             updateby.rolling_sum_tick(["Roll5 = Qty"], rev_ticks=5)], by=["Sym"])),
        Pipeline("sort", lambda i: i["trades"].sort(["Sym", "Qty", "Id"])),
        Pipeline("sort_descending_head", lambda i: i["trades"].sort_descending(["Px", "Id"]).head(50)),
        Pipeline("join_agg_sort", lambda i: i["trades"].natural_join(i["dims"], on="Sym", joins=["Sector", "Weight"])
                 .update(["Weighted = Qty * Weight"]).agg_by([agg.sum_(["Weighted"]), agg.count_("N")], by=["Sector"]).sort("Sector")),
        Pipeline("select_distinct", lambda i: i["trades"].select_distinct(["Grp", "Sym"]), ["Grp", "Sym"]),
    ]


class World:
    """The long-lived tables, all made by one session, which is their only writer."""

    def __init__(self, run: Run, url: str, seed: int, cycle_ms: int, console: bool):
        self.run = run
        self.rng = random.Random(seed)
        self.s = open_session(url)
        self.console = console
        self.clock = 0  # Ts and QTs: a counter, so that every write has its own time
        self.barrier = Barrier(self.s)
        self.inputs = {
            "trades": self.s.input_table(schema=TRADES, key_cols=["Id"]),
            "quotes": self.s.input_table(schema=QUOTES, key_cols=["Sym", "Slot"]),
            "dims": self.s.input_table(schema=DIMS, key_cols=["Sym"]),
        }
        self.events = self.s.input_table(schema=EVENTS, blink_table=True)
        self.event_count = 0
        self.event_sums: dict[int, int] = {}
        # A blink table shows a row for one cycle; an aggregation of it keeps every row's
        # contribution, so its result is known from what was ever added.
        self.event_agg = self.events.agg_by([agg.sum_(["SumV = V"]), agg.count_("N")], by=["K"])
        self.pipes = pipelines()
        self._add(self.inputs["dims"], DIMS, [SYMS, [self.rng.choice(SECTORS) for _ in SYMS], [self.rng.randrange(1, 9) for _ in SYMS]])
        self.write(initial=True)
        self.live = {p.name: p.build(self.inputs) for p in self.pipes}
        # Time tables: one that keeps its rows and one blink table. Each count is checked
        # against the rows its own table has made: row k of a time table is at the first
        # row's time plus k periods, so the count over the first is known from its last row,
        # and the count over the blink table, which counts every row it has shown since the
        # count was made, from the first and the last of them. The two tables are not
        # compared with each other: each reads the clock in its own refresh (in Java as
        # well: TimeTable#refresh), so in a cycle in which a period ends between the two
        # readings one of them has a row that the other gets a cycle later.
        self.period_ns = cycle_ms * 1_000_000
        # The first row is at a whole millisecond and `S` below is the row's time in whole
        # milliseconds, as a double: the aggregation keeps its sums by adding and
        # subtracting, and a sum of fractions kept that way differs in the last bit from one
        # made of the rows as they are now (in Java as well: DoubleChunkedSumOperator keeps
        # a running sum). `epochNanos(Timestamp) / 1000000` is no whole number even then:
        # the nanoseconds of today need 61 bits and the division is of doubles.
        self.start_ns = time.time_ns() // 1_000_000 * 1_000_000
        tt = self.s.time_table(period=self.period_ns, start_time=self.start_ns)
        tb = self.s.time_table(period=self.period_ns, start_time=self.start_ns, blink_table=True)
        tb_count = tb.agg_by([agg.count_("N"), agg.min_(["First = Timestamp"]), agg.max_(["Last = Timestamp"])], by=[])
        self.time = {
            "tt": tt,
            "tt_tail": tt.tail(600).update(["S = (double) epochMillis(Timestamp)", "M = S % 7"]),
            "tt_count": tt.agg_by([agg.count_("N")], by=[]),
            "tb_count": tb_count,
        }
        self.time["tt_agg"] = self.time_agg(self.time["tt_tail"])
        self.shared: dict[str, SharedTicket] = {}
        for name, table in (("trades", self.inputs["trades"]), ("dims", self.inputs["dims"]),
                            ("tt", tt), ("agg_by_sym", self.live["agg_by_sym"])):
            self.shared[name] = SharedTicket.random_ticket()
            self.s.publish_table(self.shared[name], table)
        if console:
            for name, table in list(self.time.items()) + [("trades", self.inputs["trades"])]:
                self.s.bind_table(f"soak_{name}", table)
            self.s.run_script(CONSOLE_SETUP.replace("TICK_NS", str(self.period_ns // 4)))

    @staticmethod
    def time_agg(t: Table) -> Table:
        return t.agg_by([agg.count_("N"), agg.sum_(["SumS = S"]), agg.max_(["MaxS = S"])], by=["M"])

    def _upload(self, schema: pa.Schema, columns: list[list]) -> Table:
        self.run.count()  # the DoPut
        return self.s.import_table(pa.Table.from_arrays([pa.array(c, f.type) for c, f in zip(columns, schema)], schema=schema))

    def _add(self, target, schema: pa.Schema, columns: list[list]) -> None:
        t = self._upload(schema, columns)
        target.add(t)
        t.close()

    def write(self, initial: bool = False) -> None:
        """One round of changes: rows added and changed in every input, rows deleted from
        the trades, rows added to the blink table. Returns when every table has them."""
        rng = self.rng
        for _ in range(1 if initial else WRITE_STEPS):
            ids = rng.sample(range(ID_SPACE), ID_SPACE // 2 if initial else rng.randrange(40, 200))
            self.clock += 1
            self._add(self.inputs["trades"], TRADES, [
                ids, [rng.choice(SYMS) for _ in ids], [rng.randrange(10) for _ in ids],
                [rng.randrange(1, 1000) for _ in ids], [rng.randrange(40, 800) / 4 for _ in ids],
                [self.clock * 1000 + n for n in range(len(ids))],
            ])
            slots = rng.sample([(s, k) for s in SYMS for k in range(SLOTS)], len(SYMS) * SLOTS if initial else 12)
            self._add(self.inputs["quotes"], QUOTES, [
                [s for s, _ in slots], [k for _, k in slots],
                [rng.randrange(40, 800) / 4 for _ in slots], [self.clock * 1000 - rng.randrange(1, 5000) for _ in slots],
            ])
            if initial:
                continue
            gone = rng.sample(range(ID_SPACE), rng.randrange(20, 120))
            self.run.count()
            t = self.s.import_table(pa.Table.from_arrays([pa.array(gone, pa.int32())], names=["Id"]))
            self.inputs["trades"].delete(t)
            t.close()
            syms = rng.sample(SYMS, 3)
            self._add(self.inputs["dims"], DIMS, [syms, [rng.choice(SECTORS) for _ in syms], [rng.randrange(1, 9) for _ in syms]])
            ks = [rng.randrange(16) for _ in range(100)]
            vs = [rng.randrange(1, 100) for _ in ks]
            self._add(self.events, EVENTS, [ks, vs])
            self.event_count += len(ks)
            for k, v in zip(ks, vs):
                self.event_sums[k] = self.event_sums.get(k, 0) + v
        self.barrier.wait()

    def fetch(self, t: Table) -> pa.Table:
        self.run.count()  # the DoGet
        return t.to_arrow()

    # -- checks ------------------------------------------------------------------------------

    def check_pipelines(self, mismatch: bool) -> None:
        """Each ticking result against the same operations on static snapshots of the
        inputs. The session is the inputs' only writer and has waited for the barrier, so
        the snapshots and the results are of the same cycle."""
        snaps = {name: t.snapshot() for name, t in self.inputs.items()}
        if mismatch:  # --inject mismatch: recompute from inputs that lack a row
            snaps["trades"] = snaps["trades"].head(snaps["trades"].size - 1)
        for p in self.pipes:
            self.run.beat(f"check {p.name}")
            expected = p.build(snaps)
            diff = difference(self.fetch(self.live[p.name]), self.fetch(expected), p.keys)
            expected.close()
            self.run.checked(p.name)
            if diff is not None:
                self.run.violate("correctness", f"pipeline {p.name}", diff)
        for t in snaps.values():
            t.close()

    def check_events(self) -> None:
        got = {r["K"]: (r["SumV"], r["N"]) for r in self.fetch(self.event_agg).to_pylist()}
        counts: dict[int, int] = {}
        self.run.checked("blink_input_agg")
        if {k: v[0] for k, v in got.items()} != self.event_sums or sum(v[1] for v in got.values()) != self.event_count:
            self.run.violate("correctness", "blink input aggregation",
                             {"got": got, "want_sums": self.event_sums, "want_rows": self.event_count, "counts": counts})

    def check_time(self) -> None:
        """The tables fed by the clock. With a console, one command takes static snapshots
        of them: a command holds the update graph's lock, so no cycle runs between two of
        its statements and the snapshots are of one cycle. Without a console they are only
        read: nothing a client can do sees two tables in the same cycle."""
        if not self.console:
            for name in ("tt_tail", "tt_agg", "tt_count", "tb_count"):
                self.fetch(self.time[name])
            self.run.checked("time_tables_read_only")
            return
        self.s.run_script(CONSOLE_SNAPSHOT)
        snap = {name: self.s.open_table(f"soak_snap_{name}") for name in ("tt_tail", "tt_agg", "tt_count", "tb_count", "ring", "ring_want")}
        expected = self.time_agg(snap["tt_tail"])
        for name, got, want, keys in (
            ("time_tail_agg", snap["tt_agg"], expected, ["M"]),
            ("ring_table", snap["ring"], snap["ring_want"], None),
        ):
            diff = difference(self.fetch(got), self.fetch(want), keys)
            self.run.checked(name)
            if diff is not None:
                self.run.violate("correctness", f"time table check {name}", diff)
        tail = self.fetch(snap["tt_tail"])
        self.run.checked("time_tail_rows")
        stamps = tail.column("S").to_pylist()
        step = self.period_ns // 1_000_000
        if tail.num_rows > 600 or any(b - a != step for a, b in zip(stamps, stamps[1:])):
            self.run.violate("correctness", "time table tail", {"rows": tail.num_rows, "first": stamps[:3], "last": stamps[-3:]})
        kept = self.fetch(snap["tt_count"]).column("N").to_pylist()
        want = (int(stamps[-1]) * 1_000_000 - self.start_ns) // self.period_ns + 1 if stamps else 0
        self.run.checked("time_count")
        if kept != [want]:
            self.run.violate("correctness", "time table check time_count", {"count": kept, "rows_to_the_last": want, "last": stamps[-3:]})
        blink = self.fetch(snap["tb_count"])
        first, last = (blink.column(c).cast(pa.int64()).to_pylist() for c in ("First", "Last"))
        shown = blink.column("N").to_pylist()
        self.run.checked("blink_time_count")
        if len(shown) != 1 or shown[0] != (last[0] - first[0]) // self.period_ns + 1:
            self.run.violate("correctness", "time table check blink_time_count", {"count": shown, "first": first, "last": last})
        expected.close()
        for t in snap.values():
            t.close()

    def check_rollup(self) -> None:
        """The rollup's root row and first level, read through a view as the web client
        reads them, against the aggregation of a snapshot of the trades."""
        from harness.views import read_view

        self.run.count(3)
        view = read_view(self.s, "soak_rollup")
        snap = self.inputs["trades"].snapshot()
        rows = self.fetch(snap)
        snap.close()
        want = {None: (pc.sum(rows.column("Qty")).as_py(), rows.num_rows)}
        for g in set(rows.column("Grp").to_pylist()):
            part = rows.filter(pc.equal(rows.column("Grp"), g))
            want[g] = (pc.sum(part.column("Qty")).as_py(), part.num_rows)
        got = {r["Grp"]: (r["Qty"], r["N"]) for r in view.to_pylist() if r.get("Sym") is None}
        self.run.checked("rollup")
        if got != want:
            self.run.violate("correctness", "rollup view", {"got": sorted(got.items(), key=str), "want": sorted(want.items(), key=str)})

    def cycle_stats(self) -> tuple[int, float]:
        """How many cycles the listener in the server has been called in, and the longest
        time between two of them since this was last asked."""
        self.s.run_script(CONSOLE_CYCLES)
        t = self.s.open_table("soak_cycle_stats")
        row = self.fetch(t).to_pylist()[0]
        t.close()
        return row["N"], row["MaxGap"]

    def console_round(self, n: int) -> None:
        """A script as a user runs them: tables made and dropped in a function, a global
        assigned again (the table it held is released), and results the script checks."""
        self.s.run_script(CONSOLE_ROUND.replace("ROUND", str(n)))
        self.run.checked("console_script")


CONSOLE_SETUP = """
from deephaven import agg as _soak_agg, ring_table as _soak_ring_table
soak_ring = _soak_ring_table(soak_tt, 256)
soak_rollup = soak_trades.rollup(aggs=[_soak_agg.sum_("Qty"), _soak_agg.count_("N")], by=["Grp", "Sym"])

# A listener the update graph calls in every cycle in which this table has a new row, which
# is every cycle: its period is a quarter of the cycle's. It keeps the count of cycles and
# the longest time between two of them.
import time as _soak_time
from deephaven import time_table as _soak_time_table
from deephaven.table_listener import listen as _soak_listen
_soak_cycles = {"n": 0, "last": None, "max_gap": 0.0}
def _soak_on_cycle(update, is_replay):
    now = _soak_time.monotonic()
    if _soak_cycles["last"] is not None:
        _soak_cycles["max_gap"] = max(_soak_cycles["max_gap"], now - _soak_cycles["last"])
    _soak_cycles["last"] = now
    _soak_cycles["n"] += 1
soak_tick = _soak_time_table(TICK_NS, blink_table=True)
soak_cycle_listener = _soak_listen(soak_tick, _soak_on_cycle)
"""

CONSOLE_CYCLES = """
from deephaven import new_table as _soak_new_table
from deephaven.column import long_col as _soak_long_col, double_col as _soak_double_col
soak_cycle_stats = _soak_new_table([_soak_long_col("N", [_soak_cycles["n"]]), _soak_double_col("MaxGap", [_soak_cycles["max_gap"]])])
_soak_cycles["max_gap"] = 0.0
"""

CONSOLE_SNAPSHOT = """
soak_snap_tt_tail = soak_tt_tail.snapshot()
soak_snap_tt_agg = soak_tt_agg.snapshot()
soak_snap_tt_count = soak_tt_count.snapshot()
soak_snap_tb_count = soak_tb_count.snapshot()
soak_snap_ring = soak_ring.snapshot()
soak_snap_ring_want = soak_tt.tail(256).snapshot()
"""

CONSOLE_ROUND = """
def _soak_round(n):
    from deephaven import empty_table
    t = empty_table(20000).update(["A = ii % 97", "B = ii * 2 + n"])
    s = t.where("A < 50").sum_by("A")
    assert s.size == 50, s.size
    j = t.head(1000).natural_join(s.view(["A", "SumB = B"]), on="A")
    assert j.size == 1000, j.size
    return t.size
assert _soak_round(ROUND) == 20000
soak_scratch = soak_trades.where("Grp == ROUND % 10").sort("Qty")
"""

# ---------------------------------------------------------------------------------------------
# A Barrage subscriber, as a dashboard is: one session that listens to two tables for the
# whole run.


class Subscriber:
    def __init__(self, run: Run, url: str, world: World):
        from pydeephaven_ticking.table_listener import TableListenerHandle, TableListener

        self.run = run
        self.s = open_session(url)
        self.lock = threading.Lock()
        self.updates = 0
        self.last_update = time.monotonic()
        self.max_gap = 0.0
        self.max_lag = 0.0
        self.replica: dict[str, tuple] = {}
        self.columns: list[str] = []
        outer = self

        class Clock(TableListener):
            """Counts the updates of the time table, whose period is the cycle: one update
            is one cycle seen. The newest row's timestamp against the clock is how far the
            update graph and the delivery are behind."""

            def on_update(self, update) -> None:
                now = time.monotonic()
                added = update.added(["Timestamp"])
                with outer.lock:
                    outer.updates += 1
                    outer.max_gap = max(outer.max_gap, now - outer.last_update)
                    outer.last_update = now
                    if added:
                        newest = pc.max(added["Timestamp"]).cast(pa.int64()).as_py()
                        outer.max_lag = max(outer.max_lag, (time.time_ns() - newest) / 1e9)

            def on_error(self, error: Exception) -> None:
                outer.run.violate("requests", "subscription to the time table failed", str(error)[:500])

        class Replica(TableListener):
            """Keeps the rows of the aggregation by `Sym` as the updates give them."""

            def on_update(self, update) -> None:
                removed, added, modified = update.removed(), update.added(), update.modified()
                with outer.lock:
                    for sym in (removed.get("Sym").to_pylist() if removed else []):
                        outer.replica.pop(sym, None)
                    for part in (added, modified):
                        if part:
                            names = list(part.keys())
                            outer.columns = names
                            for row in zip(*(part[n].to_pylist() for n in names)):
                                outer.replica[row[names.index("Sym")]] = row

            def on_error(self, error: Exception) -> None:
                outer.run.violate("requests", "subscription to the aggregation failed", str(error)[:500])

        self.tt = fetch_shared(self.s, world.shared["tt"])
        self.agg = fetch_shared(self.s, world.shared["agg_by_sym"])
        self.handles = [TableListenerHandle(self.tt, Clock()), TableListenerHandle(self.agg, Replica())]
        for h in self.handles:
            h.start()
        run.count(2)

    def stats(self) -> dict:
        with self.lock:
            return {"updates": self.updates, "max_gap": self.max_gap, "max_lag": self.max_lag,
                    "since_last": time.monotonic() - self.last_update}

    def check_replica(self, world: World) -> None:
        """After the barrier nothing changes the aggregation until the next write, so the
        rows the updates have built must come to equal a snapshot of it."""
        want_table = world.fetch(world.live["agg_by_sym"])
        deadline = time.monotonic() + 15
        while True:
            with self.lock:
                names = self.columns
                got = dict(self.replica)
            want = {}
            if names:
                for row in zip(*(want_table.column(n).to_pylist() for n in names)):
                    want[row[names.index("Sym")]] = row
            if names and got == want:
                break
            if time.monotonic() > deadline:
                keys = sorted(set(got) | set(want))
                bad = [k for k in keys if got.get(k) != want.get(k)][:3]
                self.run.violate("correctness", "barrage replica of agg_by_sym",
                                 {"differs_for": bad, "got": [got.get(k) for k in bad], "want": [want.get(k) for k in bad]})
                break
            time.sleep(0.05)
        self.run.checked("barrage_replica")

    def close(self) -> None:
        for h in self.handles:
            h.stop()
        self.s.close()


# ---------------------------------------------------------------------------------------------
# Churn: what comes and goes. Each round a new session makes tables, reads them, listens to
# one, and releases everything; some sessions are abandoned instead and left to expire.

CHURN_SCHEMA = pa.schema([("K", pa.int32()), ("V", pa.int64())])
CHURN_ROWS = 20_000
STATIC_ROWS = 50_000


def churn_menu(rng: random.Random) -> list[Callable[[Table, Table], Table]]:
    g = rng.randrange(10)
    return [
        lambda t, d: t.where(f"Grp != {g}"),
        lambda t, d: t.update(["N2 = Qty * 2", "P2 = Px + 1"]),
        lambda t, d: t.update_view(["Q3 = Qty % 3"]),
        lambda t, d: t.sort(["Px", "Id"]),
        lambda t, d: t.sort_descending(["Qty", "Id"]),
        lambda t, d: t.natural_join(d, on="Sym", joins=["JoinedWeight = Weight"]),
        lambda t, d: t.update_by([updateby.cum_sum(["Cum = Qty"]), updateby.rolling_sum_tick(["Roll = Qty"], rev_ticks=3)], by=["Sym"]),
        lambda t, d: t.tail(500),
        lambda t, d: t.head(700),
    ]


def churn_round(run: Run, url: str, world: World, rng: random.Random, n: int, leak: list) -> None:
    from pydeephaven_ticking.table_listener import TableListenerHandle, TableListener

    run.beat("churn: session")
    s = open_session(url)
    held: list[Table] = []
    trades = fetch_shared(s, world.shared["trades"])
    dims = fetch_shared(s, world.shared["dims"])

    # A chain of ticking operations over the shared tables, read once.
    run.beat("churn: ticking chain")
    menu = churn_menu(rng)
    t = trades
    for op in rng.sample(menu, 4):
        t = op(t, dims)
        held.append(t)
    final = t.agg_by([agg.sum_(["SumQty = Qty"]), agg.count_("N")], by=["Sym"])
    held.append(final)
    run.count()
    if final.to_arrow().num_rows > len(SYMS):
        run.violate("correctness", "churn chain", {"rows": final.to_arrow().num_rows})

    # Static tables of a size that would show if they were kept: results known in advance.
    run.beat("churn: static tables")
    big = s.empty_table(STATIC_ROWS).update(["K = (int)(ii % 100)", "V = ii", "D = ii * 0.25", 'S = "s" + (ii % 50)'])
    sums = big.where("V % 2 == 0").view(["K", "V"]).sum_by("K")
    top = big.sort_descending("V").head(5)
    joined = big.natural_join(sums.view(["K", "SumV = V"]), on="K")
    held += [big, sums, top, joined]
    run.count(2)
    total = pc.sum(sums.to_arrow().column("V")).as_py()
    first = top.to_arrow().column("V").to_pylist()
    want_total = sum(range(0, STATIC_ROWS, 2))
    run.checked("churn_static")
    if total != want_total or first != list(range(STATIC_ROWS - 1, STATIC_ROWS - 6, -1)) or joined.size != STATIC_ROWS:
        run.violate("correctness", "churn static tables", {"total": total, "want": want_total, "top": first, "joined": joined.size})

    # An append-only input table of the session's own, an aggregation and a running sum
    # over it, a listener on the aggregation, three appends; the result against the rows.
    run.beat("churn: input table")
    inp = s.input_table(schema=CHURN_SCHEMA)
    derived = inp.where("V % 3 != 0").agg_by([agg.sum_(["SumV = V"]), agg.count_("N"), agg.max_(["MaxV = V"])], by=["K"])
    running = inp.update_by([updateby.cum_sum(["Cum = V"])], by=["K"]).tail(50)
    held += [inp, derived, running]
    barrier = Barrier(s)
    seen = threading.Event()

    class Seen(TableListener):
        def on_update(self, update) -> None:
            seen.set()

        def on_error(self, error: Exception) -> None:
            run.violate("requests", "churn subscription failed", str(error)[:500])

    handle = TableListenerHandle(derived, Seen())
    handle.start()
    run.count()
    want: dict[int, list[int]] = {}
    for b in range(3):
        ks = [rng.randrange(64) for _ in range(CHURN_ROWS)]
        vs = [n * 1_000_000 + b * CHURN_ROWS + i for i in range(CHURN_ROWS)]
        run.count()
        batch = s.import_table(pa.Table.from_arrays([pa.array(ks, pa.int32()), pa.array(vs, pa.int64())], schema=CHURN_SCHEMA))
        inp.add(batch)
        batch.close()
        for k, v in zip(ks, vs):
            if v % 3 != 0:
                w = want.setdefault(k, [0, 0, v])
                w[0] += v
                w[1] += 1
                w[2] = max(w[2], v)
    barrier.wait()
    run.count(2)
    got = {r["K"]: [r["SumV"], r["N"], r["MaxV"]] for r in derived.to_arrow().to_pylist()}
    run.checked("churn_ticking")
    if got != want:
        bad = [k for k in sorted(set(got) | set(want)) if got.get(k) != want.get(k)][:3]
        run.violate("correctness", "churn input table aggregation", {"keys": bad, "got": [got.get(k) for k in bad], "want": [want.get(k) for k in bad]})
    if running.to_arrow().num_rows != 50:
        run.violate("correctness", "churn running sum", {"rows": running.to_arrow().num_rows})
    if not seen.wait(15):
        run.violate("liveness", "churn subscription saw no update in 15s", None)
    handle.stop()

    run.beat("churn: release")
    if leak is not None:  # --inject leak: this round's tables and session are kept
        leak.append((s, held, barrier))
        return
    for t in reversed(held):
        t.close()
    for t in barrier.rows + [barrier.table]:
        t.close()
    s.close()


class Abandoned:
    """Sessions that stop without closing: a client that crashed. They hold tables; the
    server ends each when its token has run out and must release what it held."""

    def __init__(self, run: Run, url: str):
        self.run, self.url = run, url
        self.sessions: list[tuple[float, Session, list[Table]]] = []
        self.made = 0

    def abandon(self) -> None:
        from harness.connect import PSK_AUTH_TYPE, parse_url

        server = parse_url(self.url)
        s = Session(host=server.host, port=server.port, auth_type=PSK_AUTH_TYPE, auth_token=server.psk, never_timeout=False)
        t = s.empty_table(STATIC_ROWS).update(["K = (int)(ii % 100)", "V = ii", 'S = "s" + (ii % 50)'])
        self.sessions.append((time.monotonic(), s, [t, t.sort("K"), t.view(["K", "V"]).sum_by("K")]))
        self.made += 1

    def forget_expired(self) -> None:
        """Drops the client's side of sessions the server has ended by now, without a call:
        the tables are marked closed and the channel is closed."""
        keep = []
        for made, s, tables in self.sessions:
            if time.monotonic() - made < 2.5 * TOKEN_EXPIRE_MS / 1000:
                keep.append((made, s, tables))
                continue
            for t in tables:
                t._closed = True
            s.is_connected = False
            if s.grpc_channel:
                s.grpc_channel.close()
            if s._flight_client:
                s._flight_client.close()
        self.sessions = keep


# ---------------------------------------------------------------------------------------------
# Judging the measurements.


def theil_sen(xs: list[float], ys: list[float]) -> float:
    """The median of the slopes between every two points: a line fit that a few outliers
    (a sample taken while a large table was alive) do not move."""
    n = len(xs)
    if n < 2:
        return 0.0
    # At most about 200,000 pairs: every k-th point when there are more.
    step = max(1, n // 600)
    xs, ys = xs[::step], ys[::step]
    slopes = [(ys[j] - ys[i]) / (xs[j] - xs[i]) for i in range(len(xs)) for j in range(i + 1, len(xs)) if xs[j] != xs[i]]
    return statistics.median(slopes) if slopes else 0.0


def third(values: list[float], which: int) -> list[float]:
    n = len(values)
    return values[which * n // 3:(which + 1) * n // 3] or values


def judge(rounds: list[dict], profile: Profile, subscriber: dict, cycle_ms: int) -> tuple[dict, list[dict]]:
    """The trend statistics of a run and the invariants they violate. `rounds` holds one
    sample per round, taken at its end, when the round's own tables are released."""
    out: dict[str, Any] = {}
    bad: list[dict] = []

    def violate(invariant: str, what: str, detail: Any) -> None:
        bad.append({"t": None, "invariant": invariant, "what": what, "detail": detail})

    steady = [r for r in rounds if r["t"] >= profile.warmup]
    out["rounds"], out["rounds_after_warmup"] = len(rounds), len(steady)
    if len(steady) < 8:
        violate("memory", "too few rounds after the warm-up to fit a trend", {"rounds": len(steady)})
        return out, bad
    ts = [r["t"] for r in steady]
    window = ts[-1] - ts[0]
    rss = [r["rss_kb"] / 1024 for r in steady]
    slope = theil_sen(ts, rss) * 3600
    out["rss_mb"] = {
        "start": round(rounds[0]["rss_kb"] / 1024, 1), "after_warmup": round(rss[0], 1), "end": round(rss[-1], 1),
        "median_first_third": round(statistics.median(third(rss, 0)), 1),
        "median_last_third": round(statistics.median(third(rss, 2)), 1),
        "peak": round(max(r["hwm_kb"] for r in rounds) / 1024, 1),
        "slope_mb_per_hour": round(slope, 2), "allowed_mb_per_hour": profile.rss_slope_mb_per_hour,
        "fit_window_s": round(window, 1),
    }
    if slope > profile.rss_slope_mb_per_hour:
        violate("memory", "resident memory grows", out["rss_mb"])

    cpu = [r["cpu_round_s"] for r in steady]
    cpu_slope = theil_sen(ts, cpu)
    growth = cpu_slope * window / max(statistics.median(cpu), 1e-9)
    out["cpu"] = {
        "total_s": round(rounds[-1]["cpu_s"], 1), "per_round_median_s": round(statistics.median(cpu), 3),
        "per_round_first_third_s": round(statistics.median(third(cpu, 0)), 3),
        "per_round_last_third_s": round(statistics.median(third(cpu, 2)), 3),
        "growth_over_window": round(growth, 3), "allowed_growth": profile.cpu_growth,
    }
    if growth > profile.cpu_growth:
        violate("liveness", "processor time per round climbs", out["cpu"])

    for what in ("fds", "threads"):
        values = [r[what] for r in steady]
        base = statistics.median(third(values, 0))
        last = statistics.median(third(values, 2))
        out[what] = {"baseline": base, "last_third": last, "max": max(values), "allowed_above_baseline": profile.handles_slack}
        if last > base + profile.handles_slack:
            violate("memory", f"{what} do not return to their baseline", out[what])

    barrier = [r["barrier_s"] for r in rounds]
    first, last = statistics.median(third(barrier, 0)), statistics.median(third(barrier, 2))
    out["barrier_s"] = {"median": round(statistics.median(barrier), 4), "max": round(max(barrier), 4),
                        "median_first_third": round(first, 4), "median_last_third": round(last, 4),
                        "allowed_max": profile.barrier_max}
    if max(barrier) > profile.barrier_max:
        violate("liveness", "a barrier took too long", out["barrier_s"])
    if last > profile.barrier_growth * first + 0.05:
        violate("liveness", "barriers get slower", out["barrier_s"])

    seconds = rounds[-1]["t"]
    # The server sends a subscriber at most one update a second
    # (BarrageRequestHelpers#DEFAULT_MIN_UPDATE_INTERVAL_MS), whatever the cycle.
    out["subscriber"] = {
        "updates": subscriber["updates"], "expected_about": int(seconds),
        "max_gap_s": round(subscriber["max_gap"], 3), "max_lag_s": round(subscriber["max_lag"], 3),
        "allowed_gap_s": profile.update_gap_max, "allowed_lag_s": profile.lag_max,
    }
    if subscriber["max_gap"] > profile.update_gap_max:
        violate("liveness", "the time table went too long without an update", out["subscriber"])
    if subscriber["max_lag"] > profile.lag_max:
        violate("liveness", "the time table fell behind the clock", out["subscriber"])
    if subscriber["updates"] < seconds / 5:
        violate("liveness", "far fewer updates than one a second", out["subscriber"])
    # Cycles are counted by a listener in the server, so only with a console.
    if rounds[-1].get("cycles") is not None:
        gaps = [r["cycle_gap_s"] for r in rounds]
        first, last = statistics.median(third(gaps, 0)), statistics.median(third(gaps, 2))
        out["cycles"] = {
            "count": rounds[-1]["cycles"], "expected_about": int(seconds * 1000 / cycle_ms),
            "max_gap_s": round(max(gaps), 3), "round_max_gap_first_third_s": round(first, 3),
            "round_max_gap_last_third_s": round(last, 3), "allowed_gap_s": profile.cycle_gap_max,
        }
        if max(gaps) > profile.cycle_gap_max:
            violate("liveness", "a cycle took too long to come", out["cycles"])
        if last > profile.barrier_growth * first + 0.05:
            violate("liveness", "cycles get slower", out["cycles"])
        if rounds[-1]["cycles"] < out["cycles"]["expected_about"] / 2:
            violate("liveness", "fewer than half the cycles the period allows", out["cycles"])
    return out, bad


# ---------------------------------------------------------------------------------------------
# The run.


def watchdog(run: Run, server: Server, limit: float, write_result: Callable[[str], None]) -> None:
    """Ends the run when one step of the workload has not finished in `limit` seconds: a
    request that never returns is a failure, not a wait."""
    while True:
        time.sleep(2)
        at, what = run.heartbeat
        if what == "done":
            return
        if time.monotonic() - at > limit:
            run.violate("liveness", f"stalled in step: {what}", {"seconds": round(time.monotonic() - at, 1)})
            faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
            write_result("stalled")
            server.kill()
            print("soak stalled: the server was killed; the stacks of the driver's threads are above", flush=True)
            os._exit(1)


# The parts of a round that --skip leaves out, to find which one a trend comes from.
SKIPPABLE = {"write", "check", "script", "churn", "abandon"}


def scan_log(path: str) -> list[str]:
    """Lines of the server's log that report an error or a panic."""
    with open(path, "rb") as f:
        lines = f.read().decode("utf-8", "replace").splitlines()
    return [line[:400] for line in lines if " ERROR " in line or "panicked" in line][:20]


def result_text(result: dict, rounds: list[dict]) -> str:
    """The result file: everything but the rounds indented, and the rounds as one line
    each under the names of their columns, so that the file stays small and a round is a
    line of a diff."""
    text = json.dumps(result, indent=1, default=str)
    columns = list(rounds[0]) if rounds else []
    rows = ",\n".join("  " + json.dumps([r.get(c) for c in columns]) for r in rounds)
    return f'{text[:-2]},\n "rounds": {{"columns": {json.dumps(columns)}, "rows": [\n{rows}\n ]}}\n}}\n'


def run_soak(args: argparse.Namespace) -> int:
    profile = PROFILES[args.profile]
    if args.duration is not None:
        scale = args.duration / profile.duration
        profile = Profile(**{**profile.__dict__, "duration": args.duration,
                             "warmup": args.warmup if args.warmup is not None else profile.warmup * min(1.0, scale)})
    elif args.warmup is not None:
        profile = Profile(**{**profile.__dict__, "warmup": args.warmup})
    faulthandler.enable()
    faulthandler.register(signal.SIGUSR1, all_threads=True)  # kill -USR1: every thread's stack
    log_path = args.server_log or os.path.join(tempfile.mkdtemp(prefix="soak-"), "server.log")
    run = Run()
    count_and_bound_client_calls(run)
    server = Server(args.binary, args.cycle_ms, log_path, args.console)
    print(f"server pid {server.pid}, log {log_path}", flush=True)
    result: dict[str, Any] = {
        "meta": {
            "started": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "profile": args.profile, "duration_s": profile.duration, "warmup_s": profile.warmup,
            "seed": args.seed, "cycle_ms": args.cycle_ms, "round_s": args.round_seconds,
            "cpus": os.cpu_count(), "loadavg_start": os.getloadavg(), "inject": args.inject,
            "write_steps": WRITE_STEPS,
        },
    }
    skip = set(filter(None, args.skip.split(",")))
    if skip - SKIPPABLE:
        sys.exit(f"--skip: unknown part {sorted(skip - SKIPPABLE)}; the parts are {sorted(SKIPPABLE)}")
    result["meta"]["skip"] = sorted(skip)
    rounds: list[dict] = []
    state: dict[str, Any] = {"subscriber": {"updates": 0, "max_gap": 0.0, "max_lag": 0.0}}

    def write_result(status: str) -> None:
        result["status"] = status
        result["violations"] = run.violations
        result["known_failures"] = run.known
        result["checks"] = dict(run.checks)
        result["requests"] = run.requests
        with open(args.out, "w") as f:
            f.write(result_text(result, rounds))

    threading.Thread(target=watchdog, args=(run, server, args.step_timeout, write_result), daemon=True).start()
    world = subscriber = None
    abandoned = Abandoned(run, server.url)
    leak: list | None = [] if args.inject == "leak" else None
    churn_rng = random.Random(args.seed + 1)
    try:
        # Whether the server has a console is asked, not assumed: a script that fails later
        # is then a failure of the run.
        probe = open_session(server.url)
        try:
            probe.run_script("soak_probe = 1")
            console = True
        except Exception as e:  # noqa: BLE001 - the server says it has no console
            console = False
            print(f"no console: {failure(e).get('message', e)}", flush=True)
        probe.close()
        result["meta"]["console"] = console
        world = World(run, server.url, args.seed, args.cycle_ms, console)
        subscriber = Subscriber(run, server.url, world)
        base = server.sample()
        last_cpu = base["cpu_s"]
        n = 0
        while run.now() < profile.duration and not run.too_many():
            round_start = time.monotonic()
            n += 1

            def step(name: str, fn: Callable[[], None]) -> None:
                run.beat(name)
                try:
                    fn()
                except Exception as e:  # noqa: BLE001 - every failed request is a violation
                    run.violate("requests", f"{name} failed", failure(e))

            if "write" not in skip:
                step("write", world.write)
            if "check" not in skip:
                step("check pipelines", lambda: world.check_pipelines(args.inject == "mismatch" and n == 3))
                step("check blink input", world.check_events)
                step("check replica", lambda: subscriber.check_replica(world))
                step("check time tables", world.check_time)
                if console:
                    step("check rollup", world.check_rollup)
            if console and "script" not in skip:
                step("console script", lambda: world.console_round(n))
            if "churn" not in skip:
                step("churn", lambda: churn_round(run, server.url, world, churn_rng, n, leak))
            if n % 8 == 0 and "abandon" not in skip:
                step("abandon a session", abandoned.abandon)
            abandoned.forget_expired()
            if args.inject == "stall" and n == 3:
                run.beat("injected stall")
                time.sleep(args.step_timeout + 10)
            collect_garbage()
            # The quiet point: this round's tables are released and a cycle has passed.
            run.beat("barrier")
            t0 = time.monotonic()
            try:
                world.barrier.wait()
            except Exception as e:  # noqa: BLE001
                run.violate("requests", "barrier failed", failure(e))
            barrier_s = time.monotonic() - t0
            work = time.monotonic() - round_start
            run.beat("sleep")
            time.sleep(max(0.0, args.round_seconds - work))
            sample = server.sample()
            if sample is None:
                run.violate("liveness", "the server process is gone", {"exit_code": server.proc.poll()})
                break
            stats = subscriber.stats()
            state["subscriber"] = stats
            cycles: dict[str, Any] = {}
            if console:
                try:
                    count, gap = world.cycle_stats()
                    cycles = {"cycles": count, "cycle_gap_s": round(gap, 4)}
                except Exception as e:  # noqa: BLE001
                    run.violate("requests", "reading the cycle count failed", failure(e))
                    cycles = {"cycles": rounds[-1]["cycles"] if rounds else 0, "cycle_gap_s": 0.0}
            rounds.append({"t": run.now(), "work_s": round(work, 3), "barrier_s": round(barrier_s, 4), **cycles,
                           "cpu_round_s": round(sample["cpu_s"] - last_cpu, 3), **sample,
                           "updates": stats["updates"], "load": os.getloadavg()[0]})
            last_cpu = sample["cpu_s"]
            if n % 15 == 0:
                print(f"t={run.now():7.0f}s round {n} rss={sample['rss_kb'] / 1024:.1f}MB cpu={sample['cpu_s']:.0f}s "
                      f"cycles={cycles.get('cycles', '-')} updates={stats['updates']} requests={run.requests} "
                      f"violations={len(run.violations)}", flush=True)
                write_result("running")
        result["meta"]["abandoned_sessions"] = abandoned.made
    except Exception as e:  # noqa: BLE001 - the set-up itself failed
        import traceback

        traceback.print_exc()
        run.violate("requests", "the workload could not continue", failure(e))
    finally:
        run.beat("closing")
        try:
            if subscriber is not None:
                state["subscriber"] = subscriber.stats()
                subscriber.close()
            if world is not None:
                world.s.close()
        except Exception as e:  # noqa: BLE001
            run.violate("requests", "closing the long-lived sessions failed", failure(e))
        result["shutdown"] = server.stop()
        run.beat("done")
    if not result["shutdown"]["clean"]:
        run.violate("shutdown", "the server did not stop cleanly", result["shutdown"])
    errors = scan_log(log_path)
    result["server_log_errors"] = errors
    if errors:
        run.violate("requests", "the server logged errors", errors[:5])
    result["meta"]["loadavg_end"] = os.getloadavg()
    result["meta"]["seconds"] = run.now()
    result["summary"], trend = judge(rounds, profile, state["subscriber"], args.cycle_ms) if rounds else ({}, [])
    for v in trend:
        run.violate(v["invariant"], v["what"], v["detail"])
    write_result("failed" if run.violations else "passed")
    print_summary(result)
    return 1 if run.violations else 0


def print_summary(result: dict) -> None:
    meta, s = result["meta"], result.get("summary", {})
    print()
    print(f"soak {result['status']}: profile {meta['profile']}, {meta.get('seconds', 0):.0f}s "
          f"({meta['warmup_s']:.0f}s warm-up), seed {meta['seed']}, cycle {meta['cycle_ms']}ms, "
          f"console {'yes' if meta.get('console') else 'no'}, "
          f"{'without ' + ', '.join(meta['skip']) + ', ' if meta.get('skip') else ''}{meta['cpus']} cpus, "
          f"load {meta['loadavg_start'][0]:.1f} -> {meta.get('loadavg_end', [0])[0]:.1f}")
    if s.get("rss_mb"):
        r, c, sb, b, cy = s["rss_mb"], s["cpu"], s["subscriber"], s["barrier_s"], s.get("cycles")
        print(f"  rounds {s['rounds']}, requests {result['requests']}, checks {sum(result['checks'].values())} "
              f"({len(result['checks'])} kinds), sessions abandoned {meta.get('abandoned_sessions', 0)}")
        if cy:
            print(f"  cycles {cy['count']} (the period alone gives about {cy['expected_about']}; a write asks for one), longest time between two "
                  f"{cy['max_gap_s']}s (allowed {cy['allowed_gap_s']}), per-round longest "
                  f"{cy['round_max_gap_first_third_s']}s -> {cy['round_max_gap_last_third_s']}s")
        else:
            print("  cycles: not counted (the server has no console to count them in)")
        print(f"  subscriber: {sb['updates']} updates, longest gap {sb['max_gap_s']}s, furthest behind {sb['max_lag_s']}s")
        print(f"  rss MB: start {r['start']}, after warm-up {r['after_warmup']}, end {r['end']}, peak {r['peak']}, "
              f"slope {r['slope_mb_per_hour']} MB/h (allowed {r['allowed_mb_per_hour']})")
        print(f"  cpu: {c['total_s']}s, per round {c['per_round_first_third_s']}s -> {c['per_round_last_third_s']}s "
              f"(growth {c['growth_over_window']:+.1%}, allowed {c['allowed_growth']:.0%})")
        print(f"  barrier: median {b['median'] * 1000:.1f}ms, max {b['max'] * 1000:.0f}ms; "
              f"fds {s['fds']['baseline']:.0f} -> {s['fds']['last_third']:.0f}, threads {s['threads']['baseline']:.0f} -> {s['threads']['last_third']:.0f}")
    sd = result.get("shutdown", {})
    print(f"  shutdown: {sd.get('how')}, exit {sd.get('exit_code')}, {sd.get('seconds')}s; "
          f"server log errors {len(result.get('server_log_errors', []))}")
    for k in result.get("known_failures", [])[:10]:
        print(f"  KNOWN FAILURE [{k['invariant']}] {k['what']} ({k['known']})")
    print(f"  violations: {len(result['violations'])}")
    for v in result["violations"][:20]:
        print(f"    [{v['invariant']}] {v['what']}: {json.dumps(v['detail'], default=str)[:300]}")


def report(args: argparse.Namespace) -> int:
    with open(args.file) as f:
        result = json.load(f)
    print_summary(result)
    return 1 if result["violations"] or result["status"] != "passed" else 0


def selftest(_: argparse.Namespace) -> int:
    """The judging on made-up series: a steady run passes, and each kind of trouble is
    caught. A check that cannot fail is no evidence."""
    profile = PROFILES["long"]
    rng = random.Random(7)

    def series(rss_per_hour=0.0, cpu_factor=1.0, barrier_end=0.02, fds_end=40, spike=False, cycle_share=1.0, cycle_gap_end=0.0):
        rows = []
        for i in range(600):
            t = i * 4.5
            frac = i / 600
            rows.append({
                "t": t, "rss_kb": int((150 + rng.uniform(-3, 3) + rss_per_hour * t / 3600 + (60 if spike and i % 97 == 0 else 0)) * 1024),
                "hwm_kb": 260 * 1024, "cpu_s": i * 0.5, "cpu_round_s": 0.5 * (1 + (cpu_factor - 1) * frac) + rng.uniform(-0.05, 0.05),
                "fds": 40 + int((fds_end - 40) * frac), "threads": 12,
                "barrier_s": 0.02 + (barrier_end - 0.02) * frac + rng.uniform(0, 0.005),
                "cycles": int(t * 10 * cycle_share), "cycle_gap_s": 0.12 + cycle_gap_end * frac + rng.uniform(0, 0.02),
            })
        return rows

    sub = {"updates": 2700, "max_gap": 1.1, "max_lag": 0.5}
    cases = [
        ("steady", series(), sub, []),
        ("steady with spikes", series(spike=True), sub, []),
        ("leak of 60 MB per hour", series(rss_per_hour=60), sub, ["resident memory grows"]),
        ("processor time up by half", series(cpu_factor=1.5), sub, ["processor time per round climbs"]),
        ("barriers twenty times slower", series(barrier_end=0.4), sub, ["barriers get slower"]),
        ("open files climb", series(fds_end=80), sub, ["fds do not return to their baseline"]),
        ("subscriber starved", series(), {"updates": 2000, "max_gap": 30.0, "max_lag": 31.0},
         ["the time table went too long without an update", "the time table fell behind the clock"]),
        ("subscriber gets a tenth of the updates", series(), {"updates": 270, "max_gap": 1.1, "max_lag": 0.5}, ["far fewer updates than one a second"]),
        ("one cycle in three runs", series(cycle_share=0.33), sub, ["fewer than half the cycles the period allows"]),
        ("cycles come ever later", series(cycle_gap_end=1.0), sub, ["cycles get slower"]),
        ("a cycle never came", series(cycle_gap_end=12.0), sub, ["a cycle took too long to come", "cycles get slower"]),
    ]
    failed = 0
    for name, rows, subscriber, want in cases:
        got = [v["what"] for v in judge(rows, profile, subscriber, 100)[1]]
        ok = got == want
        failed += not ok
        print(f"{'ok  ' if ok else 'FAIL'} {name}: {got or 'no violation'}")
    a = pa.table({"K": [1, 2, 3], "V": [1.0, 2.0, 3.0]})
    comparisons = [
        ("equal tables", difference(a, a, None) is None),
        ("same rows in another order, as a set", difference(a, a.take([2, 0, 1]), ["K"]) is None),
        ("same rows in another order, in order", difference(a, a.take([2, 0, 1]), None) is not None),
        ("one value differs", difference(a, pa.table({"K": [1, 2, 3], "V": [1.0, 2.0, 3.5]}), ["K"]) is not None),
        ("a row is missing", difference(a, a.slice(0, 2), ["K"]) is not None),
        ("a type differs", difference(a, pa.table({"K": [1, 2, 3], "V": [1, 2, 3]}), ["K"]) is not None),
    ]
    for name, ok in comparisons:
        failed += not ok
        print(f"{'ok  ' if ok else 'FAIL'} comparison: {name}")
    return 1 if failed else 0


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--binary", required=True, help="the oxhaven-server to start")
    r.add_argument("--profile", choices=sorted(PROFILES), default="long")
    r.add_argument("--duration", type=float, help="seconds of workload, instead of the profile's")
    r.add_argument("--warmup", type=float, help="seconds left out of the trend fits, instead of the profile's")
    r.add_argument("--seed", type=int, default=1)
    r.add_argument("--cycle-ms", type=int, default=100, help="the server's --cycle-ms and the time tables' period")
    r.add_argument("--round-seconds", type=float, default=2.0, help="one round of the workload starts this often")
    r.add_argument("--step-timeout", type=float, default=STEP_TIMEOUT,
                   help="seconds one step of the workload may take before the run is ended as stalled")
    r.add_argument("--console", choices=["auto", "python", "none"], default="auto", help="the server's --console")
    r.add_argument("--server-log", help="where the server's output goes (default: a temporary directory)")
    r.add_argument("--inject", choices=["leak", "mismatch", "stall"],
                   help="break one thing on purpose, to see the run fail: keep every round's tables, "
                        "recompute one check from the wrong rows, or stop making progress")
    r.add_argument("--skip", default="", help="comma-separated parts of a round to leave out, to find where a trend "
                   "comes from: write, check, script, churn, abandon")
    r.add_argument("--out", required=True)
    rp = sub.add_parser("report")
    rp.add_argument("file")
    sub.add_parser("selftest")
    args = p.parse_args(argv[1:])
    code = {"run": run_soak, "report": report, "selftest": selftest}[args.cmd](args)
    if args.cmd == "run":
        # Not through the interpreter's shutdown: it would finalize the client's tables of
        # sessions the server has ended, each with a call that waits for its deadline.
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(code)
    return code


if __name__ == "__main__":
    sys.exit(main(sys.argv))
