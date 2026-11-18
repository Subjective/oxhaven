# Soak test

Every other check of this repository is short: a unit test, a seed, a replayed scenario, a
benchmark case. The soak test runs one server for a long time under a ticking workload and
checks, while it runs, that results stay right, that memory and processor time stay flat, that
the update cycle keeps up, and that the server stops cleanly. It exists to find what only time
shows: a slow leak, a table that is never released, a cycle that falls behind, a result that
drifts after many cycles.

The driver is `bench/soak.py` (task 12.3, `tasks/12.3-soak-test.md`). It starts the server,
drives it through the Python client as `bench/bench.py` does, and measures the process from
outside, in `/proc/<pid>`: the server's own memory report (`GetHeapInfo`) is not used, because
with mimalloc it counts only part of the memory (`docs/CONSOLE.md`, "The memory figures").

## How to run it

```bash
# The client's environment: the oracle's pins, in a venv anywhere.
python3 -m venv /tmp/soak-venv && /tmp/soak-venv/bin/pip install -r oracle/requirements-replay.txt

# The server as it is deployed: a release build with the Python console. The Python on PATH
# when it is built is the one it embeds, and must be the one the venv was made from.
cargo build --release --locked -p oxhaven-server --features python

# 45 minutes; exits 0 when every invariant held.
VIRTUAL_ENV=/tmp/soak-venv /tmp/soak-venv/bin/python bench/soak.py run \
    --binary target/release/oxhaven-server --profile long --out soak.json

# 2 minutes, the same invariants with looser statistics; any build of the server.
VIRTUAL_ENV=/tmp/soak-venv /tmp/soak-venv/bin/python bench/soak.py run \
    --binary target/debug/oxhaven-server --profile short --out soak-short.json

/tmp/soak-venv/bin/python bench/soak.py report soak.json   # the summary again, from the file
/tmp/soak-venv/bin/python bench/soak.py selftest           # the judging, on made-up series
```

`run` starts the server on a free port of 127.0.0.1 with a random key, `--cycle-ms 100` and
`--token-expire-ms 30000`, and stops it with SIGINT at the end. Options: `--duration` and
`--warmup` (seconds, instead of the profile's), `--seed` (default 1), `--cycle-ms`,
`--round-seconds` (default 2), `--console python|none` (default: what the server was built
with), `--server-log <file>`, `--step-timeout`, `--inject` and `--skip` (both below). A server
built without the `python` feature can be soaked too; the checks that need the console say
that they did not run.

The run is deterministic in what it sends: the rows, the choice of operations and their order
come from `--seed`. What the clock decides (which cycle a write lands in, the rows of the time
tables) is not, and no check depends on it.

## The workload

One round every two seconds; a round does all of the following and then waits for its period
to end, so the load is the same from the first round to the last.

- **A long-lived graph over three keyed input tables** (`trades` with 2,000 possible keys,
  `quotes`, `dims`), written by one session: each round adds and changes rows in all three and
  deletes rows from `trades`, ten times over (about 1,200 trades written and 700 deleted a
  round). The key spaces are fixed, so the tables stop growing once the
  keys have been written (about 1,100 trades at any time) and a steady state exists. Thirteen
  chains of ticking operations hang off them: `where`, `update`, `where` + `update_view`,
  `agg_by` with six aggregations by one key and by two, `sort` + `last_by`, `natural_join`,
  `aj`, `sort` + `update_by` (cumulative sum and maximum, rolling sum), `sort`,
  `sort_descending` + `head`, a join, formula, aggregation and sort in a row, and
  `select_distinct`.
- **A blink input table** with an aggregation over it, which keeps every row's contribution
  after the row is gone.
- **Time tables**: one that keeps its rows and a blink one, both with the cycle's period;
  `tail(600)` + `update` + `agg_by` over the first, a count over each. With a console also a
  `ring_table` over the first, a `rollup` over `trades`, and a listener in the server that
  counts cycles.
- **A subscriber**: a second session that holds two Barrage subscriptions for the whole run
  (through `pydeephaven_ticking`), to a time table and to the aggregation by `Sym`, and keeps
  a copy of the aggregation from the updates it receives.
- **Churn**: each round a new session fetches the shared tables, builds a chain of four
  ticking operations picked by the seed and an aggregation of it, reads it; makes static tables
  of 50,000 rows with a filter, an aggregation, a sort and a join; makes an append-only input
  table of its own, an aggregation and a running sum over it, subscribes to the aggregation,
  appends 60,000 rows in three batches; then releases every table and closes the session.
  The tables are sized so that one that stayed behind would show: a round holds several
  megabytes for a moment.
- **Abandoned sessions**: every eighth round a session makes three tables of 50,000 rows and
  is never closed and never refreshes its token, as a client that crashed. The server must end
  it when its token runs out (30 seconds) and release what it held.
- **A console session** (if the server has a console): each round a script makes and drops
  tables in a function, checks their sizes with `assert`, and assigns a ticking table to a
  global that held last round's.

## What is checked

A violated invariant is recorded with the time and what was seen, the run goes on (up to 40
violations), and the exit code is 1.

### Correctness

Checked every round, so a result is compared about 1,300 times in a long run.

- **Each ticking chain against a recomputation.** The writing session is the only writer of
  the input tables. After its writes it waits for the barrier (two changes to a private input
  table; when the second returns, a cycle that started after the writes has finished), takes a
  static `snapshot()` of each input table, runs the chain's operations on the snapshots, and
  compares the ticking result with the static one, exactly: same schema, same rows, and the
  same order unless the chain is an aggregation (whose groups are in order of first appearance,
  which a recomputation from the rows as they are now does not reproduce; those are compared
  as sets by their key). Nothing writes between the barrier and the reads, so both are of the
  same cycle. This is the check the engine's tests make with `ConsistencyChecker` and a
  recompute closure (`crates/oxhaven-testkit`), made on a server after thousands of cycles.
  Sums and averages are over whole numbers and prices are multiples of a quarter, so the two
  are equal to the last bit and no tolerance is needed.
- **The tables fed by the clock, in one cycle.** No client call sees two tables in the same
  cycle, but a console command holds the update graph's lock for its whole length
  (`docs/CONSOLE.md`, "Listeners, locks, liveness scopes and execution contexts"). One command
  takes static snapshots of the time-table chain's input and its result, of the two counts,
  and of the ring table and `tail(256)` of its source. The aggregation is recomputed from the
  input's snapshot; the ring table must equal the tail; the tail's timestamps must be one
  period apart; and the count over the blink table must stay exactly as far behind the other
  count as it was at the first check. (It is behind by the rows the blink table showed before
  its count existed: none when the two requests fall in one cycle, one on a slow server.)
  Without a console these tables are only read.
- **What the driver knows.** The blink input table's aggregation against the sums of every
  row ever added; the churn session's aggregation against its 60,000 rows; the static tables
  against values known in advance.
- **The Barrage copy.** After the barrier, the copy the subscriber has built from updates
  must come to equal a snapshot of the aggregation (within 15 seconds; an update is sent at
  most once a second).
- **The rollup**, with a console: its root row and first level, read through a view as the
  web client reads them, against the aggregation of a snapshot of `trades`.

**No failed table, no failed request.** The workload expects no request to fail, so any that
does is a violation, with the status code and message. A table that has failed fails the
requests that read it, and the subscriptions report it. A line of the server's log at `ERROR`,
or one that reports a panic, is a violation too: the update graph logs a cycle that failed.

### Memory

One sample per round, taken at its end: the round's tables are released, the session is
closed, and a cycle has passed. A line is fitted to the resident set size (`VmRSS`) of the
samples after the warm-up, by the Theil-Sen method (the median of the slopes between every two
samples), which a few high samples do not move. Its slope must not exceed the profile's limit:

| profile | warm-up | limit | what it catches |
| --- | ---: | ---: | --- |
| `long` | 15 min | 24 MB per hour | 13 KB kept per round: a table of a few hundred rows, a session, a subscription, or 11 bytes per trade written |
| `short` | 40 s | 1,500 MB per hour | 830 KB kept per round: one of the churn's 50,000-row tables |

The limits come from the noise. Between two quiet points the resident size moves by 10 to
20 MB without any trend, as the allocator returns freed pages to the system late and in
pieces. Over the 30 minutes the long profile fits, two runs side by side, with the same
seed, measured -9 MB per hour on the server as it is and +54 on the server with the leak
described below: 24 is between the two with room on both sides. Over windows of 80 seconds,
which is what the short profile fits, the slopes of the first long run ranged from -790 to
+1,200 MB per hour, so the short limit is above the largest. A run with `--inject leak`,
which keeps every round's tables, measured 16,700 MB per hour.

The warm-up of the long profile is long because resident memory takes long to level: it
rises in steps for the first 9 to 13 minutes (155 to about 173 MB in the run above) and
then stays.

Open file descriptors and threads of the process are sampled with it: the median of the last
third of the run may be at most 8 above the median of the first third after the warm-up. A
connection or a thread per session that is never given back shows here before it shows in
memory.

**The count of live exports and sessions is not checked**, because the server does not expose
it: there is no call that says how many sessions exist or how many exports a session
holds. What would be needed is a small diagnostic, for example
two numbers in the server's log at an interval or a field of a console object, counted where
`SessionStore` and `Session` keep them. Until then a kept export is found by its memory, which
is why the churn's tables are large.

### Liveness

- **The cycle keeps up.** With a console, a listener in the server (`deephaven.table_listener`
  on a blink time table whose period is a quarter of the cycle's, so it is called in every
  cycle) counts the cycles and keeps the longest time between two. That time must stay under
  the profile's limit (5 s long, 10 s short: the machine is shared, and a cycle that takes
  seconds is the failure, not one that takes 150 ms), the count must be at least half of what
  the period gives, and the per-round longest time of the last third of the run may be at most
  twice (short: three times) that of the first third, plus 50 ms.
- **The barrier** (a requested cycle, twice) is timed each round with the same three limits.
- **The subscriber** must get an update of the time table about once a second (the server
  sends a subscriber at most one a second, as Java does: `BarrageRequestHelpers`), never go
  10 s (short: 20 s) without one, and the newest row it has seen must never be more than that
  behind the clock.
- **No deadlock.** Every unary call has a deadline of 60 seconds. Above that, a watchdog ends
  the run when one step of a round has not finished in 180 seconds, whatever it waits in: it
  records the step, prints every thread's stack, kills the server and exits 1.
- **Processor time** of the server process per round (user and system, from
  `/proc/<pid>/stat`): the fitted slope times the fitted window may be at most 20% (short: 60%)
  of the median. Processor time is used, not elapsed time, so that other work on the machine
  does not count.

### Shutdown

After the last round the long-lived sessions are closed and the server gets SIGINT. It must
exit with code 0 within 15 seconds.

## Proving that the checks can fail

- `soak.py selftest` runs the judging on made-up series: a steady one and one with spikes must
  pass; a leak of 60 MB per hour, processor time up by half, barriers twenty times slower,
  climbing file descriptors, a starved subscriber, cycles that stop or come ever later must each
  be reported under their own name; and the comparison must tell equal tables from ones that
  differ in a value, a row, a type or an order. CI runs it before each soak run.
- `--inject leak` keeps every round's tables and session: the memory, file descriptor and
  processor time invariants fail.
- `--inject mismatch` recomputes round 3 from inputs that lack one row: the correctness check
  fails for every chain that the row reaches.
- `--inject stall --step-timeout 8` stops making progress in round 3: the watchdog ends the run.

## Reading a failure

The summary names each violation; the JSON file has the same with full detail, and one line
per round under `rounds` (`columns` names the values of a row: time, work time, barrier
time, cycle count and longest cycle gap, processor time of the round, resident size, peak
resident size, threads, file descriptors, processor time so far, subscriber updates, load
average).

To find which part of a round a trend comes from, leave parts out: `--skip` takes any of
`write`, `check`, `script`, `churn`, `abandon`, and with `--round-seconds 0.05` the parts that
stay run many times as often, so a leak per round shows in minutes. `--skip
check,script,churn,abandon` leaves the writes, and is how the leak below was found to be in
the long-lived graph and not in what comes and goes.

| violation | what it means | where to look |
| --- | --- | --- |
| `[correctness] pipeline <name>` | a ticking result differs from the recomputation; the detail has the first differing row of both | the operation's incremental path in `crates/oxhaven-engine/src/ops`; run with the same `--seed` to get the same writes |
| `[correctness] time table check …`, `time table tail` | a chain fed by the clock, the blink count or the ring table is wrong in one cycle | time table, blink and ring sources |
| `[correctness] barrage replica of agg_by_sym` | the updates sent to a subscriber do not add up to the table | `crates/oxhaven-server/src/barrage` |
| `[correctness] rollup view` | the rollup's first levels differ from the aggregation | `docs/HIERARCHICAL.md` |
| `[correctness] churn …`, `blink input aggregation` | a result differs from what the driver computed itself | the operations named in the detail |
| `[requests] <step> failed` | a request the workload expects to succeed failed; the detail has the gRPC code and message | the server log (`--server-log`) at that time |
| `[requests] the server logged errors` | an `ERROR` line or a panic in the log | the log |
| `[memory] resident memory grows` | the fitted slope is above the limit | plot `rss_kb` of `rounds`; a staircase points at the abandoned sessions (every eighth round), a ramp at the churn or the long-lived graph. Rerun with `--console none` to take the console out |
| `[memory] fds …`, `threads …` | connections or threads are not given back | sessions and subscriptions |
| `[liveness] a cycle took too long to come`, `cycles get slower`, `a barrier took too long`, `barriers get slower` | the update graph falls behind | `cycle_gap_s` and `barrier_s` of `rounds` against `load`: a single spike with a high load average is the machine |
| `[liveness] processor time per round climbs` | the same work costs more as the run goes on | `cpu_round_s` of `rounds` |
| `[liveness] stalled in step: <step>` | a request never returned | the stacks printed above it, and the server log |
| `[shutdown] the server did not stop cleanly` | no exit, or a non-zero one, after SIGINT | the end of the server log |

A check that is known to fail can be listed in `KNOWN_FAILURES` of `bench/soak.py` with the
place its reproduction is written down; it is then reported as a known failure and does not
fail the run, and every other check stays enforced. The list is empty.

## What it has found

**A sort by a rising column leaked 12 bytes per row** (fixed with task 12.3). The first long
run failed the memory invariant. With `--skip` the growth was in the writes alone, 7.8 KB a
round; with one chain at a time it was in every chain that starts with `sort("Ts")`, where
`Ts` rises with every write, and in no other. The incremental sort put rows that sort last
after the last row and grew its key space for them, and never used the keys of the rows that
had left again, so the arrays it indexes by row key grew with every row there ever was
(`crates/oxhaven-engine/src/ops/sort/gapped.rs`, test
`rows_appended_while_others_leave_keep_the_key_space_in_proportion`). The measurements are
in `tasks/12.3-soak-test.md`.

## In CI

- **Pull requests**: the `oracle-replay` job of `.github/workflows/ci.yml` has already built
  the server with its console and has the client's environment, so the short profile runs
  there as a last step, against that debug binary: `selftest`, then two minutes of workload.
  It adds about two and a half minutes to that job and no build.
- **Nightly**: the `soak` job of `.github/workflows/nightly.yml` builds the release server
  with its console and runs the long profile, 45 minutes of workload; with the build, about an
  hour. The result file and the server log are kept as an artifact, pass or fail.

## What it does not cover

- Storage (Parquet, CSV) and Kafka tables, partitioned tables, trees, figures and plugins are
  not in the workload.
- One seed per run. The nightly run uses seed 1 every night; the churn's choice of operations
  varies within a run, not between runs.
- Without a console: no cycle count, no same-cycle check of the tables fed by the clock, no
  ring table, no rollup.
- The count of live exports and sessions, as said under "Memory".
- The rollup is compared at its root and first level only, in the default expansion.
